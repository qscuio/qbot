use chrono::{Datelike, Duration, Utc};
use redis::AsyncCommands;
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, warn};

use crate::analysis::adapters::official_event_source::OfficialEventSource;
use crate::analysis::adapters::{EventSource, FetchedEvent};
use crate::analysis::events::extraction::StockCodeDirectory;
use crate::analysis::events::{
    render_daily_brief, AShareTradingDateResolver, EventIntelligence, TradingDateResolver,
};
use crate::analysis::market_snapshot::{
    ingestion::PointInTimeIngestion, MarketSnapshotModule, MARKET_SNAPSHOT_VERSION,
};
use crate::analysis::patterns::matcher::PatternEngine;
use crate::data::provider::DataProvider;
use crate::market_time::{beijing_today, beijing_tz};
use crate::services::{
    limit_up::LimitUpService, market::MarketService, market_report::MarketReportService,
    scanner::ScannerService, sector::SectorService, signal_auto_trading::SignalAutoTradingService,
    stock_history::StockHistoryService,
};
use crate::state::AppState;
use crate::storage::event_repository::{DailyEventBriefRow, EventEvidenceRow, EventRepository};
use crate::storage::market_repository::MarketRepository;
use crate::storage::pattern_repository::PatternRepository;
use crate::storage::postgres;
use crate::telegram::pusher::TelegramPusher;

const FETCH_JOB_CRON: &str = "0 0 17 * * Mon,Tue,Wed,Thu,Fri";
const POINT_IN_TIME_TRADE_DATE_JOB_CRON: &str = "0 10 17 * * Mon,Tue,Wed,Thu,Fri";
const MARKET_SNAPSHOT_JOB_CRON: &str = "0 20 17 * * Mon,Tue,Wed,Thu,Fri";
const SCAN_JOB_CRON: &str = "0 30 17 * * Mon,Tue,Wed,Thu,Fri";
const PATTERN_SHADOW_JOB_CRON: &str = "0 40 17 * * Mon,Tue,Wed,Thu,Fri";
const EVENT_INGESTION_JOB_CRON: &str = "0 5 9-17 * * Mon,Tue,Wed,Thu,Fri";
const EVENT_FACT_BRIEF_JOB_CRON: &str = "0 50 17 * * Mon,Tue,Wed,Thu,Fri";
const DAILY_SIGNAL_ARCHIVE_JOB_CRON: &str = "0 5 20 * * Mon,Tue,Wed,Thu,Fri";
const DAILY_REPORT_JOB_CRON: &str = "0 0 18 * * Mon,Tue,Wed,Thu,Fri";
const WEEKLY_REPORT_JOB_CRON: &str = "0 0 20 * * Fri";
const POINT_IN_TIME_REFERENCE_JOB_CRON: &str = "0 15 17 * * Fri";
static EVENT_FACT_BRIEF_JOB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Fetch today's OHLCV, limit-up stocks, and sector data (17:00 job).
pub async fn run_fetch_job(state: Arc<AppState>, provider: Arc<dyn DataProvider>) {
    let _guard = state.fetch_job_lock.lock().await;
    let today = beijing_today();
    info!("Fetch job: OHLCV + limit-up + sector for {}", today);

    let history_svc = StockHistoryService::new(state.clone(), provider.clone());
    if let Err(e) = history_svc.update_today().await {
        warn!("Daily data fetch failed: {}", e);
    }

    let limit_svc = LimitUpService::new(state.clone(), provider.clone());
    match limit_svc.fetch_and_save(today).await {
        Ok(stocks) => info!("Limit-up: {} stocks", stocks.len()),
        Err(e) => warn!("Limit-up fetch failed: {}", e),
    }

    let sector_svc = SectorService::new(state.clone(), provider.clone());
    if let Err(e) = sector_svc.fetch_and_save(today).await {
        warn!("Sector data failed: {}", e);
    }
}

pub async fn run_point_in_time_reference_refresh_job(state: Arc<AppState>) {
    let _guard = state.analysis_job_lock.lock().await;
    let ingestion =
        PointInTimeIngestion::new(state.point_in_time_provider.clone(), state.db.clone());
    match ingestion.refresh_reference_data(Utc::now()).await {
        Ok(result) => info!(
            "Point-in-time reference refresh: status={:?}, inserted={}, estimated={}, excluded={}",
            result.status,
            result.inserted_rows,
            result.estimated_rows,
            result.excluded_estimated_rows
        ),
        Err(e) => warn!("Point-in-time reference refresh failed: {}", e),
    }
}

pub async fn run_point_in_time_trade_date_refresh_job(state: Arc<AppState>) {
    let _guard = state.analysis_job_lock.lock().await;
    let trade_date = beijing_today();
    let ingestion =
        PointInTimeIngestion::new(state.point_in_time_provider.clone(), state.db.clone());
    match ingestion.refresh_trade_date(trade_date, Utc::now()).await {
        Ok(result) => info!(
            "Point-in-time trade-date refresh: date={}, status={:?}, inserted={}, estimated={}, excluded={}",
            trade_date,
            result.status,
            result.inserted_rows,
            result.estimated_rows,
            result.excluded_estimated_rows
        ),
        Err(e) => warn!("Point-in-time trade-date refresh failed: {}", e),
    }
}

pub async fn run_market_snapshot_job(state: Arc<AppState>) {
    let _guard = state.analysis_job_lock.lock().await;
    let trade_date = match postgres::latest_stock_trade_date(&state.db).await {
        Ok(Some(value)) => value,
        Ok(None) => return,
        Err(error) => {
            warn!(
                "Market snapshot skipped: latest trade date lookup failed: {}",
                error
            );
            return;
        }
    };

    let module = MarketSnapshotModule::new(state.db.clone());
    if let Err(error) = module
        .build_trade_date(trade_date, chrono::Utc::now())
        .await
    {
        warn!("Market snapshot failed: {}", error);
    }
}

/// Run all enabled signal detectors and cache results to Redis (17:30 job).
pub async fn run_scan_job(state: Arc<AppState>) {
    let _guard = state.scan_job_lock.lock().await;
    info!("Scan job: running full signal scan");
    let scanner = ScannerService::new(state.clone());
    match scanner.run_full_scan().await {
        Ok(results) => {
            if state.config.enable_signal_auto_trading {
                let auto_svc = SignalAutoTradingService::new(
                    state.clone(),
                    Arc::new(crate::data::sina::SinaClient::new()),
                );
                if let Err(e) = auto_svc.prepare_candidates_from_scan(&results).await {
                    warn!("Signal auto candidate prep failed: {}", e);
                }
            }
        }
        Err(e) => {
            warn!("Scan failed: {}", e);
        }
    }
}

/// Match latest published patterns against the latest complete market snapshot (17:40 job).
pub async fn run_pattern_shadow_job(state: Arc<AppState>) {
    let _guard = state.analysis_job_lock.lock().await;
    let pattern_repo = PatternRepository::new(state.db.clone());
    let market_repo = MarketRepository::new(state.db.clone());

    let pattern_set = match pattern_repo.latest_published_set().await {
        Ok(Some(pattern_set)) => pattern_set,
        Ok(None) => {
            info!("Pattern shadow job skipped: no latest published pattern set");
            return;
        }
        Err(error) => {
            warn!("Pattern shadow job skipped: latest published set lookup failed: {error}");
            return;
        }
    };

    let trade_date = match postgres::latest_stock_trade_date(&state.db).await {
        Ok(Some(trade_date)) => trade_date,
        Ok(None) => match market_repo
            .latest_market_snapshot(MARKET_SNAPSHOT_VERSION)
            .await
        {
            Ok(Some(snapshot)) if snapshot.data_complete => snapshot.trade_date,
            Ok(Some(snapshot)) => {
                warn!(
                    "Pattern shadow job skipped: latest market snapshot is incomplete: trade_date={}, missing_inputs={}",
                    snapshot.trade_date,
                    snapshot.missing_inputs.len()
                );
                return;
            }
            Ok(None) => {
                warn!("Pattern shadow job skipped: no stock trade date or market snapshot");
                return;
            }
            Err(error) => {
                warn!("Pattern shadow job skipped: latest market snapshot lookup failed: {error}");
                return;
            }
        },
        Err(error) => {
            warn!("Pattern shadow job skipped: latest stock trade date lookup failed: {error}");
            return;
        }
    };

    let snapshot = match market_repo
        .market_snapshot(trade_date, MARKET_SNAPSHOT_VERSION)
        .await
    {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            warn!("Pattern shadow job skipped: market snapshot missing for {trade_date}");
            return;
        }
        Err(error) => {
            warn!("Pattern shadow job skipped: market snapshot lookup failed: {error}");
            return;
        }
    };
    if !snapshot.data_complete {
        warn!(
            "Pattern shadow job skipped: market snapshot incomplete for {}, missing_inputs={}",
            trade_date,
            snapshot.missing_inputs.len()
        );
        return;
    }

    let engine = PatternEngine::new(pattern_repo, market_repo);
    match engine
        .match_market(trade_date, pattern_set.pattern_set_id)
        .await
    {
        Ok(candidates) => info!(
            "Pattern shadow job persisted candidates: trade_date={}, pattern_set_id={}, count={}",
            trade_date,
            pattern_set.pattern_set_id,
            candidates.len()
        ),
        Err(error) => warn!("Pattern shadow job failed: {error}"),
    }
}

/// Run all enabled signal detectors and save a daily archive snapshot (20:05 job).
pub async fn run_daily_signal_archive_job(state: Arc<AppState>) {
    let _guard = state.scan_job_lock.lock().await;
    let scan_date = match postgres::latest_stock_trade_date(&state.db).await {
        Ok(Some(date)) => date,
        Ok(None) => {
            warn!("Daily signal archive skipped: stock_daily_bars is empty");
            return;
        }
        Err(e) => {
            warn!(
                "Daily signal archive skipped: latest trade date failed: {}",
                e
            );
            return;
        }
    };

    info!("Daily signal archive job: scanning for {}", scan_date);
    let scanner = ScannerService::new(state.clone());
    match scanner.run_daily_archive_scan(scan_date).await {
        Ok(summary) => info!(
            "Daily signal archive saved: date={}, rows={}, codes={}, signals={}",
            summary.scan_date, summary.rows, summary.codes, summary.signals
        ),
        Err(e) => warn!("Daily signal archive failed: {}", e),
    }
}

pub async fn run_event_ingestion_job(state: Arc<AppState>) {
    let _guard = state.analysis_job_lock.lock().await;
    let now = Utc::now();
    let source = match OfficialEventSource::from_config(state.config.as_ref()) {
        Ok(Some(source)) => source,
        Ok(None) => {
            info!("Event ingestion skipped: OFFICIAL_EVENT_FEED_URL is not configured");
            return;
        }
        Err(error) => {
            warn!(
                "Event ingestion skipped: official source config failed: {}",
                error
            );
            return;
        }
    };

    let cursor_key = event_cursor_key(source.source_id());
    let current_cursor = match event_cursor_get(state.redis.clone(), &cursor_key).await {
        Ok(cursor) => cursor,
        Err(error) => {
            warn!("Event ingestion skipped: cursor read failed: {}", error);
            return;
        }
    };

    let batch = match source.fetch(current_cursor.clone(), now).await {
        Ok(batch) => batch,
        Err(error) => {
            warn!("Event ingestion fetch failed: {}", error);
            return;
        }
    };

    let repo = EventRepository::new(state.db.clone());
    let mut ingest_failed = false;
    for item in &batch.items {
        if let Err(error) = ingest_fetched_event(&repo, &source, item, now).await {
            ingest_failed = true;
            warn!(
                "Event ingestion upsert failed for source={} item={}: {}",
                source.source_id(),
                item.source_item_id,
                error
            );
        }
    }

    if !ingest_failed {
        if let Some(next_cursor) = batch.next_cursor.as_deref() {
            if let Err(error) =
                event_cursor_set(state.redis.clone(), &cursor_key, next_cursor).await
            {
                warn!("Event ingestion cursor write failed: {}", error);
                return;
            }
        }
    } else {
        warn!(
            "Event ingestion cursor preserved for source={} because at least one item failed",
            source.source_id()
        );
    }

    let stock_list = match state.provider.get_stock_list().await {
        Ok(stocks) => stocks,
        Err(error) => {
            warn!(
                "Event ingestion extraction skipped: stock directory load failed: {}",
                error
            );
            return;
        }
    };
    let extractor =
        match crate::analysis::adapters::llm_event_extractor::LlmEventExtractor::from_config(
            state.config.as_ref(),
            Arc::new(StockCodeDirectory::from_known_codes(
                stock_list.iter().map(|stock| stock.code.as_str()),
            )),
        ) {
            Ok(extractor) => Arc::new(extractor),
            Err(error) => {
                warn!(
                    "Event ingestion extraction skipped: extractor config failed: {}",
                    error
                );
                return;
            }
        };
    let intelligence = EventIntelligence::with_repository_resolver_and_extractor(
        repo,
        Arc::new(AShareTradingDateResolver),
        extractor,
    );
    if let Err(error) = intelligence.process_pending(now).await {
        warn!("Event ingestion extraction failed: {}", error);
    }
}

pub async fn run_event_fact_brief_job(state: Arc<AppState>) {
    let _guard = EVENT_FACT_BRIEF_JOB_LOCK.lock().await;
    let trade_date = beijing_today();
    let intelligence = EventIntelligence::new(state.db.clone());
    let brief = match intelligence.build_daily_brief(trade_date).await {
        Ok(brief) => brief,
        Err(error) => {
            warn!(
                "Event fact brief failed to build for {}: {}",
                trade_date, error
            );
            return;
        }
    };
    let content = match render_daily_brief(&brief) {
        Ok(content) => content,
        Err(error) => {
            warn!(
                "Event fact brief failed to render for {}: {}",
                trade_date, error
            );
            return;
        }
    };

    let row = DailyEventBriefRow {
        trade_date,
        brief_version: "daily_event_brief_v1".to_string(),
        content: content.clone(),
        structured_payload: match serde_json::to_value(&brief) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    "Event fact brief failed to serialize for {}: {}",
                    trade_date, error
                );
                return;
            }
        },
        input_fingerprint: brief.input_fingerprint.clone(),
        generated_at: Utc::now(),
    };
    if let Err(error) = EventRepository::new(state.db.clone())
        .save_daily_brief(&row)
        .await
    {
        warn!(
            "Event fact brief persistence failed for {}: {}",
            trade_date, error
        );
        return;
    }

    if let Some(channel) = &state.config.report_channel {
        if let Err(error) = state.pusher.push(channel, &content).await {
            warn!("Event fact brief push failed for {}: {}", trade_date, error);
        }
    }
}

/// Generate daily market report and push to Telegram (18:00 job).
pub async fn run_daily_report_job(
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
    pusher: Arc<TelegramPusher>,
) {
    let _guard = state.daily_report_job_lock.lock().await;
    let today = beijing_today();
    info!("Daily report job for {}", today);

    let market_svc = Arc::new(MarketService::new(state.clone(), provider.clone()));
    let limit_svc = Arc::new(LimitUpService::new(state.clone(), provider.clone()));
    let sector_svc = Arc::new(SectorService::new(state.clone(), provider.clone()));
    let report_svc = MarketReportService::new(state.clone(), market_svc, limit_svc, sector_svc);

    match report_svc.generate_daily(today).await {
        Ok(report) => {
            if let Some(channel) = &state.config.report_channel {
                if let Err(e) = pusher.push(channel, &report).await {
                    warn!("Telegram push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Daily report failed: {}", e),
    }

    let alert_channel = state
        .config
        .stock_alert_channel
        .as_ref()
        .or(state.config.report_channel.as_ref());

    match report_svc.generate_limitup_report(today).await {
        Ok(report) => {
            if let Some(channel) = alert_channel {
                let push_result = match report_svc.load_limitup_report_data(today).await {
                    Ok(stocks) => match crate::telegram::formatter::limit_up_report_markup(&stocks)
                    {
                        Some(markup) => pusher.push_with_markup(channel, &report, markup).await,
                        None => pusher.push(channel, &report).await,
                    },
                    Err(_) => pusher.push(channel, &report).await,
                };
                if let Err(e) = push_result {
                    warn!("Limit-up report push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Limit-up standalone report failed: {}", e),
    }

    match report_svc.generate_strong_report(today, 7).await {
        Ok(report) => {
            if let Some(channel) = alert_channel {
                let push_result = match report_svc.load_strong_report_data(7).await {
                    Ok(stocks) => {
                        match crate::telegram::formatter::strong_stock_report_markup(&stocks) {
                            Some(markup) => pusher.push_with_markup(channel, &report, markup).await,
                            None => pusher.push(channel, &report).await,
                        }
                    }
                    Err(_) => pusher.push(channel, &report).await,
                };
                if let Err(e) = push_result {
                    warn!("Strong-stock report push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Strong-stock standalone report failed: {}", e),
    }

    if state.config.enable_signal_auto_trading {
        let auto_svc = SignalAutoTradingService::new(
            state.clone(),
            Arc::new(crate::data::sina::SinaClient::new()),
        );
        match auto_svc.generate_daily_report(today).await {
            Ok(report) => {
                if let Some(channel) = alert_channel {
                    if let Err(e) = pusher.push(channel, &report).await {
                        warn!("Signal-auto report push failed: {}", e);
                    }
                }
            }
            Err(e) => warn!("Signal-auto daily report failed: {}", e),
        }
    }
}

/// Generate weekly market report and push to Telegram (Friday 20:00 job).
pub async fn run_weekly_report_job(
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
    pusher: Arc<TelegramPusher>,
) {
    let _guard = state.weekly_report_job_lock.lock().await;
    info!("Weekly report job");

    let market_svc = Arc::new(MarketService::new(state.clone(), provider.clone()));
    let limit_svc = Arc::new(LimitUpService::new(state.clone(), provider.clone()));
    let sector_svc = Arc::new(SectorService::new(state.clone(), provider.clone()));
    let report_svc = MarketReportService::new(state.clone(), market_svc, limit_svc, sector_svc);
    let today = beijing_today();
    let start = today - Duration::days(today.weekday().num_days_from_monday() as i64);

    match report_svc.generate_weekly().await {
        Ok(report) => {
            if let Some(channel) = &state.config.report_channel {
                let push_result = match report_svc.load_weekly_report_rows(start, today).await {
                    Ok(rows) => match crate::services::market_report::weekly_report_markup(&rows) {
                        Some(markup) => pusher.push_with_markup(channel, &report, markup).await,
                        None => pusher.push(channel, &report).await,
                    },
                    Err(_) => pusher.push(channel, &report).await,
                };
                if let Err(e) = push_result {
                    warn!("Telegram push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Weekly report failed: {}", e),
    }
}

pub async fn start_scheduler(
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
    pusher: Arc<TelegramPusher>,
) -> anyhow::Result<JobScheduler> {
    let sched = JobScheduler::new().await?;

    // 17:00 weekdays
    {
        let s = state.clone();
        let p = provider.clone();
        sched
            .add(Job::new_async_tz(
                FETCH_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    let p = p.clone();
                    Box::pin(async move { run_fetch_job(s, p).await })
                },
            )?)
            .await?;
    }

    // 17:10 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                POINT_IN_TIME_TRADE_DATE_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_point_in_time_trade_date_refresh_job(s).await })
                },
            )?)
            .await?;
    }

    // 17:15 Friday
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                POINT_IN_TIME_REFERENCE_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_point_in_time_reference_refresh_job(s).await })
                },
            )?)
            .await?;
    }

    // 17:20 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                MARKET_SNAPSHOT_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_market_snapshot_job(s).await })
                },
            )?)
            .await?;
    }

    // 17:30 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                SCAN_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_scan_job(s).await })
                },
            )?)
            .await?;
    }

    // 17:40 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                PATTERN_SHADOW_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_pattern_shadow_job(s).await })
                },
            )?)
            .await?;
    }

    // hourly weekdays during configured event-ingestion hours
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                EVENT_INGESTION_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_event_ingestion_job(s).await })
                },
            )?)
            .await?;
    }

    // 17:50 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                EVENT_FACT_BRIEF_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_event_fact_brief_job(s).await })
                },
            )?)
            .await?;
    }

    // 18:00 weekdays
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched
            .add(Job::new_async_tz(
                DAILY_REPORT_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    let p = p.clone();
                    let push = push.clone();
                    Box::pin(async move { run_daily_report_job(s, p, push).await })
                },
            )?)
            .await?;
    }

    // 20:00 Friday
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched
            .add(Job::new_async_tz(
                WEEKLY_REPORT_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    let p = p.clone();
                    let push = push.clone();
                    Box::pin(async move { run_weekly_report_job(s, p, push).await })
                },
            )?)
            .await?;
    }

    // 20:05 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                DAILY_SIGNAL_ARCHIVE_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_daily_signal_archive_job(s).await })
                },
            )?)
            .await?;
    }

    sched.start().await?;
    info!("Scheduler started with 11 jobs");
    Ok(sched)
}

fn production_job_crons_in_registration_order() -> Vec<&'static str> {
    vec![
        FETCH_JOB_CRON,
        POINT_IN_TIME_TRADE_DATE_JOB_CRON,
        POINT_IN_TIME_REFERENCE_JOB_CRON,
        MARKET_SNAPSHOT_JOB_CRON,
        SCAN_JOB_CRON,
        PATTERN_SHADOW_JOB_CRON,
        EVENT_INGESTION_JOB_CRON,
        EVENT_FACT_BRIEF_JOB_CRON,
        DAILY_REPORT_JOB_CRON,
        WEEKLY_REPORT_JOB_CRON,
        DAILY_SIGNAL_ARCHIVE_JOB_CRON,
    ]
}

fn event_cursor_key(source_id: &str) -> String {
    format!("market_event:provider_cursor:{source_id}")
}

async fn event_cursor_get(
    mut redis: redis::aio::ConnectionManager,
    key: &str,
) -> crate::error::Result<Option<String>> {
    redis.get(key).await.map_err(crate::error::AppError::Redis)
}

async fn event_cursor_set(
    mut redis: redis::aio::ConnectionManager,
    key: &str,
    value: &str,
) -> crate::error::Result<()> {
    redis
        .set::<_, _, ()>(key, value)
        .await
        .map_err(crate::error::AppError::Redis)?;
    Ok(())
}

async fn ingest_fetched_event(
    repo: &EventRepository,
    source: &impl EventSource,
    item: &FetchedEvent,
    first_seen_at: chrono::DateTime<Utc>,
) -> crate::error::Result<()> {
    let canonical_source_url =
        crate::storage::event_repository::canonicalize_source_url(&item.source_url)?;
    let content_hash = event_content_hash(&item.title, item.content.as_deref());
    let available_at = item.published_at;
    let effective_trade_date = AShareTradingDateResolver.effective_trade_date(available_at)?;

    if let Some(latest) = repo
        .latest_evidence_for_source_item(source.source_id(), &item.source_item_id)
        .await?
    {
        if latest_event_matches_fetched(
            &latest,
            item,
            canonical_source_url.as_str(),
            content_hash.as_str(),
        ) {
            return Ok(());
        }

        repo.insert_evidence(&EventEvidenceRow {
            evidence_id: uuid::Uuid::new_v4(),
            source_id: source.source_id().to_string(),
            source_item_id: item.source_item_id.clone(),
            source_url: Some(canonical_source_url),
            source_tier: "official".to_string(),
            source_terms_version: "terms-v1".to_string(),
            occurred_at: Some(item.published_at),
            published_at: Some(item.published_at),
            first_seen_at,
            available_at,
            effective_trade_date,
            title: item.title.clone(),
            content: item.content.clone(),
            language: "und".to_string(),
            content_hash,
            raw_payload: item.raw_payload.clone(),
            version: latest.version + 1,
            supersedes_evidence_id: Some(latest.evidence_id),
            status: "pending".to_string(),
            created_at: first_seen_at,
        })
        .await?;
        return Ok(());
    }

    repo.insert_evidence(&EventEvidenceRow {
        evidence_id: uuid::Uuid::new_v4(),
        source_id: source.source_id().to_string(),
        source_item_id: item.source_item_id.clone(),
        source_url: Some(canonical_source_url),
        source_tier: "official".to_string(),
        source_terms_version: "terms-v1".to_string(),
        occurred_at: Some(item.published_at),
        published_at: Some(item.published_at),
        first_seen_at,
        available_at,
        effective_trade_date,
        title: item.title.clone(),
        content: item.content.clone(),
        language: "und".to_string(),
        content_hash,
        raw_payload: item.raw_payload.clone(),
        version: 1,
        supersedes_evidence_id: None,
        status: "pending".to_string(),
        created_at: first_seen_at,
    })
    .await?;

    Ok(())
}

fn latest_event_matches_fetched(
    latest: &EventEvidenceRow,
    fetched: &FetchedEvent,
    canonical_source_url: &str,
    content_hash: &str,
) -> bool {
    latest.source_url.as_deref() == Some(canonical_source_url)
        && latest.published_at == Some(fetched.published_at)
        && latest.title == fetched.title
        && latest.content.as_deref() == fetched.content.as_deref()
        && latest.content_hash == content_hash
        && latest.raw_payload == fetched.raw_payload
}

fn event_content_hash(title: &str, content: Option<&str>) -> String {
    use sha2::{Digest, Sha256};

    let normalize = |value: &str| value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut hasher = Sha256::new();
    hasher.update(normalize(title));
    if let Some(content) = content {
        hasher.update([0]);
        hasher.update(normalize(content));
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::{http::header, routing::get, Router};
    use chrono::{DateTime, Duration, NaiveDate, TimeZone};
    use serde_json::{json, Value};
    use sqlx::PgPool;
    use std::collections::BTreeMap;
    use std::sync::Arc as StdArc;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    use tokio::time::{timeout, Duration as TokioDuration};
    use uuid::Uuid;

    use crate::analysis::market_snapshot::{
        AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, MarketSnapshot,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion, MARKET_SNAPSHOT_VERSION,
    };
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
    use crate::error::Result;
    use crate::storage::event_repository::{
        ClaimEvidenceRow, ClaimRow, EventRepository, ExtractionRow,
    };
    use crate::storage::market_repository::MarketRepository;

    #[test]
    fn weekday_pipeline_runs_after_tushare_eod_window() {
        assert_eq!(FETCH_JOB_CRON, "0 0 17 * * Mon,Tue,Wed,Thu,Fri");
        assert_eq!(
            POINT_IN_TIME_TRADE_DATE_JOB_CRON,
            "0 10 17 * * Mon,Tue,Wed,Thu,Fri"
        );
        assert_eq!(MARKET_SNAPSHOT_JOB_CRON, "0 20 17 * * Mon,Tue,Wed,Thu,Fri");
        assert_eq!(SCAN_JOB_CRON, "0 30 17 * * Mon,Tue,Wed,Thu,Fri");
        assert_eq!(DAILY_REPORT_JOB_CRON, "0 0 18 * * Mon,Tue,Wed,Thu,Fri");
        assert_eq!(
            DAILY_SIGNAL_ARCHIVE_JOB_CRON,
            "0 5 20 * * Mon,Tue,Wed,Thu,Fri"
        );
    }

    #[test]
    fn pattern_shadow_job_runs_after_scan_and_before_daily_report() {
        assert_eq!(PATTERN_SHADOW_JOB_CRON, "0 40 17 * * Mon,Tue,Wed,Thu,Fri");
    }

    #[test]
    fn event_jobs_run_before_the_daily_market_report() {
        assert_eq!(EVENT_INGESTION_JOB_CRON, "0 5 9-17 * * Mon,Tue,Wed,Thu,Fri");
        assert_eq!(EVENT_FACT_BRIEF_JOB_CRON, "0 50 17 * * Mon,Tue,Wed,Thu,Fri");
    }

    #[test]
    fn weekly_report_schedule_stays_on_friday_evening() {
        assert_eq!(WEEKLY_REPORT_JOB_CRON, "0 0 20 * * Fri");
        assert_eq!(POINT_IN_TIME_REFERENCE_JOB_CRON, "0 15 17 * * Fri");
    }

    #[test]
    fn analysis_jobs_register_reference_refresh_before_friday_snapshot() {
        assert_eq!(
            production_job_crons_in_registration_order(),
            vec![
                "0 0 17 * * Mon,Tue,Wed,Thu,Fri",
                "0 10 17 * * Mon,Tue,Wed,Thu,Fri",
                "0 15 17 * * Fri",
                "0 20 17 * * Mon,Tue,Wed,Thu,Fri",
                "0 30 17 * * Mon,Tue,Wed,Thu,Fri",
                "0 40 17 * * Mon,Tue,Wed,Thu,Fri",
                "0 5 9-17 * * Mon,Tue,Wed,Thu,Fri",
                "0 50 17 * * Mon,Tue,Wed,Thu,Fri",
                "0 0 18 * * Mon,Tue,Wed,Thu,Fri",
                "0 0 20 * * Fri",
                "0 5 20 * * Mon,Tue,Wed,Thu,Fri",
            ]
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pattern_shadow_job_skips_without_published_model_and_preserves_scan_results(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 10);
        seed_stock_daily_bar(&pool, trade_date, "600001.SH").await?;
        seed_scan_result(&pool).await?;
        MarketRepository::new(pool.clone())
            .save_market_snapshot(&MarketSnapshot {
                trade_date,
                snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                available_at: dt(2026, 7, 10, 10),
                data_complete: true,
                metrics: json!({"market_regime": "normal"}),
                missing_inputs: Vec::new(),
                input_fingerprint: "complete-snapshot".to_string(),
            })
            .await
            .unwrap();

        let scan_count_before = count_rows(&pool, "scan_results").await?;
        let strategy_candidate_count_before =
            count_rows(&pool, "signal_strategy_candidates").await?;

        run_pattern_shadow_job(state).await;

        assert_eq!(count_rows(&pool, "scan_results").await?, scan_count_before);
        assert_eq!(count_rows(&pool, "analysis_shadow_candidates").await?, 0);
        assert_eq!(
            count_rows(&pool, "signal_strategy_candidates").await?,
            strategy_candidate_count_before
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pattern_shadow_job_skips_incomplete_snapshot_and_preserves_scan_results(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 10);
        seed_stock_daily_bar(&pool, trade_date, "600001.SH").await?;
        seed_scan_result(&pool).await?;
        seed_published_pattern_set(&pool).await?;
        MarketRepository::new(pool.clone())
            .save_market_snapshot(&MarketSnapshot {
                trade_date,
                snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                available_at: dt(2026, 7, 10, 10),
                data_complete: false,
                metrics: json!({"market_regime": "normal"}),
                missing_inputs: vec!["daily_basic:600001.SH:2026-07-10".to_string()],
                input_fingerprint: "incomplete-snapshot".to_string(),
            })
            .await
            .unwrap();

        let scan_count_before = count_rows(&pool, "scan_results").await?;
        let strategy_candidate_count_before =
            count_rows(&pool, "signal_strategy_candidates").await?;

        run_pattern_shadow_job(state).await;

        assert_eq!(count_rows(&pool, "scan_results").await?, scan_count_before);
        assert_eq!(count_rows(&pool, "analysis_shadow_candidates").await?, 0);
        assert_eq!(
            count_rows(&pool, "signal_strategy_candidates").await?,
            strategy_candidate_count_before
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pattern_shadow_job_persists_shadow_candidates_without_strategy_candidates_or_scan_changes(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 10);
        let code = "600001.SH";
        seed_stock_daily_bar(&pool, trade_date, code).await?;
        seed_pattern_market_inputs(&pool, trade_date, code).await?;
        seed_scan_result(&pool).await?;
        seed_published_pattern_set(&pool).await?;
        MarketRepository::new(pool.clone())
            .save_market_snapshot(&MarketSnapshot {
                trade_date,
                snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                available_at: dt(2026, 7, 10, 10),
                data_complete: true,
                metrics: json!({
                    "breadth": {
                        "up_count": 1,
                        "down_count": 0,
                        "flat_count": 0,
                        "above_ma20_count": 1
                    }
                }),
                missing_inputs: Vec::new(),
                input_fingerprint: "complete-pattern-snapshot".to_string(),
            })
            .await
            .unwrap();

        let scan_count_before = count_rows(&pool, "scan_results").await?;
        let shadow_count_before = count_rows(&pool, "analysis_shadow_candidates").await?;
        let strategy_candidate_count_before =
            count_rows(&pool, "signal_strategy_candidates").await?;

        run_pattern_shadow_job(state).await;

        let shadow_count_after = count_rows(&pool, "analysis_shadow_candidates").await?;
        assert!(shadow_count_after > shadow_count_before);
        assert_eq!(count_rows(&pool, "scan_results").await?, scan_count_before);
        assert_eq!(
            count_rows(&pool, "signal_strategy_candidates").await?,
            strategy_candidate_count_before
        );
        assert_eq!(strategy_candidate_count_before, 0);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_ingestion_job_returns_cleanly_when_official_source_config_is_invalid(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let mut config = (*state.config).clone();
        config.official_event_feed_url = Some("https://example.test/feed".to_string());
        config.official_event_source_id = "official:unsupported".to_string();
        let state = Arc::new(AppState {
            config: Arc::new(config),
            ..(*state).clone()
        });

        run_event_ingestion_job(state).await;

        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM market_event_evidence")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_ingestion_job_does_not_advance_cursor_when_any_item_ingest_fails(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let (feed_url, server) = spawn_event_feed_server(
            r#"{
              "next_cursor": "cursor-2",
              "items": [
                {
                  "source_item_id": "notice-001",
                  "published_at": "2026-07-10T08:15:00Z",
                  "title": "Exchange trading status update",
                  "content": "Full bulletin body.",
                  "summary": "Exchange confirms normal trading conditions.",
                  "source_url": "https://example.test/notices/notice-001",
                  "category": "market-status"
                },
                {
                  "source_item_id": "notice-002",
                  "published_at": "2026-07-10T08:30:00Z",
                  "title": "Broken notice payload",
                  "content": "Bad source URL should fail ingestion.",
                  "summary": "Broken source URL should fail ingestion.",
                  "source_url": "not-a-url",
                  "category": "market-status"
                }
              ]
            }"#,
        )
        .await;
        let mut config = (*state.config).clone();
        config.official_event_feed_url = Some(feed_url);
        config.official_event_store_full_content = true;
        let state = Arc::new(AppState {
            config: Arc::new(config),
            ..(*state).clone()
        });
        let cursor_key = event_cursor_key("official:market_event");
        event_cursor_set(state.redis.clone(), &cursor_key, "cursor-1")
            .await
            .unwrap();

        run_event_ingestion_job(state.clone()).await;

        assert_eq!(
            event_cursor_get(state.redis.clone(), &cursor_key)
                .await
                .unwrap()
                .as_deref(),
            Some("cursor-1")
        );
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM market_event_evidence")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 1);

        server.abort();
        let _ = server.await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_fact_brief_job_keeps_failures_isolated_and_does_not_persist_bad_briefs(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let published = event_evidence(
            "official:market_event",
            "notice-brief-001",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        let unrelated = event_evidence(
            "official:market_event",
            "notice-brief-999",
            1,
            "pending",
            date(2026, 7, 9),
        );
        repo.insert_evidence(&published).await.unwrap();
        repo.insert_evidence(&unrelated).await.unwrap();
        repo.save_extraction(&ExtractionRow {
            extraction_id: Uuid::new_v4(),
            evidence_id: published.evidence_id,
            schema_version: "event-schema-v1".to_string(),
            prompt_version: Some("prompt-v1".to_string()),
            model_name: Some("test-model".to_string()),
            model_parameters: json!({"temperature": 0}),
            extracted_payload: json!({
                "event_type": "issuer_disclosure",
                "event_subtype": null,
                "claims": [],
                "entities": [],
                "amounts": [],
                "dates": [],
                "uncertainties": [],
                "missing_information": []
            }),
            validation_status: "valid".to_string(),
            validation_errors: json!([]),
            input_fingerprint: "fingerprint-v1".to_string(),
            claims: vec![ClaimRow {
                claim_id: Uuid::new_v4(),
                claim_type: "fact".to_string(),
                claim_text: "公司披露新的正式事项。".to_string(),
                confidence: 0.98,
                review_status: "published".to_string(),
                evidence: vec![ClaimEvidenceRow {
                    evidence_id: unrelated.evidence_id,
                }],
                created_at: Utc::now(),
            }],
            created_at: Utc::now(),
        })
        .await
        .unwrap();

        run_event_fact_brief_job(state).await;

        assert!(repo
            .find_daily_brief(Some(date(2026, 7, 10)))
            .await
            .unwrap()
            .is_none());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_fact_brief_job_does_not_wait_on_daily_report_lock(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let _daily_report_guard = state.daily_report_job_lock.lock().await;

        let run = timeout(
            TokioDuration::from_secs(1),
            run_event_fact_brief_job(state.clone()),
        )
        .await;

        assert!(
            run.is_ok(),
            "fact brief job should not block on daily report lock"
        );
        Ok(())
    }

    #[test]
    fn event_fact_brief_job_pushes_rendered_content_after_persistence() {
        let source = include_str!("mod.rs");
        let implementation_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("scheduler source includes implementation before tests");
        let fact_job_source = implementation_source
            .split("pub async fn run_event_fact_brief_job")
            .nth(1)
            .and_then(|section| section.split("/// Generate daily market report").next())
            .expect("fact brief job implementation present");

        assert!(
            fact_job_source.contains(".save_daily_brief(&row)")
                && fact_job_source.contains("state.pusher.push(channel, &content).await"),
            "fact brief job must persist the brief and then push the rendered content without wrapper text"
        );
    }

    #[test]
    fn scheduler_does_not_reference_auto_trading_candidate_table() {
        let source = include_str!("mod.rs");
        let implementation_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("scheduler source includes implementation before tests");
        let forbidden_table = concat!("signal", "_strategy", "_candidates");
        assert!(!implementation_source.contains(forbidden_table));
    }

    fn event_evidence(
        source_id: &str,
        source_item_id: &str,
        version: i32,
        status: &str,
        effective_trade_date: NaiveDate,
    ) -> EventEvidenceRow {
        EventEvidenceRow {
            evidence_id: Uuid::new_v4(),
            source_id: source_id.to_string(),
            source_item_id: source_item_id.to_string(),
            source_url: Some(format!("https://example.test/{source_item_id}")),
            source_tier: "official".to_string(),
            source_terms_version: "terms-v1".to_string(),
            occurred_at: Some(dt(2026, 7, 10, 8)),
            published_at: Some(dt(2026, 7, 10, 8)),
            first_seen_at: dt(2026, 7, 10, 9),
            available_at: dt(2026, 7, 10, 8),
            effective_trade_date,
            title: format!("Title {source_item_id}"),
            content: Some(format!("Content {source_item_id}")),
            language: "und".to_string(),
            content_hash: format!("hash-{source_item_id}-{version}"),
            raw_payload: json!({"source_item_id": source_item_id, "version": version}),
            version,
            supersedes_evidence_id: None,
            status: status.to_string(),
            created_at: dt(2026, 7, 10, 9),
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    async fn seed_stock_daily_bar(
        pool: &PgPool,
        trade_date: NaiveDate,
        code: &str,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO stock_daily_bars
               (code, trade_date, open, high, low, close, volume, amount)
               VALUES ($1, $2, 10, 11, 9, 10.5, 1000, 10000)"#,
        )
        .bind(code)
        .bind(trade_date)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_scan_result(pool: &PgPool) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO scan_results (run_id, code, name, signal_id, metadata)
               VALUES ($1, '600001.SH', 'Alpha Bank', 'test_signal', '{"score":1}')"#,
        )
        .bind(Uuid::new_v4())
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_published_pattern_set(pool: &PgPool) -> sqlx::Result<()> {
        let dataset_version = format!("dataset-{}", Uuid::new_v4());
        sqlx::query(
            r#"INSERT INTO analysis_dataset_manifests
               (dataset_version, schema_version, feature_version, horizon, data_cutoff,
                available_at_cutoff, row_count, date_from, date_to, manifest, input_fingerprint)
               VALUES ($1, '1', 'feature-v1', 'week', '2026-06-30', '2026-07-01T00:00:00Z',
                       21, '2026-01-01', '2026-06-30', '{"files":["pattern-fixture.parquet"]}', 'pattern-fp')"#,
        )
        .bind(&dataset_version)
        .execute(pool)
        .await?;

        let pattern_version_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO analysis_pattern_versions
               (pattern_version_id, pattern_id, horizon, pattern_type, status,
                schema_version, feature_version, logic_version, dataset_version,
                model_payload, validation_payload, trained_from, trained_until,
                available_at_cutoff, approved_by, published_at)
               VALUES ($1, $2, 'week', 'trend', 'published',
                       '1', 'feature-v1', 'logic-v1', $3,
                       $4, $5,
                       '2026-01-01', '2026-06-30', '2026-07-01T00:00:00Z',
                       'reviewer', '2026-07-10T08:00:00Z')"#,
        )
        .bind(pattern_version_id)
        .bind(format!("pattern-{dataset_version}"))
        .bind(&dataset_version)
        .bind(pattern_model_payload())
        .bind(pattern_validation_payload())
        .execute(pool)
        .await?;

        let pattern_set_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO analysis_pattern_sets (pattern_set_id, name, status, published_at)
               VALUES ($1, 'published-set', 'published', '2026-07-10T09:00:00Z')"#,
        )
        .bind(pattern_set_id)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO analysis_pattern_set_members
               (pattern_set_id, pattern_version_id, member_order)
               VALUES ($1, $2, 1)"#,
        )
        .bind(pattern_set_id)
        .bind(pattern_version_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_pattern_market_inputs(
        pool: &PgPool,
        trade_date: NaiveDate,
        code: &str,
    ) -> sqlx::Result<()> {
        let available_at = dt(2026, 7, 10, 12);
        for offset in 0..=20 {
            let bar_date = trade_date - Duration::days(i64::from(20 - offset));
            let close = if offset == 20 {
                120.0
            } else {
                100.0 + f64::from(offset)
            };
            sqlx::query(
                r#"INSERT INTO stock_daily_bar_versions
                   (code, trade_date, open, high, low, close, volume, amount,
                    turnover, pe, pb, available_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                           1.2, 12.0, 1.4, $9, 'observed', 'test')"#,
            )
            .bind(code)
            .bind(bar_date)
            .bind(close)
            .bind(close + 1.0)
            .bind(close - 1.0)
            .bind(close)
            .bind(10_000_i64 + i64::from(offset))
            .bind(1_000_000.0 + f64::from(offset))
            .bind(available_at)
            .execute(pool)
            .await?;
            sqlx::query(
                r#"INSERT INTO stock_adjustment_factors
                   (code, trade_date, adj_factor, available_at, availability_quality, source)
                   VALUES ($1, $2, 1.0, $3, 'observed', 'test')"#,
            )
            .bind(code)
            .bind(bar_date)
            .bind(available_at)
            .execute(pool)
            .await?;
        }

        sqlx::query(
            r#"INSERT INTO security_master_versions
               (code, name, market, exchange, list_status, list_date,
                available_at, availability_quality, source)
               VALUES ($1, 'Alpha Bank', 'A', 'SH', 'L', '2020-01-01',
                       $2, 'observed', 'test')"#,
        )
        .bind(code)
        .bind(available_at)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO security_daily_status
               (code, trade_date, listed_days, is_st, is_suspended, price_limit_pct,
                available_at, availability_quality, source)
               VALUES ($1, $2, 1000, false, false, 10.0, $3, 'observed', 'test')"#,
        )
        .bind(code)
        .bind(trade_date)
        .bind(available_at)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO stock_daily_basic_versions
               (code, trade_date, turnover_rate, volume_ratio, pe, pb, ps,
                total_share, float_share, total_mv, circ_mv,
                available_at, availability_quality, source)
               VALUES ($1, $2, 1.2, 1.4, 12.0, 1.4, 2.0,
                       100000000, 80000000, 1200000000, 960000000,
                       $3, 'observed', 'test')"#,
        )
        .bind(code)
        .bind(trade_date)
        .bind(available_at)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO stock_sector_membership
               (code, sector_code, sector_name, sector_type, valid_from,
                available_at, availability_quality, source)
               VALUES ($1, 'BK001', 'Banking', 'industry', '2020-01-01',
                       $2, 'observed', 'test')"#,
        )
        .bind(code)
        .bind(available_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    fn pattern_model_payload() -> Value {
        serde_json::from_str(include_str!("../../tests/fixtures/pattern_model_v1.json")).unwrap()
    }

    fn pattern_validation_payload() -> Value {
        json!({
            "candidate_id": "trend:kmeans:k2:c0",
            "positive_sample_count": 12,
            "control_sample_count": 18,
            "effective_sample_count": 8.0,
            "base_rate": 0.40,
            "precision": 0.75,
            "lift": 2.0,
            "lift_over_base_rate": 2.0,
            "coverage": 0.27,
            "false_positive_rate": 0.11,
            "precision_at_10": 0.70,
            "precision_at_50": 0.62,
            "cost_adjusted_return": 0.032,
            "max_drawdown": -0.045,
            "turnover": 0.20,
            "yearly_results": {"2026": {"sample_count": 30, "precision": 0.75}},
            "regime_results": {"bull": {"sample_count": 18, "precision": 0.80}},
            "top_stock_contribution": 0.20,
            "top_period_contribution": 0.25,
            "mean_excess_return": 0.024,
            "median_excess_return": 0.020,
            "win_rate": 0.72,
            "profit_factor": 2.40,
            "max_losing_streak": 2,
            "capacity_estimate": 1000000.0,
            "cluster_stability": 0.86,
            "calibration_error": 0.05,
            "majority_windows_positive_lift": true,
            "baseline_comparison": {
                "best_required_baseline_return": 0.01,
                "cost_adjusted_return_delta": 0.022
            },
            "release_gate_passed": true,
            "candidate_status": "validated"
        })
    }

    async fn count_rows(pool: &PgPool, table: &str) -> sqlx::Result<i64> {
        let query = match table {
            "scan_results" => "SELECT COUNT(*) FROM scan_results",
            "analysis_shadow_candidates" => "SELECT COUNT(*) FROM analysis_shadow_candidates",
            "signal_strategy_candidates" => "SELECT COUNT(*) FROM signal_strategy_candidates",
            _ => panic!("unexpected table {table}"),
        };
        let (count,): (i64,) = sqlx::query_as(query).fetch_one(pool).await?;
        Ok(count)
    }

    async fn spawn_event_feed_server(body: &'static str) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = StdArc::new(body);
        let app = Router::new().route(
            "/feed",
            get(move || {
                let body = body.clone();
                async move {
                    (
                        [(header::CONTENT_TYPE, "application/json")],
                        body.as_ref().to_string(),
                    )
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{addr}/feed"), server)
    }

    async fn test_state(pool: PgPool) -> Arc<AppState> {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let redis_client = redis::Client::open(redis_url).unwrap();
        let redis = redis::aio::ConnectionManager::new(redis_client)
            .await
            .unwrap();
        Arc::new(AppState {
            config: Arc::new(Config {
                tushare_token: "test".to_string(),
                database_url: "postgresql://qbot:qbot@127.0.0.1/qbot".to_string(),
                redis_url: "redis://127.0.0.1:6379".to_string(),
                telegram_bot_token: "test".to_string(),
                telegram_webhook_secret: None,
                webhook_url: None,
                stock_alert_channel: None,
                report_channel: None,
                daban_channel: None,
                api_port: 8080,
                api_key: Some("test-key".to_string()),
                ai_api_key: None,
                ai_base_url: "https://api.openai.com/v1".to_string(),
                ai_model: "gpt-4o-mini".to_string(),
                data_proxy: None,
                official_event_feed_url: None,
                official_event_feed_api_key: None,
                official_event_source_id: "official:market_event".to_string(),
                official_event_store_full_content: false,
                enable_burst_monitor: false,
                enable_daban_live: false,
                enable_ai_analysis: false,
                enable_chip_dist: false,
                enable_signal_auto_trading: false,
            }),
            db: pool,
            redis,
            provider: Arc::new(FakeProvider),
            point_in_time_provider: Arc::new(FakePointInTimeProvider),
            pusher: Arc::new(TelegramPusher::new("test".to_string())),
            fetch_job_lock: Arc::new(Mutex::new(())),
            analysis_job_lock: Arc::new(Mutex::new(())),
            scan_job_lock: Arc::new(Mutex::new(())),
            daily_report_job_lock: Arc::new(Mutex::new(())),
            weekly_report_job_lock: Arc::new(Mutex::new(())),
        })
    }

    struct FakeProvider;

    #[async_trait]
    impl DataProvider for FakeProvider {
        fn name(&self) -> &'static str {
            "fake"
        }

        async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
            Ok(Vec::new())
        }

        async fn get_daily_bars_by_date(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<(String, Candle)>> {
            Ok(Vec::new())
        }

        async fn get_daily_bars_for_stock(
            &self,
            _code: &str,
            _start_date: NaiveDate,
            _end_date: NaiveDate,
        ) -> Result<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn get_trading_dates(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<NaiveDate>> {
            Ok(Vec::new())
        }

        async fn get_limit_up_stocks(&self, _trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
            Ok(Vec::new())
        }

        async fn get_index_daily(
            &self,
            _code: &str,
            _trade_date: NaiveDate,
        ) -> Result<Option<IndexData>> {
            Ok(None)
        }

        async fn get_sector_data(&self, _trade_date: NaiveDate) -> Result<Vec<SectorData>> {
            Ok(Vec::new())
        }
    }

    struct FakePointInTimeProvider;

    #[async_trait]
    impl PointInTimeDataProvider for FakePointInTimeProvider {
        async fn probe_capabilities(&self) -> Result<PointInTimeCapabilities> {
            Ok(PointInTimeCapabilities {
                security_master_history: true,
                corporate_actions: true,
                adjustment_factors: true,
                daily_basic: true,
                daily_security_status: true,
                historical_index_bars: true,
                historical_sector_membership: true,
                details: BTreeMap::new(),
            })
        }

        async fn get_security_master_versions(&self) -> Result<Vec<SecurityMasterVersion>> {
            Ok(Vec::new())
        }

        async fn get_corporate_actions(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<CorporateAction>> {
            Ok(Vec::new())
        }

        async fn get_adjustment_factors(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<AdjustmentFactor>> {
            Ok(Vec::new())
        }

        async fn get_daily_basics(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<DailyBasicSnapshot>> {
            Ok(Vec::new())
        }

        async fn get_security_statuses(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<SecurityDailyStatus>> {
            Ok(Vec::new())
        }

        async fn get_index_daily_range(
            &self,
            _codes: &[String],
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<IndexDailyBar>> {
            Ok(Vec::new())
        }

        async fn get_sector_memberships(
            &self,
            _as_of_date: NaiveDate,
        ) -> Result<Vec<SectorMembership>> {
            Ok(Vec::new())
        }
    }
}
