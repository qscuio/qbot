use chrono::{Datelike, Duration, Utc};
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, warn};

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
use crate::storage::market_repository::MarketRepository;
use crate::storage::pattern_repository::PatternRepository;
use crate::storage::postgres;
use crate::telegram::pusher::TelegramPusher;

const FETCH_JOB_CRON: &str = "0 0 17 * * Mon,Tue,Wed,Thu,Fri";
const POINT_IN_TIME_TRADE_DATE_JOB_CRON: &str = "0 10 17 * * Mon,Tue,Wed,Thu,Fri";
const MARKET_SNAPSHOT_JOB_CRON: &str = "0 20 17 * * Mon,Tue,Wed,Thu,Fri";
const SCAN_JOB_CRON: &str = "0 30 17 * * Mon,Tue,Wed,Thu,Fri";
const PATTERN_SHADOW_JOB_CRON: &str = "0 40 17 * * Mon,Tue,Wed,Thu,Fri";
const DAILY_SIGNAL_ARCHIVE_JOB_CRON: &str = "0 5 20 * * Mon,Tue,Wed,Thu,Fri";
const DAILY_REPORT_JOB_CRON: &str = "0 0 18 * * Mon,Tue,Wed,Thu,Fri";
const WEEKLY_REPORT_JOB_CRON: &str = "0 0 20 * * Fri";
const POINT_IN_TIME_REFERENCE_JOB_CRON: &str = "0 15 17 * * Fri";

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
    info!("Scheduler started with 9 jobs");
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
        DAILY_REPORT_JOB_CRON,
        WEEKLY_REPORT_JOB_CRON,
        DAILY_SIGNAL_ARCHIVE_JOB_CRON,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, NaiveDate, TimeZone};
    use serde_json::json;
    use sqlx::PgPool;
    use std::collections::BTreeMap;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use crate::analysis::market_snapshot::{
        AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, MarketSnapshot,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion, MARKET_SNAPSHOT_VERSION,
    };
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
    use crate::error::Result;
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

        run_pattern_shadow_job(state).await;

        assert_eq!(count_rows(&pool, "scan_results").await?, scan_count_before);
        assert_eq!(count_rows(&pool, "analysis_shadow_candidates").await?, 0);
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

        run_pattern_shadow_job(state).await;

        assert_eq!(count_rows(&pool, "scan_results").await?, scan_count_before);
        assert_eq!(count_rows(&pool, "analysis_shadow_candidates").await?, 0);
        Ok(())
    }

    #[test]
    fn scheduler_does_not_reference_auto_trading_candidate_table() {
        let source = include_str!("mod.rs");
        let forbidden_table = concat!("signal", "_strategy", "_candidates");
        assert!(!source.contains(forbidden_table));
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
        sqlx::query(
            r#"INSERT INTO analysis_pattern_sets (pattern_set_id, name, status, published_at)
               VALUES ($1, 'published-set', 'published', '2026-07-10T09:00:00Z')"#,
        )
        .bind(Uuid::new_v4())
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn count_rows(pool: &PgPool, table: &str) -> sqlx::Result<i64> {
        let query = match table {
            "scan_results" => "SELECT COUNT(*) FROM scan_results",
            "analysis_shadow_candidates" => "SELECT COUNT(*) FROM analysis_shadow_candidates",
            _ => panic!("unexpected table {table}"),
        };
        let (count,): (i64,) = sqlx::query_as(query).fetch_one(pool).await?;
        Ok(count)
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
