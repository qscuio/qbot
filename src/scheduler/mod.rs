use chrono::{DateTime, Datelike, Duration, Utc};
use redis::AsyncCommands;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, warn};
use uuid::Uuid;

use crate::analysis::adapters::gdelt::GdeltEventSource;
use crate::analysis::adapters::official_event_source::OfficialEventSource;
use crate::analysis::adapters::{EventSource, FetchedEvent};
use crate::analysis::decision_support::{DecisionSupport, DecisionSupportConfig};
use crate::analysis::events::claims::ClaimGraph;
use crate::analysis::events::extraction::{EventExtractionV1, StockCodeDirectory};
use crate::analysis::events::market_observation::{
    observe_market_alignment, CausalConfidenceInputs, EventImportance,
    MarketSnapshotObservationInput, ObservationEntity, ObservationWindow, ObservedReturn,
    WindowEvent,
};
use crate::analysis::events::{
    compute_event_delta, render_daily_brief, AShareTradingDateResolver, CandidateCluster,
    ClaimEntityRole, ClusterMention, ClusterVersionRef, EndOfDayRefiner, EventClaimSnapshot,
    EventClusterVersionSnapshot, EventDelta, EventIntelligence, EventMention,
    FrozenImpactHypothesis, IncrementalClusterer, IncrementalClusteringConfig,
    LockedClusterRelations, RefinedCluster, TradingDateResolver,
};
use crate::analysis::market_snapshot::{
    ingestion::PointInTimeIngestion, MarketSnapshotModule, PointInTimeContext,
    MARKET_SNAPSHOT_VERSION,
};
use crate::analysis::patterns::matcher::PatternEngine;
use crate::data::provider::DataProvider;
use crate::error::Result;
use crate::market_time::{beijing_today, beijing_tz};
use crate::services::{
    limit_up::LimitUpService, market::MarketService, market_report::MarketReportService,
    scanner::ScannerService, sector::SectorService, signal_auto_trading::SignalAutoTradingService,
    stock_history::StockHistoryService,
};
use crate::state::AppState;
use crate::storage::event_repository::{
    DailyEventBriefRow, DuplicateGroupRow, EventDeltaRow, EventEvidenceRow, EventHypothesisRow,
    EventMentionClusterLinkRow, EventMentionRow, EventRepository, ExtractionRow,
    MarketObservationRow,
};
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
const EVENT_CLUSTER_REFINEMENT_JOB_CRON: &str = "0 52 17 * * Mon,Tue,Wed,Thu,Fri";
const EVENT_MARKET_OBSERVATION_JOB_CRON: &str = "0 54 17 * * Mon,Tue,Wed,Thu,Fri";
const DECISION_SUPPORT_JOB_CRON: &str = "0 55 17 * * Mon,Tue,Wed,Thu,Fri";
const DAILY_SIGNAL_ARCHIVE_JOB_CRON: &str = "0 5 20 * * Mon,Tue,Wed,Thu,Fri";
const DAILY_REPORT_JOB_CRON: &str = "0 0 18 * * Mon,Tue,Wed,Thu,Fri";
const WEEKLY_REPORT_JOB_CRON: &str = "0 0 20 * * Fri";
const POINT_IN_TIME_REFERENCE_JOB_CRON: &str = "0 15 17 * * Fri";
const EVENT_MARKET_BENCHMARK_CODE: &str = "000001.SH";
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
    let mut sources: Vec<Box<dyn EventSource>> = Vec::new();
    match OfficialEventSource::from_config(state.config.as_ref()) {
        Ok(Some(source)) => sources.push(Box::new(source)),
        Ok(None) => {}
        Err(error) => {
            warn!("Event ingestion skipped official source config: {}", error);
        }
    }
    match GdeltEventSource::from_config(state.config.as_ref()) {
        Ok(Some(source)) => sources.push(Box::new(source)),
        Ok(None) => {}
        Err(error) => {
            warn!("Event ingestion skipped GDELT source config: {}", error);
        }
    }

    run_event_ingestion_sources(state.clone(), sources).await;
}

async fn run_event_ingestion_sources(state: Arc<AppState>, sources: Vec<Box<dyn EventSource>>) {
    if sources.is_empty() {
        info!("Event ingestion skipped: no event sources configured");
        return;
    }

    let now = Utc::now();
    let repo = EventRepository::new(state.db.clone());
    for source in sources {
        process_event_ingestion_source(state.clone(), &repo, source.as_ref(), now).await;
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

pub async fn run_event_cluster_refinement_job(state: Arc<AppState>) {
    let _guard = state.analysis_job_lock.lock().await;
    let repo = EventRepository::new(state.db.clone());
    match persist_event_cluster_refinement_outputs(&repo).await {
        Ok((delta_count, hypothesis_count)) => info!(
            "Event cluster refinement persisted {} delta rows and {} frozen hypotheses",
            delta_count, hypothesis_count
        ),
        Err(error) => warn!("Event cluster refinement failed: {}", error),
    }
}

pub async fn run_event_market_observation_job(state: Arc<AppState>) {
    let _guard = state.analysis_job_lock.lock().await;
    let repo = EventRepository::new(state.db.clone());
    let market_repo = MarketRepository::new(state.db.clone());
    match persist_event_market_observations(&repo, &market_repo).await {
        Ok(observation_count) => info!(
            "Event market observation persisted {} repository-backed observation rows",
            observation_count
        ),
        Err(error) => warn!("Event market observation failed: {}", error),
    }
}

pub async fn run_decision_support_job(state: Arc<AppState>) {
    let _guard = state.analysis_job_lock.lock().await;
    let trade_date = match resolve_decision_support_trade_date(state.as_ref()).await {
        Ok(trade_date) => trade_date,
        Err(error) => {
            warn!("DecisionSupport skipped: {}", error);
            return;
        }
    };

    let mut config = DecisionSupportConfig::from(&*state.config);
    config.persist_run = true;

    match DecisionSupport::new(state.db.clone())
        .build_daily(trade_date, config)
        .await
    {
        Ok(support) => info!(
            "DecisionSupport persisted: trade_date={}, run_id={}, candidates={}",
            trade_date,
            support.run_id,
            support.candidates.len()
        ),
        Err(error) => warn!("DecisionSupport failed for {}: {}", trade_date, error),
    }
}

async fn resolve_decision_support_trade_date(state: &AppState) -> Result<chrono::NaiveDate> {
    match postgres::latest_stock_trade_date(&state.db).await {
        Ok(Some(trade_date)) => Ok(trade_date),
        Ok(None) => {
            let market_repo = MarketRepository::new(state.db.clone());
            market_repo
                .latest_market_snapshot(MARKET_SNAPSHOT_VERSION)
                .await?
                .map(|snapshot| snapshot.trade_date)
                .ok_or_else(|| {
                    crate::error::AppError::NotFound(
                        "no market snapshot or trade date available".to_string(),
                    )
                })
        }
        Err(error) => Err(error),
    }
}

async fn persist_event_cluster_refinement_outputs(
    repo: &EventRepository,
) -> Result<(usize, usize)> {
    let _persisted_cluster_versions = persist_scheduled_event_clusters(repo).await?;
    let latest_clusters = repo.list_latest_cluster_versions().await?;
    let mut persisted_deltas = 0usize;
    let mut persisted_hypotheses = 0usize;

    for latest_cluster in latest_clusters {
        let cluster_versions = repo
            .list_cluster_versions(latest_cluster.event_cluster_id)
            .await?;
        let mut prior_hypothesis: Option<FrozenImpactHypothesis> = None;

        for cluster_version in cluster_versions {
            if let Some(existing) = repo
                .find_latest_hypothesis_for_cluster_version(
                    cluster_version.event_cluster_id,
                    cluster_version.cluster_version,
                )
                .await?
            {
                prior_hypothesis = Some(serde_json::from_value(existing.graph_payload)?);
                continue;
            }

            if cluster_version.cluster_version > 1
                && repo
                    .find_event_delta(
                        cluster_version.event_cluster_id,
                        cluster_version.cluster_version - 1,
                        cluster_version.cluster_version,
                    )
                    .await?
                    .is_none()
            {
                let Some(previous_cluster) = repo
                    .find_event_cluster_version(
                        cluster_version.event_cluster_id,
                        cluster_version.cluster_version - 1,
                    )
                    .await?
                else {
                    continue;
                };
                let Some(previous_snapshot) = cluster_snapshot_from_row(&previous_cluster)? else {
                    continue;
                };
                let Some(current_snapshot) = cluster_snapshot_from_row(&cluster_version)? else {
                    continue;
                };
                let delta = compute_event_delta(&previous_snapshot, &current_snapshot);
                repo.save_event_delta(&EventDeltaRow {
                    event_cluster_id: cluster_version.event_cluster_id,
                    from_version: cluster_version.cluster_version - 1,
                    to_version: cluster_version.cluster_version,
                    delta_payload: serde_json::to_value(&delta)?,
                    created_at: cluster_version.created_at,
                })
                .await?;
                persisted_deltas += 1;
            }

            let Some(claim_graph) = repo
                .find_latest_claim_graph_for_evidence(cluster_version.primary_evidence_id)
                .await?
                .map(|row| serde_json::from_value::<ClaimGraph>(row.graph_payload))
                .transpose()?
            else {
                continue;
            };
            let extraction = repo
                .list_latest_extractions_for_evidence_ids(&[cluster_version.primary_evidence_id])
                .await?
                .into_iter()
                .find(|row| row.evidence_id == cluster_version.primary_evidence_id);
            let Some(extraction) = extraction else {
                continue;
            };
            let claim_ids = published_claim_ids(&extraction);
            if claim_ids.is_empty() {
                continue;
            }

            let next_hypothesis = if cluster_version.cluster_version == 1 {
                FrozenImpactHypothesis::initial(
                    &claim_graph,
                    claim_ids,
                    cluster_version.created_at,
                )?
            } else {
                let Some(prior_hypothesis) = prior_hypothesis.clone() else {
                    continue;
                };
                let Some(delta_row) = repo
                    .find_event_delta(
                        cluster_version.event_cluster_id,
                        cluster_version.cluster_version - 1,
                        cluster_version.cluster_version,
                    )
                    .await?
                else {
                    continue;
                };
                let delta: EventDelta = serde_json::from_value(delta_row.delta_payload)?;
                prior_hypothesis.evolve(&claim_graph, &delta, cluster_version.created_at)?
            };

            repo.save_frozen_hypothesis(&EventHypothesisRow {
                hypothesis_id: next_hypothesis.hypothesis_id(),
                event_cluster_id: cluster_version.event_cluster_id,
                cluster_version: cluster_version.cluster_version,
                hypothesis_version: next_hypothesis.hypothesis_version(),
                schema_version: next_hypothesis.graph().schema_version.clone(),
                graph_payload: serde_json::to_value(&next_hypothesis)?,
                frozen_at: next_hypothesis.graph().frozen_at,
                based_on_claim_ids: next_hypothesis.graph().based_on_claim_ids.clone(),
                review_status: "frozen".to_string(),
                supersedes_id: next_hypothesis.supersedes_hypothesis_id(),
                created_at: cluster_version.created_at,
            })
            .await?;
            prior_hypothesis = Some(next_hypothesis);
            persisted_hypotheses += 1;
        }
    }

    Ok((persisted_deltas, persisted_hypotheses))
}

async fn persist_event_market_observations(
    repo: &EventRepository,
    market_repo: &MarketRepository,
) -> Result<usize> {
    let hypotheses = repo.list_latest_hypotheses().await?;
    let latest_publishable = repo.list_latest_publishable_evidence().await?;
    let publishable_ids = latest_publishable
        .iter()
        .map(|row| row.evidence_id)
        .collect::<Vec<_>>();
    let extraction_by_evidence = repo
        .list_latest_extractions_for_evidence_ids(&publishable_ids)
        .await?
        .into_iter()
        .map(|row| (row.evidence_id, row))
        .collect::<BTreeMap<_, _>>();
    let cluster_links_by_evidence = repo
        .list_latest_cluster_links_for_evidence_ids(&publishable_ids)
        .await?
        .into_iter()
        .map(|row| (row.evidence_id, row))
        .collect::<BTreeMap<_, _>>();
    let latest_clusters_by_id = repo
        .list_latest_cluster_versions()
        .await?
        .into_iter()
        .map(|row| (row.event_cluster_id, row))
        .collect::<BTreeMap<_, _>>();
    let mut persisted = 0usize;

    for hypothesis_row in hypotheses {
        let hypothesis: FrozenImpactHypothesis =
            serde_json::from_value(hypothesis_row.graph_payload.clone())?;
        let trade_date = hypothesis
            .graph()
            .frozen_at
            .date_naive()
            .succ_opt()
            .unwrap_or_else(|| hypothesis.graph().frozen_at.date_naive());
        let window = ObservationWindow {
            label: "t+1".to_string(),
            expires_on: trade_date.succ_opt().unwrap_or(trade_date),
        };
        let context = PointInTimeContext {
            trade_date,
            as_of: trade_date.and_hms_opt(17, 0, 0).unwrap().and_utc(),
        };
        let existing = repo
            .list_market_observations_for_hypothesis(hypothesis_row.hypothesis_id)
            .await?;

        for entity in direct_observation_entities(&hypothesis) {
            if existing.iter().any(|row| {
                row.entity_type == entity.entity_type
                    && row.entity_id == entity.entity_id
                    && row.trade_date == trade_date
            }) {
                continue;
            }

            let Some(observed_inputs) =
                load_market_observation_inputs(market_repo, &entity, &context).await?
            else {
                info!(
                    "Event market observation skipped: no eligible PIT market inputs for hypothesis={} entity={} trade_date={}",
                    hypothesis_row.hypothesis_id,
                    entity.entity_id,
                    trade_date
                );
                continue;
            };
            let related_events = related_window_events_for_entity(
                &latest_publishable,
                &extraction_by_evidence,
                &cluster_links_by_evidence,
                &latest_clusters_by_id,
                &hypothesis_row,
                &entity,
                &window,
                &context,
            )?;

            let observation = observe_market_alignment(&MarketSnapshotObservationInput {
                context,
                hypothesis: hypothesis.clone(),
                entity: entity.clone(),
                window: window.clone(),
                stock_return: Some(observed_inputs.stock_return),
                market_return: Some(observed_inputs.market_return),
                industry_return: Some(observed_inputs.industry_return),
                snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                benchmark_id: observed_inputs.benchmark_id,
                industry_benchmark_id: observed_inputs.industry_benchmark_id,
                causal_inputs: CausalConfidenceInputs {
                    evidence_strength: 0.0,
                    timing_quality: 0.0,
                    identification_quality: 0.0,
                },
                related_events,
                observed_at: context.as_of,
            })?;

            repo.save_market_observation(&MarketObservationRow {
                hypothesis_id: observation.hypothesis_id,
                entity_type: observation.entity_type,
                entity_id: observation.entity_id,
                trade_date: observation.trade_date,
                observation_status: serde_json::to_value(observation.observation_status)?
                    .as_str()
                    .expect("market observation status serializes to string")
                    .to_string(),
                market_alignment_score: observation.market_alignment_score,
                causal_confidence: observation.causal_confidence,
                abnormal_market_return: observation.abnormal_market_return,
                abnormal_industry_return: observation.abnormal_industry_return,
                market_metrics: serde_json::to_value(&observation.market_metrics)?,
                confounding_events: serde_json::to_value(&observation.confounding_events)?,
                created_at: observation.created_at,
            })
            .await?;
            persisted += 1;
        }
    }

    Ok(persisted)
}

#[derive(Debug, Clone)]
struct PreparedEventMention {
    mention: EventMention,
    evidence: EventEvidenceRow,
    extraction: Option<ExtractionRow>,
    existing_cluster_link: Option<EventMentionClusterLinkRow>,
    duplicate_group_id: Option<Uuid>,
}

async fn persist_scheduled_event_clusters(repo: &EventRepository) -> Result<usize> {
    let evidence_rows = repo.list_latest_publishable_evidence().await?;
    if evidence_rows.is_empty() {
        return Ok(0);
    }

    let evidence_ids = evidence_rows
        .iter()
        .map(|row| row.evidence_id)
        .collect::<Vec<_>>();
    let extraction_by_evidence = repo
        .list_latest_extractions_for_evidence_ids(&evidence_ids)
        .await?
        .into_iter()
        .map(|row| (row.evidence_id, row))
        .collect::<BTreeMap<_, _>>();
    let cluster_links_by_evidence = repo
        .list_latest_cluster_links_for_evidence_ids(&evidence_ids)
        .await?
        .into_iter()
        .map(|row| (row.evidence_id, row))
        .collect::<BTreeMap<_, _>>();
    let duplicate_groups = repo
        .list_duplicate_groups_for_evidence_ids(&evidence_ids)
        .await?;
    let duplicate_group_by_evidence = duplicate_group_membership_map(&duplicate_groups);

    let mut prepared_mentions = Vec::new();
    for evidence in evidence_rows {
        let extraction = extraction_by_evidence.get(&evidence.evidence_id).cloned();
        let Some(mention) = build_event_mention(&evidence, extraction.as_ref())? else {
            continue;
        };
        prepared_mentions.push(PreparedEventMention {
            mention,
            evidence: evidence.clone(),
            extraction,
            existing_cluster_link: cluster_links_by_evidence
                .get(&evidence.evidence_id)
                .cloned(),
            duplicate_group_id: duplicate_group_by_evidence
                .get(&evidence.evidence_id)
                .copied(),
        });
    }

    if prepared_mentions.is_empty() {
        return Ok(0);
    }

    let seeded_clusters = seeded_candidate_clusters(&prepared_mentions);
    let config = IncrementalClusteringConfig::default();
    let mut clusterer = IncrementalClusterer::with_clusters(config.clone(), seeded_clusters);
    for prepared in prepared_mentions
        .iter()
        .filter(|item| item.existing_cluster_link.is_none())
    {
        clusterer.ingest_mention(prepared.mention.clone(), prepared.duplicate_group_id);
    }

    let locked_relations =
        locked_cluster_relations_from_duplicate_groups(clusterer.clusters(), &duplicate_groups);
    let refined_clusters =
        EndOfDayRefiner::new(config).refine(clusterer.clusters(), &locked_relations);
    let prepared_by_evidence = prepared_mentions
        .into_iter()
        .map(|prepared| (prepared.evidence.evidence_id, prepared))
        .collect::<BTreeMap<_, _>>();

    let mut persisted = 0usize;
    for cluster in refined_clusters {
        let Some(row) = build_cluster_row(&cluster, &prepared_by_evidence)? else {
            continue;
        };
        let should_persist = match repo.latest_cluster_version(row.event_cluster_id).await? {
            Some(existing) => !cluster_rows_equivalent(&existing, &row),
            None => true,
        };
        if !should_persist {
            continue;
        }

        let mut mention_rows = Vec::with_capacity(cluster.mentions.len());
        for cluster_mention in &cluster.mentions {
            let Some(prepared) = prepared_by_evidence.get(&cluster_mention.mention.evidence_id)
            else {
                continue;
            };
            mention_rows.push(EventMentionRow {
                mention_id: Uuid::new_v4(),
                evidence_id: prepared.evidence.evidence_id,
                event_cluster_id: Some(row.event_cluster_id),
                cluster_version: Some(row.cluster_version),
                mention_time: cluster_mention
                    .mention
                    .event_time
                    .unwrap_or(prepared.evidence.available_at),
                adds_new_fact: cluster_mention.mention.adds_new_fact,
                source_independence: cluster_mention.mention.source_independence,
                mention_payload: mention_payload(
                    &cluster_mention.mention,
                    prepared.extraction.as_ref(),
                )?,
                created_at: row.created_at,
            });
        }
        repo.save_event_cluster_version_with_mentions(&row, &mention_rows)
            .await?;
        persisted += 1;
    }

    Ok(persisted)
}

fn build_event_mention(
    evidence: &EventEvidenceRow,
    extraction: Option<&ExtractionRow>,
) -> Result<Option<EventMention>> {
    let Some(payload) = extraction
        .map(|row| serde_json::from_value::<EventExtractionV1>(row.extracted_payload.clone()))
        .transpose()?
    else {
        return Ok(None);
    };

    let entity_ids = mention_entity_ids(&payload);
    let action_tokens = mention_action_tokens(&payload, evidence);
    if entity_ids.is_empty() || action_tokens.is_empty() {
        return Ok(None);
    }

    let location_tokens = payload
        .entities
        .iter()
        .filter(|entity| entity.entity_type == "location")
        .map(|entity| canonical_token(&entity.text))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let semantic_input = format!(
        "{} {} {}",
        evidence.title,
        evidence.content.clone().unwrap_or_default(),
        payload
            .claims
            .iter()
            .map(|claim| claim.text.as_str())
            .collect::<Vec<_>>()
            .join(" ")
    );

    Ok(Some(EventMention {
        mention_id: Uuid::new_v4(),
        evidence_id: evidence.evidence_id,
        event_time: mention_time_from_payload(evidence, &payload),
        entity_ids,
        action_tokens,
        location_tokens,
        semantic_vector: semantic_vector(&semantic_input),
        adds_new_fact: payload
            .claims
            .iter()
            .any(|claim| claim.claim_type == crate::analysis::events::extraction::ClaimType::Fact),
        source_independence: 1.0,
    }))
}

fn mention_time_from_payload(
    evidence: &EventEvidenceRow,
    payload: &EventExtractionV1,
) -> Option<DateTime<Utc>> {
    evidence.occurred_at.or(evidence.published_at).or_else(|| {
        payload
            .dates
            .first()
            .and_then(|value| parse_extracted_date(&value.value))
    })
}

fn parse_extracted_date(value: &str) -> Option<DateTime<Utc>> {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|value| value.and_utc())
}

fn mention_entity_ids(payload: &EventExtractionV1) -> Vec<String> {
    let mut entity_ids = payload
        .entities
        .iter()
        .filter(|entity| entity.role == "subject")
        .filter_map(|entity| {
            entity
                .stock_code
                .clone()
                .or_else(|| (!entity.text.trim().is_empty()).then(|| entity.text.clone()))
        })
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    entity_ids.sort();
    entity_ids.dedup();
    entity_ids
}

fn mention_action_tokens(payload: &EventExtractionV1, evidence: &EventEvidenceRow) -> Vec<String> {
    let mut tokens = Vec::new();
    tokens.extend(tokenize_for_event_terms(&payload.event_type));
    if let Some(subtype) = &payload.event_subtype {
        tokens.extend(tokenize_for_event_terms(subtype));
    }
    for claim in &payload.claims {
        tokens.extend(tokenize_for_event_terms(&claim.text));
    }
    tokens.extend(tokenize_for_event_terms(&evidence.title));
    if let Some(content) = &evidence.content {
        tokens.extend(tokenize_for_event_terms(content));
    }
    tokens.retain(|token| !is_stock_code_token(token));
    tokens.sort();
    tokens.dedup();
    tokens
}

fn tokenize_for_event_terms(value: &str) -> Vec<String> {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '.')
        .map(canonical_token)
        .filter(|token| token.len() > 2)
        .collect()
}

fn canonical_token(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn is_stock_code_token(value: &str) -> bool {
    value.len() == 9
        && value.as_bytes().get(6) == Some(&b'.')
        && value[..6].chars().all(|ch| ch.is_ascii_digit())
        && matches!(&value[7..], "sh" | "sz")
}

fn semantic_vector(value: &str) -> Vec<f32> {
    let mut buckets = [0.0_f32; 8];
    for token in tokenize_for_event_terms(value) {
        let bucket = token
            .bytes()
            .fold(0_usize, |acc, byte| acc.wrapping_add(byte as usize))
            % buckets.len();
        buckets[bucket] += 1.0;
    }
    let magnitude = buckets
        .iter()
        .map(|value| value * value)
        .sum::<f32>()
        .sqrt();
    if magnitude > 0.0 {
        for bucket in &mut buckets {
            *bucket /= magnitude;
        }
    }
    buckets.to_vec()
}

fn duplicate_group_membership_map(groups: &[DuplicateGroupRow]) -> BTreeMap<Uuid, Uuid> {
    let mut memberships = BTreeMap::new();
    for group in groups {
        for member in &group.members {
            memberships.insert(member.evidence_id, group.duplicate_group_id);
        }
    }
    memberships
}

fn seeded_candidate_clusters(prepared_mentions: &[PreparedEventMention]) -> Vec<CandidateCluster> {
    let mut grouped = BTreeMap::<(Uuid, i32), Vec<&PreparedEventMention>>::new();
    for prepared in prepared_mentions {
        let Some(link) = &prepared.existing_cluster_link else {
            continue;
        };
        grouped
            .entry((link.event_cluster_id, link.cluster_version))
            .or_default()
            .push(prepared);
    }

    grouped
        .into_iter()
        .map(
            |((event_cluster_id, cluster_version), members)| CandidateCluster {
                event_cluster_id,
                cluster_version: Some(cluster_version),
                input_cluster_versions: vec![ClusterVersionRef {
                    event_cluster_id,
                    cluster_version: Some(cluster_version),
                }],
                mentions: members
                    .into_iter()
                    .map(|prepared| ClusterMention {
                        origin_cluster_id: event_cluster_id,
                        duplicate_group_id: prepared.duplicate_group_id,
                        mention: prepared.mention.clone(),
                    })
                    .collect(),
                review_required: false,
            },
        )
        .collect()
}

fn locked_cluster_relations_from_duplicate_groups(
    clusters: &[CandidateCluster],
    groups: &[DuplicateGroupRow],
) -> LockedClusterRelations {
    let evidence_to_origin = clusters
        .iter()
        .flat_map(|cluster| {
            cluster
                .mentions
                .iter()
                .map(|mention| (mention.mention.evidence_id, mention.origin_cluster_id))
        })
        .collect::<BTreeMap<_, _>>();
    let mut relations = LockedClusterRelations::default();

    for group in groups.iter().filter(|group| group.locked_by_user) {
        let mut origin_ids = group
            .members
            .iter()
            .filter_map(|member| evidence_to_origin.get(&member.evidence_id).copied())
            .collect::<Vec<_>>();
        origin_ids.sort();
        origin_ids.dedup();
        for left in 0..origin_ids.len() {
            for right in (left + 1)..origin_ids.len() {
                let pair = ordered_cluster_pair(origin_ids[left], origin_ids[right]);
                match group.relation_type.as_str() {
                    "exact" | "near" => {
                        relations.merge_pairs.insert(pair);
                    }
                    "independent" => {
                        relations.split_pairs.insert(pair);
                    }
                    _ => {}
                }
            }
        }
    }

    relations
}

fn ordered_cluster_pair(left: Uuid, right: Uuid) -> (Uuid, Uuid) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn build_cluster_row(
    cluster: &RefinedCluster,
    prepared_by_evidence: &BTreeMap<Uuid, PreparedEventMention>,
) -> Result<Option<crate::storage::event_repository::EventClusterRow>> {
    let mut mentions = cluster
        .mentions
        .iter()
        .filter_map(|mention| {
            prepared_by_evidence
                .get(&mention.mention.evidence_id)
                .map(|prepared| (mention, prepared))
        })
        .collect::<Vec<_>>();
    if mentions.is_empty() {
        return Ok(None);
    }

    mentions.sort_by_key(|(_, prepared)| {
        (
            prepared.evidence.available_at,
            prepared.evidence.first_seen_at,
            prepared.evidence.evidence_id,
        )
    });
    let representative = mentions
        .iter()
        .find(|(_, prepared)| prepared.evidence.evidence_id == cluster.representative_evidence_id)
        .copied()
        .unwrap_or(mentions[0]);
    let representative_title = representative.1.evidence.title.clone();
    let representative_entities = representative.1.extraction.as_ref().and_then(|row| {
        serde_json::from_value::<EventExtractionV1>(row.extracted_payload.clone()).ok()
    });
    let event_type = representative_entities
        .as_ref()
        .map(|payload| payload.event_type.clone())
        .unwrap_or_else(|| "issuer_disclosure".to_string());
    let event_subtype = representative_entities
        .as_ref()
        .and_then(|payload| payload.event_subtype.clone());
    let snapshot = EventClusterVersionSnapshot {
        event_cluster_id: cluster.event_cluster_id,
        cluster_version: cluster.cluster_version,
        lifecycle_status: "active".to_string(),
        claims: cluster_claim_snapshots(&mentions)?,
        expectation: None,
        uncertainties: cluster_uncertainties(&mentions)?,
    };

    let representative_ids = mentions
        .iter()
        .map(|(_, prepared)| prepared.evidence.evidence_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let first_seen_at = mentions
        .iter()
        .map(|(_, prepared)| prepared.evidence.first_seen_at)
        .min()
        .unwrap_or(representative.1.evidence.first_seen_at);
    let last_seen_at = mentions
        .iter()
        .map(|(_, prepared)| prepared.evidence.available_at)
        .max()
        .unwrap_or(representative.1.evidence.available_at);
    let created_at = last_seen_at;
    let event_time = mentions
        .iter()
        .filter_map(|(mention, _)| mention.mention.event_time)
        .min();

    Ok(Some(crate::storage::event_repository::EventClusterRow {
        event_cluster_id: cluster.event_cluster_id,
        cluster_version: cluster.cluster_version,
        canonical_title: representative_title,
        event_time,
        first_seen_at,
        last_seen_at,
        lifecycle_status: "active".to_string(),
        primary_evidence_id: representative.1.evidence.evidence_id,
        representative_ids,
        source_entropy: cluster.source_entropy,
        independent_sources: cluster.independent_sources as i32,
        mention_count: cluster.mentions.len() as i32,
        cluster_payload: json!({
            "eventType": event_type,
            "eventSubtype": event_subtype,
            "deltaSnapshot": snapshot
        }),
        supersedes_version: cluster.supersedes_version,
        created_at,
    }))
}

fn cluster_claim_snapshots(
    mentions: &[(&ClusterMention, &PreparedEventMention)],
) -> Result<Vec<EventClaimSnapshot>> {
    let mut snapshots = Vec::new();
    for (_, prepared) in mentions {
        let Some(extraction) = &prepared.extraction else {
            continue;
        };
        let payload: EventExtractionV1 =
            serde_json::from_value(extraction.extracted_payload.clone())?;
        let entity_roles = payload
            .entities
            .iter()
            .map(|entity| ClaimEntityRole {
                entity_id: entity
                    .stock_code
                    .clone()
                    .unwrap_or_else(|| entity.text.clone()),
                role: entity.role.clone(),
            })
            .collect::<Vec<_>>();
        let claim_date =
            mention_time_from_payload(&prepared.evidence, &payload).map(|value| value.date_naive());
        for claim in extraction
            .claims
            .iter()
            .filter(|claim| claim.review_status == "published")
        {
            snapshots.push(EventClaimSnapshot {
                claim_id: claim.claim_id,
                canonical_claim_id: canonical_claim_id(&claim.claim_text),
                status: Some(claim.review_status.clone()),
                value: None,
                entity_roles: entity_roles.clone(),
                claim_date,
            });
        }
    }
    snapshots.sort_by_key(|snapshot| snapshot.claim_id);
    Ok(snapshots)
}

fn cluster_uncertainties(
    mentions: &[(&ClusterMention, &PreparedEventMention)],
) -> Result<Vec<String>> {
    let mut uncertainties = BTreeSet::new();
    for (_, prepared) in mentions {
        let Some(extraction) = &prepared.extraction else {
            continue;
        };
        let payload: EventExtractionV1 =
            serde_json::from_value(extraction.extracted_payload.clone())?;
        for uncertainty in payload.uncertainties {
            if !uncertainty.trim().is_empty() {
                uncertainties.insert(uncertainty);
            }
        }
    }
    Ok(uncertainties.into_iter().collect())
}

fn canonical_claim_id(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn cluster_rows_equivalent(
    existing: &crate::storage::event_repository::EventClusterRow,
    next: &crate::storage::event_repository::EventClusterRow,
) -> bool {
    existing.canonical_title == next.canonical_title
        && existing.event_time == next.event_time
        && existing.lifecycle_status == next.lifecycle_status
        && existing.primary_evidence_id == next.primary_evidence_id
        && existing.representative_ids == next.representative_ids
        && existing.source_entropy == next.source_entropy
        && existing.independent_sources == next.independent_sources
        && existing.mention_count == next.mention_count
        && existing.cluster_payload == next.cluster_payload
}

fn mention_payload(mention: &EventMention, extraction: Option<&ExtractionRow>) -> Result<Value> {
    let extraction_payload = extraction
        .map(|row| row.extracted_payload.clone())
        .unwrap_or_else(|| json!({}));
    Ok(json!({
        "entityIds": mention.entity_ids.clone(),
        "actionTokens": mention.action_tokens.clone(),
        "locationTokens": mention.location_tokens.clone(),
        "semanticVector": mention.semantic_vector.clone(),
        "extractedEvent": extraction_payload
    }))
}

fn related_window_events_for_entity(
    evidence_rows: &[EventEvidenceRow],
    extraction_by_evidence: &BTreeMap<Uuid, ExtractionRow>,
    cluster_links_by_evidence: &BTreeMap<Uuid, EventMentionClusterLinkRow>,
    latest_clusters_by_id: &BTreeMap<Uuid, crate::storage::event_repository::EventClusterRow>,
    hypothesis_row: &EventHypothesisRow,
    entity: &ObservationEntity,
    window: &ObservationWindow,
    context: &PointInTimeContext,
) -> Result<Vec<WindowEvent>> {
    let current_primary_evidence = latest_clusters_by_id
        .get(&hypothesis_row.event_cluster_id)
        .map(|row| row.primary_evidence_id);
    let hypothesis_trade_date = hypothesis_row
        .frozen_at
        .date_naive()
        .succ_opt()
        .unwrap_or_else(|| hypothesis_row.frozen_at.date_naive());
    let mut related_events = Vec::new();

    for evidence in evidence_rows {
        if current_primary_evidence == Some(evidence.evidence_id) {
            continue;
        }
        if evidence
            .effective_trade_date
            .succ_opt()
            .unwrap_or(evidence.effective_trade_date)
            != hypothesis_trade_date
        {
            continue;
        }
        if let Some(link) = cluster_links_by_evidence.get(&evidence.evidence_id) {
            if link.event_cluster_id == hypothesis_row.event_cluster_id {
                continue;
            }
        }
        let Some(extraction) = extraction_by_evidence.get(&evidence.evidence_id) else {
            continue;
        };
        let payload: EventExtractionV1 =
            serde_json::from_value(extraction.extracted_payload.clone())?;
        if !payload.entities.iter().any(|extracted| {
            extracted.role == "subject"
                && extracted
                    .stock_code
                    .as_deref()
                    .map(|stock_code| stock_code == entity.entity_id)
                    .unwrap_or(false)
        }) {
            continue;
        }
        let available_at = evidence.available_at;
        if !context.can_use(available_at) {
            continue;
        }

        related_events.push(WindowEvent {
            entity_id: entity.entity_id.clone(),
            window_label: window.label.clone(),
            event_type: payload.event_type.clone(),
            importance: confounder_event_importance(&payload, evidence),
            available_at,
        });
    }

    Ok(related_events)
}

fn confounder_event_importance(
    payload: &EventExtractionV1,
    evidence: &EventEvidenceRow,
) -> EventImportance {
    match payload.event_type.as_str() {
        "earnings"
        | "trading_suspension"
        | "trading_resumption"
        | "suspension"
        | "resumption"
        | "regulatory_penalty"
        | "major_corporate_action" => EventImportance::Medium,
        _ if payload
            .event_subtype
            .as_deref()
            .map(|value| value.contains("major"))
            .unwrap_or(false)
            || evidence.title.to_ascii_lowercase().contains("major") =>
        {
            EventImportance::High
        }
        _ => EventImportance::Low,
    }
}

struct LoadedMarketObservationInputs {
    stock_return: ObservedReturn,
    market_return: ObservedReturn,
    industry_return: ObservedReturn,
    benchmark_id: String,
    industry_benchmark_id: String,
}

async fn process_event_ingestion_source(
    state: Arc<AppState>,
    repo: &EventRepository,
    source: &dyn EventSource,
    now: chrono::DateTime<Utc>,
) {
    let cursor_key = event_cursor_key(source.source_id());
    let current_cursor = match event_cursor_get(state.redis.clone(), &cursor_key).await {
        Ok(cursor) => cursor,
        Err(error) => {
            warn!(
                "Event ingestion skipped for source={}: cursor read failed: {}",
                source.source_id(),
                error
            );
            return;
        }
    };

    let batch = match source.fetch(current_cursor.clone(), now).await {
        Ok(batch) => batch,
        Err(error) => {
            warn!(
                "Event ingestion fetch failed for source={}: {}",
                source.source_id(),
                error
            );
            return;
        }
    };

    let mut ingest_failed = false;
    for item in &batch.items {
        if let Err(error) = ingest_fetched_event(repo, source, item, now).await {
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
                warn!(
                    "Event ingestion cursor write failed for source={}: {}",
                    source.source_id(),
                    error
                );
            }
        }
    } else {
        warn!(
            "Event ingestion cursor preserved for source={} because at least one item failed",
            source.source_id()
        );
    }
}

async fn load_market_observation_inputs(
    market_repo: &MarketRepository,
    entity: &ObservationEntity,
    context: &PointInTimeContext,
) -> Result<Option<LoadedMarketObservationInputs>> {
    let Some(stock_return) = load_stock_return(
        market_repo,
        &entity.entity_id,
        context.trade_date,
        context.as_of,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(market_return) =
        load_market_index_return(market_repo, context.trade_date, context.as_of).await?
    else {
        return Ok(None);
    };
    let Some((industry_benchmark_id, industry_return)) = load_industry_return(
        market_repo,
        &entity.entity_id,
        context.trade_date,
        context.as_of,
    )
    .await?
    else {
        return Ok(None);
    };

    Ok(Some(LoadedMarketObservationInputs {
        stock_return,
        market_return,
        industry_return,
        benchmark_id: EVENT_MARKET_BENCHMARK_CODE.to_string(),
        industry_benchmark_id,
    }))
}

async fn load_stock_return(
    market_repo: &MarketRepository,
    code: &str,
    trade_date: chrono::NaiveDate,
    as_of: chrono::DateTime<Utc>,
) -> Result<Option<ObservedReturn>> {
    let bars = market_repo
        .daily_bar_history_for_code_as_of(code, trade_date, as_of, 2)
        .await?;
    if bars.len() < 2
        || bars
            .last()
            .map(|row| row.trade_date != trade_date)
            .unwrap_or(true)
    {
        return Ok(None);
    }
    if bars.iter().any(|row| row.bar.is_none()) {
        return Ok(None);
    }

    let bar_dates = bars.iter().map(|row| row.trade_date).collect::<Vec<_>>();
    let factors = market_repo
        .adjustment_factors_as_of(&[code.to_string()], bar_dates[0], trade_date, as_of)
        .await?;
    if factors.len() < bar_dates.len()
        || bar_dates
            .iter()
            .any(|date| !factors.iter().any(|factor| factor.trade_date == *date))
    {
        return Ok(None);
    }

    let adjusted = crate::analysis::market_snapshot::adjustment::adjust_candles(
        &bars
            .iter()
            .filter_map(|row| row.bar.clone())
            .collect::<Vec<_>>(),
        &factors,
    )?;
    let previous_close = adjusted[adjusted.len() - 2].close;
    let current_close = adjusted[adjusted.len() - 1].close;
    if previous_close == 0.0 {
        return Ok(None);
    }

    let available_at = bars
        .iter()
        .map(|row| row.available_at)
        .chain(factors.iter().map(|factor| factor.available_at))
        .max()
        .expect("stock returns require at least one available_at");

    Ok(Some(ObservedReturn {
        value: round_metric((current_close / previous_close) - 1.0),
        available_at,
        source: bars
            .last()
            .map(|row| row.source.clone())
            .unwrap_or_else(|| "unknown".to_string()),
    }))
}

async fn load_market_index_return(
    market_repo: &MarketRepository,
    trade_date: chrono::NaiveDate,
    as_of: chrono::DateTime<Utc>,
) -> Result<Option<ObservedReturn>> {
    let bars = market_repo
        .index_history(EVENT_MARKET_BENCHMARK_CODE, trade_date, as_of, 2)
        .await?;
    let Some(current) = bars.first() else {
        return Ok(None);
    };
    if current.trade_date != trade_date {
        return Ok(None);
    }

    let value = match current.change_pct {
        Some(change_pct) => change_pct / 100.0,
        None => {
            if bars.len() < 2 || bars[1].close == 0.0 {
                return Ok(None);
            }
            (current.close / bars[1].close) - 1.0
        }
    };

    Ok(Some(ObservedReturn {
        value: round_metric(value),
        available_at: current.available_at,
        source: current.source.clone(),
    }))
}

async fn load_industry_return(
    market_repo: &MarketRepository,
    code: &str,
    trade_date: chrono::NaiveDate,
    as_of: chrono::DateTime<Utc>,
) -> Result<Option<(String, ObservedReturn)>> {
    let Some(industry_membership) = market_repo
        .active_sector_memberships(code, trade_date, as_of)
        .await?
        .into_iter()
        .find(|membership| membership.sector_type == "industry")
    else {
        return Ok(None);
    };

    let Some(sector_row) = market_repo
        .sector_version_as_of(&industry_membership.sector_code, trade_date, as_of)
        .await?
    else {
        return Ok(None);
    };
    let Some(change_pct) = sector_row.change_pct else {
        return Ok(None);
    };

    Ok(Some((
        industry_membership.sector_code,
        ObservedReturn {
            value: round_metric(change_pct / 100.0),
            available_at: std::cmp::max(industry_membership.available_at, sector_row.available_at),
            source: sector_row.source,
        },
    )))
}

fn cluster_snapshot_from_row(
    row: &crate::storage::event_repository::EventClusterRow,
) -> Result<Option<EventClusterVersionSnapshot>> {
    match row.cluster_payload.get("deltaSnapshot") {
        Some(snapshot) => Ok(Some(serde_json::from_value(snapshot.clone())?)),
        None => {
            if row.cluster_payload.get("claims").is_some() {
                Ok(Some(serde_json::from_value(row.cluster_payload.clone())?))
            } else {
                Ok(None)
            }
        }
    }
}

fn published_claim_ids(
    extraction: &crate::storage::event_repository::ExtractionRow,
) -> Vec<uuid::Uuid> {
    let mut claim_ids = extraction
        .claims
        .iter()
        .filter(|claim| claim.review_status == "published")
        .map(|claim| claim.claim_id)
        .collect::<Vec<_>>();
    claim_ids.sort_unstable();
    claim_ids.dedup();
    claim_ids
}

fn direct_observation_entities(hypothesis: &FrozenImpactHypothesis) -> Vec<ObservationEntity> {
    let mut entities = Vec::new();

    for entity in &hypothesis.graph().direct_observation_entities {
        if entity.entity_type != "company" {
            continue;
        }
        if entities
            .iter()
            .any(|candidate: &ObservationEntity| candidate.entity_id == entity.entity_id)
        {
            continue;
        }
        entities.push(ObservationEntity {
            entity_type: entity.entity_type.clone(),
            entity_id: entity.entity_id.clone(),
        });
    }

    entities
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

    // 17:52 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                EVENT_CLUSTER_REFINEMENT_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_event_cluster_refinement_job(s).await })
                },
            )?)
            .await?;
    }

    // 17:54 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                EVENT_MARKET_OBSERVATION_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_event_market_observation_job(s).await })
                },
            )?)
            .await?;
    }

    // 17:55 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                DECISION_SUPPORT_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_decision_support_job(s).await })
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
    info!("Scheduler started with 14 jobs");
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
        EVENT_CLUSTER_REFINEMENT_JOB_CRON,
        EVENT_MARKET_OBSERVATION_JOB_CRON,
        DECISION_SUPPORT_JOB_CRON,
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
    source: &dyn EventSource,
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
            source_tier: source_tier_for(source.source_id()).to_string(),
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
        source_tier: source_tier_for(source.source_id()).to_string(),
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

fn source_tier_for(source_id: &str) -> &'static str {
    if source_id.starts_with("gdelt:") {
        "supplement"
    } else {
        "official"
    }
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

fn round_metric(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::scan_ranker::POOL_SHORT_A_ID;
    use crate::storage::decision_support_repository::DecisionSupportRepository;
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

    use crate::analysis::events::claims::{ClaimGraph, ClaimNode};
    use crate::analysis::events::{
        ClaimEntityRole, EventClaimSnapshot, EventClusterVersionSnapshot, FrozenImpactHypothesis,
    };
    use crate::analysis::market_snapshot::{
        AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, MarketSnapshot,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion, MARKET_SNAPSHOT_VERSION,
    };
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
    use crate::error::Result;
    use crate::storage::event_repository::{
        ClaimEvidenceRow, ClaimGraphRow, ClaimRow, DailyEventBriefRow, DuplicateGroupMemberRow,
        DuplicateGroupRow, EventClusterRow, EventHypothesisRow, EventRepository, ExtractionRow,
    };
    use crate::storage::market_repository::MarketRepository;
    use crate::storage::pattern_repository::{PatternRepository, ShadowCandidateRow};
    use crate::storage::postgres::{save_daily_signal_scan_results, DailySignalScanRow};

    static EVENT_INGESTION_CURSOR_TEST_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

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
        assert_eq!(
            EVENT_CLUSTER_REFINEMENT_JOB_CRON,
            "0 52 17 * * Mon,Tue,Wed,Thu,Fri"
        );
        assert_eq!(
            EVENT_MARKET_OBSERVATION_JOB_CRON,
            "0 54 17 * * Mon,Tue,Wed,Thu,Fri"
        );
    }

    #[test]
    fn decision_support_job_runs_after_event_observation_and_before_daily_report() {
        assert_eq!(DECISION_SUPPORT_JOB_CRON, "0 55 17 * * Mon,Tue,Wed,Thu,Fri");
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
                "0 52 17 * * Mon,Tue,Wed,Thu,Fri",
                "0 54 17 * * Mon,Tue,Wed,Thu,Fri",
                "0 55 17 * * Mon,Tue,Wed,Thu,Fri",
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
        let _guard = EVENT_INGESTION_CURSOR_TEST_LOCK.lock().await;
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

    #[sqlx::test(migrations = "./migrations")]
    async fn event_cluster_refinement_job_returns_cleanly_without_persisted_clusters(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool).await;

        let run = timeout(
            TokioDuration::from_secs(1),
            run_event_cluster_refinement_job(state),
        )
        .await;

        assert!(run.is_ok(), "cluster refinement job should return cleanly");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_cluster_refinement_job_persists_missing_deltas_and_frozen_hypotheses(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let previous = event_evidence(
            "official:market_event",
            "notice-cluster-prev",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        let current = event_evidence(
            "official:market_event",
            "notice-cluster-current",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        repo.insert_evidence(&previous).await.unwrap();
        repo.insert_evidence(&current).await.unwrap();

        let prior_claim_id = Uuid::from_u128(101);
        let repeated_claim_id = Uuid::from_u128(102);
        let new_claim_id = Uuid::from_u128(202);
        repo.save_extraction(&extraction_row(
            previous.evidence_id,
            vec![published_claim(
                previous.evidence_id,
                prior_claim_id,
                "600519.SH wins major automation order",
            )],
        ))
        .await
        .unwrap();
        repo.save_extraction(&extraction_row(
            current.evidence_id,
            vec![
                published_claim(
                    current.evidence_id,
                    repeated_claim_id,
                    "600519.SH wins major automation order",
                ),
                published_claim(
                    current.evidence_id,
                    new_claim_id,
                    "600519.SH secures follow-on contract",
                ),
            ],
        ))
        .await
        .unwrap();
        repo.save_claim_graph(&claim_graph_row(
            previous.evidence_id,
            1,
            vec!["600519.SH wins major automation order"],
        ))
        .await
        .unwrap();
        repo.save_claim_graph(&claim_graph_row(
            current.evidence_id,
            1,
            vec![
                "600519.SH wins major automation order",
                "600519.SH secures follow-on contract",
            ],
        ))
        .await
        .unwrap();

        let cluster_id = Uuid::new_v4();
        repo.save_event_cluster_version(&cluster_row_with_snapshot(
            cluster_id,
            1,
            previous.evidence_id,
            vec![prior_claim_id],
        ))
        .await
        .unwrap();
        repo.save_event_cluster_version(&cluster_row_with_snapshot(
            cluster_id,
            2,
            current.evidence_id,
            vec![repeated_claim_id, new_claim_id],
        ))
        .await
        .unwrap();

        run_event_cluster_refinement_job(state).await;

        let delta_count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*)
               FROM market_event_deltas
               WHERE event_cluster_id = $1
                 AND from_version = 1
                 AND to_version = 2"#,
        )
        .bind(cluster_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(delta_count, 1);

        let latest_hypothesis: (i32, i32, Option<Uuid>) = sqlx::query_as(
            r#"SELECT cluster_version, hypothesis_version, supersedes_id
               FROM market_event_hypotheses
               WHERE event_cluster_id = $1
               ORDER BY cluster_version DESC, hypothesis_version DESC
               LIMIT 1"#,
        )
        .bind(cluster_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(latest_hypothesis.0, 2);
        assert_eq!(latest_hypothesis.1, 2);
        assert!(latest_hypothesis.2.is_some());

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_cluster_refinement_job_builds_mentions_and_clusters_from_extracted_evidence(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let first = event_evidence(
            "official:market_event",
            "cluster-fresh-first",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        let second = event_evidence(
            "official:market_event",
            "cluster-fresh-second",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        repo.insert_evidence(&first).await.unwrap();
        repo.insert_evidence(&second).await.unwrap();

        repo.save_extraction(&extraction_row_with_event_payload(
            first.evidence_id,
            "issuer_disclosure",
            None,
            vec![published_claim(
                first.evidence_id,
                Uuid::from_u128(401),
                "600519.SH wins major automation order in Shanghai",
            )],
            vec![
                extracted_entity("Kweichow Moutai", "issuer", "subject", Some("600519.SH")),
                extracted_entity("Shanghai", "location", "location", None),
            ],
            vec![],
        ))
        .await
        .unwrap();
        repo.save_extraction(&extraction_row_with_event_payload(
            second.evidence_id,
            "issuer_disclosure",
            None,
            vec![published_claim(
                second.evidence_id,
                Uuid::from_u128(402),
                "600519.SH secures automation order update in Shanghai",
            )],
            vec![
                extracted_entity("Kweichow Moutai", "issuer", "subject", Some("600519.SH")),
                extracted_entity("Shanghai", "location", "location", None),
            ],
            vec![],
        ))
        .await
        .unwrap();
        repo.save_claim_graph(&claim_graph_row(
            first.evidence_id,
            1,
            vec!["600519.SH wins major automation order in Shanghai"],
        ))
        .await
        .unwrap();
        repo.save_claim_graph(&claim_graph_row(
            second.evidence_id,
            1,
            vec!["600519.SH secures automation order update in Shanghai"],
        ))
        .await
        .unwrap();

        run_event_cluster_refinement_job(state).await;

        let mention_rows: Vec<(Uuid, Uuid, i32, bool, Value)> = sqlx::query_as(
            r#"SELECT evidence_id,
                      event_cluster_id,
                      cluster_version,
                      adds_new_fact,
                      mention_payload
               FROM market_event_mentions
               ORDER BY evidence_id ASC"#,
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(mention_rows.len(), 2);
        assert!(mention_rows.iter().all(|row| row.3));
        assert_eq!(mention_rows[0].1, mention_rows[1].1);
        assert_eq!(mention_rows[0].2, 1);
        assert_eq!(mention_rows[1].2, 1);
        assert_eq!(mention_rows[0].4["entityIds"], json!(["600519.SH"]));
        assert_eq!(mention_rows[1].4["entityIds"], json!(["600519.SH"]));

        let clusters: Vec<(Uuid, i32, i32, i32, Value)> = sqlx::query_as(
            r#"SELECT event_cluster_id,
                      cluster_version,
                      mention_count,
                      independent_sources,
                      cluster_payload
               FROM market_event_clusters
               ORDER BY created_at ASC, event_cluster_id ASC"#,
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].1, 1);
        assert_eq!(clusters[0].2, 2);
        assert_eq!(clusters[0].2 as usize, mention_rows.len());
        assert_eq!(clusters[0].3, 2);
        assert_eq!(
            clusters[0].4["deltaSnapshot"]["claims"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );

        let hypothesis_count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*)
               FROM market_event_hypotheses"#,
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(hypothesis_count, 1);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_cluster_refinement_job_respects_locked_duplicate_merge_relations(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let left = event_evidence(
            "official:market_event",
            "cluster-locked-merge-left",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        let right = event_evidence(
            "official:market_event",
            "cluster-locked-merge-right",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        repo.insert_evidence(&left).await.unwrap();
        repo.insert_evidence(&right).await.unwrap();

        repo.save_extraction(&extraction_row_with_event_payload(
            left.evidence_id,
            "issuer_disclosure",
            None,
            vec![published_claim(
                left.evidence_id,
                Uuid::from_u128(451),
                "600519.SH signs a port logistics agreement",
            )],
            vec![extracted_entity(
                "Kweichow Moutai",
                "issuer",
                "subject",
                Some("600519.SH"),
            )],
            vec![],
        ))
        .await
        .unwrap();
        repo.save_extraction(&extraction_row_with_event_payload(
            right.evidence_id,
            "issuer_disclosure",
            None,
            vec![published_claim(
                right.evidence_id,
                Uuid::from_u128(452),
                "600519.SH opens a bonded warehouse center",
            )],
            vec![extracted_entity(
                "Kweichow Moutai",
                "issuer",
                "subject",
                Some("600519.SH"),
            )],
            vec![],
        ))
        .await
        .unwrap();
        repo.save_claim_graph(&claim_graph_row(
            left.evidence_id,
            1,
            vec!["600519.SH signs a port logistics agreement"],
        ))
        .await
        .unwrap();
        repo.save_claim_graph(&claim_graph_row(
            right.evidence_id,
            1,
            vec!["600519.SH opens a bonded warehouse center"],
        ))
        .await
        .unwrap();

        repo.save_duplicate_group(&DuplicateGroupRow {
            duplicate_group_id: Uuid::new_v4(),
            relation_type: "exact".to_string(),
            confidence: 1.0,
            locked_by_user: true,
            members: vec![
                DuplicateGroupMemberRow {
                    evidence_id: left.evidence_id,
                    is_representative: true,
                },
                DuplicateGroupMemberRow {
                    evidence_id: right.evidence_id,
                    is_representative: false,
                },
            ],
            created_at: dt(2026, 7, 10, 12),
        })
        .await
        .unwrap();

        run_event_cluster_refinement_job(state).await;

        let clusters: Vec<(Uuid, i32, i32)> = sqlx::query_as(
            r#"SELECT event_cluster_id, cluster_version, mention_count
               FROM market_event_clusters
               ORDER BY created_at ASC, event_cluster_id ASC"#,
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(clusters.len(), 1, "{clusters:?}");
        assert_eq!(clusters[0].1, 1);
        assert_eq!(clusters[0].2, 2);

        let mention_links: Vec<(Uuid, Uuid)> = sqlx::query_as(
            r#"SELECT evidence_id, event_cluster_id
               FROM market_event_mentions
               ORDER BY evidence_id ASC"#,
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(mention_links.len(), 2);
        assert_eq!(mention_links[0].1, mention_links[1].1);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_market_observation_job_returns_cleanly_without_persisted_observations(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool).await;

        let run = timeout(
            TokioDuration::from_secs(1),
            run_event_market_observation_job(state),
        )
        .await;

        assert!(run.is_ok(), "market observation job should return cleanly");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_market_observation_job_persists_real_rows_when_market_inputs_exist(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let evidence = event_evidence(
            "official:market_event",
            "notice-market-observation",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        repo.insert_evidence(&evidence).await.unwrap();

        let cluster_id = Uuid::new_v4();
        repo.save_event_cluster_version(&cluster_row_with_snapshot(
            cluster_id,
            1,
            evidence.evidence_id,
            vec![Uuid::from_u128(301)],
        ))
        .await
        .unwrap();
        let hypothesis = frozen_hypothesis_row(cluster_id, 1, 1, None, vec![Uuid::from_u128(301)]);
        repo.save_frozen_hypothesis(&hypothesis).await.unwrap();
        seed_event_market_inputs(&pool, date(2026, 7, 11), "600519.SH").await?;

        run_event_market_observation_job(state).await;

        let stored: (String, Option<f64>, Option<f64>, Value) = sqlx::query_as(
            r#"SELECT observation_status,
                      market_alignment_score::float8,
                      abnormal_market_return::float8,
                      market_metrics
               FROM market_event_market_observations
               WHERE hypothesis_id = $1
               LIMIT 1"#,
        )
        .bind(hypothesis.hypothesis_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored.0, "market_aligned");
        assert_eq!(stored.1, Some(0.025));
        assert_eq!(stored.2, Some(0.03));
        assert_eq!(stored.3["benchmark_id"], json!("000001.SH"));
        assert_eq!(stored.3["industry_benchmark_id"], json!("BANK"));
        assert_eq!(stored.3["stock_return"], json!(0.05));
        assert_eq!(stored.3["market_return"], json!(0.02));
        assert_eq!(stored.3["industry_return"], json!(0.03));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_market_observation_job_reads_structured_entities_from_generated_hypotheses(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let evidence = event_evidence(
            "official:market_event",
            "notice-market-observation-structured-entity",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        repo.insert_evidence(&evidence).await.unwrap();

        let cluster_id = Uuid::new_v4();
        repo.save_event_cluster_version(&cluster_row_with_snapshot(
            cluster_id,
            1,
            evidence.evidence_id,
            vec![Uuid::from_u128(701)],
        ))
        .await
        .unwrap();
        let hypothesis =
            generated_sanitized_hypothesis_row(cluster_id, 1, 1, None, vec![Uuid::from_u128(701)]);
        repo.save_frozen_hypothesis(&hypothesis).await.unwrap();
        seed_event_market_inputs(&pool, date(2026, 7, 11), "600519.SH").await?;

        run_event_market_observation_job(state).await;

        let stored: (String, String, Option<f64>) = sqlx::query_as(
            r#"SELECT entity_type,
                      entity_id,
                      market_alignment_score::float8
               FROM market_event_market_observations
               WHERE hypothesis_id = $1
               LIMIT 1"#,
        )
        .bind(hypothesis.hypothesis_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored.0, "company");
        assert_eq!(stored.1, "600519.SH");
        assert_eq!(stored.2, Some(0.025));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_market_observation_job_persists_confounded_rows_for_same_entity_events(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let evidence = event_evidence(
            "official:market_event",
            "notice-market-observation-confounded",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        let confounder = event_evidence(
            "official:market_event",
            "notice-market-observation-earnings",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        repo.insert_evidence(&evidence).await.unwrap();
        repo.insert_evidence(&confounder).await.unwrap();

        let cluster_id = Uuid::new_v4();
        repo.save_event_cluster_version(&cluster_row_with_snapshot(
            cluster_id,
            1,
            evidence.evidence_id,
            vec![Uuid::from_u128(501)],
        ))
        .await
        .unwrap();
        let hypothesis = frozen_hypothesis_row(cluster_id, 1, 1, None, vec![Uuid::from_u128(501)]);
        repo.save_frozen_hypothesis(&hypothesis).await.unwrap();
        seed_event_market_inputs(&pool, date(2026, 7, 11), "600519.SH").await?;

        repo.save_extraction(&extraction_row_with_event_payload(
            confounder.evidence_id,
            "earnings",
            Some("guidance"),
            vec![published_claim(
                confounder.evidence_id,
                Uuid::from_u128(502),
                "600519.SH reports quarterly earnings guidance",
            )],
            vec![extracted_entity(
                "Kweichow Moutai",
                "issuer",
                "subject",
                Some("600519.SH"),
            )],
            vec![],
        ))
        .await
        .unwrap();

        run_event_market_observation_job(state).await;

        let stored: (String, Value) = sqlx::query_as(
            r#"SELECT observation_status, confounding_events
               FROM market_event_market_observations
               WHERE hypothesis_id = $1
               LIMIT 1"#,
        )
        .bind(hypothesis.hypothesis_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored.0, "confounded");
        assert_eq!(
            stored.1,
            json!([
                {
                    "kind": "earnings",
                    "event_type": "earnings"
                }
            ])
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_market_observation_job_skips_placeholder_rows_when_market_inputs_are_unavailable(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let evidence = event_evidence(
            "official:market_event",
            "notice-market-observation-missing-inputs",
            1,
            "publishable",
            date(2026, 7, 10),
        );
        repo.insert_evidence(&evidence).await.unwrap();

        let cluster_id = Uuid::new_v4();
        repo.save_event_cluster_version(&cluster_row_with_snapshot(
            cluster_id,
            1,
            evidence.evidence_id,
            vec![Uuid::from_u128(302)],
        ))
        .await
        .unwrap();
        let hypothesis = frozen_hypothesis_row(cluster_id, 1, 1, None, vec![Uuid::from_u128(302)]);
        repo.save_frozen_hypothesis(&hypothesis).await.unwrap();

        run_event_market_observation_job(state).await;

        let count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*)
               FROM market_event_market_observations
               WHERE hypothesis_id = $1"#,
        )
        .bind(hypothesis.hypothesis_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(count, 0);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn decision_support_job_persists_run_and_artifacts_when_inputs_exist(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 10);

        seed_stock_daily_bar(&pool, trade_date, "600001.SH").await?;
        seed_decision_market_snapshot(&pool, trade_date, true, Vec::new()).await;
        seed_ranked_pool_candidate(&pool, trade_date, "600001.SH", "Alpha Bank", 95.0).await;
        seed_published_pattern_set(&pool).await?;
        seed_shadow_candidate_for_latest_published_set(&pool, trade_date, "600001.SH").await?;
        seed_daily_event_brief(&pool, trade_date, "event-fingerprint").await;

        run_decision_support_job(state).await;

        let repo = DecisionSupportRepository::new(pool.clone());
        let run = repo
            .find_run_by_trade_date(trade_date)
            .await
            .unwrap()
            .expect("expected persisted decision support run");
        assert_eq!(run.status, "completed");
        assert!(run.pattern_set_id.is_some());
        assert_eq!(
            run.event_brief_version.as_deref(),
            Some("daily_event_brief_v1")
        );

        let candidates = repo.list_candidates(run.run_id).await.unwrap();
        assert!(!candidates.is_empty());
        let brief = repo
            .find_brief(run.run_id)
            .await
            .unwrap()
            .expect("expected persisted brief");
        assert_eq!(brief.trade_date, trade_date);
        assert_eq!(brief.structured_payload["tradeDate"], json!(trade_date));
        assert_eq!(
            brief.structured_payload["candidateCount"],
            json!(candidates.len())
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn decision_support_job_marks_missing_pattern_results_and_uses_scan_ranker_baseline(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 10);

        seed_stock_daily_bar(&pool, trade_date, "600001.SH").await?;
        seed_decision_market_snapshot(&pool, trade_date, true, Vec::new()).await;
        seed_ranked_pool_candidate(&pool, trade_date, "600001.SH", "Alpha Bank", 95.0).await;
        seed_published_pattern_set(&pool).await?;

        run_decision_support_job(state).await;

        let repo = DecisionSupportRepository::new(pool.clone());
        let run = repo
            .find_run_by_trade_date(trade_date)
            .await
            .unwrap()
            .expect("expected persisted decision support run");
        let candidates = repo.list_candidates(run.run_id).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].base_source, "scan_ranker");
        assert!(candidates[0].unknowns.to_string().contains("pattern"));
        assert!(candidates[0].unknowns.to_string().contains("missing"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn decision_support_job_handles_missing_event_brief_without_event_adjustments(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 10);

        seed_stock_daily_bar(&pool, trade_date, "600001.SH").await?;
        seed_decision_market_snapshot(&pool, trade_date, true, Vec::new()).await;
        seed_ranked_pool_candidate(&pool, trade_date, "600001.SH", "Alpha Bank", 95.0).await;

        run_decision_support_job(state).await;

        let repo = DecisionSupportRepository::new(pool.clone());
        let run = repo
            .find_run_by_trade_date(trade_date)
            .await
            .unwrap()
            .expect("expected persisted decision support run");
        assert_eq!(run.event_brief_version, None);

        let candidates = repo.list_candidates(run.run_id).await.unwrap();
        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|candidate| candidate.event_adjustment == Some(0.0)));

        let brief = repo
            .find_brief(run.run_id)
            .await
            .unwrap()
            .expect("expected persisted brief");
        assert_eq!(brief.structured_payload["eventSummary"], Value::Null);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn decision_support_job_persists_data_status_and_withholds_a_tier_when_snapshot_is_incomplete(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 10);
        let missing_input = "security_status:600001.SH:2026-07-10".to_string();

        seed_stock_daily_bar(&pool, trade_date, "600001.SH").await?;
        seed_decision_market_snapshot(&pool, trade_date, false, vec![missing_input.clone()]).await;
        seed_ranked_pool_candidate(&pool, trade_date, "600001.SH", "Alpha Bank", 95.0).await;

        run_decision_support_job(state).await;

        let repo = DecisionSupportRepository::new(pool.clone());
        let run = repo
            .find_run_by_trade_date(trade_date)
            .await
            .unwrap()
            .expect("expected persisted decision support run");
        let candidates = repo.list_candidates(run.run_id).await.unwrap();
        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|candidate| candidate.support_tier != "A"));

        let brief = repo
            .find_brief(run.run_id)
            .await
            .unwrap()
            .expect("expected persisted brief");
        assert_eq!(
            brief.structured_payload["dataStatus"]["dataComplete"],
            json!(false)
        );
        assert_eq!(
            brief.structured_payload["dataStatus"]["missingInputs"],
            json!([missing_input])
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn decision_support_job_returns_cleanly_on_failure_without_touching_trading_tables(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 10);

        seed_stock_daily_bar(&pool, trade_date, "600001.SH").await?;
        seed_decision_market_snapshot(&pool, trade_date, true, Vec::new()).await;
        seed_ranked_pool_candidate(&pool, trade_date, "600001.SH", "Alpha Bank", 95.0).await;

        let repo = DecisionSupportRepository::new(pool.clone());
        let existing_run_id = Uuid::new_v4();
        repo.create_run(
            &crate::storage::decision_support_repository::DecisionSupportRunRow {
                run_id: existing_run_id,
                trade_date,
                support_version: crate::analysis::decision_support::DECISION_SUPPORT_VERSION
                    .to_string(),
                market_snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                pattern_set_id: None,
                event_brief_version: None,
                event_score_enabled: false,
                event_score_limit: 0.0,
                status: "completed".to_string(),
                input_fingerprint: "existing-run".to_string(),
                started_at: dt(2026, 7, 10, 18),
                completed_at: Some(dt(2026, 7, 10, 18)),
                error_message: None,
            },
        )
        .await
        .unwrap();

        let strategy_candidate_count_before =
            count_rows(&pool, "signal_strategy_candidates").await?;
        let _daily_report_guard = state.daily_report_job_lock.lock().await;

        let run = timeout(
            TokioDuration::from_secs(1),
            run_decision_support_job(state.clone()),
        )
        .await;

        assert!(run.is_ok(), "decision support job should return cleanly");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM analysis_decision_support_runs")
                .fetch_one(&pool)
                .await?,
            1,
            "failed decision support run should not persist a duplicate run"
        );
        assert_eq!(
            count_rows(&pool, "signal_strategy_candidates").await?,
            strategy_candidate_count_before
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_ingestion_job_processes_gdelt_sources_with_independent_cursor_handling(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let _guard = EVENT_INGESTION_CURSOR_TEST_LOCK.lock().await;
        let state = test_state(pool.clone()).await;
        let (official_feed_url, official_server) = spawn_event_feed_server(
            r#"{
              "next_cursor": "official-cursor-2",
              "items": [
                {
                  "source_item_id": "official-001",
                  "published_at": "2026-07-10T08:15:00Z",
                  "title": "Official exchange update",
                  "content": "Full bulletin body.",
                  "summary": "Exchange confirms normal trading conditions.",
                  "source_url": "https://example.test/notices/official-001",
                  "category": "market-status"
                }
              ]
            }"#,
        )
        .await;
        let (gdelt_feed_url, gdelt_server) = spawn_event_feed_server(
            r#"{
              "articles": [
                {
                  "source_item_id": "gdelt-001",
                  "seendate": "20260710082000",
                  "title": "Shipping disruption raises insurance costs",
                  "url": "https://example.test/gdelt/gdelt-001",
                  "language": "eng",
                  "themes": ["MARITIME"],
                  "locations": ["Red Sea"],
                  "organizations": ["Global Shipping Co"],
                  "description": "Macro logistics disruption continues."
                },
                {
                  "source_item_id": "gdelt-002",
                  "seendate": "20260710083000",
                  "title": "Broken GDELT article",
                  "url": "not-a-url",
                  "language": "eng",
                  "themes": ["MARITIME"],
                  "locations": ["Red Sea"],
                  "organizations": ["Global Shipping Co"],
                  "description": "Invalid URL should fail ingestion."
                }
              ]
            }"#,
        )
        .await;

        let official_source = OfficialEventSource::new(
            "official:market_event",
            official_feed_url,
            None,
            crate::analysis::adapters::ContentRetentionPolicy::StoreSummaryOnly,
            None,
        )
        .unwrap();
        let gdelt_source = crate::analysis::adapters::gdelt::GdeltEventSource::with_endpoint(
            "red sea shipping".to_string(),
            250,
            gdelt_feed_url,
            None,
        )
        .unwrap();

        let official_cursor_key = event_cursor_key("official:market_event");
        let gdelt_cursor_key = event_cursor_key("gdelt:macro_event");
        event_cursor_set(
            state.redis.clone(),
            &official_cursor_key,
            "official-cursor-1",
        )
        .await
        .unwrap();
        event_cursor_set(
            state.redis.clone(),
            &gdelt_cursor_key,
            "2026-07-10T08:10:00+00:00|gdelt-000",
        )
        .await
        .unwrap();

        run_event_ingestion_sources(
            state.clone(),
            vec![
                Box::new(official_source) as Box<dyn EventSource>,
                Box::new(gdelt_source) as Box<dyn EventSource>,
            ],
        )
        .await;

        assert_eq!(
            event_cursor_get(state.redis.clone(), &official_cursor_key)
                .await
                .unwrap()
                .as_deref(),
            Some("official-cursor-2")
        );
        assert_eq!(
            event_cursor_get(state.redis.clone(), &gdelt_cursor_key)
                .await
                .unwrap()
                .as_deref(),
            Some("2026-07-10T08:10:00+00:00|gdelt-000")
        );

        let stored_sources: Vec<(String, String, Value)> = sqlx::query_as(
            r#"SELECT source_id, source_item_id, raw_payload
               FROM market_event_evidence
               ORDER BY source_id, source_item_id"#,
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(stored_sources.len(), 2, "{stored_sources:?}");
        assert_eq!(stored_sources[0].0, "gdelt:macro_event");
        assert_eq!(stored_sources[0].1, "gdelt-001");
        assert_eq!(stored_sources[0].2["sourceRole"], json!("macro_supplement"));
        assert_eq!(stored_sources[0].2["companyFactEligible"], json!(false));
        assert_eq!(stored_sources[1].0, "official:market_event");
        assert_eq!(stored_sources[1].1, "official-001");

        official_server.abort();
        let _ = official_server.await;
        gdelt_server.abort();
        let _ = gdelt_server.await;
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

    fn extraction_row(evidence_id: Uuid, claims: Vec<ClaimRow>) -> ExtractionRow {
        extraction_row_with_event_payload(
            evidence_id,
            "issuer_disclosure",
            None,
            claims,
            Vec::new(),
            Vec::new(),
        )
    }

    fn extraction_row_with_event_payload(
        evidence_id: Uuid,
        event_type: &str,
        event_subtype: Option<&str>,
        claims: Vec<ClaimRow>,
        entities: Vec<Value>,
        uncertainties: Vec<&str>,
    ) -> ExtractionRow {
        ExtractionRow {
            extraction_id: Uuid::new_v4(),
            evidence_id,
            schema_version: "event-schema-v1".to_string(),
            prompt_version: Some("prompt-v1".to_string()),
            model_name: Some("test-model".to_string()),
            model_parameters: json!({"temperature": 0}),
            extracted_payload: json!({
                "event_type": event_type,
                "event_subtype": event_subtype,
                "claims": claims.iter().map(|claim| {
                    json!({
                        "claim_type": claim.claim_type,
                        "text": claim.claim_text,
                        "evidence_ids": claim.evidence.iter().map(|row| row.evidence_id).collect::<Vec<_>>(),
                        "confidence": claim.confidence
                    })
                }).collect::<Vec<_>>(),
                "entities": entities,
                "amounts": [],
                "dates": [],
                "uncertainties": uncertainties,
                "missing_information": []
            }),
            validation_status: "valid".to_string(),
            validation_errors: json!([]),
            input_fingerprint: format!("fingerprint-{evidence_id}"),
            claims,
            created_at: dt(2026, 7, 10, 12),
        }
    }

    fn extracted_entity(
        text: &str,
        entity_type: &str,
        role: &str,
        stock_code: Option<&str>,
    ) -> Value {
        json!({
            "text": text,
            "entity_type": entity_type,
            "role": role,
            "stock_code": stock_code
        })
    }

    fn published_claim(evidence_id: Uuid, claim_id: Uuid, claim_text: &str) -> ClaimRow {
        ClaimRow {
            claim_id,
            claim_type: "fact".to_string(),
            claim_text: claim_text.to_string(),
            confidence: 0.95,
            review_status: "published".to_string(),
            evidence: vec![ClaimEvidenceRow { evidence_id }],
            created_at: dt(2026, 7, 10, 12),
        }
    }

    fn claim_graph_row(evidence_id: Uuid, graph_version: i32, labels: Vec<&str>) -> ClaimGraphRow {
        ClaimGraphRow {
            claim_graph_id: Uuid::new_v4(),
            evidence_id,
            graph_version,
            schema_version: "claim_graph_v1".to_string(),
            graph_payload: json!({
                "schema_version": "claim_graph_v1",
                "nodes": labels.iter().enumerate().map(|(index, label)| {
                    json!({
                        "node_id": format!("order-{}", index + 1),
                        "node_type": "CompanyFact",
                        "label": label,
                        "evidence_ids": [evidence_id],
                        "confidence": 0.91
                    })
                }).collect::<Vec<_>>(),
                "edges": []
            }),
            review_status: "published".to_string(),
            created_at: dt(2026, 7, 10, 12),
        }
    }

    fn cluster_row_with_snapshot(
        event_cluster_id: Uuid,
        cluster_version: i32,
        primary_evidence_id: Uuid,
        claim_ids: Vec<Uuid>,
    ) -> EventClusterRow {
        let claims = claim_ids
            .into_iter()
            .enumerate()
            .map(|(index, claim_id)| EventClaimSnapshot {
                claim_id,
                canonical_claim_id: format!("claim-{}", index + 1),
                status: Some("published".to_string()),
                value: None,
                entity_roles: vec![ClaimEntityRole {
                    entity_id: "600519.SH".to_string(),
                    role: "subject".to_string(),
                }],
                claim_date: Some(date(2026, 7, 10)),
            })
            .collect::<Vec<_>>();
        let snapshot = EventClusterVersionSnapshot {
            event_cluster_id,
            cluster_version,
            lifecycle_status: "active".to_string(),
            claims,
            expectation: None,
            uncertainties: Vec::new(),
        };

        EventClusterRow {
            event_cluster_id,
            cluster_version,
            canonical_title: format!("Cluster {event_cluster_id} v{cluster_version}"),
            event_time: Some(dt(2026, 7, 10, 9)),
            first_seen_at: dt(2026, 7, 10, 9),
            last_seen_at: dt(2026, 7, 10, 10 + cluster_version as u32),
            lifecycle_status: "active".to_string(),
            primary_evidence_id,
            representative_ids: vec![primary_evidence_id],
            source_entropy: 0.42,
            independent_sources: cluster_version,
            mention_count: cluster_version,
            cluster_payload: json!({
                "deltaSnapshot": snapshot
            }),
            supersedes_version: (cluster_version > 1).then_some(cluster_version - 1),
            created_at: dt(2026, 7, 10, 11 + cluster_version as u32),
        }
    }

    fn frozen_hypothesis_row(
        event_cluster_id: Uuid,
        cluster_version: i32,
        hypothesis_version: i32,
        supersedes_id: Option<Uuid>,
        based_on_claim_ids: Vec<Uuid>,
    ) -> EventHypothesisRow {
        let hypothesis_id = Uuid::new_v4();
        EventHypothesisRow {
            hypothesis_id,
            event_cluster_id,
            cluster_version,
            hypothesis_version,
            schema_version: "impact_hypothesis_graph_v1".to_string(),
            graph_payload: json!({
                "hypothesis_id": hypothesis_id,
                "hypothesis_version": hypothesis_version,
                "supersedes_hypothesis_id": supersedes_id,
                "graph": {
                    "schema_version": "impact_hypothesis_graph_v1",
                    "nodes": [
                        {
                            "node_id": "company_order_v1:source:order-1",
                            "node_type": "CompanyFact",
                            "label": "600519.SH wins major automation order"
                        },
                        {
                            "node_id": "company_order_v1:impact:600519-sh:0",
                            "node_type": "RevenueImpact",
                            "label": "600519.SH"
                        }
                    ],
                    "edges": [
                        {
                            "from": "company_order_v1:source:order-1",
                            "to": "company_order_v1:impact:600519-sh:0",
                            "relation": "increases",
                            "generation_method": "domain_rule",
                            "logic_rule_id": "company_order_v1",
                            "confidence": 0.91,
                            "assumptions": ["Customer executes the awarded order."],
                            "expected_horizon": "t+1",
                            "observable_indicators": ["Order-related revenue expectations strengthen."],
                            "counter_scenario": ["Order is canceled or materially delayed."],
                            "invalidation_conditions": ["The order is publicly withdrawn."]
                        }
                    ],
                    "direct_observation_entities": [
                        {
                            "entity_type": "company",
                            "entity_id": "600519.SH",
                            "display_name": "Kweichow Moutai"
                        }
                    ],
                    "based_on_claim_ids": based_on_claim_ids,
                    "frozen_at": dt(2026, 7, 10, 16)
                }
            }),
            frozen_at: dt(2026, 7, 10, 16),
            based_on_claim_ids,
            review_status: "frozen".to_string(),
            supersedes_id,
            created_at: dt(2026, 7, 10, 16),
        }
    }

    fn generated_sanitized_hypothesis_row(
        event_cluster_id: Uuid,
        cluster_version: i32,
        hypothesis_version: i32,
        supersedes_id: Option<Uuid>,
        based_on_claim_ids: Vec<Uuid>,
    ) -> EventHypothesisRow {
        let generated = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![ClaimNode {
                    node_id: "order-1".to_string(),
                    node_type: "CompanyFact".to_string(),
                    label: "Kweichow Moutai 600519.SH wins major automation order".to_string(),
                    evidence_ids: vec![Uuid::from_u128(9001)],
                    confidence: 0.91,
                }],
                Vec::new(),
            )
            .unwrap(),
            based_on_claim_ids.clone(),
            dt(2026, 7, 10, 16),
        )
        .unwrap();

        assert!(generated
            .graph()
            .nodes
            .iter()
            .all(|node| !node.label.contains("600519.SH")));

        EventHypothesisRow {
            hypothesis_id: generated.hypothesis_id(),
            event_cluster_id,
            cluster_version,
            hypothesis_version,
            schema_version: "impact_hypothesis_graph_v1".to_string(),
            graph_payload: serde_json::to_value(&generated).unwrap(),
            frozen_at: dt(2026, 7, 10, 16),
            based_on_claim_ids,
            review_status: "frozen".to_string(),
            supersedes_id,
            created_at: dt(2026, 7, 10, 16),
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

    async fn seed_event_market_inputs(
        pool: &PgPool,
        trade_date: NaiveDate,
        code: &str,
    ) -> sqlx::Result<()> {
        let available_at = dt(2026, 7, 11, 16);
        for (bar_date, close) in [(trade_date - Duration::days(1), 100.0), (trade_date, 105.0)] {
            sqlx::query(
                r#"INSERT INTO stock_daily_bar_versions
                   (code, trade_date, open, high, low, close, volume, amount,
                    turnover, pe, pb, available_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                           1.2, 12.0, 1.4, $9, 'observed', 'test')"#,
            )
            .bind(code)
            .bind(bar_date)
            .bind(close - 1.0)
            .bind(close + 1.0)
            .bind(close - 2.0)
            .bind(close)
            .bind(10_000_i64)
            .bind(1_000_000.0)
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
            r#"INSERT INTO stock_sector_membership
               (code, sector_code, sector_name, sector_type, valid_from,
                available_at, availability_quality, source)
               VALUES ($1, 'BANK', 'Banking', 'industry', '2020-01-01',
                       $2, 'observed', 'test')"#,
        )
        .bind(code)
        .bind(available_at)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO sector_daily_versions
               (code, name, sector_type, change_pct, amount, trade_date,
                available_at, availability_quality, source)
               VALUES ('BANK', 'Banking', 'industry', 3.0, 1000000.0, $1,
                       $2, 'observed', 'test')"#,
        )
        .bind(trade_date)
        .bind(available_at)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO index_daily_bars
               (code, trade_date, close, change_pct, volume, amount,
                available_at, availability_quality, source)
               VALUES ('000001.SH', $1, 3200.0, 2.0, 100000000, 1000000000.0,
                       $2, 'observed', 'test')"#,
        )
        .bind(trade_date)
        .bind(available_at)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn seed_decision_market_snapshot(
        pool: &PgPool,
        trade_date: NaiveDate,
        data_complete: bool,
        missing_inputs: Vec<String>,
    ) {
        MarketRepository::new(pool.clone())
            .save_market_snapshot(&MarketSnapshot {
                trade_date,
                snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                available_at: dt(2026, 7, 10, 18),
                data_complete,
                metrics: json!({"breadth": {"up_count": 1}}),
                missing_inputs,
                input_fingerprint: format!("decision-snapshot-{trade_date}"),
            })
            .await
            .unwrap();
    }

    async fn seed_ranked_pool_candidate(
        pool: &PgPool,
        trade_date: NaiveDate,
        code: &str,
        name: &str,
        score: f64,
    ) {
        save_daily_signal_scan_results(
            pool,
            trade_date,
            Uuid::new_v4(),
            &[DailySignalScanRow {
                code: code.to_string(),
                name: name.to_string(),
                signal_id: POOL_SHORT_A_ID.to_string(),
                signal_name: "短线A档".to_string(),
                icon: "🔥".to_string(),
                metadata: json!({
                    "line_type": "short",
                    "tier": "A",
                    "trigger_id": "breakout",
                    "trigger_name": "突破信号",
                    "score": score,
                    "reasons": ["突破确认"],
                    "risk_flags": ["量能不足"],
                    "factor_breakdown": [
                        {"name": "trend", "score": 18.5},
                        {"name": "volume", "score": 11.2}
                    ],
                    "supporting_signals": ["breakout"],
                    "matched_setups": [{"id": "breakout", "name": "突破信号"}]
                }),
            }],
        )
        .await
        .unwrap();
    }

    async fn seed_shadow_candidate_for_latest_published_set(
        pool: &PgPool,
        trade_date: NaiveDate,
        code: &str,
    ) -> sqlx::Result<()> {
        let pattern_repo = PatternRepository::new(pool.clone());
        let pattern_set = pattern_repo
            .latest_published_set()
            .await
            .unwrap()
            .expect("expected published pattern set");
        let pattern_version_id: Uuid = sqlx::query_scalar(
            r#"SELECT pattern_version_id
               FROM analysis_pattern_set_members
               WHERE pattern_set_id = $1
               LIMIT 1"#,
        )
        .bind(pattern_set.pattern_set_id)
        .fetch_one(pool)
        .await?;

        pattern_repo
            .upsert_shadow_candidates(&[ShadowCandidateRow {
                trade_date,
                code: code.to_string(),
                name: Some("Alpha Bank".to_string()),
                horizon: "week".to_string(),
                pattern_version_id,
                pattern_set_id: pattern_set.pattern_set_id,
                pattern_type: "trend".to_string(),
                similarity_score: 0.91,
                validated_lift: 1.25,
                final_score: 2.2,
                shadow_tier: "shadow_a".to_string(),
                matched_features: json!({"raw": {"relative_strength_20d": 1.2}}),
                risk_flags: json!({
                    "has_triggered": false,
                    "has_unevaluable": false,
                    "triggered": [],
                    "unevaluable": [],
                    "risk_adjustment": 0.0
                }),
                supporting_signals: json!({
                    "score_components": {
                        "validated_pattern_strength": 0.8,
                        "current_similarity": 0.4,
                        "risk_adjustment": 0.0
                    }
                }),
                invalidations: json!([]),
                input_fingerprint: format!("shadow-{trade_date}-{code}"),
                created_at: dt(2026, 7, 10, 18),
            }])
            .await
            .unwrap();
        Ok(())
    }

    async fn seed_daily_event_brief(pool: &PgPool, trade_date: NaiveDate, fingerprint: &str) {
        EventRepository::new(pool.clone())
            .save_daily_brief(&DailyEventBriefRow {
                trade_date,
                brief_version: "daily_event_brief_v1".to_string(),
                content: "brief".to_string(),
                structured_payload: json!({
                    "tradeDate": trade_date,
                    "newFacts": [],
                    "revisions": [],
                    "unconfirmed": [],
                    "directEntities": [],
                    "sources": [],
                    "inputFingerprint": fingerprint
                }),
                input_fingerprint: fingerprint.to_string(),
                generated_at: dt(2026, 7, 10, 18),
            })
            .await
            .unwrap();
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
                enable_gdelt_events: false,
                gdelt_event_query: String::new(),
                gdelt_max_records: 250,
                enable_burst_monitor: false,
                enable_daban_live: false,
                enable_ai_analysis: false,
                enable_chip_dist: false,
                enable_event_score_adjustment: false,
                max_event_score_adjustment: 0.0,
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
