use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::data::chip::{
    ChipBucket, ChipDayInput, ChipSnapshot, ChipSourceDecision, ChipValidationRun,
    OfficialChipBucket, OfficialChipPerformance, OfficialChipProvider,
};
use crate::data::company::CompanyDataProvider;
use crate::error::{AppError, Result};
use crate::market_time::{beijing_today, beijing_tz};
use crate::services::chip_model::{ChipModelV2, CHIP_MODEL_VERSION};
use crate::services::chip_validation::{
    aggregate_chip_comparisons, build_validation_sample, compare_chip_performance,
    compare_chip_snapshots, decide_chip_source, ChipPerformancePoint, ChipValidationSample,
    ValidationCorporateAction, ValidationObservation, ValidationStock,
};
use crate::storage::chip_repository::{ChipRepository, MAX_CHIP_SNAPSHOTS_PER_TRANSACTION};
use crate::storage::company_repository::{
    CheckpointClaimOutcome, CheckpointClaimPolicy, CheckpointLease, CompanyRepository,
};

const FINANCIAL_BACKFILL_PHASE: &str = "financials";
const DIVIDEND_BACKFILL_PHASE: &str = "dividends";
const FINANCIAL_LATEST_PHASE: &str = "financials_latest";
const DIVIDEND_LATEST_PHASE: &str = "dividends_latest";
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(15 * 60);
const CHIP_BACKFILL_PHASE: &str = "chip_backfill";
const OFFICIAL_CHIP_FIRST_DATE: NaiveDate =
    NaiveDate::from_ymd_opt(2018, 1, 1).expect("valid official chip first date");
const CHIP_BUCKET_COUNT: usize = 30;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompanySyncReport {
    pub completed: usize,
    pub failed: usize,
    pub pending: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompanyIntelligenceUpdateReport {
    pub financials: CompanySyncReport,
    pub dividends: CompanySyncReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChipBenchmarkReport {
    pub reused: bool,
    pub decision: ChipSourceDecision,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChipBackfillReport {
    pub completed: usize,
    pub failed: usize,
    pub pending: usize,
    pub snapshots: usize,
}

#[derive(Debug, Clone)]
struct ChipHistoryRow {
    input: ChipDayInput,
}

#[derive(Debug, Clone)]
struct RawChipHistoryRow {
    trade_date: NaiveDate,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
    turnover_rate: Option<f64>,
    adjustment_factor: Option<f64>,
}

impl RawChipHistoryRow {
    fn into_input(self, code: &str) -> Result<ChipDayInput> {
        Ok(ChipDayInput {
            code: code.to_string(),
            trade_date: self.trade_date,
            open: finite_positive(self.open, code, self.trade_date, "open")?,
            high: finite_positive(self.high, code, self.trade_date, "high")?,
            low: finite_positive(self.low, code, self.trade_date, "low")?,
            close: finite_positive(self.close, code, self.trade_date, "close")?,
            volume: finite_in_range(self.volume, code, self.trade_date, "volume", 0.0, f64::MAX)?,
            turnover_rate: finite_in_range(
                self.turnover_rate,
                code,
                self.trade_date,
                "turnover",
                0.0,
                100.0,
            )?,
            // Deterministic fallback 1.0 is supplied by SQL only when no
            // official factor exists at or before this date.
            adjustment_factor: finite_positive(
                self.adjustment_factor,
                code,
                self.trade_date,
                "adjustment factor",
            )?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum Dataset {
    Financials,
    Dividends,
}

impl Dataset {
    fn label(self) -> &'static str {
        match self {
            Self::Financials => "financials",
            Self::Dividends => "dividends",
        }
    }
}

#[derive(Debug, Clone)]
struct CurrentStock {
    code: String,
    list_date: NaiveDate,
}

#[derive(Debug, Default)]
struct SyncOutcome {
    report: CompanySyncReport,
    errors: Vec<String>,
}

pub struct CompanyIntelligenceService<P: CompanyDataProvider> {
    pool: PgPool,
    repository: CompanyRepository,
    provider: Arc<P>,
    today: NaiveDate,
    lease_ttl: Duration,
}

impl<P: CompanyDataProvider> CompanyIntelligenceService<P> {
    pub fn new(pool: PgPool, provider: Arc<P>) -> Self {
        Self::new_at(pool, provider, beijing_today())
    }

    pub fn new_at(pool: PgPool, provider: Arc<P>, today: NaiveDate) -> Self {
        Self {
            repository: CompanyRepository::new(pool.clone()),
            pool,
            provider,
            today,
            lease_ttl: DEFAULT_LEASE_TTL,
        }
    }

    pub fn with_lease_ttl(mut self, lease_ttl: Duration) -> Self {
        self.lease_ttl = lease_ttl;
        self
    }

    pub async fn backfill_financials(&self) -> Result<CompanySyncReport> {
        self.finish(
            Dataset::Financials,
            self.synchronize(Dataset::Financials, FINANCIAL_BACKFILL_PHASE, false)
                .await?,
        )
    }

    pub async fn backfill_dividends(&self) -> Result<CompanySyncReport> {
        self.finish(
            Dataset::Dividends,
            self.synchronize(Dataset::Dividends, DIVIDEND_BACKFILL_PHASE, false)
                .await?,
        )
    }

    pub async fn update_latest(&self) -> Result<CompanyIntelligenceUpdateReport> {
        let financials = self
            .synchronize(Dataset::Financials, FINANCIAL_LATEST_PHASE, true)
            .await;
        let dividends = self
            .synchronize(Dataset::Dividends, DIVIDEND_LATEST_PHASE, true)
            .await;

        let mut errors = Vec::new();
        let financials = match financials {
            Ok(outcome) => {
                errors.extend(outcome.errors);
                outcome.report
            }
            Err(error) => {
                errors.push(format!("financials: {error}"));
                CompanySyncReport::default()
            }
        };
        let dividends = match dividends {
            Ok(outcome) => {
                errors.extend(outcome.errors);
                outcome.report
            }
            Err(error) => {
                errors.push(format!("dividends: {error}"));
                CompanySyncReport::default()
            }
        };
        let report = CompanyIntelligenceUpdateReport {
            financials,
            dividends,
        };

        if errors.is_empty() {
            Ok(report)
        } else {
            Err(AppError::DataProvider(format!(
                "company latest sync incomplete: financials={financials:?}, dividends={dividends:?}; {}",
                error_summary(&errors)
            )))
        }
    }

    async fn synchronize(
        &self,
        dataset: Dataset,
        phase: &str,
        latest_only: bool,
    ) -> Result<SyncOutcome> {
        let stocks = self.current_stocks().await?;
        let mut outcome = SyncOutcome::default();

        for stock in stocks {
            let windows = if latest_only {
                latest_windows(stock.list_date, self.today)
            } else {
                yearly_windows(stock.list_date, self.today)
            };
            if windows.is_empty() {
                // A future listing date cannot be current, but treating it as pending is safer
                // than creating a nonsensical checkpoint range.
                outcome.report.pending += 1;
                continue;
            }
            let mut stock_failed = false;
            let mut stock_pending = false;
            for (start, end) in windows {
                let checkpoint_end = if latest_only {
                    NaiveDate::from_ymd_opt(end.year(), 12, 31).expect("valid year end")
                } else {
                    end
                };
                let claim = self
                    .repository
                    .claim_checkpoint_window_with_policy(
                        phase,
                        &stock.code,
                        start,
                        checkpoint_end,
                        self.lease_ttl,
                        if latest_only {
                            CheckpointClaimPolicy::RefreshCompletedBefore(beijing_day_cutoff(
                                self.today,
                            ))
                        } else {
                            CheckpointClaimPolicy::Resume
                        },
                    )
                    .await;
                let lease = match claim {
                    Ok(CheckpointClaimOutcome::Claimed(lease)) => lease,
                    Ok(CheckpointClaimOutcome::Completed) => continue,
                    Ok(CheckpointClaimOutcome::Busy) => {
                        stock_pending = true;
                        continue;
                    }
                    Err(error) => {
                        stock_failed = true;
                        outcome.errors.push(format!(
                            "{}/{}/{start}..{end}/claim: {error}",
                            dataset.label(),
                            stock.code
                        ));
                        continue;
                    }
                };

                if let Err(error) = self
                    .synchronize_claimed_window(dataset, &stock.code, start, end, lease)
                    .await
                {
                    stock_failed = true;
                    outcome.errors.push(error);
                }
            }

            if stock_failed {
                outcome.report.failed += 1;
            } else if stock_pending {
                outcome.report.pending += 1;
            } else {
                outcome.report.completed += 1;
            }
        }

        Ok(outcome)
    }

    async fn synchronize_claimed_window(
        &self,
        dataset: Dataset,
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
        lease: CheckpointLease,
    ) -> std::result::Result<(), String> {
        match dataset {
            Dataset::Financials => {
                let reports = match self.provider.financial_reports(code, start, end).await {
                    Ok(reports) => reports,
                    Err(error) => return Err(self.fail_owned_window(dataset, lease, error).await),
                };
                if let Err(error) = self
                    .repository
                    .persist_financial_reports_and_complete_window(&lease, &reports, self.lease_ttl)
                    .await
                {
                    return Err(self.fail_owned_window(dataset, lease, error).await);
                }
            }
            Dataset::Dividends => {
                let records = match self.provider.dividends(code, start, end).await {
                    Ok(records) => records,
                    Err(error) => return Err(self.fail_owned_window(dataset, lease, error).await),
                };
                if let Err(error) = self
                    .repository
                    .persist_dividends_and_complete_window(&lease, &records, self.lease_ttl)
                    .await
                {
                    return Err(self.fail_owned_window(dataset, lease, error).await);
                }
            }
        }

        Ok(())
    }

    async fn fail_owned_window(
        &self,
        dataset: Dataset,
        lease: CheckpointLease,
        error: AppError,
    ) -> String {
        let release_suffix = self
            .repository
            .fail_checkpoint(&lease, &error.to_string())
            .await
            .err()
            .map(|release_error| format!("; checkpoint release failed: {release_error}"))
            .unwrap_or_default();
        format!(
            "{}/{}/{}..{}: {error}{release_suffix}",
            dataset.label(),
            lease.code,
            lease.start_date,
            lease.end_date
        )
    }

    async fn current_stocks(&self) -> Result<Vec<CurrentStock>> {
        Ok(sqlx::query_as::<_, (String, NaiveDate)>(
            r#"WITH latest AS (
                   SELECT DISTINCT ON (code)
                          code, list_status, list_date, delist_date
                   FROM security_master_versions
                   ORDER BY code, available_at DESC, ingested_at DESC, source DESC
               )
               SELECT code, list_date
               FROM latest
               WHERE list_status = 'L'
                 AND list_date IS NOT NULL
                 AND list_date <= $1
                 AND (delist_date IS NULL OR delist_date > $1)
               ORDER BY code"#,
        )
        .bind(self.today)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|(code, list_date)| CurrentStock { code, list_date })
        .collect())
    }

    fn finish(&self, dataset: Dataset, outcome: SyncOutcome) -> Result<CompanySyncReport> {
        if outcome.errors.is_empty() {
            Ok(outcome.report)
        } else {
            Err(AppError::DataProvider(format!(
                "company {} sync incomplete: completed={}, failed={}, pending={}; {}",
                dataset.label(),
                outcome.report.completed,
                outcome.report.failed,
                outcome.report.pending,
                error_summary(&outcome.errors)
            )))
        }
    }
}

impl<P> CompanyIntelligenceService<P>
where
    P: CompanyDataProvider + OfficialChipProvider,
{
    /// Validates the estimator once per model version. A successful stored
    /// decision is immutable input to later backfills; incomplete or failed
    /// runs remain retryable.
    pub async fn run_chip_benchmark(&self) -> Result<ChipBenchmarkReport> {
        let repository = ChipRepository::new(self.pool.clone());
        if let Some(decision) = repository
            .latest_validation_decision(CHIP_MODEL_VERSION)
            .await?
        {
            return Ok(ChipBenchmarkReport {
                reused: true,
                decision,
            });
        }

        let started_at = Utc::now();
        let run_id = Uuid::new_v4();
        match self.execute_chip_benchmark().await {
            Ok((sample, report, decision)) => {
                repository
                    .save_validation_run(&ChipValidationRun {
                        run_id,
                        model_version: CHIP_MODEL_VERSION.to_string(),
                        sample_definition: json_value(&sample)?,
                        aggregate_metrics: json_value(&report.aggregate)?,
                        subgroup_metrics: json_value(&report.subgroups)?,
                        decision: Some(decision),
                        started_at,
                        completed_at: Some(Utc::now()),
                        error_summary: None,
                    })
                    .await?;
                Ok(ChipBenchmarkReport {
                    reused: false,
                    decision,
                })
            }
            Err(error) => {
                let summary = error.to_string();
                repository
                    .save_validation_run(&ChipValidationRun {
                        run_id,
                        model_version: CHIP_MODEL_VERSION.to_string(),
                        sample_definition: json!({}),
                        aggregate_metrics: json!({}),
                        subgroup_metrics: json!({}),
                        decision: None,
                        started_at,
                        completed_at: Some(Utc::now()),
                        error_summary: Some(summary.clone()),
                    })
                    .await?;
                Err(AppError::DataProvider(format!(
                    "chip benchmark failed: {summary}"
                )))
            }
        }
    }

    async fn execute_chip_benchmark(
        &self,
    ) -> Result<(
        ChipValidationSample,
        crate::services::chip_validation::ChipValidationReport,
        ChipSourceDecision,
    )> {
        let (universe, observations, actions) = self.load_validation_inputs().await?;
        let sample =
            build_validation_sample(CHIP_MODEL_VERSION, &universe, &observations, &actions)?;
        validate_benchmark_sample_shape(&sample)?;
        let mut comparisons = Vec::new();

        for sampled in &sample.stocks {
            let requested = sampled
                .performance_dates
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            let distribution_dates = sampled
                .distribution_dates
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            let history = self.load_chip_history(&sampled.code).await?;
            let estimates =
                estimate_history_at_dates(history.into_iter().map(|row| row.input), &requested)?;
            ensure_exact_dates("local estimator", &requested, estimates.keys().copied())?;

            let official_performance = self
                .fetch_official_performance_dates(&sampled.code, &requested)
                .await?;
            let official_distributions = self
                .fetch_official_distribution_dates(&sampled.code, &distribution_dates)
                .await?;

            for trade_date in &requested {
                let local = estimates.get(trade_date).ok_or_else(|| {
                    AppError::DataProvider(format!(
                        "local estimator missing {}/{}",
                        sampled.code, trade_date
                    ))
                })?;
                let official = official_performance.get(trade_date).ok_or_else(|| {
                    AppError::DataProvider(format!(
                        "official performance missing {}/{}",
                        sampled.code, trade_date
                    ))
                })?;
                if distribution_dates.contains(trade_date) {
                    let buckets = official_distributions.get(trade_date).ok_or_else(|| {
                        AppError::DataProvider(format!(
                            "official distribution missing {}/{}",
                            sampled.code, trade_date
                        ))
                    })?;
                    let official_snapshot = official_snapshot(official, buckets)?;
                    comparisons.push(compare_chip_snapshots(local, &official_snapshot)?);
                } else {
                    comparisons.push(compare_chip_performance(
                        &ChipPerformancePoint {
                            code: local.code.clone(),
                            trade_date: *trade_date,
                            average_cost: local.average_cost,
                            winner_rate: local.winner_rate,
                        },
                        &ChipPerformancePoint {
                            code: official.code.clone(),
                            trade_date: *trade_date,
                            average_cost: official.average_cost,
                            winner_rate: official.winner_rate,
                        },
                    )?);
                }
            }
        }

        let report = aggregate_chip_comparisons(&sample, &comparisons)?;
        let decision = decide_chip_source(&report);
        Ok((sample, report, decision))
    }

    pub async fn backfill_chips(&self) -> Result<ChipBackfillReport> {
        let chip_repository = ChipRepository::new(self.pool.clone());
        let decision = chip_repository
            .latest_validation_decision(CHIP_MODEL_VERSION)
            .await?
            .ok_or_else(|| {
                AppError::DataProvider(format!(
                    "chip backfill has no successful {CHIP_MODEL_VERSION} validation decision"
                ))
            })?;
        let stocks = self.current_stocks().await?;
        let mut report = ChipBackfillReport::default();
        let mut errors = Vec::new();

        for stock in stocks {
            match self
                .backfill_chip_stock(&chip_repository, decision, &stock.code)
                .await
            {
                Ok(stock_report) => {
                    report.snapshots += stock_report.snapshots;
                    if stock_report.failed > 0 {
                        report.failed += 1;
                    } else if stock_report.pending > 0 {
                        report.pending += 1;
                    } else {
                        report.completed += 1;
                    }
                }
                Err(error) => {
                    report.failed += 1;
                    errors.push(format!("{}/chip_backfill: {error}", stock.code));
                }
            }
        }

        if errors.is_empty() {
            Ok(report)
        } else {
            Err(AppError::DataProvider(format!(
                "chip backfill incomplete: completed={}, failed={}, pending={}, snapshots={}; {}",
                report.completed,
                report.failed,
                report.pending,
                report.snapshots,
                error_summary(&errors)
            )))
        }
    }

    async fn backfill_chip_stock(
        &self,
        chip_repository: &ChipRepository,
        decision: ChipSourceDecision,
        code: &str,
    ) -> Result<ChipBackfillReport> {
        let history = self.load_raw_chip_history(code).await?;
        if history.is_empty() {
            return Err(AppError::DataProvider(format!(
                "no valid daily bars for {code}"
            )));
        }
        let mut model = ChipModelV2::new(CHIP_BUCKET_COUNT);
        let mut report = ChipBackfillReport::default();
        let company_repository = CompanyRepository::new(self.pool.clone());

        for rows in history.chunks(MAX_CHIP_SNAPSHOTS_PER_TRANSACTION) {
            let start = rows.first().expect("non-empty chunk").trade_date;
            let end = rows.last().expect("non-empty chunk").trade_date;

            let claim = company_repository
                .claim_checkpoint_window(CHIP_BACKFILL_PHASE, code, start, end, self.lease_ttl)
                .await?;
            let lease = match claim {
                CheckpointClaimOutcome::Completed => None,
                CheckpointClaimOutcome::Busy => {
                    report.pending += 1;
                    break;
                }
                CheckpointClaimOutcome::Claimed(lease) => Some(lease),
            };

            let mut estimates = Vec::with_capacity(rows.len());
            let mut input_error = None;
            for row in rows.iter().cloned() {
                match row.into_input(code).and_then(|input| model.update(input)) {
                    Ok(snapshot) => estimates.push(snapshot),
                    Err(error) => {
                        input_error = Some(error);
                        break;
                    }
                }
            }
            if let Some(error) = input_error {
                if let Some(lease) = lease.as_ref() {
                    company_repository
                        .fail_checkpoint(lease, &format!("invalid chip input: {error}"))
                        .await?;
                    return Err(AppError::DataProvider(format!(
                        "invalid chip input in {code}/{start}..{end}: {error}"
                    )));
                }
                return Err(AppError::DataProvider(format!(
                    "completed chip checkpoint {code}/{start}..{end} can no longer be replayed: {error}"
                )));
            }
            let Some(lease) = lease else {
                // Completed snapshots are not rewritten, but every bar still
                // advances the estimator for the next incomplete window.
                continue;
            };

            let fallback_estimates = estimates.clone();
            let snapshots = match self
                .canonical_snapshots_for_window(decision, estimates)
                .await
            {
                Ok(snapshots) => snapshots,
                Err(error) => {
                    // Estimates are useful fallback evidence but are explicitly
                    // unvalidated and the checkpoint remains retryable.
                    if decision == ChipSourceDecision::Official {
                        chip_repository
                            .upsert_unvalidated_estimate_batch(&fallback_estimates)
                            .await?;
                    }
                    let failure = format!("official chip data unavailable: {error}");
                    company_repository.fail_checkpoint(&lease, &failure).await?;
                    return Err(AppError::DataProvider(failure));
                }
            };
            let state = model.state().ok_or_else(|| {
                AppError::Internal("chip model did not produce state".to_string())
            })?;
            match chip_repository
                .persist_snapshot_batch_and_complete(&lease, &snapshots, Some(&state))
                .await
            {
                Ok(changed) => report.snapshots += changed,
                Err(error) => {
                    let release_suffix = company_repository
                        .fail_checkpoint(&lease, &error.to_string())
                        .await
                        .err()
                        .map(|release| format!("; checkpoint release failed: {release}"))
                        .unwrap_or_default();
                    return Err(AppError::DataProvider(format!(
                        "chip batch persistence failed for {code}/{start}..{end}: {error}{release_suffix}"
                    )));
                }
            }
        }
        Ok(report)
    }

    async fn canonical_snapshots_for_window(
        &self,
        decision: ChipSourceDecision,
        mut estimates: Vec<ChipSnapshot>,
    ) -> Result<Vec<ChipSnapshot>> {
        match decision {
            ChipSourceDecision::Estimate => {
                for estimate in &mut estimates {
                    estimate.validated = true;
                }
                Ok(estimates)
            }
            ChipSourceDecision::Official => {
                let code = estimates
                    .first()
                    .map(|snapshot| snapshot.code.clone())
                    .ok_or_else(|| AppError::BadRequest("empty chip window".to_string()))?;
                let official_dates = estimates
                    .iter()
                    .filter(|snapshot| snapshot.trade_date >= OFFICIAL_CHIP_FIRST_DATE)
                    .map(|snapshot| snapshot.trade_date)
                    .collect::<BTreeSet<_>>();
                let performance = self
                    .fetch_official_performance_dates(&code, &official_dates)
                    .await?;
                let distributions = self
                    .fetch_official_distribution_dates(&code, &official_dates)
                    .await?;
                let mut canonical = Vec::with_capacity(estimates.len());
                for mut estimate in estimates {
                    if estimate.trade_date < OFFICIAL_CHIP_FIRST_DATE {
                        estimate.validated = false;
                        canonical.push(estimate);
                    } else {
                        canonical.push(official_snapshot(
                            performance.get(&estimate.trade_date).ok_or_else(|| {
                                AppError::DataProvider(format!(
                                    "official performance missing {}/{}",
                                    code, estimate.trade_date
                                ))
                            })?,
                            distributions.get(&estimate.trade_date).ok_or_else(|| {
                                AppError::DataProvider(format!(
                                    "official distribution missing {}/{}",
                                    code, estimate.trade_date
                                ))
                            })?,
                        )?);
                    }
                }
                Ok(canonical)
            }
        }
    }

    async fn fetch_official_performance_dates(
        &self,
        code: &str,
        dates: &BTreeSet<NaiveDate>,
    ) -> Result<BTreeMap<NaiveDate, OfficialChipPerformance>> {
        let mut rows = BTreeMap::new();
        for (start, end) in bounded_date_windows(dates, 366) {
            for row in self.provider.chip_performance(code, start, end).await? {
                if dates.contains(&row.trade_date) && rows.insert(row.trade_date, row).is_some() {
                    return Err(AppError::DataProvider(format!(
                        "duplicate official performance for {code}"
                    )));
                }
            }
        }
        ensure_exact_dates("official performance", dates, rows.keys().copied())?;
        Ok(rows)
    }

    async fn fetch_official_distribution_dates(
        &self,
        code: &str,
        dates: &BTreeSet<NaiveDate>,
    ) -> Result<BTreeMap<NaiveDate, Vec<OfficialChipBucket>>> {
        let mut rows = BTreeMap::<NaiveDate, Vec<OfficialChipBucket>>::new();
        for (start, end) in bounded_date_windows(dates, 45) {
            for row in self.provider.chip_distribution(code, start, end).await? {
                if dates.contains(&row.trade_date) {
                    rows.entry(row.trade_date).or_default().push(row);
                }
            }
        }
        ensure_exact_dates("official distribution", dates, rows.keys().copied())?;
        Ok(rows)
    }

    async fn load_validation_inputs(
        &self,
    ) -> Result<(
        Vec<ValidationStock>,
        Vec<ValidationObservation>,
        Vec<ValidationCorporateAction>,
    )> {
        let current_count: i64 = sqlx::query_scalar(
            r#"WITH latest_master AS (
                   SELECT DISTINCT ON (code) code, list_status, list_date, delist_date
                   FROM security_master_versions
                   ORDER BY code, available_at DESC, ingested_at DESC, source DESC
               )
               SELECT COUNT(*) FROM latest_master
               WHERE list_status = 'L' AND list_date <= $1
                 AND (delist_date IS NULL OR delist_date > $1)"#,
        )
        .bind(self.today)
        .fetch_one(&self.pool)
        .await?;
        let stocks = sqlx::query_as::<_, (String, String, f64)>(
            r#"WITH latest_master AS (
                   SELECT DISTINCT ON (code) code, exchange, list_status, list_date, delist_date
                   FROM security_master_versions
                   ORDER BY code, available_at DESC, ingested_at DESC, source DESC
               ), latest_value AS (
                   SELECT DISTINCT ON (code) code, total_mv::float8 AS total_mv
                   FROM stock_daily_basic_versions
                   WHERE total_mv IS NOT NULL
                   ORDER BY code, trade_date DESC, available_at DESC, ingested_at DESC
               )
               SELECT master.code, COALESCE(NULLIF(master.exchange, ''),
                                           split_part(master.code, '.', 2)), value.total_mv
               FROM latest_master master
               JOIN latest_value value USING (code)
               WHERE master.list_status = 'L'
                 AND master.list_date <= $1
                 AND (master.delist_date IS NULL OR master.delist_date > $1)
                 AND value.total_mv >= 0
               ORDER BY master.code"#,
        )
        .bind(self.today)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|(code, exchange, market_value)| ValidationStock {
            code,
            exchange,
            // Tushare total_mv is in ten-thousand CNY units.
            market_value: market_value * 10_000.0,
        })
        .collect::<Vec<_>>();
        if stocks.is_empty() {
            return Err(AppError::DataProvider(
                "chip benchmark has no stocks with usable market value".to_string(),
            ));
        }
        if stocks.len() != current_count as usize {
            return Err(AppError::DataProvider(format!(
                "chip benchmark market value coverage is incomplete: usable={}, current={current_count}",
                stocks.len()
            )));
        }
        let codes = stocks
            .iter()
            .map(|stock| stock.code.clone())
            .collect::<Vec<_>>();
        let raw = sqlx::query_as::<_, (String, NaiveDate, Option<f64>, Option<f64>)>(
            r#"SELECT bars.code, bars.trade_date, bars.close::float8,
                      COALESCE(bars.turnover::float8, basic.turnover_rate)
               FROM stock_daily_bars bars
               LEFT JOIN LATERAL (
                   SELECT turnover_rate::float8 AS turnover_rate
                   FROM stock_daily_basic_versions daily
                   WHERE daily.code = bars.code AND daily.trade_date = bars.trade_date
                   ORDER BY daily.available_at DESC, daily.ingested_at DESC
                   LIMIT 1
               ) basic ON TRUE
               WHERE bars.code = ANY($1)
                 AND bars.trade_date >= DATE '2018-01-01'
                 AND bars.trade_date <= $2
               ORDER BY bars.code, bars.trade_date"#,
        )
        .bind(&codes)
        .bind(self.today)
        .fetch_all(&self.pool)
        .await?;
        let mut previous = BTreeMap::<String, f64>::new();
        let mut observations = Vec::with_capacity(raw.len());
        for (code, trade_date, close, turnover) in raw {
            let close = finite_positive(close, &code, trade_date, "close")?;
            let turnover_rate =
                finite_in_range(turnover, &code, trade_date, "turnover", 0.0, 100.0)?;
            let volatility = previous
                .insert(code.clone(), close)
                .map_or(0.0, |prior| ((close / prior) - 1.0).abs());
            observations.push(ValidationObservation {
                code,
                trade_date,
                turnover_rate,
                volatility,
            });
        }
        let actions = sqlx::query_as::<_, (String, NaiveDate)>(
            r#"SELECT DISTINCT ON (code, action_date) code, action_date
               FROM (
                   SELECT code, COALESCE(ex_date, record_date, announcement_date) AS action_date,
                          available_at, ingested_at
                   FROM corporate_action_versions
                   WHERE code = ANY($1)
               ) actions
               WHERE action_date IS NOT NULL
               ORDER BY code, action_date, available_at DESC, ingested_at DESC"#,
        )
        .bind(&codes)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|(code, action_date)| ValidationCorporateAction { code, action_date })
        .collect();
        Ok((stocks, observations, actions))
    }

    async fn load_chip_history(&self, code: &str) -> Result<Vec<ChipHistoryRow>> {
        self.load_raw_chip_history(code)
            .await?
            .into_iter()
            .map(|row| row.into_input(code).map(|input| ChipHistoryRow { input }))
            .collect()
    }

    async fn load_raw_chip_history(&self, code: &str) -> Result<Vec<RawChipHistoryRow>> {
        let rows = sqlx::query_as::<
            _,
            (
                NaiveDate,
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<f64>,
            ),
        >(
            r#"SELECT bars.trade_date, bars.open::float8, bars.high::float8,
                      bars.low::float8, bars.close::float8, bars.volume::float8,
                      COALESCE(bars.turnover::float8, basic.turnover_rate),
                      COALESCE(factor.adj_factor, 1.0)::float8
               FROM stock_daily_bars bars
               LEFT JOIN LATERAL (
                   SELECT turnover_rate::float8 AS turnover_rate
                   FROM stock_daily_basic_versions daily
                   WHERE daily.code = bars.code AND daily.trade_date = bars.trade_date
                   ORDER BY daily.available_at DESC, daily.ingested_at DESC
                   LIMIT 1
               ) basic ON TRUE
               LEFT JOIN LATERAL (
                   SELECT adj_factor
                   FROM stock_adjustment_factors adjustment
                   WHERE adjustment.code = bars.code
                     AND adjustment.trade_date <= bars.trade_date
                   ORDER BY adjustment.trade_date DESC, adjustment.available_at DESC,
                            adjustment.ingested_at DESC
                   LIMIT 1
               ) factor ON TRUE
               WHERE bars.code = $1 AND bars.trade_date <= $2
               ORDER BY bars.trade_date ASC"#,
        )
        .bind(code)
        .bind(self.today)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(trade_date, open, high, low, close, volume, turnover_rate, adjustment_factor)| {
                    RawChipHistoryRow {
                        trade_date,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        turnover_rate,
                        adjustment_factor,
                    }
                },
            )
            .collect())
    }
}

fn json_value(value: &impl Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(AppError::from)
}

fn estimate_history_at_dates(
    history: impl IntoIterator<Item = ChipDayInput>,
    requested: &BTreeSet<NaiveDate>,
) -> Result<BTreeMap<NaiveDate, ChipSnapshot>> {
    let mut model = ChipModelV2::new(CHIP_BUCKET_COUNT);
    let mut estimates = BTreeMap::new();
    for input in history {
        let snapshot = model.update(input)?;
        if requested.contains(&snapshot.trade_date) {
            estimates.insert(snapshot.trade_date, snapshot);
        }
    }
    Ok(estimates)
}

fn validate_benchmark_sample_shape(sample: &ChipValidationSample) -> Result<()> {
    const REQUIRED_STOCKS: usize = 200;
    const REQUIRED_PERFORMANCE_DATES: usize = 24;
    const REQUIRED_DISTRIBUTION_STOCKS: usize = 50;
    const REQUIRED_DISTRIBUTION_DATES: usize = 12;

    let distribution_stocks = sample
        .stocks
        .iter()
        .filter(|stock| !stock.distribution_dates.is_empty())
        .count();
    if sample.stocks.len() != REQUIRED_STOCKS
        || sample
            .stocks
            .iter()
            .any(|stock| stock.performance_dates.len() != REQUIRED_PERFORMANCE_DATES)
        || distribution_stocks != REQUIRED_DISTRIBUTION_STOCKS
        || sample.stocks.iter().any(|stock| {
            !stock.distribution_dates.is_empty()
                && stock.distribution_dates.len() != REQUIRED_DISTRIBUTION_DATES
        })
    {
        return Err(AppError::DataProvider(format!(
            "chip benchmark sample is incomplete: stocks={}, distribution_stocks={distribution_stocks}",
            sample.stocks.len()
        )));
    }
    Ok(())
}

fn bounded_date_windows(
    dates: &BTreeSet<NaiveDate>,
    max_calendar_days: i64,
) -> Vec<(NaiveDate, NaiveDate)> {
    let mut windows = Vec::new();
    let mut iter = dates.iter().copied().peekable();
    while let Some(start) = iter.next() {
        let limit = start + ChronoDuration::days(max_calendar_days - 1);
        let mut end = start;
        while iter.peek().is_some_and(|date| *date <= limit) {
            end = iter.next().expect("peeked date exists");
        }
        windows.push((start, end));
    }
    windows
}

fn ensure_exact_dates(
    label: &str,
    expected: &BTreeSet<NaiveDate>,
    actual: impl IntoIterator<Item = NaiveDate>,
) -> Result<()> {
    let actual = actual.into_iter().collect::<BTreeSet<_>>();
    if actual != *expected {
        let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
        let unexpected = actual.difference(expected).copied().collect::<Vec<_>>();
        return Err(AppError::DataProvider(format!(
            "{label} date set mismatch: missing={missing:?}, unexpected={unexpected:?}"
        )));
    }
    Ok(())
}

fn official_snapshot(
    performance: &OfficialChipPerformance,
    buckets: &[OfficialChipBucket],
) -> Result<ChipSnapshot> {
    if buckets.is_empty() {
        return Err(AppError::DataProvider(format!(
            "official distribution is empty for {}/{}",
            performance.code, performance.trade_date
        )));
    }
    if buckets.iter().any(|bucket| {
        bucket.code != performance.code || bucket.trade_date != performance.trade_date
    }) {
        return Err(AppError::DataProvider(
            "official chip code/date mismatch".to_string(),
        ));
    }
    let sum = buckets.iter().map(|bucket| bucket.weight).sum::<f64>();
    if !sum.is_finite() || (sum - 1.0).abs() > 1e-6 {
        return Err(AppError::DataProvider(format!(
            "official distribution is not normalized (sum={sum})"
        )));
    }
    let mut distribution = buckets
        .iter()
        .map(|bucket| ChipBucket {
            price: bucket.price,
            weight: bucket.weight,
        })
        .collect::<Vec<_>>();
    distribution.sort_by(|left, right| left.price.total_cmp(&right.price));
    let dominant_peak_price = distribution
        .iter()
        .max_by(|left, right| {
            left.weight
                .total_cmp(&right.weight)
                .then_with(|| right.price.total_cmp(&left.price))
        })
        .expect("non-empty distribution")
        .price;
    let mut weights = distribution
        .iter()
        .map(|bucket| bucket.weight)
        .collect::<Vec<_>>();
    weights.sort_by(|left, right| right.total_cmp(left));
    let concentration = weights.into_iter().take(5).sum::<f64>() * 100.0;
    Ok(ChipSnapshot {
        code: performance.code.clone(),
        trade_date: performance.trade_date,
        distribution,
        average_cost: performance.average_cost,
        winner_rate: performance.winner_rate,
        concentration: concentration.clamp(0.0, 100.0),
        dominant_peak_price,
        source: "tushare".to_string(),
        model_version: None,
        validated: true,
        source_updated_at: Utc::now(),
    })
}

fn finite_positive(
    value: Option<f64>,
    code: &str,
    trade_date: NaiveDate,
    field: &str,
) -> Result<f64> {
    let value = value.ok_or_else(|| {
        AppError::DataProvider(format!("missing {field} for {code}/{trade_date}"))
    })?;
    if !value.is_finite() || value <= 0.0 {
        return Err(AppError::DataProvider(format!(
            "invalid {field} for {code}/{trade_date}: {value}"
        )));
    }
    Ok(value)
}

fn finite_in_range(
    value: Option<f64>,
    code: &str,
    trade_date: NaiveDate,
    field: &str,
    minimum: f64,
    maximum: f64,
) -> Result<f64> {
    let value = value.ok_or_else(|| {
        AppError::DataProvider(format!("missing {field} for {code}/{trade_date}"))
    })?;
    if !value.is_finite() || value < minimum || value > maximum {
        return Err(AppError::DataProvider(format!(
            "invalid {field} for {code}/{trade_date}: {value}"
        )));
    }
    Ok(value)
}

fn error_summary(errors: &[String]) -> String {
    const MAX_DETAILS: usize = 10;
    let mut summary = errors
        .iter()
        .take(MAX_DETAILS)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    if errors.len() > MAX_DETAILS {
        summary.push_str(&format!(
            "; and {} additional failures",
            errors.len() - MAX_DETAILS
        ));
    }
    summary
}

fn beijing_day_cutoff(today: NaiveDate) -> DateTime<Utc> {
    beijing_tz()
        .from_local_datetime(
            &today
                .and_hms_opt(0, 0, 0)
                .expect("a date has a valid midnight"),
        )
        .single()
        .expect("Beijing midnight is unambiguous")
        .with_timezone(&Utc)
}

fn yearly_windows(list_date: NaiveDate, today: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    if list_date > today {
        return Vec::new();
    }
    (list_date.year()..=today.year())
        .map(|year| {
            let year_start = NaiveDate::from_ymd_opt(year, 1, 1).expect("valid year start");
            let year_end = NaiveDate::from_ymd_opt(year, 12, 31).expect("valid year end");
            (list_date.max(year_start), today.min(year_end))
        })
        .filter(|(start, end)| start <= end)
        .collect()
}

fn latest_windows(list_date: NaiveDate, today: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    let prior_year = today.year().saturating_sub(1);
    let start = NaiveDate::from_ymd_opt(prior_year, 1, 1).expect("valid prior fiscal year");
    yearly_windows(list_date.max(start), today)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::{Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use sqlx::PgPool;

    use super::{
        bounded_date_windows, estimate_history_at_dates, official_snapshot,
        CompanyIntelligenceService,
    };
    use crate::data::chip::{
        ChipDayInput, ChipSourceDecision, OfficialChipBucket, OfficialChipPerformance,
        OfficialChipProvider,
    };
    use crate::data::company::{
        CompanyDataProvider, DividendRecord, FinancialFrequency, FinancialReport,
    };
    use crate::error::{AppError, Result};
    use crate::storage::company_repository::CompanyRepository;

    #[derive(Default)]
    struct RecordingChipProvider {
        official_calls: Mutex<usize>,
        fail_official: bool,
    }

    #[async_trait]
    impl CompanyDataProvider for RecordingChipProvider {
        async fn financial_reports(
            &self,
            _code: &str,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<FinancialReport>> {
            Ok(Vec::new())
        }

        async fn dividends(
            &self,
            _code: &str,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<DividendRecord>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl OfficialChipProvider for RecordingChipProvider {
        async fn chip_performance(
            &self,
            code: &str,
            start: NaiveDate,
            end: NaiveDate,
        ) -> Result<Vec<OfficialChipPerformance>> {
            *self.official_calls.lock().unwrap() += 1;
            if self.fail_official {
                return Err(AppError::DataProvider("injected official failure".into()));
            }
            Ok(days(start, end)
                .map(|trade_date| OfficialChipPerformance {
                    code: code.to_string(),
                    trade_date,
                    historical_low: 8.0,
                    historical_high: 12.0,
                    cost_5pct: 8.5,
                    cost_15pct: 9.0,
                    cost_50pct: 10.0,
                    cost_85pct: 11.0,
                    cost_95pct: 11.5,
                    average_cost: 10.25,
                    winner_rate: 62.0,
                })
                .collect())
        }

        async fn chip_distribution(
            &self,
            code: &str,
            start: NaiveDate,
            end: NaiveDate,
        ) -> Result<Vec<OfficialChipBucket>> {
            *self.official_calls.lock().unwrap() += 1;
            if self.fail_official {
                return Err(AppError::DataProvider("injected official failure".into()));
            }
            Ok(days(start, end)
                .flat_map(|trade_date| {
                    [
                        OfficialChipBucket {
                            code: code.to_string(),
                            trade_date,
                            price: 10.0,
                            weight: 0.6,
                        },
                        OfficialChipBucket {
                            code: code.to_string(),
                            trade_date,
                            price: 11.0,
                            weight: 0.4,
                        },
                    ]
                })
                .collect())
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum CallKind {
        Financials,
        Dividends,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ProviderCall {
        kind: CallKind,
        code: String,
        start: NaiveDate,
        end: NaiveDate,
    }

    #[derive(Default)]
    struct RecordingProvider {
        calls: Mutex<Vec<ProviderCall>>,
        financial_failures: Mutex<HashMap<String, usize>>,
        financial_window_failures: Mutex<HashMap<(String, NaiveDate), usize>>,
        dividend_failures: Mutex<HashMap<String, usize>>,
        financial_results: Mutex<HashMap<String, Vec<FinancialReport>>>,
        dividend_results: Mutex<HashMap<String, Vec<DividendRecord>>>,
    }

    impl RecordingProvider {
        fn calls(&self) -> Vec<ProviderCall> {
            self.calls.lock().unwrap().clone()
        }

        fn fail_financials(&self, code: &str, times: usize) {
            self.financial_failures
                .lock()
                .unwrap()
                .insert(code.to_string(), times);
        }

        fn fail_financial_window(&self, code: &str, start: NaiveDate, times: usize) {
            self.financial_window_failures
                .lock()
                .unwrap()
                .insert((code.to_string(), start), times);
        }

        fn clear_failure(&self, code: &str) {
            self.financial_failures.lock().unwrap().remove(code);
            self.dividend_failures.lock().unwrap().remove(code);
        }

        fn fail_dividends(&self, code: &str, times: usize) {
            self.dividend_failures
                .lock()
                .unwrap()
                .insert(code.to_string(), times);
        }

        fn return_financials(&self, code: &str, reports: Vec<FinancialReport>) {
            self.financial_results
                .lock()
                .unwrap()
                .insert(code.to_string(), reports);
        }

        fn return_dividends(&self, code: &str, records: Vec<DividendRecord>) {
            self.dividend_results
                .lock()
                .unwrap()
                .insert(code.to_string(), records);
        }
    }

    #[async_trait]
    impl CompanyDataProvider for RecordingProvider {
        async fn financial_reports(
            &self,
            code: &str,
            start: NaiveDate,
            end: NaiveDate,
        ) -> Result<Vec<FinancialReport>> {
            self.calls.lock().unwrap().push(ProviderCall {
                kind: CallKind::Financials,
                code: code.to_string(),
                start,
                end,
            });
            let mut window_failures = self.financial_window_failures.lock().unwrap();
            if let Some(remaining) = window_failures.get_mut(&(code.to_string(), start)) {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(AppError::DataProvider(format!(
                        "recording provider failed for {code}/{start}"
                    )));
                }
            }
            drop(window_failures);
            let mut failures = self.financial_failures.lock().unwrap();
            if let Some(remaining) = failures.get_mut(code) {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(AppError::DataProvider(format!(
                        "recording provider failed for {code}"
                    )));
                }
            }
            drop(failures);
            Ok(self
                .financial_results
                .lock()
                .unwrap()
                .get(code)
                .cloned()
                .unwrap_or_default())
        }

        async fn dividends(
            &self,
            code: &str,
            start: NaiveDate,
            end: NaiveDate,
        ) -> Result<Vec<DividendRecord>> {
            self.calls.lock().unwrap().push(ProviderCall {
                kind: CallKind::Dividends,
                code: code.to_string(),
                start,
                end,
            });
            let mut failures = self.dividend_failures.lock().unwrap();
            if let Some(remaining) = failures.get_mut(code) {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(AppError::DataProvider(format!(
                        "recording dividend provider failed for {code}"
                    )));
                }
            }
            drop(failures);
            Ok(self
                .dividend_results
                .lock()
                .unwrap()
                .get(code)
                .cloned()
                .unwrap_or_default())
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn days(start: NaiveDate, end: NaiveDate) -> impl Iterator<Item = NaiveDate> {
        (0..=(end - start).num_days()).map(move |offset| start + ChronoDuration::days(offset))
    }

    async fn seed_chip_decision(pool: &PgPool, decision: ChipSourceDecision) {
        sqlx::query(
            r#"INSERT INTO chip_model_validation_runs
               (run_id, model_version, sample_definition, aggregate_metrics,
                subgroup_metrics, decision, started_at, completed_at)
               VALUES (gen_random_uuid(), 'qbot-chip-v2', '{}', '{}', '{}', $1,
                       NOW() - INTERVAL '1 minute', NOW())"#,
        )
        .bind(decision.as_str())
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_chip_bars(pool: &PgPool, code: &str, start: NaiveDate, count: usize) {
        for offset in 0..count {
            let trade_date = start + ChronoDuration::days(offset as i64);
            let price = 10.0 + offset as f64 / 100.0;
            sqlx::query(
                r#"INSERT INTO stock_daily_bars
                   (code, trade_date, open, high, low, close, volume, turnover)
                   VALUES ($1, $2, $3, $4, $5, $6, 1000, 5.0)"#,
            )
            .bind(code)
            .bind(trade_date)
            .bind(price)
            .bind(price + 0.5)
            .bind(price - 0.5)
            .bind(price + 0.1)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    fn migration_024_sql() -> String {
        std::fs::read_to_string(format!(
            "{}/migrations/024_expand_company_checkpoints.sql",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap_or_default()
    }

    fn financial_report(code: &str, source: &str, revision: &str) -> FinancialReport {
        FinancialReport {
            source: source.to_string(),
            code: code.to_string(),
            end_date: date(2025, 12, 31),
            announcement_date: Some(date(2026, 3, 30)),
            report_type: "1".to_string(),
            frequency: FinancialFrequency::Annual,
            source_revision: revision.to_string(),
            total_revenue: None,
            revenue: None,
            operating_profit: None,
            total_profit: None,
            net_profit_parent: None,
            deducted_net_profit: None,
            basic_eps: None,
            diluted_eps: None,
            roe: None,
            gross_margin: None,
            net_margin: None,
            revenue_yoy: None,
            net_profit_yoy: None,
            raw_payload: json!({"revision": revision}),
            available_at: Utc.with_ymd_and_hms(2026, 3, 30, 8, 0, 0).unwrap(),
            ingested_at: Utc.with_ymd_and_hms(2026, 7, 19, 8, 0, 0).unwrap(),
        }
    }

    fn dividend_record(code: &str, source: &str, revision: &str) -> DividendRecord {
        DividendRecord {
            source: source.to_string(),
            action_key: format!("{code}-2025-final"),
            code: code.to_string(),
            announcement_date: Some(date(2026, 3, 30)),
            record_date: None,
            ex_date: Some(date(2026, 6, 20)),
            pay_date: None,
            implementation_status: "implemented".to_string(),
            cash_dividend: None,
            cash_dividend_tax: None,
            stock_ratio: None,
            source_revision: revision.to_string(),
            raw_payload: json!({"revision": revision}),
            available_at: Utc.with_ymd_and_hms(2026, 3, 30, 8, 0, 0).unwrap(),
            ingested_at: Utc.with_ymd_and_hms(2026, 7, 19, 8, 0, 0).unwrap(),
        }
    }

    async fn seed_current_stock(pool: &PgPool, code: &str, list_date: NaiveDate) {
        sqlx::query(
            r#"INSERT INTO security_master_versions
               (code, name, market, exchange, list_status, list_date, delist_date,
                available_at, availability_quality, source)
               VALUES ($1, $1, 'A', 'SZ', 'L', $2, NULL,
                       '2026-07-19 00:00:00+00', 'observed', 'test')"#,
        )
        .bind(code)
        .bind(list_date)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_stock_version(
        pool: &PgPool,
        code: &str,
        status: &str,
        list_date: Option<NaiveDate>,
        delist_date: Option<NaiveDate>,
        available_at: chrono::DateTime<Utc>,
    ) {
        sqlx::query(
            r#"INSERT INTO security_master_versions
               (code, name, market, exchange, list_status, list_date, delist_date,
                available_at, availability_quality, source)
               VALUES ($1, $1, 'A', 'SZ', $2, $3, $4, $5, 'observed', 'test')"#,
        )
        .bind(code)
        .bind(status)
        .bind(list_date)
        .bind(delist_date)
        .bind(available_at)
        .execute(pool)
        .await
        .unwrap();
    }

    #[test]
    fn official_conversion_uses_lower_tied_peak_and_percentage_concentration() {
        let performance = OfficialChipPerformance {
            code: "600519.SH".into(),
            trade_date: date(2026, 7, 17),
            historical_low: 8.0,
            historical_high: 14.0,
            cost_5pct: 8.0,
            cost_15pct: 9.0,
            cost_50pct: 10.0,
            cost_85pct: 12.0,
            cost_95pct: 13.0,
            average_cost: 10.5,
            winner_rate: 55.0,
        };
        let buckets = (0..6)
            .map(|index| OfficialChipBucket {
                code: performance.code.clone(),
                trade_date: performance.trade_date,
                price: 10.0 + index as f64,
                weight: [0.2, 0.2, 0.18, 0.16, 0.14, 0.12][index],
            })
            .collect::<Vec<_>>();

        let snapshot = official_snapshot(&performance, &buckets).unwrap();
        assert_eq!(snapshot.source, "tushare");
        assert_eq!(snapshot.model_version, None);
        assert!(snapshot.validated);
        assert_eq!(snapshot.average_cost, 10.5);
        assert_eq!(snapshot.winner_rate, 55.0);
        assert_eq!(snapshot.dominant_peak_price, 10.0);
        assert!((snapshot.concentration - 88.0).abs() < 1e-9);

        let dates = [
            date(2020, 1, 1),
            date(2020, 1, 20),
            date(2020, 2, 14),
            date(2020, 2, 15),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            bounded_date_windows(&dates, 45),
            vec![
                (date(2020, 1, 1), date(2020, 2, 14)),
                (date(2020, 2, 15), date(2020, 2, 15))
            ]
        );
    }

    #[test]
    fn sampled_estimate_evolves_from_full_history_not_the_first_sample_date() {
        let first = ChipDayInput {
            code: "600519.SH".into(),
            trade_date: date(2020, 1, 2),
            open: 9.0,
            high: 10.0,
            low: 8.0,
            close: 9.0,
            volume: 1_000.0,
            turnover_rate: 100.0,
            adjustment_factor: 1.0,
        };
        let sampled = ChipDayInput {
            trade_date: date(2020, 1, 3),
            open: 19.0,
            high: 20.0,
            low: 18.0,
            close: 19.0,
            turnover_rate: 0.0,
            ..first.clone()
        };
        let requested = [sampled.trade_date].into_iter().collect();

        let full = estimate_history_at_dates([first, sampled.clone()], &requested).unwrap()
            [&sampled.trade_date]
            .average_cost;
        let warm_started = estimate_history_at_dates([sampled.clone()], &requested).unwrap()
            [&sampled.trade_date]
            .average_cost;
        assert!(
            full < 11.0,
            "old mass must survive zero-turnover sample day"
        );
        assert!(warm_started > 18.0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn successful_validation_decision_is_reused_without_provider_work(pool: PgPool) {
        seed_chip_decision(&pool, ChipSourceDecision::Estimate).await;
        let provider = Arc::new(RecordingChipProvider::default());
        let service = CompanyIntelligenceService::new_at(pool, provider.clone(), date(2026, 7, 19));

        let report = service.run_chip_benchmark().await.unwrap();
        assert!(report.reused);
        assert_eq!(report.decision, ChipSourceDecision::Estimate);
        assert_eq!(*provider.official_calls.lock().unwrap(), 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn failed_benchmark_is_persisted_completed_and_retried(pool: PgPool) {
        let provider = Arc::new(RecordingChipProvider::default());
        let service = CompanyIntelligenceService::new_at(pool.clone(), provider, date(2026, 7, 19));

        assert!(service.run_chip_benchmark().await.is_err());
        assert!(service.run_chip_benchmark().await.is_err());
        let rows: Vec<(Option<String>, bool, Option<String>)> = sqlx::query_as(
            r#"SELECT decision, completed_at IS NOT NULL, error_summary
               FROM chip_model_validation_runs
               WHERE model_version = 'qbot-chip-v2'
               ORDER BY recorded_at"#,
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| row.0.is_none() && row.1 && row.2.is_some()));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn chip_backfill_estimate_policy_batches_250_plus_remainder_and_resumes(pool: PgPool) {
        let code = "000001.SZ";
        let start = date(2017, 1, 1);
        seed_current_stock(&pool, code, start).await;
        seed_chip_decision(&pool, ChipSourceDecision::Estimate).await;
        seed_chip_bars(&pool, code, start, 251).await;
        let provider = Arc::new(RecordingChipProvider::default());
        let service = CompanyIntelligenceService::new_at(
            pool.clone(),
            provider.clone(),
            start + ChronoDuration::days(250),
        );

        let report = service.backfill_chips().await.unwrap();
        assert_eq!(report.completed, 1);
        assert_eq!(report.snapshots, 251);
        assert_eq!(*provider.official_calls.lock().unwrap(), 0);
        let checkpoints: Vec<(NaiveDate, NaiveDate, String)> = sqlx::query_as(
            "SELECT start_date, end_date, status FROM company_data_repair_checkpoints WHERE phase = 'chip_backfill' ORDER BY start_date",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(checkpoints.len(), 2);
        assert_eq!(checkpoints[0].0, start);
        assert_eq!(checkpoints[0].1, start + ChronoDuration::days(249));
        assert_eq!(checkpoints[1].0, start + ChronoDuration::days(250));
        assert!(checkpoints
            .iter()
            .all(|checkpoint| checkpoint.2 == "completed"));
        let provenance: (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*) FILTER (WHERE source = 'qbot_estimate' AND validated), COUNT(*) FROM chip_distribution WHERE code = $1",
        )
        .bind(code)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(provenance, (251, 251));

        let resumed = service.backfill_chips().await.unwrap();
        assert_eq!(resumed.completed, 1);
        assert_eq!(resumed.snapshots, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn official_policy_keeps_pre_2018_estimate_and_writes_exact_official_metrics(
        pool: PgPool,
    ) {
        let code = "000001.SZ";
        let start = date(2017, 12, 31);
        seed_current_stock(&pool, code, start).await;
        seed_chip_decision(&pool, ChipSourceDecision::Official).await;
        seed_chip_bars(&pool, code, start, 3).await;
        let provider = Arc::new(RecordingChipProvider::default());
        let service = CompanyIntelligenceService::new_at(pool.clone(), provider, date(2018, 1, 2));

        service.backfill_chips().await.unwrap();
        let rows: Vec<(NaiveDate, String, Option<String>, bool, f64, f64, f64, f64)> =
            sqlx::query_as(
                r#"SELECT trade_date, source, model_version, validated,
                          avg_cost::float8, profit_ratio::float8,
                          concentration::float8, dominant_peak_price::float8
                   FROM chip_distribution WHERE code = $1 ORDER BY trade_date"#,
            )
            .bind(code)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].1, "qbot_estimate");
        assert_eq!(rows[0].2.as_deref(), Some("qbot-chip-v2"));
        assert!(!rows[0].3);
        for row in &rows[1..] {
            assert_eq!(row.1, "tushare");
            assert_eq!(row.2, None);
            assert!(row.3);
            assert_eq!(row.4, 10.25);
            assert_eq!(row.5, 62.0);
            assert_eq!(row.6, 100.0);
            assert_eq!(row.7, 10.0);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn official_failure_leaves_only_unvalidated_estimates_and_failed_retryable_checkpoint(
        pool: PgPool,
    ) {
        let code = "000001.SZ";
        let start = date(2020, 1, 2);
        seed_current_stock(&pool, code, start).await;
        seed_chip_decision(&pool, ChipSourceDecision::Official).await;
        seed_chip_bars(&pool, code, start, 2).await;
        let provider = Arc::new(RecordingChipProvider {
            fail_official: true,
            ..Default::default()
        });
        let service = CompanyIntelligenceService::new_at(
            pool.clone(),
            provider,
            start + ChronoDuration::days(1),
        );

        let error = service.backfill_chips().await.unwrap_err();
        assert!(error.to_string().contains("injected official failure"));
        let provenance: Vec<(String, Option<String>, bool)> = sqlx::query_as(
            "SELECT source, model_version, validated FROM chip_distribution WHERE code = $1 ORDER BY trade_date",
        )
        .bind(code)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(provenance.len(), 2);
        assert!(provenance.iter().all(|row| row.0 == "qbot_estimate"
            && row.1.as_deref() == Some("qbot-chip-v2")
            && !row.2));
        let checkpoint = CompanyRepository::new(pool)
            .checkpoint_window(
                "chip_backfill",
                code,
                start,
                start + ChronoDuration::days(1),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.status, "failed");
        assert!(checkpoint.lease_token.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn invalid_chronological_input_fails_owned_window_without_later_snapshots(pool: PgPool) {
        let code = "000001.SZ";
        let start = date(2020, 1, 2);
        seed_current_stock(&pool, code, start).await;
        seed_chip_decision(&pool, ChipSourceDecision::Estimate).await;
        seed_chip_bars(&pool, code, start, 3).await;
        sqlx::query(
            "UPDATE stock_daily_bars SET turnover = NULL WHERE code = $1 AND trade_date = $2",
        )
        .bind(code)
        .bind(start + ChronoDuration::days(1))
        .execute(&pool)
        .await
        .unwrap();
        let service = CompanyIntelligenceService::new_at(
            pool.clone(),
            Arc::new(RecordingChipProvider::default()),
            start + ChronoDuration::days(2),
        );

        let error = service.backfill_chips().await.unwrap_err();
        assert!(error.to_string().contains("missing turnover"));
        let checkpoint = CompanyRepository::new(pool.clone())
            .checkpoint_window(
                "chip_backfill",
                code,
                start,
                start + ChronoDuration::days(2),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.status, "failed");
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM chip_distribution WHERE code = $1")
                .bind(code)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn backfill_carries_latest_applicable_adjustment_factor_chronologically(pool: PgPool) {
        let code = "000001.SZ";
        let start = date(2020, 1, 2);
        seed_current_stock(&pool, code, start).await;
        seed_chip_decision(&pool, ChipSourceDecision::Estimate).await;
        seed_chip_bars(&pool, code, start, 3).await;
        sqlx::query(
            r#"INSERT INTO stock_adjustment_factors
               (code, trade_date, adj_factor, available_at,
                availability_quality, source)
               VALUES ($1, $2, 2.0, '2020-01-03 09:00:00+00', 'observed', 'test')"#,
        )
        .bind(code)
        .bind(start + ChronoDuration::days(1))
        .execute(&pool)
        .await
        .unwrap();
        let service = CompanyIntelligenceService::new_at(
            pool.clone(),
            Arc::new(RecordingChipProvider::default()),
            start + ChronoDuration::days(2),
        );

        service.backfill_chips().await.unwrap();
        let (through_date, factor): (NaiveDate, f64) = sqlx::query_as(
            r#"SELECT through_date, last_adjustment_factor::float8
               FROM chip_model_states WHERE code = $1 AND model_version = 'qbot-chip-v2'"#,
        )
        .bind(code)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(through_date, start + ChronoDuration::days(2));
        assert_eq!(
            factor, 2.0,
            "the factor from day two carries into day three"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn backfill_uses_yearly_listing_bounded_windows_and_resumes_other_stocks(pool: PgPool) {
        seed_current_stock(&pool, "000002.SZ", date(2025, 1, 1)).await;
        seed_current_stock(&pool, "000001.SZ", date(2024, 6, 15)).await;
        let provider = Arc::new(RecordingProvider::default());
        provider.fail_financials("000002.SZ", 1);
        let service =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));

        assert!(service.backfill_financials().await.is_err());
        let first_run_calls = provider.calls();
        assert_eq!(
            first_run_calls,
            vec![
                ProviderCall {
                    kind: CallKind::Financials,
                    code: "000001.SZ".to_string(),
                    start: date(2024, 6, 15),
                    end: date(2024, 12, 31),
                },
                ProviderCall {
                    kind: CallKind::Financials,
                    code: "000001.SZ".to_string(),
                    start: date(2025, 1, 1),
                    end: date(2025, 12, 31),
                },
                ProviderCall {
                    kind: CallKind::Financials,
                    code: "000001.SZ".to_string(),
                    start: date(2026, 1, 1),
                    end: date(2026, 7, 19),
                },
                ProviderCall {
                    kind: CallKind::Financials,
                    code: "000002.SZ".to_string(),
                    start: date(2025, 1, 1),
                    end: date(2025, 12, 31),
                },
                ProviderCall {
                    kind: CallKind::Financials,
                    code: "000002.SZ".to_string(),
                    start: date(2026, 1, 1),
                    end: date(2026, 7, 19),
                },
            ]
        );

        provider.clear_failure("000002.SZ");
        let report = service.backfill_financials().await.unwrap();
        assert_eq!(report.completed, 2);
        assert_eq!(report.failed, 0);
        assert_eq!(report.pending, 0);
        assert_eq!(
            provider.calls()[..first_run_calls.len()],
            first_run_calls[..]
        );
        assert_eq!(
            provider
                .calls()
                .iter()
                .filter(|call| call.code == "000001.SZ")
                .count(),
            3,
            "the completed stock must not be fetched again"
        );
        let repo = CompanyRepository::new(pool);
        assert_eq!(
            repo.checkpoint_window(
                "financials",
                "000002.SZ",
                date(2025, 1, 1),
                date(2025, 12, 31),
            )
            .await
            .unwrap()
            .unwrap()
            .status,
            "completed"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn migrated_completed_broad_range_skips_all_provider_windows(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2024, 6, 15)).await;
        sqlx::query(
            r#"INSERT INTO company_data_repair_checkpoints
               (phase, code, start_date, end_date, status, attempts, completed_at)
               VALUES ('financials', '000001.SZ', '2024-06-15', '2026-07-19',
                       'completed', 1, NOW())"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(&migration_024_sql())
            .execute(&pool)
            .await
            .unwrap();
        let provider = Arc::new(RecordingProvider::default());
        let service = CompanyIntelligenceService::new_at(pool, provider.clone(), date(2026, 7, 19));

        assert_eq!(service.backfill_financials().await.unwrap().completed, 1);
        assert!(provider.calls().is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn sentinel_open_coverage_refetches_the_exact_service_window(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2026, 1, 1)).await;
        sqlx::query(
            r#"INSERT INTO company_data_repair_checkpoints
               (phase, code, start_date, end_date, status, attempts, completed_at)
               VALUES ('financials', '000001.SZ', '0001-01-01', '9999-12-31',
                       'completed', 4, NOW())"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let provider = Arc::new(RecordingProvider::default());
        let service =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));

        assert_eq!(service.backfill_financials().await.unwrap().completed, 1);
        assert_eq!(
            provider.calls(),
            vec![ProviderCall {
                kind: CallKind::Financials,
                code: "000001.SZ".into(),
                start: date(2026, 1, 1),
                end: date(2026, 7, 19),
            }]
        );
        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM company_data_repair_checkpoints WHERE phase = 'financials' AND code = '000001.SZ'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rows, 2, "sentinel audit and exact completion coexist");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn late_year_failure_keeps_completed_windows_and_retry_fetches_only_failed_window(
        pool: PgPool,
    ) {
        seed_current_stock(&pool, "000001.SZ", date(2024, 1, 1)).await;
        let provider = Arc::new(RecordingProvider::default());
        provider.fail_financial_window("000001.SZ", date(2025, 1, 1), 1);
        let service =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));

        let first_error = service.backfill_financials().await.unwrap_err();
        assert!(first_error
            .to_string()
            .contains("completed=0, failed=1, pending=0"));
        assert_eq!(
            provider
                .calls()
                .iter()
                .map(|call| call.start)
                .collect::<Vec<_>>(),
            [date(2024, 1, 1), date(2025, 1, 1), date(2026, 1, 1)],
            "a failed window must not stop later independent windows"
        );

        let repo = CompanyRepository::new(pool);
        assert_eq!(
            repo.checkpoint_window(
                "financials",
                "000001.SZ",
                date(2024, 1, 1),
                date(2024, 12, 31),
            )
            .await
            .unwrap()
            .unwrap()
            .status,
            "completed"
        );
        assert_eq!(
            repo.checkpoint_window(
                "financials",
                "000001.SZ",
                date(2025, 1, 1),
                date(2025, 12, 31),
            )
            .await
            .unwrap()
            .unwrap()
            .status,
            "failed"
        );
        assert_eq!(
            repo.checkpoint_window(
                "financials",
                "000001.SZ",
                date(2026, 1, 1),
                date(2026, 7, 19),
            )
            .await
            .unwrap()
            .unwrap()
            .status,
            "completed"
        );

        let report = service.backfill_financials().await.unwrap();
        assert_eq!(
            report,
            super::CompanySyncReport {
                completed: 1,
                failed: 0,
                pending: 0,
            }
        );
        assert_eq!(
            provider
                .calls()
                .iter()
                .map(|call| call.start)
                .collect::<Vec<_>>(),
            [
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2026, 1, 1),
                date(2025, 1, 1),
            ],
            "retry must fetch only the failed window"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn financial_and_dividend_backfills_have_independent_checkpoints(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2026, 1, 1)).await;
        let provider = Arc::new(RecordingProvider::default());
        provider.fail_dividends("000001.SZ", 1);
        let service =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));

        assert_eq!(service.backfill_financials().await.unwrap().completed, 1);
        let error = service.backfill_dividends().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("completed=0, failed=1, pending=0"));

        let repo = CompanyRepository::new(pool);
        let financials = repo
            .checkpoint_window(
                "financials",
                "000001.SZ",
                date(2026, 1, 1),
                date(2026, 7, 19),
            )
            .await
            .unwrap()
            .unwrap();
        let dividends = repo
            .checkpoint_window(
                "dividends",
                "000001.SZ",
                date(2026, 1, 1),
                date(2026, 7, 19),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(financials.status, "completed");
        assert_eq!(dividends.status, "failed");
        assert!(financials.lease_token.is_none());
        assert!(dividends.lease_token.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn provider_failures_release_the_lease_and_do_not_stop_later_stocks(pool: PgPool) {
        for code in ["000001.SZ", "000002.SZ", "000003.SZ"] {
            seed_current_stock(&pool, code, date(2026, 1, 1)).await;
        }
        let provider = Arc::new(RecordingProvider::default());
        provider.fail_financials("000002.SZ", 1);
        let service =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));

        let error = service.backfill_financials().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("completed=2, failed=1, pending=0"));
        assert_eq!(
            provider
                .calls()
                .into_iter()
                .map(|call| call.code)
                .collect::<Vec<_>>(),
            ["000001.SZ", "000002.SZ", "000003.SZ"]
        );
        let failed = CompanyRepository::new(pool)
            .checkpoint_window(
                "financials",
                "000002.SZ",
                date(2026, 1, 1),
                date(2026, 7, 19),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, "failed");
        assert!(failed.lease_token.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn storage_failure_rolls_back_the_window_and_releases_the_lease(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2026, 1, 1)).await;
        let provider = Arc::new(RecordingProvider::default());
        provider.return_financials(
            "000001.SZ",
            vec![
                financial_report("000001.SZ", "test", "valid-before-error"),
                financial_report("000001.SZ", "", "invalid-source"),
            ],
        );
        let service = CompanyIntelligenceService::new_at(pool.clone(), provider, date(2026, 7, 19));

        assert!(service.backfill_financials().await.is_err());
        let stored: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM stock_financial_report_versions WHERE code = '000001.SZ'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, 0);
        let checkpoint = CompanyRepository::new(pool)
            .checkpoint_window(
                "financials",
                "000001.SZ",
                date(2026, 1, 1),
                date(2026, 7, 19),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.status, "failed");
        assert!(checkpoint.lease_token.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn active_checkpoint_ownership_is_pending_and_never_stolen(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2025, 1, 1)).await;
        let repo = CompanyRepository::new(pool.clone());
        let owner = repo
            .claim_checkpoint_window(
                "financials",
                "000001.SZ",
                date(2025, 1, 1),
                date(2025, 12, 31),
                Duration::from_secs(300),
            )
            .await
            .unwrap();
        let crate::storage::company_repository::CheckpointClaimOutcome::Claimed(owner) = owner
        else {
            panic!("test owner should claim the window")
        };
        let provider = Arc::new(RecordingProvider::default());
        let service = CompanyIntelligenceService::new_at(pool, provider.clone(), date(2026, 7, 19));

        let report = service.backfill_financials().await.unwrap();
        assert_eq!(
            report,
            super::CompanySyncReport {
                completed: 0,
                failed: 0,
                pending: 1,
            }
        );
        assert_eq!(
            provider
                .calls()
                .iter()
                .map(|call| (call.start, call.end))
                .collect::<Vec<_>>(),
            [(date(2026, 1, 1), date(2026, 7, 19))],
            "a busy window must not block another window for the same stock"
        );
        assert_eq!(
            repo.checkpoint_window(
                "financials",
                "000001.SZ",
                date(2025, 1, 1),
                date(2025, 12, 31),
            )
            .await
            .unwrap()
            .unwrap()
            .lease_token,
            Some(owner.token)
        );
        repo.fail_checkpoint(&owner, "test cleanup").await.unwrap();

        assert_eq!(service.backfill_financials().await.unwrap().completed, 1);
        assert_eq!(
            provider
                .calls()
                .iter()
                .map(|call| call.start)
                .collect::<Vec<_>>(),
            [date(2026, 1, 1), date(2025, 1, 1)],
            "retry fetches only the released window"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn checkpoint_claim_database_error_counts_failed_and_continues_other_stocks(
        pool: PgPool,
    ) {
        for code in ["000001.SZ", "000002.SZ", "000003.SZ"] {
            seed_current_stock(&pool, code, date(2026, 1, 1)).await;
        }
        sqlx::raw_sql(
            r#"CREATE FUNCTION reject_one_company_checkpoint_claim() RETURNS trigger AS $$
               BEGIN
                   IF NEW.code = '000002.SZ' THEN
                       RAISE EXCEPTION 'injected checkpoint claim failure';
                   END IF;
                   RETURN NEW;
               END;
               $$ LANGUAGE plpgsql;
               CREATE TRIGGER reject_one_company_checkpoint_claim
               BEFORE INSERT ON company_data_repair_checkpoints
               FOR EACH ROW EXECUTE FUNCTION reject_one_company_checkpoint_claim();"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let provider = Arc::new(RecordingProvider::default());
        let service = CompanyIntelligenceService::new_at(pool, provider.clone(), date(2026, 7, 19));

        let error = service.backfill_financials().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("completed=2, failed=1, pending=0"));
        assert_eq!(
            provider
                .calls()
                .iter()
                .map(|call| call.code.as_str())
                .collect::<Vec<_>>(),
            ["000001.SZ", "000003.SZ"]
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn checkpoint_completion_error_releases_window_and_continues_other_stocks(pool: PgPool) {
        for code in ["000001.SZ", "000002.SZ", "000003.SZ"] {
            seed_current_stock(&pool, code, date(2026, 1, 1)).await;
        }
        sqlx::raw_sql(
            r#"CREATE FUNCTION reject_one_company_checkpoint_completion() RETURNS trigger AS $$
               BEGIN
                   IF OLD.code = '000002.SZ' AND NEW.status = 'completed' THEN
                       RAISE EXCEPTION 'injected checkpoint completion failure';
                   END IF;
                   RETURN NEW;
               END;
               $$ LANGUAGE plpgsql;
               CREATE TRIGGER reject_one_company_checkpoint_completion
               BEFORE UPDATE ON company_data_repair_checkpoints
               FOR EACH ROW EXECUTE FUNCTION reject_one_company_checkpoint_completion();"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let provider = Arc::new(RecordingProvider::default());
        provider.return_financials(
            "000002.SZ",
            vec![financial_report(
                "000002.SZ",
                "test",
                "rolls-back-with-completion",
            )],
        );
        let service =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));

        let error = service.backfill_financials().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("completed=2, failed=1, pending=0"));
        assert_eq!(provider.calls().len(), 3);
        let stored: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM stock_financial_report_versions WHERE code = '000002.SZ'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            stored, 0,
            "record insert rolls back when checkpoint completion fails"
        );
        let failed = CompanyRepository::new(pool)
            .checkpoint_window(
                "financials",
                "000002.SZ",
                date(2026, 1, 1),
                date(2026, 7, 19),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, "failed");
        assert!(failed.lease_token.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn latest_refreshes_prior_and_current_fiscal_years_for_both_datasets(pool: PgPool) {
        seed_current_stock(&pool, "000002.SZ", date(2026, 4, 10)).await;
        seed_current_stock(&pool, "000001.SZ", date(2020, 1, 1)).await;
        let provider = Arc::new(RecordingProvider::default());
        let service =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));

        let report = service.update_latest().await.unwrap();
        assert_eq!(report.financials.completed, 2);
        assert_eq!(report.dividends.completed, 2);
        assert_eq!(
            provider.calls(),
            vec![
                ProviderCall {
                    kind: CallKind::Financials,
                    code: "000001.SZ".into(),
                    start: date(2025, 1, 1),
                    end: date(2025, 12, 31)
                },
                ProviderCall {
                    kind: CallKind::Financials,
                    code: "000001.SZ".into(),
                    start: date(2026, 1, 1),
                    end: date(2026, 7, 19)
                },
                ProviderCall {
                    kind: CallKind::Financials,
                    code: "000002.SZ".into(),
                    start: date(2026, 4, 10),
                    end: date(2026, 7, 19)
                },
                ProviderCall {
                    kind: CallKind::Dividends,
                    code: "000001.SZ".into(),
                    start: date(2025, 1, 1),
                    end: date(2025, 12, 31)
                },
                ProviderCall {
                    kind: CallKind::Dividends,
                    code: "000001.SZ".into(),
                    start: date(2026, 1, 1),
                    end: date(2026, 7, 19)
                },
                ProviderCall {
                    kind: CallKind::Dividends,
                    code: "000002.SZ".into(),
                    start: date(2026, 4, 10),
                    end: date(2026, 7, 19)
                },
            ]
        );

        let same_day = service.update_latest().await.unwrap();
        assert_eq!(same_day.financials.completed, 2);
        assert_eq!(same_day.dividends.completed, 2);
        assert_eq!(
            provider.calls().len(),
            6,
            "same-day completed latest windows must be skipped atomically"
        );
        let repo = CompanyRepository::new(pool);
        assert_eq!(
            repo.checkpoint_window(
                "financials_latest",
                "000001.SZ",
                date(2025, 1, 1),
                date(2025, 12, 31),
            )
            .await
            .unwrap()
            .unwrap()
            .attempts,
            1
        );
        assert_eq!(
            repo.checkpoint_window(
                "dividends_latest",
                "000001.SZ",
                date(2025, 1, 1),
                date(2025, 12, 31),
            )
            .await
            .unwrap()
            .unwrap()
            .attempts,
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn latest_checkpoint_rows_stay_bounded_and_refresh_once_per_beijing_day(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2020, 1, 1)).await;
        let provider = Arc::new(RecordingProvider::default());
        let day_one =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));
        day_one.update_latest().await.unwrap();
        assert_eq!(provider.calls().len(), 4);
        sqlx::query(
            "UPDATE company_data_repair_checkpoints SET completed_at = '2026-07-19 01:00:00+00' WHERE phase IN ('financials_latest', 'dividends_latest')",
        ).execute(&pool).await.unwrap();

        let day_two =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 20));
        day_two.update_latest().await.unwrap();
        assert_eq!(
            provider.calls().len(),
            8,
            "each stable window refreshes the following day"
        );
        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM company_data_repair_checkpoints WHERE phase IN ('financials_latest', 'dividends_latest')",
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(rows, 4, "daily refresh reuses stable dataset/year keys");
        let attempts: Vec<i32> = sqlx::query_scalar(
            "SELECT attempts FROM company_data_repair_checkpoints WHERE phase IN ('financials_latest', 'dividends_latest')",
        ).fetch_all(&pool).await.unwrap();
        assert!(attempts.iter().all(|attempt| *attempt == 2));

        sqlx::query(
            "UPDATE company_data_repair_checkpoints SET completed_at = '2026-07-20 01:00:00+00' WHERE phase IN ('financials_latest', 'dividends_latest')",
        ).execute(&pool).await.unwrap();
        day_two.update_latest().await.unwrap();
        assert_eq!(
            provider.calls().len(),
            8,
            "same Beijing day is already completed"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn only_latest_current_listings_with_known_dates_are_synchronized(pool: PgPool) {
        let older = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let newer = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        seed_stock_version(&pool, "000001.SZ", "L", Some(date(2020, 1, 1)), None, newer).await;
        seed_stock_version(&pool, "000002.SZ", "L", Some(date(2020, 1, 1)), None, older).await;
        seed_stock_version(
            &pool,
            "000002.SZ",
            "D",
            Some(date(2020, 1, 1)),
            Some(date(2026, 6, 1)),
            newer,
        )
        .await;
        seed_stock_version(&pool, "000003.SZ", "L", None, None, newer).await;
        seed_stock_version(&pool, "000004.SZ", "L", Some(date(2027, 1, 1)), None, newer).await;
        let provider = Arc::new(RecordingProvider::default());
        let service = CompanyIntelligenceService::new_at(pool, provider.clone(), date(2026, 7, 19));

        assert_eq!(service.backfill_dividends().await.unwrap().completed, 1);
        assert!(provider.calls().iter().all(|call| call.code == "000001.SZ"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn empty_results_complete_both_kinds_without_wedging_claims(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2026, 1, 1)).await;
        let provider = Arc::new(RecordingProvider::default());
        // Explicitly exercise configured empty responses rather than relying only on defaults.
        provider.return_financials("000001.SZ", Vec::new());
        provider.return_dividends("000001.SZ", Vec::new());
        let service = CompanyIntelligenceService::new_at(pool.clone(), provider, date(2026, 7, 19));

        assert_eq!(service.backfill_financials().await.unwrap().completed, 1);
        assert_eq!(service.backfill_dividends().await.unwrap().completed, 1);
        let repo = CompanyRepository::new(pool);
        for phase in ["financials", "dividends"] {
            let checkpoint = repo
                .checkpoint_window(phase, "000001.SZ", date(2026, 1, 1), date(2026, 7, 19))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(checkpoint.status, "completed");
            assert!(checkpoint.lease_token.is_none());
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dividend_storage_failure_rolls_back_and_does_not_change_financial_completion(
        pool: PgPool,
    ) {
        seed_current_stock(&pool, "000001.SZ", date(2026, 1, 1)).await;
        let provider = Arc::new(RecordingProvider::default());
        provider.return_dividends(
            "000001.SZ",
            vec![
                dividend_record("000001.SZ", "test", "valid-before-error"),
                dividend_record("000001.SZ", "", "invalid-source"),
            ],
        );
        let service = CompanyIntelligenceService::new_at(pool.clone(), provider, date(2026, 7, 19));

        assert_eq!(service.backfill_financials().await.unwrap().completed, 1);
        assert!(service.backfill_dividends().await.is_err());
        let stored: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM stock_dividend_versions WHERE code = '000001.SZ'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, 0);
        let repo = CompanyRepository::new(pool);
        assert_eq!(
            repo.checkpoint_window(
                "financials",
                "000001.SZ",
                date(2026, 1, 1),
                date(2026, 7, 19),
            )
            .await
            .unwrap()
            .unwrap()
            .status,
            "completed"
        );
        let failed = repo
            .checkpoint_window(
                "dividends",
                "000001.SZ",
                date(2026, 1, 1),
                date(2026, 7, 19),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, "failed");
        assert!(failed.lease_token.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn latest_continues_dividends_when_financials_fail(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2026, 1, 1)).await;
        let provider = Arc::new(RecordingProvider::default());
        provider.fail_financials("000001.SZ", 1);
        let service =
            CompanyIntelligenceService::new_at(pool.clone(), provider.clone(), date(2026, 7, 19));

        let error = service.update_latest().await.unwrap_err();
        assert!(error.to_string().contains("financials=CompanySyncReport"));
        assert!(provider
            .calls()
            .iter()
            .any(|call| call.kind == CallKind::Dividends));
        let repo = CompanyRepository::new(pool);
        assert_eq!(
            repo.checkpoint_window(
                "financials_latest",
                "000001.SZ",
                date(2026, 1, 1),
                date(2026, 12, 31),
            )
            .await
            .unwrap()
            .unwrap()
            .status,
            "failed"
        );
        assert_eq!(
            repo.checkpoint_window(
                "dividends_latest",
                "000001.SZ",
                date(2026, 1, 1),
                date(2026, 12, 31),
            )
            .await
            .unwrap()
            .unwrap()
            .status,
            "completed"
        );
    }
}
