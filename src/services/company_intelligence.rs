use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use sqlx::PgPool;

use crate::data::company::CompanyDataProvider;
use crate::error::{AppError, Result};
use crate::market_time::{beijing_today, beijing_tz};
use crate::storage::company_repository::{
    CheckpointClaimOutcome, CheckpointClaimPolicy, CheckpointLease, CompanyRepository,
};

const FINANCIAL_BACKFILL_PHASE: &str = "financials";
const DIVIDEND_BACKFILL_PHASE: &str = "dividends";
const FINANCIAL_LATEST_PHASE: &str = "financials_latest";
const DIVIDEND_LATEST_PHASE: &str = "dividends_latest";
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(15 * 60);

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
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use sqlx::PgPool;

    use super::CompanyIntelligenceService;
    use crate::data::company::{
        CompanyDataProvider, DividendRecord, FinancialFrequency, FinancialReport,
    };
    use crate::error::{AppError, Result};
    use crate::storage::company_repository::CompanyRepository;

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
