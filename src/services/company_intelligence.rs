use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use sqlx::PgPool;

use crate::data::company::CompanyDataProvider;
use crate::error::{AppError, Result};
use crate::market_time::beijing_today;
use crate::storage::company_repository::CompanyRepository;

const FINANCIAL_BACKFILL_PHASE: &str = "financials";
const DIVIDEND_BACKFILL_PHASE: &str = "dividends";
const FINANCIAL_LATEST_PHASE: &str = "financials_latest";
const DIVIDEND_LATEST_PHASE: &str = "dividends_latest";

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
        }
    }

    pub async fn backfill_financials(&self) -> Result<CompanySyncReport> {
        self.finish(
            Dataset::Financials,
            self.synchronize(Dataset::Financials, FINANCIAL_BACKFILL_PHASE, true, false)
                .await?,
        )
    }

    pub async fn backfill_dividends(&self) -> Result<CompanySyncReport> {
        self.finish(
            Dataset::Dividends,
            self.synchronize(Dataset::Dividends, DIVIDEND_BACKFILL_PHASE, true, false)
                .await?,
        )
    }

    pub async fn update_latest(&self) -> Result<CompanyIntelligenceUpdateReport> {
        let financials = self
            .synchronize(Dataset::Financials, FINANCIAL_LATEST_PHASE, false, true)
            .await;
        let dividends = self
            .synchronize(Dataset::Dividends, DIVIDEND_LATEST_PHASE, false, true)
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
        skip_completed: bool,
        latest_only: bool,
    ) -> Result<SyncOutcome> {
        let stocks = self.current_stocks().await?;
        let mut outcome = SyncOutcome::default();

        for stock in stocks {
            if skip_completed
                && self
                    .repository
                    .checkpoint(phase, &stock.code)
                    .await?
                    .is_some_and(|checkpoint| checkpoint.status == "completed")
            {
                outcome.report.completed += 1;
                continue;
            }

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
            let checkpoint_start = windows.first().map(|window| window.0);
            let checkpoint_end = windows.last().map(|window| window.1);
            let lease = match self
                .repository
                .claim_checkpoint(phase, &stock.code, checkpoint_start, checkpoint_end)
                .await
            {
                Ok(lease) => lease,
                Err(AppError::BadRequest(message)) if message.contains("already running") => {
                    outcome.report.pending += 1;
                    continue;
                }
                Err(error) => {
                    outcome.report.failed += 1;
                    outcome.errors.push(format!(
                        "{}/{}/claim: {error}",
                        dataset.label(),
                        stock.code
                    ));
                    continue;
                }
            };

            let work = async {
                for (start, end) in windows {
                    match dataset {
                        Dataset::Financials => {
                            let reports = self
                                .provider
                                .financial_reports(&stock.code, start, end)
                                .await?;
                            self.repository.upsert_financial_reports(&reports).await?;
                        }
                        Dataset::Dividends => {
                            let records = self.provider.dividends(&stock.code, start, end).await?;
                            self.repository.upsert_dividends(&records).await?;
                        }
                    }
                }
                Result::<()>::Ok(())
            }
            .await;

            if let Err(error) = work {
                outcome.report.failed += 1;
                let release = self
                    .repository
                    .fail_checkpoint(&lease, &error.to_string())
                    .await;
                let release_suffix = release
                    .err()
                    .map(|release_error| format!("; checkpoint release failed: {release_error}"))
                    .unwrap_or_default();
                outcome.errors.push(format!(
                    "{}/{}: {error}{release_suffix}",
                    dataset.label(),
                    stock.code
                ));
                continue;
            }

            match self.repository.complete_checkpoint(&lease).await {
                Ok(()) => outcome.report.completed += 1,
                Err(error) => {
                    outcome.report.failed += 1;
                    let release = self
                        .repository
                        .fail_checkpoint(&lease, &error.to_string())
                        .await;
                    let release_suffix = release
                        .err()
                        .map(|release_error| {
                            format!("; checkpoint release failed: {release_error}")
                        })
                        .unwrap_or_default();
                    outcome.errors.push(format!(
                        "{}/{}/complete: {error}{release_suffix}",
                        dataset.label(),
                        stock.code
                    ));
                }
            }
        }

        Ok(outcome)
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
            repo.checkpoint("financials", "000002.SZ")
                .await
                .unwrap()
                .unwrap()
                .status,
            "completed"
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
            .checkpoint("financials", "000001.SZ")
            .await
            .unwrap()
            .unwrap();
        let dividends = repo
            .checkpoint("dividends", "000001.SZ")
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
            .checkpoint("financials", "000002.SZ")
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
            .checkpoint("financials", "000001.SZ")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.status, "failed");
        assert!(checkpoint.lease_token.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn active_checkpoint_ownership_is_pending_and_never_stolen(pool: PgPool) {
        seed_current_stock(&pool, "000001.SZ", date(2026, 1, 1)).await;
        let repo = CompanyRepository::new(pool.clone());
        let owner = repo
            .claim_checkpoint(
                "financials",
                "000001.SZ",
                Some(date(2026, 1, 1)),
                Some(date(2026, 7, 19)),
            )
            .await
            .unwrap();
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
        assert!(provider.calls().is_empty());
        assert_eq!(
            repo.checkpoint("financials", "000001.SZ")
                .await
                .unwrap()
                .unwrap()
                .lease_token,
            Some(owner.token)
        );
        repo.fail_checkpoint(&owner, "test cleanup").await.unwrap();
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

        service.update_latest().await.unwrap();
        assert_eq!(
            provider.calls().len(),
            12,
            "latest refreshes revisions again"
        );
        let repo = CompanyRepository::new(pool);
        assert_eq!(
            repo.checkpoint("financials_latest", "000001.SZ")
                .await
                .unwrap()
                .unwrap()
                .attempts,
            2
        );
        assert_eq!(
            repo.checkpoint("dividends_latest", "000001.SZ")
                .await
                .unwrap()
                .unwrap()
                .attempts,
            2
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
            let checkpoint = repo.checkpoint(phase, "000001.SZ").await.unwrap().unwrap();
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
            repo.checkpoint("financials", "000001.SZ")
                .await
                .unwrap()
                .unwrap()
                .status,
            "completed"
        );
        let failed = repo
            .checkpoint("dividends", "000001.SZ")
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
            repo.checkpoint("financials_latest", "000001.SZ")
                .await
                .unwrap()
                .unwrap()
                .status,
            "failed"
        );
        assert_eq!(
            repo.checkpoint("dividends_latest", "000001.SZ")
                .await
                .unwrap()
                .unwrap()
                .status,
            "completed"
        );
    }
}
