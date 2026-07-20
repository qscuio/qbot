use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc, Weekday};
use sqlx::{PgPool, Postgres, QueryBuilder};

use crate::analysis::market_snapshot::{AdjustmentFactor, AvailabilityQuality, DailyBasicSnapshot};
use crate::data::point_in_time_provider::PointInTimeDataProvider;
use crate::error::{AppError, Result};
use crate::storage::company_repository::{CheckpointClaimOutcome, CompanyRepository};

const CHIP_PREREQUISITE_PHASE: &str = "chip_prerequisites_v1";
const CHIP_PREREQUISITE_CODE: &str = "__market__";
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChipPrerequisiteRepairReport {
    pub completed_dates: usize,
    pub skipped_dates: usize,
    pub turnover_rows: usize,
    pub factor_rows: usize,
    pub market_value_rows: usize,
}

pub struct ChipPrerequisiteRepairService {
    pool: PgPool,
    provider: Arc<dyn PointInTimeDataProvider>,
    today: NaiveDate,
    lease_ttl: Duration,
}

impl ChipPrerequisiteRepairService {
    pub fn new(pool: PgPool, provider: Arc<dyn PointInTimeDataProvider>) -> Self {
        Self::new_at(pool, provider, crate::market_time::beijing_today())
    }

    pub(crate) fn new_at(
        pool: PgPool,
        provider: Arc<dyn PointInTimeDataProvider>,
        today: NaiveDate,
    ) -> Self {
        Self {
            pool,
            provider,
            today,
            lease_ttl: DEFAULT_LEASE_TTL,
        }
    }

    pub async fn repair(&self) -> Result<ChipPrerequisiteRepairReport> {
        let dates: Vec<NaiveDate> = sqlx::query_scalar(
            r#"SELECT DISTINCT bars.trade_date
               FROM stock_daily_bars bars
               WHERE bars.trade_date <= $1
               ORDER BY bars.trade_date"#,
        )
        .bind(self.today)
        .fetch_all(&self.pool)
        .await?;
        let repository = CompanyRepository::new(self.pool.clone());
        let missing_market_values = self.missing_market_value_codes().await?;
        let first_unfinished = self.first_unfinished_date(&dates).await?;
        let mut factor_state = match first_unfinished {
            Some(date) => self.factor_state_before(date).await?,
            None => BTreeMap::new(),
        };
        let mut report = ChipPrerequisiteRepairReport::default();

        for trade_date in dates {
            let claim = repository
                .claim_checkpoint_window(
                    CHIP_PREREQUISITE_PHASE,
                    CHIP_PREREQUISITE_CODE,
                    trade_date,
                    trade_date,
                    self.lease_ttl,
                )
                .await?;
            let lease = match claim {
                CheckpointClaimOutcome::Completed => {
                    report.skipped_dates += 1;
                    continue;
                }
                CheckpointClaimOutcome::Busy => {
                    return Err(AppError::DataProvider(format!(
                        "chip prerequisite date {trade_date} is owned by another repair worker"
                    )));
                }
                CheckpointClaimOutcome::Claimed(lease) => lease,
            };

            let result = self
                .repair_claimed_date(
                    &repository,
                    &lease,
                    trade_date,
                    &missing_market_values,
                    &factor_state,
                )
                .await;
            match result {
                Ok(outcome) => {
                    report.completed_dates += 1;
                    report.turnover_rows += outcome.turnover_rows;
                    report.factor_rows += outcome.factor_rows;
                    report.market_value_rows += outcome.market_value_rows;
                    factor_state.extend(outcome.factor_state);
                }
                Err(error) => {
                    let detail = error.to_string();
                    let release = repository.fail_checkpoint(&lease, &detail).await;
                    return Err(match release {
                        Ok(()) => error,
                        Err(release) => AppError::DataProvider(format!(
                            "{detail}; prerequisite checkpoint release failed: {release}"
                        )),
                    });
                }
            }
        }

        Ok(report)
    }

    async fn first_unfinished_date(&self, dates: &[NaiveDate]) -> Result<Option<NaiveDate>> {
        if dates.is_empty() {
            return Ok(None);
        }
        sqlx::query_scalar(
            r#"SELECT MIN(date_value)
               FROM UNNEST($1::date[]) dates(date_value)
               LEFT JOIN company_data_repair_checkpoints checkpoint
                 ON checkpoint.phase = $2
                AND checkpoint.code = $3
                AND checkpoint.start_date = dates.date_value
                AND checkpoint.end_date = dates.date_value
                AND checkpoint.status = 'completed'
               WHERE checkpoint.phase IS NULL"#,
        )
        .bind(dates)
        .bind(CHIP_PREREQUISITE_PHASE)
        .bind(CHIP_PREREQUISITE_CODE)
        .fetch_one(&self.pool)
        .await
        .map_err(AppError::from)
    }

    async fn missing_market_value_codes(&self) -> Result<BTreeSet<String>> {
        Ok(sqlx::query_scalar(
            r#"WITH latest_master AS (
                   SELECT DISTINCT ON (code) code, list_status, list_date, delist_date
                   FROM security_master_versions
                   ORDER BY code, available_at DESC, ingested_at DESC, source DESC
               ), latest_value AS (
                   SELECT DISTINCT code
                   FROM stock_daily_basic_versions
                   WHERE total_mv IS NOT NULL AND total_mv >= 0
               )
               SELECT master.code
               FROM latest_master master
               LEFT JOIN latest_value value USING (code)
               WHERE master.list_status = 'L'
                 AND master.list_date <= $1
                 AND (master.delist_date IS NULL OR master.delist_date > $1)
                 AND value.code IS NULL
               ORDER BY master.code"#,
        )
        .bind(self.today)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect())
    }

    async fn factor_state_before(&self, trade_date: NaiveDate) -> Result<BTreeMap<String, f64>> {
        Ok(sqlx::query_as::<_, (String, f64)>(
            r#"SELECT DISTINCT ON (code) code, adj_factor::float8
               FROM stock_adjustment_factors
               WHERE trade_date < $1
               ORDER BY code, trade_date DESC, available_at DESC, ingested_at DESC"#,
        )
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect())
    }

    async fn repair_claimed_date(
        &self,
        repository: &CompanyRepository,
        lease: &crate::storage::company_repository::CheckpointLease,
        trade_date: NaiveDate,
        missing_market_values: &BTreeSet<String>,
        factor_state: &BTreeMap<String, f64>,
    ) -> Result<DateRepairOutcome> {
        let required_codes: Vec<String> = sqlx::query_scalar(
            "SELECT code FROM stock_daily_bars WHERE trade_date = $1 ORDER BY code",
        )
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await?;
        let missing_turnover_codes: BTreeSet<String> = sqlx::query_scalar(
            "SELECT code FROM stock_daily_bars WHERE trade_date = $1 AND turnover IS NULL ORDER BY code",
        )
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect();

        let daily_basics = self.provider.get_daily_basics(trade_date).await?;
        let daily_by_code = validate_daily_basics(trade_date, daily_basics)?;
        ensure_daily_turnover_coverage(
            "daily_basic",
            trade_date,
            &missing_turnover_codes,
            &daily_by_code,
        )?;

        let adjustment_factors = self
            .provider
            .get_adjustment_factors(trade_date, trade_date)
            .await?;
        let factor_by_code = validate_adjustment_factors(trade_date, adjustment_factors)?;
        let required_factor_codes = required_codes.into_iter().collect::<BTreeSet<_>>();
        ensure_coverage(
            "adjustment_factor",
            trade_date,
            &required_factor_codes,
            &factor_by_code,
        )?;

        let observed_at = Utc::now();
        let available_at = conservative_estimated_availability(trade_date);
        let turnover_rows = missing_turnover_codes
            .iter()
            .map(|code| {
                let row = &daily_by_code[code];
                (code.clone(), row.turnover_rate.expect("validated turnover"))
            })
            .collect::<Vec<_>>();
        let market_value_rows = daily_by_code
            .values()
            .filter(|row| {
                missing_market_values.contains(&row.code)
                    && row
                        .total_mv
                        .is_some_and(|value| value.is_finite() && value >= 0.0)
            })
            .cloned()
            .map(|mut row| {
                row.available_at = available_at;
                row.ingested_at = observed_at;
                row.availability_quality = AvailabilityQuality::Estimated;
                row
            })
            .collect::<Vec<_>>();
        let changed_factors = factor_by_code
            .values()
            .filter(|row| {
                factor_state
                    .get(&row.code)
                    .is_none_or(|previous| (previous - row.adj_factor).abs() > 1e-12)
            })
            .cloned()
            .map(|mut row| {
                row.available_at = available_at;
                row.ingested_at = observed_at;
                row.availability_quality = AvailabilityQuality::Estimated;
                row
            })
            .collect::<Vec<_>>();

        let mut transaction = self.pool.begin().await?;
        let updated = update_turnover(&mut transaction, trade_date, &turnover_rows).await?;
        if updated != turnover_rows.len() as u64 {
            return Err(AppError::DataProvider(format!(
                "chip prerequisite turnover update changed {updated} rows; expected {} on {trade_date}",
                turnover_rows.len()
            )));
        }
        insert_daily_basics(&mut transaction, &market_value_rows).await?;
        insert_adjustment_factors(&mut transaction, &changed_factors).await?;
        repository
            .complete_checkpoint_in_transaction(&mut transaction, lease)
            .await?;
        transaction.commit().await?;

        Ok(DateRepairOutcome {
            turnover_rows: turnover_rows.len(),
            market_value_rows: market_value_rows.len(),
            factor_rows: changed_factors.len(),
            factor_state: factor_by_code
                .into_iter()
                .map(|(code, row)| (code, row.adj_factor))
                .collect(),
        })
    }
}

struct DateRepairOutcome {
    turnover_rows: usize,
    factor_rows: usize,
    market_value_rows: usize,
    factor_state: BTreeMap<String, f64>,
}

fn validate_daily_basics(
    trade_date: NaiveDate,
    rows: Vec<DailyBasicSnapshot>,
) -> Result<BTreeMap<String, DailyBasicSnapshot>> {
    let mut by_code = BTreeMap::new();
    for row in rows {
        if row.trade_date != trade_date {
            return Err(AppError::DataProvider(format!(
                "daily_basic returned {} for requested {trade_date}",
                row.trade_date
            )));
        }
        if row
            .turnover_rate
            .is_some_and(|value| !value.is_finite() || !(0.0..=100.0).contains(&value))
        {
            return Err(AppError::DataProvider(format!(
                "daily_basic has invalid turnover for {} on {trade_date}",
                row.code
            )));
        }
        let code = row.code.clone();
        if by_code.insert(code.clone(), row).is_some() {
            return Err(AppError::DataProvider(format!(
                "daily_basic returned duplicate {code} on {trade_date}"
            )));
        }
    }
    Ok(by_code)
}

fn validate_adjustment_factors(
    trade_date: NaiveDate,
    rows: Vec<AdjustmentFactor>,
) -> Result<BTreeMap<String, AdjustmentFactor>> {
    let mut by_code = BTreeMap::new();
    for row in rows {
        if row.trade_date != trade_date || !row.adj_factor.is_finite() || row.adj_factor <= 0.0 {
            return Err(AppError::DataProvider(format!(
                "adjustment_factor returned invalid row for {} on requested {trade_date}",
                row.code
            )));
        }
        let code = row.code.clone();
        if by_code.insert(code.clone(), row).is_some() {
            return Err(AppError::DataProvider(format!(
                "adjustment_factor returned duplicate {code} on {trade_date}"
            )));
        }
    }
    Ok(by_code)
}

fn ensure_coverage<T>(
    dataset: &str,
    trade_date: NaiveDate,
    required: &BTreeSet<String>,
    actual: &BTreeMap<String, T>,
) -> Result<()> {
    let missing = required
        .iter()
        .filter(|code| !actual.contains_key(*code))
        .take(8)
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(AppError::DataProvider(format!(
            "{dataset} coverage is incomplete on {trade_date}: missing {}",
            missing.join(",")
        )))
    }
}

fn ensure_daily_turnover_coverage(
    dataset: &str,
    trade_date: NaiveDate,
    required: &BTreeSet<String>,
    actual: &BTreeMap<String, DailyBasicSnapshot>,
) -> Result<()> {
    let missing = required
        .iter()
        .filter(|code| {
            actual
                .get(*code)
                .and_then(|row| row.turnover_rate)
                .is_none()
        })
        .take(8)
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(AppError::DataProvider(format!(
            "{dataset} turnover coverage is incomplete on {trade_date}: missing {}",
            missing.join(",")
        )))
    }
}

fn conservative_estimated_availability(source_date: NaiveDate) -> DateTime<Utc> {
    let mut next = source_date + ChronoDuration::days(1);
    while matches!(next.weekday(), Weekday::Sat | Weekday::Sun) {
        next += ChronoDuration::days(1);
    }
    Utc.with_ymd_and_hms(next.year(), next.month(), next.day(), 1, 0, 0)
        .single()
        .expect("09:00 Asia/Shanghai maps to valid UTC")
}

async fn update_turnover(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    trade_date: NaiveDate,
    rows: &[(String, f64)],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let codes = rows
        .iter()
        .map(|(code, _)| code.clone())
        .collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    Ok(sqlx::query(
        r#"UPDATE stock_daily_bars bars
           SET turnover = input.turnover
           FROM UNNEST($1::text[], $2::float8[]) input(code, turnover)
           WHERE bars.code = input.code
             AND bars.trade_date = $3
             AND bars.turnover IS NULL"#,
    )
    .bind(&codes)
    .bind(&values)
    .bind(trade_date)
    .execute(&mut **transaction)
    .await?
    .rows_affected())
}

async fn insert_daily_basics(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[DailyBasicSnapshot],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut query = QueryBuilder::new(
        r#"INSERT INTO stock_daily_basic_versions
           (code, trade_date, turnover_rate, volume_ratio, pe, pb, ps,
            total_share, float_share, total_mv, circ_mv, available_at,
            ingested_at, availability_quality, source) "#,
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(&row.code)
            .push_bind(row.trade_date)
            .push_bind(row.turnover_rate)
            .push_bind(row.volume_ratio)
            .push_bind(row.pe)
            .push_bind(row.pb)
            .push_bind(row.ps)
            .push_bind(row.total_share)
            .push_bind(row.float_share)
            .push_bind(row.total_mv)
            .push_bind(row.circ_mv)
            .push_bind(row.available_at)
            .push_bind(row.ingested_at)
            .push_bind("estimated")
            .push_bind(&row.source);
    });
    query.push(" ON CONFLICT (code, trade_date, available_at) DO NOTHING");
    query.build().execute(&mut **transaction).await?;
    Ok(())
}

async fn insert_adjustment_factors(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[AdjustmentFactor],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut query = QueryBuilder::new(
        r#"INSERT INTO stock_adjustment_factors
           (code, trade_date, adj_factor, available_at, ingested_at,
            availability_quality, source) "#,
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(&row.code)
            .push_bind(row.trade_date)
            .push_bind(row.adj_factor)
            .push_bind(row.available_at)
            .push_bind(row.ingested_at)
            .push_bind("estimated")
            .push_bind(&row.source);
    });
    query.push(" ON CONFLICT (code, trade_date, available_at) DO NOTHING");
    query.build().execute(&mut **transaction).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
    use sqlx::PgPool;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use crate::analysis::market_snapshot::{
        AdjustmentFactor, AvailabilityQuality, CorporateAction, DailyBasicSnapshot, IndexDailyBar,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
    };
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::error::Result;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn timestamp(day: NaiveDate) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(day.year(), day.month(), day.day(), 10, 0, 0)
            .unwrap()
    }

    #[derive(Default)]
    struct RecordingProvider {
        calls: Mutex<Vec<(String, NaiveDate)>>,
        omit_daily_code: Option<&'static str>,
    }

    impl RecordingProvider {
        fn calls(&self) -> Vec<(String, NaiveDate)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PointInTimeDataProvider for RecordingProvider {
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

        async fn get_daily_basics(&self, trade_date: NaiveDate) -> Result<Vec<DailyBasicSnapshot>> {
            self.calls
                .lock()
                .unwrap()
                .push(("daily_basic".into(), trade_date));
            Ok(["000001.SZ", "000002.SZ"]
                .into_iter()
                .filter(|code| Some(*code) != self.omit_daily_code)
                .map(|code| DailyBasicSnapshot {
                    code: code.into(),
                    trade_date,
                    turnover_rate: Some(if code == "000001.SZ" { 3.0 } else { 5.0 }),
                    volume_ratio: None,
                    pe: None,
                    pb: None,
                    ps: None,
                    total_share: None,
                    float_share: None,
                    total_mv: Some(if code == "000001.SZ" { 100.0 } else { 200.0 }),
                    circ_mv: None,
                    available_at: timestamp(trade_date),
                    ingested_at: timestamp(trade_date),
                    availability_quality: AvailabilityQuality::Observed,
                    source: "test".into(),
                })
                .collect())
        }

        async fn get_adjustment_factors(
            &self,
            start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<AdjustmentFactor>> {
            self.calls
                .lock()
                .unwrap()
                .push(("adjustment_factor".into(), start));
            let changed = start == date(2026, 1, 3);
            Ok(["000001.SZ", "000002.SZ"]
                .into_iter()
                .map(|code| AdjustmentFactor {
                    code: code.into(),
                    trade_date: start,
                    adj_factor: if changed && code == "000001.SZ" {
                        2.0
                    } else {
                        1.0
                    },
                    available_at: timestamp(start),
                    ingested_at: timestamp(start),
                    availability_quality: AvailabilityQuality::Observed,
                    source: "test".into(),
                })
                .collect())
        }

        async fn get_security_master_versions(&self) -> Result<Vec<SecurityMasterVersion>> {
            unreachable!()
        }
        async fn get_corporate_actions(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<CorporateAction>> {
            unreachable!()
        }
        async fn get_security_statuses(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<SecurityDailyStatus>> {
            unreachable!()
        }
        async fn get_index_daily_range(
            &self,
            _codes: &[String],
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<IndexDailyBar>> {
            unreachable!()
        }
        async fn get_sector_memberships(
            &self,
            _as_of_date: NaiveDate,
        ) -> Result<Vec<SectorMembership>> {
            unreachable!()
        }
    }

    async fn seed(pool: &PgPool) {
        for code in ["000001.SZ", "000002.SZ"] {
            sqlx::query(
                r#"INSERT INTO security_master_versions
                   (code, name, market, exchange, list_status, list_date,
                    available_at, availability_quality, source)
                   VALUES ($1, $1, 'A', 'SZ', 'L', DATE '2020-01-01',
                           NOW(), 'observed', 'test')"#,
            )
            .bind(code)
            .execute(pool)
            .await
            .unwrap();
            for trade_date in [date(2026, 1, 2), date(2026, 1, 3)] {
                sqlx::query(
                    r#"INSERT INTO stock_daily_bars
                       (code, trade_date, open, high, low, close, volume, turnover)
                       VALUES ($1, $2, 10, 11, 9, 10, 1000, NULL)"#,
                )
                .bind(code)
                .bind(trade_date)
                .execute(pool)
                .await
                .unwrap();
            }
        }
        sqlx::query(
            r#"INSERT INTO stock_daily_basic_versions
               (code, trade_date, turnover_rate, total_mv, available_at,
                availability_quality, source)
               VALUES ('000001.SZ', DATE '2026-01-01', 1, 50, NOW(), 'observed', 'test')"#,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn repair_fills_turnover_market_value_and_sparse_adjustment_history(pool: PgPool) {
        seed(&pool).await;
        let provider = Arc::new(RecordingProvider::default());
        let service =
            ChipPrerequisiteRepairService::new_at(pool.clone(), provider.clone(), date(2026, 1, 3));

        let report = service.repair().await.unwrap();

        assert_eq!(report.completed_dates, 2);
        assert_eq!(report.turnover_rows, 4);
        assert_eq!(report.factor_rows, 3, "two baselines plus one change");
        let missing_turnover: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM stock_daily_bars WHERE turnover IS NULL")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(missing_turnover, 0);
        let missing_market_value: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM (VALUES ('000001.SZ'), ('000002.SZ')) codes(code)
               WHERE NOT EXISTS (
                   SELECT 1 FROM stock_daily_basic_versions daily
                   WHERE daily.code = codes.code AND daily.total_mv IS NOT NULL
               )"#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(missing_market_value, 0);
        let factors: Vec<(String, NaiveDate, f64)> = sqlx::query_as(
            "SELECT code, trade_date, adj_factor::float8 FROM stock_adjustment_factors ORDER BY code, trade_date",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(factors.len(), 3);

        let second = service.repair().await.unwrap();
        assert_eq!(second.completed_dates, 0);
        assert_eq!(second.skipped_dates, 2);
        assert_eq!(provider.calls().len(), 4, "completed dates do not refetch");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn incomplete_provider_day_is_failed_without_partial_database_writes(pool: PgPool) {
        seed(&pool).await;
        let provider = Arc::new(RecordingProvider {
            omit_daily_code: Some("000002.SZ"),
            ..Default::default()
        });
        let service =
            ChipPrerequisiteRepairService::new_at(pool.clone(), provider, date(2026, 1, 3));

        let error = service.repair().await.unwrap_err().to_string();

        assert!(error.contains("daily_basic turnover coverage is incomplete"));
        assert!(error.contains("000002.SZ"));
        let repaired: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM stock_daily_bars WHERE turnover IS NOT NULL")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(repaired, 0);
        let checkpoint: (String, bool) = sqlx::query_as(
            r#"SELECT status, lease_token IS NULL
               FROM company_data_repair_checkpoints
               WHERE phase = 'chip_prerequisites_v1'"#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(checkpoint, ("failed".into(), true));
    }
}
