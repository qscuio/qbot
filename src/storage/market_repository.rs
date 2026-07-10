use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::analysis::market_snapshot::{
    AdjustmentFactor, AvailabilityQuality, CorporateAction, DailyBasicSnapshot, IndexDailyBar,
    MarketSnapshot, SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
};
use crate::data::point_in_time_provider::PointInTimeCapabilities;
use crate::data::types::{Candle, LimitUpStock, SectorData};
use crate::error::Result;

pub const POINT_IN_TIME_CAPABILITY_PROBE_RUN_TYPE: &str = "point_in_time_capability_probe";
pub const POINT_IN_TIME_REFERENCE_REFRESH_RUN_TYPE: &str = "point_in_time_reference_refresh";
pub const POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE: &str = "point_in_time_trade_date_refresh";
pub const POINT_IN_TIME_BACKFILL_RUN_TYPE: &str = "point_in_time_backfill";

#[derive(Debug, Clone)]
pub struct AnalysisDataStatus {
    pub run_type: String,
    pub status: String,
    pub details: Value,
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct MarketRepository {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct PointInTimeDailyBarVersion {
    pub code: String,
    pub bar: Candle,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub source: String,
}

impl MarketRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn persist_point_in_time_capability_probe(
        &self,
        result: &crate::error::Result<PointInTimeCapabilities>,
    ) -> Result<()> {
        let (status, details, error_message) = match result {
            Ok(capabilities) => {
                let missing: Vec<&str> = [
                    (
                        "security_master_history",
                        capabilities.security_master_history,
                    ),
                    ("corporate_actions", capabilities.corporate_actions),
                    ("adjustment_factors", capabilities.adjustment_factors),
                    ("daily_basic", capabilities.daily_basic),
                    ("daily_security_status", capabilities.daily_security_status),
                    ("historical_index_bars", capabilities.historical_index_bars),
                    (
                        "historical_sector_membership",
                        capabilities.historical_sector_membership,
                    ),
                ]
                .into_iter()
                .filter_map(|(name, supported)| (!supported).then_some(name))
                .collect();
                let status = if missing.is_empty() { "ok" } else { "missing" };
                let details = serde_json::json!({
                    "capabilities": capabilities,
                    "missing_capabilities": missing,
                });
                (status, details, None)
            }
            Err(error) => (
                "failed",
                serde_json::json!({
                    "missing_capabilities": ["point_in_time_capability_probe"],
                    "error": error.to_string(),
                }),
                Some(error.to_string()),
            ),
        };

        sqlx::query(
            r#"INSERT INTO analysis_data_runs
               (run_id, run_type, status, details, error_message, completed_at)
               VALUES ($1, $2, $3, $4, $5, NOW())"#,
        )
        .bind(Uuid::new_v4())
        .bind(POINT_IN_TIME_CAPABILITY_PROBE_RUN_TYPE)
        .bind(status)
        .bind(details)
        .bind(error_message)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn latest_point_in_time_data_status(&self) -> Result<Option<AnalysisDataStatus>> {
        let row = sqlx::query_as::<
            _,
            (
                String,
                String,
                Value,
                Option<String>,
                DateTime<Utc>,
                Option<DateTime<Utc>>,
            ),
        >(
            r#"SELECT run_type, status, details, error_message, started_at, completed_at
               FROM analysis_data_runs
               WHERE run_type = $1
               ORDER BY started_at DESC
               LIMIT 1"#,
        )
        .bind(POINT_IN_TIME_CAPABILITY_PROBE_RUN_TYPE)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(
            |(run_type, status, details, error_message, started_at, completed_at)| {
                AnalysisDataStatus {
                    run_type,
                    status,
                    details,
                    error_message,
                    started_at,
                    completed_at,
                }
            },
        ))
    }

    pub async fn record_analysis_data_run(
        &self,
        run_type: &str,
        trade_date: Option<NaiveDate>,
        status: &str,
        details: Value,
        error_message: Option<String>,
    ) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO analysis_data_runs
               (run_id, run_type, trade_date, status, details, error_message, completed_at)
               VALUES ($1, $2, $3, $4, $5, $6, NOW())"#,
        )
        .bind(Uuid::new_v4())
        .bind(run_type)
        .bind(trade_date)
        .bind(status)
        .bind(details)
        .bind(error_message)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn latest_security_master_payload_unchanged(
        &self,
        row: &SecurityMasterVersion,
    ) -> Result<bool> {
        let (unchanged,): (bool,) = sqlx::query_as(
            r#"SELECT COALESCE((
                   SELECT name = $2
                      AND market IS NOT DISTINCT FROM $3
                      AND exchange IS NOT DISTINCT FROM $4
                      AND list_status = $5
                      AND list_date IS NOT DISTINCT FROM $6
                      AND delist_date IS NOT DISTINCT FROM $7
                      AND source = $8
                   FROM security_master_versions
                   WHERE code = $1
                   ORDER BY available_at DESC
                   LIMIT 1
               ), FALSE)"#,
        )
        .bind(&row.code)
        .bind(&row.name)
        .bind(&row.market)
        .bind(&row.exchange)
        .bind(&row.list_status)
        .bind(row.list_date)
        .bind(row.delist_date)
        .bind(&row.source)
        .fetch_one(&self.pool)
        .await?;

        Ok(unchanged)
    }

    pub async fn latest_corporate_action_payload_unchanged(
        &self,
        row: &CorporateAction,
    ) -> Result<bool> {
        let (unchanged,): (bool,) = sqlx::query_as(
            r#"SELECT COALESCE((
                   SELECT code = $3
                      AND action_type = $4
                      AND announcement_date IS NOT DISTINCT FROM $5
                      AND record_date IS NOT DISTINCT FROM $6
                      AND ex_date IS NOT DISTINCT FROM $7
                      AND pay_date IS NOT DISTINCT FROM $8
                      AND cash_dividend::float8 IS NOT DISTINCT FROM $9
                      AND stock_ratio::float8 IS NOT DISTINCT FROM $10
                      AND rights_ratio::float8 IS NOT DISTINCT FROM $11
                      AND rights_price::float8 IS NOT DISTINCT FROM $12
                   FROM corporate_action_versions
                   WHERE source = $1
                     AND action_key = $2
                   ORDER BY available_at DESC
                   LIMIT 1
               ), FALSE)"#,
        )
        .bind(&row.source)
        .bind(&row.action_key)
        .bind(&row.code)
        .bind(&row.action_type)
        .bind(row.announcement_date)
        .bind(row.record_date)
        .bind(row.ex_date)
        .bind(row.pay_date)
        .bind(row.cash_dividend)
        .bind(row.stock_ratio)
        .bind(row.rights_ratio)
        .bind(row.rights_price)
        .fetch_one(&self.pool)
        .await?;

        Ok(unchanged)
    }

    pub async fn latest_sector_membership_payload_unchanged(
        &self,
        row: &SectorMembership,
    ) -> Result<bool> {
        let (unchanged,): (bool,) = sqlx::query_as(
            r#"SELECT COALESCE((
                   SELECT sector_name = $4
                      AND sector_type = $5
                      AND valid_to IS NOT DISTINCT FROM $6
                      AND source = $7
                   FROM stock_sector_membership
                   WHERE code = $1
                     AND sector_code = $2
                     AND valid_from = $3
                   ORDER BY available_at DESC
                   LIMIT 1
               ), FALSE)"#,
        )
        .bind(&row.code)
        .bind(&row.sector_code)
        .bind(row.valid_from)
        .bind(&row.sector_name)
        .bind(&row.sector_type)
        .bind(row.valid_to)
        .bind(&row.source)
        .fetch_one(&self.pool)
        .await?;

        Ok(unchanged)
    }

    pub async fn task5_version_writes_exist(
        &self,
        trade_date: NaiveDate,
    ) -> Result<(bool, bool, bool)> {
        let (daily_bars, sector_versions, limit_up_versions): (bool, bool, bool) = sqlx::query_as(
            r#"SELECT
                   EXISTS(SELECT 1 FROM stock_daily_bar_versions WHERE trade_date = $1),
                   EXISTS(SELECT 1 FROM sector_daily_versions WHERE trade_date = $1),
                   EXISTS(SELECT 1 FROM limit_up_stock_versions WHERE trade_date = $1)"#,
        )
        .bind(trade_date)
        .fetch_one(&self.pool)
        .await?;

        Ok((daily_bars, sector_versions, limit_up_versions))
    }

    pub async fn current_daily_bars_for_date(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<(String, Candle)>> {
        let rows: Vec<(
            String,
            NaiveDate,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<i64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, open::float8, high::float8, low::float8,
                      close::float8, volume, amount::float8, turnover::float8,
                      pe::float8, pb::float8
               FROM stock_daily_bars
               WHERE trade_date = $1
               ORDER BY code"#,
        )
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(code, trade_date, open, high, low, close, volume, amount, turnover, pe, pb)| {
                    (
                        code,
                        Candle {
                            trade_date,
                            open: open.unwrap_or(0.0),
                            high: high.unwrap_or(0.0),
                            low: low.unwrap_or(0.0),
                            close: close.unwrap_or(0.0),
                            volume: volume.unwrap_or(0),
                            amount: amount.unwrap_or(0.0),
                            turnover,
                            pe,
                            pb,
                        },
                    )
                },
            )
            .collect())
    }

    pub async fn current_sector_data_for_date(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<SectorData>> {
        let rows: Vec<(
            String,
            Option<String>,
            Option<String>,
            Option<f64>,
            Option<f64>,
            NaiveDate,
        )> = sqlx::query_as(
            r#"SELECT code, name, sector_type, change_pct::float8, amount::float8, trade_date
               FROM sector_daily
               WHERE trade_date = $1
               ORDER BY code"#,
        )
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(code, name, sector_type, change_pct, amount, trade_date)| SectorData {
                    code,
                    name: name.unwrap_or_default(),
                    sector_type: sector_type.unwrap_or_else(|| "unknown".to_string()),
                    change_pct: change_pct.unwrap_or(0.0),
                    amount: amount.unwrap_or(0.0),
                    trade_date,
                },
            )
            .collect())
    }

    pub async fn current_limit_up_stocks_for_date(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<LimitUpStock>> {
        let rows: Vec<(
            String,
            Option<String>,
            NaiveDate,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<String>,
            Option<i32>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT code, name, trade_date, close::float8, pct_chg::float8,
                      seal_amount::float8, limit_time, burst_count, strth::float8
               FROM limit_up_stocks
               WHERE trade_date = $1
               ORDER BY code"#,
        )
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    name,
                    trade_date,
                    close,
                    pct_chg,
                    seal_amount,
                    first_time,
                    open_times,
                    strth,
                )| LimitUpStock {
                    code,
                    name: name.unwrap_or_default(),
                    trade_date,
                    close: close.unwrap_or(0.0),
                    pct_chg: pct_chg.unwrap_or(0.0),
                    fd_amount: seal_amount.unwrap_or(0.0),
                    first_time,
                    last_time: None,
                    open_times: open_times.unwrap_or(0),
                    strth: strth.unwrap_or(0.0),
                    limit: "U".to_string(),
                },
            )
            .collect())
    }

    pub async fn append_daily_bar_versions(
        &self,
        bars: &[(String, Candle)],
        available_at: chrono::DateTime<chrono::Utc>,
        availability_quality: &str,
        source: &str,
    ) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let inserted = Self::append_daily_bar_versions_in_tx(
            &mut tx,
            bars,
            available_at,
            availability_quality,
            source,
        )
        .await?;
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn append_daily_bar_versions_with_ingested_at(
        &self,
        bars: &[(String, Candle)],
        available_at: DateTime<Utc>,
        availability_quality: &str,
        source: &str,
        ingested_at: DateTime<Utc>,
    ) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let mut inserted = 0;
        for (code, bar) in bars {
            inserted += sqlx::query(
                r#"INSERT INTO stock_daily_bar_versions
                   (code, trade_date, open, high, low, close, volume, amount,
                    turnover, pe, pb, available_at, availability_quality, source, ingested_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(code)
            .bind(bar.trade_date)
            .bind(bar.open)
            .bind(bar.high)
            .bind(bar.low)
            .bind(bar.close)
            .bind(bar.volume)
            .bind(bar.amount)
            .bind(bar.turnover)
            .bind(bar.pe)
            .bind(bar.pb)
            .bind(available_at)
            .bind(availability_quality)
            .bind(source)
            .bind(ingested_at)
            .execute(tx.as_mut())
            .await?
            .rows_affected() as usize;
        }
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn append_daily_bar_versions_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        bars: &[(String, Candle)],
        available_at: chrono::DateTime<chrono::Utc>,
        availability_quality: &str,
        source: &str,
    ) -> Result<usize> {
        let mut inserted = 0;
        for (code, bar) in bars {
            inserted += sqlx::query(
                r#"INSERT INTO stock_daily_bar_versions
                   (code, trade_date, open, high, low, close, volume, amount,
                    turnover, pe, pb, available_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(code)
            .bind(bar.trade_date)
            .bind(bar.open)
            .bind(bar.high)
            .bind(bar.low)
            .bind(bar.close)
            .bind(bar.volume)
            .bind(bar.amount)
            .bind(bar.turnover)
            .bind(bar.pe)
            .bind(bar.pb)
            .bind(available_at)
            .bind(availability_quality)
            .bind(source)
            .execute(&mut **tx)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_sector_versions_with_ingested_at(
        &self,
        rows: &[SectorData],
        available_at: DateTime<Utc>,
        availability_quality: &str,
        source: &str,
        ingested_at: DateTime<Utc>,
    ) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO sector_daily_versions
                   (code, name, sector_type, change_pct, amount, trade_date,
                    available_at, availability_quality, source, ingested_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(&row.name)
            .bind(&row.sector_type)
            .bind(row.change_pct)
            .bind(row.amount)
            .bind(row.trade_date)
            .bind(available_at)
            .bind(availability_quality)
            .bind(source)
            .bind(ingested_at)
            .execute(tx.as_mut())
            .await?
            .rows_affected() as usize;
        }
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn append_sector_versions(
        &self,
        rows: &[SectorData],
        available_at: DateTime<Utc>,
        availability_quality: &str,
        source: &str,
    ) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let inserted = Self::append_sector_versions_in_tx(
            &mut tx,
            rows,
            available_at,
            availability_quality,
            source,
        )
        .await?;
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn append_limit_up_versions_with_ingested_at(
        &self,
        rows: &[LimitUpStock],
        available_at: DateTime<Utc>,
        availability_quality: &str,
        source: &str,
        ingested_at: DateTime<Utc>,
    ) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO limit_up_stock_versions
                   (code, trade_date, name, limit_time, seal_amount, burst_count,
                    close, pct_chg, strth, available_at, availability_quality, source, ingested_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(row.trade_date)
            .bind(&row.name)
            .bind(&row.first_time)
            .bind(row.fd_amount)
            .bind(row.open_times)
            .bind(row.close)
            .bind(row.pct_chg)
            .bind(row.strth)
            .bind(available_at)
            .bind(availability_quality)
            .bind(source)
            .bind(ingested_at)
            .execute(tx.as_mut())
            .await?
            .rows_affected() as usize;
        }
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn append_sector_versions_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        rows: &[SectorData],
        available_at: DateTime<Utc>,
        availability_quality: &str,
        source: &str,
    ) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO sector_daily_versions
                   (code, name, sector_type, change_pct, amount, trade_date,
                    available_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(&row.name)
            .bind(&row.sector_type)
            .bind(row.change_pct)
            .bind(row.amount)
            .bind(row.trade_date)
            .bind(available_at)
            .bind(availability_quality)
            .bind(source)
            .execute(&mut **tx)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_limit_up_versions(
        &self,
        rows: &[LimitUpStock],
        available_at: DateTime<Utc>,
        availability_quality: &str,
        source: &str,
    ) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let inserted = Self::append_limit_up_versions_in_tx(
            &mut tx,
            rows,
            available_at,
            availability_quality,
            source,
        )
        .await?;
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn append_limit_up_versions_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        rows: &[LimitUpStock],
        available_at: DateTime<Utc>,
        availability_quality: &str,
        source: &str,
    ) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO limit_up_stock_versions
                   (code, trade_date, name, limit_time, seal_amount, burst_count,
                    close, pct_chg, strth, available_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(row.trade_date)
            .bind(&row.name)
            .bind(&row.first_time)
            .bind(row.fd_amount)
            .bind(row.open_times)
            .bind(row.close)
            .bind(row.pct_chg)
            .bind(row.strth)
            .bind(available_at)
            .bind(availability_quality)
            .bind(source)
            .execute(&mut **tx)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_daily_basics(&self, rows: &[DailyBasicSnapshot]) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO stock_daily_basic_versions
                   (code, trade_date, turnover_rate, volume_ratio, pe, pb, ps,
                    total_share, float_share, total_mv, circ_mv, available_at,
                    ingested_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(row.trade_date)
            .bind(row.turnover_rate)
            .bind(row.volume_ratio)
            .bind(row.pe)
            .bind(row.pb)
            .bind(row.ps)
            .bind(row.total_share)
            .bind(row.float_share)
            .bind(row.total_mv)
            .bind(row.circ_mv)
            .bind(row.available_at)
            .bind(row.ingested_at)
            .bind(quality_to_str(row.availability_quality))
            .bind(&row.source)
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_security_master_versions(
        &self,
        rows: &[SecurityMasterVersion],
    ) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO security_master_versions
                   (code, name, market, exchange, list_status, list_date, delist_date,
                    available_at, ingested_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                   ON CONFLICT (code, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(&row.name)
            .bind(&row.market)
            .bind(&row.exchange)
            .bind(&row.list_status)
            .bind(row.list_date)
            .bind(row.delist_date)
            .bind(row.available_at)
            .bind(row.ingested_at)
            .bind(quality_to_str(row.availability_quality))
            .bind(&row.source)
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_corporate_actions(&self, rows: &[CorporateAction]) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO corporate_action_versions
                   (source, action_key, code, action_type, announcement_date, record_date,
                    ex_date, pay_date, cash_dividend, stock_ratio, rights_ratio, rights_price,
                    available_at, ingested_at, availability_quality)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                   ON CONFLICT (source, action_key, available_at) DO NOTHING"#,
            )
            .bind(&row.source)
            .bind(&row.action_key)
            .bind(&row.code)
            .bind(&row.action_type)
            .bind(row.announcement_date)
            .bind(row.record_date)
            .bind(row.ex_date)
            .bind(row.pay_date)
            .bind(row.cash_dividend)
            .bind(row.stock_ratio)
            .bind(row.rights_ratio)
            .bind(row.rights_price)
            .bind(row.available_at)
            .bind(row.ingested_at)
            .bind(quality_to_str(row.availability_quality))
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_adjustment_factors(&self, rows: &[AdjustmentFactor]) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO stock_adjustment_factors
                   (code, trade_date, adj_factor, available_at, ingested_at,
                    availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(row.trade_date)
            .bind(row.adj_factor)
            .bind(row.available_at)
            .bind(row.ingested_at)
            .bind(quality_to_str(row.availability_quality))
            .bind(&row.source)
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_security_statuses(&self, rows: &[SecurityDailyStatus]) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO security_daily_status
                   (code, trade_date, listed_days, is_st, is_suspended, price_limit_pct,
                    available_at, ingested_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(row.trade_date)
            .bind(row.listed_days)
            .bind(row.is_st)
            .bind(row.is_suspended)
            .bind(row.price_limit_pct)
            .bind(row.available_at)
            .bind(row.ingested_at)
            .bind(quality_to_str(row.availability_quality))
            .bind(&row.source)
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_index_bars(&self, rows: &[IndexDailyBar]) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO index_daily_bars
                   (code, trade_date, close, change_pct, volume, amount, available_at,
                    ingested_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                   ON CONFLICT (code, trade_date, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(row.trade_date)
            .bind(row.close)
            .bind(row.change_pct)
            .bind(row.volume)
            .bind(row.amount)
            .bind(row.available_at)
            .bind(row.ingested_at)
            .bind(quality_to_str(row.availability_quality))
            .bind(&row.source)
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn append_sector_memberships(&self, rows: &[SectorMembership]) -> Result<usize> {
        let mut inserted = 0;
        for row in rows {
            inserted += sqlx::query(
                r#"INSERT INTO stock_sector_membership
                   (code, sector_code, sector_name, sector_type, valid_from, valid_to,
                    available_at, ingested_at, availability_quality, source)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                   ON CONFLICT (code, sector_code, valid_from, available_at) DO NOTHING"#,
            )
            .bind(&row.code)
            .bind(&row.sector_code)
            .bind(&row.sector_name)
            .bind(&row.sector_type)
            .bind(row.valid_from)
            .bind(row.valid_to)
            .bind(row.available_at)
            .bind(row.ingested_at)
            .bind(quality_to_str(row.availability_quality))
            .bind(&row.source)
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
        }
        Ok(inserted)
    }

    pub async fn security_master(
        &self,
        code: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<SecurityMasterVersion>> {
        let row: Option<(
            String,
            String,
            Option<String>,
            Option<String>,
            String,
            Option<NaiveDate>,
            Option<NaiveDate>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, name, market, exchange, list_status, list_date, delist_date,
                      available_at, ingested_at, availability_quality, source
               FROM security_master_versions
               WHERE code = $1
                 AND available_at <= $2
               ORDER BY available_at DESC
               LIMIT 1"#,
        )
        .bind(code)
        .bind(as_of)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(
            |(
                code,
                name,
                market,
                exchange,
                list_status,
                list_date,
                delist_date,
                available_at,
                ingested_at,
                quality,
                source,
            )| SecurityMasterVersion {
                code,
                name,
                market,
                exchange,
                list_status,
                list_date,
                delist_date,
                available_at,
                ingested_at,
                availability_quality: parse_quality(&quality),
                source,
            },
        ))
    }

    pub async fn daily_basic(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Option<DailyBasicSnapshot>> {
        let row: Option<(
            String,
            NaiveDate,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, turnover_rate::float8, volume_ratio::float8,
                      pe::float8, pb::float8, ps::float8, total_share::float8,
                      float_share::float8, total_mv::float8, circ_mv::float8,
                      available_at, ingested_at, availability_quality, source
               FROM stock_daily_basic_versions
               WHERE code = $1
                 AND trade_date = $2
                 AND available_at <= $3
               ORDER BY available_at DESC
               LIMIT 1"#,
        )
        .bind(code)
        .bind(trade_date)
        .bind(as_of)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(
            |(
                code,
                trade_date,
                turnover_rate,
                volume_ratio,
                pe,
                pb,
                ps,
                total_share,
                float_share,
                total_mv,
                circ_mv,
                available_at,
                ingested_at,
                quality,
                source,
            )| DailyBasicSnapshot {
                code,
                trade_date,
                turnover_rate,
                volume_ratio,
                pe,
                pb,
                ps,
                total_share,
                float_share,
                total_mv,
                circ_mv,
                available_at,
                ingested_at,
                availability_quality: parse_quality(&quality),
                source,
            },
        ))
    }

    pub async fn daily_bar_history_as_of(
        &self,
        end: NaiveDate,
        as_of: DateTime<Utc>,
        lookback: i64,
    ) -> Result<Vec<PointInTimeDailyBarVersion>> {
        let rows: Vec<(
            String,
            NaiveDate,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<i64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
        )> = sqlx::query_as(
            r#"WITH latest AS (
                   SELECT code, trade_date, open, high, low, close, volume, amount,
                          turnover, pe, pb, available_at, ingested_at, source,
                          ROW_NUMBER() OVER (
                              PARTITION BY code, trade_date
                              ORDER BY available_at DESC
                          ) AS version_rank
                   FROM stock_daily_bar_versions
                   WHERE trade_date <= $1
                     AND available_at <= $2
               ),
               ranked AS (
                   SELECT code, trade_date, open, high, low, close, volume, amount,
                          turnover, pe, pb, available_at, ingested_at, source,
                          ROW_NUMBER() OVER (
                              PARTITION BY code
                              ORDER BY trade_date DESC
                          ) AS history_rank
                   FROM latest
                   WHERE version_rank = 1
               )
               SELECT code, trade_date, open::float8, high::float8, low::float8,
                      close::float8, volume, amount::float8, turnover::float8,
                      pe::float8, pb::float8, available_at, ingested_at, source
               FROM ranked
               WHERE history_rank <= $3
               ORDER BY code, trade_date"#,
        )
        .bind(end)
        .bind(as_of)
        .bind(lookback)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    trade_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    amount,
                    turnover,
                    pe,
                    pb,
                    available_at,
                    ingested_at,
                    source,
                )| PointInTimeDailyBarVersion {
                    code,
                    bar: Candle {
                        trade_date,
                        open: open.unwrap_or(0.0),
                        high: high.unwrap_or(0.0),
                        low: low.unwrap_or(0.0),
                        close: close.unwrap_or(0.0),
                        volume: volume.unwrap_or(0),
                        amount: amount.unwrap_or(0.0),
                        turnover,
                        pe,
                        pb,
                    },
                    available_at,
                    ingested_at,
                    source,
                },
            )
            .collect())
    }

    pub async fn latest_adjustment_factor(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Option<AdjustmentFactor>> {
        let row: Option<(
            String,
            NaiveDate,
            f64,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, adj_factor::float8, available_at,
                      ingested_at, availability_quality, source
               FROM stock_adjustment_factors
               WHERE code = $1
                 AND trade_date <= $2
                 AND available_at <= $3
               ORDER BY trade_date DESC, available_at DESC
               LIMIT 1"#,
        )
        .bind(code)
        .bind(trade_date)
        .bind(as_of)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(
            |(code, trade_date, adj_factor, available_at, ingested_at, quality, source)| {
                AdjustmentFactor {
                    code,
                    trade_date,
                    adj_factor,
                    available_at,
                    ingested_at,
                    availability_quality: parse_quality(&quality),
                    source,
                }
            },
        ))
    }

    pub async fn adjustment_factors_as_of(
        &self,
        codes: &[String],
        start: NaiveDate,
        end: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<AdjustmentFactor>> {
        if codes.is_empty() {
            return Ok(Vec::new());
        }

        let rows: Vec<(
            String,
            NaiveDate,
            f64,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, adj_factor::float8, available_at,
                      ingested_at, availability_quality, source
               FROM (
                   SELECT code, trade_date, adj_factor, available_at, ingested_at,
                          availability_quality, source,
                          ROW_NUMBER() OVER (
                              PARTITION BY code, trade_date
                              ORDER BY available_at DESC
                          ) AS version_rank
                   FROM stock_adjustment_factors
                   WHERE code = ANY($1)
                     AND trade_date BETWEEN $2 AND $3
                     AND available_at <= $4
               ) latest
               WHERE version_rank = 1
               ORDER BY code, trade_date"#,
        )
        .bind(codes)
        .bind(start)
        .bind(end)
        .bind(as_of)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(code, trade_date, adj_factor, available_at, ingested_at, quality, source)| {
                    AdjustmentFactor {
                        code,
                        trade_date,
                        adj_factor,
                        available_at,
                        ingested_at,
                        availability_quality: parse_quality(&quality),
                        source,
                    }
                },
            )
            .collect())
    }

    pub async fn corporate_actions(
        &self,
        code: &str,
        end: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<CorporateAction>> {
        let rows: Vec<(
            String,
            String,
            String,
            String,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
        )> = sqlx::query_as(
            r#"SELECT source, action_key, code, action_type, announcement_date, record_date,
                      ex_date, pay_date, cash_dividend::float8, stock_ratio::float8,
                      rights_ratio::float8, rights_price::float8, available_at,
                      ingested_at, availability_quality
               FROM (
                   SELECT DISTINCT ON (source, action_key)
                          source, action_key, code, action_type, announcement_date, record_date,
                          ex_date, pay_date, cash_dividend, stock_ratio, rights_ratio, rights_price,
                          available_at, ingested_at, availability_quality
                   FROM corporate_action_versions
                   WHERE code = $1
                     AND available_at <= $2
                   ORDER BY source, action_key, available_at DESC
               ) latest
               WHERE ex_date <= $3
               ORDER BY ex_date, source, action_key"#,
        )
        .bind(code)
        .bind(as_of)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    source,
                    action_key,
                    code,
                    action_type,
                    announcement_date,
                    record_date,
                    ex_date,
                    pay_date,
                    cash_dividend,
                    stock_ratio,
                    rights_ratio,
                    rights_price,
                    available_at,
                    ingested_at,
                    quality,
                )| CorporateAction {
                    source,
                    action_key,
                    code,
                    action_type,
                    announcement_date,
                    record_date,
                    ex_date,
                    pay_date,
                    cash_dividend,
                    stock_ratio,
                    rights_ratio,
                    rights_price,
                    available_at,
                    ingested_at,
                    availability_quality: parse_quality(&quality),
                },
            )
            .collect())
    }

    pub async fn security_status(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Option<SecurityDailyStatus>> {
        let row: Option<(
            String,
            NaiveDate,
            Option<i32>,
            bool,
            bool,
            Option<f64>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, listed_days, is_st, is_suspended,
                      price_limit_pct::float8, available_at, ingested_at,
                      availability_quality, source
               FROM security_daily_status
               WHERE code = $1
                 AND trade_date = $2
                 AND available_at <= $3
               ORDER BY available_at DESC
               LIMIT 1"#,
        )
        .bind(code)
        .bind(trade_date)
        .bind(as_of)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(
            |(
                code,
                trade_date,
                listed_days,
                is_st,
                is_suspended,
                price_limit_pct,
                available_at,
                ingested_at,
                quality,
                source,
            )| SecurityDailyStatus {
                code,
                trade_date,
                listed_days,
                is_st,
                is_suspended,
                price_limit_pct,
                available_at,
                ingested_at,
                availability_quality: parse_quality(&quality),
                source,
            },
        ))
    }

    pub async fn security_statuses_as_of(
        &self,
        codes: &[String],
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SecurityDailyStatus>> {
        if codes.is_empty() {
            return Ok(Vec::new());
        }

        let rows: Vec<(
            String,
            NaiveDate,
            Option<i32>,
            bool,
            bool,
            Option<f64>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, listed_days, is_st, is_suspended,
                      price_limit_pct::float8, available_at, ingested_at,
                      availability_quality, source
               FROM (
                   SELECT code, trade_date, listed_days, is_st, is_suspended,
                          price_limit_pct, available_at, ingested_at,
                          availability_quality, source,
                          ROW_NUMBER() OVER (
                              PARTITION BY code, trade_date
                              ORDER BY available_at DESC
                          ) AS version_rank
                   FROM security_daily_status
                   WHERE code = ANY($1)
                     AND trade_date = $2
                     AND available_at <= $3
               ) latest
               WHERE version_rank = 1
               ORDER BY code"#,
        )
        .bind(codes)
        .bind(trade_date)
        .bind(as_of)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    trade_date,
                    listed_days,
                    is_st,
                    is_suspended,
                    price_limit_pct,
                    available_at,
                    ingested_at,
                    quality,
                    source,
                )| SecurityDailyStatus {
                    code,
                    trade_date,
                    listed_days,
                    is_st,
                    is_suspended,
                    price_limit_pct,
                    available_at,
                    ingested_at,
                    availability_quality: parse_quality(&quality),
                    source,
                },
            )
            .collect())
    }

    pub async fn security_status_universe_as_of(
        &self,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SecurityDailyStatus>> {
        let rows: Vec<(
            String,
            NaiveDate,
            Option<i32>,
            bool,
            bool,
            Option<f64>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, listed_days, is_st, is_suspended,
                      price_limit_pct::float8, available_at, ingested_at,
                      availability_quality, source
               FROM (
                   SELECT code, trade_date, listed_days, is_st, is_suspended,
                          price_limit_pct, available_at, ingested_at,
                          availability_quality, source,
                          ROW_NUMBER() OVER (
                              PARTITION BY code, trade_date
                              ORDER BY available_at DESC
                          ) AS version_rank
                   FROM security_daily_status
                   WHERE trade_date = $1
                     AND available_at <= $2
               ) latest
               WHERE version_rank = 1
               ORDER BY code"#,
        )
        .bind(trade_date)
        .bind(as_of)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    trade_date,
                    listed_days,
                    is_st,
                    is_suspended,
                    price_limit_pct,
                    available_at,
                    ingested_at,
                    quality,
                    source,
                )| SecurityDailyStatus {
                    code,
                    trade_date,
                    listed_days,
                    is_st,
                    is_suspended,
                    price_limit_pct,
                    available_at,
                    ingested_at,
                    availability_quality: parse_quality(&quality),
                    source,
                },
            )
            .collect())
    }

    pub async fn active_sector_memberships(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SectorMembership>> {
        let rows: Vec<(
            String,
            String,
            String,
            String,
            NaiveDate,
            Option<NaiveDate>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, sector_code, sector_name, sector_type, valid_from, valid_to,
                      available_at, ingested_at, availability_quality, source
               FROM (
                   SELECT DISTINCT ON (code, sector_code, valid_from)
                          code, sector_code, sector_name, sector_type, valid_from, valid_to,
                          available_at, ingested_at, availability_quality, source
                   FROM stock_sector_membership
                   WHERE code = $1
                     AND available_at <= $2
                   ORDER BY code, sector_code, valid_from, available_at DESC
               ) latest
               WHERE valid_from <= $3
                 AND (valid_to IS NULL OR valid_to >= $3)
               ORDER BY sector_type, sector_code"#,
        )
        .bind(code)
        .bind(as_of)
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    sector_code,
                    sector_name,
                    sector_type,
                    valid_from,
                    valid_to,
                    available_at,
                    ingested_at,
                    quality,
                    source,
                )| SectorMembership {
                    code,
                    sector_code,
                    sector_name,
                    sector_type,
                    valid_from,
                    valid_to,
                    available_at,
                    ingested_at,
                    availability_quality: parse_quality(&quality),
                    source,
                },
            )
            .collect())
    }

    pub async fn index_history(
        &self,
        code: &str,
        end: NaiveDate,
        as_of: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<IndexDailyBar>> {
        let rows: Vec<(
            String,
            NaiveDate,
            f64,
            Option<f64>,
            Option<i64>,
            Option<f64>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, close::float8, change_pct::float8, volume,
                      amount::float8, available_at, ingested_at, availability_quality, source
               FROM (
                   SELECT DISTINCT ON (code, trade_date)
                          code, trade_date, close, change_pct, volume, amount,
                          available_at, ingested_at, availability_quality, source
                   FROM index_daily_bars
                   WHERE code = $1
                     AND trade_date <= $2
                     AND available_at <= $3
                   ORDER BY code, trade_date, available_at DESC
               ) latest
               ORDER BY trade_date DESC
               LIMIT $4"#,
        )
        .bind(code)
        .bind(end)
        .bind(as_of)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    trade_date,
                    close,
                    change_pct,
                    volume,
                    amount,
                    available_at,
                    ingested_at,
                    quality,
                    source,
                )| IndexDailyBar {
                    code,
                    trade_date,
                    close,
                    change_pct,
                    volume,
                    amount,
                    available_at,
                    ingested_at,
                    availability_quality: parse_quality(&quality),
                    source,
                },
            )
            .collect())
    }

    pub async fn index_bars_as_of(
        &self,
        codes: &[String],
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<IndexDailyBar>> {
        if codes.is_empty() {
            return Ok(Vec::new());
        }

        let rows: Vec<(
            String,
            NaiveDate,
            f64,
            Option<f64>,
            Option<i64>,
            Option<f64>,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT code, trade_date, close::float8, change_pct::float8, volume,
                      amount::float8, available_at, ingested_at, availability_quality, source
               FROM (
                   SELECT code, trade_date, close, change_pct, volume, amount,
                          available_at, ingested_at, availability_quality, source,
                          ROW_NUMBER() OVER (
                              PARTITION BY code, trade_date
                              ORDER BY available_at DESC
                          ) AS version_rank
                   FROM index_daily_bars
                   WHERE code = ANY($1)
                     AND trade_date = $2
                     AND available_at <= $3
               ) latest
               WHERE version_rank = 1
               ORDER BY code"#,
        )
        .bind(codes)
        .bind(trade_date)
        .bind(as_of)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    trade_date,
                    close,
                    change_pct,
                    volume,
                    amount,
                    available_at,
                    ingested_at,
                    quality,
                    source,
                )| IndexDailyBar {
                    code,
                    trade_date,
                    close,
                    change_pct,
                    volume,
                    amount,
                    available_at,
                    ingested_at,
                    availability_quality: parse_quality(&quality),
                    source,
                },
            )
            .collect())
    }

    pub async fn save_market_snapshot(&self, snapshot: &MarketSnapshot) -> Result<()> {
        let missing_inputs = serde_json::to_value(&snapshot.missing_inputs)?;
        sqlx::query(
            r#"INSERT INTO market_daily_snapshots
               (trade_date, snapshot_version, available_at, data_complete, metrics,
                missing_inputs, input_fingerprint)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (trade_date, snapshot_version) DO UPDATE
               SET available_at = EXCLUDED.available_at,
                   data_complete = EXCLUDED.data_complete,
                   metrics = EXCLUDED.metrics,
                   missing_inputs = EXCLUDED.missing_inputs,
                   input_fingerprint = EXCLUDED.input_fingerprint"#,
        )
        .bind(snapshot.trade_date)
        .bind(&snapshot.snapshot_version)
        .bind(snapshot.available_at)
        .bind(snapshot.data_complete)
        .bind(&snapshot.metrics)
        .bind(missing_inputs)
        .bind(&snapshot.input_fingerprint)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn market_snapshot(
        &self,
        trade_date: NaiveDate,
        version: &str,
    ) -> Result<Option<MarketSnapshot>> {
        let row: Option<(
            NaiveDate,
            String,
            DateTime<Utc>,
            bool,
            serde_json::Value,
            serde_json::Value,
            String,
        )> = sqlx::query_as(
            r#"SELECT trade_date, snapshot_version, available_at, data_complete,
                      metrics, missing_inputs, input_fingerprint
               FROM market_daily_snapshots
               WHERE trade_date = $1
                 AND snapshot_version = $2"#,
        )
        .bind(trade_date)
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;

        row.map(
            |(
                trade_date,
                snapshot_version,
                available_at,
                data_complete,
                metrics,
                missing_inputs,
                input_fingerprint,
            )| {
                let missing_inputs = serde_json::from_value(missing_inputs)?;
                Ok(MarketSnapshot {
                    trade_date,
                    snapshot_version,
                    available_at,
                    data_complete,
                    metrics,
                    missing_inputs,
                    input_fingerprint,
                })
            },
        )
        .transpose()
    }
}

fn parse_quality(value: &str) -> AvailabilityQuality {
    match value {
        "observed" => AvailabilityQuality::Observed,
        _ => AvailabilityQuality::Estimated,
    }
}

fn quality_to_str(value: AvailabilityQuality) -> &'static str {
    match value {
        AvailabilityQuality::Observed => "observed",
        AvailabilityQuality::Estimated => "estimated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::types::Candle;
    use chrono::TimeZone;
    use serde_json::json;
    use sqlx::PgPool;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn point_in_time_tables_exist(pool: PgPool) -> sqlx::Result<()> {
        let tables: Vec<(String,)> = sqlx::query_as(
            r#"SELECT table_name
               FROM information_schema.tables
               WHERE table_schema = 'public'
                 AND table_name = ANY($1)
               ORDER BY table_name"#,
        )
        .bind(vec![
            "analysis_data_runs".to_string(),
            "corporate_action_versions".to_string(),
            "index_daily_bars".to_string(),
            "limit_up_stock_versions".to_string(),
            "market_daily_snapshots".to_string(),
            "security_daily_status".to_string(),
            "security_master_versions".to_string(),
            "sector_daily_versions".to_string(),
            "stock_adjustment_factors".to_string(),
            "stock_daily_bar_versions".to_string(),
            "stock_daily_basic_versions".to_string(),
            "stock_sector_membership".to_string(),
        ])
        .fetch_all(&pool)
        .await?;

        assert_eq!(tables.len(), 12);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn point_in_time_capability_probe_status_round_trips(pool: PgPool) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let mut details = std::collections::BTreeMap::new();
        details.insert(
            "daily_security_status".to_string(),
            "unsupported: missing dependencies: stock_basic: unsupported: unauthorized".to_string(),
        );
        let capabilities = crate::data::point_in_time_provider::PointInTimeCapabilities {
            security_master_history: true,
            corporate_actions: true,
            adjustment_factors: true,
            daily_basic: true,
            daily_security_status: false,
            historical_index_bars: true,
            historical_sector_membership: true,
            details,
        };

        repo.persist_point_in_time_capability_probe(&Ok(capabilities))
            .await
            .unwrap();

        let status = repo
            .latest_point_in_time_data_status()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(status.run_type, POINT_IN_TIME_CAPABILITY_PROBE_RUN_TYPE);
        assert_eq!(status.status, "missing");
        assert!(status.error_message.is_none());
        assert_eq!(
            status.details["missing_capabilities"],
            json!(["daily_security_status"])
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn latest_adjustment_factor_respects_as_of(pool: PgPool) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO stock_adjustment_factors
               (code, trade_date, adj_factor, available_at, availability_quality, source)
               VALUES
               ('600000.SH', '2026-07-09', 1.1, '2026-07-09T10:00:00Z', 'observed', 'test'),
               ('600000.SH', '2026-07-10', 1.2, '2026-07-10T10:00:00Z', 'observed', 'test')"#,
        )
        .execute(&pool)
        .await?;

        let repo = MarketRepository::new(pool);
        let value = repo
            .latest_adjustment_factor(
                "600000.SH",
                chrono::NaiveDate::from_ymd_opt(2026, 7, 10).unwrap(),
                chrono::DateTime::parse_from_rfc3339("2026-07-10T09:00:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc),
            )
            .await
            .unwrap();

        assert_eq!(value.unwrap().trade_date.to_string(), "2026-07-09");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn adjustment_factor_append_is_idempotent_and_reads_latest_as_of(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let early = AdjustmentFactor {
            code: "600000.SH".to_string(),
            trade_date: date(2026, 7, 10),
            adj_factor: 1.10,
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        };
        let later = AdjustmentFactor {
            adj_factor: 1.20,
            available_at: dt(2026, 7, 10, 12),
            ingested_at: dt(2026, 7, 10, 12),
            availability_quality: AvailabilityQuality::Estimated,
            ..early.clone()
        };

        let inserted = repo
            .append_adjustment_factors(&[early.clone(), early.clone(), later.clone()])
            .await
            .unwrap();

        assert_eq!(inserted, 2);
        let at_09 = repo
            .latest_adjustment_factor("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 9))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_09.adj_factor, 1.10);

        let at_13 = repo
            .latest_adjustment_factor("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 13))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_13.adj_factor, 1.20);
        assert_eq!(at_13.availability_quality, AvailabilityQuality::Estimated);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn daily_basic_append_is_idempotent_and_reads_latest_as_of(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let early = DailyBasicSnapshot {
            code: "600000.SH".to_string(),
            trade_date: date(2026, 7, 10),
            turnover_rate: Some(1.23),
            volume_ratio: Some(0.98),
            pe: Some(11.1),
            pb: Some(1.2),
            ps: Some(3.4),
            total_share: Some(1000.0),
            float_share: Some(800.0),
            total_mv: Some(12000.0),
            circ_mv: Some(9600.0),
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        };
        let later = DailyBasicSnapshot {
            turnover_rate: Some(2.34),
            available_at: dt(2026, 7, 10, 12),
            ingested_at: dt(2026, 7, 10, 12),
            availability_quality: AvailabilityQuality::Estimated,
            ..early.clone()
        };

        let inserted = repo
            .append_daily_basics(&[early.clone(), early.clone(), later.clone()])
            .await
            .unwrap();

        assert_eq!(inserted, 2);
        assert!(repo
            .daily_basic("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 7))
            .await
            .unwrap()
            .is_none());

        let at_09 = repo
            .daily_basic("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 9))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_09.turnover_rate, Some(1.23));
        assert_eq!(at_09.availability_quality, AvailabilityQuality::Observed);

        let at_13 = repo
            .daily_basic("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 13))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_13.turnover_rate, Some(2.34));
        assert_eq!(at_13.availability_quality, AvailabilityQuality::Estimated);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn append_daily_bar_versions_is_point_in_time_safe(pool: PgPool) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool.clone());
        let early_available_at = dt(2026, 7, 10, 8);
        let later_available_at = dt(2026, 7, 10, 12);
        let early = Candle {
            trade_date: date(2026, 7, 10),
            open: 10.0,
            high: 11.0,
            low: 9.5,
            close: 10.5,
            volume: 1_000,
            amount: 10_500.0,
            turnover: Some(1.1),
            pe: Some(12.2),
            pb: Some(1.3),
        };
        let revised = Candle {
            close: 10.8,
            turnover: Some(2.2),
            pe: Some(13.3),
            pb: Some(1.4),
            ..early.clone()
        };

        let first_inserted = repo
            .append_daily_bar_versions(
                &[("600000.SH".to_string(), early.clone())],
                early_available_at,
                "observed",
                "test",
            )
            .await
            .unwrap();
        let duplicate_inserted = repo
            .append_daily_bar_versions(
                &[("600000.SH".to_string(), revised.clone())],
                early_available_at,
                "observed",
                "test",
            )
            .await
            .unwrap();
        let later_inserted = repo
            .append_daily_bar_versions(
                &[("600000.SH".to_string(), revised)],
                later_available_at,
                "estimated",
                "test",
            )
            .await
            .unwrap();

        let rows: Vec<(chrono::DateTime<Utc>, f64, f64, String, String)> = sqlx::query_as(
            r#"SELECT available_at, close::float8, turnover::float8,
                      availability_quality, source
               FROM stock_daily_bar_versions
               WHERE code = '600000.SH' AND trade_date = '2026-07-10'
               ORDER BY available_at"#,
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(first_inserted, 1);
        assert_eq!(duplicate_inserted, 0);
        assert_eq!(later_inserted, 1);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            (
                early_available_at,
                10.5,
                1.1,
                "observed".to_string(),
                "test".to_string()
            )
        );
        assert_eq!(
            rows[1],
            (
                later_available_at,
                10.8,
                2.2,
                "estimated".to_string(),
                "test".to_string()
            )
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn append_sector_versions_records_metadata_and_is_idempotent(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool.clone());
        let available_at = dt(2026, 7, 10, 8);
        let row = SectorData {
            code: "BK0477".to_string(),
            name: "Semiconductors".to_string(),
            sector_type: "industry".to_string(),
            change_pct: 2.34,
            amount: 123_456_789.0,
            trade_date: date(2026, 7, 10),
        };

        let inserted = repo
            .append_sector_versions(&[row.clone(), row], available_at, "observed", "test")
            .await
            .unwrap();

        let rows: Vec<(String, String, f64, chrono::DateTime<Utc>, String, String)> =
            sqlx::query_as(
                r#"SELECT name, sector_type, change_pct::float8, available_at,
                          availability_quality, source
                   FROM sector_daily_versions
                   WHERE code = 'BK0477' AND trade_date = '2026-07-10'"#,
            )
            .fetch_all(&pool)
            .await?;

        assert_eq!(inserted, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            (
                "Semiconductors".to_string(),
                "industry".to_string(),
                2.34,
                available_at,
                "observed".to_string(),
                "test".to_string()
            )
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn append_limit_up_versions_records_metadata_and_is_idempotent(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool.clone());
        let available_at = dt(2026, 7, 10, 8);
        let row = LimitUpStock {
            code: "600000.SH".to_string(),
            name: "Alpha".to_string(),
            trade_date: date(2026, 7, 10),
            close: 10.5,
            pct_chg: 10.01,
            fd_amount: 987_654_321.0,
            first_time: Some("09:35".to_string()),
            last_time: Some("14:55".to_string()),
            open_times: 2,
            strth: 88.8,
            limit: "U".to_string(),
        };

        let inserted = repo
            .append_limit_up_versions(&[row.clone(), row], available_at, "observed", "test")
            .await
            .unwrap();

        let rows: Vec<(
            String,
            String,
            f64,
            i32,
            f64,
            chrono::DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT name, limit_time, seal_amount::float8, burst_count,
                      strth::float8, available_at, availability_quality, source
               FROM limit_up_stock_versions
               WHERE code = '600000.SH' AND trade_date = '2026-07-10'"#,
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(inserted, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            (
                "Alpha".to_string(),
                "09:35".to_string(),
                987_654_321.0,
                2,
                88.8,
                available_at,
                "observed".to_string(),
                "test".to_string()
            )
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn security_master_append_is_idempotent_and_reads_latest_as_of(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let listed = SecurityMasterVersion {
            code: "600000.SH".to_string(),
            name: "Old Name".to_string(),
            market: Some("A".to_string()),
            exchange: Some("SSE".to_string()),
            list_status: "L".to_string(),
            list_date: Some(date(1999, 11, 10)),
            delist_date: None,
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        };
        let renamed = SecurityMasterVersion {
            name: "New Name".to_string(),
            available_at: dt(2026, 7, 10, 12),
            ingested_at: dt(2026, 7, 10, 12),
            ..listed.clone()
        };

        let inserted = repo
            .append_security_master_versions(&[listed.clone(), listed.clone(), renamed.clone()])
            .await
            .unwrap();

        assert_eq!(inserted, 2);
        assert!(repo
            .security_master("600000.SH", dt(2026, 7, 10, 7))
            .await
            .unwrap()
            .is_none());

        let at_09 = repo
            .security_master("600000.SH", dt(2026, 7, 10, 9))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_09.name, "Old Name");

        let at_13 = repo
            .security_master("600000.SH", dt(2026, 7, 10, 13))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_13.name, "New Name");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn corporate_actions_append_is_idempotent_and_reads_latest_as_of(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let early = CorporateAction {
            source: "test".to_string(),
            action_key: "600000.SH-div-2026".to_string(),
            code: "600000.SH".to_string(),
            action_type: "cash_dividend".to_string(),
            announcement_date: Some(date(2026, 7, 1)),
            record_date: Some(date(2026, 7, 9)),
            ex_date: Some(date(2026, 7, 10)),
            pay_date: Some(date(2026, 7, 20)),
            cash_dividend: Some(0.10),
            stock_ratio: None,
            rights_ratio: None,
            rights_price: None,
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            availability_quality: AvailabilityQuality::Observed,
        };
        let revised = CorporateAction {
            cash_dividend: Some(0.20),
            available_at: dt(2026, 7, 10, 12),
            ingested_at: dt(2026, 7, 10, 12),
            availability_quality: AvailabilityQuality::Estimated,
            ..early.clone()
        };
        let future_ex_date = CorporateAction {
            action_key: "600000.SH-div-2026-later".to_string(),
            ex_date: Some(date(2026, 7, 11)),
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            ..early.clone()
        };

        let inserted = repo
            .append_corporate_actions(&[
                early.clone(),
                early.clone(),
                revised.clone(),
                future_ex_date.clone(),
            ])
            .await
            .unwrap();

        assert_eq!(inserted, 3);

        let at_09 = repo
            .corporate_actions("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 9))
            .await
            .unwrap();
        assert_eq!(at_09.len(), 1);
        assert_eq!(at_09[0].cash_dividend, Some(0.10));
        assert_eq!(at_09[0].availability_quality, AvailabilityQuality::Observed);

        let at_13 = repo
            .corporate_actions("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 13))
            .await
            .unwrap();
        assert_eq!(at_13.len(), 1);
        assert_eq!(at_13[0].cash_dividend, Some(0.20));
        assert_eq!(
            at_13[0].availability_quality,
            AvailabilityQuality::Estimated
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn security_status_append_is_idempotent_and_reads_latest_as_of(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let normal = SecurityDailyStatus {
            code: "600000.SH".to_string(),
            trade_date: date(2026, 7, 10),
            listed_days: Some(100),
            is_st: false,
            is_suspended: false,
            price_limit_pct: Some(10.0),
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        };
        let suspended = SecurityDailyStatus {
            is_suspended: true,
            available_at: dt(2026, 7, 10, 12),
            ingested_at: dt(2026, 7, 10, 12),
            ..normal.clone()
        };

        let inserted = repo
            .append_security_statuses(&[normal.clone(), normal.clone(), suspended.clone()])
            .await
            .unwrap();

        assert_eq!(inserted, 2);
        let at_09 = repo
            .security_status("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 9))
            .await
            .unwrap()
            .unwrap();
        assert!(!at_09.is_suspended);

        let at_13 = repo
            .security_status("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 13))
            .await
            .unwrap()
            .unwrap();
        assert!(at_13.is_suspended);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn index_history_append_is_idempotent_and_reads_latest_as_of(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let previous_day = IndexDailyBar {
            code: "000001.SH".to_string(),
            trade_date: date(2026, 7, 9),
            close: 2990.0,
            change_pct: Some(-0.1),
            volume: Some(1_000),
            amount: Some(10_000.0),
            available_at: dt(2026, 7, 9, 18),
            ingested_at: dt(2026, 7, 9, 18),
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        };
        let early = IndexDailyBar {
            trade_date: date(2026, 7, 10),
            close: 3000.0,
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            ..previous_day.clone()
        };
        let revised = IndexDailyBar {
            close: 3010.0,
            available_at: dt(2026, 7, 10, 12),
            ingested_at: dt(2026, 7, 10, 12),
            availability_quality: AvailabilityQuality::Estimated,
            ..early.clone()
        };

        let inserted = repo
            .append_index_bars(&[
                previous_day.clone(),
                early.clone(),
                early.clone(),
                revised.clone(),
            ])
            .await
            .unwrap();

        assert_eq!(inserted, 3);

        let at_09 = repo
            .index_history("000001.SH", date(2026, 7, 10), dt(2026, 7, 10, 9), 2)
            .await
            .unwrap();
        assert_eq!(at_09.len(), 2);
        assert_eq!(at_09[0].trade_date, date(2026, 7, 10));
        assert_eq!(at_09[0].close, 3000.0);
        assert_eq!(at_09[1].trade_date, date(2026, 7, 9));

        let at_13 = repo
            .index_history("000001.SH", date(2026, 7, 10), dt(2026, 7, 10, 13), 1)
            .await
            .unwrap();
        assert_eq!(at_13.len(), 1);
        assert_eq!(at_13[0].close, 3010.0);
        assert_eq!(
            at_13[0].availability_quality,
            AvailabilityQuality::Estimated
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn sector_membership_append_is_idempotent_and_reads_active_as_of(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let active_early = SectorMembership {
            code: "600000.SH".to_string(),
            sector_code: "BANK".to_string(),
            sector_name: "Banking".to_string(),
            sector_type: "industry".to_string(),
            valid_from: date(2026, 7, 1),
            valid_to: None,
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        };
        let ended_later = SectorMembership {
            valid_to: Some(date(2026, 7, 9)),
            available_at: dt(2026, 7, 10, 12),
            ingested_at: dt(2026, 7, 10, 12),
            availability_quality: AvailabilityQuality::Estimated,
            ..active_early.clone()
        };
        let active_theme = SectorMembership {
            sector_code: "VALUE".to_string(),
            sector_name: "Value".to_string(),
            sector_type: "theme".to_string(),
            valid_from: date(2026, 7, 5),
            available_at: dt(2026, 7, 10, 8),
            ingested_at: dt(2026, 7, 10, 8),
            ..active_early.clone()
        };

        let inserted = repo
            .append_sector_memberships(&[
                active_early.clone(),
                active_early.clone(),
                ended_later.clone(),
                active_theme.clone(),
            ])
            .await
            .unwrap();

        assert_eq!(inserted, 3);

        let at_09 = repo
            .active_sector_memberships("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 9))
            .await
            .unwrap();
        assert_eq!(at_09.len(), 2);
        assert_eq!(at_09[0].sector_code, "BANK");
        assert_eq!(at_09[1].sector_code, "VALUE");

        let at_13 = repo
            .active_sector_memberships("600000.SH", date(2026, 7, 10), dt(2026, 7, 10, 13))
            .await
            .unwrap();
        assert_eq!(at_13.len(), 1);
        assert_eq!(at_13[0].sector_code, "VALUE");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn market_snapshot_save_upserts_latest_snapshot(pool: PgPool) -> sqlx::Result<()> {
        let repo = MarketRepository::new(pool);
        let snapshot = MarketSnapshot {
            trade_date: date(2026, 7, 10),
            snapshot_version: "gate0".to_string(),
            available_at: dt(2026, 7, 10, 18),
            data_complete: false,
            metrics: json!({"advancers": 123, "decliners": 456}),
            missing_inputs: vec!["index_history".to_string()],
            input_fingerprint: "fingerprint-a".to_string(),
        };
        let conflicting = MarketSnapshot {
            data_complete: true,
            metrics: json!({"advancers": 999}),
            missing_inputs: Vec::new(),
            input_fingerprint: "fingerprint-b".to_string(),
            ..snapshot.clone()
        };

        repo.save_market_snapshot(&snapshot).await.unwrap();
        repo.save_market_snapshot(&conflicting).await.unwrap();

        let saved = repo
            .market_snapshot(date(2026, 7, 10), "gate0")
            .await
            .unwrap()
            .unwrap();
        assert!(saved.data_complete);
        assert_eq!(saved.metrics, json!({"advancers": 999}));
        assert!(saved.missing_inputs.is_empty());
        assert_eq!(saved.input_fingerprint, "fingerprint-b");

        assert!(repo
            .market_snapshot(date(2026, 7, 10), "missing")
            .await
            .unwrap()
            .is_none());
        Ok(())
    }
}
