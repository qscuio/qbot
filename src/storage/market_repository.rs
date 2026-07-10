use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgPool;

use crate::analysis::market_snapshot::{
    AdjustmentFactor, AvailabilityQuality, CorporateAction, DailyBasicSnapshot, IndexDailyBar,
    MarketSnapshot, SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
};
use crate::error::Result;

#[derive(Clone)]
pub struct MarketRepository {
    pool: PgPool,
}

impl MarketRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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

    pub async fn save_market_snapshot(&self, snapshot: &MarketSnapshot) -> Result<()> {
        let missing_inputs = serde_json::to_value(&snapshot.missing_inputs)?;
        sqlx::query(
            r#"INSERT INTO market_daily_snapshots
               (trade_date, snapshot_version, available_at, data_complete, metrics,
                missing_inputs, input_fingerprint)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (trade_date, snapshot_version) DO NOTHING"#,
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
    async fn market_snapshot_save_is_idempotent_and_reads_original_snapshot(
        pool: PgPool,
    ) -> sqlx::Result<()> {
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
        assert!(!saved.data_complete);
        assert_eq!(saved.metrics, json!({"advancers": 123, "decliners": 456}));
        assert_eq!(saved.missing_inputs, vec!["index_history".to_string()]);
        assert_eq!(saved.input_fingerprint, "fingerprint-a");

        assert!(repo
            .market_snapshot(date(2026, 7, 10), "missing")
            .await
            .unwrap()
            .is_none());
        Ok(())
    }
}
