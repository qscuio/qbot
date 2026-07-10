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
    use sqlx::PgPool;

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
}
