use async_trait::async_trait;
use chrono::NaiveDate;
use sqlx::PgPool;

use crate::data::provider::DataProvider;
use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
use crate::error::Result;

pub struct DbDataProvider {
    pool: PgPool,
}

impl DbDataProvider {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DataProvider for DbDataProvider {
    fn name(&self) -> &'static str {
        "db"
    }

    async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
        let rows: Vec<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
            r#"SELECT code, name, market, industry
               FROM stock_info
               ORDER BY code"#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(code, name, market, industry)| StockInfo {
                code,
                name,
                market: market.unwrap_or_default(),
                industry,
            })
            .collect())
    }

    async fn get_daily_bars_by_date(&self, trade_date: NaiveDate) -> Result<Vec<(String, Candle)>> {
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
            r#"SELECT code, trade_date,
                      open::float8, high::float8, low::float8, close::float8,
                      volume, amount::float8, turnover::float8, pe::float8, pb::float8
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

    async fn get_daily_bars_for_stock(
        &self,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Candle>> {
        let rows: Vec<(
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
            r#"SELECT trade_date,
                      open::float8, high::float8, low::float8, close::float8,
                      volume, amount::float8, turnover::float8, pe::float8, pb::float8
               FROM stock_daily_bars
               WHERE code = $1 AND trade_date >= $2 AND trade_date <= $3
               ORDER BY trade_date"#,
        )
        .bind(code)
        .bind(start_date)
        .bind(end_date)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(trade_date, open, high, low, close, volume, amount, turnover, pe, pb)| Candle {
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
            .collect())
    }

    async fn get_trading_dates(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
        let rows: Vec<(NaiveDate,)> = sqlx::query_as(
            r#"SELECT DISTINCT trade_date
               FROM stock_daily_bars
               WHERE trade_date >= $1 AND trade_date <= $2
               ORDER BY trade_date"#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn get_limit_up_stocks(&self, trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
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
                    limit_time,
                    burst_count,
                    strth,
                )| {
                    LimitUpStock {
                        code,
                        name: name.unwrap_or_default(),
                        trade_date,
                        close: close.unwrap_or(0.0),
                        pct_chg: pct_chg.unwrap_or(0.0),
                        fd_amount: seal_amount.unwrap_or(0.0),
                        first_time: limit_time,
                        last_time: None,
                        open_times: burst_count.unwrap_or(0),
                        strth: strth.unwrap_or(0.0),
                        limit: "U".to_string(),
                    }
                },
            )
            .collect())
    }

    async fn get_index_daily(
        &self,
        code: &str,
        trade_date: NaiveDate,
    ) -> Result<Option<IndexData>> {
        let row: Option<(Option<f64>, Option<f64>, Option<i64>, Option<f64>)> = sqlx::query_as(
            r#"SELECT close::float8, 0::float8, volume, amount::float8
               FROM stock_daily_bars
               WHERE code = $1 AND trade_date = $2
               LIMIT 1"#,
        )
        .bind(code)
        .bind(trade_date)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(close, change_pct, volume, amount)| IndexData {
            code: code.to_string(),
            name: code.to_string(),
            trade_date,
            close: close.unwrap_or(0.0),
            change_pct: change_pct.unwrap_or(0.0),
            volume: volume.unwrap_or(0),
            amount: amount.unwrap_or(0.0),
        }))
    }

    async fn get_sector_data(&self, trade_date: NaiveDate) -> Result<Vec<SectorData>> {
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
}
