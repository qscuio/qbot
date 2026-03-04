use chrono::NaiveDate;
use sqlx::PgPool;
use uuid::Uuid;

use crate::data::types::{Candle, LimitUpStock, SectorData, StockInfo};
use crate::error::{AppError, Result};

/// Run sqlx migrations
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|e| AppError::Database(e.into()))?;
    tracing::info!("Migrations applied");
    Ok(())
}

/// Upsert daily bars (batch)
pub async fn upsert_daily_bars(pool: &PgPool, bars: &[(String, Candle)]) -> Result<usize> {
    if bars.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await?;
    let mut count = 0usize;

    for (code, bar) in bars {
        sqlx::query(
            r#"INSERT INTO stock_daily_bars
               (code, trade_date, open, high, low, close, volume, amount, turnover, pe, pb)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               ON CONFLICT (code, trade_date) DO UPDATE SET
               open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
               close=EXCLUDED.close, volume=EXCLUDED.volume, amount=EXCLUDED.amount"#,
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
        .execute(&mut *tx)
        .await?;
        count += 1;
    }

    tx.commit().await?;
    Ok(count)
}

/// Fetch OHLCV history for a stock (sorted ascending)
pub async fn get_stock_history(pool: &PgPool, code: &str, days: usize) -> Result<Vec<Candle>> {
    let rows: Vec<(NaiveDate, Option<f64>, Option<f64>, Option<f64>, Option<f64>, Option<i64>, Option<f64>, Option<f64>, Option<f64>, Option<f64>)> =
        sqlx::query_as(
            r#"SELECT trade_date, open::float8, high::float8, low::float8, close::float8,
                      volume, amount::float8, turnover::float8, pe::float8, pb::float8
               FROM stock_daily_bars
               WHERE code = $1
               ORDER BY trade_date DESC
               LIMIT $2"#,
        )
        .bind(code)
        .bind(days as i64)
        .fetch_all(pool)
        .await?;

    let mut bars: Vec<Candle> = rows
        .into_iter()
        .map(|(trade_date, open, high, low, close, volume, amount, turnover, pe, pb)| Candle {
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
        })
        .collect();

    bars.sort_by_key(|b| b.trade_date);
    Ok(bars)
}

/// Fetch all stock codes that have data
pub async fn get_stock_codes_with_data(pool: &PgPool) -> Result<Vec<String>> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT DISTINCT code FROM stock_daily_bars ORDER BY code")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|r| r.0).collect())
}

/// Upsert stock info
pub async fn upsert_stock_info(pool: &PgPool, stocks: &[StockInfo]) -> Result<()> {
    let mut tx = pool.begin().await?;
    for s in stocks {
        sqlx::query(
            r#"INSERT INTO stock_info (code, name, market, industry)
               VALUES ($1,$2,$3,$4)
               ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, industry=EXCLUDED.industry"#,
        )
        .bind(&s.code)
        .bind(&s.name)
        .bind(&s.market)
        .bind(&s.industry)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Save scan results
pub async fn save_scan_results(
    pool: &PgPool,
    run_id: Uuid,
    results: &[(String, String, String, serde_json::Value)], // (code, name, signal_id, metadata)
) -> Result<()> {
    let mut tx = pool.begin().await?;
    for (code, name, signal_id, metadata) in results {
        sqlx::query(
            "INSERT INTO scan_results (run_id, code, name, signal_id, metadata) VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(run_id)
        .bind(code)
        .bind(name)
        .bind(signal_id)
        .bind(metadata)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Save limit-up stocks
pub async fn save_limit_up_stocks(pool: &PgPool, stocks: &[LimitUpStock]) -> Result<()> {
    let mut tx = pool.begin().await?;
    for s in stocks {
        sqlx::query(
            r#"INSERT INTO limit_up_stocks
               (code, trade_date, name, limit_time, seal_amount, burst_count, close, pct_chg, strth)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
               ON CONFLICT (code, trade_date) DO UPDATE SET
               name=EXCLUDED.name, seal_amount=EXCLUDED.seal_amount,
               burst_count=EXCLUDED.burst_count"#,
        )
        .bind(&s.code)
        .bind(s.trade_date)
        .bind(&s.name)
        .bind(&s.first_time)
        .bind(s.fd_amount)
        .bind(s.open_times)
        .bind(s.close)
        .bind(s.pct_chg)
        .bind(s.strth)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Save sector data
pub async fn save_sector_data(pool: &PgPool, sectors: &[SectorData]) -> Result<()> {
    let mut tx = pool.begin().await?;
    for s in sectors {
        sqlx::query(
            r#"INSERT INTO sector_daily (code, name, sector_type, change_pct, amount, trade_date)
               VALUES ($1,$2,$3,$4,$5,$6)
               ON CONFLICT (code, trade_date) DO UPDATE SET
               change_pct=EXCLUDED.change_pct, amount=EXCLUDED.amount"#,
        )
        .bind(&s.code)
        .bind(&s.name)
        .bind(&s.sector_type)
        .bind(s.change_pct)
        .bind(s.amount)
        .bind(s.trade_date)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Get latest report by type
pub async fn get_latest_report(pool: &PgPool, report_type: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT content FROM reports WHERE report_type=$1 ORDER BY generated_at DESC LIMIT 1",
    )
    .bind(report_type)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0))
}

/// Save report
pub async fn save_report(pool: &PgPool, report_type: &str, content: &str) -> Result<()> {
    sqlx::query("INSERT INTO reports (report_type, content) VALUES ($1, $2)")
        .bind(report_type)
        .bind(content)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get stock name by code
pub async fn get_stock_name(pool: &PgPool, code: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT name FROM stock_info WHERE code = $1")
            .bind(code)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0))
}
