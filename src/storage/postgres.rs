use chrono::NaiveDate;
use sqlx::PgPool;
use uuid::Uuid;

use crate::data::types::{Candle, LimitUpStock, SectorData, StockInfo};
use crate::error::{AppError, Result};

#[derive(Debug, Clone, serde::Serialize)]
pub struct StrongLimitUpStock {
    pub code: String,
    pub name: String,
    pub limit_count: i64,
    pub latest_trade_date: NaiveDate,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StartupWatchStock {
    pub code: String,
    pub name: String,
    pub first_limit_date: NaiveDate,
    pub first_limit_close: f64,
}

#[derive(Debug, Clone)]
pub struct SignalOutcomeRow {
    pub signal_id: String,
    pub entry_close: f64,
    pub close_1d: Option<f64>,
    pub close_3d: Option<f64>,
    pub close_5d: Option<f64>,
    pub close_10d: Option<f64>,
}

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

pub async fn list_signal_outcome_samples(
    pool: &PgPool,
    lookback_days: i64,
    signal_id: Option<&str>,
) -> Result<Vec<SignalOutcomeRow>> {
    let days = lookback_days.clamp(1, 3650) as i32;

    let rows: Vec<(
        String,
        f64,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
    )> = sqlx::query_as(
        r#"WITH deduped AS (
                   SELECT DISTINCT ON (
                       sr.signal_id,
                       sr.code,
                       ((sr.scanned_at AT TIME ZONE 'Asia/Shanghai')::date)
                   )
                          sr.signal_id,
                          sr.code,
                          (sr.scanned_at AT TIME ZONE 'Asia/Shanghai')::date AS signal_date
                   FROM scan_results sr
                   WHERE sr.scanned_at >= NOW() - ($1::int * INTERVAL '1 day')
                     AND ($2::text IS NULL OR sr.signal_id = $2)
                   ORDER BY sr.signal_id,
                            sr.code,
                            ((sr.scanned_at AT TIME ZONE 'Asia/Shanghai')::date),
                            sr.scanned_at DESC
               )
               SELECT d.signal_id,
                      entry.close::float8 AS entry_close,
                      h1.close::float8 AS close_1d,
                      h3.close::float8 AS close_3d,
                      h5.close::float8 AS close_5d,
                      h10.close::float8 AS close_10d
               FROM deduped d
               JOIN stock_daily_bars entry
                 ON entry.code = d.code
                AND entry.trade_date = d.signal_date
               LEFT JOIN LATERAL (
                   SELECT b.close
                   FROM stock_daily_bars b
                   WHERE b.code = d.code
                     AND b.trade_date > d.signal_date
                   ORDER BY b.trade_date ASC
                   OFFSET 0
                   LIMIT 1
               ) h1 ON TRUE
               LEFT JOIN LATERAL (
                   SELECT b.close
                   FROM stock_daily_bars b
                   WHERE b.code = d.code
                     AND b.trade_date > d.signal_date
                   ORDER BY b.trade_date ASC
                   OFFSET 2
                   LIMIT 1
               ) h3 ON TRUE
               LEFT JOIN LATERAL (
                   SELECT b.close
                   FROM stock_daily_bars b
                   WHERE b.code = d.code
                     AND b.trade_date > d.signal_date
                   ORDER BY b.trade_date ASC
                   OFFSET 4
                   LIMIT 1
               ) h5 ON TRUE
               LEFT JOIN LATERAL (
                   SELECT b.close
                   FROM stock_daily_bars b
                   WHERE b.code = d.code
                     AND b.trade_date > d.signal_date
                   ORDER BY b.trade_date ASC
                   OFFSET 9
                   LIMIT 1
               ) h10 ON TRUE
               ORDER BY d.signal_id, d.signal_date DESC, d.code"#,
    )
    .bind(days)
    .bind(signal_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(signal_id, entry_close, close_1d, close_3d, close_5d, close_10d)| SignalOutcomeRow {
                signal_id,
                entry_close,
                close_1d,
                close_3d,
                close_5d,
                close_10d,
            },
        )
        .collect())
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

pub async fn latest_limit_up_trade_date(pool: &PgPool) -> Result<Option<NaiveDate>> {
    let row: (Option<NaiveDate>,) = sqlx::query_as("SELECT MAX(trade_date) FROM limit_up_stocks")
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

pub async fn list_strong_limit_up_stocks(
    pool: &PgPool,
    days: i64,
    min_limit_count: i64,
) -> Result<Vec<StrongLimitUpStock>> {
    let lookback_days = days.max(1) - 1;
    let min_hits = min_limit_count.max(1);

    let rows: Vec<(String, String, i64, NaiveDate)> = sqlx::query_as(
        r#"WITH anchor AS (
               SELECT MAX(trade_date) AS trade_date
               FROM limit_up_stocks
           )
           SELECT s.code,
                  COALESCE(MAX(s.name), s.code) AS name,
                  COUNT(*)::bigint AS limit_count,
                  MAX(s.trade_date) AS latest_trade_date
           FROM limit_up_stocks s
           CROSS JOIN anchor a
           WHERE a.trade_date IS NOT NULL
             AND s.trade_date BETWEEN a.trade_date - $1::int AND a.trade_date
           GROUP BY s.code
           HAVING COUNT(*) >= $2
           ORDER BY limit_count DESC, latest_trade_date DESC, s.code"#,
    )
    .bind(lookback_days as i32)
    .bind(min_hits)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(code, name, limit_count, latest_trade_date)| StrongLimitUpStock {
                code,
                name,
                limit_count,
                latest_trade_date,
            },
        )
        .collect())
}

pub async fn rebuild_startup_watchlist(pool: &PgPool, anchor_date: NaiveDate) -> Result<()> {
    let mut tx = pool.begin().await?;

    sqlx::query("DELETE FROM startup_watchlist")
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        r#"INSERT INTO startup_watchlist (code, name, first_limit_date, first_limit_close)
           WITH candidates AS (
               SELECT code,
                      COALESCE(MAX(name), code) AS name,
                      MIN(trade_date) AS first_limit_date,
                      COUNT(*)::bigint AS limit_count
               FROM limit_up_stocks
               WHERE trade_date BETWEEN $1::date - 29 AND $1
               GROUP BY code
               HAVING COUNT(*) = 1
           )
           SELECT c.code,
                  c.name,
                  c.first_limit_date,
                  COALESCE(l.close::float8, 0)
           FROM candidates c
           JOIN limit_up_stocks l
             ON l.code = c.code
            AND l.trade_date = c.first_limit_date
           ORDER BY c.first_limit_date DESC, c.code"#,
    )
    .bind(anchor_date)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

pub async fn list_startup_watchlist(pool: &PgPool) -> Result<Vec<StartupWatchStock>> {
    let rows: Vec<(String, String, NaiveDate, f64)> = sqlx::query_as(
        r#"SELECT code,
                  COALESCE(name, code) AS name,
                  first_limit_date,
                  COALESCE(first_limit_close::float8, 0)
           FROM startup_watchlist
           ORDER BY first_limit_date DESC, code"#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(code, name, first_limit_date, first_limit_close)| StartupWatchStock {
                code,
                name,
                first_limit_date,
                first_limit_close,
            },
        )
        .collect())
}

pub async fn list_limit_up_stocks_by_date(
    pool: &PgPool,
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
        Option<String>,
        Option<i32>,
        Option<f64>,
    )> = sqlx::query_as(
        r#"SELECT code,
                  name,
                  trade_date,
                  close::float8,
                  pct_chg::float8,
                  seal_amount::float8,
                  limit_time,
                  limit_time,
                  burst_count,
                  strth::float8
           FROM limit_up_stocks
           WHERE trade_date = $1
           ORDER BY seal_amount DESC NULLS LAST, code"#,
    )
    .bind(trade_date)
    .fetch_all(pool)
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
                last_time,
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
                last_time,
                open_times: open_times.unwrap_or(0),
                strth: strth.unwrap_or(0.0),
                limit: "U".to_string(),
            },
        )
        .collect())
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
    let row: Option<(String,)> = sqlx::query_as("SELECT name FROM stock_info WHERE code = $1")
        .bind(code)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.0))
}

/// Resolve a flexible stock code into canonical Tushare code (e.g. 600519 -> 600519.SH)
pub async fn resolve_stock_code(pool: &PgPool, raw_code: &str) -> Result<Option<String>> {
    let code = raw_code.trim().to_uppercase();
    if code.is_empty() {
        return Ok(None);
    }

    if code.contains('.') {
        let row: Option<(String,)> = sqlx::query_as(
            r#"SELECT code
               FROM stock_info
               WHERE UPPER(code) = $1
               LIMIT 1"#,
        )
        .bind(code)
        .fetch_optional(pool)
        .await?;
        return Ok(row.map(|r| r.0));
    }

    let pattern = format!("{}.%", code);
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"SELECT code
           FROM stock_info
           WHERE code ILIKE $1
           ORDER BY
             CASE
               WHEN $2 LIKE '6%' AND code LIKE '%.SH' THEN 0
               WHEN $2 NOT LIKE '6%' AND code LIKE '%.SZ' THEN 0
               ELSE 1
             END,
             code
           LIMIT 1"#,
    )
    .bind(pattern)
    .bind(code)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().next().map(|r| r.0))
}

/// Search stocks by code or name.
pub async fn search_stocks(pool: &PgPool, q: &str, limit: i64) -> Result<Vec<StockInfo>> {
    let keyword = q.trim();
    if keyword.is_empty() {
        return Ok(vec![]);
    }

    let lim = limit.clamp(1, 50);
    let pattern = format!("%{}%", keyword);
    let rows: Vec<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
        r#"SELECT code, name, market, industry
           FROM stock_info
           WHERE code ILIKE $1 OR name ILIKE $1
           ORDER BY code
           LIMIT $2"#,
    )
    .bind(pattern)
    .bind(lim)
    .fetch_all(pool)
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

/// Add stock to a user's watchlist.
pub async fn add_watchlist_stock(pool: &PgPool, user_id: i64, code: &str) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO user_watchlist (user_id, code)
           VALUES ($1, $2)
           ON CONFLICT (user_id, code) DO NOTHING"#,
    )
    .bind(user_id)
    .bind(code)
    .execute(pool)
    .await?;
    Ok(())
}

/// Remove stock from a user's watchlist. Returns true if a row was removed.
pub async fn remove_watchlist_stock(pool: &PgPool, user_id: i64, code: &str) -> Result<bool> {
    let res = sqlx::query(
        r#"DELETE FROM user_watchlist
           WHERE user_id = $1 AND code = $2"#,
    )
    .bind(user_id)
    .bind(code)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Check if stock is in a user's watchlist.
pub async fn is_watchlist_stock(pool: &PgPool, user_id: i64, code: &str) -> Result<bool> {
    let row: Option<(bool,)> = sqlx::query_as(
        r#"SELECT EXISTS(
             SELECT 1 FROM user_watchlist WHERE user_id = $1 AND code = $2
           )"#,
    )
    .bind(user_id)
    .bind(code)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(false))
}

/// List a user's watchlist with stock names.
pub async fn list_watchlist_stocks(pool: &PgPool, user_id: i64) -> Result<Vec<(String, String)>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"SELECT w.code, COALESCE(i.name, w.code) AS name
           FROM user_watchlist w
           LEFT JOIN stock_info i ON i.code = w.code
           WHERE w.user_id = $1
           ORDER BY w.added_at DESC"#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn d(value: &str) -> NaiveDate {
        NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap()
    }

    async fn seed_limit_up(
        pool: &PgPool,
        code: &str,
        name: &str,
        trade_date: NaiveDate,
        close: f64,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO limit_up_stocks
               (code, trade_date, name, close)
               VALUES ($1, $2, $3, $4)"#,
        )
        .bind(code)
        .bind(trade_date)
        .bind(name)
        .bind(close)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn strong_limit_up_query_ranks_recent_stocks(pool: PgPool) -> sqlx::Result<()> {
        seed_limit_up(&pool, "600001.SH", "Alpha", d("2026-03-03"), 10.1).await?;
        seed_limit_up(&pool, "600001.SH", "Alpha", d("2026-03-05"), 10.8).await?;
        seed_limit_up(&pool, "600001.SH", "Alpha", d("2026-03-08"), 11.3).await?;

        seed_limit_up(&pool, "600002.SH", "Beta", d("2026-03-03"), 12.0).await?;
        seed_limit_up(&pool, "600002.SH", "Beta", d("2026-03-04"), 12.6).await?;
        seed_limit_up(&pool, "600002.SH", "Beta", d("2026-03-06"), 13.4).await?;
        seed_limit_up(&pool, "600002.SH", "Beta", d("2026-03-09"), 14.1).await?;

        seed_limit_up(&pool, "600003.SH", "Gamma", d("2026-03-04"), 8.9).await?;
        seed_limit_up(&pool, "600003.SH", "Gamma", d("2026-03-09"), 9.3).await?;

        let strong = list_strong_limit_up_stocks(&pool, 7, 3).await.unwrap();

        assert_eq!(strong.len(), 2);
        assert_eq!(strong[0].code, "600002.SH");
        assert_eq!(strong[0].name, "Beta");
        assert_eq!(strong[0].limit_count, 4);
        assert_eq!(strong[1].code, "600001.SH");
        assert_eq!(strong[1].limit_count, 3);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn startup_watchlist_rebuild_keeps_only_single_recent_limit_up(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        seed_limit_up(&pool, "600010.SH", "Solo", d("2026-03-08"), 9.8).await?;

        seed_limit_up(&pool, "600011.SH", "Repeat", d("2026-02-20"), 6.2).await?;
        seed_limit_up(&pool, "600011.SH", "Repeat", d("2026-03-09"), 6.9).await?;

        seed_limit_up(&pool, "600012.SH", "Old", d("2026-01-10"), 5.5).await?;

        rebuild_startup_watchlist(&pool, d("2026-03-09"))
            .await
            .unwrap();
        let items = list_startup_watchlist(&pool).await.unwrap();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].code, "600010.SH");
        assert_eq!(items[0].name, "Solo");
        assert_eq!(items[0].first_limit_date, d("2026-03-08"));
        assert!((items[0].first_limit_close - 9.8).abs() < f64::EPSILON);
        Ok(())
    }
}
