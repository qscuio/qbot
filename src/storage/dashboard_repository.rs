use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::data::types::Candle;
use crate::error::Result;

#[derive(Debug, Clone, FromRow)]
pub struct ScanHitRow {
    pub code: String,
    pub name: String,
    pub signal_id: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, FromRow)]
pub struct ScanRunRow {
    pub run_id: Uuid,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub stocks_checked: i32,
    pub hit_count: i32,
    pub error_summary: Option<String>,
}

#[derive(Debug, FromRow)]
struct WeeklyCandleRow {
    trade_date: NaiveDate,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<i64>,
    amount: Option<f64>,
    turnover: Option<f64>,
    pe: Option<f64>,
    pb: Option<f64>,
}

impl From<WeeklyCandleRow> for Candle {
    fn from(row: WeeklyCandleRow) -> Self {
        Self {
            trade_date: row.trade_date,
            open: row.open.unwrap_or(0.0),
            high: row.high.unwrap_or(0.0),
            low: row.low.unwrap_or(0.0),
            close: row.close.unwrap_or(0.0),
            volume: row.volume.unwrap_or(0),
            amount: row.amount.unwrap_or(0.0),
            turnover: row.turnover,
            pe: row.pe,
            pb: row.pb,
        }
    }
}

#[derive(Clone)]
pub struct DashboardRepository {
    pool: PgPool,
}

impl DashboardRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn start_scan_run(&self, run_id: Uuid) -> Result<()> {
        sqlx::query("INSERT INTO scan_runs (run_id, status) VALUES ($1, 'running')")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn complete_scan_run(
        &self,
        run_id: Uuid,
        stocks_checked: usize,
        hit_count: usize,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE scan_runs
               SET status = 'completed',
                   completed_at = NOW(),
                   stocks_checked = $2,
                   hit_count = $3,
                   error_summary = NULL
               WHERE run_id = $1"#,
        )
        .bind(run_id)
        .bind(stocks_checked as i32)
        .bind(hit_count as i32)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_scan_run(&self, run_id: Uuid, error: &str) -> Result<()> {
        let summary: String = error.chars().take(500).collect();
        sqlx::query(
            r#"UPDATE scan_runs
               SET status = 'failed', completed_at = NOW(), error_summary = $2
               WHERE run_id = $1"#,
        )
        .bind(run_id)
        .bind(summary)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn latest_completed_scan(&self) -> Result<Option<ScanRunRow>> {
        Ok(sqlx::query_as::<_, ScanRunRow>(
            r#"SELECT run_id, status, started_at, completed_at,
                      stocks_checked, hit_count, error_summary
               FROM scan_runs
               WHERE status = 'completed'
               ORDER BY completed_at DESC, started_at DESC
               LIMIT 1"#,
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn scan_hits(&self, run_id: Uuid) -> Result<Vec<ScanHitRow>> {
        Ok(sqlx::query_as::<_, ScanHitRow>(
            r#"SELECT sr.code, COALESCE(si.name, sr.name, sr.code) AS name,
                      sr.signal_id, sr.metadata
               FROM scan_results sr
               JOIN stock_info si ON si.code = sr.code
               WHERE sr.run_id = $1
               ORDER BY sr.code, sr.signal_id"#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn weekly_history(&self, code: &str) -> Result<Vec<Candle>> {
        let rows = sqlx::query_as::<_, WeeklyCandleRow>(
            r#"SELECT MAX(trade_date) AS trade_date,
                      (ARRAY_AGG(open::float8 ORDER BY trade_date))[1] AS open,
                      MAX(high)::float8 AS high,
                      MIN(low)::float8 AS low,
                      (ARRAY_AGG(close::float8 ORDER BY trade_date DESC))[1] AS close,
                      SUM(volume)::int8 AS volume,
                      SUM(amount)::float8 AS amount,
                      (ARRAY_AGG(turnover::float8 ORDER BY trade_date DESC)
                         FILTER (WHERE turnover IS NOT NULL))[1] AS turnover,
                      (ARRAY_AGG(pe::float8 ORDER BY trade_date DESC)
                         FILTER (WHERE pe IS NOT NULL))[1] AS pe,
                      (ARRAY_AGG(pb::float8 ORDER BY trade_date DESC)
                         FILTER (WHERE pb IS NOT NULL))[1] AS pb
               FROM stock_daily_bars
               WHERE code = $1
               GROUP BY date_trunc('week', trade_date)
               ORDER BY trade_date"#,
        )
        .bind(code)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;
    use uuid::Uuid;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn scan_run_lifecycle_persists_zero_hit_completion(pool: PgPool) {
        let repo = DashboardRepository::new(pool.clone());
        let run_id = Uuid::new_v4();

        repo.start_scan_run(run_id).await.unwrap();
        repo.complete_scan_run(run_id, 5_000, 0).await.unwrap();

        let latest = repo.latest_completed_scan().await.unwrap().unwrap();
        assert_eq!(latest.run_id, run_id);
        assert_eq!(latest.hit_count, 0);
        assert_eq!(latest.status, "completed");

        let failed_run = Uuid::new_v4();
        repo.start_scan_run(failed_run).await.unwrap();
        repo.fail_scan_run(failed_run, &"x".repeat(700))
            .await
            .unwrap();

        let latest_after_failure = repo.latest_completed_scan().await.unwrap().unwrap();
        assert_eq!(latest_after_failure.run_id, run_id);

        let failure: (String, Option<String>) =
            sqlx::query_as("SELECT status, error_summary FROM scan_runs WHERE run_id = $1")
                .bind(failed_run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(failure.0, "failed");
        assert_eq!(failure.1.unwrap().chars().count(), 500);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dashboard_scan_hits_hide_codes_missing_from_current_stock_master(pool: PgPool) {
        let run_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO stock_info (code, name, market)
               VALUES ('000001.SZ', '平安银行', 'SZ')"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO scan_results (run_id, code, name, signal_id, metadata)
               VALUES
               ($1, '000001.SZ', '平安银行', 'ma_bullish', '{}'),
               ($1, '000618.SZ', '000618.SZ', 'ma_bullish', '{}')"#,
        )
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
        let repo = DashboardRepository::new(pool);

        let hits = repo.scan_hits(run_id).await.unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].code, "000001.SZ");
        assert_eq!(hits[0].name, "平安银行");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn weekly_history_aggregates_every_stored_daily_bar(pool: PgPool) {
        sqlx::query(
            r#"INSERT INTO stock_daily_bars
               (code, trade_date, open, high, low, close, volume, amount, turnover, pe, pb)
               SELECT '600519.SH', trade_date, 10, 11, 9, 10.5, 100, 1000, 1, 10, 1
               FROM (
                   SELECT stored_day::date AS trade_date
                   FROM generate_series('2024-01-01'::date, '2025-12-31'::date, '1 day') stored_day
                   WHERE EXTRACT(ISODOW FROM stored_day) <= 5
                   ORDER BY stored_day
                   LIMIT 501
               ) stored_days"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO stock_daily_bars
               (code, trade_date, open, high, low, close, volume, amount, turnover, pe, pb)
               VALUES
               ('600519.SH', '2026-01-05', 100, 110, 95, 105, 100, 1000, 1, NULL, 1),
               ('600519.SH', '2026-01-06', 105, 115, 100, 110, 200, 2000, NULL, 20, NULL),
               ('600519.SH', '2026-01-07', 110, 120, 90, 115, 300, 3000, 3, NULL, 3)"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let weekly = DashboardRepository::new(pool)
            .weekly_history("600519.SH")
            .await
            .unwrap();

        assert!(weekly.len() > 100);
        assert_eq!(weekly.first().unwrap().trade_date, date(2024, 1, 5));

        let final_week = weekly.last().unwrap();
        assert_eq!(final_week.trade_date, date(2026, 1, 7));
        assert_eq!(final_week.open, 100.0);
        assert_eq!(final_week.high, 120.0);
        assert_eq!(final_week.low, 90.0);
        assert_eq!(final_week.close, 115.0);
        assert_eq!(final_week.volume, 600);
        assert_eq!(final_week.amount, 6_000.0);
        assert_eq!(final_week.turnover, Some(3.0));
        assert_eq!(final_week.pe, Some(20.0));
        assert_eq!(final_week.pb, Some(3.0));
    }
}
