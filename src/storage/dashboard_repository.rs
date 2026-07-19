use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;
    use uuid::Uuid;

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
}
