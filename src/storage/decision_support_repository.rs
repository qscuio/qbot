use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::error::Result;

#[derive(Debug, Clone, PartialEq)]
pub struct DecisionSupportRunRow {
    pub run_id: Uuid,
    pub trade_date: NaiveDate,
    pub support_version: String,
    pub market_snapshot_version: String,
    pub pattern_set_id: Option<Uuid>,
    pub event_brief_version: Option<String>,
    pub event_score_enabled: bool,
    pub event_score_limit: f64,
    pub status: String,
    pub input_fingerprint: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecisionCandidateRow {
    pub run_id: Uuid,
    pub code: String,
    pub name: String,
    pub horizon: String,
    pub base_source: String,
    pub base_score: f64,
    pub pattern_score: Option<f64>,
    pub event_adjustment: Option<f64>,
    pub risk_adjustment: Option<f64>,
    pub final_score: f64,
    pub support_tier: String,
    pub facts: Value,
    pub calculations: Value,
    pub inferences: Value,
    pub unknowns: Value,
    pub risk_flags: Value,
    pub invalidations: Value,
    pub source_refs: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecisionBriefRow {
    pub run_id: Uuid,
    pub trade_date: NaiveDate,
    pub content: String,
    pub structured_payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct DecisionSupportRepository {
    pool: PgPool,
}

impl DecisionSupportRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_run(&self, row: &DecisionSupportRunRow) -> Result<Uuid> {
        sqlx::query(
            r#"INSERT INTO analysis_decision_support_runs
               (run_id, trade_date, support_version, market_snapshot_version, pattern_set_id,
                event_brief_version, event_score_enabled, event_score_limit, status,
                input_fingerprint, started_at, completed_at, error_message)
               VALUES ($1, $2, $3, $4, $5,
                       $6, $7, $8, $9,
                       $10, $11, $12, $13)"#,
        )
        .bind(row.run_id)
        .bind(row.trade_date)
        .bind(&row.support_version)
        .bind(&row.market_snapshot_version)
        .bind(row.pattern_set_id)
        .bind(&row.event_brief_version)
        .bind(row.event_score_enabled)
        .bind(row.event_score_limit)
        .bind(&row.status)
        .bind(&row.input_fingerprint)
        .bind(row.started_at)
        .bind(row.completed_at)
        .bind(&row.error_message)
        .execute(&self.pool)
        .await?;

        Ok(row.run_id)
    }

    pub async fn save_candidates(&self, rows: &[DecisionCandidateRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        let mut count = 0usize;
        for row in rows {
            count += insert_candidate(&mut tx, row).await?;
        }
        tx.commit().await?;
        Ok(count)
    }

    pub async fn save_brief(&self, row: &DecisionBriefRow) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO analysis_decision_daily_briefs
               (run_id, trade_date, content, structured_payload, created_at)
               VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind(row.run_id)
        .bind(row.trade_date)
        .bind(&row.content)
        .bind(&row.structured_payload)
        .bind(row.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn latest_run(&self) -> Result<Option<DecisionSupportRunRow>> {
        let row = sqlx::query(
            r#"SELECT run_id,
                      trade_date,
                      support_version,
                      market_snapshot_version,
                      pattern_set_id,
                      event_brief_version,
                      event_score_enabled,
                      event_score_limit::float8 AS event_score_limit,
                      status,
                      input_fingerprint,
                      started_at,
                      completed_at,
                      error_message
               FROM analysis_decision_support_runs
               ORDER BY trade_date DESC, started_at DESC, run_id DESC
               LIMIT 1"#,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| map_run_row(&row)))
    }

    pub async fn list_candidates(&self, run_id: Uuid) -> Result<Vec<DecisionCandidateRow>> {
        let rows = sqlx::query(
            r#"SELECT run_id,
                      code,
                      name,
                      horizon,
                      base_source,
                      base_score::float8 AS base_score,
                      pattern_score::float8 AS pattern_score,
                      event_adjustment::float8 AS event_adjustment,
                      risk_adjustment::float8 AS risk_adjustment,
                      final_score::float8 AS final_score,
                      support_tier,
                      facts,
                      calculations,
                      inferences,
                      unknowns,
                      risk_flags,
                      invalidations,
                      source_refs,
                      created_at
               FROM analysis_decision_candidates
               WHERE run_id = $1
               ORDER BY final_score DESC, code ASC, horizon ASC"#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| map_candidate_row(&row))
            .collect())
    }
}

async fn insert_candidate(
    tx: &mut Transaction<'_, Postgres>,
    row: &DecisionCandidateRow,
) -> Result<usize> {
    let rows_affected = sqlx::query(
        r#"INSERT INTO analysis_decision_candidates
               (run_id, code, name, horizon, base_source, base_score, pattern_score,
                event_adjustment, risk_adjustment, final_score, support_tier, facts,
                calculations, inferences, unknowns, risk_flags, invalidations,
                source_refs, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7,
                       $8, $9, $10, $11, $12,
                       $13, $14, $15, $16, $17,
                       $18, $19)"#,
    )
    .bind(row.run_id)
    .bind(&row.code)
    .bind(&row.name)
    .bind(&row.horizon)
    .bind(&row.base_source)
    .bind(row.base_score)
    .bind(row.pattern_score)
    .bind(row.event_adjustment.unwrap_or(0.0))
    .bind(row.risk_adjustment.unwrap_or(0.0))
    .bind(row.final_score)
    .bind(&row.support_tier)
    .bind(&row.facts)
    .bind(&row.calculations)
    .bind(&row.inferences)
    .bind(&row.unknowns)
    .bind(&row.risk_flags)
    .bind(&row.invalidations)
    .bind(&row.source_refs)
    .bind(row.created_at)
    .execute(&mut **tx)
    .await?
    .rows_affected();

    Ok(rows_affected as usize)
}

fn map_run_row(row: &sqlx::postgres::PgRow) -> DecisionSupportRunRow {
    DecisionSupportRunRow {
        run_id: row.get("run_id"),
        trade_date: row.get("trade_date"),
        support_version: row.get("support_version"),
        market_snapshot_version: row.get("market_snapshot_version"),
        pattern_set_id: row.get("pattern_set_id"),
        event_brief_version: row.get("event_brief_version"),
        event_score_enabled: row.get("event_score_enabled"),
        event_score_limit: row.get("event_score_limit"),
        status: row.get("status"),
        input_fingerprint: row.get("input_fingerprint"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        error_message: row.get("error_message"),
    }
}

fn map_candidate_row(row: &sqlx::postgres::PgRow) -> DecisionCandidateRow {
    DecisionCandidateRow {
        run_id: row.get("run_id"),
        code: row.get("code"),
        name: row.get("name"),
        horizon: row.get("horizon"),
        base_source: row.get("base_source"),
        base_score: row.get("base_score"),
        pattern_score: row.get("pattern_score"),
        event_adjustment: Some(row.get("event_adjustment")),
        risk_adjustment: Some(row.get("risk_adjustment")),
        final_score: row.get("final_score"),
        support_tier: row.get("support_tier"),
        facts: row.get("facts"),
        calculations: row.get("calculations"),
        inferences: row.get("inferences"),
        unknowns: row.get("unknowns"),
        risk_flags: row.get("risk_flags"),
        invalidations: row.get("invalidations"),
        source_refs: row.get("source_refs"),
        created_at: row.get("created_at"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use chrono::TimeZone;
    use serde_json::json;
    use sqlx::PgPool;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    fn run_row(
        run_id: Uuid,
        trade_date: NaiveDate,
        support_version: &str,
        started_at: DateTime<Utc>,
    ) -> DecisionSupportRunRow {
        DecisionSupportRunRow {
            run_id,
            trade_date,
            support_version: support_version.to_string(),
            market_snapshot_version: "snapshot-v1".to_string(),
            pattern_set_id: None,
            event_brief_version: Some("brief-v1".to_string()),
            event_score_enabled: true,
            event_score_limit: 5.0,
            status: "completed".to_string(),
            input_fingerprint: format!("fp-{support_version}-{trade_date}"),
            started_at,
            completed_at: Some(started_at),
            error_message: None,
        }
    }

    fn candidate_row(run_id: Uuid) -> DecisionCandidateRow {
        DecisionCandidateRow {
            run_id,
            code: "600000".to_string(),
            name: "Example Corp".to_string(),
            horizon: "week".to_string(),
            base_source: "scan_ranker".to_string(),
            base_score: 81.25,
            pattern_score: Some(2.75),
            event_adjustment: None,
            risk_adjustment: Some(0.0),
            final_score: 84.0,
            support_tier: "watch".to_string(),
            facts: json!(["fact-1"]),
            calculations: json!(["calc-1"]),
            inferences: json!(["inference-1"]),
            unknowns: json!(["unknown-1"]),
            risk_flags: json!(["risk-1"]),
            invalidations: json!(["invalid-1"]),
            source_refs: json!(["src-1"]),
            created_at: dt(2026, 7, 11, 9),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn create_run_enforces_one_run_per_trade_date_and_support_version(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = DecisionSupportRepository::new(pool);
        let trade_date = date(2026, 7, 11);

        repo.create_run(&run_row(
            Uuid::new_v4(),
            trade_date,
            "support-v1",
            dt(2026, 7, 11, 8),
        ))
        .await
        .unwrap();

        let err = repo
            .create_run(&run_row(
                Uuid::new_v4(),
                trade_date,
                "support-v1",
                dt(2026, 7, 11, 9),
            ))
            .await
            .unwrap_err();

        assert!(matches!(err, AppError::Database(_)));
        assert!(
            err.to_string()
                .contains("analysis_decision_support_runs_trade_date_support_version_key")
                || err.to_string().contains("duplicate key")
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn save_candidates_preserves_reason_buckets_and_uses_zero_adjustment_defaults(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = DecisionSupportRepository::new(pool.clone());
        let run = run_row(
            Uuid::new_v4(),
            date(2026, 7, 11),
            "support-v1",
            dt(2026, 7, 11, 8),
        );
        repo.create_run(&run).await.unwrap();

        let mut candidate = candidate_row(run.run_id);
        candidate.risk_adjustment = None;

        repo.save_candidates(&[candidate]).await.unwrap();

        let rows = repo.list_candidates(run.run_id).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].facts, json!(["fact-1"]));
        assert_eq!(rows[0].calculations, json!(["calc-1"]));
        assert_eq!(rows[0].inferences, json!(["inference-1"]));
        assert_eq!(rows[0].unknowns, json!(["unknown-1"]));
        assert_eq!(rows[0].event_adjustment, Some(0.0));
        assert_eq!(rows[0].risk_adjustment, Some(0.0));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn latest_run_and_brief_round_trip(pool: PgPool) -> sqlx::Result<()> {
        let repo = DecisionSupportRepository::new(pool.clone());
        let older_run = run_row(
            Uuid::new_v4(),
            date(2026, 7, 10),
            "support-v1",
            dt(2026, 7, 10, 8),
        );
        let newer_run = run_row(
            Uuid::new_v4(),
            date(2026, 7, 11),
            "support-v2",
            dt(2026, 7, 11, 8),
        );

        repo.create_run(&older_run).await.unwrap();
        repo.create_run(&newer_run).await.unwrap();
        repo.save_brief(&DecisionBriefRow {
            run_id: newer_run.run_id,
            trade_date: newer_run.trade_date,
            content: "Daily brief".to_string(),
            structured_payload: json!({"headlines": ["A", "B"]}),
            created_at: dt(2026, 7, 11, 10),
        })
        .await
        .unwrap();

        let latest = repo.latest_run().await.unwrap().unwrap();
        assert_eq!(latest.run_id, newer_run.run_id);

        let stored: (String, Value) = sqlx::query_as(
            r#"SELECT content, structured_payload
               FROM analysis_decision_daily_briefs
               WHERE run_id = $1"#,
        )
        .bind(newer_run.run_id)
        .fetch_one(&pool)
        .await?;

        assert_eq!(stored.0, "Daily brief");
        assert_eq!(stored.1, json!({"headlines": ["A", "B"]}));

        Ok(())
    }
}
