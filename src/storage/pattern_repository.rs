use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::{AppError, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct PatternVersionRow {
    pub pattern_version_id: Uuid,
    pub pattern_id: String,
    pub horizon: String,
    pub pattern_type: String,
    pub status: String,
    pub schema_version: String,
    pub feature_version: String,
    pub logic_version: String,
    pub dataset_version: String,
    pub model_payload: Value,
    pub validation_payload: Value,
    pub trained_from: NaiveDate,
    pub trained_until: NaiveDate,
    pub available_at_cutoff: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternSetRow {
    pub pattern_set_id: Uuid,
    pub name: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShadowCandidateRow {
    pub trade_date: NaiveDate,
    pub code: String,
    pub name: Option<String>,
    pub horizon: String,
    pub pattern_version_id: Uuid,
    pub pattern_set_id: Uuid,
    pub pattern_type: String,
    pub similarity_score: f64,
    pub validated_lift: f64,
    pub final_score: f64,
    pub shadow_tier: String,
    pub matched_features: Value,
    pub risk_flags: Value,
    pub supporting_signals: Value,
    pub invalidations: Value,
    pub input_fingerprint: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct PatternRepository {
    pool: PgPool,
}

impl PatternRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list_published_patterns(
        &self,
        pattern_set_id: Uuid,
    ) -> Result<Vec<PatternVersionRow>> {
        let rows = sqlx::query(
            r#"SELECT pv.pattern_version_id,
                      pv.pattern_id,
                      pv.horizon,
                      pv.pattern_type,
                      pv.status,
                      pv.schema_version,
                      pv.feature_version,
                      pv.logic_version,
                      pv.dataset_version,
                      pv.model_payload,
                      pv.validation_payload,
                      pv.trained_from,
                      pv.trained_until,
                      pv.available_at_cutoff,
                      pv.approved_by,
                      pv.published_at,
                      pv.created_at
               FROM analysis_pattern_set_members psm
               INNER JOIN analysis_pattern_sets ps
                   ON ps.pattern_set_id = psm.pattern_set_id
               INNER JOIN analysis_pattern_versions pv
                   ON pv.pattern_version_id = psm.pattern_version_id
               WHERE psm.pattern_set_id = $1
                 AND ps.status = 'published'
                 AND ps.published_at IS NOT NULL
                 AND pv.status = 'published'
                 AND pv.horizon IN ('week', 'month')
                 AND pv.approved_by IS NOT NULL
                 AND pv.published_at IS NOT NULL
               ORDER BY psm.member_order ASC"#,
        )
        .bind(pattern_set_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| PatternVersionRow {
                pattern_version_id: row.get("pattern_version_id"),
                pattern_id: row.get("pattern_id"),
                horizon: row.get("horizon"),
                pattern_type: row.get("pattern_type"),
                status: row.get("status"),
                schema_version: row.get("schema_version"),
                feature_version: row.get("feature_version"),
                logic_version: row.get("logic_version"),
                dataset_version: row.get("dataset_version"),
                model_payload: row.get("model_payload"),
                validation_payload: row.get("validation_payload"),
                trained_from: row.get("trained_from"),
                trained_until: row.get("trained_until"),
                available_at_cutoff: row.get("available_at_cutoff"),
                approved_by: row.get("approved_by"),
                published_at: row.get("published_at"),
                created_at: row.get("created_at"),
            })
            .collect())
    }

    pub async fn upsert_shadow_candidates(&self, rows: &[ShadowCandidateRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        for row in rows {
            validate_shadow_tier(&row.shadow_tier)?;
        }

        let mut tx = self.pool.begin().await?;
        let mut count = 0usize;
        for row in rows {
            let rows_affected = sqlx::query(
                r#"INSERT INTO analysis_shadow_candidates
                   (trade_date, code, name, horizon, pattern_version_id, pattern_set_id,
                    pattern_type, similarity_score, validated_lift, final_score,
                    shadow_tier, matched_features, risk_flags, supporting_signals,
                    invalidations, input_fingerprint, created_at)
                   SELECT $1, $2, $3, $4, $5, $6,
                          $7, $8, $9, $10,
                          $11, $12, $13, $14,
                          $15, $16, $17
                   FROM analysis_pattern_set_members psm
                   INNER JOIN analysis_pattern_sets ps
                       ON ps.pattern_set_id = psm.pattern_set_id
                   INNER JOIN analysis_pattern_versions pv
                       ON pv.pattern_version_id = psm.pattern_version_id
                   WHERE psm.pattern_set_id = $6
                     AND psm.pattern_version_id = $5
                     AND pv.horizon = $4
                     AND pv.pattern_type = $7
                     AND pv.status = 'published'
                     AND pv.horizon IN ('week', 'month')
                     AND pv.approved_by IS NOT NULL
                     AND pv.published_at IS NOT NULL
                     AND ps.status = 'published'
                     AND ps.published_at IS NOT NULL
                   ON CONFLICT (trade_date, code, horizon, pattern_version_id) DO UPDATE SET
                       pattern_set_id = EXCLUDED.pattern_set_id,
                       name = EXCLUDED.name,
                       pattern_type = EXCLUDED.pattern_type,
                       similarity_score = EXCLUDED.similarity_score,
                       validated_lift = EXCLUDED.validated_lift,
                       final_score = EXCLUDED.final_score,
                       shadow_tier = EXCLUDED.shadow_tier,
                       matched_features = EXCLUDED.matched_features,
                       risk_flags = EXCLUDED.risk_flags,
                       supporting_signals = EXCLUDED.supporting_signals,
                       invalidations = EXCLUDED.invalidations,
                       input_fingerprint = EXCLUDED.input_fingerprint,
                       created_at = EXCLUDED.created_at"#,
            )
            .bind(row.trade_date)
            .bind(&row.code)
            .bind(&row.name)
            .bind(&row.horizon)
            .bind(row.pattern_version_id)
            .bind(row.pattern_set_id)
            .bind(&row.pattern_type)
            .bind(row.similarity_score)
            .bind(row.validated_lift)
            .bind(row.final_score)
            .bind(&row.shadow_tier)
            .bind(&row.matched_features)
            .bind(&row.risk_flags)
            .bind(&row.supporting_signals)
            .bind(&row.invalidations)
            .bind(&row.input_fingerprint)
            .bind(row.created_at)
            .execute(&mut *tx)
            .await?
            .rows_affected() as usize;

            if rows_affected != 1 {
                return Err(AppError::Internal(format!(
                    "shadow candidate does not match a published week/month pattern in a published set: code={}, trade_date={}, horizon={}, pattern_version_id={}, pattern_set_id={}",
                    row.code, row.trade_date, row.horizon, row.pattern_version_id, row.pattern_set_id
                )));
            }

            count += rows_affected;
        }
        tx.commit().await?;
        Ok(count)
    }

    pub async fn latest_published_set(&self) -> Result<Option<PatternSetRow>> {
        let row: Option<(Uuid, String, String, DateTime<Utc>, Option<DateTime<Utc>>)> =
            sqlx::query_as(
                r#"SELECT pattern_set_id, name, status, created_at, published_at
                   FROM analysis_pattern_sets
                   WHERE status = 'published'
                     AND published_at IS NOT NULL
                   ORDER BY published_at DESC NULLS LAST, created_at DESC
                   LIMIT 1"#,
            )
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(
            |(pattern_set_id, name, status, created_at, published_at)| PatternSetRow {
                pattern_set_id,
                name,
                status,
                created_at,
                published_at,
            },
        ))
    }

    pub async fn list_shadow_candidates(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<ShadowCandidateRow>> {
        let rows = sqlx::query(
            r#"SELECT trade_date,
                      code,
                      name,
                      horizon,
                      pattern_version_id,
                      pattern_set_id,
                      pattern_type,
                      similarity_score::float8 AS similarity_score,
                      validated_lift::float8 AS validated_lift,
                      final_score::float8 AS final_score,
                      shadow_tier,
                      matched_features,
                      risk_flags,
                      supporting_signals,
                      invalidations,
                      input_fingerprint,
                      created_at
               FROM analysis_shadow_candidates
               WHERE trade_date = $1
               ORDER BY final_score DESC, code ASC, horizon ASC, pattern_version_id ASC"#,
        )
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ShadowCandidateRow {
                trade_date: row.get("trade_date"),
                code: row.get("code"),
                name: row.get("name"),
                horizon: row.get("horizon"),
                pattern_version_id: row.get("pattern_version_id"),
                pattern_set_id: row.get("pattern_set_id"),
                pattern_type: row.get("pattern_type"),
                similarity_score: row.get("similarity_score"),
                validated_lift: row.get("validated_lift"),
                final_score: row.get("final_score"),
                shadow_tier: row.get("shadow_tier"),
                matched_features: row.get("matched_features"),
                risk_flags: row.get("risk_flags"),
                supporting_signals: row.get("supporting_signals"),
                invalidations: row.get("invalidations"),
                input_fingerprint: row.get("input_fingerprint"),
                created_at: row.get("created_at"),
            })
            .collect())
    }
}

fn validate_shadow_tier(shadow_tier: &str) -> Result<()> {
    match shadow_tier {
        "shadow_a" | "shadow_b" | "watch" | "reject" => Ok(()),
        _ => Err(AppError::Internal(format!(
            "invalid shadow_tier {}; expected one of shadow_a, shadow_b, watch, reject",
            shadow_tier
        ))),
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

    async fn insert_dataset_manifest(pool: &PgPool, dataset_version: &str) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO analysis_dataset_manifests
               (dataset_version, schema_version, feature_version, horizon, data_cutoff,
                available_at_cutoff, row_count, date_from, date_to, manifest, input_fingerprint)
               VALUES ($1, 'v1', 'f1', 'week', '2026-06-30', '2026-07-01T00:00:00Z',
                       10, '2026-01-01', '2026-06-30', '{"files":["x.parquet"]}', 'fp-1')"#,
        )
        .bind(dataset_version)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_pattern_version(
        pool: &PgPool,
        pattern_version_id: Uuid,
        pattern_id: &str,
        dataset_version: &str,
        status: &str,
        horizon: &str,
        pattern_type: &str,
        approved_by: Option<&str>,
        published_at: Option<DateTime<Utc>>,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO analysis_pattern_versions
               (pattern_version_id, pattern_id, horizon, pattern_type, status,
                schema_version, feature_version, logic_version, dataset_version,
                model_payload, validation_payload, trained_from, trained_until,
                available_at_cutoff, approved_by, published_at)
               VALUES ($1, $2, $3, $4, $5,
                       'schema-v1', 'feature-v1', 'logic-v1', $6,
                       '{"centroid":{"close_strength":1.1}}',
                       '{"lift":0.42}',
                       '2026-01-01', '2026-06-30', '2026-07-01T00:00:00Z',
                       $7, $8)"#,
        )
        .bind(pattern_version_id)
        .bind(pattern_id)
        .bind(horizon)
        .bind(pattern_type)
        .bind(status)
        .bind(dataset_version)
        .bind(approved_by)
        .bind(published_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_pattern_set(
        pool: &PgPool,
        pattern_set_id: Uuid,
        name: &str,
        status: &str,
        published_at: Option<DateTime<Utc>>,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO analysis_pattern_sets (pattern_set_id, name, status, published_at)
               VALUES ($1, $2, $3, $4)"#,
        )
        .bind(pattern_set_id)
        .bind(name)
        .bind(status)
        .bind(published_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_pattern_set_member(
        pool: &PgPool,
        pattern_set_id: Uuid,
        pattern_version_id: Uuid,
        member_order: i32,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO analysis_pattern_set_members
               (pattern_set_id, pattern_version_id, member_order)
               VALUES ($1, $2, $3)"#,
        )
        .bind(pattern_set_id)
        .bind(pattern_version_id)
        .bind(member_order)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn draft_model_insertion_round_trips(pool: PgPool) -> sqlx::Result<()> {
        insert_dataset_manifest(&pool, "dataset-draft").await?;
        let pattern_version_id = Uuid::new_v4();

        insert_pattern_version(
            &pool,
            pattern_version_id,
            "trend-weekly",
            "dataset-draft",
            "draft",
            "week",
            "trend",
            None,
            None,
        )
        .await?;

        let row: (String, String, String) = sqlx::query_as(
            r#"SELECT pattern_id, status, dataset_version
               FROM analysis_pattern_versions
               WHERE pattern_version_id = $1"#,
        )
        .bind(pattern_version_id)
        .fetch_one(&pool)
        .await?;

        assert_eq!(row.0, "trend-weekly");
        assert_eq!(row.1, "draft");
        assert_eq!(row.2, "dataset-draft");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn published_model_query_returns_only_published_members_from_published_set(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        insert_dataset_manifest(&pool, "dataset-published").await?;
        let published_id = Uuid::new_v4();
        let draft_id = Uuid::new_v4();
        let other_set_published_id = Uuid::new_v4();
        let published_set_id = Uuid::new_v4();
        let draft_set_id = Uuid::new_v4();

        insert_pattern_version(
            &pool,
            published_id,
            "trend-published",
            "dataset-published",
            "published",
            "week",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 8)),
        )
        .await?;
        insert_pattern_version(
            &pool,
            draft_id,
            "trend-draft",
            "dataset-published",
            "draft",
            "week",
            "trend",
            None,
            None,
        )
        .await?;
        insert_pattern_version(
            &pool,
            other_set_published_id,
            "vcp-published",
            "dataset-published",
            "published",
            "month",
            "vcp_breakout",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 9)),
        )
        .await?;

        insert_pattern_set(
            &pool,
            published_set_id,
            "published-set",
            "published",
            Some(dt(2026, 7, 10, 10)),
        )
        .await?;
        insert_pattern_set(&pool, draft_set_id, "draft-set", "draft", None).await?;

        insert_pattern_set_member(&pool, published_set_id, draft_id, 1).await?;
        insert_pattern_set_member(&pool, published_set_id, published_id, 2).await?;
        insert_pattern_set_member(&pool, draft_set_id, other_set_published_id, 1).await?;

        let repo = PatternRepository::new(pool);
        let rows = repo
            .list_published_patterns(published_set_id)
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pattern_version_id, published_id);
        assert_eq!(rows[0].pattern_id, "trend-published");
        assert_eq!(rows[0].status, "published");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pattern_set_rejects_duplicate_members_and_member_order(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        insert_dataset_manifest(&pool, "dataset-members").await?;
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        let set_id = Uuid::new_v4();

        insert_pattern_version(
            &pool,
            first_id,
            "pattern-one",
            "dataset-members",
            "published",
            "week",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 8)),
        )
        .await?;
        insert_pattern_version(
            &pool,
            second_id,
            "pattern-two",
            "dataset-members",
            "published",
            "week",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 9)),
        )
        .await?;
        insert_pattern_set(
            &pool,
            set_id,
            "duplicate-check",
            "published",
            Some(dt(2026, 7, 10, 10)),
        )
        .await?;

        insert_pattern_set_member(&pool, set_id, first_id, 1).await?;

        let duplicate_member = insert_pattern_set_member(&pool, set_id, first_id, 2).await;
        assert!(duplicate_member.is_err());

        let duplicate_order = insert_pattern_set_member(&pool, set_id, second_id, 1).await;
        assert!(duplicate_order.is_err());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn published_pattern_versions_reject_forbidden_horizons_and_manual_metadata(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        insert_dataset_manifest(&pool, "dataset-invalid-published").await?;

        let forbidden_quarter = insert_pattern_version(
            &pool,
            Uuid::new_v4(),
            "quarter-pattern",
            "dataset-invalid-published",
            "published",
            "quarter",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 8)),
        )
        .await;
        assert!(forbidden_quarter.is_err());

        let forbidden_year = insert_pattern_version(
            &pool,
            Uuid::new_v4(),
            "year-pattern",
            "dataset-invalid-published",
            "published",
            "year",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 8)),
        )
        .await;
        assert!(forbidden_year.is_err());

        let missing_approval = insert_pattern_version(
            &pool,
            Uuid::new_v4(),
            "missing-approval",
            "dataset-invalid-published",
            "published",
            "week",
            "trend",
            None,
            Some(dt(2026, 7, 10, 8)),
        )
        .await;
        assert!(missing_approval.is_err());

        let missing_published_at = insert_pattern_version(
            &pool,
            Uuid::new_v4(),
            "missing-published-at",
            "dataset-invalid-published",
            "published",
            "month",
            "trend",
            Some("reviewer"),
            None,
        )
        .await;
        assert!(missing_published_at.is_err());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn published_pattern_sets_require_published_at(pool: PgPool) -> sqlx::Result<()> {
        let result = insert_pattern_set(
            &pool,
            Uuid::new_v4(),
            "invalid-published-set",
            "published",
            None,
        )
        .await;

        assert!(result.is_err());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn published_readers_ignore_rows_that_break_manual_publish_invariants(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        insert_dataset_manifest(&pool, "dataset-reader-filter").await?;

        sqlx::query(
            r#"ALTER TABLE analysis_pattern_versions
               DROP CONSTRAINT IF EXISTS analysis_pattern_versions_published_contract_check"#,
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            r#"ALTER TABLE analysis_pattern_sets
               DROP CONSTRAINT IF EXISTS analysis_pattern_sets_published_contract_check"#,
        )
        .execute(&pool)
        .await?;

        let valid_pattern_id = Uuid::new_v4();
        let invalid_pattern_id = Uuid::new_v4();
        let valid_set_id = Uuid::new_v4();
        let invalid_set_id = Uuid::new_v4();

        insert_pattern_version(
            &pool,
            valid_pattern_id,
            "valid-pattern",
            "dataset-reader-filter",
            "published",
            "week",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 8)),
        )
        .await?;
        insert_pattern_version(
            &pool,
            invalid_pattern_id,
            "invalid-pattern",
            "dataset-reader-filter",
            "published",
            "quarter",
            "trend",
            None,
            None,
        )
        .await?;

        insert_pattern_set(
            &pool,
            valid_set_id,
            "valid-set",
            "published",
            Some(dt(2026, 7, 10, 9)),
        )
        .await?;
        insert_pattern_set(&pool, invalid_set_id, "invalid-set", "published", None).await?;

        insert_pattern_set_member(&pool, valid_set_id, valid_pattern_id, 1).await?;
        insert_pattern_set_member(&pool, valid_set_id, invalid_pattern_id, 2).await?;

        let repo = PatternRepository::new(pool);
        let patterns = repo.list_published_patterns(valid_set_id).await.unwrap();
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].pattern_version_id, valid_pattern_id);

        let latest_set = repo.latest_published_set().await.unwrap().unwrap();
        assert_eq!(latest_set.pattern_set_id, valid_set_id);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn upsert_shadow_candidates_rejects_inconsistent_metadata_and_set_membership(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        insert_dataset_manifest(&pool, "dataset-shadow-invalid").await?;
        let pattern_version_id = Uuid::new_v4();
        let member_set_id = Uuid::new_v4();
        let non_member_set_id = Uuid::new_v4();

        insert_pattern_version(
            &pool,
            pattern_version_id,
            "pattern-shadow-invalid",
            "dataset-shadow-invalid",
            "published",
            "week",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 8)),
        )
        .await?;
        insert_pattern_set(
            &pool,
            member_set_id,
            "member-set",
            "published",
            Some(dt(2026, 7, 10, 9)),
        )
        .await?;
        insert_pattern_set(
            &pool,
            non_member_set_id,
            "non-member-set",
            "published",
            Some(dt(2026, 7, 10, 10)),
        )
        .await?;
        insert_pattern_set_member(&pool, member_set_id, pattern_version_id, 1).await?;

        let repo = PatternRepository::new(pool);
        let trade_date = date(2026, 7, 10);

        let mismatched_metadata = ShadowCandidateRow {
            trade_date,
            code: "600001.SH".to_string(),
            name: Some("Alpha Bank".to_string()),
            horizon: "month".to_string(),
            pattern_version_id,
            pattern_set_id: member_set_id,
            pattern_type: "vcp_breakout".to_string(),
            similarity_score: 0.71,
            validated_lift: 0.12,
            final_score: 1.2345,
            shadow_tier: "watch".to_string(),
            matched_features: json!({"close_strength": 1.1}),
            risk_flags: json!(["extended"]),
            supporting_signals: json!(["scan_ranker"]),
            invalidations: json!([]),
            input_fingerprint: "shadow-fp-invalid-meta".to_string(),
            created_at: dt(2026, 7, 10, 10),
        };

        let missing_membership = ShadowCandidateRow {
            trade_date,
            code: "600002.SH".to_string(),
            name: Some("Beta Steel".to_string()),
            horizon: "week".to_string(),
            pattern_version_id,
            pattern_set_id: non_member_set_id,
            pattern_type: "trend".to_string(),
            similarity_score: 0.72,
            validated_lift: 0.13,
            final_score: 1.3456,
            shadow_tier: "watch".to_string(),
            matched_features: json!({"close_strength": 1.2}),
            risk_flags: json!(["extended"]),
            supporting_signals: json!(["scan_ranker"]),
            invalidations: json!([]),
            input_fingerprint: "shadow-fp-missing-member".to_string(),
            created_at: dt(2026, 7, 10, 11),
        };

        let mismatched_error = repo
            .upsert_shadow_candidates(&[mismatched_metadata])
            .await
            .unwrap_err();
        assert!(matches!(mismatched_error, AppError::Internal(_)));

        let membership_error = repo
            .upsert_shadow_candidates(&[missing_membership])
            .await
            .unwrap_err();
        assert!(matches!(membership_error, AppError::Internal(_)));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn upsert_shadow_candidates_rejects_invalid_shadow_tier_before_sql(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = PatternRepository::new(pool);
        let row = ShadowCandidateRow {
            trade_date: date(2026, 7, 10),
            code: "600001.SH".to_string(),
            name: Some("Alpha Bank".to_string()),
            horizon: "week".to_string(),
            pattern_version_id: Uuid::new_v4(),
            pattern_set_id: Uuid::new_v4(),
            pattern_type: "trend".to_string(),
            similarity_score: 0.71,
            validated_lift: 0.12,
            final_score: 1.2345,
            shadow_tier: "tier1".to_string(),
            matched_features: json!({"close_strength": 1.1}),
            risk_flags: json!(["extended"]),
            supporting_signals: json!(["scan_ranker"]),
            invalidations: json!([]),
            input_fingerprint: "shadow-fp-invalid-tier".to_string(),
            created_at: dt(2026, 7, 10, 10),
        };

        let error = repo.upsert_shadow_candidates(&[row]).await.unwrap_err();

        assert!(matches!(error, AppError::Internal(_)));
        assert!(error.to_string().contains("invalid shadow_tier"));
        assert!(error.to_string().contains("tier1"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn duplicate_shadow_candidate_upserts_are_deterministic(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        insert_dataset_manifest(&pool, "dataset-shadow").await?;
        let pattern_version_id = Uuid::new_v4();
        let pattern_set_id = Uuid::new_v4();

        insert_pattern_version(
            &pool,
            pattern_version_id,
            "pattern-shadow",
            "dataset-shadow",
            "published",
            "week",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 8)),
        )
        .await?;
        insert_pattern_set(
            &pool,
            pattern_set_id,
            "shadow-set",
            "published",
            Some(dt(2026, 7, 10, 9)),
        )
        .await?;
        insert_pattern_set_member(&pool, pattern_set_id, pattern_version_id, 1).await?;

        let repo = PatternRepository::new(pool);
        let trade_date = date(2026, 7, 10);
        let first = ShadowCandidateRow {
            trade_date,
            code: "600000.SH".to_string(),
            name: Some("Pudong Bank".to_string()),
            horizon: "week".to_string(),
            pattern_version_id,
            pattern_set_id,
            pattern_type: "trend".to_string(),
            similarity_score: 0.71,
            validated_lift: 0.12,
            final_score: 1.2345,
            shadow_tier: "watch".to_string(),
            matched_features: json!({"close_strength": 1.1}),
            risk_flags: json!(["extended"]),
            supporting_signals: json!(["scan_ranker"]),
            invalidations: json!([]),
            input_fingerprint: "shadow-fp-1".to_string(),
            created_at: dt(2026, 7, 10, 10),
        };
        let mut second = first.clone();
        second.similarity_score = 0.92;
        second.name = Some("Pudong Bank Updated".to_string());
        second.validated_lift = 0.22;
        second.final_score = 2.3456;
        second.shadow_tier = "shadow_b".to_string();
        second.matched_features = json!({"close_strength": 2.2});
        second.risk_flags = json!(["none"]);
        second.supporting_signals = json!(["scan_ranker", "pattern_engine"]);
        second.invalidations = json!(["late_breakout"]);
        second.input_fingerprint = "shadow-fp-2".to_string();
        second.created_at = dt(2026, 7, 10, 11);

        assert_eq!(repo.upsert_shadow_candidates(&[first]).await.unwrap(), 1);
        assert_eq!(
            repo.upsert_shadow_candidates(&[second.clone()])
                .await
                .unwrap(),
            1
        );

        let rows = repo.list_shadow_candidates(trade_date).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], second);
        assert_eq!(rows[0].name.as_deref(), Some("Pudong Bank Updated"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn duplicate_shadow_candidate_upserts_within_one_batch_are_deterministic(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        insert_dataset_manifest(&pool, "dataset-shadow-batch").await?;
        let pattern_version_id = Uuid::new_v4();
        let pattern_set_id = Uuid::new_v4();

        insert_pattern_version(
            &pool,
            pattern_version_id,
            "pattern-shadow-batch",
            "dataset-shadow-batch",
            "published",
            "week",
            "trend",
            Some("reviewer"),
            Some(dt(2026, 7, 10, 8)),
        )
        .await?;
        insert_pattern_set(
            &pool,
            pattern_set_id,
            "shadow-batch-set",
            "published",
            Some(dt(2026, 7, 10, 9)),
        )
        .await?;
        insert_pattern_set_member(&pool, pattern_set_id, pattern_version_id, 1).await?;

        let repo = PatternRepository::new(pool);
        let trade_date = date(2026, 7, 10);
        let first = ShadowCandidateRow {
            trade_date,
            code: "600003.SH".to_string(),
            name: Some("Gamma Energy".to_string()),
            horizon: "week".to_string(),
            pattern_version_id,
            pattern_set_id,
            pattern_type: "trend".to_string(),
            similarity_score: 0.55,
            validated_lift: 0.08,
            final_score: 0.9876,
            shadow_tier: "watch".to_string(),
            matched_features: json!({"close_strength": 0.9}),
            risk_flags: json!(["extended"]),
            supporting_signals: json!(["scan_ranker"]),
            invalidations: json!([]),
            input_fingerprint: "shadow-batch-fp-1".to_string(),
            created_at: dt(2026, 7, 10, 10),
        };
        let mut second = first.clone();
        second.similarity_score = 0.83;
        second.name = Some("Gamma Energy Updated".to_string());
        second.validated_lift = 0.19;
        second.final_score = 1.8765;
        second.shadow_tier = "shadow_b".to_string();
        second.matched_features = json!({"close_strength": 1.8});
        second.risk_flags = json!(["none"]);
        second.supporting_signals = json!(["scan_ranker", "pattern_engine"]);
        second.invalidations = json!(["late_breakout"]);
        second.input_fingerprint = "shadow-batch-fp-2".to_string();
        second.created_at = dt(2026, 7, 10, 11);

        assert_eq!(
            repo.upsert_shadow_candidates(&[first.clone(), second.clone()])
                .await
                .unwrap(),
            2
        );

        let rows = repo.list_shadow_candidates(trade_date).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], second);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn latest_published_set_returns_most_recent_publication(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let draft_set_id = Uuid::new_v4();
        let older_published_set_id = Uuid::new_v4();
        let newer_published_set_id = Uuid::new_v4();

        insert_pattern_set(&pool, draft_set_id, "draft-set", "draft", None).await?;
        insert_pattern_set(
            &pool,
            older_published_set_id,
            "older-set",
            "published",
            Some(dt(2026, 7, 9, 9)),
        )
        .await?;
        insert_pattern_set(
            &pool,
            newer_published_set_id,
            "newer-set",
            "published",
            Some(dt(2026, 7, 10, 9)),
        )
        .await?;

        let repo = PatternRepository::new(pool);
        let row = repo.latest_published_set().await.unwrap().unwrap();

        assert_eq!(row.pattern_set_id, newer_published_set_id);
        assert_eq!(row.name, "newer-set");
        assert_eq!(row.status, "published");
        assert_eq!(row.published_at, Some(dt(2026, 7, 10, 9)));
        Ok(())
    }
}
