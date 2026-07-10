use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::{AppError, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct EventEvidenceRow {
    pub evidence_id: Uuid,
    pub source_id: String,
    pub source_item_id: String,
    pub source_url: Option<String>,
    pub source_tier: String,
    pub source_terms_version: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub published_at: Option<DateTime<Utc>>,
    pub first_seen_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub effective_trade_date: NaiveDate,
    pub title: String,
    pub content: Option<String>,
    pub language: String,
    pub content_hash: String,
    pub raw_payload: Value,
    pub version: i32,
    pub supersedes_evidence_id: Option<Uuid>,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DuplicateGroupMemberRow {
    pub evidence_id: Uuid,
    pub is_representative: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DuplicateGroupRow {
    pub duplicate_group_id: Uuid,
    pub relation_type: String,
    pub confidence: f64,
    pub locked_by_user: bool,
    pub members: Vec<DuplicateGroupMemberRow>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimEvidenceRow {
    pub evidence_id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimRow {
    pub claim_id: Uuid,
    pub claim_type: String,
    pub claim_text: String,
    pub confidence: f64,
    pub review_status: String,
    pub evidence: Vec<ClaimEvidenceRow>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExtractionRow {
    pub extraction_id: Uuid,
    pub evidence_id: Uuid,
    pub schema_version: String,
    pub prompt_version: Option<String>,
    pub model_name: Option<String>,
    pub model_parameters: Value,
    pub extracted_payload: Value,
    pub validation_status: String,
    pub validation_errors: Value,
    pub input_fingerprint: String,
    pub claims: Vec<ClaimRow>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimGraphRow {
    pub claim_graph_id: Uuid,
    pub evidence_id: Uuid,
    pub graph_version: i32,
    pub schema_version: String,
    pub graph_payload: Value,
    pub review_status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DailyEventBriefRow {
    pub trade_date: NaiveDate,
    pub brief_version: String,
    pub content: String,
    pub structured_payload: Value,
    pub input_fingerprint: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct EventRepository {
    pool: PgPool,
}

impl EventRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert_evidence(&self, row: &EventEvidenceRow) -> Result<Uuid> {
        sqlx::query(
            r#"INSERT INTO market_event_evidence
               (evidence_id, source_id, source_item_id, source_url, source_tier,
                source_terms_version, occurred_at, published_at, first_seen_at,
                available_at, effective_trade_date, title, content, language,
                content_hash, raw_payload, version, supersedes_evidence_id, status, created_at)
               VALUES ($1, $2, $3, $4, $5,
                       $6, $7, $8, $9,
                       $10, $11, $12, $13, $14,
                       $15, $16, $17, $18, $19, $20)"#,
        )
        .bind(row.evidence_id)
        .bind(&row.source_id)
        .bind(&row.source_item_id)
        .bind(&row.source_url)
        .bind(&row.source_tier)
        .bind(&row.source_terms_version)
        .bind(row.occurred_at)
        .bind(row.published_at)
        .bind(row.first_seen_at)
        .bind(row.available_at)
        .bind(row.effective_trade_date)
        .bind(&row.title)
        .bind(&row.content)
        .bind(&row.language)
        .bind(&row.content_hash)
        .bind(&row.raw_payload)
        .bind(row.version)
        .bind(row.supersedes_evidence_id)
        .bind(&row.status)
        .bind(row.created_at)
        .execute(&self.pool)
        .await?;

        Ok(row.evidence_id)
    }

    pub async fn find_existing_source_item(
        &self,
        source_id: &str,
        source_item_id: &str,
    ) -> Result<Vec<EventEvidenceRow>> {
        let sql = evidence_select_sql(
            r#"WHERE source_id = $1
                 AND source_item_id = $2
               ORDER BY version ASC, created_at ASC, evidence_id ASC"#,
        );
        let rows = sqlx::query(&sql)
            .bind(source_id)
            .bind(source_item_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(event_evidence_from_row).collect())
    }

    pub async fn find_by_content_hash(&self, hash: &str) -> Result<Vec<EventEvidenceRow>> {
        let sql = evidence_select_sql(
            r#"WHERE content_hash = $1
               ORDER BY effective_trade_date ASC, available_at ASC, source_id ASC,
                        source_item_id ASC, version ASC, evidence_id ASC"#,
        );
        let rows = sqlx::query(&sql).bind(hash).fetch_all(&self.pool).await?;

        Ok(rows.into_iter().map(event_evidence_from_row).collect())
    }

    pub async fn save_duplicate_group(&self, group: &DuplicateGroupRow) -> Result<Uuid> {
        let mut tx = self.pool.begin().await?;

        let existing_locked = sqlx::query_scalar::<_, bool>(
            r#"SELECT locked_by_user
               FROM market_event_duplicate_groups
               WHERE duplicate_group_id = $1
               FOR UPDATE"#,
        )
        .bind(group.duplicate_group_id)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(false);

        if existing_locked {
            tx.commit().await?;
            return Ok(group.duplicate_group_id);
        }

        sqlx::query(
            r#"INSERT INTO market_event_duplicate_groups
               (duplicate_group_id, relation_type, confidence, locked_by_user, created_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (duplicate_group_id) DO UPDATE SET
                   relation_type = EXCLUDED.relation_type,
                   confidence = EXCLUDED.confidence,
                   locked_by_user = market_event_duplicate_groups.locked_by_user
                                    OR EXCLUDED.locked_by_user"#,
        )
        .bind(group.duplicate_group_id)
        .bind(&group.relation_type)
        .bind(group.confidence)
        .bind(group.locked_by_user)
        .bind(group.created_at)
        .execute(&mut *tx)
        .await?;

        for member in &group.members {
            sqlx::query(
                r#"INSERT INTO market_event_duplicate_members
                   (duplicate_group_id, evidence_id, is_representative)
                   VALUES ($1, $2, $3)
                   ON CONFLICT (duplicate_group_id, evidence_id) DO UPDATE SET
                       is_representative = EXCLUDED.is_representative"#,
            )
            .bind(group.duplicate_group_id)
            .bind(member.evidence_id)
            .bind(member.is_representative)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(group.duplicate_group_id)
    }

    pub async fn save_extraction(&self, extraction: &ExtractionRow) -> Result<Uuid> {
        for claim in &extraction.claims {
            if claim.review_status == "published" && claim.evidence.is_empty() {
                return Err(AppError::Internal(format!(
                    "published market event claim {} must reference evidence",
                    claim.claim_id
                )));
            }
        }

        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"INSERT INTO market_event_extractions
               (extraction_id, evidence_id, schema_version, prompt_version, model_name,
                model_parameters, extracted_payload, validation_status, validation_errors,
                input_fingerprint, created_at)
               VALUES ($1, $2, $3, $4, $5,
                       $6, $7, $8, $9,
                       $10, $11)"#,
        )
        .bind(extraction.extraction_id)
        .bind(extraction.evidence_id)
        .bind(&extraction.schema_version)
        .bind(&extraction.prompt_version)
        .bind(&extraction.model_name)
        .bind(&extraction.model_parameters)
        .bind(&extraction.extracted_payload)
        .bind(&extraction.validation_status)
        .bind(&extraction.validation_errors)
        .bind(&extraction.input_fingerprint)
        .bind(extraction.created_at)
        .execute(&mut *tx)
        .await?;

        for claim in &extraction.claims {
            sqlx::query(
                r#"INSERT INTO market_event_claims
                   (claim_id, extraction_id, claim_type, claim_text, confidence,
                    review_status, created_at)
                   VALUES ($1, $2, $3, $4, $5,
                           $6, $7)"#,
            )
            .bind(claim.claim_id)
            .bind(extraction.extraction_id)
            .bind(&claim.claim_type)
            .bind(&claim.claim_text)
            .bind(claim.confidence)
            .bind(&claim.review_status)
            .bind(claim.created_at)
            .execute(&mut *tx)
            .await?;

            for evidence in &claim.evidence {
                sqlx::query(
                    r#"INSERT INTO market_event_claim_evidence (claim_id, evidence_id)
                       VALUES ($1, $2)"#,
                )
                .bind(claim.claim_id)
                .bind(evidence.evidence_id)
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;
        Ok(extraction.extraction_id)
    }

    pub async fn save_claim_graph(&self, graph: &ClaimGraphRow) -> Result<Uuid> {
        sqlx::query(
            r#"INSERT INTO market_event_claim_graphs
               (claim_graph_id, evidence_id, graph_version, schema_version, graph_payload,
                review_status, created_at)
               VALUES ($1, $2, $3, $4, $5,
                       $6, $7)"#,
        )
        .bind(graph.claim_graph_id)
        .bind(graph.evidence_id)
        .bind(graph.graph_version)
        .bind(&graph.schema_version)
        .bind(&graph.graph_payload)
        .bind(&graph.review_status)
        .bind(graph.created_at)
        .execute(&self.pool)
        .await?;

        Ok(graph.claim_graph_id)
    }

    pub async fn save_daily_brief(&self, brief: &DailyEventBriefRow) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO market_event_daily_briefs
               (trade_date, brief_version, content, structured_payload,
                input_fingerprint, generated_at)
               VALUES ($1, $2, $3, $4,
                       $5, $6)
               ON CONFLICT (trade_date) DO UPDATE SET
                   brief_version = EXCLUDED.brief_version,
                   content = EXCLUDED.content,
                   structured_payload = EXCLUDED.structured_payload,
                   input_fingerprint = EXCLUDED.input_fingerprint,
                   generated_at = EXCLUDED.generated_at"#,
        )
        .bind(brief.trade_date)
        .bind(&brief.brief_version)
        .bind(&brief.content)
        .bind(&brief.structured_payload)
        .bind(&brief.input_fingerprint)
        .bind(brief.generated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn list_publishable_evidence(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<EventEvidenceRow>> {
        let sql = evidence_select_sql(
            r#"WHERE effective_trade_date = $1
                 AND status = 'publishable'
               ORDER BY available_at ASC, first_seen_at ASC, source_id ASC,
                        source_item_id ASC, version ASC, evidence_id ASC"#,
        );
        let rows = sqlx::query(&sql)
            .bind(trade_date)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(event_evidence_from_row).collect())
    }
}

fn evidence_select_sql(where_and_order: &str) -> String {
    format!(
        r#"SELECT evidence_id,
                  source_id,
                  source_item_id,
                  source_url,
                  source_tier,
                  source_terms_version,
                  occurred_at,
                  published_at,
                  first_seen_at,
                  available_at,
                  effective_trade_date,
                  title,
                  content,
                  language,
                  content_hash,
                  raw_payload,
                  version,
                  supersedes_evidence_id,
                  status,
                  created_at
           FROM market_event_evidence
           {where_and_order}"#
    )
}

fn event_evidence_from_row(row: sqlx::postgres::PgRow) -> EventEvidenceRow {
    EventEvidenceRow {
        evidence_id: row.get("evidence_id"),
        source_id: row.get("source_id"),
        source_item_id: row.get("source_item_id"),
        source_url: row.get("source_url"),
        source_tier: row.get("source_tier"),
        source_terms_version: row.get("source_terms_version"),
        occurred_at: row.get("occurred_at"),
        published_at: row.get("published_at"),
        first_seen_at: row.get("first_seen_at"),
        available_at: row.get("available_at"),
        effective_trade_date: row.get("effective_trade_date"),
        title: row.get("title"),
        content: row.get("content"),
        language: row.get("language"),
        content_hash: row.get("content_hash"),
        raw_payload: row.get("raw_payload"),
        version: row.get("version"),
        supersedes_evidence_id: row.get("supersedes_evidence_id"),
        status: row.get("status"),
        created_at: row.get("created_at"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClaimEvidenceRow, ClaimGraphRow, ClaimRow, DailyEventBriefRow, DuplicateGroupMemberRow,
        DuplicateGroupRow, EventEvidenceRow, EventRepository, ExtractionRow,
    };
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use serde_json::{json, Value};
    use sqlx::PgPool;
    use uuid::Uuid;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    fn evidence(source_item_id: &str, version: i32, status: &str) -> EventEvidenceRow {
        EventEvidenceRow {
            evidence_id: Uuid::new_v4(),
            source_id: "manual".to_string(),
            source_item_id: source_item_id.to_string(),
            source_url: Some(format!("https://example.test/{source_item_id}/{version}")),
            source_tier: "manual".to_string(),
            source_terms_version: "terms-v1".to_string(),
            occurred_at: Some(dt(2026, 7, 10, 8)),
            published_at: Some(dt(2026, 7, 10, 9)),
            first_seen_at: dt(2026, 7, 10, 10),
            available_at: dt(2026, 7, 10, 10),
            effective_trade_date: date(2026, 7, 10),
            title: format!("Event {source_item_id} v{version}"),
            content: Some(format!("payload {source_item_id} v{version}")),
            language: "en".to_string(),
            content_hash: format!("{source_item_id}-hash-{version}"),
            raw_payload: json!({"source_item_id": source_item_id, "version": version}),
            version,
            supersedes_evidence_id: None,
            status: status.to_string(),
            created_at: dt(2026, 7, 10, 11),
        }
    }

    fn extraction(evidence_id: Uuid, claims: Vec<ClaimRow>) -> ExtractionRow {
        ExtractionRow {
            extraction_id: Uuid::new_v4(),
            evidence_id,
            schema_version: "event-schema-v1".to_string(),
            prompt_version: Some("prompt-v1".to_string()),
            model_name: Some("test-model".to_string()),
            model_parameters: json!({"temperature": 0}),
            extracted_payload: json!({"claims": claims.len()}),
            validation_status: "valid".to_string(),
            validation_errors: json!([]),
            input_fingerprint: "fingerprint-v1".to_string(),
            claims,
            created_at: dt(2026, 7, 10, 12),
        }
    }

    fn published_claim(evidence_id: Uuid) -> ClaimRow {
        ClaimRow {
            claim_id: Uuid::new_v4(),
            claim_type: "fact".to_string(),
            claim_text: "Issuer announced an official update".to_string(),
            confidence: 0.95,
            review_status: "published".to_string(),
            evidence: vec![ClaimEvidenceRow { evidence_id }],
            created_at: dt(2026, 7, 10, 12),
        }
    }

    async fn save_evidence(pool: &PgPool, row: &EventEvidenceRow) {
        EventRepository::new(pool.clone())
            .insert_evidence(row)
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn evidence_versions_are_append_only(pool: PgPool) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool);
        let v1 = evidence("source-a", 1, "publishable");
        let mut v2 = evidence("source-a", 2, "rejected");
        v2.content = Some("changed payload".to_string());
        v2.content_hash = "source-a-hash-2".to_string();
        v2.supersedes_evidence_id = Some(v1.evidence_id);

        assert_eq!(repo.insert_evidence(&v1).await.unwrap(), v1.evidence_id);
        assert_eq!(repo.insert_evidence(&v2).await.unwrap(), v2.evidence_id);

        let rows = repo
            .find_existing_source_item("manual", "source-a")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].version, 1);
        assert_eq!(rows[0].content, Some("payload source-a v1".to_string()));
        assert_eq!(rows[0].status, "publishable");
        assert_eq!(rows[1].version, 2);
        assert_eq!(rows[1].content, Some("changed payload".to_string()));
        assert_eq!(rows[1].status, "rejected");

        let duplicate = repo.insert_evidence(&v1).await;
        assert!(duplicate.is_err());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn evidence_rows_reject_direct_update_and_delete(pool: PgPool) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let row = evidence("source-a-immutable", 1, "publishable");
        repo.insert_evidence(&row).await.unwrap();

        let update_error = sqlx::query(
            r#"UPDATE market_event_evidence
               SET title = 'mutated title'
               WHERE evidence_id = $1"#,
        )
        .bind(row.evidence_id)
        .execute(&pool)
        .await
        .unwrap_err();
        let update_message = update_error.to_string();
        assert!(update_message.contains("append-only"));

        let delete_error = sqlx::query(
            r#"DELETE FROM market_event_evidence
               WHERE evidence_id = $1"#,
        )
        .bind(row.evidence_id)
        .execute(&pool)
        .await
        .unwrap_err();
        let delete_message = delete_error.to_string();
        assert!(delete_message.contains("append-only"));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn published_claims_require_joinable_evidence(pool: PgPool) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let row = evidence("source-published-claim", 1, "publishable");
        save_evidence(&pool, &row).await;

        let claim = published_claim(row.evidence_id);
        let valid_extraction = extraction(row.evidence_id, vec![claim.clone()]);
        repo.save_extraction(&valid_extraction).await.unwrap();

        let linked: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*)
               FROM market_event_claims c
               INNER JOIN market_event_claim_evidence ce ON ce.claim_id = c.claim_id
               INNER JOIN market_event_evidence e ON e.evidence_id = ce.evidence_id
               WHERE c.claim_id = $1
                 AND c.review_status = 'published'"#,
        )
        .bind(claim.claim_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(linked.0, 1);

        let unlinked_claim = ClaimRow {
            evidence: Vec::new(),
            ..published_claim(row.evidence_id)
        };
        let invalid_extraction = extraction(row.evidence_id, vec![unlinked_claim]);
        let result = repo.save_extraction(&invalid_extraction).await;
        assert!(result.is_err());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn user_locked_duplicate_groups_are_not_overwritten_by_reprocessing(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let original_member = evidence("source-duplicate-original", 1, "publishable");
        let reprocessed_member = evidence("source-duplicate-reprocessed", 1, "publishable");
        save_evidence(&pool, &original_member).await;
        save_evidence(&pool, &reprocessed_member).await;

        let group_id = Uuid::new_v4();
        let locked = DuplicateGroupRow {
            duplicate_group_id: group_id,
            relation_type: "exact".to_string(),
            confidence: 1.0,
            locked_by_user: true,
            members: vec![DuplicateGroupMemberRow {
                evidence_id: original_member.evidence_id,
                is_representative: true,
            }],
            created_at: dt(2026, 7, 10, 12),
        };
        repo.save_duplicate_group(&locked).await.unwrap();

        let unlocked_update = DuplicateGroupRow {
            locked_by_user: false,
            confidence: 0.8,
            relation_type: "near".to_string(),
            members: vec![
                DuplicateGroupMemberRow {
                    evidence_id: original_member.evidence_id,
                    is_representative: false,
                },
                DuplicateGroupMemberRow {
                    evidence_id: reprocessed_member.evidence_id,
                    is_representative: true,
                },
            ],
            ..locked
        };
        repo.save_duplicate_group(&unlocked_update).await.unwrap();

        let stored: (bool, String, f64) = sqlx::query_as(
            r#"SELECT locked_by_user, relation_type, confidence::float8
               FROM market_event_duplicate_groups
               WHERE duplicate_group_id = $1"#,
        )
        .bind(group_id)
        .fetch_one(&pool)
        .await?;
        assert!(stored.0);
        assert_eq!(stored.1, "exact");
        assert_eq!(stored.2, 1.0);

        let members: Vec<(Uuid, bool)> = sqlx::query_as(
            r#"SELECT evidence_id, is_representative
               FROM market_event_duplicate_members
               WHERE duplicate_group_id = $1
               ORDER BY evidence_id ASC"#,
        )
        .bind(group_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(members, vec![(original_member.evidence_id, true)]);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn unlocked_duplicate_groups_can_update_relation_metadata_and_members(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let first_member = evidence("source-duplicate-unlocked-1", 1, "publishable");
        let second_member = evidence("source-duplicate-unlocked-2", 1, "publishable");
        save_evidence(&pool, &first_member).await;
        save_evidence(&pool, &second_member).await;

        let group_id = Uuid::new_v4();
        let original = DuplicateGroupRow {
            duplicate_group_id: group_id,
            relation_type: "exact".to_string(),
            confidence: 0.55,
            locked_by_user: false,
            members: vec![
                DuplicateGroupMemberRow {
                    evidence_id: first_member.evidence_id,
                    is_representative: true,
                },
                DuplicateGroupMemberRow {
                    evidence_id: second_member.evidence_id,
                    is_representative: false,
                },
            ],
            created_at: dt(2026, 7, 10, 12),
        };
        repo.save_duplicate_group(&original).await.unwrap();

        let updated = DuplicateGroupRow {
            relation_type: "near".to_string(),
            confidence: 0.85,
            members: vec![
                DuplicateGroupMemberRow {
                    evidence_id: first_member.evidence_id,
                    is_representative: false,
                },
                DuplicateGroupMemberRow {
                    evidence_id: second_member.evidence_id,
                    is_representative: true,
                },
            ],
            ..original
        };
        repo.save_duplicate_group(&updated).await.unwrap();

        let stored: (bool, String, f64) = sqlx::query_as(
            r#"SELECT locked_by_user, relation_type, confidence::float8
               FROM market_event_duplicate_groups
               WHERE duplicate_group_id = $1"#,
        )
        .bind(group_id)
        .fetch_one(&pool)
        .await?;
        assert!(!stored.0);
        assert_eq!(stored.1, "near");
        assert_eq!(stored.2, 0.85);

        let members: Vec<(Uuid, bool)> = sqlx::query_as(
            r#"SELECT evidence_id, is_representative
               FROM market_event_duplicate_members
               WHERE duplicate_group_id = $1
               ORDER BY evidence_id ASC"#,
        )
        .bind(group_id)
        .fetch_all(&pool)
        .await?;
        let mut expected_members = vec![
            (first_member.evidence_id, false),
            (second_member.evidence_id, true),
        ];
        expected_members.sort_by_key(|member| member.0);
        assert_eq!(members, expected_members);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn daily_briefs_upsert_by_trade_date(pool: PgPool) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let trade_date = date(2026, 7, 10);

        let first = DailyEventBriefRow {
            trade_date,
            brief_version: "v1".to_string(),
            content: "first brief".to_string(),
            structured_payload: json!({"facts": ["first"]}),
            input_fingerprint: "fp-1".to_string(),
            generated_at: dt(2026, 7, 10, 13),
        };
        let second = DailyEventBriefRow {
            brief_version: "v2".to_string(),
            content: "second brief".to_string(),
            structured_payload: json!({"facts": ["second"]}),
            input_fingerprint: "fp-2".to_string(),
            generated_at: dt(2026, 7, 10, 14),
            ..first.clone()
        };

        repo.save_daily_brief(&first).await.unwrap();
        repo.save_daily_brief(&second).await.unwrap();

        let stored: (String, String, Value, String, DateTime<Utc>) = sqlx::query_as(
            r#"SELECT brief_version, content, structured_payload, input_fingerprint, generated_at
               FROM market_event_daily_briefs
               WHERE trade_date = $1"#,
        )
        .bind(trade_date)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored.0, "v2");
        assert_eq!(stored.1, "second brief");
        assert_eq!(stored.2, json!({"facts": ["second"]}));
        assert_eq!(stored.3, "fp-2");
        assert_eq!(stored.4, dt(2026, 7, 10, 14));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn publishable_evidence_is_trade_date_and_status_scoped(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool);
        let publishable = evidence("publishable", 1, "publishable");
        let draft = evidence("draft", 1, "draft");
        let other_date = EventEvidenceRow {
            effective_trade_date: date(2026, 7, 13),
            ..evidence("other-date", 1, "publishable")
        };

        repo.insert_evidence(&publishable).await.unwrap();
        repo.insert_evidence(&draft).await.unwrap();
        repo.insert_evidence(&other_date).await.unwrap();

        let rows = repo
            .list_publishable_evidence(date(2026, 7, 10))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].evidence_id, publishable.evidence_id);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn claim_graph_versions_are_unique_per_evidence(pool: PgPool) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool);
        let row = evidence("claim-graph", 1, "publishable");
        repo.insert_evidence(&row).await.unwrap();

        let graph = ClaimGraphRow {
            claim_graph_id: Uuid::new_v4(),
            evidence_id: row.evidence_id,
            graph_version: 1,
            schema_version: "claim-graph-v1".to_string(),
            graph_payload: json!({"nodes": []}),
            review_status: "draft".to_string(),
            created_at: dt(2026, 7, 10, 15),
        };
        repo.save_claim_graph(&graph).await.unwrap();

        let duplicate = ClaimGraphRow {
            claim_graph_id: Uuid::new_v4(),
            graph_payload: json!({"nodes": ["changed"]}),
            ..graph
        };
        let result = repo.save_claim_graph(&duplicate).await;
        assert!(result.is_err());
        Ok(())
    }
}
