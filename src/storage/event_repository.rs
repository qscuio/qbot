use chrono::{DateTime, NaiveDate, Utc};
use reqwest::Url;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
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

#[derive(Debug, Clone, PartialEq)]
pub struct ManualEvidenceInsertResult {
    pub existing_rows: Vec<EventEvidenceRow>,
    pub existing_candidates: Vec<ManualDuplicateCandidateRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManualDuplicateCandidateRow {
    pub row: EventEvidenceRow,
    pub representative_evidence_id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ManualEvidenceInsertContext {
    pub submitted_row: EventEvidenceRow,
    pub existing_rows: Vec<EventEvidenceRow>,
    pub existing_candidates: Vec<ManualDuplicateCandidateRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ManualEvidenceInsertEffect<T> {
    pub result: T,
    pub duplicate_group: Option<DuplicateGroupRow>,
}

#[derive(Clone)]
pub struct EventRepository {
    pool: PgPool,
    #[cfg(test)]
    manual_insert_test_hook: Option<ManualInsertTestHook>,
}

impl EventRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            #[cfg(test)]
            manual_insert_test_hook: None,
        }
    }

    pub async fn insert_evidence(&self, row: &EventEvidenceRow) -> Result<Uuid> {
        let row = canonicalized_evidence_row(row)?;
        insert_evidence_in_txless(&self.pool, &row).await?;
        Ok(row.evidence_id)
    }

    pub async fn insert_manual_evidence(
        &self,
        row: &EventEvidenceRow,
    ) -> Result<ManualEvidenceInsertResult> {
        self.insert_manual_evidence_with_effect(row, |context| async move {
            Ok(ManualEvidenceInsertEffect {
                result: ManualEvidenceInsertResult {
                    existing_rows: context.existing_rows,
                    existing_candidates: context.existing_candidates,
                },
                duplicate_group: None,
            })
        })
        .await
    }

    pub(crate) async fn insert_manual_evidence_with_effect<T, F, Fut>(
        &self,
        row: &EventEvidenceRow,
        build_effect: F,
    ) -> Result<T>
    where
        F: FnOnce(ManualEvidenceInsertContext) -> Fut,
        Fut: std::future::Future<Output = Result<ManualEvidenceInsertEffect<T>>>,
    {
        let row = canonicalized_evidence_row(row)?;
        let mut tx = self.pool.begin().await?;
        lock_manual_duplicate_discovery_scope(&mut tx, &row).await?;

        let existing_candidates = find_manual_duplicate_candidates_in_tx(&mut tx, &row).await?;
        #[cfg(test)]
        self.run_manual_insert_test_hook(&row, &existing_candidates)
            .await;
        let existing_rows = existing_candidates
            .iter()
            .map(|candidate| candidate.row.clone())
            .collect();
        let effect = build_effect(ManualEvidenceInsertContext {
            submitted_row: row.clone(),
            existing_rows,
            existing_candidates,
        })
        .await?;

        insert_evidence_in_tx(&mut tx, &row).await?;
        #[cfg(test)]
        self.run_manual_insert_test_hook_before_duplicate_group_append(
            &row,
            effect.duplicate_group.as_ref(),
        )
        .await;
        if let Some(duplicate_group) = effect.duplicate_group.as_ref() {
            append_duplicate_group_in_tx(&mut tx, duplicate_group).await?;
        }

        tx.commit().await?;

        Ok(effect.result)
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
        save_duplicate_group_in_tx(&mut tx, group).await?;
        tx.commit().await?;
        Ok(group.duplicate_group_id)
    }

    #[cfg(test)]
    pub(crate) fn clone_with_manual_insert_sleep_after_candidate_discovery_for_test(
        &self,
        duration: std::time::Duration,
    ) -> Self {
        let mut clone = self.clone();
        clone.manual_insert_test_hook = Some(
            ManualInsertTestHook::with_sleep_after_candidate_discovery(duration),
        );
        clone
    }

    #[cfg(test)]
    pub(crate) fn clone_with_manual_insert_duplicate_group_persistence_gate_for_test(
        &self,
        content_hash: impl Into<String>,
    ) -> (Self, DuplicateGroupPersistenceGateHandle) {
        let mut clone = self.clone();
        let (hook, handle) =
            ManualInsertTestHook::with_duplicate_group_persistence_gate(content_hash);
        clone.manual_insert_test_hook = Some(hook);
        (clone, handle)
    }

    #[cfg(test)]
    pub(crate) fn clone_with_manual_insert_candidate_discovery_gate_for_test(
        &self,
        content_hash: impl Into<String>,
    ) -> (Self, CandidateDiscoveryGateHandle) {
        let mut clone = self.clone();
        let (hook, handle) = ManualInsertTestHook::with_candidate_discovery_gate(content_hash);
        clone.manual_insert_test_hook = Some(hook);
        (clone, handle)
    }

    #[cfg(test)]
    async fn run_manual_insert_test_hook(
        &self,
        row: &EventEvidenceRow,
        candidates: &[ManualDuplicateCandidateRow],
    ) {
        if let Some(hook) = &self.manual_insert_test_hook {
            hook.wait_after_candidate_discovery(row, candidates).await;
        }
    }

    #[cfg(test)]
    async fn run_manual_insert_test_hook_before_duplicate_group_append(
        &self,
        row: &EventEvidenceRow,
        duplicate_group: Option<&DuplicateGroupRow>,
    ) {
        if let Some(hook) = &self.manual_insert_test_hook {
            hook.wait_before_duplicate_group_append(row, duplicate_group)
                .await;
        }
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

pub(crate) fn canonicalize_source_url(value: &str) -> Result<String> {
    let trimmed = value.trim();
    let mut url = Url::parse(trimmed).map_err(|error| {
        AppError::Internal(format!("manual event source URL is invalid: {error}"))
    })?;
    url.set_fragment(None);
    if matches!(
        (url.scheme(), url.port()),
        ("https", Some(443)) | ("http", Some(80))
    ) {
        let _ = url.set_port(None);
    }
    if url.path().is_empty() {
        url.set_path("/");
    }

    Ok(url.to_string())
}

fn canonicalized_evidence_row(row: &EventEvidenceRow) -> Result<EventEvidenceRow> {
    let source_url = row
        .source_url
        .as_deref()
        .map(canonicalize_source_url)
        .transpose()?;

    Ok(EventEvidenceRow {
        source_url,
        ..row.clone()
    })
}

async fn insert_evidence_in_txless(pool: &PgPool, row: &EventEvidenceRow) -> Result<()> {
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
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_evidence_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    row: &EventEvidenceRow,
) -> Result<()> {
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
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn find_manual_duplicate_candidates_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    row: &EventEvidenceRow,
) -> Result<Vec<ManualDuplicateCandidateRow>> {
    let sql = format!(
        r#"WITH matched AS (
               SELECT evidence_id
               FROM market_event_evidence
               WHERE effective_trade_date = $1
                  OR content_hash = $2
                  OR ($3::text IS NOT NULL
                      AND market_event_canonical_source_url(source_url) = $3)
                  OR (source_id = $4 AND source_item_id = $5 AND version = $6)
           ),
           expanded AS (
               SELECT evidence_id
               FROM matched
               UNION
               SELECT representative.representative_evidence_id
               FROM matched
               JOIN LATERAL (
                   SELECT representative_member.evidence_id AS representative_evidence_id
                   FROM market_event_duplicate_members matched_member
                   JOIN market_event_duplicate_members representative_member
                     ON representative_member.duplicate_group_id = matched_member.duplicate_group_id
                    AND representative_member.is_representative = TRUE
                   WHERE matched_member.evidence_id = matched.evidence_id
                   ORDER BY representative_member.evidence_id ASC
                   LIMIT 1
               ) AS representative ON TRUE
           )
           SELECT evidence_id,
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
                  created_at,
                  COALESCE(representative.representative_evidence_id, evidence_id)
                      AS representative_evidence_id
           FROM market_event_evidence
           LEFT JOIN LATERAL (
               SELECT representative_member.evidence_id AS representative_evidence_id
               FROM market_event_duplicate_members matched_member
               JOIN market_event_duplicate_members representative_member
                 ON representative_member.duplicate_group_id = matched_member.duplicate_group_id
                AND representative_member.is_representative = TRUE
               WHERE matched_member.evidence_id = market_event_evidence.evidence_id
               ORDER BY representative_member.evidence_id ASC
               LIMIT 1
           ) AS representative ON TRUE
           WHERE evidence_id IN (SELECT evidence_id FROM expanded)
           ORDER BY effective_trade_date ASC, available_at ASC, source_id ASC,
                    source_item_id ASC, version ASC, evidence_id ASC"#
    );
    let rows = sqlx::query(&sql)
        .bind(row.effective_trade_date)
        .bind(&row.content_hash)
        .bind(&row.source_url)
        .bind(&row.source_id)
        .bind(&row.source_item_id)
        .bind(row.version)
        .fetch_all(&mut **tx)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| ManualDuplicateCandidateRow {
            representative_evidence_id: row.get("representative_evidence_id"),
            row: event_evidence_from_row(row),
        })
        .collect())
}

async fn lock_manual_duplicate_discovery_scope(
    tx: &mut Transaction<'_, Postgres>,
    _row: &EventEvidenceRow,
) -> Result<()> {
    sqlx::query(r#"SELECT pg_advisory_xact_lock($1::bigint)"#)
        .bind(manual_duplicate_scope_lock_key())
        .fetch_one(&mut **tx)
        .await?;

    Ok(())
}

async fn save_duplicate_group_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    group: &DuplicateGroupRow,
) -> Result<()> {
    let existing_locked = sqlx::query_scalar::<_, bool>(
        r#"SELECT locked_by_user
           FROM market_event_duplicate_groups
           WHERE duplicate_group_id = $1
           FOR UPDATE"#,
    )
    .bind(group.duplicate_group_id)
    .fetch_optional(&mut **tx)
    .await?
    .unwrap_or(false);

    if existing_locked {
        return Ok(());
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
    .execute(&mut **tx)
    .await?;

    let member_ids: Vec<Uuid> = group
        .members
        .iter()
        .map(|member| member.evidence_id)
        .collect();
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
        .execute(&mut **tx)
        .await?;
    }

    sqlx::query(
        r#"DELETE FROM market_event_duplicate_members
           WHERE duplicate_group_id = $1
             AND NOT (evidence_id = ANY($2::uuid[]))"#,
    )
    .bind(group.duplicate_group_id)
    .bind(&member_ids)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn append_duplicate_group_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    group: &DuplicateGroupRow,
) -> Result<()> {
    let existing_locked = sqlx::query_scalar::<_, bool>(
        r#"SELECT locked_by_user
           FROM market_event_duplicate_groups
           WHERE duplicate_group_id = $1
           FOR UPDATE"#,
    )
    .bind(group.duplicate_group_id)
    .fetch_optional(&mut **tx)
    .await?
    .unwrap_or(false);

    if existing_locked {
        return Ok(());
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
    .execute(&mut **tx)
    .await?;

    let existing_members: Vec<(Uuid, bool)> = sqlx::query_as(
        r#"SELECT evidence_id, is_representative
           FROM market_event_duplicate_members
           WHERE duplicate_group_id = $1"#,
    )
    .bind(group.duplicate_group_id)
    .fetch_all(&mut **tx)
    .await?;

    let mut merged_members = std::collections::BTreeMap::new();
    for (evidence_id, is_representative) in existing_members {
        merged_members.insert(evidence_id, is_representative);
    }
    for member in &group.members {
        merged_members.insert(member.evidence_id, member.is_representative);
    }

    let representative_id = group
        .members
        .iter()
        .find(|member| member.is_representative)
        .map(|member| member.evidence_id)
        .or_else(|| {
            merged_members
                .iter()
                .find_map(|(evidence_id, is_representative)| {
                    (*is_representative).then_some(*evidence_id)
                })
        });
    if let Some(representative_id) = representative_id {
        for is_representative in merged_members.values_mut() {
            *is_representative = false;
        }
        if let Some(is_representative) = merged_members.get_mut(&representative_id) {
            *is_representative = true;
        }
    }

    for (evidence_id, is_representative) in merged_members {
        sqlx::query(
            r#"INSERT INTO market_event_duplicate_members
               (duplicate_group_id, evidence_id, is_representative)
               VALUES ($1, $2, $3)
               ON CONFLICT (duplicate_group_id, evidence_id) DO UPDATE SET
                   is_representative = EXCLUDED.is_representative"#,
        )
        .bind(group.duplicate_group_id)
        .bind(evidence_id)
        .bind(is_representative)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

fn manual_duplicate_scope_lock_key() -> i64 {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest("market-event-manual-duplicate-discovery".as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
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
#[derive(Clone)]
struct ManualInsertTestHook {
    sleep_after_candidate_discovery: Option<std::time::Duration>,
    candidate_discovery_gate: Option<CandidateDiscoveryGate>,
    duplicate_group_persistence_gate: Option<DuplicateGroupPersistenceGate>,
}

#[cfg(test)]
impl ManualInsertTestHook {
    fn with_sleep_after_candidate_discovery(duration: std::time::Duration) -> Self {
        Self {
            sleep_after_candidate_discovery: Some(duration),
            candidate_discovery_gate: None,
            duplicate_group_persistence_gate: None,
        }
    }

    fn with_candidate_discovery_gate(
        content_hash: impl Into<String>,
    ) -> (Self, CandidateDiscoveryGateHandle) {
        let (gate, handle) = CandidateDiscoveryGate::for_content_hash(content_hash);
        (
            Self {
                sleep_after_candidate_discovery: None,
                candidate_discovery_gate: Some(gate),
                duplicate_group_persistence_gate: None,
            },
            handle,
        )
    }

    fn with_duplicate_group_persistence_gate(
        content_hash: impl Into<String>,
    ) -> (Self, DuplicateGroupPersistenceGateHandle) {
        let (gate, handle) = DuplicateGroupPersistenceGate::for_content_hash(content_hash);
        (
            Self {
                sleep_after_candidate_discovery: None,
                candidate_discovery_gate: None,
                duplicate_group_persistence_gate: Some(gate),
            },
            handle,
        )
    }

    async fn wait_after_candidate_discovery(
        &self,
        row: &EventEvidenceRow,
        _candidates: &[ManualDuplicateCandidateRow],
    ) {
        if let Some(duration) = self.sleep_after_candidate_discovery {
            tokio::time::sleep(duration).await;
        }
        if let Some(gate) = &self.candidate_discovery_gate {
            gate.wait(&row.content_hash).await;
        }
    }

    async fn wait_before_duplicate_group_append(
        &self,
        row: &EventEvidenceRow,
        duplicate_group: Option<&DuplicateGroupRow>,
    ) {
        if duplicate_group.is_none() {
            return;
        }

        if let Some(gate) = &self.duplicate_group_persistence_gate {
            gate.wait(&row.content_hash).await;
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct CandidateDiscoveryGateHandle {
    blocked: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct CandidateDiscoveryGate {
    content_hash: String,
    blocked: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
    triggered: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl CandidateDiscoveryGate {
    fn for_content_hash(content_hash: impl Into<String>) -> (Self, CandidateDiscoveryGateHandle) {
        let blocked = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());

        (
            Self {
                content_hash: content_hash.into(),
                blocked: blocked.clone(),
                release: release.clone(),
                triggered: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
            CandidateDiscoveryGateHandle { blocked, release },
        )
    }

    async fn wait(&self, content_hash: &str) {
        if self.content_hash == content_hash
            && !self
                .triggered
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.blocked.notify_one();
            self.release.notified().await;
        }
    }
}

#[cfg(test)]
impl CandidateDiscoveryGateHandle {
    pub(crate) async fn wait_until_blocked(&self) {
        self.blocked.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct DuplicateGroupPersistenceGateHandle {
    blocked: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct DuplicateGroupPersistenceGate {
    content_hash: String,
    blocked: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
    triggered: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl DuplicateGroupPersistenceGate {
    fn for_content_hash(
        content_hash: impl Into<String>,
    ) -> (Self, DuplicateGroupPersistenceGateHandle) {
        let blocked = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());

        (
            Self {
                content_hash: content_hash.into(),
                blocked: blocked.clone(),
                release: release.clone(),
                triggered: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
            DuplicateGroupPersistenceGateHandle { blocked, release },
        )
    }

    async fn wait(&self, content_hash: &str) {
        if self.content_hash == content_hash
            && !self
                .triggered
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.blocked.notify_one();
            self.release.notified().await;
        }
    }
}

#[cfg(test)]
impl DuplicateGroupPersistenceGateHandle {
    pub(crate) async fn wait_until_blocked(&self) {
        self.blocked.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClaimEvidenceRow, ClaimGraphRow, ClaimRow, DailyEventBriefRow, DuplicateGroupMemberRow,
        DuplicateGroupRow, EventEvidenceRow, EventRepository, ExtractionRow,
        ManualEvidenceInsertEffect,
    };
    use crate::error::Result;
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use serde_json::{json, Value};
    use sqlx::PgPool;
    use std::time::Duration;
    use tokio::task::yield_now;
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

    async fn insert_raw_evidence_row(pool: &PgPool, row: &EventEvidenceRow) {
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
        .execute(pool)
        .await
        .unwrap();
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ManualDuplicateDiscoveryOutcome {
        InsertedWithoutExisting,
        ExistingFound,
    }

    async fn classify_manual_insert(
        repo: EventRepository,
        row: EventEvidenceRow,
    ) -> Result<ManualDuplicateDiscoveryOutcome> {
        repo.insert_manual_evidence_with_effect(&row, |context| async move {
            let result = if context.existing_rows.is_empty() {
                ManualDuplicateDiscoveryOutcome::InsertedWithoutExisting
            } else {
                ManualDuplicateDiscoveryOutcome::ExistingFound
            };
            Ok(ManualEvidenceInsertEffect {
                result,
                duplicate_group: None,
            })
        })
        .await
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
    async fn evidence_lookup_indexes_match_repository_access_paths(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let indexes: Vec<(String, String)> = sqlx::query_as(
            r#"SELECT indexname, indexdef
               FROM pg_indexes
               WHERE schemaname = 'public'
                 AND tablename = 'market_event_evidence'
               ORDER BY indexname ASC"#,
        )
        .fetch_all(&pool)
        .await?;

        let content_hash_lookup = indexes
            .iter()
            .find(|(name, _)| name == "idx_event_evidence_content_hash_lookup")
            .map(|(_, definition)| definition.as_str())
            .expect("missing idx_event_evidence_content_hash_lookup");
        assert!(content_hash_lookup.contains(
            "(content_hash, effective_trade_date, available_at, source_id, source_item_id, version, evidence_id)"
        ));

        let publishable_lookup = indexes
            .iter()
            .find(|(name, _)| name == "idx_event_evidence_publishable_lookup")
            .map(|(_, definition)| definition.as_str())
            .expect("missing idx_event_evidence_publishable_lookup");
        assert!(publishable_lookup.contains(
            "(effective_trade_date, status, available_at, first_seen_at, source_id, source_item_id, version, evidence_id)"
        ));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn updating_claim_evidence_cannot_orphan_a_published_claim(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let row = evidence("source-published-claim-update", 1, "publishable");
        save_evidence(&pool, &row).await;

        let published = published_claim(row.evidence_id);
        let draft = ClaimRow {
            claim_id: Uuid::new_v4(),
            claim_type: "fact".to_string(),
            claim_text: "Internal draft note".to_string(),
            confidence: 0.25,
            review_status: "draft".to_string(),
            evidence: Vec::new(),
            created_at: dt(2026, 7, 10, 12),
        };
        let seeded = extraction(row.evidence_id, vec![published.clone(), draft.clone()]);
        repo.save_extraction(&seeded).await.unwrap();

        let mut tx = pool.begin().await?;
        sqlx::query(
            r#"UPDATE market_event_claim_evidence
               SET claim_id = $1
               WHERE claim_id = $2
                 AND evidence_id = $3"#,
        )
        .bind(draft.claim_id)
        .bind(published.claim_id)
        .bind(row.evidence_id)
        .execute(&mut *tx)
        .await?;

        let commit_error = tx.commit().await.unwrap_err();
        assert!(commit_error
            .to_string()
            .contains("published market event claim"));

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
    async fn manual_insert_surfaces_same_trade_date_near_duplicate_candidates(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let mut existing = evidence("source-near-existing", 1, "pending");
        existing.title = "Acme wins major supply contract in Shenzhen".to_string();
        existing.content = Some(
            "Acme signed a long-term supply contract with Shenzhen transit authority today."
                .to_string(),
        );
        existing.content_hash =
            "417ab9cf2680f1ff64957b9964bbca6311e035f9d4ea46dbdcb5b1ffd7f86f1b".to_string();
        existing.source_url = Some("https://example.test/existing-near".to_string());
        save_evidence(&pool, &existing).await;

        let mut near_duplicate = evidence("source-near-submitted", 1, "pending");
        near_duplicate.title = "Acme wins major supply contract in Shenzhen market".to_string();
        near_duplicate.content = Some(
            "Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up details differ.".to_string(),
        );
        near_duplicate.content_hash =
            "4efbe4c81ea18ee94cb09ea8c4db3b3b367b2817d1f0218b9793ed0d5e7b06fa".to_string();
        near_duplicate.source_url = Some("https://example.test/submitted-near".to_string());
        near_duplicate.created_at = dt(2026, 7, 10, 13);
        near_duplicate.first_seen_at = dt(2026, 7, 10, 13);
        near_duplicate.available_at = dt(2026, 7, 10, 13);

        let inserted = repo.insert_manual_evidence(&near_duplicate).await.unwrap();

        assert_eq!(
            inserted
                .existing_rows
                .iter()
                .map(|row| row.evidence_id)
                .collect::<Vec<_>>(),
            vec![existing.evidence_id]
        );
        assert_ne!(
            inserted.existing_rows[0].content_hash,
            near_duplicate.content_hash
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_insert_surfaces_cross_trade_date_exact_duplicate_candidates(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let mut existing = evidence("source-cross-date-existing", 1, "pending");
        existing.title = "Acme restates quarterly guidance".to_string();
        existing.content = Some("Management reaffirmed the same guidance ranges.".to_string());
        existing.content_hash =
            "a4efde61076cb7ceb4c12e2e262019d8526e8b556dc8055c66263fe56bf0851d".to_string();
        existing.source_url = Some("https://example.test/existing-guidance".to_string());
        existing.effective_trade_date = date(2026, 7, 10);
        existing.available_at = dt(2026, 7, 10, 10);
        existing.first_seen_at = dt(2026, 7, 10, 10);
        existing.created_at = dt(2026, 7, 10, 11);
        save_evidence(&pool, &existing).await;

        let mut duplicate = evidence("source-cross-date-submitted", 1, "pending");
        duplicate.title = "Acme restates quarterly guidance".to_string();
        duplicate.content = Some("Management reaffirmed the same guidance ranges.".to_string());
        duplicate.content_hash = existing.content_hash.clone();
        duplicate.source_url = Some("https://example.test/submitted-guidance".to_string());
        duplicate.effective_trade_date = date(2026, 7, 13);
        duplicate.available_at = dt(2026, 7, 10, 13);
        duplicate.first_seen_at = dt(2026, 7, 10, 13);
        duplicate.created_at = dt(2026, 7, 10, 13);

        let inserted = repo.insert_manual_evidence(&duplicate).await.unwrap();

        assert_eq!(
            inserted
                .existing_rows
                .iter()
                .map(|row| row.evidence_id)
                .collect::<Vec<_>>(),
            vec![existing.evidence_id]
        );
        assert_eq!(
            inserted.existing_rows[0].effective_trade_date,
            date(2026, 7, 10)
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_insert_surfaces_cross_trade_date_canonical_url_exact_duplicate_candidates(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let mut existing = evidence("source-cross-url-existing", 1, "pending");
        existing.title = "Acme contract post from archive".to_string();
        existing.content = Some("Archived copy with legacy formatting.".to_string());
        existing.content_hash =
            "3c8d4cf22f5a8ab349a0d1238b531f8cbe9a2459f1bfd4f64d2c7d90840bbce1".to_string();
        existing.source_url = Some("HTTPS://Example.test:443/contracts/acme#archive".to_string());
        existing.effective_trade_date = date(2026, 7, 10);
        existing.available_at = dt(2026, 7, 10, 10);
        existing.first_seen_at = dt(2026, 7, 10, 10);
        existing.created_at = dt(2026, 7, 10, 11);
        insert_raw_evidence_row(&pool, &existing).await;

        let mut duplicate = evidence("source-cross-url-submitted", 1, "pending");
        duplicate.title = "Acme contract post mirrored later".to_string();
        duplicate.content = Some("Mirror copy after formatting changes.".to_string());
        duplicate.content_hash =
            "d1d0d4f6e86dcb0df4ccf5b00b1588f2bba4f43c516a2de0060d44e0f8ef4614".to_string();
        duplicate.source_url = Some("https://example.test/contracts/acme".to_string());
        duplicate.effective_trade_date = date(2026, 7, 13);
        duplicate.available_at = dt(2026, 7, 10, 13);
        duplicate.first_seen_at = dt(2026, 7, 10, 13);
        duplicate.created_at = dt(2026, 7, 10, 13);

        let inserted = repo.insert_manual_evidence(&duplicate).await.unwrap();

        assert_eq!(
            inserted
                .existing_rows
                .iter()
                .map(|row| row.evidence_id)
                .collect::<Vec<_>>(),
            vec![existing.evidence_id]
        );
        assert_eq!(
            inserted.existing_rows[0].source_url.as_deref(),
            Some("HTTPS://Example.test:443/contracts/acme#archive")
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn concurrent_mixed_tier_exact_duplicates_share_one_discovery_lock(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone())
            .clone_with_manual_insert_sleep_after_candidate_discovery_for_test(
                Duration::from_millis(250),
            );
        let shared_hash = "shared-cross-tier-duplicate-hash".to_string();

        let mut first = evidence("mixed-tier-first", 1, "pending");
        first.source_id = "manual:rest".to_string();
        first.source_tier = "manual".to_string();
        first.title = "Cross-tier duplicate candidate".to_string();
        first.content = Some("Shared duplicate payload across tiers.".to_string());
        first.content_hash = shared_hash.clone();
        first.source_url = Some("https://example.test/mixed-tier-first".to_string());
        first.available_at = dt(2026, 7, 10, 12);
        first.first_seen_at = dt(2026, 7, 10, 12);
        first.created_at = dt(2026, 7, 10, 12);

        let mut second = evidence("mixed-tier-second", 1, "pending");
        second.source_id = "feed:wire".to_string();
        second.source_tier = "wire".to_string();
        second.title = "Cross-tier duplicate candidate from another tier".to_string();
        second.content = Some("Shared duplicate payload across tiers.".to_string());
        second.content_hash = shared_hash.clone();
        second.source_url = Some("https://example.test/mixed-tier-second".to_string());
        second.available_at = dt(2026, 7, 10, 13);
        second.first_seen_at = dt(2026, 7, 10, 13);
        second.created_at = dt(2026, 7, 10, 13);

        let first_worker = tokio::spawn({
            let repo = repo.clone();
            let first = first.clone();
            async move { classify_manual_insert(repo, first).await }
        });
        yield_now().await;
        let second_worker = tokio::spawn(async move { classify_manual_insert(repo, second).await });

        let outcomes = [
            first_worker.await.unwrap().unwrap(),
            second_worker.await.unwrap().unwrap(),
        ];
        let inserted_without_existing = outcomes
            .iter()
            .filter(|outcome| {
                matches!(
                    outcome,
                    ManualDuplicateDiscoveryOutcome::InsertedWithoutExisting
                )
            })
            .count();
        let existing_found = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ManualDuplicateDiscoveryOutcome::ExistingFound))
            .count();

        assert_eq!(inserted_without_existing, 1);
        assert_eq!(existing_found, 1);

        let stored = EventRepository::new(pool)
            .find_by_content_hash(&shared_hash)
            .await
            .unwrap();
        assert_eq!(stored.len(), 2);

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
    async fn unlocked_duplicate_groups_remove_omitted_members_on_resave(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let retained_member = evidence("source-duplicate-retained", 1, "publishable");
        let omitted_member = evidence("source-duplicate-omitted", 1, "publishable");
        save_evidence(&pool, &retained_member).await;
        save_evidence(&pool, &omitted_member).await;

        let group_id = Uuid::new_v4();
        let original = DuplicateGroupRow {
            duplicate_group_id: group_id,
            relation_type: "exact".to_string(),
            confidence: 0.55,
            locked_by_user: false,
            members: vec![
                DuplicateGroupMemberRow {
                    evidence_id: retained_member.evidence_id,
                    is_representative: false,
                },
                DuplicateGroupMemberRow {
                    evidence_id: omitted_member.evidence_id,
                    is_representative: true,
                },
            ],
            created_at: dt(2026, 7, 10, 12),
        };
        repo.save_duplicate_group(&original).await.unwrap();

        let updated = DuplicateGroupRow {
            relation_type: "near".to_string(),
            confidence: 0.85,
            members: vec![DuplicateGroupMemberRow {
                evidence_id: retained_member.evidence_id,
                is_representative: true,
            }],
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
        assert_eq!(members, vec![(retained_member.evidence_id, true)]);
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
