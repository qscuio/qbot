use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

use super::dedup::{DuplicateCandidate, DuplicateDecider, DuplicateResolution, DuplicateSubject};
use super::time::{effective_trade_date_for_manual, manual_available_at};
use super::{
    EventEvidence, ExistingEventEvidenceRelation, ManualEventInput, ManualEventSubmissionOutcome,
    TradingDateResolver,
};
use crate::error::{AppError, Result};
use crate::storage::event_repository::{
    DuplicateGroupMemberRow, DuplicateGroupRow, EventEvidenceRow, EventRepository,
    ManualDuplicateCandidateRow, ManualEvidenceInsertContext, ManualEvidenceInsertEffect,
};

pub(crate) const MANUAL_SOURCE_TELEGRAM: &str = "manual:telegram";
pub(crate) const MANUAL_SOURCE_REST: &str = "manual:rest";

const MANUAL_SOURCE_TIER: &str = "manual";
const MANUAL_SOURCE_TERMS_VERSION: &str = "terms-v1";
const MANUAL_LANGUAGE: &str = "und";
const MANUAL_STATUS_PENDING: &str = "pending";
const MANUAL_AUTO_NEAR_DUPLICATE_THRESHOLD: f64 = 0.92;
const DERIVED_TITLE_MAX_CHARS: usize = 120;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManualSource {
    Telegram,
    Rest,
}

impl ManualSource {
    pub(crate) fn source_id(self) -> &'static str {
        match self {
            Self::Telegram => MANUAL_SOURCE_TELEGRAM,
            Self::Rest => MANUAL_SOURCE_REST,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ManualEvidenceIngestor {
    repo: EventRepository,
    resolver: Arc<dyn TradingDateResolver>,
    auto_near_duplicate_threshold: f64,
}

impl ManualEvidenceIngestor {
    pub(crate) fn new(repo: EventRepository, resolver: Arc<dyn TradingDateResolver>) -> Self {
        Self::with_auto_near_duplicate_threshold(
            repo,
            resolver,
            MANUAL_AUTO_NEAR_DUPLICATE_THRESHOLD,
        )
    }

    pub(crate) fn with_auto_near_duplicate_threshold(
        repo: EventRepository,
        resolver: Arc<dyn TradingDateResolver>,
        auto_near_duplicate_threshold: f64,
    ) -> Self {
        Self {
            repo,
            resolver,
            auto_near_duplicate_threshold,
        }
    }

    pub(crate) async fn submit_at(
        &self,
        source: ManualSource,
        input: ManualEventInput,
        first_seen_at: DateTime<Utc>,
    ) -> Result<ManualEventSubmissionOutcome> {
        let normalized_content = input
            .content
            .as_deref()
            .map(normalize_text)
            .and_then(|content| (!content.is_empty()).then_some(content));
        let normalized_title =
            normalized_manual_title(&input.title, normalized_content.as_deref())?;
        let canonical_source_url = input
            .source_url
            .as_deref()
            .map(canonicalize_source_url)
            .transpose()?;
        let content_hash = content_hash(&normalized_title, normalized_content.as_deref());
        let available_at = manual_available_at(input.published_at, first_seen_at);
        let effective_trade_date =
            effective_trade_date_for_manual(self.resolver.as_ref(), available_at)?;

        let row = EventEvidenceRow {
            evidence_id: Uuid::new_v4(),
            source_id: source.source_id().to_string(),
            source_item_id: Uuid::new_v4().to_string(),
            source_url: canonical_source_url,
            source_tier: MANUAL_SOURCE_TIER.to_string(),
            source_terms_version: MANUAL_SOURCE_TERMS_VERSION.to_string(),
            occurred_at: input.published_at,
            published_at: input.published_at,
            first_seen_at,
            available_at,
            effective_trade_date,
            title: normalized_title,
            content: normalized_content,
            language: MANUAL_LANGUAGE.to_string(),
            content_hash: content_hash.clone(),
            raw_payload: json!({
                "submitted_by": input.submitted_by,
                "manual_source_id": source.source_id(),
            }),
            version: 1,
            supersedes_evidence_id: None,
            status: MANUAL_STATUS_PENDING.to_string(),
            created_at: first_seen_at,
        };
        let submitted = event_evidence_from_row(&row);
        let outcome = self
            .repo
            .insert_manual_evidence_with_effect(&row, |context| async move {
                build_manual_submission_effect(
                    self.auto_near_duplicate_threshold,
                    submitted,
                    context,
                )
            })
            .await?;

        Ok(outcome)
    }
}

pub(crate) fn normalize_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalized_manual_title(title: &str, content: Option<&str>) -> Result<String> {
    let normalized_title = normalize_text(title);
    if !normalized_title.is_empty() {
        return Ok(normalized_title);
    }

    content
        .map(derive_title_from_content)
        .ok_or_else(|| AppError::BadRequest("manual event title cannot be empty".to_string()))
}

fn derive_title_from_content(content: &str) -> String {
    content
        .chars()
        .take(DERIVED_TITLE_MAX_CHARS)
        .collect::<String>()
}

pub(crate) fn content_hash(title: &str, content: Option<&str>) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(normalize_text(title));
    if let Some(content) = content {
        hasher.update([0]);
        hasher.update(normalize_text(content));
    }
    format!("{:x}", hasher.finalize())
}

pub(crate) fn canonicalize_source_url(value: &str) -> Result<String> {
    crate::storage::event_repository::canonicalize_source_url(value)
}

fn event_evidence_from_row(row: &EventEvidenceRow) -> EventEvidence {
    EventEvidence {
        evidence_id: row.evidence_id,
        source_id: row.source_id.clone(),
        source_item_id: row.source_item_id.clone(),
        source_tier: row.source_tier.clone(),
        published_at: row.published_at,
        first_seen_at: row.first_seen_at,
        available_at: row.available_at,
        effective_trade_date: row.effective_trade_date,
        title: row.title.clone(),
        content_hash: row.content_hash.clone(),
        status: row.status.clone(),
    }
}

fn duplicate_subject_from_row(row: &EventEvidenceRow) -> DuplicateSubject {
    DuplicateSubject {
        source_id: row.source_id.clone(),
        source_item_id: row.source_item_id.clone(),
        version: row.version,
        source_url: row.source_url.clone(),
        title: row.title.clone(),
        content: row.content.clone(),
        content_hash: row.content_hash.clone(),
    }
}

fn duplicate_candidates_from_rows(rows: &[ManualDuplicateCandidateRow]) -> Vec<DuplicateCandidate> {
    rows.iter()
        .map(|candidate| DuplicateCandidate {
            evidence_id: candidate.row.evidence_id,
            representative_id: candidate.representative_evidence_id,
            source_id: candidate.row.source_id.clone(),
            source_item_id: candidate.row.source_item_id.clone(),
            version: candidate.row.version,
            source_url: candidate.row.source_url.clone(),
            title: candidate.row.title.clone(),
            content: candidate.row.content.clone(),
            content_hash: candidate.row.content_hash.clone(),
        })
        .collect()
}

fn build_manual_submission_effect(
    auto_near_duplicate_threshold: f64,
    submitted: EventEvidence,
    context: ManualEvidenceInsertContext,
) -> Result<ManualEvidenceInsertEffect<ManualEventSubmissionOutcome>> {
    if context.existing_rows.is_empty() {
        return Ok(ManualEvidenceInsertEffect {
            result: ManualEventSubmissionOutcome::Inserted(submitted),
            duplicate_group: None,
        });
    }

    let decision = DuplicateDecider::new(auto_near_duplicate_threshold).classify(
        &duplicate_subject_from_row(&context.submitted_row),
        &duplicate_candidates_from_rows(&context.existing_candidates),
    );

    if matches!(decision, DuplicateResolution::Independent) {
        return Ok(ManualEvidenceInsertEffect {
            result: ManualEventSubmissionOutcome::Inserted(submitted),
            duplicate_group: None,
        });
    }

    let duplicate_group = duplicate_group_from_decision(
        &decision,
        &context.existing_candidates,
        &context.submitted_row,
    );
    let representative_id = decision
        .representative_id()
        .expect("non-independent duplicate decisions must select a representative");
    let representative = context
        .existing_rows
        .iter()
        .find(|existing| existing.evidence_id == representative_id)
        .cloned()
        .expect("duplicate decision representative must exist in the existing-row candidate set");

    Ok(ManualEvidenceInsertEffect {
        result: ManualEventSubmissionOutcome::Existing(ExistingEventEvidenceRelation {
            submitted,
            existing: event_evidence_from_row(&representative),
        }),
        duplicate_group: Some(duplicate_group),
    })
}

fn duplicate_group_from_decision(
    decision: &DuplicateResolution,
    existing_candidates: &[ManualDuplicateCandidateRow],
    submitted_row: &EventEvidenceRow,
) -> DuplicateGroupRow {
    let representative_id = decision
        .representative_id()
        .expect("independent submissions must not persist duplicate groups");
    let confidence = decision
        .confidence()
        .expect("independent submissions must not persist duplicate groups");
    let default_relation_type = decision
        .relation_type()
        .expect("independent submissions must not persist duplicate groups");
    let candidate_ids: std::collections::BTreeSet<Uuid> =
        decision.candidate_ids().iter().copied().collect();
    let involved_representative_ids: std::collections::BTreeSet<Uuid> = existing_candidates
        .iter()
        .filter(|candidate| {
            candidate.row.evidence_id == representative_id
                || candidate_ids.contains(&candidate.row.evidence_id)
        })
        .map(|candidate| candidate.representative_evidence_id)
        .collect();
    let relation_type = if involved_representative_ids.len() > 1 {
        "review_required"
    } else {
        default_relation_type
    };

    let member_id_set: std::collections::BTreeSet<Uuid> = existing_candidates
        .iter()
        .filter(|candidate| {
            candidate.row.evidence_id == representative_id
                || (candidate.representative_evidence_id == representative_id
                    && candidate_ids.contains(&candidate.row.evidence_id))
        })
        .map(|candidate| candidate.row.evidence_id)
        .chain(std::iter::once(submitted_row.evidence_id))
        .collect();
    let mut members: Vec<DuplicateGroupMemberRow> = existing_candidates
        .iter()
        .filter(|candidate| member_id_set.contains(&candidate.row.evidence_id))
        .map(|candidate| DuplicateGroupMemberRow {
            evidence_id: candidate.row.evidence_id,
            is_representative: candidate.row.evidence_id == representative_id,
        })
        .collect();
    members.push(DuplicateGroupMemberRow {
        evidence_id: submitted_row.evidence_id,
        is_representative: false,
    });

    DuplicateGroupRow {
        duplicate_group_id: duplicate_group_id(representative_id),
        relation_type: relation_type.to_string(),
        confidence,
        locked_by_user: false,
        members,
        created_at: submitted_row.created_at,
    }
}

fn duplicate_group_id(representative_id: Uuid) -> Uuid {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(format!("market-event-duplicate:{representative_id}").as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
#[path = "evidence_duplicate_ingestion_tests.rs"]
mod duplicate_ingestion_tests;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use sha2::{Digest, Sha256};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::{ManualEvidenceIngestor, ManualSource, MANUAL_SOURCE_REST, MANUAL_SOURCE_TELEGRAM};
    use crate::analysis::events::{AShareTradingDateResolver, EventIntelligence, ManualEventInput};
    use crate::storage::event_repository::EventRepository;

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_ingestion_normalizes_text_url_hash_and_trade_date(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor =
            ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));
        let first_seen_at = dt(2026, 7, 10, 6, 45, 0);

        let outcome = ingestor
            .submit_at(
                ManualSource::Telegram,
                ManualEventInput {
                    title: "  Major\t update \n from   ACME  ".to_string(),
                    content: Some("  first line \n\n second\tline  ".to_string()),
                    source_url: Some(
                        "HTTPS://Example.com:443/news/flash?a=1&b=2#ignored".to_string(),
                    ),
                    submitted_by: "analyst".to_string(),
                    published_at: Some(dt(2026, 7, 10, 6, 30, 0)),
                },
                first_seen_at,
            )
            .await
            .unwrap();

        let inserted = assert_inserted(outcome);
        assert_eq!(inserted.source_id, MANUAL_SOURCE_TELEGRAM);
        assert_eq!(inserted.source_tier, "manual");
        assert_eq!(inserted.status, "pending");
        assert_eq!(inserted.published_at, Some(dt(2026, 7, 10, 6, 30, 0)));
        assert_eq!(
            inserted.effective_trade_date,
            NaiveDate::from_ymd_opt(2026, 7, 10).unwrap()
        );
        assert!(Uuid::parse_str(&inserted.source_item_id).is_ok());

        let rows = repo
            .find_existing_source_item(&inserted.source_id, &inserted.source_item_id)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "Major update from ACME");
        assert_eq!(rows[0].content.as_deref(), Some("first line second line"));
        assert_eq!(
            rows[0].source_url.as_deref(),
            Some("https://example.com/news/flash?a=1&b=2")
        );
        assert_eq!(
            rows[0].content_hash,
            expected_content_hash("Major update from ACME", Some("first line second line"))
        );
        assert_eq!(rows[0].content_hash, inserted.content_hash);
        assert_eq!(rows[0].first_seen_at, first_seen_at);
        assert_eq!(rows[0].available_at, dt(2026, 7, 10, 6, 30, 0));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_intelligence_submit_manual_event_uses_rest_source(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let intelligence = EventIntelligence::with_repository_and_resolver(
            EventRepository::new(pool.clone()),
            Arc::new(AShareTradingDateResolver),
        );

        let evidence = assert_public_inserted(
            intelligence
                .submit_manual_event(ManualEventInput {
                    title: " REST submitted event ".to_string(),
                    content: None,
                    source_url: None,
                    submitted_by: "api-user".to_string(),
                    published_at: None,
                })
                .await
                .unwrap(),
        );

        assert_eq!(evidence.source_id, MANUAL_SOURCE_REST);

        let rows = EventRepository::new(pool)
            .find_existing_source_item(&evidence.source_id, &evidence.source_item_id)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "REST submitted event");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_intelligence_public_constructor_wires_manual_event_submission(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let intelligence = EventIntelligence::new(pool.clone());

        let evidence = assert_public_inserted(
            intelligence
                .submit_manual_event(ManualEventInput {
                    title: " Public constructor event ".to_string(),
                    content: Some("normalized  body".to_string()),
                    source_url: Some("https://example.com/public-constructor".to_string()),
                    submitted_by: "api-user".to_string(),
                    published_at: None,
                })
                .await
                .unwrap(),
        );

        let rows = EventRepository::new(pool)
            .find_existing_source_item(&evidence.source_id, &evidence.source_item_id)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "Public constructor event");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_ingestion_derives_title_from_content_when_title_is_blank(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor =
            ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

        let outcome = ingestor
            .submit_at(
                ManualSource::Rest,
                ManualEventInput {
                    title: " \n\t ".to_string(),
                    content: Some(
                        "  ACME   signs definitive merger agreement \n after board approval  "
                            .to_string(),
                    ),
                    source_url: None,
                    submitted_by: "api-user".to_string(),
                    published_at: None,
                },
                dt(2026, 7, 10, 8, 30, 0),
            )
            .await
            .unwrap();

        let inserted = assert_public_inserted(outcome);
        let expected_title = "ACME signs definitive merger agreement after board approval";
        assert_eq!(inserted.title, expected_title);
        assert_eq!(
            inserted.content_hash,
            expected_content_hash(
                expected_title,
                Some("ACME signs definitive merger agreement after board approval")
            )
        );

        let rows = repo
            .find_existing_source_item(&inserted.source_id, &inserted.source_item_id)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, expected_title);
        assert_eq!(
            rows[0].content.as_deref(),
            Some("ACME signs definitive merger agreement after board approval")
        );
        assert_eq!(rows[0].content_hash, inserted.content_hash);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_intelligence_submit_manual_event_exposes_existing_relation_publicly(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let intelligence = EventIntelligence::with_repository_and_resolver(
            EventRepository::new(pool),
            Arc::new(AShareTradingDateResolver),
        );
        let input = ManualEventInput {
            title: " REST submitted duplicate ".to_string(),
            content: Some("same payload".to_string()),
            source_url: None,
            submitted_by: "api-user".to_string(),
            published_at: None,
        };

        let first = assert_public_inserted(
            intelligence
                .submit_manual_event(input.clone())
                .await
                .unwrap(),
        );
        let duplicate =
            assert_public_existing(intelligence.submit_manual_event(input).await.unwrap());

        assert_eq!(duplicate.existing.evidence_id, first.evidence_id);
        assert_ne!(duplicate.submitted.evidence_id, first.evidence_id);

        Ok(())
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .unwrap()
    }

    fn expected_content_hash(title: &str, content: Option<&str>) -> String {
        let mut hasher = Sha256::new();
        hasher.update(normalize_for_expectation(title));
        if let Some(content) = content {
            hasher.update([0]);
            hasher.update(normalize_for_expectation(content));
        }
        format!("{:x}", hasher.finalize())
    }

    fn normalize_for_expectation(value: &str) -> String {
        value.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn assert_inserted(
        outcome: crate::analysis::events::ManualEventSubmissionOutcome,
    ) -> crate::analysis::events::EventEvidence {
        match outcome {
            crate::analysis::events::ManualEventSubmissionOutcome::Inserted(evidence) => evidence,
            crate::analysis::events::ManualEventSubmissionOutcome::Existing(existing) => {
                panic!(
                    "expected inserted evidence, got duplicate relation for {}",
                    existing.existing.evidence_id
                )
            }
        }
    }

    fn assert_public_inserted(
        outcome: crate::analysis::events::ManualEventSubmissionOutcome,
    ) -> crate::analysis::events::EventEvidence {
        match outcome {
            crate::analysis::events::ManualEventSubmissionOutcome::Inserted(evidence) => evidence,
            crate::analysis::events::ManualEventSubmissionOutcome::Existing(existing) => {
                panic!(
                    "expected public inserted evidence, got duplicate relation for {}",
                    existing.existing.evidence_id
                )
            }
        }
    }

    fn assert_public_existing(
        outcome: crate::analysis::events::ManualEventSubmissionOutcome,
    ) -> crate::analysis::events::ExistingEventEvidenceRelation {
        match outcome {
            crate::analysis::events::ManualEventSubmissionOutcome::Inserted(evidence) => {
                panic!(
                    "expected public duplicate relation, got inserted {}",
                    evidence.evidence_id
                )
            }
            crate::analysis::events::ManualEventSubmissionOutcome::Existing(existing) => existing,
        }
    }
}
