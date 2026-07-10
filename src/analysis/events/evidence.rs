use std::sync::Arc;

use chrono::{DateTime, Utc};
use reqwest::Url;
use serde_json::json;
use uuid::Uuid;

use super::time::{effective_trade_date_for_manual, manual_available_at};
use super::{
    EventEvidence, ExistingEventEvidenceRelation, ManualEventInput, ManualEventSubmissionOutcome,
    TradingDateResolver,
};
use crate::error::{AppError, Result};
use crate::storage::event_repository::{EventEvidenceRow, EventRepository};

pub(crate) const MANUAL_SOURCE_TELEGRAM: &str = "manual:telegram";
pub(crate) const MANUAL_SOURCE_REST: &str = "manual:rest";

const MANUAL_SOURCE_TIER: &str = "manual";
const MANUAL_SOURCE_TERMS_VERSION: &str = "terms-v1";
const MANUAL_LANGUAGE: &str = "und";
const MANUAL_STATUS_PENDING: &str = "pending";

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
}

impl ManualEvidenceIngestor {
    pub(crate) fn new(repo: EventRepository, resolver: Arc<dyn TradingDateResolver>) -> Self {
        Self { repo, resolver }
    }

    pub(crate) async fn submit_at(
        &self,
        source: ManualSource,
        input: ManualEventInput,
        first_seen_at: DateTime<Utc>,
    ) -> Result<ManualEventSubmissionOutcome> {
        let normalized_title = normalize_text(&input.title);
        if normalized_title.is_empty() {
            return Err(AppError::Internal(
                "manual event title cannot be empty after normalization".to_string(),
            ));
        }

        let normalized_content = input
            .content
            .as_deref()
            .map(normalize_text)
            .and_then(|content| (!content.is_empty()).then_some(content));
        let canonical_source_url = input
            .source_url
            .as_deref()
            .map(canonicalize_source_url)
            .transpose()?;
        let content_hash = content_hash(&normalized_title, normalized_content.as_deref());
        let available_at = manual_available_at(input.published_at, first_seen_at);
        let effective_trade_date =
            effective_trade_date_for_manual(self.resolver.as_ref(), available_at)?;
        #[cfg(test)]
        {
            let _ = self.repo.find_by_content_hash(&content_hash).await?;
            wait_after_duplicate_lookup().await;
        }

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
        let insert_result = self.repo.insert_manual_evidence(&row).await?;

        let submitted = event_evidence_from_row(&row);
        if insert_result.existing_rows.is_empty() {
            return Ok(ManualEventSubmissionOutcome::Inserted(submitted));
        }

        let representative = insert_result.existing_rows[0].clone();
        let duplicate_group_id = insert_result
            .duplicate_group_id
            .expect("duplicate submissions must record a duplicate group");

        Ok(ManualEventSubmissionOutcome::Existing(
            ExistingEventEvidenceRelation {
                submitted,
                existing: event_evidence_from_row(&representative),
                duplicate_group_id,
            },
        ))
    }
}

pub(crate) fn normalize_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
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

fn canonicalize_source_url(value: &str) -> Result<String> {
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

#[cfg(test)]
async fn wait_after_duplicate_lookup() {
    test_support::wait_after_duplicate_lookup().await;
}

#[cfg(test)]
mod test_support {
    use std::sync::{Arc, Mutex, OnceLock};

    use tokio::sync::Barrier;

    static AFTER_DUPLICATE_LOOKUP_BARRIER: OnceLock<Mutex<Option<Arc<Barrier>>>> = OnceLock::new();

    pub(super) struct DuplicateLookupBarrierGuard;

    pub(super) fn install_duplicate_lookup_barrier(parties: usize) -> DuplicateLookupBarrierGuard {
        let barrier = Arc::new(Barrier::new(parties));
        let mut slot = barrier_slot().lock().unwrap();
        assert!(slot.replace(barrier).is_none());
        DuplicateLookupBarrierGuard
    }

    pub(super) async fn wait_after_duplicate_lookup() {
        let barrier = {
            let slot = barrier_slot().lock().unwrap();
            slot.clone()
        };

        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
    }

    impl Drop for DuplicateLookupBarrierGuard {
        fn drop(&mut self) {
            let mut slot = barrier_slot().lock().unwrap();
            *slot = None;
        }
    }

    fn barrier_slot() -> &'static Mutex<Option<Arc<Barrier>>> {
        AFTER_DUPLICATE_LOOKUP_BARRIER.get_or_init(|| Mutex::new(None))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use sha2::{Digest, Sha256};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::{
        test_support::install_duplicate_lookup_barrier, ManualEvidenceIngestor, ManualSource,
        MANUAL_SOURCE_REST, MANUAL_SOURCE_TELEGRAM,
    };
    use crate::analysis::events::{
        AShareTradingDateResolver, EventIntelligence, ExistingEventEvidenceRelation,
        ManualEventInput, ManualEventSubmissionOutcome,
    };
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
    async fn repeated_manual_submission_returns_existing_evidence_relation(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor =
            ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));
        let input = ManualEventInput {
            title: " ACME   wins   contract ".to_string(),
            content: Some("Order value\n exceeds guidance".to_string()),
            source_url: Some("https://example.com/contracts/acme".to_string()),
            submitted_by: "operator".to_string(),
            published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
        };

        let first = assert_inserted(
            ingestor
                .submit_at(ManualSource::Rest, input.clone(), dt(2026, 7, 10, 8, 0, 0))
                .await
                .unwrap(),
        );
        let duplicate = assert_existing(
            ingestor
                .submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 5, 0))
                .await
                .unwrap(),
        );

        assert_eq!(first.source_id, MANUAL_SOURCE_REST);
        assert_eq!(duplicate.existing.evidence_id, first.evidence_id);
        assert_eq!(duplicate.existing.content_hash, first.content_hash);
        assert_ne!(duplicate.submitted.evidence_id, first.evidence_id);
        assert_ne!(duplicate.submitted.source_item_id, first.source_item_id);
        assert_eq!(duplicate.submitted.source_id, MANUAL_SOURCE_REST);

        let same_hash = repo
            .find_by_content_hash(&first.content_hash)
            .await
            .unwrap();
        assert_eq!(same_hash.len(), 2);

        let group: (String, f64) = sqlx::query_as(
            r#"SELECT relation_type, confidence::float8
               FROM market_event_duplicate_groups
               WHERE duplicate_group_id = $1"#,
        )
        .bind(duplicate.duplicate_group_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(group.0, "exact");
        assert_eq!(group.1, 1.0);

        let members: Vec<(Uuid, bool)> = sqlx::query_as(
            r#"SELECT evidence_id, is_representative
               FROM market_event_duplicate_members
               WHERE duplicate_group_id = $1
               ORDER BY is_representative DESC, evidence_id ASC"#,
        )
        .bind(duplicate.duplicate_group_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(members.len(), 2);
        assert_eq!(members[0], (first.evidence_id, true));
        assert!(members
            .iter()
            .any(|member| member.0 == duplicate.submitted.evidence_id && !member.1));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn concurrent_identical_manual_submissions_report_one_insert_and_one_existing(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor =
            ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));
        let input = ManualEventInput {
            title: " ACME   wins   contract ".to_string(),
            content: Some("Order value\n exceeds guidance".to_string()),
            source_url: Some("https://example.com/contracts/acme".to_string()),
            submitted_by: "operator".to_string(),
            published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
        };
        let _barrier = install_duplicate_lookup_barrier(2);

        let (left, right) = tokio::join!(
            ingestor.submit_at(ManualSource::Rest, input.clone(), dt(2026, 7, 10, 8, 0, 0)),
            ingestor.submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 0, 1)),
        );
        let outcomes = [left.unwrap(), right.unwrap()];

        let inserted_count = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ManualEventSubmissionOutcome::Inserted(_)))
            .count();
        let existing_relations: Vec<ExistingEventEvidenceRelation> = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                ManualEventSubmissionOutcome::Inserted(_) => None,
                ManualEventSubmissionOutcome::Existing(existing) => Some(existing.clone()),
            })
            .collect();

        assert_eq!(inserted_count, 1);
        assert_eq!(existing_relations.len(), 1);

        let same_hash = repo
            .find_by_content_hash(&existing_relations[0].existing.content_hash)
            .await
            .unwrap();
        assert_eq!(same_hash.len(), 2);

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
        outcome: ManualEventSubmissionOutcome,
    ) -> crate::analysis::events::EventEvidence {
        match outcome {
            ManualEventSubmissionOutcome::Inserted(evidence) => evidence,
            ManualEventSubmissionOutcome::Existing(existing) => {
                panic!(
                    "expected inserted evidence, got duplicate relation for {}",
                    existing.existing.evidence_id
                )
            }
        }
    }

    fn assert_existing(outcome: ManualEventSubmissionOutcome) -> ExistingEventEvidenceRelation {
        match outcome {
            ManualEventSubmissionOutcome::Inserted(evidence) => {
                panic!(
                    "expected duplicate relation, got inserted {}",
                    evidence.evidence_id
                )
            }
            ManualEventSubmissionOutcome::Existing(existing) => existing,
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
