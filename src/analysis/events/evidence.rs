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
    ManualDuplicateCandidateRow,
};

pub(crate) const MANUAL_SOURCE_TELEGRAM: &str = "manual:telegram";
pub(crate) const MANUAL_SOURCE_REST: &str = "manual:rest";

const MANUAL_SOURCE_TIER: &str = "manual";
const MANUAL_SOURCE_TERMS_VERSION: &str = "terms-v1";
const MANUAL_LANGUAGE: &str = "und";
const MANUAL_STATUS_PENDING: &str = "pending";
const MANUAL_AUTO_NEAR_DUPLICATE_THRESHOLD: f64 = 0.92;

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
    #[cfg(test)]
    duplicate_lookup_barrier: Option<test_support::DuplicateLookupBarrier>,
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
            #[cfg(test)]
            duplicate_lookup_barrier: None,
        }
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
            self.wait_after_duplicate_lookup(&content_hash).await;
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

        let decision = DuplicateDecider::new(self.auto_near_duplicate_threshold).classify(
            &duplicate_subject_from_row(&row),
            &duplicate_candidates_from_rows(&insert_result.existing_candidates),
        );

        if matches!(decision, DuplicateResolution::Independent) {
            return Ok(ManualEventSubmissionOutcome::Inserted(submitted));
        }

        let duplicate_group =
            duplicate_group_from_decision(&decision, &insert_result.existing_rows, &row);
        self.repo.append_duplicate_group(&duplicate_group).await?;

        let representative_id = decision
            .representative_id()
            .expect("non-independent duplicate decisions must select a representative");
        let representative = insert_result
            .existing_rows
            .iter()
            .find(|existing| existing.evidence_id == representative_id)
            .cloned()
            .expect(
                "duplicate decision representative must exist in the existing-row candidate set",
            );

        Ok(ManualEventSubmissionOutcome::Existing(
            ExistingEventEvidenceRelation {
                submitted,
                existing: event_evidence_from_row(&representative),
            },
        ))
    }

    #[cfg(test)]
    fn clone_with_duplicate_lookup_barrier_for_test(
        &self,
        content_hash: impl Into<String>,
        parties: usize,
    ) -> Self {
        let mut clone = self.clone();
        clone.duplicate_lookup_barrier = Some(
            test_support::DuplicateLookupBarrier::for_content_hash(content_hash, parties),
        );
        clone
    }

    #[cfg(test)]
    async fn wait_after_duplicate_lookup(&self, content_hash: &str) {
        if let Some(barrier) = &self.duplicate_lookup_barrier {
            barrier.wait(content_hash).await;
        }
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

fn duplicate_group_from_decision(
    decision: &DuplicateResolution,
    existing_rows: &[EventEvidenceRow],
    submitted_row: &EventEvidenceRow,
) -> DuplicateGroupRow {
    let representative_id = decision
        .representative_id()
        .expect("independent submissions must not persist duplicate groups");
    let confidence = decision
        .confidence()
        .expect("independent submissions must not persist duplicate groups");
    let relation_type = decision
        .relation_type()
        .expect("independent submissions must not persist duplicate groups");

    let member_id_set: std::collections::BTreeSet<Uuid> = decision
        .candidate_ids()
        .iter()
        .copied()
        .chain(decision.representative_id())
        .chain(std::iter::once(submitted_row.evidence_id))
        .collect();
    let mut members: Vec<DuplicateGroupMemberRow> = existing_rows
        .iter()
        .filter(|row| member_id_set.contains(&row.evidence_id))
        .map(|row| DuplicateGroupMemberRow {
            evidence_id: row.evidence_id,
            is_representative: row.evidence_id == representative_id,
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
mod test_support {
    use std::sync::Arc;

    use tokio::sync::Barrier;

    #[derive(Clone)]
    pub(super) enum DuplicateLookupBarrierScope {
        ContentHash(String),
    }

    #[derive(Clone)]
    pub(super) struct DuplicateLookupBarrier {
        scope: DuplicateLookupBarrierScope,
        barrier: Arc<Barrier>,
    }

    impl DuplicateLookupBarrier {
        pub(super) fn for_content_hash(content_hash: impl Into<String>, parties: usize) -> Self {
            Self {
                scope: DuplicateLookupBarrierScope::ContentHash(content_hash.into()),
                barrier: Arc::new(Barrier::new(parties)),
            }
        }
        pub(super) async fn wait(&self, content_hash: &str) {
            let should_wait = match &self.scope {
                DuplicateLookupBarrierScope::ContentHash(expected_hash) => {
                    expected_hash == content_hash
                }
            };
            if should_wait {
                self.barrier.wait().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use sqlx::PgPool;
    use tokio::task::yield_now;
    use uuid::Uuid;

    use super::{
        content_hash, ManualEvidenceIngestor, ManualSource, MANUAL_SOURCE_REST,
        MANUAL_SOURCE_TELEGRAM,
    };
    use crate::analysis::events::{
        AShareTradingDateResolver, EventIntelligence, ExistingEventEvidenceRelation,
        ManualEventInput, ManualEventSubmissionOutcome,
    };
    use crate::storage::event_repository::{EventEvidenceRow, EventRepository};

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
        let duplicate_group_id: Uuid = sqlx::query_scalar(
            r#"SELECT duplicate_group_id
               FROM market_event_duplicate_members
               WHERE evidence_id = $1"#,
        )
        .bind(duplicate.submitted.evidence_id)
        .fetch_one(&pool)
        .await?;

        let group: (String, f64) = sqlx::query_as(
            r#"SELECT relation_type, confidence::float8
               FROM market_event_duplicate_groups
               WHERE duplicate_group_id = $1"#,
        )
        .bind(duplicate_group_id)
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
        .bind(duplicate_group_id)
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
    async fn near_duplicate_manual_submission_reaches_live_ingest_path(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor =
            ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

        let first = assert_inserted(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme wins major supply contract in Shenzhen".to_string(),
                        content: Some(
                            "Acme signed a long-term supply contract with Shenzhen transit authority today.".to_string(),
                        ),
                        source_url: Some("https://example.com/contracts/acme-primary".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
                    },
                    dt(2026, 7, 10, 8, 0, 0),
                )
                .await
                .unwrap(),
        );

        let duplicate = assert_existing(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme wins major supply contract in Shenzhen market".to_string(),
                        content: Some(
                            "Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up details differ.".to_string(),
                        ),
                        source_url: Some("https://example.com/contracts/acme-follow-up".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 7, 35, 0)),
                    },
                    dt(2026, 7, 10, 8, 5, 0),
                )
                .await
                .unwrap(),
        );

        assert_eq!(duplicate.existing.evidence_id, first.evidence_id);
        assert_ne!(duplicate.submitted.evidence_id, first.evidence_id);
        assert_ne!(duplicate.submitted.content_hash, first.content_hash);

        let duplicate_group_id: Uuid = sqlx::query_scalar(
            r#"SELECT duplicate_group_id
               FROM market_event_duplicate_members
               WHERE evidence_id = $1"#,
        )
        .bind(duplicate.submitted.evidence_id)
        .fetch_one(&pool)
        .await?;

        let group: (String, f64) = sqlx::query_as(
            r#"SELECT relation_type, confidence::float8
               FROM market_event_duplicate_groups
               WHERE duplicate_group_id = $1"#,
        )
        .bind(duplicate_group_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(group.0, "near");
        assert!(group.1 >= 0.92);
        assert!(group.1 < 1.0);

        let members: Vec<(Uuid, bool)> = sqlx::query_as(
            r#"SELECT evidence_id, is_representative
               FROM market_event_duplicate_members
               WHERE duplicate_group_id = $1
               ORDER BY is_representative DESC, evidence_id ASC"#,
        )
        .bind(duplicate_group_id)
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
    async fn exact_duplicate_manual_submission_detects_matching_content_hash_across_trade_dates(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor =
            ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

        let first = assert_inserted(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme restates quarterly guidance".to_string(),
                        content: Some(
                            "Management reaffirmed the same guidance ranges.".to_string(),
                        ),
                        source_url: Some("https://example.com/acme-guidance-initial".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 6, 30, 0)),
                    },
                    dt(2026, 7, 10, 8, 0, 0),
                )
                .await
                .unwrap(),
        );

        let duplicate = assert_existing(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme restates quarterly guidance".to_string(),
                        content: Some(
                            "Management reaffirmed the same guidance ranges.".to_string(),
                        ),
                        source_url: Some("https://example.com/acme-guidance-later".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
                    },
                    dt(2026, 7, 10, 8, 5, 0),
                )
                .await
                .unwrap(),
        );

        assert_eq!(duplicate.existing.evidence_id, first.evidence_id);
        assert_eq!(
            duplicate.existing.effective_trade_date,
            chrono::NaiveDate::from_ymd_opt(2026, 7, 10).unwrap()
        );
        assert_eq!(
            duplicate.submitted.effective_trade_date,
            chrono::NaiveDate::from_ymd_opt(2026, 7, 13).unwrap()
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn exact_duplicate_manual_submission_detects_matching_canonical_url_across_trade_dates(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor =
            ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

        let existing = EventEvidenceRow {
            evidence_id: Uuid::new_v4(),
            source_id: MANUAL_SOURCE_REST.to_string(),
            source_item_id: "legacy-canonical-url".to_string(),
            source_url: Some("HTTPS://Example.com:443/contracts/acme#primary".to_string()),
            source_tier: "manual".to_string(),
            source_terms_version: "terms-v1".to_string(),
            occurred_at: Some(dt(2026, 7, 10, 6, 30, 0)),
            published_at: Some(dt(2026, 7, 10, 6, 30, 0)),
            first_seen_at: dt(2026, 7, 10, 8, 0, 0),
            available_at: dt(2026, 7, 10, 8, 0, 0),
            effective_trade_date: NaiveDate::from_ymd_opt(2026, 7, 10).unwrap(),
            title: "Archived Acme contract bulletin".to_string(),
            content: Some("Legacy bulletin wording from the first post.".to_string()),
            language: "und".to_string(),
            content_hash: content_hash(
                "Archived Acme contract bulletin",
                Some("Legacy bulletin wording from the first post."),
            ),
            raw_payload: json!({
                "submitted_by": "operator",
                "manual_source_id": MANUAL_SOURCE_REST,
            }),
            version: 1,
            supersedes_evidence_id: None,
            status: "pending".to_string(),
            created_at: dt(2026, 7, 10, 8, 0, 0),
        };
        repo.insert_evidence(&existing).await.unwrap();

        let duplicate = assert_existing(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Mirror of Acme contract bulletin".to_string(),
                        content: Some("Later repost with different body text.".to_string()),
                        source_url: Some("https://example.com/contracts/acme".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
                    },
                    dt(2026, 7, 10, 8, 5, 0),
                )
                .await
                .unwrap(),
        );

        assert_eq!(duplicate.existing.evidence_id, existing.evidence_id);
        assert_eq!(
            duplicate.submitted.effective_trade_date,
            chrono::NaiveDate::from_ymd_opt(2026, 7, 13).unwrap()
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn appending_duplicate_through_ingestion_preserves_older_unlocked_group_members(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor =
            ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

        let first = assert_inserted(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme wins major supply contract in Shenzhen".to_string(),
                        content: Some(
                            "Acme signed a long-term supply contract with Shenzhen transit authority today.".to_string(),
                        ),
                        source_url: Some("https://example.com/contracts/acme-primary".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 6, 30, 0)),
                    },
                    dt(2026, 7, 10, 8, 0, 0),
                )
                .await
                .unwrap(),
        );
        let second = assert_existing(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme wins major supply contract in Shenzhen market".to_string(),
                        content: Some(
                            "Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up details differ.".to_string(),
                        ),
                        source_url: Some("https://example.com/contracts/acme-follow-up".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 6, 35, 0)),
                    },
                    dt(2026, 7, 10, 8, 5, 0),
                )
                .await
                .unwrap(),
        );
        let original_group_id: Uuid = sqlx::query_scalar(
            r#"SELECT duplicate_group_id
               FROM market_event_duplicate_members
               WHERE evidence_id = $1"#,
        )
        .bind(second.submitted.evidence_id)
        .fetch_one(&pool)
        .await?;

        let third = assert_existing(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme wins major supply contract in Shenzhen market".to_string(),
                        content: Some(
                            "Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up details differ.".to_string(),
                        ),
                        source_url: Some("https://example.com/contracts/acme-follow-up-later".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 7, 35, 0)),
                    },
                    dt(2026, 7, 10, 8, 10, 0),
                )
                .await
                .unwrap(),
        );

        let members: Vec<(Uuid, bool)> = sqlx::query_as(
            r#"SELECT evidence_id, is_representative
               FROM market_event_duplicate_members
               WHERE duplicate_group_id = $1
               ORDER BY is_representative DESC, evidence_id ASC"#,
        )
        .bind(original_group_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(members.len(), 3);
        assert!(members.contains(&(first.evidence_id, true)));
        assert!(members
            .iter()
            .any(|member| member.0 == second.submitted.evidence_id && !member.1));
        assert!(members
            .iter()
            .any(|member| member.0 == third.submitted.evidence_id && !member.1));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn live_manual_ingestion_uses_configured_threshold_for_review_required_duplicates(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let ingestor = ManualEvidenceIngestor::with_auto_near_duplicate_threshold(
            repo.clone(),
            Arc::new(AShareTradingDateResolver),
            0.90,
        );

        let first = assert_inserted(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme wins major supply contract in Shenzhen".to_string(),
                        content: Some(
                            "Acme signed a long-term supply contract with Shenzhen transit authority today.".to_string(),
                        ),
                        source_url: Some("https://example.com/contracts/acme-primary".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
                    },
                    dt(2026, 7, 10, 8, 0, 0),
                )
                .await
                .unwrap(),
        );

        let duplicate = assert_existing(
            ingestor
                .submit_at(
                    ManualSource::Rest,
                    ManualEventInput {
                        title: "Acme wins major supply contract in Shenzhen market".to_string(),
                        content: Some(
                            "Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up details differ.".to_string(),
                        ),
                        source_url: Some("https://example.com/contracts/acme-follow-up".to_string()),
                        submitted_by: "operator".to_string(),
                        published_at: Some(dt(2026, 7, 10, 7, 35, 0)),
                    },
                    dt(2026, 7, 10, 8, 5, 0),
                )
                .await
                .unwrap(),
        );

        assert_eq!(duplicate.existing.evidence_id, first.evidence_id);

        let stored_group: (String, f64) = sqlx::query_as(
            r#"SELECT relation_type, confidence::float8
               FROM market_event_duplicate_groups g
               INNER JOIN market_event_duplicate_members m
                   ON m.duplicate_group_id = g.duplicate_group_id
               WHERE m.evidence_id = $1"#,
        )
        .bind(duplicate.submitted.evidence_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored_group.0, "review_required");
        assert!(stored_group.1 >= 0.90);
        assert!(stored_group.1 < 1.0);

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
        let expected_hash = content_hash(&input.title, input.content.as_deref());
        let ingestor = ingestor.clone_with_duplicate_lookup_barrier_for_test(expected_hash, 2);

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
    async fn concurrent_different_hash_near_duplicates_do_not_both_return_inserted(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone())
            .clone_with_manual_insert_sleep_after_candidate_discovery_for_test(
                Duration::from_millis(200),
            );
        let ingestor = ManualEvidenceIngestor::new(repo, Arc::new(AShareTradingDateResolver));

        let left_input = ManualEventInput {
            title: "Acme wins major supply contract in Shenzhen".to_string(),
            content: Some(
                "Acme signed a long-term supply contract with Shenzhen transit authority today."
                    .to_string(),
            ),
            source_url: Some("https://example.com/contracts/acme-primary".to_string()),
            submitted_by: "operator".to_string(),
            published_at: Some(dt(2026, 7, 10, 6, 30, 0)),
        };
        let right_input = ManualEventInput {
            title: "Acme wins major supply contract in Shenzhen market".to_string(),
            content: Some(
                "Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up details differ.".to_string(),
            ),
            source_url: Some("https://example.com/contracts/acme-follow-up".to_string()),
            submitted_by: "operator".to_string(),
            published_at: Some(dt(2026, 7, 10, 6, 35, 0)),
        };

        let first = tokio::spawn({
            let ingestor = ingestor.clone();
            async move {
                ingestor
                    .submit_at(ManualSource::Rest, left_input, dt(2026, 7, 10, 8, 0, 0))
                    .await
            }
        });
        yield_now().await;
        let second = tokio::spawn(async move {
            ingestor
                .submit_at(ManualSource::Rest, right_input, dt(2026, 7, 10, 8, 0, 1))
                .await
        });

        let outcomes = [
            first.await.unwrap().unwrap(),
            second.await.unwrap().unwrap(),
        ];

        let inserted_count = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ManualEventSubmissionOutcome::Inserted(_)))
            .count();
        let existing_count = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ManualEventSubmissionOutcome::Existing(_)))
            .count();

        assert_eq!(inserted_count, 1);
        assert_eq!(existing_count, 1);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn duplicate_lookup_barrier_does_not_accept_same_hash_from_unrelated_ingestor(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let resolver = Arc::new(AShareTradingDateResolver);
        let base_ingestor = ManualEvidenceIngestor::new(repo.clone(), resolver.clone());
        let unrelated_ingestor = base_ingestor.clone();
        let input = ManualEventInput {
            title: " ACME   wins   contract ".to_string(),
            content: Some("Order value\n exceeds guidance".to_string()),
            source_url: Some("https://example.com/contracts/acme".to_string()),
            submitted_by: "operator".to_string(),
            published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
        };
        let expected_hash = content_hash(&input.title, input.content.as_deref());
        let barrier_ingestor =
            base_ingestor.clone_with_duplicate_lookup_barrier_for_test(expected_hash, 2);

        let mut first_barrier_worker = tokio::spawn({
            let barrier_ingestor = barrier_ingestor.clone();
            let input = input.clone();
            async move {
                barrier_ingestor
                    .submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 0, 0))
                    .await
            }
        });
        yield_now().await;

        let unrelated = tokio::spawn(async move {
            unrelated_ingestor
                .submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 0, 1))
                .await
        });

        if let Ok(first) =
            tokio::time::timeout(Duration::from_millis(200), &mut first_barrier_worker).await
        {
            first.unwrap().unwrap();
            unrelated.await.unwrap().unwrap();
            panic!("unrelated ingestor with the same content hash consumed the barrier");
        }

        let second_barrier_worker = barrier_ingestor.submit_at(
            ManualSource::Rest,
            ManualEventInput {
                title: " ACME   wins   contract ".to_string(),
                content: Some("Order value\n exceeds guidance".to_string()),
                source_url: Some("https://example.com/contracts/acme".to_string()),
                submitted_by: "operator".to_string(),
                published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
            },
            dt(2026, 7, 10, 8, 0, 2),
        );

        let (first, second, unrelated) =
            tokio::join!(first_barrier_worker, second_barrier_worker, unrelated);
        first.unwrap().unwrap();
        second.unwrap();
        unrelated.unwrap().unwrap();

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
