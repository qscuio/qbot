use std::collections::BTreeSet;

use uuid::Uuid;

use super::evidence::{canonicalize_source_url, normalize_text};

const CONSERVATIVE_NEAR_DUPLICATE_FLOOR: f64 = 0.92;
const CONTENT_PREFIX_LIMIT: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub enum DuplicateDecision {
    Exact {
        representative_id: Uuid,
    },
    NearDuplicate {
        representative_id: Uuid,
        confidence: f64,
    },
    Independent,
    ReviewRequired {
        candidate_ids: Vec<Uuid>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct DuplicateSubject {
    pub(crate) source_id: String,
    pub(crate) source_item_id: String,
    pub(crate) version: i32,
    pub(crate) source_url: Option<String>,
    pub(crate) title: String,
    pub(crate) content: Option<String>,
    pub(crate) content_hash: String,
}

#[derive(Debug, Clone)]
pub(crate) struct DuplicateCandidate {
    pub(crate) representative_id: Uuid,
    pub(crate) source_id: String,
    pub(crate) source_item_id: String,
    pub(crate) version: i32,
    pub(crate) source_url: Option<String>,
    pub(crate) title: String,
    pub(crate) content: Option<String>,
    pub(crate) content_hash: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DuplicateDecider {
    auto_near_duplicate_threshold: f64,
}

impl DuplicateDecider {
    pub(crate) fn new(auto_near_duplicate_threshold: f64) -> Self {
        Self {
            auto_near_duplicate_threshold,
        }
    }

    pub(crate) fn decide(
        &self,
        subject: &DuplicateSubject,
        candidates: &[DuplicateCandidate],
    ) -> DuplicateDecision {
        if let Some(exact) = candidates
            .iter()
            .find(|candidate| is_exact_match(subject, candidate))
        {
            return DuplicateDecision::Exact {
                representative_id: exact.representative_id,
            };
        }

        let scored_candidates: Vec<(Uuid, f64)> = candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.representative_id,
                    similarity_score(subject, candidate),
                )
            })
            .collect();

        if self.auto_near_duplicate_threshold >= CONSERVATIVE_NEAR_DUPLICATE_FLOOR {
            if let Some((representative_id, confidence)) =
                best_scored_candidate(&scored_candidates, self.auto_near_duplicate_threshold)
            {
                return DuplicateDecision::NearDuplicate {
                    representative_id,
                    confidence,
                };
            }
        } else {
            let candidate_ids: Vec<Uuid> = scored_candidates
                .iter()
                .copied()
                .filter(|(_, score)| *score >= self.auto_near_duplicate_threshold)
                .map(|(representative_id, _)| representative_id)
                .collect();
            if !candidate_ids.is_empty() {
                return DuplicateDecision::ReviewRequired { candidate_ids };
            }
        }

        DuplicateDecision::Independent
    }
}

fn is_exact_match(subject: &DuplicateSubject, candidate: &DuplicateCandidate) -> bool {
    same_source_item_and_version(subject, candidate)
        || same_canonical_url(
            subject.source_url.as_deref(),
            candidate.source_url.as_deref(),
        )
        || subject.content_hash == candidate.content_hash
}

fn same_source_item_and_version(
    subject: &DuplicateSubject,
    candidate: &DuplicateCandidate,
) -> bool {
    subject.source_id == candidate.source_id
        && subject.source_item_id == candidate.source_item_id
        && subject.version == candidate.version
}

fn same_canonical_url(subject_url: Option<&str>, candidate_url: Option<&str>) -> bool {
    let (Some(subject_url), Some(candidate_url)) = (subject_url, candidate_url) else {
        return false;
    };

    let (Ok(subject_url), Ok(candidate_url)) = (
        canonicalize_source_url(subject_url),
        canonicalize_source_url(candidate_url),
    ) else {
        return false;
    };

    subject_url == candidate_url
}

fn best_scored_candidate(
    scored_candidates: &[(Uuid, f64)],
    minimum_score: f64,
) -> Option<(Uuid, f64)> {
    scored_candidates
        .iter()
        .copied()
        .filter(|(_, score)| *score >= minimum_score)
        .max_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        })
}

fn similarity_score(subject: &DuplicateSubject, candidate: &DuplicateCandidate) -> f64 {
    let title_score = title_token_jaccard(&subject.title, &candidate.title);
    let content_score = match (&subject.content, &candidate.content) {
        (Some(subject_content), Some(candidate_content)) => Some(content_prefix_similarity(
            subject_content,
            candidate_content,
        )),
        _ => None,
    };

    if let Some(content_score) = content_score {
        (title_score + content_score) / 2.0
    } else {
        title_score
    }
}

fn title_token_jaccard(left: &str, right: &str) -> f64 {
    let left_tokens = normalized_token_set(left);
    let right_tokens = normalized_token_set(right);

    if left_tokens.is_empty() && right_tokens.is_empty() {
        return 1.0;
    }

    let intersection = left_tokens.intersection(&right_tokens).count();
    let union = left_tokens.union(&right_tokens).count();

    intersection as f64 / union as f64
}

fn normalized_token_set(value: &str) -> BTreeSet<String> {
    normalize_text(value)
        .to_lowercase()
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

fn content_prefix_similarity(left: &str, right: &str) -> f64 {
    let left_prefix = normalized_prefix(left);
    let right_prefix = normalized_prefix(right);

    if left_prefix.is_empty() || right_prefix.is_empty() {
        return 0.0;
    }

    let common_prefix = left_prefix
        .chars()
        .zip(right_prefix.chars())
        .take_while(|(left_char, right_char)| left_char == right_char)
        .count();
    let shorter_length = left_prefix
        .chars()
        .count()
        .min(right_prefix.chars().count());

    common_prefix as f64 / shorter_length as f64
}

fn normalized_prefix(value: &str) -> String {
    normalize_text(value)
        .to_lowercase()
        .chars()
        .take(CONTENT_PREFIX_LIMIT)
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::{DuplicateCandidate, DuplicateDecider, DuplicateDecision, DuplicateSubject};
    use crate::analysis::events::evidence::{content_hash, normalize_text};
    use crate::storage::event_repository::{
        DuplicateGroupMemberRow, DuplicateGroupRow, EventEvidenceRow, EventRepository,
    };

    #[test]
    fn returns_exact_for_matching_source_item_and_version() {
        let subject = subject(
            "wire:cn",
            "story-123",
            2,
            Some("https://example.com/story-123"),
            "Acme wins supply contract",
            Some("Acme disclosed the contract award."),
        );
        let candidate = candidate(
            "other-source",
            "story-999",
            1,
            Some("https://example.com/other"),
            "Different title",
            Some("Different body"),
        );
        let exact = DuplicateCandidate {
            representative_id: Uuid::new_v4(),
            source_id: subject.source_id.clone(),
            source_item_id: subject.source_item_id.clone(),
            version: subject.version,
            source_url: candidate.source_url.clone(),
            title: candidate.title.clone(),
            content: candidate.content.clone(),
            content_hash: candidate.content_hash.clone(),
        };

        let decision = DuplicateDecider::new(0.92).decide(&subject, &[candidate, exact.clone()]);

        assert_eq!(
            decision,
            DuplicateDecision::Exact {
                representative_id: exact.representative_id,
            }
        );
    }

    #[test]
    fn returns_exact_for_matching_canonical_url() {
        let subject = subject(
            "wire:cn",
            "story-123",
            1,
            Some("HTTPS://Example.com:443/news/flash?a=1#ignored"),
            "Acme wins supply contract",
            Some("Acme disclosed the contract award."),
        );
        let exact = candidate(
            "other-source",
            "story-999",
            1,
            Some("https://example.com/news/flash?a=1"),
            "Different title",
            Some("Different body"),
        );

        let decision = DuplicateDecider::new(0.92).decide(&subject, &[exact.clone()]);

        assert_eq!(
            decision,
            DuplicateDecision::Exact {
                representative_id: exact.representative_id,
            }
        );
    }

    #[test]
    fn does_not_treat_matching_invalid_url_text_as_exact() {
        let subject = subject(
            "wire:cn",
            "story-123",
            1,
            Some("NOT A VALID URL"),
            "Acme wins supply contract",
            Some("Acme disclosed the contract award."),
        );
        let candidate = candidate(
            "other-source",
            "story-999",
            1,
            Some(" not a valid url "),
            "Beta cuts factory shifts",
            Some("Management cited export weakness in a separate filing."),
        );

        let decision = DuplicateDecider::new(0.92).decide(&subject, &[candidate]);

        assert_eq!(decision, DuplicateDecision::Independent);
    }

    #[test]
    fn returns_exact_for_matching_content_hash() {
        let subject = subject(
            "wire:cn",
            "story-123",
            1,
            Some("https://example.com/story-123"),
            "Acme wins supply contract",
            Some("Acme disclosed the contract award."),
        );
        let exact = DuplicateCandidate {
            representative_id: Uuid::new_v4(),
            source_id: "other-source".to_string(),
            source_item_id: "story-999".to_string(),
            version: 7,
            source_url: Some("https://example.com/other".to_string()),
            title: "Different title".to_string(),
            content: Some("Different body".to_string()),
            content_hash: subject.content_hash.clone(),
        };

        let decision = DuplicateDecider::new(0.92).decide(&subject, &[exact.clone()]);

        assert_eq!(
            decision,
            DuplicateDecision::Exact {
                representative_id: exact.representative_id,
            }
        );
    }

    #[test]
    fn returns_near_duplicate_when_similarity_meets_conservative_threshold() {
        let subject = subject(
            "wire:cn",
            "story-123",
            1,
            None,
            "Acme wins major supply contract in Shenzhen",
            Some("Acme signed a long-term supply contract with Shenzhen transit authority today."),
        );
        let near = candidate(
            "other-source",
            "story-999",
            1,
            None,
            "Acme wins major supply contract in Shenzhen market",
            Some("Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up details differ."),
        );

        let decision = DuplicateDecider::new(0.92).decide(&subject, &[near.clone()]);

        match decision {
            DuplicateDecision::NearDuplicate {
                representative_id,
                confidence,
            } => {
                assert_eq!(representative_id, near.representative_id);
                assert!(
                    confidence >= 0.92,
                    "expected conservative confidence, got {confidence}"
                );
            }
            other => panic!("expected near duplicate, got {other:?}"),
        }
    }

    #[test]
    fn returns_review_required_when_lower_threshold_would_otherwise_auto_match_above_floor() {
        let subject = subject(
            "wire:cn",
            "story-123",
            1,
            None,
            "Acme wins major supply contract in Shenzhen",
            Some("Acme signed a long-term supply contract with Shenzhen transit authority today."),
        );
        let review = candidate(
            "other-source",
            "story-999",
            1,
            None,
            "Acme wins major supply contract in Shenzhen market",
            Some("Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up details differ."),
        );

        let decision = DuplicateDecider::new(0.90).decide(&subject, &[review.clone()]);

        match DuplicateDecider::new(0.92).decide(&subject, &[review.clone()]) {
            DuplicateDecision::NearDuplicate { confidence, .. } => {
                assert!(
                    confidence >= 0.92,
                    "expected similarity above the conservative floor, got {confidence}"
                );
            }
            other => panic!("expected near duplicate at conservative floor, got {other:?}"),
        }

        assert_eq!(
            decision,
            DuplicateDecision::ReviewRequired {
                candidate_ids: vec![review.representative_id],
            }
        );
    }

    #[test]
    fn returns_independent_when_similarity_stays_below_thresholds() {
        let subject = subject(
            "wire:cn",
            "story-123",
            1,
            None,
            "Acme wins major supply contract in Shenzhen",
            Some("Acme signed a long-term supply contract with Shenzhen transit authority today."),
        );
        let independent = candidate(
            "other-source",
            "story-999",
            1,
            None,
            "Beta cuts factory shifts after demand slowdown",
            Some("Management cited export weakness and inventory pressure in a separate filing."),
        );

        let decision = DuplicateDecider::new(0.92).decide(&subject, &[independent]);

        assert_eq!(decision, DuplicateDecision::Independent);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn locked_independent_relation_is_not_overwritten_by_reprocessing(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let original_member = evidence_with_shared_content(
            "source-independent-original",
            1,
            "Shared duplicate title",
            "Shared duplicate body",
        );
        let duplicate_member = evidence_with_shared_content(
            "source-independent-duplicate",
            1,
            "Shared duplicate title",
            "Shared duplicate body",
        );
        let reprocessed_member = evidence_with_shared_content(
            "source-independent-reprocessed",
            1,
            "Shared duplicate title",
            "Shared duplicate body",
        );
        repo.insert_evidence(&original_member).await.unwrap();
        let inserted = repo
            .insert_manual_evidence(&duplicate_member)
            .await
            .unwrap();
        let group_id = inserted
            .duplicate_group_id
            .expect("duplicate insert should create the deterministic duplicate group");
        let locked = DuplicateGroupRow {
            duplicate_group_id: group_id,
            relation_type: "independent".to_string(),
            confidence: 1.0,
            locked_by_user: true,
            members: vec![DuplicateGroupMemberRow {
                evidence_id: original_member.evidence_id,
                is_representative: true,
            }],
            created_at: dt(2026, 7, 10, 12, 0, 0),
        };
        repo.save_duplicate_group(&locked).await.unwrap();

        let reprocessed = repo
            .insert_manual_evidence(&reprocessed_member)
            .await
            .unwrap();
        assert_eq!(reprocessed.duplicate_group_id, Some(group_id));

        let stored: (bool, String, f64) = sqlx::query_as(
            r#"SELECT locked_by_user, relation_type, confidence::float8
               FROM market_event_duplicate_groups
               WHERE duplicate_group_id = $1"#,
        )
        .bind(group_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored, (true, "independent".to_string(), 1.0));

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
    async fn locked_duplicate_relation_is_not_overwritten_by_reprocessing(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let repo = EventRepository::new(pool.clone());
        let original_member = evidence_with_shared_content(
            "source-duplicate-original",
            1,
            "Shared duplicate title",
            "Shared duplicate body",
        );
        let duplicate_member = evidence_with_shared_content(
            "source-duplicate-existing",
            1,
            "Shared duplicate title",
            "Shared duplicate body",
        );
        let reprocessed_member = evidence_with_shared_content(
            "source-duplicate-reprocessed",
            1,
            "Shared duplicate title",
            "Shared duplicate body",
        );
        repo.insert_evidence(&original_member).await.unwrap();
        let inserted = repo
            .insert_manual_evidence(&duplicate_member)
            .await
            .unwrap();
        let group_id = inserted
            .duplicate_group_id
            .expect("duplicate insert should create the deterministic duplicate group");
        let locked = DuplicateGroupRow {
            duplicate_group_id: group_id,
            relation_type: "exact".to_string(),
            confidence: 1.0,
            locked_by_user: true,
            members: vec![
                DuplicateGroupMemberRow {
                    evidence_id: original_member.evidence_id,
                    is_representative: true,
                },
                DuplicateGroupMemberRow {
                    evidence_id: duplicate_member.evidence_id,
                    is_representative: false,
                },
            ],
            created_at: dt(2026, 7, 10, 12, 0, 0),
        };
        repo.save_duplicate_group(&locked).await.unwrap();

        let reprocessed = repo
            .insert_manual_evidence(&reprocessed_member)
            .await
            .unwrap();
        assert_eq!(reprocessed.duplicate_group_id, Some(group_id));

        let stored: (bool, String, f64) = sqlx::query_as(
            r#"SELECT locked_by_user, relation_type, confidence::float8
               FROM market_event_duplicate_groups
               WHERE duplicate_group_id = $1"#,
        )
        .bind(group_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored, (true, "exact".to_string(), 1.0));

        let members: Vec<(Uuid, bool)> = sqlx::query_as(
            r#"SELECT evidence_id, is_representative
               FROM market_event_duplicate_members
               WHERE duplicate_group_id = $1
               ORDER BY evidence_id ASC"#,
        )
        .bind(group_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(members.len(), 2);
        assert!(members.contains(&(original_member.evidence_id, true)));
        assert!(members.contains(&(duplicate_member.evidence_id, false)));

        Ok(())
    }

    fn subject(
        source_id: &str,
        source_item_id: &str,
        version: i32,
        source_url: Option<&str>,
        title: &str,
        content: Option<&str>,
    ) -> DuplicateSubject {
        DuplicateSubject {
            source_id: source_id.to_string(),
            source_item_id: source_item_id.to_string(),
            version,
            source_url: source_url.map(str::to_string),
            title: normalize_text(title),
            content: content.map(normalize_text),
            content_hash: content_hash(title, content),
        }
    }

    fn candidate(
        source_id: &str,
        source_item_id: &str,
        version: i32,
        source_url: Option<&str>,
        title: &str,
        content: Option<&str>,
    ) -> DuplicateCandidate {
        DuplicateCandidate {
            representative_id: Uuid::new_v4(),
            source_id: source_id.to_string(),
            source_item_id: source_item_id.to_string(),
            version,
            source_url: source_url.map(str::to_string),
            title: normalize_text(title),
            content: content.map(normalize_text),
            content_hash: content_hash(title, content),
        }
    }

    fn evidence_with_shared_content(
        source_item_id: &str,
        version: i32,
        title: &str,
        content: &str,
    ) -> EventEvidenceRow {
        let content = Some(content.to_string());
        EventEvidenceRow {
            evidence_id: Uuid::new_v4(),
            source_id: "manual:rest".to_string(),
            source_item_id: source_item_id.to_string(),
            source_url: Some(format!("https://example.com/{source_item_id}")),
            source_tier: "manual".to_string(),
            source_terms_version: "terms-v1".to_string(),
            occurred_at: None,
            published_at: None,
            first_seen_at: dt(2026, 7, 10, 8, 0, 0),
            available_at: dt(2026, 7, 10, 8, 0, 0),
            effective_trade_date: dt(2026, 7, 10, 8, 0, 0).date_naive(),
            title: title.to_string(),
            content: content.clone(),
            language: "und".to_string(),
            content_hash: content_hash(&title, content.as_deref()),
            raw_payload: serde_json::json!({ "source_item_id": source_item_id }),
            version,
            supersedes_evidence_id: None,
            status: "publishable".to_string(),
            created_at: dt(2026, 7, 10, 8, 0, 0),
        }
    }

    fn dt(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .unwrap()
    }
}
