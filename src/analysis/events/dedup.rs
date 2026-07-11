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

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DuplicateResolution {
    Exact(DuplicateMatch),
    NearDuplicate(DuplicateMatch),
    Independent,
    ReviewRequired(DuplicateMatch),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DuplicateMatch {
    pub(crate) representative_id: Uuid,
    pub(crate) confidence: f64,
    pub(crate) candidate_ids: Vec<Uuid>,
}

impl DuplicateResolution {
    pub(crate) fn public_decision(&self) -> DuplicateDecision {
        match self {
            Self::Exact(duplicate_match) => DuplicateDecision::Exact {
                representative_id: duplicate_match.representative_id,
            },
            Self::NearDuplicate(duplicate_match) => DuplicateDecision::NearDuplicate {
                representative_id: duplicate_match.representative_id,
                confidence: duplicate_match.confidence,
            },
            Self::Independent => DuplicateDecision::Independent,
            Self::ReviewRequired(duplicate_match) => DuplicateDecision::ReviewRequired {
                candidate_ids: duplicate_match.candidate_ids.clone(),
            },
        }
    }

    pub(crate) fn representative_id(&self) -> Option<Uuid> {
        match self {
            Self::Exact(duplicate_match)
            | Self::NearDuplicate(duplicate_match)
            | Self::ReviewRequired(duplicate_match) => Some(duplicate_match.representative_id),
            Self::Independent => None,
        }
    }

    pub(crate) fn confidence(&self) -> Option<f64> {
        match self {
            Self::Exact(_) => Some(1.0),
            Self::NearDuplicate(duplicate_match) | Self::ReviewRequired(duplicate_match) => {
                Some(duplicate_match.confidence)
            }
            Self::Independent => None,
        }
    }

    pub(crate) fn relation_type(&self) -> Option<&'static str> {
        match self {
            Self::Exact(_) => Some("exact"),
            Self::NearDuplicate(_) => Some("near"),
            Self::ReviewRequired(_) => Some("review_required"),
            Self::Independent => None,
        }
    }

    pub(crate) fn candidate_ids(&self) -> &[Uuid] {
        match self {
            Self::Exact(duplicate_match)
            | Self::NearDuplicate(duplicate_match)
            | Self::ReviewRequired(duplicate_match) => duplicate_match.candidate_ids.as_slice(),
            Self::Independent => &[],
        }
    }
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
    pub(crate) evidence_id: Uuid,
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
        self.classify(subject, candidates).public_decision()
    }

    pub(crate) fn classify(
        &self,
        subject: &DuplicateSubject,
        candidates: &[DuplicateCandidate],
    ) -> DuplicateResolution {
        let exact_matches: Vec<&DuplicateCandidate> = candidates
            .iter()
            .filter(|candidate| is_exact_match(subject, candidate))
            .collect();
        if let Some(representative_id) = exact_matches
            .first()
            .map(|candidate| candidate.representative_id)
        {
            return DuplicateResolution::Exact(DuplicateMatch {
                representative_id,
                confidence: 1.0,
                candidate_ids: exact_matches
                    .into_iter()
                    .map(|candidate| candidate.evidence_id)
                    .collect(),
            });
        }

        let scored_candidates: Vec<ScoredCandidate> = candidates
            .iter()
            .map(|candidate| ScoredCandidate {
                evidence_id: candidate.evidence_id,
                representative_id: candidate.representative_id,
                confidence: similarity_score(subject, candidate),
            })
            .collect();

        if self.auto_near_duplicate_threshold >= CONSERVATIVE_NEAR_DUPLICATE_FLOOR {
            if let Some(duplicate_match) =
                matching_scored_candidates(&scored_candidates, self.auto_near_duplicate_threshold)
            {
                return DuplicateResolution::NearDuplicate(duplicate_match);
            }
        } else {
            if let Some(duplicate_match) =
                matching_scored_candidates(&scored_candidates, self.auto_near_duplicate_threshold)
            {
                return DuplicateResolution::ReviewRequired(duplicate_match);
            }
        }

        DuplicateResolution::Independent
    }
}

#[derive(Debug, Clone, Copy)]
struct ScoredCandidate {
    evidence_id: Uuid,
    representative_id: Uuid,
    confidence: f64,
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

fn matching_scored_candidates(
    scored_candidates: &[ScoredCandidate],
    minimum_score: f64,
) -> Option<DuplicateMatch> {
    let mut matching_candidates: Vec<ScoredCandidate> = scored_candidates
        .iter()
        .copied()
        .filter(|candidate| candidate.confidence >= minimum_score)
        .collect();
    matching_candidates.sort_by(|left, right| {
        right
            .confidence
            .total_cmp(&left.confidence)
            .then_with(|| left.evidence_id.cmp(&right.evidence_id))
    });

    matching_candidates.first().map(|candidate| DuplicateMatch {
        representative_id: candidate.representative_id,
        confidence: candidate.confidence,
        candidate_ids: matching_candidates
            .iter()
            .map(|candidate| candidate.evidence_id)
            .collect(),
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
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::{DuplicateCandidate, DuplicateDecider, DuplicateDecision, DuplicateSubject};
    use crate::analysis::events::evidence::{content_hash, normalize_text};
    use crate::analysis::events::{
        AShareTradingDateResolver, ManualEventInput, ManualEventSubmissionOutcome,
    };
    use crate::storage::event_repository::{
        DuplicateGroupMemberRow, DuplicateGroupRow, EventRepository,
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
            evidence_id: Uuid::new_v4(),
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
            evidence_id: Uuid::new_v4(),
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
                candidate_ids: vec![review.evidence_id],
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
        let ingestor = crate::analysis::events::evidence::ManualEvidenceIngestor::new(
            repo.clone(),
            Arc::new(AShareTradingDateResolver),
        );
        let original_member = assert_inserted(
            ingestor
                .submit_at(
                    crate::analysis::events::evidence::ManualSource::Rest,
                    shared_content_input(
                        "https://example.com/source-independent-original",
                        "Shared duplicate title",
                        "Shared duplicate body",
                    ),
                    dt(2026, 7, 10, 8, 0, 0),
                )
                .await
                .unwrap(),
        );
        let duplicate_member = assert_existing(
            ingestor
                .submit_at(
                    crate::analysis::events::evidence::ManualSource::Rest,
                    shared_content_input(
                        "https://example.com/source-independent-duplicate",
                        "Shared duplicate title",
                        "Shared duplicate body",
                    ),
                    dt(2026, 7, 10, 8, 0, 1),
                )
                .await
                .unwrap(),
        );
        let reprocessed_member = shared_content_input(
            "https://example.com/source-independent-reprocessed",
            "Shared duplicate title",
            "Shared duplicate body",
        );
        let group_id: Uuid = sqlx::query_scalar(
            r#"SELECT duplicate_group_id
               FROM market_event_duplicate_members
               WHERE evidence_id = $1"#,
        )
        .bind(duplicate_member.submitted.evidence_id)
        .fetch_one(&pool)
        .await?;
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

        let reprocessed = ingestor
            .submit_at(
                crate::analysis::events::evidence::ManualSource::Rest,
                reprocessed_member,
                dt(2026, 7, 10, 8, 0, 2),
            )
            .await
            .unwrap();
        let reprocessed = assert_existing(reprocessed);
        assert_eq!(
            reprocessed.existing.evidence_id,
            original_member.evidence_id
        );

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
        let ingestor = crate::analysis::events::evidence::ManualEvidenceIngestor::new(
            repo.clone(),
            Arc::new(AShareTradingDateResolver),
        );
        let original_member = assert_inserted(
            ingestor
                .submit_at(
                    crate::analysis::events::evidence::ManualSource::Rest,
                    shared_content_input(
                        "https://example.com/source-duplicate-original",
                        "Shared duplicate title",
                        "Shared duplicate body",
                    ),
                    dt(2026, 7, 10, 8, 0, 0),
                )
                .await
                .unwrap(),
        );
        let duplicate_member = assert_existing(
            ingestor
                .submit_at(
                    crate::analysis::events::evidence::ManualSource::Rest,
                    shared_content_input(
                        "https://example.com/source-duplicate-existing",
                        "Shared duplicate title",
                        "Shared duplicate body",
                    ),
                    dt(2026, 7, 10, 8, 0, 1),
                )
                .await
                .unwrap(),
        );
        let reprocessed_member = shared_content_input(
            "https://example.com/source-duplicate-reprocessed",
            "Shared duplicate title",
            "Shared duplicate body",
        );
        let group_id: Uuid = sqlx::query_scalar(
            r#"SELECT duplicate_group_id
               FROM market_event_duplicate_members
               WHERE evidence_id = $1"#,
        )
        .bind(duplicate_member.submitted.evidence_id)
        .fetch_one(&pool)
        .await?;
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
                    evidence_id: duplicate_member.submitted.evidence_id,
                    is_representative: false,
                },
            ],
            created_at: dt(2026, 7, 10, 12, 0, 0),
        };
        repo.save_duplicate_group(&locked).await.unwrap();

        let reprocessed = ingestor
            .submit_at(
                crate::analysis::events::evidence::ManualSource::Rest,
                reprocessed_member,
                dt(2026, 7, 10, 8, 0, 2),
            )
            .await
            .unwrap();
        let reprocessed = assert_existing(reprocessed);
        assert_eq!(
            reprocessed.existing.evidence_id,
            original_member.evidence_id
        );

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
        assert!(members.contains(&(duplicate_member.submitted.evidence_id, false)));

        Ok(())
    }

    #[test]
    fn duplicate_decision_public_shape_matches_task_brief() {
        let exact = DuplicateDecision::Exact {
            representative_id: Uuid::nil(),
        };
        let near = DuplicateDecision::NearDuplicate {
            representative_id: Uuid::nil(),
            confidence: 0.92,
        };
        let review = DuplicateDecision::ReviewRequired {
            candidate_ids: vec![Uuid::nil()],
        };

        match exact {
            DuplicateDecision::Exact { representative_id } => {
                let _: Uuid = representative_id;
            }
            _ => panic!("expected exact variant"),
        }

        match near {
            DuplicateDecision::NearDuplicate {
                representative_id,
                confidence,
            } => {
                let _: Uuid = representative_id;
                let _: f64 = confidence;
            }
            _ => panic!("expected near-duplicate variant"),
        }

        assert!(matches!(
            DuplicateDecision::Independent,
            DuplicateDecision::Independent
        ));

        match review {
            DuplicateDecision::ReviewRequired { candidate_ids } => {
                let _: Vec<Uuid> = candidate_ids;
            }
            _ => panic!("expected review-required variant"),
        }
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
            evidence_id: Uuid::new_v4(),
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

    fn shared_content_input(source_url: &str, title: &str, content: &str) -> ManualEventInput {
        ManualEventInput {
            title: title.to_string(),
            content: Some(content.to_string()),
            source_url: Some(source_url.to_string()),
            submitted_by: "operator".to_string(),
            published_at: None,
        }
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

    fn assert_existing(
        outcome: ManualEventSubmissionOutcome,
    ) -> crate::analysis::events::ExistingEventEvidenceRelation {
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
}
