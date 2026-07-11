use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::task::yield_now;
use uuid::Uuid;

use super::{content_hash, ManualEvidenceIngestor, ManualSource, MANUAL_SOURCE_REST};
use crate::analysis::events::{
    AShareTradingDateResolver, ExistingEventEvidenceRelation, ManualEventInput,
    ManualEventSubmissionOutcome,
};
use crate::storage::event_repository::{
    DuplicateGroupMemberRow, DuplicateGroupRow, EventEvidenceRow, EventRepository,
};

#[sqlx::test(migrations = "./migrations")]
async fn repeated_manual_submission_returns_existing_evidence_relation(
    pool: PgPool,
) -> sqlx::Result<()> {
    let repo = EventRepository::new(pool.clone());
    let ingestor = ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));
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
    let ingestor = ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

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
    let ingestor = ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

    let first = assert_inserted(
        ingestor
            .submit_at(
                ManualSource::Rest,
                ManualEventInput {
                    title: "Acme restates quarterly guidance".to_string(),
                    content: Some("Management reaffirmed the same guidance ranges.".to_string()),
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
                    content: Some("Management reaffirmed the same guidance ranges.".to_string()),
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
        NaiveDate::from_ymd_opt(2026, 7, 10).unwrap()
    );
    assert_eq!(
        duplicate.submitted.effective_trade_date,
        NaiveDate::from_ymd_opt(2026, 7, 13).unwrap()
    );

    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn exact_duplicate_manual_submission_detects_matching_canonical_url_across_trade_dates(
    pool: PgPool,
) -> sqlx::Result<()> {
    let repo = EventRepository::new(pool.clone());
    let ingestor = ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

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
    insert_raw_evidence_row(&pool, &existing).await?;

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
        NaiveDate::from_ymd_opt(2026, 7, 13).unwrap()
    );

    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn appending_duplicate_through_ingestion_preserves_older_unlocked_group_members(
    pool: PgPool,
) -> sqlx::Result<()> {
    let repo = EventRepository::new(pool.clone());
    let ingestor = ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

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
async fn matching_multiple_existing_duplicate_groups_persists_one_auditable_review_group_without_overlap(
    pool: PgPool,
) -> sqlx::Result<()> {
    let repo = EventRepository::new(pool.clone());
    let ingestor = ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));
    let matching_title = "Acme restates quarterly guidance";
    let matching_content = Some("Management reaffirmed the same guidance ranges.");

    let first_group_representative = seeded_manual_evidence_row(
        "split-group-a-representative",
        "https://example.com/acme-guidance-a",
        matching_title,
        matching_content,
        dt(2026, 7, 10, 7, 0, 0),
        dt(2026, 7, 10, 7, 0, 0),
    );
    let first_group_member = seeded_manual_evidence_row(
        "split-group-a-member",
        "https://example.com/acme-guidance-a-member",
        matching_title,
        matching_content,
        dt(2026, 7, 10, 7, 1, 0),
        dt(2026, 7, 10, 7, 1, 0),
    );
    let second_group_representative = seeded_manual_evidence_row(
        "split-group-b-representative",
        "https://example.com/acme-guidance-b",
        matching_title,
        matching_content,
        dt(2026, 7, 10, 7, 2, 0),
        dt(2026, 7, 10, 7, 2, 0),
    );
    let second_group_member = seeded_manual_evidence_row(
        "split-group-b-member",
        "https://example.com/acme-guidance-b-member",
        matching_title,
        matching_content,
        dt(2026, 7, 10, 7, 3, 0),
        dt(2026, 7, 10, 7, 3, 0),
    );

    for row in [
        &first_group_representative,
        &first_group_member,
        &second_group_representative,
        &second_group_member,
    ] {
        repo.insert_evidence(row).await.unwrap();
    }

    repo.save_duplicate_group(&DuplicateGroupRow {
        duplicate_group_id: seeded_duplicate_group_id(first_group_representative.evidence_id),
        relation_type: "exact".to_string(),
        confidence: 1.0,
        locked_by_user: false,
        members: vec![
            DuplicateGroupMemberRow {
                evidence_id: first_group_representative.evidence_id,
                is_representative: true,
            },
            DuplicateGroupMemberRow {
                evidence_id: first_group_member.evidence_id,
                is_representative: false,
            },
        ],
        created_at: dt(2026, 7, 10, 7, 4, 0),
    })
    .await
    .unwrap();
    repo.save_duplicate_group(&DuplicateGroupRow {
        duplicate_group_id: seeded_duplicate_group_id(second_group_representative.evidence_id),
        relation_type: "exact".to_string(),
        confidence: 1.0,
        locked_by_user: false,
        members: vec![
            DuplicateGroupMemberRow {
                evidence_id: second_group_representative.evidence_id,
                is_representative: true,
            },
            DuplicateGroupMemberRow {
                evidence_id: second_group_member.evidence_id,
                is_representative: false,
            },
        ],
        created_at: dt(2026, 7, 10, 7, 5, 0),
    })
    .await
    .unwrap();

    let duplicate = assert_existing(
        ingestor
            .submit_at(
                ManualSource::Rest,
                ManualEventInput {
                    title: matching_title.to_string(),
                    content: matching_content.map(str::to_string),
                    source_url: Some("https://example.com/acme-guidance-submitted".to_string()),
                    submitted_by: "operator".to_string(),
                    published_at: Some(dt(2026, 7, 10, 7, 10, 0)),
                },
                dt(2026, 7, 10, 7, 10, 0),
            )
            .await
            .unwrap(),
    );

    assert_eq!(
        duplicate.existing.evidence_id,
        first_group_representative.evidence_id
    );

    let persisted_group: (Uuid, String) = sqlx::query_as(
        r#"SELECT g.duplicate_group_id, g.relation_type
           FROM market_event_duplicate_groups g
           JOIN market_event_duplicate_members m
             ON m.duplicate_group_id = g.duplicate_group_id
           WHERE m.evidence_id = $1"#,
    )
    .bind(duplicate.submitted.evidence_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(persisted_group.1, "review_required");

    let persisted_members: Vec<Uuid> = sqlx::query_scalar(
        r#"SELECT evidence_id
           FROM market_event_duplicate_members
           WHERE duplicate_group_id = $1
           ORDER BY evidence_id ASC"#,
    )
    .bind(persisted_group.0)
    .fetch_all(&pool)
    .await?;
    assert!(persisted_members.contains(&first_group_representative.evidence_id));
    assert!(persisted_members.contains(&first_group_member.evidence_id));
    assert!(persisted_members.contains(&duplicate.submitted.evidence_id));
    assert!(!persisted_members.contains(&second_group_representative.evidence_id));
    assert!(!persisted_members.contains(&second_group_member.evidence_id));

    let relevant_evidence_ids = vec![
        first_group_representative.evidence_id,
        first_group_member.evidence_id,
        second_group_representative.evidence_id,
        second_group_member.evidence_id,
        duplicate.submitted.evidence_id,
    ];
    let membership_counts: Vec<(Uuid, i64)> = sqlx::query_as(
        r#"SELECT evidence_id, COUNT(DISTINCT duplicate_group_id) AS membership_count
           FROM market_event_duplicate_members
           WHERE evidence_id = ANY($1::uuid[])
           GROUP BY evidence_id
           ORDER BY evidence_id ASC"#,
    )
    .bind(&relevant_evidence_ids)
    .fetch_all(&pool)
    .await?;
    let mut expected_membership_counts = relevant_evidence_ids
        .iter()
        .copied()
        .map(|evidence_id| (evidence_id, 1))
        .collect::<Vec<_>>();
    expected_membership_counts.sort_by_key(|(evidence_id, _)| *evidence_id);
    assert_eq!(membership_counts, expected_membership_counts);

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
    let input = ManualEventInput {
        title: " ACME   wins   contract ".to_string(),
        content: Some("Order value\n exceeds guidance".to_string()),
        source_url: Some("https://example.com/contracts/acme".to_string()),
        submitted_by: "operator".to_string(),
        published_at: Some(dt(2026, 7, 10, 7, 30, 0)),
    };
    let expected_hash = content_hash(&input.title, input.content.as_deref());
    let (gated_repo, gate) =
        repo.clone_with_manual_insert_candidate_discovery_gate_for_test(expected_hash.clone());
    let ingestor = ManualEvidenceIngestor::new(gated_repo, Arc::new(AShareTradingDateResolver));

    let left_worker = tokio::spawn({
        let ingestor = ingestor.clone();
        let input = input.clone();
        async move {
            ingestor
                .submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 0, 0))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_blocked())
        .await
        .expect("first identical submission should reach the repository candidate-discovery gate");

    let mut right_worker = tokio::spawn({
        let ingestor = ingestor.clone();
        async move {
            ingestor
                .submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 0, 1))
                .await
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut right_worker)
            .await
            .is_err(),
        "second identical submission should stay blocked on the repository transaction while the first is paused",
    );

    gate.release();
    let outcomes = [
        tokio::time::timeout(Duration::from_secs(5), left_worker)
            .await
            .expect("first identical submission should finish after the gate releases")
            .unwrap()
            .unwrap(),
        tokio::time::timeout(Duration::from_secs(5), right_worker)
            .await
            .expect("second identical submission should finish after the gate releases")
            .unwrap()
            .unwrap(),
    ];

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
        .clone_with_manual_insert_sleep_after_candidate_discovery_for_test(Duration::from_millis(
            200,
        ));
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
async fn concurrent_different_hash_near_duplicates_share_one_duplicate_group_and_representative(
    pool: PgPool,
) -> sqlx::Result<()> {
    let repo = EventRepository::new(pool.clone());
    let base_ingestor =
        ManualEvidenceIngestor::new(repo.clone(), Arc::new(AShareTradingDateResolver));

    let base = assert_inserted(
        base_ingestor
            .submit_at(
                ManualSource::Rest,
                ManualEventInput {
                    title: "Acme wins supply contract in Shenzhen".to_string(),
                    content: Some(
                        "Acme signed a long-term supply contract with Shenzhen transit authority today."
                            .to_string(),
                    ),
                    source_url: Some("https://example.com/contracts/acme-base".to_string()),
                    submitted_by: "operator".to_string(),
                    published_at: Some(dt(2026, 7, 10, 6, 20, 0)),
                },
                dt(2026, 7, 10, 8, 0, 0),
            )
            .await
            .unwrap(),
    );

    let left_input = ManualEventInput {
        title: "Acme wins major supply contract in Shenzhen".to_string(),
        content: Some(
            "Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up market note."
                .to_string(),
        ),
        source_url: Some("https://example.com/contracts/acme-left".to_string()),
        submitted_by: "operator".to_string(),
        published_at: Some(dt(2026, 7, 10, 6, 30, 0)),
    };
    let right_input = ManualEventInput {
        title: "Acme wins major supply contract in Shenzhen".to_string(),
        content: Some(
            "Acme signed a long-term supply contract with Shenzhen transit authority today. Follow-up market note with pricing context."
                .to_string(),
        ),
        source_url: Some("https://example.com/contracts/acme-right".to_string()),
        submitted_by: "operator".to_string(),
        published_at: Some(dt(2026, 7, 10, 6, 35, 0)),
    };
    let (gated_repo, gate) = repo
        .clone_with_manual_insert_duplicate_group_persistence_gate_for_test(content_hash(
            &left_input.title,
            left_input.content.as_deref(),
        ));
    let ingestor = ManualEvidenceIngestor::new(gated_repo, Arc::new(AShareTradingDateResolver));

    let left_worker = tokio::spawn({
        let ingestor = ingestor.clone();
        let input = left_input.clone();
        async move {
            ingestor
                .submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 5, 0))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_blocked())
        .await
        .expect("left near-duplicate submission should reach the persistence gate");

    let mut right_worker = tokio::spawn({
        let ingestor = ingestor.clone();
        async move {
            ingestor
                .submit_at(ManualSource::Rest, right_input, dt(2026, 7, 10, 8, 5, 1))
                .await
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut right_worker)
            .await
            .is_err(),
        "right near-duplicate submission should still be blocked while the left transaction is paused",
    );

    gate.release();
    let right = assert_existing(
        tokio::time::timeout(Duration::from_secs(5), right_worker)
            .await
            .expect("right near-duplicate worker should resume after the gate releases")
            .unwrap()
            .unwrap(),
    );
    let left = assert_existing(
        tokio::time::timeout(Duration::from_secs(5), left_worker)
            .await
            .expect("left near-duplicate worker should resume after the gate releases")
            .unwrap()
            .unwrap(),
    );

    let evidence_ids = vec![
        base.evidence_id,
        left.submitted.evidence_id,
        right.submitted.evidence_id,
    ];
    let membership_counts: Vec<(Uuid, i64)> = sqlx::query_as(
        r#"SELECT evidence_id, COUNT(DISTINCT duplicate_group_id) AS membership_count
           FROM market_event_duplicate_members
           WHERE evidence_id = ANY($1::uuid[])
           GROUP BY evidence_id
           ORDER BY evidence_id ASC"#,
    )
    .bind(&evidence_ids)
    .fetch_all(&pool)
    .await?;
    let mut expected_membership_counts = evidence_ids
        .iter()
        .copied()
        .map(|evidence_id| (evidence_id, 1))
        .collect::<Vec<_>>();
    expected_membership_counts.sort_by_key(|(evidence_id, _)| *evidence_id);
    assert_eq!(membership_counts, expected_membership_counts);

    let duplicate_group_id: Uuid = sqlx::query_scalar(
        r#"SELECT duplicate_group_id
           FROM market_event_duplicate_members
           WHERE evidence_id = $1"#,
    )
    .bind(base.evidence_id)
    .fetch_one(&pool)
    .await?;
    let members: Vec<(Uuid, bool)> = sqlx::query_as(
        r#"SELECT evidence_id, is_representative
           FROM market_event_duplicate_members
           WHERE duplicate_group_id = $1
           ORDER BY is_representative DESC, evidence_id ASC"#,
    )
    .bind(duplicate_group_id)
    .fetch_all(&pool)
    .await?;
    assert_eq!(members.len(), 3);
    assert_eq!(members[0], (base.evidence_id, true));
    assert!(members
        .iter()
        .any(|member| member.0 == left.submitted.evidence_id && !member.1));
    assert!(members
        .iter()
        .any(|member| member.0 == right.submitted.evidence_id && !member.1));

    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn candidate_discovery_gate_does_not_coordinate_unrelated_ingestors(
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
    let (gated_repo, gate) =
        repo.clone_with_manual_insert_candidate_discovery_gate_for_test(expected_hash);
    let gated_ingestor = ManualEvidenceIngestor::new(gated_repo, resolver);

    let mut first_gated_worker = tokio::spawn({
        let gated_ingestor = gated_ingestor.clone();
        let input = input.clone();
        async move {
            gated_ingestor
                .submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 0, 0))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_blocked())
        .await
        .expect("gated submission should reach the repository candidate-discovery gate");

    let unrelated = tokio::spawn(async move {
        unrelated_ingestor
            .submit_at(ManualSource::Rest, input, dt(2026, 7, 10, 8, 0, 1))
            .await
    });

    if let Ok(first) =
        tokio::time::timeout(Duration::from_millis(200), &mut first_gated_worker).await
    {
        first.unwrap().unwrap();
        unrelated.await.unwrap().unwrap();
        panic!("unrelated ingestor with the same content hash released the repository gate");
    }

    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut first_gated_worker)
            .await
            .is_err(),
        "unrelated ingestor must not consume or release the repository candidate-discovery gate",
    );

    gate.release();
    let (gated_outcome, unrelated_outcome) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(5), first_gated_worker),
        tokio::time::timeout(Duration::from_secs(5), unrelated),
    );
    assert_inserted(
        gated_outcome
            .expect("gated submission should complete after the gate releases")
            .unwrap()
            .unwrap(),
    );
    assert_existing(
        unrelated_outcome
            .expect("unrelated submission should complete after the gated transaction commits")
            .unwrap()
            .unwrap(),
    );

    Ok(())
}

fn dt(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .unwrap()
}

fn seeded_manual_evidence_row(
    source_item_id: &str,
    source_url: &str,
    title: &str,
    content: Option<&str>,
    available_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
) -> EventEvidenceRow {
    EventEvidenceRow {
        evidence_id: Uuid::new_v4(),
        source_id: MANUAL_SOURCE_REST.to_string(),
        source_item_id: source_item_id.to_string(),
        source_url: Some(source_url.to_string()),
        source_tier: "manual".to_string(),
        source_terms_version: "terms-v1".to_string(),
        occurred_at: Some(available_at),
        published_at: Some(available_at),
        first_seen_at: available_at,
        available_at,
        effective_trade_date: NaiveDate::from_ymd_opt(2026, 7, 10).unwrap(),
        title: title.to_string(),
        content: content.map(str::to_string),
        language: "und".to_string(),
        content_hash: content_hash(title, content),
        raw_payload: json!({
            "submitted_by": "seed",
            "manual_source_id": MANUAL_SOURCE_REST,
        }),
        version: 1,
        supersedes_evidence_id: None,
        status: "pending".to_string(),
        created_at,
    }
}

fn seeded_duplicate_group_id(representative_id: Uuid) -> Uuid {
    let digest = Sha256::digest(format!("market-event-duplicate:{representative_id}").as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

async fn insert_raw_evidence_row(pool: &PgPool, row: &EventEvidenceRow) -> sqlx::Result<()> {
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
