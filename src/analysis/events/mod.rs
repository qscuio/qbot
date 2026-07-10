use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use self::evidence::{ManualEvidenceIngestor, ManualSource};
use self::extraction::{EventExtractionInput, EventExtractionOutput, EventExtractor};
use crate::error::{AppError, Result};
use crate::storage::event_repository::{
    DailyEventBriefRow, EventEvidenceRow, EventRepository, EventRevisionRow,
};

pub(crate) mod claims;
pub mod contracts;
mod dedup;
pub(crate) mod entity_linking;
mod evidence;
pub(crate) mod extraction;
mod time;

pub use contracts::{
    AShareTradingDateResolver, BriefEntity, BriefFact, BriefRevision, BriefSource,
    BriefUnconfirmed, DailyEventBrief, EventDetail, EventEvidence, EventListItem,
    EventProcessingSummary, EventReviewResult, ExistingEventEvidenceRelation, ManualEventInput,
    ManualEventSubmissionOutcome, PersistedDailyEventBrief, TradingDateResolver,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventReviewAction {
    Publish,
    Reject,
}

pub struct EventIntelligence {
    deps: EventIntelligenceDependencies,
}

impl EventIntelligence {
    pub fn new(pool: PgPool) -> Self {
        Self::with_repository_and_resolver(
            EventRepository::new(pool),
            Arc::new(AShareTradingDateResolver),
        )
    }

    pub async fn submit_manual_event(
        &self,
        input: ManualEventInput,
    ) -> Result<ManualEventSubmissionOutcome> {
        self.submit_manual_event_from_source_at(ManualSource::Rest, input, Utc::now())
            .await
    }

    pub async fn list_events(&self, limit: Option<usize>) -> Result<Vec<EventListItem>> {
        self.deps
            .repo
            .list_latest_evidence(limit)
            .await
            .map(|rows| rows.into_iter().map(event_list_item_from_row).collect())
    }

    pub async fn get_event_detail(&self, event_id: Uuid) -> Result<EventDetail> {
        let row = self
            .deps
            .repo
            .find_evidence_by_id(event_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("event evidence {event_id}")))?;

        Ok(event_detail_from_row(&row))
    }

    pub async fn review_event(
        &self,
        event_id: Uuid,
        reviewed_by: String,
        action: EventReviewAction,
    ) -> Result<EventReviewResult> {
        let current = self
            .deps
            .repo
            .find_evidence_by_id(event_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("event evidence {event_id}")))?;
        let latest = self
            .deps
            .repo
            .latest_evidence_for_source_item(&current.source_id, &current.source_item_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("event evidence {event_id}")))?;

        if latest.evidence_id != current.evidence_id || current.status != "pending" {
            return Err(AppError::BadRequest(
                "unauthorized review action".to_string(),
            ));
        }

        let reviewed_at = Utc::now();
        let next = reviewed_event_row(&current, &reviewed_by, reviewed_at, action);
        self.deps.repo.insert_evidence(&next).await?;
        self.deps
            .repo
            .save_revision(&EventRevisionRow {
                revision_id: Uuid::new_v4(),
                object_type: "market_event_evidence_review".to_string(),
                object_id: next.evidence_id,
                previous_payload: json!({
                    "evidenceId": current.evidence_id,
                    "processingStatus": current.status,
                    "version": current.version,
                }),
                revised_payload: json!({
                    "evidenceId": next.evidence_id,
                    "processingStatus": next.status,
                    "version": next.version,
                    "reviewAction": review_action_label(action),
                    "reviewedBy": reviewed_by.clone(),
                    "reviewedAt": reviewed_at,
                }),
                revised_by: reviewed_by.clone(),
                reason: format!("manual {} review", review_action_label(action)),
                created_at: reviewed_at,
            })
            .await?;

        Ok(EventReviewResult {
            evidence_id: next.evidence_id,
            supersedes_evidence_id: current.evidence_id,
            source_item_id: next.source_item_id,
            processing_status: next.status,
            effective_trade_date: next.effective_trade_date,
            version: next.version,
            reviewed_by,
        })
    }

    pub async fn get_daily_brief(
        &self,
        trade_date: Option<NaiveDate>,
    ) -> Result<PersistedDailyEventBrief> {
        let brief = self
            .deps
            .repo
            .find_daily_brief(trade_date)
            .await?
            .ok_or_else(|| {
                AppError::NotFound(match trade_date {
                    Some(date) => format!("daily event brief for {date}"),
                    None => "daily event brief".to_string(),
                })
            })?;

        Ok(persisted_brief_from_row(&brief))
    }

    pub async fn process_pending(&self, _cutoff: DateTime<Utc>) -> Result<EventProcessingSummary> {
        Err(task_two_not_wired_error())
    }

    pub async fn build_daily_brief(&self, _trade_date: NaiveDate) -> Result<DailyEventBrief> {
        Err(task_two_not_wired_error())
    }

    pub(crate) fn with_repository_and_resolver(
        event_repo: EventRepository,
        resolver: Arc<dyn TradingDateResolver>,
    ) -> Self {
        Self {
            deps: EventIntelligenceDependencies::wired(event_repo, resolver),
        }
    }

    pub(crate) async fn submit_manual_event_from_source_at(
        &self,
        source: ManualSource,
        input: ManualEventInput,
        first_seen_at: DateTime<Utc>,
    ) -> Result<ManualEventSubmissionOutcome> {
        self.manual_ingestor()?
            .submit_at(source, input, first_seen_at)
            .await
    }

    fn manual_ingestor(&self) -> Result<ManualEvidenceIngestor> {
        Ok(ManualEvidenceIngestor::new(
            self.deps.repo.clone(),
            Arc::clone(&self.deps.resolver),
        ))
    }
}

fn task_two_not_wired_error() -> AppError {
    AppError::Internal("event intelligence Task 2 interface is not wired yet".to_string())
}

fn event_list_item_from_row(row: EventEvidenceRow) -> EventListItem {
    let source_readable = source_readable_from_content(row.content.as_deref());
    let content = row.content.clone();
    EventListItem {
        evidence_id: row.evidence_id,
        source_id: row.source_id,
        source_item_id: row.source_item_id,
        source_url: row.source_url,
        source_tier: row.source_tier,
        published_at: row.published_at,
        first_seen_at: row.first_seen_at,
        available_at: row.available_at,
        effective_trade_date: row.effective_trade_date,
        title: row.title,
        content,
        processing_status: row.status,
        version: row.version,
        supersedes_evidence_id: row.supersedes_evidence_id,
        source_readable,
        manual_review_needed: None,
    }
}

fn event_detail_from_row(row: &EventEvidenceRow) -> EventDetail {
    EventDetail {
        evidence_id: row.evidence_id,
        source_id: row.source_id.clone(),
        source_item_id: row.source_item_id.clone(),
        source_url: row.source_url.clone(),
        source_tier: row.source_tier.clone(),
        source_terms_version: row.source_terms_version.clone(),
        occurred_at: row.occurred_at,
        published_at: row.published_at,
        first_seen_at: row.first_seen_at,
        available_at: row.available_at,
        effective_trade_date: row.effective_trade_date,
        title: row.title.clone(),
        content: row.content.clone(),
        language: row.language.clone(),
        content_hash: row.content_hash.clone(),
        processing_status: row.status.clone(),
        version: row.version,
        supersedes_evidence_id: row.supersedes_evidence_id,
        source_readable: source_readable_from_content(row.content.as_deref()),
        manual_review_needed: None,
    }
}

fn source_readable_from_content(content: Option<&str>) -> Option<bool> {
    content
        .map(str::trim)
        .map(|normalized| !normalized.is_empty())
}

fn reviewed_event_row(
    current: &EventEvidenceRow,
    reviewed_by: &str,
    reviewed_at: DateTime<Utc>,
    action: EventReviewAction,
) -> EventEvidenceRow {
    let mut raw_payload = current.raw_payload.clone();
    if let Some(map) = raw_payload.as_object_mut() {
        map.insert("reviewed_by".to_string(), json!(reviewed_by));
        map.insert("reviewed_at".to_string(), json!(reviewed_at));
        map.insert(
            "review_action".to_string(),
            json!(review_action_label(action)),
        );
    }

    EventEvidenceRow {
        evidence_id: Uuid::new_v4(),
        raw_payload,
        version: current.version + 1,
        supersedes_evidence_id: Some(current.evidence_id),
        status: reviewed_status(action).to_string(),
        created_at: reviewed_at,
        ..current.clone()
    }
}

fn reviewed_status(action: EventReviewAction) -> &'static str {
    match action {
        EventReviewAction::Publish => "publishable",
        EventReviewAction::Reject => "rejected",
    }
}

fn review_action_label(action: EventReviewAction) -> &'static str {
    match action {
        EventReviewAction::Publish => "publish",
        EventReviewAction::Reject => "reject",
    }
}

fn persisted_brief_from_row(row: &DailyEventBriefRow) -> PersistedDailyEventBrief {
    PersistedDailyEventBrief {
        trade_date: row.trade_date,
        brief_version: row.brief_version.clone(),
        content: row.content.clone(),
        structured_payload: row.structured_payload.clone(),
        input_fingerprint: row.input_fingerprint.clone(),
        generated_at: row.generated_at,
    }
}

struct EventIntelligenceDependencies {
    repo: EventRepository,
    resolver: Arc<dyn TradingDateResolver>,
    _extractor: Arc<dyn EventExtractor>,
}

impl EventIntelligenceDependencies {
    fn wired(event_repo: EventRepository, resolver: Arc<dyn TradingDateResolver>) -> Self {
        Self {
            repo: event_repo,
            resolver,
            _extractor: Arc::new(NoopEventExtractor),
        }
    }
}

#[derive(Debug, Default)]
struct NoopEventExtractor;

#[async_trait]
impl EventExtractor for NoopEventExtractor {
    async fn extract(&self, _input: EventExtractionInput) -> Result<EventExtractionOutput> {
        Err(AppError::Internal(
            "event extraction is not wired yet".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::EventIntelligence;

    #[test]
    fn event_intelligence_exposes_a_small_public_constructor() {
        let _constructor: fn(sqlx::PgPool) -> EventIntelligence = EventIntelligence::new;
    }

    #[test]
    fn event_intelligence_module_keeps_internal_collaborators_private() {
        let module_source = fs::read_to_string(module_source_path()).unwrap();

        assert!(module_source
            .lines()
            .any(|line| line.trim() == "pub fn new(pool: PgPool) -> Self {"));
        assert!(!module_source
            .lines()
            .any(|line| line.trim_start().starts_with("pub trait EventExtractor")));
        assert!(!module_source
            .lines()
            .any(|line| line.trim() == "pub repo: EventRepository,"));
    }

    fn module_source_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(file!())
    }
}
