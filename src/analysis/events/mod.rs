use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::json;
use sqlx::PgPool;
use tracing::warn;
use uuid::Uuid;

use self::evidence::{ManualEvidenceIngestor, ManualSource};
use self::extraction::{
    ClaimType, EventExtractionInput, EventExtractionOutput, EventExtractionV1, EventExtractor,
    ExtractionEvidence,
};
use self::reporting::{
    build_daily_event_brief, render_daily_event_brief, BriefClaimRecord, BriefEntityRecord,
    BriefEvidenceRecord,
};
use crate::error::{AppError, Result};
use crate::storage::event_repository::{
    ClaimEvidenceRow, ClaimRow, DailyEventBriefRow, EventEvidenceRow, EventRepository,
    EventRevisionRow, ExtractionRow,
};

pub(crate) mod claims;
pub mod contracts;
mod dedup;
pub(crate) mod entity_linking;
mod evidence;
pub(crate) mod extraction;
mod reporting;
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
        let revision = EventRevisionRow {
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
        };
        self.deps
            .repo
            .save_reviewed_evidence_revision(&next, &revision)
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
        let pending = self.deps.repo.list_latest_pending_evidence(_cutoff).await?;
        let pending_count = pending.len();
        let mut processed_count = 0usize;

        for evidence in pending {
            match self.process_pending_evidence(&evidence).await {
                Ok(()) => {
                    processed_count += 1;
                }
                Err(error) => {
                    warn!(
                        "Event extraction/publish failed for evidence {} (source={} item={}): {}",
                        evidence.evidence_id, evidence.source_id, evidence.source_item_id, error
                    );
                }
            }
        }

        Ok(EventProcessingSummary {
            cutoff: _cutoff,
            pending_evidence_count: pending_count,
            processed_evidence_count: processed_count,
        })
    }

    pub async fn build_daily_brief(&self, trade_date: NaiveDate) -> Result<DailyEventBrief> {
        let latest_publishable =
            latest_publishable_rows(self.deps.repo.list_publishable_evidence(trade_date).await?);
        let mut lineage_rows = Vec::new();
        for evidence in &latest_publishable {
            lineage_rows.extend(
                self.deps
                    .repo
                    .find_existing_source_item(&evidence.source_id, &evidence.source_item_id)
                    .await?,
            );
        }

        let lineage_rows = dedupe_lineage_rows(lineage_rows);
        let extraction_rows = self
            .deps
            .repo
            .list_latest_extractions_for_evidence_ids(
                &lineage_rows
                    .iter()
                    .map(|row| row.evidence_id)
                    .collect::<Vec<_>>(),
            )
            .await?;

        let extraction_by_evidence = extraction_rows
            .into_iter()
            .map(|row| (row.evidence_id, row))
            .collect::<std::collections::BTreeMap<_, _>>();
        let lineage_by_source_item = group_lineage_by_source_item(lineage_rows);
        let lineage_to_latest_published =
            lineage_to_latest_published_map(&latest_publishable, &lineage_by_source_item);

        let evidence_records = latest_publishable
            .iter()
            .map(|row| BriefEvidenceRecord {
                evidence_id: row.evidence_id,
                source_id: row.source_id.clone(),
                source_item_id: row.source_item_id.clone(),
                published_at: row.published_at,
                available_at: row.available_at,
                title: row.title.clone(),
                supersedes_evidence_id: row.supersedes_evidence_id,
            })
            .collect::<Vec<_>>();

        let mut claim_records = Vec::new();
        let mut entity_records = Vec::new();
        for evidence in &latest_publishable {
            let Some(lineage) = lineage_by_source_item.get(&source_item_key(evidence)) else {
                continue;
            };
            let Some(current_extraction_evidence_id) =
                current_extraction_evidence_id(evidence, lineage, &extraction_by_evidence)
            else {
                continue;
            };
            let Some(current_extraction) =
                extraction_by_evidence.get(&current_extraction_evidence_id)
            else {
                continue;
            };

            let previous_fact_ids = previous_published_fact_ids(
                current_extraction_evidence_id,
                lineage,
                &extraction_by_evidence,
            );
            claim_records.extend(build_brief_claim_records(
                current_extraction,
                &lineage_to_latest_published,
                previous_fact_ids,
            ));
            entity_records.extend(build_brief_entity_records(
                current_extraction,
                &lineage_to_latest_published,
            )?);
        }

        build_daily_event_brief(trade_date, evidence_records, claim_records, entity_records)
    }

    pub(crate) fn with_repository_and_resolver(
        event_repo: EventRepository,
        resolver: Arc<dyn TradingDateResolver>,
    ) -> Self {
        Self {
            deps: EventIntelligenceDependencies::wired(event_repo, resolver),
        }
    }

    pub(crate) fn with_repository_resolver_and_extractor(
        event_repo: EventRepository,
        resolver: Arc<dyn TradingDateResolver>,
        extractor: Arc<dyn EventExtractor>,
    ) -> Self {
        Self {
            deps: EventIntelligenceDependencies::with_extractor(event_repo, resolver, extractor),
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

    async fn process_pending_evidence(&self, evidence: &EventEvidenceRow) -> Result<()> {
        let extracted_at = Utc::now();
        let input = EventExtractionInput {
            evidence_id: evidence.evidence_id,
            input_fingerprint: extraction_input_fingerprint(evidence)?,
            evidence: vec![ExtractionEvidence::new(
                evidence.evidence_id,
                extraction_source_text(evidence),
            )],
        };
        let input_fingerprint = input.input_fingerprint.clone();
        let output = self.deps.extractor.extract(input).await?;
        let extraction = extraction_row_from_output(
            evidence.evidence_id,
            input_fingerprint,
            extracted_at,
            output,
        )?;
        self.deps.repo.save_extraction(&extraction).await?;

        let publish_at = Utc::now();
        let reviewed_by = "system:event_ingestion".to_string();
        let next = reviewed_event_row(
            evidence,
            &reviewed_by,
            publish_at,
            EventReviewAction::Publish,
        );
        let revision = EventRevisionRow {
            revision_id: Uuid::new_v4(),
            object_type: "market_event_evidence_review".to_string(),
            object_id: next.evidence_id,
            previous_payload: json!({
                "evidenceId": evidence.evidence_id,
                "processingStatus": evidence.status,
                "version": evidence.version,
            }),
            revised_payload: json!({
                "evidenceId": next.evidence_id,
                "processingStatus": next.status,
                "version": next.version,
                "reviewAction": review_action_label(EventReviewAction::Publish),
                "reviewedBy": reviewed_by,
                "reviewedAt": publish_at,
            }),
            revised_by: reviewed_by,
            reason: "automated publish after structured extraction".to_string(),
            created_at: publish_at,
        };
        self.deps
            .repo
            .save_reviewed_evidence_revision(&next, &revision)
            .await?;
        Ok(())
    }
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

pub(crate) fn render_daily_brief(brief: &DailyEventBrief) -> Result<String> {
    render_daily_event_brief(brief)
}

fn extraction_input_fingerprint(evidence: &EventEvidenceRow) -> Result<String> {
    use sha2::{Digest, Sha256};

    let payload = serde_json::to_vec(&json!({
        "evidenceId": evidence.evidence_id,
        "sourceId": evidence.source_id,
        "sourceItemId": evidence.source_item_id,
        "publishedAt": evidence.published_at,
        "availableAt": evidence.available_at,
        "title": evidence.title,
        "content": evidence.content,
    }))
    .map_err(AppError::Json)?;

    let mut hasher = Sha256::new();
    hasher.update(payload);
    Ok(format!("{:x}", hasher.finalize()))
}

fn extraction_source_text(evidence: &EventEvidenceRow) -> String {
    match evidence.content.as_deref() {
        Some(content) if !content.trim().is_empty() => {
            format!("{}\n{}", evidence.title.trim(), content.trim())
        }
        _ => evidence.title.trim().to_string(),
    }
}

fn extraction_row_from_output(
    evidence_id: Uuid,
    input_fingerprint: String,
    created_at: DateTime<Utc>,
    output: EventExtractionOutput,
) -> Result<ExtractionRow> {
    Ok(ExtractionRow {
        extraction_id: Uuid::new_v4(),
        evidence_id,
        schema_version: output.metadata.schema_version,
        prompt_version: Some(output.metadata.prompt_version),
        model_name: Some(output.metadata.model_name),
        model_parameters: output.metadata.model_parameters,
        extracted_payload: serde_json::to_value(&output.extraction).map_err(AppError::Json)?,
        validation_status: "valid".to_string(),
        validation_errors: json!([]),
        input_fingerprint,
        claims: output
            .extraction
            .claims
            .into_iter()
            .map(|claim| ClaimRow {
                claim_id: Uuid::new_v4(),
                claim_type: extracted_claim_type_label(claim.claim_type).to_string(),
                claim_text: claim.text,
                confidence: claim.confidence,
                review_status: extracted_claim_review_status(claim.claim_type).to_string(),
                evidence: claim
                    .evidence_ids
                    .into_iter()
                    .map(|evidence_id| ClaimEvidenceRow { evidence_id })
                    .collect(),
                created_at,
            })
            .collect(),
        created_at,
    })
}

fn extracted_claim_type_label(claim_type: ClaimType) -> &'static str {
    match claim_type {
        ClaimType::Fact => "fact",
        ClaimType::DirectQuote => "direct_quote",
        ClaimType::ThirdPartyClaim => "third_party_claim",
        ClaimType::JournalistInterpretation => "journalist_interpretation",
        ClaimType::Rumor => "rumor",
        ClaimType::Unknown => "unknown",
    }
}

fn extracted_claim_review_status(claim_type: ClaimType) -> &'static str {
    match claim_type {
        ClaimType::Fact => "published",
        ClaimType::DirectQuote
        | ClaimType::ThirdPartyClaim
        | ClaimType::JournalistInterpretation
        | ClaimType::Rumor
        | ClaimType::Unknown => "draft",
    }
}

fn latest_publishable_rows(rows: Vec<EventEvidenceRow>) -> Vec<EventEvidenceRow> {
    let mut latest_by_source =
        std::collections::BTreeMap::<(String, String), EventEvidenceRow>::new();
    for row in rows {
        let key = source_item_key(&row);
        match latest_by_source.get(&key) {
            Some(existing)
                if existing.version > row.version
                    || (existing.version == row.version
                        && existing.created_at >= row.created_at) => {}
            _ => {
                latest_by_source.insert(key, row);
            }
        }
    }
    let mut latest = latest_by_source.into_values().collect::<Vec<_>>();
    latest.sort_by(|left, right| {
        left.available_at
            .cmp(&right.available_at)
            .then(left.first_seen_at.cmp(&right.first_seen_at))
            .then(left.source_id.cmp(&right.source_id))
            .then(left.source_item_id.cmp(&right.source_item_id))
            .then(left.version.cmp(&right.version))
            .then(left.evidence_id.cmp(&right.evidence_id))
    });
    latest
}

fn dedupe_lineage_rows(rows: Vec<EventEvidenceRow>) -> Vec<EventEvidenceRow> {
    let mut seen = std::collections::BTreeSet::new();
    let mut deduped = Vec::new();
    for row in rows {
        if seen.insert(row.evidence_id) {
            deduped.push(row);
        }
    }
    deduped.sort_by(|left, right| {
        source_item_key(left)
            .cmp(&source_item_key(right))
            .then(left.version.cmp(&right.version))
            .then(left.evidence_id.cmp(&right.evidence_id))
    });
    deduped
}

fn group_lineage_by_source_item(
    rows: Vec<EventEvidenceRow>,
) -> std::collections::BTreeMap<(String, String), Vec<EventEvidenceRow>> {
    let mut grouped = std::collections::BTreeMap::<(String, String), Vec<EventEvidenceRow>>::new();
    for row in rows {
        grouped.entry(source_item_key(&row)).or_default().push(row);
    }
    for lineage in grouped.values_mut() {
        lineage.sort_by(|left, right| {
            left.version
                .cmp(&right.version)
                .then(left.evidence_id.cmp(&right.evidence_id))
        });
    }
    grouped
}

fn lineage_to_latest_published_map(
    latest_publishable: &[EventEvidenceRow],
    lineage_by_source_item: &std::collections::BTreeMap<(String, String), Vec<EventEvidenceRow>>,
) -> std::collections::BTreeMap<Uuid, Uuid> {
    let mut mapping = std::collections::BTreeMap::new();
    for published in latest_publishable {
        if let Some(lineage) = lineage_by_source_item.get(&source_item_key(published)) {
            for row in lineage {
                mapping.insert(row.evidence_id, published.evidence_id);
            }
        }
    }
    mapping
}

fn current_extraction_evidence_id(
    published: &EventEvidenceRow,
    lineage: &[EventEvidenceRow],
    extraction_by_evidence: &std::collections::BTreeMap<Uuid, ExtractionRow>,
) -> Option<Uuid> {
    let published_index = lineage
        .iter()
        .position(|row| row.evidence_id == published.evidence_id)?;
    lineage[..=published_index]
        .iter()
        .rev()
        .find(|row| extraction_by_evidence.contains_key(&row.evidence_id))
        .map(|row| row.evidence_id)
}

fn previous_published_fact_ids(
    current_extraction_evidence_id: Uuid,
    lineage: &[EventEvidenceRow],
    extraction_by_evidence: &std::collections::BTreeMap<Uuid, ExtractionRow>,
) -> Vec<Uuid> {
    let Some(current_index) = lineage
        .iter()
        .position(|row| row.evidence_id == current_extraction_evidence_id)
    else {
        return Vec::new();
    };

    for row in lineage[..current_index].iter().rev() {
        let Some(extraction) = extraction_by_evidence.get(&row.evidence_id) else {
            continue;
        };
        let fact_ids = extraction
            .claims
            .iter()
            .filter(|claim| claim.review_status == "published" && claim.claim_type == "fact")
            .map(|claim| claim.claim_id)
            .collect::<Vec<_>>();
        if !fact_ids.is_empty() {
            return fact_ids;
        }
    }

    Vec::new()
}

fn build_brief_claim_records(
    extraction: &ExtractionRow,
    lineage_to_latest_published: &std::collections::BTreeMap<Uuid, Uuid>,
    previous_fact_ids: Vec<Uuid>,
) -> Vec<BriefClaimRecord> {
    extraction
        .claims
        .iter()
        .enumerate()
        .map(|(index, claim)| BriefClaimRecord {
            claim_id: claim.claim_id,
            claim_type: claim.claim_type.clone(),
            claim_text: claim.claim_text.clone(),
            review_status: claim.review_status.clone(),
            evidence_ids: claim
                .evidence
                .iter()
                .map(|evidence| {
                    lineage_to_latest_published
                        .get(&evidence.evidence_id)
                        .copied()
                        .unwrap_or(evidence.evidence_id)
                })
                .collect(),
            previous_fact_id: previous_fact_ids.get(index).copied(),
        })
        .collect()
}

fn build_brief_entity_records(
    extraction: &ExtractionRow,
    lineage_to_latest_published: &std::collections::BTreeMap<Uuid, Uuid>,
) -> Result<Vec<BriefEntityRecord>> {
    let parsed = serde_json::from_value::<EventExtractionV1>(extraction.extracted_payload.clone())
        .map_err(AppError::Json)?;
    let _published_evidence_id = lineage_to_latest_published
        .get(&extraction.evidence_id)
        .copied()
        .unwrap_or(extraction.evidence_id);

    Ok(parsed
        .entities
        .into_iter()
        .filter_map(|entity| match entity.entity_type.as_str() {
            "industry" | "sector" => Some(BriefEntityRecord {
                entity_id: format!("industry:{}", entity.text),
                display_name: entity.text,
            }),
            "organization" | "company" | "issuer" => Some(BriefEntityRecord {
                entity_id: entity
                    .stock_code
                    .clone()
                    .unwrap_or_else(|| format!("company:{}", entity.text)),
                display_name: entity.text,
            }),
            _ => None,
        })
        .collect())
}

fn source_item_key(row: &EventEvidenceRow) -> (String, String) {
    (row.source_id.clone(), row.source_item_id.clone())
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
    extractor: Arc<dyn EventExtractor>,
}

impl EventIntelligenceDependencies {
    fn wired(event_repo: EventRepository, resolver: Arc<dyn TradingDateResolver>) -> Self {
        Self::with_extractor(event_repo, resolver, Arc::new(NoopEventExtractor))
    }

    fn with_extractor(
        event_repo: EventRepository,
        resolver: Arc<dyn TradingDateResolver>,
        extractor: Arc<dyn EventExtractor>,
    ) -> Self {
        Self {
            repo: event_repo,
            resolver,
            extractor,
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
