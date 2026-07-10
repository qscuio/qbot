use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgPool;

use self::evidence::{ManualEvidenceIngestor, ManualSource};
use crate::error::{AppError, Result};
use crate::storage::event_repository::EventRepository;

pub mod contracts;
mod dedup;
mod evidence;
mod time;

pub use contracts::{
    AShareTradingDateResolver, BriefEntity, BriefFact, BriefRevision, BriefSource,
    BriefUnconfirmed, DailyEventBrief, EventEvidence, EventProcessingSummary,
    ExistingEventEvidenceRelation, ManualEventInput, ManualEventSubmissionOutcome,
    TradingDateResolver,
};
pub use dedup::DuplicateDecision;

trait EventExtractor: Send + Sync {}

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

impl EventExtractor for NoopEventExtractor {}

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
