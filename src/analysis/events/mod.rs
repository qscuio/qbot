use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};

use crate::error::{AppError, Result};
use crate::storage::event_repository::EventRepository;

pub mod contracts;

pub use contracts::{
    AShareTradingDateResolver, BriefEntity, BriefFact, BriefRevision, BriefSource,
    BriefUnconfirmed, DailyEventBrief, EventEvidence, EventProcessingSummary, ManualEventInput,
    TradingDateResolver,
};

trait EventExtractor: Send + Sync {}

pub struct EventIntelligence {
    _deps: EventIntelligenceDependencies,
}

impl EventIntelligence {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn submit_manual_event(&self, _input: ManualEventInput) -> Result<EventEvidence> {
        Err(task_two_not_wired_error())
    }

    pub async fn process_pending(&self, _cutoff: DateTime<Utc>) -> Result<EventProcessingSummary> {
        Err(task_two_not_wired_error())
    }

    pub async fn build_daily_brief(&self, _trade_date: NaiveDate) -> Result<DailyEventBrief> {
        Err(task_two_not_wired_error())
    }
}

fn task_two_not_wired_error() -> AppError {
    AppError::Internal("event intelligence Task 2 interface is not wired yet".to_string())
}

impl Default for EventIntelligence {
    fn default() -> Self {
        Self {
            _deps: EventIntelligenceDependencies::unwired(),
        }
    }
}

struct EventIntelligenceDependencies {
    _repo: Option<EventRepository>,
    _resolver: Arc<dyn TradingDateResolver>,
    _extractor: Arc<dyn EventExtractor>,
}

impl EventIntelligenceDependencies {
    fn unwired() -> Self {
        Self {
            _repo: None,
            _resolver: Arc::new(AShareTradingDateResolver),
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
        let _ = EventIntelligence::new();
    }

    #[test]
    fn event_intelligence_module_keeps_internal_collaborators_private() {
        let module_source = fs::read_to_string(module_source_path()).unwrap();

        assert!(module_source
            .lines()
            .any(|line| line.trim() == "pub fn new() -> Self {"));
        assert!(!module_source
            .lines()
            .any(|line| line.trim_start().starts_with("pub trait EventExtractor")));
        assert!(!module_source
            .lines()
            .any(|line| line.trim() == "repo: EventRepository,"));
    }

    fn module_source_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(file!())
    }
}
