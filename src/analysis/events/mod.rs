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

pub trait EventExtractor: Send + Sync {}

pub struct EventIntelligence {
    repo: EventRepository,
    resolver: Arc<dyn TradingDateResolver>,
    extractor: Arc<dyn EventExtractor>,
}

impl EventIntelligence {
    pub fn new(
        repo: EventRepository,
        resolver: Arc<dyn TradingDateResolver>,
        extractor: Arc<dyn EventExtractor>,
    ) -> Self {
        Self {
            repo,
            resolver,
            extractor,
        }
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
