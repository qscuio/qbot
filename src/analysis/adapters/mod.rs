use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::error::Result;

pub mod gdelt;
pub mod llm_event_extractor;
pub mod official_event_source;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentRetentionPolicy {
    StoreFullContent,
    StoreSummaryOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedEvent {
    pub source_item_id: String,
    pub published_at: DateTime<Utc>,
    pub title: String,
    pub content: Option<String>,
    pub source_url: String,
    pub raw_payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchBatch {
    pub items: Vec<FetchedEvent>,
    pub next_cursor: Option<String>,
}

#[async_trait]
pub trait EventSource: Send + Sync {
    fn source_id(&self) -> &'static str;
    fn retention_policy(&self) -> ContentRetentionPolicy;
    async fn fetch(&self, cursor: Option<String>, until: DateTime<Utc>) -> Result<FetchBatch>;
}
