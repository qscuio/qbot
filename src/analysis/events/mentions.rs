use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct EventMention {
    pub mention_id: Uuid,
    pub evidence_id: Uuid,
    pub event_time: Option<DateTime<Utc>>,
    pub entity_ids: Vec<String>,
    pub action_tokens: Vec<String>,
    pub location_tokens: Vec<String>,
    pub semantic_vector: Vec<f32>,
    pub adds_new_fact: bool,
    pub source_independence: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterMention {
    pub origin_cluster_id: Uuid,
    pub duplicate_group_id: Option<Uuid>,
    pub mention: EventMention,
}
