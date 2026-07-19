use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipBucket {
    pub price: f64,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipSnapshot {
    pub code: String,
    pub trade_date: NaiveDate,
    pub distribution: Vec<ChipBucket>,
    pub average_cost: f64,
    pub winner_rate: f64,
    pub concentration: f64,
    pub dominant_peak_price: f64,
    pub source: String,
    pub model_version: Option<String>,
    pub validated: bool,
    pub source_updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipDayInput {
    pub code: String,
    pub trade_date: NaiveDate,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover_rate: f64,
    pub adjustment_factor: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipModelState {
    pub code: String,
    pub model_version: String,
    pub through_date: NaiveDate,
    pub distribution: Vec<ChipBucket>,
    pub last_adjustment_factor: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChipSourceDecision {
    Estimate,
    Official,
}

impl ChipSourceDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Estimate => "estimate",
            Self::Official => "official",
        }
    }

    pub fn from_storage(value: &str) -> Option<Self> {
        match value {
            "estimate" => Some(Self::Estimate),
            "official" => Some(Self::Official),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipValidationRun {
    pub run_id: Uuid,
    pub model_version: String,
    pub sample_definition: Value,
    pub aggregate_metrics: Value,
    pub subgroup_metrics: Value,
    pub decision: Option<ChipSourceDecision>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_summary: Option<String>,
}
