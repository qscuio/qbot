use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::analysis::events::DailyEventBrief;

pub const DECISION_SUPPORT_VERSION: &str = "decision-support-v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StatementBucket {
    Fact,
    Calculation,
    Inference,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SupportStatementKind {
    EventFact,
    PatternSimilarity,
    PatternLift,
    ImpactHypothesis,
    MissingStatus,
    OtherFact,
    OtherCalculation,
    OtherInference,
    OtherUnknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SupportStatement {
    pub kind: SupportStatementKind,
    pub statement: String,
    pub source_refs: Vec<String>,
}

impl SupportStatement {
    pub fn new(
        kind: SupportStatementKind,
        statement: impl Into<String>,
        source_refs: Vec<String>,
    ) -> Self {
        Self {
            kind,
            statement: statement.into(),
            source_refs,
        }
    }

    pub fn event_fact(statement: impl Into<String>, source_refs: Vec<String>) -> Self {
        Self::new(SupportStatementKind::EventFact, statement, source_refs)
    }

    pub fn pattern_similarity(statement: impl Into<String>) -> Self {
        Self::new(
            SupportStatementKind::PatternSimilarity,
            statement,
            Vec::new(),
        )
    }

    pub fn pattern_lift(statement: impl Into<String>) -> Self {
        Self::new(SupportStatementKind::PatternLift, statement, Vec::new())
    }

    pub fn impact_hypothesis(statement: impl Into<String>) -> Self {
        Self::new(
            SupportStatementKind::ImpactHypothesis,
            statement,
            Vec::new(),
        )
    }

    pub fn missing_status(statement: impl Into<String>) -> Self {
        Self::new(SupportStatementKind::MissingStatus, statement, Vec::new())
    }

    pub fn bucket(&self) -> StatementBucket {
        match self.kind {
            SupportStatementKind::EventFact => {
                if self.source_refs.is_empty() {
                    StatementBucket::Unknown
                } else {
                    StatementBucket::Fact
                }
            }
            SupportStatementKind::PatternSimilarity
            | SupportStatementKind::PatternLift
            | SupportStatementKind::OtherCalculation => StatementBucket::Calculation,
            SupportStatementKind::ImpactHypothesis | SupportStatementKind::OtherInference => {
                StatementBucket::Inference
            }
            SupportStatementKind::MissingStatus | SupportStatementKind::OtherUnknown => {
                StatementBucket::Unknown
            }
            SupportStatementKind::OtherFact => StatementBucket::Fact,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DecisionSupportConfig {
    pub support_version: String,
    pub market_snapshot_version: String,
    pub persist_run: bool,
    pub event_score_enabled: bool,
    pub event_score_limit: f64,
}

impl From<&crate::config::Config> for DecisionSupportConfig {
    fn from(config: &crate::config::Config) -> Self {
        Self {
            event_score_enabled: config.enable_event_score_adjustment,
            event_score_limit: config.max_event_score_adjustment.clamp(0.0, 5.0),
            ..Self::default()
        }
    }
}

impl Default for DecisionSupportConfig {
    fn default() -> Self {
        Self {
            support_version: DECISION_SUPPORT_VERSION.to_string(),
            market_snapshot_version: crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION
                .to_string(),
            persist_run: false,
            event_score_enabled: false,
            event_score_limit: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DecisionSupportConfig;
    use crate::config::Config;

    #[test]
    fn decision_support_config_default_disables_event_score_adjustment_budget() {
        let config = DecisionSupportConfig::default();

        assert!(!config.event_score_enabled);
        assert_eq!(config.event_score_limit, 0.0);
    }

    #[test]
    fn decision_support_config_from_env_config_clamps_event_score_limit() {
        let config = Config {
            tushare_token: "test".to_string(),
            database_url: "postgresql://qbot:qbot@127.0.0.1/qbot".to_string(),
            redis_url: "redis://127.0.0.1:6379".to_string(),
            telegram_bot_token: "test".to_string(),
            telegram_webhook_secret: None,
            webhook_url: None,
            stock_alert_channel: None,
            report_channel: None,
            daban_channel: None,
            api_port: 8080,
            api_key: None,
            dashboard_public_url: None,
            dashboard_username: None,
            dashboard_password_hash: None,
            dashboard_session_secret: None,
            ai_api_key: None,
            ai_base_url: "http://localhost".to_string(),
            ai_model: "gpt-4o-mini".to_string(),
            data_proxy: None,
            official_event_feed_url: None,
            official_event_feed_api_key: None,
            official_event_source_id: "official:market_event".to_string(),
            official_event_store_full_content: false,
            enable_gdelt_events: false,
            gdelt_event_query: String::new(),
            gdelt_max_records: 250,
            enable_burst_monitor: false,
            enable_daban_live: false,
            enable_ai_analysis: false,
            enable_chip_dist: false,
            enable_event_score_adjustment: true,
            max_event_score_adjustment: 8.0,
            enable_signal_auto_trading: false,
        };

        let support_config = DecisionSupportConfig::from(&config);

        assert!(support_config.event_score_enabled);
        assert_eq!(support_config.event_score_limit, 5.0);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MarketSummary {
    pub trade_date: NaiveDate,
    pub snapshot_version: String,
    pub available_at: DateTime<Utc>,
    pub data_complete: bool,
    pub metrics: Value,
    pub missing_inputs: Vec<String>,
    pub input_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DataStatus {
    pub requested_trade_date: NaiveDate,
    pub latest_trade_date: Option<NaiveDate>,
    pub snapshot_version: String,
    pub available_at: Option<DateTime<Utc>>,
    pub data_complete: bool,
    pub missing_inputs: Vec<String>,
    pub input_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EventScoreAdjustmentAudit {
    pub event_id: Uuid,
    pub entity_relation: String,
    pub market_alignment: Option<f64>,
    pub causal_confidence: Option<f64>,
    pub raw_adjustment: f64,
    pub applied_adjustment: f64,
    pub cap: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DecisionCandidate {
    pub code: String,
    pub name: String,
    pub horizon: String,
    pub base_source: String,
    pub base_score: f64,
    pub pattern_score: Option<f64>,
    pub event_adjustment: f64,
    pub risk_adjustment: f64,
    pub final_score: f64,
    pub support_tier: String,
    pub facts: Vec<SupportStatement>,
    pub calculations: Vec<SupportStatement>,
    pub inferences: Vec<SupportStatement>,
    pub unknowns: Vec<SupportStatement>,
    pub risk_flags: Vec<String>,
    pub invalidations: Vec<String>,
    #[serde(default)]
    pub event_score_audit: Vec<EventScoreAdjustmentAudit>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DailyDecisionSupport {
    pub trade_date: NaiveDate,
    pub run_id: Uuid,
    pub candidates: Vec<DecisionCandidate>,
    pub market_summary: MarketSummary,
    pub event_summary: Option<DailyEventBrief>,
    pub data_status: DataStatus,
}
