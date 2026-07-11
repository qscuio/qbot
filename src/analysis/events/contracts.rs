use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc, Weekday};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::Result;
use crate::market_time;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManualEventInput {
    pub title: String,
    pub content: Option<String>,
    pub source_url: Option<String>,
    pub submitted_by: String,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventEvidence {
    pub evidence_id: Uuid,
    pub source_id: String,
    pub source_item_id: String,
    pub source_tier: String,
    pub published_at: Option<DateTime<Utc>>,
    pub first_seen_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub effective_trade_date: NaiveDate,
    pub title: String,
    pub content_hash: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExistingEventEvidenceRelation {
    pub submitted: EventEvidence,
    pub existing: EventEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ManualEventSubmissionOutcome {
    Inserted(EventEvidence),
    Existing(ExistingEventEvidenceRelation),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BriefFact {
    pub fact_id: Uuid,
    pub summary: String,
    pub evidence_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BriefRevision {
    pub revision_id: Uuid,
    pub previous_fact_id: Uuid,
    pub summary: String,
    pub evidence_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BriefUnconfirmed {
    pub item_id: Uuid,
    pub summary: String,
    pub evidence_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BriefEntity {
    pub entity_id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BriefSource {
    pub evidence_id: Uuid,
    pub source_id: String,
    pub source_item_id: String,
    pub published_at: Option<DateTime<Utc>>,
    pub available_at: DateTime<Utc>,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DailyEventBrief {
    pub trade_date: NaiveDate,
    pub new_facts: Vec<BriefFact>,
    pub revisions: Vec<BriefRevision>,
    pub unconfirmed: Vec<BriefUnconfirmed>,
    pub direct_entities: Vec<BriefEntity>,
    pub sources: Vec<BriefSource>,
    pub input_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventProcessingSummary {
    pub cutoff: DateTime<Utc>,
    pub pending_evidence_count: usize,
    pub processed_evidence_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventListItem {
    pub evidence_id: Uuid,
    pub source_id: String,
    pub source_item_id: String,
    pub source_url: Option<String>,
    pub source_tier: String,
    pub published_at: Option<DateTime<Utc>>,
    pub first_seen_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub effective_trade_date: NaiveDate,
    pub title: String,
    pub content: Option<String>,
    pub processing_status: String,
    pub version: i32,
    pub supersedes_evidence_id: Option<Uuid>,
    pub source_readable: Option<bool>,
    pub manual_review_needed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventDetail {
    pub evidence_id: Uuid,
    pub source_id: String,
    pub source_item_id: String,
    pub source_url: Option<String>,
    pub source_tier: String,
    pub source_terms_version: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub published_at: Option<DateTime<Utc>>,
    pub first_seen_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub effective_trade_date: NaiveDate,
    pub title: String,
    pub content: Option<String>,
    pub language: String,
    pub content_hash: String,
    pub processing_status: String,
    pub version: i32,
    pub supersedes_evidence_id: Option<Uuid>,
    pub source_readable: Option<bool>,
    pub manual_review_needed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersistedDailyEventBrief {
    pub trade_date: NaiveDate,
    pub brief_version: String,
    pub content: String,
    pub structured_payload: Value,
    pub input_fingerprint: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventReviewResult {
    pub evidence_id: Uuid,
    pub supersedes_evidence_id: Uuid,
    pub source_item_id: String,
    pub processing_status: String,
    pub effective_trade_date: NaiveDate,
    pub version: i32,
    pub reviewed_by: String,
}

pub trait TradingDateResolver: Send + Sync {
    fn effective_trade_date(&self, available_at: DateTime<Utc>) -> Result<NaiveDate>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AShareTradingDateResolver;

impl TradingDateResolver for AShareTradingDateResolver {
    fn effective_trade_date(&self, available_at: DateTime<Utc>) -> Result<NaiveDate> {
        let beijing_time = available_at.with_timezone(&market_time::beijing_tz());
        let mut trade_date = beijing_time.date_naive();

        if matches!(beijing_time.weekday(), Weekday::Sat | Weekday::Sun) {
            trade_date = next_open_weekday(trade_date);
        } else if is_after_cash_close(beijing_time) {
            trade_date = next_open_weekday(trade_date.succ_opt().unwrap_or(trade_date));
        }

        Ok(trade_date)
    }
}

fn is_after_cash_close(available_at: DateTime<chrono::FixedOffset>) -> bool {
    available_at.hour() > 15 || (available_at.hour() == 15 && available_at.minute() > 0)
}

fn next_open_weekday(mut date: NaiveDate) -> NaiveDate {
    while matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
        date = date.succ_opt().unwrap_or(date);
    }

    date
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use chrono::{NaiveDate, TimeZone, Utc};

    use super::{AShareTradingDateResolver, TradingDateResolver};

    #[test]
    fn effective_trade_date_keeps_same_day_during_trading_hours() {
        let resolver = AShareTradingDateResolver;
        let available_at = Utc.with_ymd_and_hms(2026, 7, 10, 6, 30, 0).unwrap();

        let trade_date = resolver.effective_trade_date(available_at).unwrap();

        assert_eq!(trade_date, NaiveDate::from_ymd_opt(2026, 7, 10).unwrap());
    }

    #[test]
    fn effective_trade_date_rolls_after_close_to_next_trading_day() {
        let resolver = AShareTradingDateResolver;
        let available_at = Utc.with_ymd_and_hms(2026, 7, 10, 7, 30, 0).unwrap();

        let trade_date = resolver.effective_trade_date(available_at).unwrap();

        assert_eq!(trade_date, NaiveDate::from_ymd_opt(2026, 7, 13).unwrap());
    }

    #[test]
    fn effective_trade_date_rolls_weekends_to_next_open_date() {
        let resolver = AShareTradingDateResolver;
        let available_at = Utc.with_ymd_and_hms(2026, 7, 11, 2, 0, 0).unwrap();

        let trade_date = resolver.effective_trade_date(available_at).unwrap();

        assert_eq!(trade_date, NaiveDate::from_ymd_opt(2026, 7, 13).unwrap());
    }

    #[test]
    fn existing_event_evidence_relation_public_contract_hides_duplicate_group_id() {
        let contracts_source = fs::read_to_string(contracts_source_path()).unwrap();
        let struct_body = contracts_source
            .split("pub struct ExistingEventEvidenceRelation {")
            .nth(1)
            .and_then(|body| body.split('}').next())
            .expect("existing relation struct body");

        assert!(!struct_body.contains("duplicate_group_id"));
    }

    fn contracts_source_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(file!())
    }
}
