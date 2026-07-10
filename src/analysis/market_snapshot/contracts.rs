use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityQuality {
    Observed,
    Estimated,
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTimeContext {
    pub trade_date: NaiveDate,
    pub as_of: DateTime<Utc>,
}

impl PointInTimeContext {
    pub fn can_use(&self, available_at: DateTime<Utc>) -> bool {
        available_at <= self.as_of
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdjustmentFactor {
    pub code: String,
    pub trade_date: NaiveDate,
    pub adj_factor: f64,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub availability_quality: AvailabilityQuality,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityDailyStatus {
    pub code: String,
    pub trade_date: NaiveDate,
    pub listed_days: Option<i32>,
    pub is_st: bool,
    pub is_suspended: bool,
    pub price_limit_pct: Option<f64>,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub availability_quality: AvailabilityQuality,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDailyBar {
    pub code: String,
    pub trade_date: NaiveDate,
    pub close: f64,
    pub change_pct: Option<f64>,
    pub volume: Option<i64>,
    pub amount: Option<f64>,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub availability_quality: AvailabilityQuality,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SectorMembership {
    pub code: String,
    pub sector_code: String,
    pub sector_name: String,
    pub sector_type: String,
    pub valid_from: NaiveDate,
    pub valid_to: Option<NaiveDate>,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub availability_quality: AvailabilityQuality,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyBasicSnapshot {
    pub code: String,
    pub trade_date: NaiveDate,
    pub turnover_rate: Option<f64>,
    pub volume_ratio: Option<f64>,
    pub pe: Option<f64>,
    pub pb: Option<f64>,
    pub ps: Option<f64>,
    pub total_share: Option<f64>,
    pub float_share: Option<f64>,
    pub total_mv: Option<f64>,
    pub circ_mv: Option<f64>,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub availability_quality: AvailabilityQuality,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityMasterVersion {
    pub code: String,
    pub name: String,
    pub market: Option<String>,
    pub exchange: Option<String>,
    pub list_status: String,
    pub list_date: Option<NaiveDate>,
    pub delist_date: Option<NaiveDate>,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub availability_quality: AvailabilityQuality,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorporateAction {
    pub source: String,
    pub action_key: String,
    pub code: String,
    pub action_type: String,
    pub announcement_date: Option<NaiveDate>,
    pub record_date: Option<NaiveDate>,
    pub ex_date: Option<NaiveDate>,
    pub pay_date: Option<NaiveDate>,
    pub cash_dividend: Option<f64>,
    pub stock_ratio: Option<f64>,
    pub rights_ratio: Option<f64>,
    pub rights_price: Option<f64>,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub availability_quality: AvailabilityQuality,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub trade_date: NaiveDate,
    pub snapshot_version: String,
    pub available_at: DateTime<Utc>,
    pub data_complete: bool,
    pub metrics: serde_json::Value,
    pub missing_inputs: Vec<String>,
    pub input_fingerprint: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn point_in_time_context_rejects_data_available_after_cutoff() {
        let ctx = PointInTimeContext {
            trade_date: chrono::NaiveDate::from_ymd_opt(2026, 7, 10).unwrap(),
            as_of: Utc.with_ymd_and_hms(2026, 7, 10, 9, 0, 0).unwrap(),
        };
        let available_at = Utc.with_ymd_and_hms(2026, 7, 10, 10, 0, 0).unwrap();

        assert!(!ctx.can_use(available_at));
    }
}
