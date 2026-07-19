use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FinancialFrequency {
    Annual,
    Quarterly,
}

impl FinancialFrequency {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Annual => "annual",
            Self::Quarterly => "quarterly",
        }
    }

    pub fn from_storage(value: &str) -> Option<Self> {
        match value {
            "annual" => Some(Self::Annual),
            "quarterly" => Some(Self::Quarterly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinancialReport {
    pub source: String,
    pub code: String,
    pub end_date: NaiveDate,
    pub announcement_date: Option<NaiveDate>,
    pub report_type: String,
    pub frequency: FinancialFrequency,
    pub source_revision: String,
    pub total_revenue: Option<f64>,
    pub revenue: Option<f64>,
    pub operating_profit: Option<f64>,
    pub total_profit: Option<f64>,
    pub net_profit_parent: Option<f64>,
    pub deducted_net_profit: Option<f64>,
    pub basic_eps: Option<f64>,
    pub diluted_eps: Option<f64>,
    pub roe: Option<f64>,
    pub gross_margin: Option<f64>,
    pub net_margin: Option<f64>,
    pub revenue_yoy: Option<f64>,
    pub net_profit_yoy: Option<f64>,
    pub raw_payload: Value,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DividendRecord {
    pub source: String,
    pub action_key: String,
    pub code: String,
    pub announcement_date: Option<NaiveDate>,
    pub record_date: Option<NaiveDate>,
    pub ex_date: Option<NaiveDate>,
    pub pay_date: Option<NaiveDate>,
    pub implementation_status: String,
    pub cash_dividend: Option<f64>,
    pub cash_dividend_tax: Option<f64>,
    pub stock_ratio: Option<f64>,
    pub source_revision: String,
    pub raw_payload: Value,
    pub available_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
}
