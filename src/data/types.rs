use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

/// A-share stock info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StockInfo {
    pub code: String,      // e.g. "000001.SZ"
    pub name: String,      // e.g. "平安银行"
    pub market: String,    // SH / SZ
    pub industry: Option<String>,
}

/// Daily OHLCV candle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub trade_date: NaiveDate,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,        // shares
    pub amount: f64,        // yuan
    pub turnover: Option<f64>,  // % from daily_basic
    pub pe: Option<f64>,
    pub pb: Option<f64>,
}

/// Real-time quote (from Sina)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub code: String,
    pub name: String,
    pub price: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub prev_close: f64,
    pub change_pct: f64,
    pub volume: i64,
    pub amount: f64,
    pub timestamp: chrono::NaiveDateTime,
}

/// Limit-up stock from Tushare limit_list_d
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitUpStock {
    pub code: String,
    pub name: String,
    pub trade_date: NaiveDate,
    pub close: f64,
    pub pct_chg: f64,
    pub fd_amount: f64,     // 封单额 (seal amount)
    pub first_time: Option<String>,  // 首次涨停时间
    pub last_time: Option<String>,   // 最后涨停时间
    pub open_times: i32,    // 打开次数 (burst count)
    pub strth: f64,         // 涨停强度
    pub limit: String,      // U=涨停 D=跌停
}

/// Sector data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SectorData {
    pub code: String,       // e.g. "BK0477"
    pub name: String,       // e.g. "半导体"
    pub sector_type: String, // industry / concept
    pub change_pct: f64,
    pub amount: f64,
    pub trade_date: NaiveDate,
}

/// Market index data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexData {
    pub code: String,       // e.g. "sh000001"
    pub name: String,
    pub trade_date: NaiveDate,
    pub close: f64,
    pub change_pct: f64,
    pub volume: i64,
    pub amount: f64,
}
