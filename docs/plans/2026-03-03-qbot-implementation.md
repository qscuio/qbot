# qbot Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build qbot — a Rust-based A-share stock analysis and reporting system with Telegram channel push, modular signal detection, and GitHub Actions deployment.

**Architecture:** Layered Rust service using Tokio async runtime. Tushare REST API as primary data source, Sina Finance for real-time intraday quotes. PostgreSQL for persistence, Redis for intraday caching. All services boot from `main.rs` and are driven by a cron scheduler plus an Axum REST API.

**Tech Stack:** Rust 1.80, Tokio, Axum, SQLx (PostgreSQL), Redis, Reqwest, tokio-cron-scheduler, Serde, Chrono, Tracing

**Cargo binary:** `/usr/lib/rust-1.80/bin/cargo`

---

## Phase 1: Foundation

### Task 1: Initialize Cargo project and dependencies

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`

**Step 1: Initialize project**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cd /home/ubuntu/work/qbot
cargo init --name qbot
```

Expected: `Created binary (application) package`

**Step 2: Replace Cargo.toml with full dependencies**

```toml
[package]
name = "qbot"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "qbot"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["full"] }
axum = { version = "0.7", features = ["json"] }
tower = "0.4"
tower-http = { version = "0.5", features = ["cors", "trace"] }
sqlx = { version = "0.7", features = ["runtime-tokio-native-tls", "postgres", "chrono", "uuid", "json", "migrate"] }
redis = { version = "0.25", features = ["tokio-comp", "connection-manager"] }
reqwest = { version = "0.11", features = ["json"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio-cron-scheduler = { version = "0.10", features = ["signal"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
anyhow = "1"
thiserror = "1"
chrono = { version = "0.4", features = ["serde"] }
dotenvy = "0.15"
uuid = { version = "1", features = ["v4", "serde"] }
async-trait = "0.1"

[dev-dependencies]
tokio-test = "0.4"
```

**Step 3: Verify it compiles**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cd /home/ubuntu/work/qbot
cargo check
```

Expected: `Finished dev [unoptimized + debuginfo]`

**Step 4: Commit**

```bash
git add Cargo.toml src/main.rs
git commit -m "feat: initialize qbot Rust project with dependencies"
```

---

### Task 2: Config module

**Files:**
- Create: `src/config.rs`
- Modify: `src/main.rs`
- Create: `.env.example`

**Step 1: Write test**

```rust
// src/config.rs (bottom of file, added after implementation)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        // Only TUSHARE_TOKEN and TELEGRAM_BOT_TOKEN are required
        // DATABASE_URL and REDIS_URL have internal defaults
        std::env::set_var("TUSHARE_TOKEN", "test_token");
        std::env::set_var("TELEGRAM_BOT_TOKEN", "123:abc");
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("REDIS_URL");

        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.tushare_token, "test_token");
        assert_eq!(cfg.database_url, "postgresql://qbot:qbot@127.0.0.1/qbot");
        assert_eq!(cfg.redis_url, "redis://127.0.0.1:6379");
        assert_eq!(cfg.api_port, 8080); // default
    }
}
```

**Step 2: Run to verify it fails**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test config -- --nocapture
```

Expected: compile error (Config not defined)

**Step 3: Implement config**

```rust
// src/config.rs
use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    // Tushare
    pub tushare_token: String,

    // Database
    pub database_url: String,
    pub redis_url: String,

    // Telegram
    pub telegram_bot_token: String,
    pub stock_alert_channel: Option<String>,
    pub report_channel: Option<String>,
    pub daban_channel: Option<String>,

    // API
    pub api_port: u16,
    pub api_key: Option<String>,

    // AI analysis (optional)
    pub ai_provider: Option<String>,
    pub ai_api_key: Option<String>,

    // Data proxy (optional)
    pub data_proxy: Option<String>,

    // Feature flags
    pub enable_burst_monitor: bool,
    pub enable_trading_sim: bool,
    pub enable_daban_sim: bool,
    pub enable_ai_analysis: bool,
    pub enable_chip_dist: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        Ok(Config {
            tushare_token: std::env::var("TUSHARE_TOKEN")
                .context("TUSHARE_TOKEN is required")?,
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://qbot:qbot@127.0.0.1/qbot".to_string()),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            telegram_bot_token: std::env::var("TELEGRAM_BOT_TOKEN")
                .context("TELEGRAM_BOT_TOKEN is required")?,
            stock_alert_channel: std::env::var("STOCK_ALERT_CHANNEL").ok(),
            report_channel: std::env::var("REPORT_CHANNEL").ok(),
            daban_channel: std::env::var("DABAN_CHANNEL").ok(),
            api_port: std::env::var("API_PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .unwrap_or(8080),
            api_key: std::env::var("API_KEY").ok(),
            ai_provider: std::env::var("AI_PROVIDER").ok(),
            ai_api_key: std::env::var("AI_API_KEY").ok(),
            data_proxy: std::env::var("DATA_PROXY").ok(),
            enable_burst_monitor: std::env::var("ENABLE_BURST_MONITOR")
                .unwrap_or_else(|_| "true".to_string()) == "true",
            enable_trading_sim: std::env::var("ENABLE_TRADING_SIM")
                .unwrap_or_else(|_| "false".to_string()) == "true",
            enable_daban_sim: std::env::var("ENABLE_DABAN_SIM")
                .unwrap_or_else(|_| "false".to_string()) == "true",
            enable_ai_analysis: std::env::var("ENABLE_AI_ANALYSIS")
                .unwrap_or_else(|_| "false".to_string()) == "true",
            enable_chip_dist: std::env::var("ENABLE_CHIP_DIST")
                .unwrap_or_else(|_| "true".to_string()) == "true",
        })
    }
}
```

**Step 4: Add .env.example**

```bash
# .env.example
TUSHARE_TOKEN=your_tushare_token_here

# Internal services — pre-filled, no need to change (matches deploy/docker-compose.yml)
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1/qbot
REDIS_URL=redis://127.0.0.1:6379

TELEGRAM_BOT_TOKEN=your_bot_token_here
STOCK_ALERT_CHANNEL=-1001234567890
REPORT_CHANNEL=-1001234567890
DABAN_CHANNEL=-1001234567890

API_PORT=8080
API_KEY=your_api_key_here

# Optional AI analysis
AI_PROVIDER=claude
AI_API_KEY=your_ai_api_key

# Optional proxy for data fetching
DATA_PROXY=

# Feature flags (true/false)
ENABLE_BURST_MONITOR=true
ENABLE_TRADING_SIM=false
ENABLE_DABAN_SIM=false
ENABLE_AI_ANALYSIS=false
ENABLE_CHIP_DIST=true
```

**Step 5: Run tests**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test config
```

Expected: `test config::tests::test_config_defaults ... ok`

**Step 6: Commit**

```bash
git add src/config.rs .env.example
git commit -m "feat: add Config module with env var loading"
```

---

### Task 3: Error types and AppState

**Files:**
- Create: `src/error.rs`
- Create: `src/state.rs`

**Step 1: Implement error types**

```rust
// src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Data provider error: {0}")]
    DataProvider(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, AppError>;

// Axum response integration
impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        use axum::Json;
        use serde_json::json;

        let (status, message) = match &self {
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::Config(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}
```

**Step 2: Implement AppState**

```rust
// src/state.rs
use std::sync::Arc;
use crate::config::Config;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: sqlx::PgPool,
    pub redis: redis::aio::ConnectionManager,
}
```

**Step 3: Compile check**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
```

**Step 4: Commit**

```bash
git add src/error.rs src/state.rs
git commit -m "feat: add AppError, Result alias, and AppState"
```

---

### Task 4: Logging and main.rs scaffold

**Files:**
- Modify: `src/main.rs`

**Step 1: Write main.rs boot scaffold**

```rust
// src/main.rs
mod config;
mod error;
mod state;

use anyhow::Result;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "qbot=info,sqlx=warn".into()),
        )
        .init();

    info!("🚀 qbot starting...");

    let config = config::Config::from_env()?;
    info!("✅ Config loaded");

    // Connect to PostgreSQL
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;
    info!("✅ PostgreSQL connected");

    // Connect to Redis
    let redis_client = redis::Client::open(config.redis_url.as_str())?;
    let redis = redis::aio::ConnectionManager::new(redis_client).await?;
    info!("✅ Redis connected");

    let state = state::AppState {
        config: Arc::new(config),
        db,
        redis,
    };

    info!("✅ qbot ready");

    // Keep alive until signal
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    Ok(())
}
```

**Step 2: Verify compile**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
```

**Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: add main.rs boot scaffold with DB/Redis init"
```

---

## Phase 2: Data Layer

### Task 5: Shared data types

**Files:**
- Create: `src/data/mod.rs`
- Create: `src/data/types.rs`

**Step 1: Create data module**

```bash
mkdir -p src/data
```

**Step 2: Implement types**

```rust
// src/data/types.rs
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
```

**Step 3: Create mod.rs**

```rust
// src/data/mod.rs
pub mod types;
pub mod provider;
pub mod tushare;
pub mod sina;

pub use types::*;
```

**Step 4: Add to main.rs**

```rust
// In src/main.rs, add at top:
mod data;
```

**Step 5: Compile check**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
```

**Step 6: Commit**

```bash
git add src/data/
git commit -m "feat: add data types (StockInfo, Candle, Quote, LimitUpStock, SectorData)"
```

---

### Task 6: DataProvider trait

**Files:**
- Create: `src/data/provider.rs`

**Step 1: Implement trait**

```rust
// src/data/provider.rs
use async_trait::async_trait;
use chrono::NaiveDate;
use std::collections::HashMap;
use crate::error::Result;
use super::types::*;

#[async_trait]
pub trait DataProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Fetch full A-share stock universe
    async fn get_stock_list(&self) -> Result<Vec<StockInfo>>;

    /// Fetch OHLCV for all stocks on a specific trading date (backfill use)
    async fn get_daily_bars_by_date(&self, trade_date: NaiveDate) -> Result<Vec<(String, Candle)>>;

    /// Fetch OHLCV for a specific stock over a date range (daily update use)
    async fn get_daily_bars_for_stock(
        &self,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Candle>>;

    /// Fetch trading calendar
    async fn get_trading_dates(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>>;

    /// Fetch limit-up/down stocks for a date
    async fn get_limit_up_stocks(&self, trade_date: NaiveDate) -> Result<Vec<LimitUpStock>>;

    /// Fetch index daily bars
    async fn get_index_daily(&self, code: &str, trade_date: NaiveDate) -> Result<Option<IndexData>>;

    /// Fetch sector performance for a date
    async fn get_sector_data(&self, trade_date: NaiveDate) -> Result<Vec<SectorData>>;
}
```

**Step 2: Compile check**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
```

**Step 3: Commit**

```bash
git add src/data/provider.rs
git commit -m "feat: add DataProvider async trait"
```

---

### Task 7: Tushare HTTP client

**Files:**
- Create: `src/data/tushare.rs`

**Step 1: Write test**

```rust
// At bottom of src/data/tushare.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tushare_code_convert() {
        let client = TushareClient::new("test".to_string(), None);
        assert_eq!(client.to_sina_code("000001.SZ"), "sz000001");
        assert_eq!(client.to_sina_code("600036.SH"), "sh600036");
    }
}
```

**Step 2: Implement TushareClient**

```rust
// src/data/tushare.rs
use anyhow::Context;
use async_trait::async_trait;
use chrono::NaiveDate;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{debug, error, warn};

use crate::data::provider::DataProvider;
use crate::data::types::*;
use crate::error::{AppError, Result};

const TUSHARE_URL: &str = "https://api.tushare.pro";

pub struct TushareClient {
    token: String,
    client: Client,
}

impl TushareClient {
    pub fn new(token: String, proxy: Option<&str>) -> Self {
        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(30));

        if let Some(proxy_url) = proxy {
            if let Ok(proxy) = reqwest::Proxy::all(proxy_url) {
                builder = builder.proxy(proxy);
            }
        }

        TushareClient {
            token,
            client: builder.build().unwrap_or_default(),
        }
    }

    /// Convert Tushare code (000001.SZ) to Sina code (sz000001)
    pub fn to_sina_code(&self, tushare_code: &str) -> String {
        if let Some(parts) = tushare_code.split_once('.') {
            let (num, market) = parts;
            match market {
                "SH" => format!("sh{}", num),
                "SZ" => format!("sz{}", num),
                _ => tushare_code.to_lowercase().replace('.', ""),
            }
        } else {
            tushare_code.to_string()
        }
    }

    async fn call(&self, api_name: &str, params: Value, fields: &str) -> Result<Value> {
        let body = json!({
            "api_name": api_name,
            "token": self.token,
            "params": params,
            "fields": fields
        });

        let resp = self
            .client
            .post(TUSHARE_URL)
            .json(&body)
            .send()
            .await
            .context("Tushare HTTP request failed")
            .map_err(|e| AppError::Http(e.downcast::<reqwest::Error>()
                .unwrap_or_else(|e| panic!("{}", e))))?;

        let json: Value = resp.json().await
            .map_err(AppError::Http)?;

        if json["code"].as_i64().unwrap_or(-1) != 0 {
            let msg = json["msg"].as_str().unwrap_or("unknown error");
            return Err(AppError::DataProvider(format!("Tushare {}: {}", api_name, msg)));
        }

        Ok(json["data"].clone())
    }

    fn parse_date(s: &str) -> Option<NaiveDate> {
        NaiveDate::parse_from_str(s, "%Y%m%d").ok()
    }

    fn safe_f64(v: &Value) -> f64 {
        match v {
            Value::Number(n) => n.as_f64().unwrap_or(0.0),
            Value::String(s) => s.parse().unwrap_or(0.0),
            _ => 0.0,
        }
    }

    fn safe_i64(v: &Value) -> i64 {
        match v {
            Value::Number(n) => n.as_i64().unwrap_or(0),
            Value::String(s) => s.parse().unwrap_or(0),
            _ => 0,
        }
    }
}

#[async_trait]
impl DataProvider for TushareClient {
    fn name(&self) -> &'static str {
        "tushare"
    }

    async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
        let data = self
            .call(
                "stock_basic",
                json!({ "exchange": "", "list_status": "L" }),
                "ts_code,symbol,name,market,industry",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| -> usize {
            fields.iter().position(|f| f.as_str() == Some(name)).unwrap_or(999)
        };
        let i_code = idx("ts_code");
        let i_name = idx("name");
        let i_market = idx("market");
        let i_industry = idx("industry");

        let stocks = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(StockInfo {
                    code: arr.get(i_code)?.as_str()?.to_string(),
                    name: arr.get(i_name)?.as_str()?.to_string(),
                    market: arr.get(i_market)?.as_str().unwrap_or("").to_string(),
                    industry: arr.get(i_industry).and_then(|v| v.as_str()).map(|s| s.to_string()),
                })
            })
            .collect();

        Ok(stocks)
    }

    async fn get_daily_bars_by_date(&self, trade_date: NaiveDate) -> Result<Vec<(String, Candle)>> {
        let date_str = trade_date.format("%Y%m%d").to_string();

        let data = self
            .call(
                "daily",
                json!({ "trade_date": date_str }),
                "ts_code,trade_date,open,high,low,close,vol,amount",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| fields.iter().position(|f| f.as_str() == Some(name)).unwrap_or(999);
        let (i_code, i_date, i_open, i_high, i_low, i_close, i_vol, i_amt) =
            (idx("ts_code"), idx("trade_date"), idx("open"), idx("high"),
             idx("low"), idx("close"), idx("vol"), idx("amount"));

        let bars = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                let code = arr.get(i_code)?.as_str()?.to_string();
                let date = Self::parse_date(arr.get(i_date)?.as_str()?)?;
                Some((code, Candle {
                    trade_date: date,
                    open: Self::safe_f64(arr.get(i_open)?),
                    high: Self::safe_f64(arr.get(i_high)?),
                    low: Self::safe_f64(arr.get(i_low)?),
                    close: Self::safe_f64(arr.get(i_close)?),
                    volume: Self::safe_i64(arr.get(i_vol)?) * 100, // Tushare: lots -> shares
                    amount: Self::safe_f64(arr.get(i_amt)?) * 1000.0, // Tushare: thousands
                    turnover: None,
                    pe: None,
                    pb: None,
                }))
            })
            .collect();

        Ok(bars)
    }

    async fn get_daily_bars_for_stock(
        &self,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Candle>> {
        let data = self
            .call(
                "daily",
                json!({
                    "ts_code": code,
                    "start_date": start_date.format("%Y%m%d").to_string(),
                    "end_date": end_date.format("%Y%m%d").to_string(),
                }),
                "trade_date,open,high,low,close,vol,amount",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| fields.iter().position(|f| f.as_str() == Some(name)).unwrap_or(999);
        let (i_date, i_open, i_high, i_low, i_close, i_vol, i_amt) =
            (idx("trade_date"), idx("open"), idx("high"), idx("low"),
             idx("close"), idx("vol"), idx("amount"));

        let mut bars: Vec<Candle> = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(Candle {
                    trade_date: Self::parse_date(arr.get(i_date)?.as_str()?)?,
                    open: Self::safe_f64(arr.get(i_open)?),
                    high: Self::safe_f64(arr.get(i_high)?),
                    low: Self::safe_f64(arr.get(i_low)?),
                    close: Self::safe_f64(arr.get(i_close)?),
                    volume: Self::safe_i64(arr.get(i_vol)?) * 100,
                    amount: Self::safe_f64(arr.get(i_amt)?) * 1000.0,
                    turnover: None,
                    pe: None,
                    pb: None,
                })
            })
            .collect();

        // Sort ascending by date
        bars.sort_by_key(|b| b.trade_date);
        Ok(bars)
    }

    async fn get_trading_dates(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
        let data = self
            .call(
                "trade_cal",
                json!({
                    "exchange": "SSE",
                    "start_date": start.format("%Y%m%d").to_string(),
                    "end_date": end.format("%Y%m%d").to_string(),
                    "is_open": "1"
                }),
                "cal_date",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let dates = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Self::parse_date(arr.first()?.as_str()?)
            })
            .collect();

        Ok(dates)
    }

    async fn get_limit_up_stocks(&self, trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        let date_str = trade_date.format("%Y%m%d").to_string();
        let data = self
            .call(
                "limit_list_d",
                json!({ "trade_date": date_str, "limit_type": "U" }),
                "ts_code,name,trade_date,close,pct_chg,fd_amount,first_time,last_time,open_times,strth,limit",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| fields.iter().position(|f| f.as_str() == Some(name)).unwrap_or(999);
        let (i_code, i_name, i_date, i_close, i_pct, i_fd, i_first, i_last, i_open, i_strth, i_limit) = (
            idx("ts_code"), idx("name"), idx("trade_date"), idx("close"), idx("pct_chg"),
            idx("fd_amount"), idx("first_time"), idx("last_time"), idx("open_times"), idx("strth"), idx("limit"),
        );

        let stocks = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(LimitUpStock {
                    code: arr.get(i_code)?.as_str()?.to_string(),
                    name: arr.get(i_name)?.as_str().unwrap_or("").to_string(),
                    trade_date: Self::parse_date(arr.get(i_date)?.as_str()?)?,
                    close: Self::safe_f64(arr.get(i_close)?),
                    pct_chg: Self::safe_f64(arr.get(i_pct)?),
                    fd_amount: Self::safe_f64(arr.get(i_fd)?),
                    first_time: arr.get(i_first).and_then(|v| v.as_str()).map(|s| s.to_string()),
                    last_time: arr.get(i_last).and_then(|v| v.as_str()).map(|s| s.to_string()),
                    open_times: Self::safe_i64(arr.get(i_open)?) as i32,
                    strth: Self::safe_f64(arr.get(i_strth)?),
                    limit: arr.get(i_limit).and_then(|v| v.as_str()).unwrap_or("U").to_string(),
                })
            })
            .collect();

        Ok(stocks)
    }

    async fn get_index_daily(&self, code: &str, trade_date: NaiveDate) -> Result<Option<IndexData>> {
        let date_str = trade_date.format("%Y%m%d").to_string();
        let data = self
            .call(
                "index_daily",
                json!({ "ts_code": code, "trade_date": date_str }),
                "ts_code,trade_date,close,pct_chg,vol,amount",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| fields.iter().position(|f| f.as_str() == Some(name)).unwrap_or(999);
        let (i_code, i_date, i_close, i_pct, i_vol, i_amt) =
            (idx("ts_code"), idx("trade_date"), idx("close"), idx("pct_chg"), idx("vol"), idx("amount"));

        let names = [
            ("000001.SH", "上证指数"),
            ("399001.SZ", "深证成指"),
            ("399006.SZ", "创业板指"),
            ("000688.SH", "科创50"),
        ];
        let display_name = names.iter().find(|(c, _)| *c == code).map(|(_, n)| *n).unwrap_or(code);

        Ok(items.first().and_then(|row| {
            let arr = row.as_array()?;
            Some(IndexData {
                code: arr.get(i_code)?.as_str()?.to_string(),
                name: display_name.to_string(),
                trade_date: Self::parse_date(arr.get(i_date)?.as_str()?)?,
                close: Self::safe_f64(arr.get(i_close)?),
                change_pct: Self::safe_f64(arr.get(i_pct)?),
                volume: Self::safe_i64(arr.get(i_vol)?),
                amount: Self::safe_f64(arr.get(i_amt)?),
            })
        }))
    }

    async fn get_sector_data(&self, trade_date: NaiveDate) -> Result<Vec<SectorData>> {
        let date_str = trade_date.format("%Y%m%d").to_string();
        let data = self
            .call(
                "ths_daily",
                json!({ "trade_date": date_str }),
                "ts_code,trade_date,pct_change,turnover_rate,total_mv",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        // Note: ths_daily doesn't return name — fetch index list separately if needed
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| fields.iter().position(|f| f.as_str() == Some(name)).unwrap_or(999);
        let (i_code, i_date, i_pct, i_mv) =
            (idx("ts_code"), idx("trade_date"), idx("pct_change"), idx("total_mv"));

        let sectors = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                let code = arr.get(i_code)?.as_str()?.to_string();
                let sector_type = if code.starts_with("88") { "industry" } else { "concept" }.to_string();
                Some(SectorData {
                    name: code.clone(), // enriched later from ths_index
                    code,
                    sector_type,
                    change_pct: Self::safe_f64(arr.get(i_pct)?),
                    amount: Self::safe_f64(arr.get(i_mv)?),
                    trade_date: Self::parse_date(arr.get(i_date)?.as_str()?)?,
                })
            })
            .collect();

        Ok(sectors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tushare_code_convert() {
        let client = TushareClient::new("test".to_string(), None);
        assert_eq!(client.to_sina_code("000001.SZ"), "sz000001");
        assert_eq!(client.to_sina_code("600036.SH"), "sh600036");
    }

    #[test]
    fn test_safe_f64() {
        assert_eq!(TushareClient::safe_f64(&serde_json::json!(1.5)), 1.5);
        assert_eq!(TushareClient::safe_f64(&serde_json::json!("2.3")), 2.3);
        assert_eq!(TushareClient::safe_f64(&serde_json::json!(null)), 0.0);
    }
}
```

**Step 3: Run tests**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test tushare
```

Expected: `test data::tushare::tests::test_tushare_code_convert ... ok`

**Step 4: Commit**

```bash
git add src/data/tushare.rs src/data/mod.rs
git commit -m "feat: add TushareClient implementing DataProvider trait"
```

---

### Task 8: Sina real-time quotes client

**Files:**
- Create: `src/data/sina.rs`

**Step 1: Implement SinaClient**

```rust
// src/data/sina.rs
use chrono::NaiveDateTime;
use reqwest::Client;
use tracing::warn;
use std::collections::HashMap;

use crate::data::types::Quote;
use crate::error::{AppError, Result};

const SINA_URL: &str = "http://hq.sinajs.cn/list=";

pub struct SinaClient {
    client: Client,
}

impl SinaClient {
    pub fn new() -> Self {
        SinaClient {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Convert Tushare code (000001.SZ) to Sina code (sz000001)
    pub fn sina_code(tushare_code: &str) -> String {
        if let Some((num, market)) = tushare_code.split_once('.') {
            match market {
                "SH" => format!("sh{}", num),
                "SZ" => format!("sz{}", num),
                _ => tushare_code.to_lowercase(),
            }
        } else {
            tushare_code.to_string()
        }
    }

    /// Fetch real-time quotes for a batch of Tushare codes
    pub async fn get_quotes(&self, codes: &[&str]) -> Result<HashMap<String, Quote>> {
        if codes.is_empty() {
            return Ok(HashMap::new());
        }

        // Convert to Sina codes
        let sina_codes: Vec<String> = codes.iter().map(|c| Self::sina_code(c)).collect();
        let query = sina_codes.join(",");

        let resp = self
            .client
            .get(format!("{}{}", SINA_URL, query))
            .header("Referer", "http://finance.sina.com.cn")
            .send()
            .await
            .map_err(AppError::Http)?;

        let text = resp.text().await.map_err(AppError::Http)?;
        let mut result = HashMap::new();

        for (i, line) in text.lines().enumerate() {
            if let Some(quote) = Self::parse_line(line, codes.get(i).copied().unwrap_or("")) {
                result.insert(quote.code.clone(), quote);
            }
        }

        Ok(result)
    }

    /// Parse a Sina quote line:
    /// var hq_str_sz000001="平安银行,10.50,10.48,...,2024-01-15,15:00:00";
    fn parse_line(line: &str, tushare_code: &str) -> Option<Quote> {
        let start = line.find('"')? + 1;
        let end = line.rfind('"')?;
        let data = &line[start..end];
        let parts: Vec<&str> = data.split(',').collect();

        if parts.len() < 32 {
            return None;
        }

        let price: f64 = parts[3].parse().unwrap_or(0.0);
        let prev_close: f64 = parts[2].parse().unwrap_or(0.0);
        let change_pct = if prev_close > 0.0 {
            (price - prev_close) / prev_close * 100.0
        } else {
            0.0
        };

        let date_str = format!("{} {}", parts[30], parts[31]);
        let timestamp = NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|_| chrono::Local::now().naive_local());

        Some(Quote {
            code: tushare_code.to_string(),
            name: parts[0].to_string(),
            price,
            open: parts[1].parse().unwrap_or(0.0),
            high: parts[4].parse().unwrap_or(0.0),
            low: parts[5].parse().unwrap_or(0.0),
            prev_close,
            change_pct,
            volume: parts[8].parse::<f64>().unwrap_or(0.0) as i64,
            amount: parts[9].parse().unwrap_or(0.0),
            timestamp,
        })
    }
}

impl Default for SinaClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sina_code_convert() {
        assert_eq!(SinaClient::sina_code("000001.SZ"), "sz000001");
        assert_eq!(SinaClient::sina_code("600519.SH"), "sh600519");
    }
}
```

**Step 2: Run tests**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test sina
```

**Step 3: Commit**

```bash
git add src/data/sina.rs
git commit -m "feat: add SinaClient for real-time quote fetching"
```

---

## Phase 3: Storage Layer

### Task 9: PostgreSQL migrations

**Files:**
- Create: `migrations/001_stock_daily_bars.sql` through `migrations/009_reports.sql`

**Step 1: Create migrations directory and files**

```bash
mkdir -p migrations
```

**migrations/001_stock_daily_bars.sql:**
```sql
CREATE TABLE IF NOT EXISTS stock_daily_bars (
    code        VARCHAR(12) NOT NULL,
    trade_date  DATE NOT NULL,
    open        NUMERIC(10,3),
    high        NUMERIC(10,3),
    low         NUMERIC(10,3),
    close       NUMERIC(10,3),
    volume      BIGINT,
    amount      NUMERIC(18,2),
    turnover    NUMERIC(8,4),
    pe          NUMERIC(12,4),
    pb          NUMERIC(8,4),
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_bars_date ON stock_daily_bars(trade_date);
CREATE INDEX IF NOT EXISTS idx_bars_code ON stock_daily_bars(code);

CREATE TABLE IF NOT EXISTS stock_info (
    code        VARCHAR(12) PRIMARY KEY,
    name        VARCHAR(50) NOT NULL,
    market      VARCHAR(10),
    industry    VARCHAR(100),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
```

**migrations/002_scan_results.sql:**
```sql
CREATE TABLE IF NOT EXISTS scan_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL,
    code        VARCHAR(12) NOT NULL,
    name        VARCHAR(50),
    signal_id   VARCHAR(50) NOT NULL,
    metadata    JSONB DEFAULT '{}',
    scanned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scan_run ON scan_results(run_id);
CREATE INDEX IF NOT EXISTS idx_scan_date ON scan_results(scanned_at);
CREATE INDEX IF NOT EXISTS idx_scan_signal ON scan_results(signal_id);
```

**migrations/003_limit_up.sql:**
```sql
CREATE TABLE IF NOT EXISTS limit_up_stocks (
    code        VARCHAR(12) NOT NULL,
    trade_date  DATE NOT NULL,
    name        VARCHAR(50),
    streak      INT DEFAULT 1,
    limit_time  VARCHAR(10),
    seal_amount NUMERIC(18,2) DEFAULT 0,
    burst_count INT DEFAULT 0,
    score       NUMERIC(5,2) DEFAULT 0,
    board_type  VARCHAR(20),
    close       NUMERIC(10,3),
    pct_chg     NUMERIC(8,4),
    strth       NUMERIC(8,4),
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_limit_date ON limit_up_stocks(trade_date);
```

**migrations/004_sector_daily.sql:**
```sql
CREATE TABLE IF NOT EXISTS sector_daily (
    code        VARCHAR(20) NOT NULL,
    name        VARCHAR(100),
    sector_type VARCHAR(20),
    change_pct  NUMERIC(8,4),
    amount      NUMERIC(18,2),
    trade_date  DATE NOT NULL,
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_sector_date ON sector_daily(trade_date);
```

**migrations/005_chip_distribution.sql:**
```sql
CREATE TABLE IF NOT EXISTS chip_distribution (
    code          VARCHAR(12) NOT NULL,
    trade_date    DATE NOT NULL,
    distribution  JSONB NOT NULL DEFAULT '{}',
    avg_cost      NUMERIC(10,3),
    profit_ratio  NUMERIC(6,4),
    concentration NUMERIC(6,4),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (code, trade_date)
);
```

**migrations/006_portfolio.sql:**
```sql
CREATE TABLE IF NOT EXISTS user_portfolio (
    user_id    BIGINT NOT NULL,
    code       VARCHAR(12) NOT NULL,
    cost_price NUMERIC(10,3) NOT NULL,
    shares     INT NOT NULL,
    added_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, code)
);
```

**migrations/007_watchlist.sql:**
```sql
CREATE TABLE IF NOT EXISTS user_watchlist (
    user_id  BIGINT NOT NULL,
    code     VARCHAR(12) NOT NULL,
    added_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, code)
);
```

**migrations/008_simulators.sql:**
```sql
CREATE TABLE IF NOT EXISTS trading_sim_positions (
    id           BIGSERIAL PRIMARY KEY,
    code         VARCHAR(12) NOT NULL,
    name         VARCHAR(50),
    entry_price  NUMERIC(10,3),
    shares       INT,
    peak_price   NUMERIC(10,3),
    entry_date   DATE,
    exit_price   NUMERIC(10,3),
    exit_date    DATE,
    exit_reason  VARCHAR(50),
    pnl_pct      NUMERIC(8,4),
    is_open      BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS daban_sim_positions (
    id          BIGSERIAL PRIMARY KEY,
    code        VARCHAR(12) NOT NULL,
    name        VARCHAR(50),
    entry_price NUMERIC(10,3),
    shares      INT,
    score       NUMERIC(5,2),
    entry_date  DATE,
    exit_price  NUMERIC(10,3),
    exit_date   DATE,
    exit_reason VARCHAR(50),
    is_open     BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS sim_capital (
    sim_type    VARCHAR(20) PRIMARY KEY,
    balance     NUMERIC(18,2) NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO sim_capital (sim_type, balance) VALUES ('general', 1000000.00)
    ON CONFLICT (sim_type) DO NOTHING;
INSERT INTO sim_capital (sim_type, balance) VALUES ('daban', 100000.00)
    ON CONFLICT (sim_type) DO NOTHING;
```

**migrations/009_reports.sql:**
```sql
CREATE TABLE IF NOT EXISTS reports (
    id           BIGSERIAL PRIMARY KEY,
    report_type  VARCHAR(50) NOT NULL,
    content      TEXT NOT NULL,
    generated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_reports_type ON reports(report_type, generated_at DESC);
```

**Step 2: Commit**

```bash
git add migrations/
git commit -m "feat: add all PostgreSQL migrations (9 tables)"
```

---

### Task 10: Storage module (Postgres + Redis helpers)

**Files:**
- Create: `src/storage/mod.rs`
- Create: `src/storage/postgres.rs`
- Create: `src/storage/redis_cache.rs`

**Step 1: Create module structure**

```bash
mkdir -p src/storage
```

**Step 2: Implement postgres.rs**

```rust
// src/storage/postgres.rs
use chrono::NaiveDate;
use sqlx::PgPool;
use uuid::Uuid;

use crate::data::types::{Candle, LimitUpStock, SectorData, StockInfo};
use crate::error::Result;

/// Run sqlx migrations
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|e| crate::error::AppError::Database(e.into()))?;
    tracing::info!("✅ Migrations applied");
    Ok(())
}

/// Upsert daily bars (batch)
pub async fn upsert_daily_bars(pool: &PgPool, bars: &[(String, Candle)]) -> Result<usize> {
    if bars.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await?;
    let mut count = 0usize;

    for chunk in bars.chunks(500) {
        for (code, bar) in chunk {
            sqlx::query!(
                r#"INSERT INTO stock_daily_bars
                   (code, trade_date, open, high, low, close, volume, amount, turnover, pe, pb)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                   ON CONFLICT (code, trade_date) DO UPDATE SET
                   open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                   close=EXCLUDED.close, volume=EXCLUDED.volume, amount=EXCLUDED.amount"#,
                code,
                bar.trade_date,
                bar.open as f64,
                bar.high as f64,
                bar.low as f64,
                bar.close as f64,
                bar.volume,
                bar.amount as f64,
                bar.turnover.map(|v| v as f64),
                bar.pe.map(|v| v as f64),
                bar.pb.map(|v| v as f64),
            )
            .execute(&mut *tx)
            .await?;
            count += 1;
        }
    }

    tx.commit().await?;
    Ok(count)
}

/// Fetch OHLCV history for a stock (sorted ascending)
pub async fn get_stock_history(
    pool: &PgPool,
    code: &str,
    days: usize,
) -> Result<Vec<Candle>> {
    let rows = sqlx::query!(
        r#"SELECT trade_date, open, high, low, close, volume, amount, turnover, pe, pb
           FROM stock_daily_bars
           WHERE code = $1
           ORDER BY trade_date DESC
           LIMIT $2"#,
        code,
        days as i64,
    )
    .fetch_all(pool)
    .await?;

    let mut bars: Vec<Candle> = rows
        .into_iter()
        .map(|r| Candle {
            trade_date: r.trade_date,
            open: r.open.unwrap_or_default().try_into().unwrap_or(0.0),
            high: r.high.unwrap_or_default().try_into().unwrap_or(0.0),
            low: r.low.unwrap_or_default().try_into().unwrap_or(0.0),
            close: r.close.unwrap_or_default().try_into().unwrap_or(0.0),
            volume: r.volume.unwrap_or(0),
            amount: r.amount.unwrap_or_default().try_into().unwrap_or(0.0),
            turnover: r.turnover.map(|v| v.try_into().unwrap_or(0.0)),
            pe: r.pe.map(|v| v.try_into().unwrap_or(0.0)),
            pb: r.pb.map(|v| v.try_into().unwrap_or(0.0)),
        })
        .collect();

    bars.sort_by_key(|b| b.trade_date);
    Ok(bars)
}

/// Fetch all stock codes that have data
pub async fn get_stock_codes_with_data(pool: &PgPool) -> Result<Vec<String>> {
    let rows = sqlx::query!("SELECT DISTINCT code FROM stock_daily_bars ORDER BY code")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|r| r.code).collect())
}

/// Upsert stock info
pub async fn upsert_stock_info(pool: &PgPool, stocks: &[StockInfo]) -> Result<()> {
    let mut tx = pool.begin().await?;
    for s in stocks {
        sqlx::query!(
            r#"INSERT INTO stock_info (code, name, market, industry)
               VALUES ($1,$2,$3,$4)
               ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, industry=EXCLUDED.industry"#,
            s.code,
            s.name,
            s.market,
            s.industry,
        )
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Save scan results
pub async fn save_scan_results(
    pool: &PgPool,
    run_id: Uuid,
    results: &[(String, String, String, serde_json::Value)], // (code, name, signal_id, metadata)
) -> Result<()> {
    let mut tx = pool.begin().await?;
    for (code, name, signal_id, metadata) in results {
        sqlx::query!(
            "INSERT INTO scan_results (run_id, code, name, signal_id, metadata) VALUES ($1,$2,$3,$4,$5)",
            run_id,
            code,
            name,
            signal_id,
            metadata,
        )
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Save limit-up stocks
pub async fn save_limit_up_stocks(pool: &PgPool, stocks: &[LimitUpStock]) -> Result<()> {
    let mut tx = pool.begin().await?;
    for s in stocks {
        sqlx::query!(
            r#"INSERT INTO limit_up_stocks
               (code, trade_date, name, limit_time, seal_amount, burst_count, close, pct_chg, strth)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
               ON CONFLICT (code, trade_date) DO UPDATE SET
               name=EXCLUDED.name, seal_amount=EXCLUDED.seal_amount,
               burst_count=EXCLUDED.burst_count"#,
            s.code,
            s.trade_date,
            s.name,
            s.first_time,
            s.fd_amount,
            s.open_times,
            s.close,
            s.pct_chg,
            s.strth,
        )
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Get latest report by type
pub async fn get_latest_report(pool: &PgPool, report_type: &str) -> Result<Option<String>> {
    let row = sqlx::query!(
        "SELECT content FROM reports WHERE report_type=$1 ORDER BY generated_at DESC LIMIT 1",
        report_type
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.content))
}

/// Save report
pub async fn save_report(pool: &PgPool, report_type: &str, content: &str) -> Result<()> {
    sqlx::query!(
        "INSERT INTO reports (report_type, content) VALUES ($1, $2)",
        report_type,
        content
    )
    .execute(pool)
    .await?;
    Ok(())
}
```

**Step 3: Implement redis_cache.rs**

```rust
// src/storage/redis_cache.rs
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::time::Duration;
use tracing::warn;

use crate::error::{AppError, Result};

pub struct RedisCache {
    conn: ConnectionManager,
}

impl RedisCache {
    pub fn new(conn: ConnectionManager) -> Self {
        RedisCache { conn }
    }

    pub async fn set_json<T: serde::Serialize>(
        &mut self,
        key: &str,
        value: &T,
        ttl: Duration,
    ) -> Result<()> {
        let json = serde_json::to_string(value)?;
        self.conn
            .set_ex::<_, _, ()>(key, json, ttl.as_secs())
            .await
            .map_err(AppError::Redis)?;
        Ok(())
    }

    pub async fn get_json<T: serde::de::DeserializeOwned>(
        &mut self,
        key: &str,
    ) -> Result<Option<T>> {
        let val: Option<String> = self.conn.get(key).await.map_err(AppError::Redis)?;
        match val {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    pub async fn set_flag(&mut self, key: &str, ttl: Duration) -> Result<()> {
        self.conn
            .set_ex::<_, _, ()>(key, "1", ttl.as_secs())
            .await
            .map_err(AppError::Redis)?;
        Ok(())
    }

    pub async fn has_flag(&mut self, key: &str) -> Result<bool> {
        let exists: bool = self.conn.exists(key).await.map_err(AppError::Redis)?;
        Ok(exists)
    }

    pub async fn delete(&mut self, key: &str) -> Result<()> {
        self.conn.del::<_, ()>(key).await.map_err(AppError::Redis)?;
        Ok(())
    }

    /// Cache scan results until next trading day (TTL: 24h)
    pub async fn cache_scan_results(
        &mut self,
        results: &serde_json::Value,
    ) -> Result<()> {
        self.set_json("scan:latest", results, Duration::from_secs(86400)).await
    }

    pub async fn get_scan_results(&mut self) -> Result<Option<serde_json::Value>> {
        self.get_json("scan:latest").await
    }

    /// Cache stock universe (TTL: 24h)
    pub async fn cache_stock_universe(&mut self, stocks: &serde_json::Value) -> Result<()> {
        self.set_json("stocks:universe", stocks, Duration::from_secs(86400)).await
    }

    pub async fn get_stock_universe(&mut self) -> Result<Option<serde_json::Value>> {
        self.get_json("stocks:universe").await
    }

    /// Burst monitor cooldown (TTL: 5min)
    pub async fn set_burst_alerted(&mut self, code: &str) -> Result<()> {
        self.set_flag(&format!("burst:alerted:{}", code), Duration::from_secs(300)).await
    }

    pub async fn is_burst_alerted(&mut self, code: &str) -> Result<bool> {
        self.has_flag(&format!("burst:alerted:{}", code)).await
    }
}
```

**Step 4: Create mod.rs**

```rust
// src/storage/mod.rs
pub mod postgres;
pub mod redis_cache;

pub use postgres::*;
pub use redis_cache::RedisCache;
```

**Step 5: Add to main.rs**

```rust
mod storage;
```

**Step 6: Compile check**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
```

**Step 7: Commit**

```bash
git add src/storage/ migrations/
git commit -m "feat: add storage layer (postgres queries + redis cache helpers)"
```

---

## Phase 4: Signal System

### Task 11: Signal base traits and registry

**Files:**
- Create: `src/signals/mod.rs`
- Create: `src/signals/base.rs`
- Create: `src/signals/registry.rs`

**Step 1: Create module**

```bash
mkdir -p src/signals/volume src/signals/trend src/signals/pattern
mkdir -p src/signals/board src/signals/momentum src/signals/comprehensive
```

**Step 2: Implement base.rs**

```rust
// src/signals/base.rs
use std::collections::HashMap;
use crate::data::types::Candle;

/// Result of a signal detection
#[derive(Debug, Clone)]
pub struct SignalResult {
    pub triggered: bool,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl SignalResult {
    pub fn yes() -> Self {
        SignalResult { triggered: true, metadata: HashMap::new() }
    }

    pub fn no() -> Self {
        SignalResult { triggered: false, metadata: HashMap::new() }
    }

    pub fn with_meta(mut self, key: &str, value: serde_json::Value) -> Self {
        self.metadata.insert(key.to_string(), value);
        self
    }
}

/// Stock info passed to signal detectors
#[derive(Debug, Clone)]
pub struct StockContext {
    pub code: String,
    pub name: String,
}

/// All signals implement this trait
pub trait SignalDetector: Send + Sync {
    fn signal_id(&self) -> &'static str;
    fn display_name(&self) -> &'static str;
    fn icon(&self) -> &'static str;
    fn group(&self) -> &'static str;
    fn min_bars(&self) -> usize { 21 }
    fn priority(&self) -> i32 { 100 }
    fn enabled(&self) -> bool { true }
    fn count_in_multi(&self) -> bool { true }

    fn detect(&self, bars: &[Candle], ctx: &StockContext) -> SignalResult;
}

/// Helper: compute simple moving average
pub fn sma(values: &[f64], period: usize) -> Option<f64> {
    if values.len() < period { return None; }
    let sum: f64 = values[values.len()-period..].iter().sum();
    Some(sum / period as f64)
}

/// Helper: compute EMA
pub fn ema(values: &[f64], period: usize) -> Option<f64> {
    if values.len() < period { return None; }
    let k = 2.0 / (period as f64 + 1.0);
    let mut ema_val = values[values.len() - period..].iter().sum::<f64>() / period as f64;
    // This is a simplified EMA; for full EMA use all history
    Some(ema_val)
}

/// Helper: average volume over last N bars
pub fn avg_volume(bars: &[Candle], n: usize) -> f64 {
    if bars.len() < n { return 0.0; }
    let sum: f64 = bars[bars.len()-n..].iter().map(|b| b.volume as f64).sum();
    sum / n as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sma() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(sma(&values, 3), Some(4.0));
        assert_eq!(sma(&values, 6), None);
    }
}
```

**Step 3: Implement registry.rs**

```rust
// src/signals/registry.rs
use std::sync::OnceLock;
use super::base::SignalDetector;

static REGISTRY: OnceLock<Vec<Box<dyn SignalDetector>>> = OnceLock::new();

pub struct SignalRegistry;

impl SignalRegistry {
    /// Initialize registry with all signals
    pub fn init() -> &'static Vec<Box<dyn SignalDetector>> {
        REGISTRY.get_or_init(|| {
            use super::volume::*;
            use super::trend::*;
            use super::pattern::*;
            use super::board::*;
            use super::momentum::*;
            use super::comprehensive::*;

            let signals: Vec<Box<dyn SignalDetector>> = vec![
                // Volume
                Box::new(VolumeSurgeSignal),
                Box::new(VolumePriceSignal),
                // Trend
                Box::new(MaBullishSignal),
                Box::new(MaPullbackSignal),
                Box::new(StrongPullbackSignal),
                Box::new(UptrendBreakoutSignal),
                Box::new(DowntrendReversalSignal),
                Box::new(LinRegSignal),
                // Pattern
                Box::new(SlowBullSignal),
                Box::new(SmallBullishSignal),
                Box::new(TripleBullishSignal),
                Box::new(FanbaoSignal),
                Box::new(WeeklyMonthlyBullishSignal),
                // Board
                Box::new(BrokenBoardSignal),
                Box::new(StrongFirstNegSignal),
                // Momentum
                Box::new(BreakoutSignal),
                Box::new(StartupSignal),
                Box::new(KuangbiaoSignal),
                // Comprehensive
                Box::new(BottomQuickStartSignal),
                Box::new(LongCycleReversalSignal),
                Box::new(LowAccumulationSignal),
            ];

            tracing::info!("📡 Registered {} signals", signals.len());
            signals
        })
    }

    pub fn get_enabled() -> Vec<&'static dyn SignalDetector> {
        Self::init()
            .iter()
            .filter(|s| s.enabled())
            .map(|s| s.as_ref())
            .collect()
    }

    pub fn get_all() -> &'static Vec<Box<dyn SignalDetector>> {
        Self::init()
    }
}
```

**Step 4: Add mod.rs stubs (filled in Tasks 12-17)**

```rust
// src/signals/mod.rs
pub mod base;
pub mod registry;
pub mod volume;
pub mod trend;
pub mod pattern;
pub mod board;
pub mod momentum;
pub mod comprehensive;

pub use base::{SignalDetector, SignalResult, StockContext};
pub use registry::SignalRegistry;
```

**Step 5: Add to main.rs**

```rust
mod signals;
```

**Step 6: Commit (with stub modules)**

```bash
git add src/signals/
git commit -m "feat: add signal system base traits and registry"
```

---

### Task 12: Volume and Trend signals

**Files:**
- Create: `src/signals/volume/mod.rs`
- Create: `src/signals/trend/mod.rs`

**Step 1: Volume signals**

```rust
// src/signals/volume/mod.rs
use crate::data::types::Candle;
use super::base::{SignalDetector, SignalResult, StockContext, avg_volume, sma};

/// 放量突破 — today's volume > 2x 20-day average
pub struct VolumeSurgeSignal;
impl SignalDetector for VolumeSurgeSignal {
    fn signal_id(&self) -> &'static str { "volume_surge" }
    fn display_name(&self) -> &'static str { "放量突破" }
    fn icon(&self) -> &'static str { "📊" }
    fn group(&self) -> &'static str { "volume" }
    fn min_bars(&self) -> usize { 22 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 22 { return SignalResult::no(); }
        let today = &bars[n-1];
        let avg = avg_volume(&bars[..n-1], 20);
        if avg == 0.0 { return SignalResult::no(); }
        let ratio = today.volume as f64 / avg;
        if ratio >= 2.0 && today.close > today.open {
            SignalResult::yes()
                .with_meta("volume_ratio", serde_json::json!(format!("{:.1}x", ratio)))
        } else {
            SignalResult::no()
        }
    }
}

/// 量价配合 — up-day volume consistently > down-day volume (5-day window)
pub struct VolumePriceSignal;
impl SignalDetector for VolumePriceSignal {
    fn signal_id(&self) -> &'static str { "volume_price" }
    fn display_name(&self) -> &'static str { "量价配合" }
    fn icon(&self) -> &'static str { "📈" }
    fn group(&self) -> &'static str { "volume" }
    fn min_bars(&self) -> usize { 10 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 10 { return SignalResult::no(); }
        let window = &bars[n-5..];
        let (up_vol, down_vol): (f64, f64) = window.iter().fold((0.0, 0.0), |(u, d), b| {
            if b.close >= b.open { (u + b.volume as f64, d) }
            else { (u, d + b.volume as f64) }
        });
        if down_vol == 0.0 || up_vol / (up_vol + down_vol) >= 0.65 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}
```

**Step 2: Trend signals**

```rust
// src/signals/trend/mod.rs
use crate::data::types::Candle;
use super::base::{SignalDetector, SignalResult, StockContext, sma};

fn closes(bars: &[Candle]) -> Vec<f64> {
    bars.iter().map(|b| b.close).collect()
}

/// 均线多头 — MA5 > MA10 > MA20, all rising
pub struct MaBullishSignal;
impl SignalDetector for MaBullishSignal {
    fn signal_id(&self) -> &'static str { "ma_bullish" }
    fn display_name(&self) -> &'static str { "均线多头" }
    fn icon(&self) -> &'static str { "🐂" }
    fn group(&self) -> &'static str { "trend" }
    fn min_bars(&self) -> usize { 21 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let c = closes(bars);
        let (ma5, ma10, ma20) = match (sma(&c, 5), sma(&c, 10), sma(&c, 20)) {
            (Some(a), Some(b), Some(c)) => (a, b, c),
            _ => return SignalResult::no(),
        };
        if ma5 > ma10 && ma10 > ma20 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 均线回踩 — price pulled back to MA20 and bouncing
pub struct MaPullbackSignal;
impl SignalDetector for MaPullbackSignal {
    fn signal_id(&self) -> &'static str { "ma_pullback" }
    fn display_name(&self) -> &'static str { "均线回踩" }
    fn icon(&self) -> &'static str { "🔄" }
    fn group(&self) -> &'static str { "trend" }
    fn min_bars(&self) -> usize { 25 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 25 { return SignalResult::no(); }
        let c = closes(bars);
        let ma20 = match sma(&c, 20) { Some(v) => v, None => return SignalResult::no() };
        let today = &bars[n-1];
        let yesterday = &bars[n-2];
        // Low touched MA20 area and close > MA20
        let touched = today.low <= ma20 * 1.01 || yesterday.low <= ma20 * 1.01;
        let bouncing = today.close > ma20 && today.close > today.open;
        if touched && bouncing { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 强势回调 — above MA60, moderate pullback
pub struct StrongPullbackSignal;
impl SignalDetector for StrongPullbackSignal {
    fn signal_id(&self) -> &'static str { "strong_pullback" }
    fn display_name(&self) -> &'static str { "强势回调" }
    fn icon(&self) -> &'static str { "💪" }
    fn group(&self) -> &'static str { "trend" }
    fn min_bars(&self) -> usize { 65 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 65 { return SignalResult::no(); }
        let c = closes(bars);
        let ma20 = match sma(&c, 20) { Some(v) => v, None => return SignalResult::no() };
        let ma60 = match sma(&c, 60) { Some(v) => v, None => return SignalResult::no() };
        let today = &bars[n-1];
        // Price above MA60, pulled back to MA20 area
        if today.close > ma60 && today.low <= ma20 * 1.02 && today.close > today.open {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 上升突破 — breakout from rising channel
pub struct UptrendBreakoutSignal;
impl SignalDetector for UptrendBreakoutSignal {
    fn signal_id(&self) -> &'static str { "uptrend_breakout" }
    fn display_name(&self) -> &'static str { "上升突破" }
    fn icon(&self) -> &'static str { "🚀" }
    fn group(&self) -> &'static str { "trend" }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 21 { return SignalResult::no(); }
        let recent_high = bars[n-21..n-1].iter().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max);
        let today = &bars[n-1];
        if today.close > recent_high && today.close > today.open {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 下降反转 — breaks descending trendline with volume
pub struct DowntrendReversalSignal;
impl SignalDetector for DowntrendReversalSignal {
    fn signal_id(&self) -> &'static str { "downtrend_reversal" }
    fn display_name(&self) -> &'static str { "下降反转" }
    fn icon(&self) -> &'static str { "↗️" }
    fn group(&self) -> &'static str { "trend" }
    fn min_bars(&self) -> usize { 30 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 30 { return SignalResult::no(); }
        let c = closes(bars);
        let ma5 = match sma(&c, 5) { Some(v) => v, None => return SignalResult::no() };
        let ma10 = match sma(&c, 10) { Some(v) => v, None => return SignalResult::no() };
        let ma20 = match sma(&c, 20) { Some(v) => v, None => return SignalResult::no() };
        let today = &bars[n-1];
        // Was in downtrend, now crossing above MA5, MA5 starting to turn
        let was_bear = ma5 < ma10;
        let crossing = today.close > ma5 && today.close > today.open;
        let volume_ok = today.volume as f64 > super::base::avg_volume(&bars[..n-1], 10) * 1.2;
        if was_bear && crossing && volume_ok { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 线性回归 — linear regression slope is positive and accelerating
pub struct LinRegSignal;
impl SignalDetector for LinRegSignal {
    fn signal_id(&self) -> &'static str { "linreg" }
    fn display_name(&self) -> &'static str { "线性回归" }
    fn icon(&self) -> &'static str { "📐" }
    fn group(&self) -> &'static str { "trend" }
    fn min_bars(&self) -> usize { 21 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 21 { return SignalResult::no(); }
        let window: Vec<f64> = bars[n-20..].iter().map(|b| b.close).collect();
        let len = window.len() as f64;
        let x_mean = (len - 1.0) / 2.0;
        let y_mean: f64 = window.iter().sum::<f64>() / len;
        let num: f64 = window.iter().enumerate()
            .map(|(i, &y)| (i as f64 - x_mean) * (y - y_mean)).sum();
        let den: f64 = (0..window.len())
            .map(|i| (i as f64 - x_mean).powi(2)).sum();
        let slope = if den > 0.0 { num / den } else { 0.0 };
        // Slope positive and meaningful relative to price
        if slope > 0.0 && slope / y_mean * 100.0 > 0.1 {
            SignalResult::yes().with_meta("slope_pct", serde_json::json!(slope / y_mean * 100.0))
        } else {
            SignalResult::no()
        }
    }
}
```

**Step 3: Run tests**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test signals
```

**Step 4: Commit**

```bash
git add src/signals/
git commit -m "feat: add volume and trend signals (8 signals)"
```

---

### Task 13: Pattern, Board, Momentum and Comprehensive signals

**Files:**
- Create: `src/signals/pattern/mod.rs`
- Create: `src/signals/board/mod.rs`
- Create: `src/signals/momentum/mod.rs`
- Create: `src/signals/comprehensive/mod.rs`

**Step 1: Pattern signals**

```rust
// src/signals/pattern/mod.rs
use crate::data::types::Candle;
use super::base::{SignalDetector, SignalResult, StockContext, sma, avg_volume};

fn closes(bars: &[Candle]) -> Vec<f64> { bars.iter().map(|b| b.close).collect() }

/// 缓慢牛 — steady uptrend: close > MA5 > MA10 > MA20 for 5+ days
pub struct SlowBullSignal;
impl SignalDetector for SlowBullSignal {
    fn signal_id(&self) -> &'static str { "slow_bull" }
    fn display_name(&self) -> &'static str { "缓慢牛" }
    fn icon(&self) -> &'static str { "🐢" }
    fn group(&self) -> &'static str { "pattern" }
    fn min_bars(&self) -> usize { 25 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 25 { return SignalResult::no(); }
        // Last 5 days: close > MA5 > MA10 for most days
        let ok_days = (0..5).filter(|&i| {
            let slice = &bars[..n-i];
            let c = closes(slice);
            let close = slice.last().unwrap().close;
            let ma5 = sma(&c, 5).unwrap_or(0.0);
            let ma10 = sma(&c, 10).unwrap_or(0.0);
            close > ma5 && ma5 > ma10
        }).count();
        if ok_days >= 4 { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 小阳线 — 3+ consecutive small up candles (body < 2%)
pub struct SmallBullishSignal;
impl SignalDetector for SmallBullishSignal {
    fn signal_id(&self) -> &'static str { "small_bullish" }
    fn display_name(&self) -> &'static str { "小阳线" }
    fn icon(&self) -> &'static str { "🌱" }
    fn group(&self) -> &'static str { "pattern" }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 3 { return SignalResult::no(); }
        let consecutive = bars[n-3..].iter().all(|b| {
            b.close > b.open && (b.close - b.open) / b.open * 100.0 < 2.5
        });
        if consecutive { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 三阳开泰 — 3 consecutive up days, each close > previous close
pub struct TripleBullishSignal;
impl SignalDetector for TripleBullishSignal {
    fn signal_id(&self) -> &'static str { "triple_bullish" }
    fn display_name(&self) -> &'static str { "三阳开泰" }
    fn icon(&self) -> &'static str { "🔥" }
    fn group(&self) -> &'static str { "pattern" }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 4 { return SignalResult::no(); }
        let three = &bars[n-3..];
        let ok = three.iter().all(|b| b.close > b.open)
            && three[1].close > three[0].close
            && three[2].close > three[1].close;
        if ok { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 反包 — today's up candle fully engulfs yesterday's down candle
pub struct FanbaoSignal;
impl SignalDetector for FanbaoSignal {
    fn signal_id(&self) -> &'static str { "fanbao" }
    fn display_name(&self) -> &'static str { "反包" }
    fn icon(&self) -> &'static str { "🔁" }
    fn group(&self) -> &'static str { "pattern" }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 2 { return SignalResult::no(); }
        let prev = &bars[n-2];
        let today = &bars[n-1];
        let prev_was_down = prev.close < prev.open;
        let today_engulfs = today.close > today.open
            && today.open <= prev.close
            && today.close >= prev.open;
        if prev_was_down && today_engulfs { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 周月多头 — weekly MA5 > MA10 approximated from daily
pub struct WeeklyMonthlyBullishSignal;
impl SignalDetector for WeeklyMonthlyBullishSignal {
    fn signal_id(&self) -> &'static str { "weekly_monthly_bullish" }
    fn display_name(&self) -> &'static str { "周月多头" }
    fn icon(&self) -> &'static str { "🗓️" }
    fn group(&self) -> &'static str { "pattern" }
    fn min_bars(&self) -> usize { 60 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 60 { return SignalResult::no(); }
        // Weekly approximation: MA25 > MA50 on daily (5*5=25, 5*10=50)
        let c = closes(bars);
        let ma25 = sma(&c, 25).unwrap_or(0.0);
        let ma50 = sma(&c, 50).unwrap_or(0.0);
        if ma25 > ma50 { SignalResult::yes() } else { SignalResult::no() }
    }
}
```

**Step 2: Board signals**

```rust
// src/signals/board/mod.rs
use crate::data::types::Candle;
use super::base::{SignalDetector, SignalResult, StockContext, avg_volume};

const LIMIT_UP_PCT: f64 = 9.8;

fn is_limit_up(bar: &Candle) -> bool {
    if bar.open == 0.0 { return false; }
    (bar.close - bar.open) / bar.open * 100.0 >= LIMIT_UP_PCT
}

/// 炸板反包 — yesterday hit limit-up but closed below it; today recovers
pub struct BrokenBoardSignal;
impl SignalDetector for BrokenBoardSignal {
    fn signal_id(&self) -> &'static str { "broken_board" }
    fn display_name(&self) -> &'static str { "炸板反包" }
    fn icon(&self) -> &'static str { "💥" }
    fn group(&self) -> &'static str { "board" }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 3 { return SignalResult::no(); }
        let prev2 = &bars[n-3];
        let prev = &bars[n-2];
        let today = &bars[n-1];
        // prev2 had intraday limit-up (high touched it) but closed below
        let high_hit_limit = prev2.open > 0.0
            && (prev2.high - prev2.open) / prev2.open * 100.0 >= LIMIT_UP_PCT;
        let closed_below = prev2.close < prev2.high * 0.98;
        let recovering = today.close > today.open && today.close > prev.close;
        if high_hit_limit && closed_below && recovering {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 强势首阴 — first down day after limit-up streak, still strong
pub struct StrongFirstNegSignal;
impl SignalDetector for StrongFirstNegSignal {
    fn signal_id(&self) -> &'static str { "strong_first_neg" }
    fn display_name(&self) -> &'static str { "强势首阴" }
    fn icon(&self) -> &'static str { "⚡" }
    fn group(&self) -> &'static str { "board" }
    fn min_bars(&self) -> usize { 5 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 5 { return SignalResult::no(); }
        // Previous day was limit-up; today is first down but didn't fall much
        let prev = &bars[n-2];
        let today = &bars[n-1];
        let prev_was_limit = is_limit_up(prev);
        let mild_decline = today.close < today.open
            && (today.open - today.close) / today.open * 100.0 < 3.0;
        let above_ma5 = {
            let closes: Vec<f64> = bars[n-5..].iter().map(|b| b.close).collect();
            super::base::sma(&closes, 5).map(|ma| today.close > ma).unwrap_or(false)
        };
        if prev_was_limit && mild_decline && above_ma5 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}
```

**Step 3: Momentum signals**

```rust
// src/signals/momentum/mod.rs
use crate::data::types::Candle;
use super::base::{SignalDetector, SignalResult, StockContext, sma, avg_volume};

fn closes(bars: &[Candle]) -> Vec<f64> { bars.iter().map(|b| b.close).collect() }

/// 突破信号 — close breaks 20-day high
pub struct BreakoutSignal;
impl SignalDetector for BreakoutSignal {
    fn signal_id(&self) -> &'static str { "breakout" }
    fn display_name(&self) -> &'static str { "突破信号" }
    fn icon(&self) -> &'static str { "🔺" }
    fn group(&self) -> &'static str { "momentum" }
    fn min_bars(&self) -> usize { 21 }
    fn priority(&self) -> i32 { 10 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 21 { return SignalResult::no(); }
        let high_20 = bars[n-21..n-1].iter().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max);
        if bars[n-1].close > high_20 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 启动信号 — MACD golden cross + volume surge + MA alignment
pub struct StartupSignal;
impl SignalDetector for StartupSignal {
    fn signal_id(&self) -> &'static str { "startup" }
    fn display_name(&self) -> &'static str { "启动信号" }
    fn icon(&self) -> &'static str { "🚦" }
    fn group(&self) -> &'static str { "momentum" }
    fn min_bars(&self) -> usize { 30 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 30 { return SignalResult::no(); }
        let c = closes(bars);
        let ma5 = sma(&c, 5).unwrap_or(0.0);
        let ma10 = sma(&c, 10).unwrap_or(0.0);
        let ma20 = sma(&c, 20).unwrap_or(0.0);
        let today = &bars[n-1];
        let vol_ok = today.volume as f64 > avg_volume(&bars[..n-1], 10) * 1.5;
        let ma_ok = ma5 > ma10 && today.close > ma5;
        let price_ok = today.close > today.open;
        if vol_ok && ma_ok && price_ok { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 狂飙信号 — explosive: >7% gain with >3x volume
pub struct KuangbiaoSignal;
impl SignalDetector for KuangbiaoSignal {
    fn signal_id(&self) -> &'static str { "kuangbiao" }
    fn display_name(&self) -> &'static str { "狂飙信号" }
    fn icon(&self) -> &'static str { "🌪️" }
    fn group(&self) -> &'static str { "momentum" }
    fn min_bars(&self) -> usize { 21 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 21 { return SignalResult::no(); }
        let today = &bars[n-1];
        let prev = &bars[n-2];
        if prev.close == 0.0 { return SignalResult::no(); }
        let gain_pct = (today.close - prev.close) / prev.close * 100.0;
        let vol_ratio = today.volume as f64 / avg_volume(&bars[..n-1], 20).max(1.0);
        if gain_pct >= 7.0 && vol_ratio >= 3.0 {
            SignalResult::yes()
                .with_meta("gain_pct", serde_json::json!(format!("{:.1}%", gain_pct)))
                .with_meta("vol_ratio", serde_json::json!(format!("{:.1}x", vol_ratio)))
        } else {
            SignalResult::no()
        }
    }
}
```

**Step 4: Comprehensive signals**

```rust
// src/signals/comprehensive/mod.rs
use crate::data::types::Candle;
use super::base::{SignalDetector, SignalResult, StockContext, sma, avg_volume};

fn closes(bars: &[Candle]) -> Vec<f64> { bars.iter().map(|b| b.close).collect() }

/// 底部快速启动 — bottom reversal: was below MA20, now breaking up with volume
pub struct BottomQuickStartSignal;
impl SignalDetector for BottomQuickStartSignal {
    fn signal_id(&self) -> &'static str { "bottom_quick_start" }
    fn display_name(&self) -> &'static str { "底部快速启动" }
    fn icon(&self) -> &'static str { "⬆️" }
    fn group(&self) -> &'static str { "comprehensive" }
    fn min_bars(&self) -> usize { 30 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 30 { return SignalResult::no(); }
        let c = closes(bars);
        let ma20 = sma(&c, 20).unwrap_or(0.0);
        let today = &bars[n-1];
        // Was below MA20 within last 5 days
        let was_below = bars[n-5..n-1].iter().any(|b| b.close < ma20);
        // Now above MA20 with surge
        let above_now = today.close > ma20;
        let vol_surge = today.volume as f64 > avg_volume(&bars[..n-1], 10) * 1.8;
        if was_below && above_now && vol_surge { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 长周期反转 — weekly-level bottom: 60-day low area, breaking up
pub struct LongCycleReversalSignal;
impl SignalDetector for LongCycleReversalSignal {
    fn signal_id(&self) -> &'static str { "long_cycle_reversal" }
    fn display_name(&self) -> &'static str { "长周期反转" }
    fn icon(&self) -> &'static str { "🌅" }
    fn group(&self) -> &'static str { "comprehensive" }
    fn min_bars(&self) -> usize { 65 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 65 { return SignalResult::no(); }
        let low_60 = bars[n-60..n-1].iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
        let today = &bars[n-1];
        let near_bottom = (today.low - low_60) / low_60 * 100.0 < 10.0;
        let c = closes(bars);
        let ma5 = sma(&c, 5).unwrap_or(0.0);
        let reversing = today.close > today.open && today.close > ma5;
        if near_bottom && reversing { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 低位积累启动 — narrow range consolidation followed by breakout
pub struct LowAccumulationSignal;
impl SignalDetector for LowAccumulationSignal {
    fn signal_id(&self) -> &'static str { "low_accumulation" }
    fn display_name(&self) -> &'static str { "低位积累启动" }
    fn icon(&self) -> &'static str { "🏗️" }
    fn group(&self) -> &'static str { "comprehensive" }
    fn min_bars(&self) -> usize { 25 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 25 { return SignalResult::no(); }
        // Check 15-day consolidation range < 8%
        let window = &bars[n-16..n-1];
        let high = window.iter().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max);
        let low = window.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
        if low == 0.0 { return SignalResult::no(); }
        let range_pct = (high - low) / low * 100.0;
        let consolidated = range_pct < 8.0;
        // Today breaks out of range with volume
        let today = &bars[n-1];
        let breakout = today.close > high && today.close > today.open;
        let vol_surge = today.volume as f64 > avg_volume(&bars[..n-1], 15) * 1.5;
        if consolidated && breakout && vol_surge { SignalResult::yes() } else { SignalResult::no() }
    }
}
```

**Step 5: Run all signal tests**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test signals
```

**Step 6: Commit**

```bash
git add src/signals/
git commit -m "feat: add all 20 signal detectors (volume/trend/pattern/board/momentum/comprehensive)"
```

---

## Phase 5: Core Services

### Task 14: Stock history service

**Files:**
- Create: `src/services/mod.rs`
- Create: `src/services/stock_history.rs`

**Step 1: Create services module**

```bash
mkdir -p src/services
```

**Step 2: Implement stock_history.rs**

```rust
// src/services/stock_history.rs
use chrono::{Duration, Local, NaiveDate};
use std::sync::Arc;
use tracing::{info, warn};

use crate::data::tushare::TushareClient;
use crate::error::Result;
use crate::state::AppState;
use crate::storage::{postgres, upsert_stock_info};

pub struct StockHistoryService {
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
}

impl StockHistoryService {
    pub fn new(state: Arc<AppState>, provider: Arc<TushareClient>) -> Self {
        StockHistoryService { state, provider }
    }

    /// Full backfill: fetch all trading dates in last N years, date-by-date
    pub async fn backfill(&self, years: u32) -> Result<()> {
        let end = Local::now().naive_local().date();
        let start = end - Duration::days(years as i64 * 365);
        info!("📥 Starting backfill {} to {}", start, end);

        let dates = self.provider.get_trading_dates(start, end).await?;
        info!("📅 {} trading days to backfill", dates.len());

        for (i, date) in dates.iter().enumerate() {
            match self.provider.get_daily_bars_by_date(*date).await {
                Ok(bars) => {
                    let count = bars.len();
                    postgres::upsert_daily_bars(&self.state.db, &bars).await?;
                    if i % 50 == 0 {
                        info!("  Backfill progress: {}/{} ({}, {} bars)", i+1, dates.len(), date, count);
                    }
                }
                Err(e) => {
                    warn!("  Failed to fetch {}: {}", date, e);
                }
            }
            // Rate limit: ~200ms between calls
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        info!("✅ Backfill complete");
        Ok(())
    }

    /// Daily incremental update: fetch today's bars for all known stocks
    pub async fn update_today(&self) -> Result<()> {
        let today = Local::now().naive_local().date();
        info!("📥 Daily update for {}", today);

        let bars = self.provider.get_daily_bars_by_date(today).await?;
        let count = bars.len();
        postgres::upsert_daily_bars(&self.state.db, &bars).await?;
        info!("✅ Daily update: {} bars saved for {}", count, today);

        // Also refresh stock info
        let stocks = self.provider.get_stock_list().await?;
        upsert_stock_info(&self.state.db, &stocks).await?;
        info!("✅ Stock info refreshed: {} stocks", stocks.len());

        Ok(())
    }

    /// Check if today's data already exists (avoid duplicate fetches)
    pub async fn has_today_data(&self) -> bool {
        let today = Local::now().naive_local().date();
        let result = sqlx::query!(
            "SELECT COUNT(*) as cnt FROM stock_daily_bars WHERE trade_date = $1",
            today
        )
        .fetch_one(&self.state.db)
        .await;

        result.map(|r| r.cnt.unwrap_or(0) > 100).unwrap_or(false)
    }
}
```

**Step 3: Create services/mod.rs**

```rust
// src/services/mod.rs
pub mod stock_history;
pub mod scanner;
pub mod limit_up;
pub mod daban;
pub mod daban_sim;
pub mod sector;
pub mod market;
pub mod trend_analyzer;
pub mod chip_dist;
pub mod burst_monitor;
pub mod portfolio;
pub mod watchlist;
pub mod trading_sim;
pub mod market_report;
pub mod ai_analysis;
```

**Step 4: Add to main.rs**

```rust
mod services;
```

**Step 5: Compile check**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
```

**Step 6: Commit**

```bash
git add src/services/
git commit -m "feat: add StockHistoryService with backfill and daily update"
```

---

### Task 15: Scanner service

**Files:**
- Create: `src/services/scanner.rs`

**Step 1: Implement**

```rust
// src/services/scanner.rs
use chrono::Local;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::data::types::Candle;
use crate::error::Result;
use crate::signals::registry::SignalRegistry;
use crate::signals::base::StockContext;
use crate::state::AppState;
use crate::storage::postgres;

const BATCH_SIZE: usize = 100;
const MIN_BARS: usize = 60;
const MULTI_SIGNAL_THRESHOLD: usize = 3;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SignalHit {
    pub code: String,
    pub name: String,
    pub signal_id: String,
    pub signal_name: String,
    pub icon: String,
    pub metadata: serde_json::Value,
}

pub struct ScannerService {
    state: Arc<AppState>,
}

impl ScannerService {
    pub fn new(state: Arc<AppState>) -> Self {
        ScannerService { state }
    }

    pub async fn run_full_scan(&self) -> Result<HashMap<String, Vec<SignalHit>>> {
        info!("🔍 Starting full stock scan...");
        let run_id = Uuid::new_v4();
        let signals = SignalRegistry::get_enabled();

        if signals.is_empty() {
            warn!("No signals enabled");
            return Ok(HashMap::new());
        }

        // Load all stock codes with data
        let codes = postgres::get_stock_codes_with_data(&self.state.db).await?;
        let total = codes.len();
        info!("📊 Scanning {} stocks with {} signals", total, signals.len());

        let mut results: HashMap<String, Vec<SignalHit>> = HashMap::new();
        for sig in &signals {
            results.insert(sig.signal_id().to_string(), Vec::new());
        }
        results.insert("multi_signal".to_string(), Vec::new());

        let mut checked = 0usize;
        let mut db_inserts: Vec<(String, String, String, serde_json::Value)> = Vec::new();

        // Load stock names
        let names: HashMap<String, String> = sqlx::query!(
            "SELECT code, name FROM stock_info"
        )
        .fetch_all(&self.state.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.code, r.name))
        .collect();

        for chunk in codes.chunks(BATCH_SIZE) {
            for code in chunk {
                let bars = match postgres::get_stock_history(&self.state.db, code, 120).await {
                    Ok(b) if b.len() >= MIN_BARS => b,
                    _ => { checked += 1; continue; }
                };

                let name = names.get(code).cloned().unwrap_or_else(|| code.clone());
                let ctx = StockContext { code: code.clone(), name: name.clone() };

                let mut triggered_count = 0usize;

                for signal in &signals {
                    if bars.len() < signal.min_bars() { continue; }
                    let result = signal.detect(&bars, &ctx);
                    if result.triggered {
                        let hit = SignalHit {
                            code: code.clone(),
                            name: name.clone(),
                            signal_id: signal.signal_id().to_string(),
                            signal_name: signal.display_name().to_string(),
                            icon: signal.icon().to_string(),
                            metadata: serde_json::to_value(&result.metadata).unwrap_or_default(),
                        };
                        results.entry(signal.signal_id().to_string())
                            .or_default()
                            .push(hit.clone());
                        db_inserts.push((code.clone(), name.clone(),
                            signal.signal_id().to_string(), hit.metadata.clone()));
                        if signal.count_in_multi() { triggered_count += 1; }
                    }
                }

                if triggered_count >= MULTI_SIGNAL_THRESHOLD {
                    results.entry("multi_signal".to_string()).or_default().push(SignalHit {
                        code: code.clone(),
                        name: name.clone(),
                        signal_id: "multi_signal".to_string(),
                        signal_name: format!("多信号({triggered_count})"),
                        icon: "⭐".to_string(),
                        metadata: serde_json::json!({"count": triggered_count}),
                    });
                }

                checked += 1;
            }

            // Yield to event loop between batches
            tokio::task::yield_now().await;

            if checked % 500 == 0 {
                info!("  Scan progress: {}/{}", checked, total);
            }
        }

        // Save to DB in background
        if !db_inserts.is_empty() {
            let db = self.state.db.clone();
            let inserts = db_inserts.clone();
            tokio::spawn(async move {
                if let Err(e) = postgres::save_scan_results(&db, run_id, &inserts).await {
                    warn!("Failed to save scan results: {}", e);
                }
            });
        }

        let total_hits: usize = results.values().map(|v| v.len()).sum();
        info!("✅ Scan complete: {} stocks checked, {} signal hits", checked, total_hits);

        // Cache results
        let json = serde_json::to_value(&results).unwrap_or_default();
        if let Ok(mut cache) = self.state.redis.clone().into_owned().pipe(|_| {
            crate::storage::redis_cache::RedisCache::new(self.state.redis.clone())
        }).as_mut().map(|c| c.cache_scan_results(&json)) {
            let _ = cache.await;
        }

        Ok(results)
    }
}

// Helper to create RedisCache from ConnectionManager
trait IntoRedisCache {
    fn into_redis_cache(self) -> crate::storage::redis_cache::RedisCache;
}
impl IntoRedisCache for redis::aio::ConnectionManager {
    fn into_redis_cache(self) -> crate::storage::redis_cache::RedisCache {
        crate::storage::redis_cache::RedisCache::new(self)
    }
}
```

**Step 2: Compile check**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
```

**Step 3: Commit**

```bash
git add src/services/scanner.rs
git commit -m "feat: add ScannerService with batch signal detection across all stocks"
```

---

### Task 16: Limit-Up, Daban and Sector services

**Files:**
- Create: `src/services/limit_up.rs`
- Create: `src/services/daban.rs`
- Create: `src/services/sector.rs`

**Step 1: Implement limit_up.rs**

```rust
// src/services/limit_up.rs
use chrono::{Local, NaiveDate};
use std::sync::Arc;
use tracing::info;

use crate::data::tushare::TushareClient;
use crate::data::types::LimitUpStock;
use crate::error::Result;
use crate::state::AppState;
use crate::storage::postgres;

pub struct LimitUpService {
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
}

impl LimitUpService {
    pub fn new(state: Arc<AppState>, provider: Arc<TushareClient>) -> Self {
        LimitUpService { state, provider }
    }

    pub async fn fetch_and_save(&self, date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        let stocks = self.provider.get_limit_up_stocks(date).await?;
        info!("涨停板: {} stocks on {}", stocks.len(), date);

        // Calculate streaks
        let stocks_with_streaks = self.calculate_streaks(stocks, date).await;
        postgres::save_limit_up_stocks(&self.state.db, &stocks_with_streaks).await?;
        Ok(stocks_with_streaks)
    }

    async fn calculate_streaks(&self, mut stocks: Vec<LimitUpStock>, date: NaiveDate) -> Vec<LimitUpStock> {
        for stock in &mut stocks {
            // Count consecutive limit-up days
            let streak = self.count_streak(&stock.code, date).await;
            stock.open_times = streak as i32;  // Reuse field for streak display
        }
        stocks
    }

    async fn count_streak(&self, code: &str, current_date: NaiveDate) -> usize {
        let mut streak = 1usize;
        let mut check_date = current_date - chrono::Duration::days(1);

        for _ in 0..9 {
            let exists = sqlx::query!(
                "SELECT 1 FROM limit_up_stocks WHERE code = $1 AND trade_date = $2",
                code, check_date
            )
            .fetch_optional(&self.state.db)
            .await
            .map(|r| r.is_some())
            .unwrap_or(false);

            if exists {
                streak += 1;
                check_date -= chrono::Duration::days(1);
            } else {
                break;
            }
        }
        streak
    }

    pub async fn get_summary(&self, date: NaiveDate) -> Result<LimitUpSummary> {
        let rows = sqlx::query!(
            r#"SELECT code, name, burst_count, seal_amount, pct_chg
               FROM limit_up_stocks WHERE trade_date = $1 ORDER BY seal_amount DESC"#,
            date
        )
        .fetch_all(&self.state.db)
        .await?;

        let total = rows.len();
        let burst = rows.iter().filter(|r| r.burst_count.unwrap_or(0) > 0).count();
        let sealed = total - burst;

        Ok(LimitUpSummary {
            date,
            total,
            sealed,
            burst,
            burst_rate: if total > 0 { burst as f64 / total as f64 * 100.0 } else { 0.0 },
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct LimitUpSummary {
    pub date: NaiveDate,
    pub total: usize,
    pub sealed: usize,
    pub burst: usize,
    pub burst_rate: f64,
}
```

**Step 2: Implement daban.rs (打板 scoring)**

```rust
// src/services/daban.rs
use std::sync::Arc;
use crate::data::types::LimitUpStock;
use crate::state::AppState;

#[derive(Debug, Clone, serde::Serialize)]
pub struct DabanScore {
    pub code: String,
    pub name: String,
    pub score: f64,
    pub seal_score: f64,
    pub time_score: f64,
    pub burst_penalty: f64,
    pub executability: String,
    pub verdict: String,
}

pub struct DabanService {
    state: Arc<AppState>,
}

impl DabanService {
    pub fn new(state: Arc<AppState>) -> Self {
        DabanService { state }
    }

    pub fn score_stock(&self, stock: &LimitUpStock) -> DabanScore {
        // Seal strength score (0-30): fd_amount / 1M normalized
        let seal_score = (stock.fd_amount / 1_000_000.0).min(30.0);

        // Timing score (0-25): earlier limit-up is better
        let time_score = if let Some(ref t) = stock.first_time {
            let hour: u32 = t.split(':').next().and_then(|h| h.parse().ok()).unwrap_or(15);
            let min: u32 = t.split(':').nth(1).and_then(|m| m.parse().ok()).unwrap_or(0);
            let minutes_since_open = (hour * 60 + min).saturating_sub(9 * 60 + 30);
            (25.0 - (minutes_since_open as f64 / 6.0 * 25.0 / 60.0)).max(0.0)
        } else {
            5.0
        };

        // Burst penalty (0 to -20)
        let burst_penalty = (stock.open_times as f64) * 5.0;

        let raw_score = seal_score + time_score - burst_penalty;
        let score = raw_score.max(0.0).min(100.0);

        // Executability
        let executability = if stock.open_times == 0 && stock.pct_chg >= 9.8 {
            "一字板".to_string()
        } else if stock.open_times > 2 {
            "多次炸板".to_string()
        } else if score >= 60.0 {
            "可打".to_string()
        } else {
            "观望".to_string()
        };

        let verdict = if score >= 80.0 { "强烈推荐" }
            else if score >= 60.0 { "推荐" }
            else if score >= 40.0 { "观望" }
            else { "回避" }.to_string();

        DabanScore {
            code: stock.code.clone(),
            name: stock.name.clone(),
            score,
            seal_score,
            time_score,
            burst_penalty,
            executability,
            verdict,
        }
    }

    pub fn score_all(&self, stocks: &[LimitUpStock]) -> Vec<DabanScore> {
        let mut scores: Vec<DabanScore> = stocks.iter()
            .map(|s| self.score_stock(s))
            .collect();
        scores.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        scores
    }
}
```

**Step 3: Implement sector.rs**

```rust
// src/services/sector.rs
use chrono::NaiveDate;
use std::sync::Arc;
use tracing::info;

use crate::data::tushare::TushareClient;
use crate::error::Result;
use crate::state::AppState;

pub struct SectorService {
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
}

impl SectorService {
    pub fn new(state: Arc<AppState>, provider: Arc<TushareClient>) -> Self {
        SectorService { state, provider }
    }

    pub async fn fetch_and_save(&self, date: NaiveDate) -> Result<()> {
        let sectors = self.provider.get_sector_data(date).await?;
        info!("Sectors: {} records for {}", sectors.len(), date);

        let mut tx = self.state.db.begin().await?;
        for s in &sectors {
            sqlx::query!(
                r#"INSERT INTO sector_daily (code, name, sector_type, change_pct, amount, trade_date)
                   VALUES ($1,$2,$3,$4,$5,$6)
                   ON CONFLICT (code, trade_date) DO UPDATE SET
                   change_pct=EXCLUDED.change_pct, amount=EXCLUDED.amount"#,
                s.code, s.name, s.sector_type,
                s.change_pct, s.amount, s.trade_date
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_top_sectors(
        &self,
        date: NaiveDate,
        limit: i64,
    ) -> Result<Vec<SectorRank>> {
        let rows = sqlx::query!(
            r#"SELECT code, name, sector_type, change_pct, amount
               FROM sector_daily WHERE trade_date = $1
               ORDER BY change_pct DESC LIMIT $2"#,
            date, limit
        )
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows.into_iter().map(|r| SectorRank {
            code: r.code,
            name: r.name.unwrap_or_default(),
            sector_type: r.sector_type.unwrap_or_default(),
            change_pct: r.change_pct.unwrap_or_default().try_into().unwrap_or(0.0),
            amount: r.amount.unwrap_or_default().try_into().unwrap_or(0.0),
        }).collect())
    }

    pub async fn get_bottom_sectors(&self, date: NaiveDate, limit: i64) -> Result<Vec<SectorRank>> {
        let rows = sqlx::query!(
            r#"SELECT code, name, sector_type, change_pct, amount
               FROM sector_daily WHERE trade_date = $1
               ORDER BY change_pct ASC LIMIT $2"#,
            date, limit
        )
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows.into_iter().map(|r| SectorRank {
            code: r.code,
            name: r.name.unwrap_or_default(),
            sector_type: r.sector_type.unwrap_or_default(),
            change_pct: r.change_pct.unwrap_or_default().try_into().unwrap_or(0.0),
            amount: r.amount.unwrap_or_default().try_into().unwrap_or(0.0),
        }).collect())
    }
}

#[derive(Debug, serde::Serialize)]
pub struct SectorRank {
    pub code: String,
    pub name: String,
    pub sector_type: String,
    pub change_pct: f64,
    pub amount: f64,
}
```

**Step 4: Compile check and commit**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
git add src/services/
git commit -m "feat: add LimitUpService, DabanService (打板 scorer), SectorService"
```

---

### Task 17: Market, TrendAnalyzer, BurstMonitor, and remaining services

**Files:**
- Create: `src/services/market.rs`
- Create: `src/services/trend_analyzer.rs`
- Create: `src/services/burst_monitor.rs`
- Create: `src/services/market_report.rs`
- Create stubs: `portfolio.rs`, `watchlist.rs`, `trading_sim.rs`, `daban_sim.rs`, `chip_dist.rs`, `ai_analysis.rs`

**Step 1: Implement market.rs**

```rust
// src/services/market.rs
use chrono::NaiveDate;
use std::sync::Arc;
use crate::data::tushare::TushareClient;
use crate::data::types::IndexData;
use crate::error::Result;
use crate::state::AppState;

const INDICES: &[(&str, &str)] = &[
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
    ("000688.SH", "科创50"),
];

pub struct MarketService {
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
}

impl MarketService {
    pub fn new(state: Arc<AppState>, provider: Arc<TushareClient>) -> Self {
        MarketService { state, provider }
    }

    pub async fn get_market_overview(&self, date: NaiveDate) -> Result<MarketOverview> {
        let mut indices = Vec::new();
        for (code, _name) in INDICES {
            if let Ok(Some(data)) = self.provider.get_index_daily(code, date).await {
                indices.push(data);
            }
        }

        // Market breadth from DB
        let breadth = sqlx::query!(
            r#"SELECT
               COUNT(CASE WHEN close > open THEN 1 END) as up_count,
               COUNT(CASE WHEN close < open THEN 1 END) as down_count,
               COUNT(CASE WHEN ABS(close - open) / NULLIF(open,0) * 100 >= 9.8 THEN 1 END) as limit_up,
               SUM(amount) as total_amount
               FROM stock_daily_bars WHERE trade_date = $1"#,
            date
        )
        .fetch_one(&self.state.db)
        .await?;

        Ok(MarketOverview {
            date,
            indices,
            up_count: breadth.up_count.unwrap_or(0) as usize,
            down_count: breadth.down_count.unwrap_or(0) as usize,
            limit_up_count: breadth.limit_up.unwrap_or(0) as usize,
            total_amount: breadth.total_amount.unwrap_or_default().try_into().unwrap_or(0.0),
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct MarketOverview {
    pub date: NaiveDate,
    pub indices: Vec<IndexData>,
    pub up_count: usize,
    pub down_count: usize,
    pub limit_up_count: usize,
    pub total_amount: f64,
}
```

**Step 2: Implement trend_analyzer.rs**

```rust
// src/services/trend_analyzer.rs
use crate::data::types::Candle;
use crate::signals::base::sma;

#[derive(Debug, Clone, serde::Serialize)]
pub enum TrendStatus {
    StrongBull,   // 强势多头
    Bull,         // 多头排列
    WeakBull,     // 弱势多头
    Consolidation,// 盘整
    WeakBear,     // 弱势空头
    Bear,         // 空头排列
    StrongBear,   // 强势空头
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum BuySignal {
    StrongBuy,  // 强烈买入
    Buy,        // 买入
    Hold,       // 持有
    Wait,       // 观望
    Sell,       // 卖出
    StrongSell, // 强烈卖出
}

#[derive(Debug, serde::Serialize)]
pub struct TrendAnalysis {
    pub code: String,
    pub trend_status: TrendStatus,
    pub buy_signal: BuySignal,
    pub score: f64,
    pub ma5: f64,
    pub ma10: f64,
    pub ma20: f64,
    pub ma60: f64,
    pub price: f64,
    pub bias_ma20: f64,
}

pub struct TrendAnalyzer;

impl TrendAnalyzer {
    pub fn analyze(code: &str, bars: &[Candle]) -> Option<TrendAnalysis> {
        if bars.len() < 61 { return None; }
        let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
        let price = *closes.last()?;
        let ma5 = sma(&closes, 5)?;
        let ma10 = sma(&closes, 10)?;
        let ma20 = sma(&closes, 20)?;
        let ma60 = sma(&closes, 60)?;

        let trend_status = if ma5 > ma10 && ma10 > ma20 && ma20 > ma60 {
            let spread = (ma5 - ma60) / ma60 * 100.0;
            if spread > 5.0 { TrendStatus::StrongBull } else { TrendStatus::Bull }
        } else if ma5 > ma10 {
            TrendStatus::WeakBull
        } else if (ma5 - ma10).abs() / ma10 * 100.0 < 1.0 {
            TrendStatus::Consolidation
        } else if ma5 < ma10 && ma10 > ma20 {
            TrendStatus::WeakBear
        } else if ma5 < ma10 && ma10 < ma20 && ma20 < ma60 {
            TrendStatus::StrongBear
        } else {
            TrendStatus::Bear
        };

        let bias_ma20 = (price - ma20) / ma20 * 100.0;

        // Score: 0-100
        let score = {
            let mut s = 50.0f64;
            if ma5 > ma10 { s += 10.0; }
            if ma10 > ma20 { s += 10.0; }
            if ma20 > ma60 { s += 10.0; }
            if price > ma5 { s += 5.0; }
            if bias_ma20 > 0.0 && bias_ma20 < 10.0 { s += 5.0; }
            if bias_ma20 < 0.0 { s -= 10.0; }
            s.clamp(0.0, 100.0)
        };

        let buy_signal = if score >= 85.0 { BuySignal::StrongBuy }
            else if score >= 70.0 { BuySignal::Buy }
            else if score >= 55.0 { BuySignal::Hold }
            else if score >= 40.0 { BuySignal::Wait }
            else if score >= 25.0 { BuySignal::Sell }
            else { BuySignal::StrongSell };

        Some(TrendAnalysis {
            code: code.to_string(),
            trend_status,
            buy_signal,
            score,
            ma5, ma10, ma20, ma60,
            price,
            bias_ma20,
        })
    }
}
```

**Step 3: Implement burst_monitor.rs**

```rust
// src/services/burst_monitor.rs
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::data::sina::SinaClient;
use crate::error::Result;
use crate::state::AppState;
use crate::storage::redis_cache::RedisCache;
use crate::telegram::pusher::TelegramPusher;

const PRICE_SURGE_PCT: f64 = 3.0;
const VOLUME_SPIKE_RATIO: f64 = 5.0;
const POLL_INTERVAL_SECS: u64 = 30;

pub struct BurstMonitorService {
    state: Arc<AppState>,
    sina: Arc<SinaClient>,
    pusher: Arc<TelegramPusher>,
    price_state: HashMap<String, f64>,
}

impl BurstMonitorService {
    pub fn new(state: Arc<AppState>, sina: Arc<SinaClient>, pusher: Arc<TelegramPusher>) -> Self {
        BurstMonitorService {
            state,
            sina,
            pusher,
            price_state: HashMap::new(),
        }
    }

    pub async fn run_poll_loop(&mut self) {
        info!("👁️ Burst monitor started");
        loop {
            if let Err(e) = self.poll_once().await {
                warn!("Burst monitor poll error: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        }
    }

    async fn poll_once(&mut self) -> Result<()> {
        // Get all stocks with data
        let codes = crate::storage::postgres::get_stock_codes_with_data(&self.state.db).await?;
        if codes.is_empty() { return Ok(()); }

        // Batch fetch quotes (Sina supports ~100 per call)
        let code_refs: Vec<&str> = codes.iter().map(|s| s.as_str()).collect();
        for batch in code_refs.chunks(100) {
            let quotes = self.sina.get_quotes(batch).await?;
            let mut cache = RedisCache::new(self.state.redis.clone());

            for (code, quote) in &quotes {
                let prev_price = self.price_state.get(code).copied().unwrap_or(quote.prev_close);
                if prev_price == 0.0 { continue; }

                let surge_pct = (quote.price - prev_price) / prev_price * 100.0;

                // Price surge alert
                if surge_pct >= PRICE_SURGE_PCT && !cache.is_burst_alerted(code).await? {
                    let msg = format!(
                        "⚡ 异动提醒\n{} {}\n快速拉升 +{:.1}%\n现价: {:.2}",
                        code, quote.name, surge_pct, quote.price
                    );
                    if let Some(channel) = &self.state.config.stock_alert_channel {
                        let _ = self.pusher.push(channel, &msg).await;
                    }
                    cache.set_burst_alerted(code).await?;
                }

                self.price_state.insert(code.clone(), quote.price);
            }
        }
        Ok(())
    }
}
```

**Step 4: Implement market_report.rs**

```rust
// src/services/market_report.rs
use chrono::{Local, NaiveDate};
use std::sync::Arc;
use tracing::info;

use crate::error::Result;
use crate::services::limit_up::LimitUpService;
use crate::services::market::MarketService;
use crate::services::scanner::ScannerService;
use crate::services::sector::SectorService;
use crate::state::AppState;
use crate::storage::postgres;
use crate::telegram::formatter;

pub struct MarketReportService {
    state: Arc<AppState>,
    market: Arc<MarketService>,
    limit_up: Arc<LimitUpService>,
    sector: Arc<SectorService>,
    scanner: Arc<ScannerService>,
}

impl MarketReportService {
    pub fn new(
        state: Arc<AppState>,
        market: Arc<MarketService>,
        limit_up: Arc<LimitUpService>,
        sector: Arc<SectorService>,
        scanner: Arc<ScannerService>,
    ) -> Self {
        MarketReportService { state, market, limit_up, sector, scanner }
    }

    pub async fn generate_daily(&self, date: NaiveDate) -> Result<String> {
        info!("📝 Generating daily report for {}", date);

        let overview = self.market.get_market_overview(date).await?;
        let limit_summary = self.limit_up.get_summary(date).await?;
        let top_sectors = self.sector.get_top_sectors(date, 5).await?;
        let bottom_sectors = self.sector.get_bottom_sectors(date, 3).await?;

        // Get scan results from cache or DB
        let mut cache = crate::storage::redis_cache::RedisCache::new(self.state.redis.clone());
        let scan_hits: Option<serde_json::Value> = cache.get_scan_results().await.ok().flatten();

        let report = formatter::format_daily_report(
            &overview,
            &limit_summary,
            &top_sectors,
            &bottom_sectors,
            scan_hits.as_ref(),
        );

        postgres::save_report(&self.state.db, "daily", &report).await?;
        info!("✅ Daily report generated ({} chars)", report.len());
        Ok(report)
    }

    pub async fn generate_weekly(&self) -> Result<String> {
        let date = Local::now().naive_local().date();

        // Strongest stocks over 5 days
        let strongest = sqlx::query!(
            r#"SELECT code, name,
               (MAX(close) - MIN(close)) / NULLIF(MIN(close),0) * 100 as gain_pct
               FROM stock_daily_bars
               WHERE trade_date >= $1 AND trade_date <= $2
               GROUP BY code, name
               ORDER BY gain_pct DESC LIMIT 20"#,
            date - chrono::Duration::days(7),
            date
        )
        .fetch_all(&self.state.db)
        .await?;

        let mut report = format!("📅 <b>周报 - {}</b>\n\n", date.format("%Y-%m-%d"));
        report.push_str("🏆 <b>本周涨幅榜 Top 20</b>\n");
        for (i, s) in strongest.iter().enumerate() {
            report.push_str(&format!(
                "{}. {} {} +{:.1}%\n",
                i+1, s.code, s.name.as_deref().unwrap_or(""),
                s.gain_pct.unwrap_or_default()
            ));
        }

        postgres::save_report(&self.state.db, "weekly", &report).await?;
        Ok(report)
    }
}
```

**Step 5: Create stubs for remaining services**

Each file follows the same pattern — create with a minimal struct that compiles:

```rust
// src/services/portfolio.rs
use std::sync::Arc;
use crate::state::AppState;
pub struct PortfolioService { pub state: Arc<AppState> }
impl PortfolioService { pub fn new(state: Arc<AppState>) -> Self { Self { state } } }
```

Repeat for: `watchlist.rs`, `trading_sim.rs`, `daban_sim.rs`, `chip_dist.rs`, `ai_analysis.rs`

**Step 6: Compile check and commit**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
git add src/services/
git commit -m "feat: add MarketService, TrendAnalyzer, BurstMonitor, MarketReport, service stubs"
```

---

## Phase 6: Telegram and REST API

### Task 18: Telegram pusher and formatter

**Files:**
- Create: `src/telegram/mod.rs`
- Create: `src/telegram/pusher.rs`
- Create: `src/telegram/formatter.rs`

**Step 1: Create module**

```bash
mkdir -p src/telegram
```

**Step 2: Implement pusher.rs**

```rust
// src/telegram/pusher.rs
use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::error::{AppError, Result};

const TG_API: &str = "https://api.telegram.org/bot";
const MAX_MESSAGE_LEN: usize = 4096;

pub struct TelegramPusher {
    token: String,
    client: Client,
}

impl TelegramPusher {
    pub fn new(token: String) -> Self {
        TelegramPusher {
            token,
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    pub async fn push(&self, channel: &str, text: &str) -> Result<()> {
        // Split long messages
        for chunk in Self::split_message(text) {
            self.send_message(channel, &chunk).await?;
            if chunk.len() > 100 {
                sleep(Duration::from_millis(500)).await;
            }
        }
        Ok(())
    }

    async fn send_message(&self, chat_id: &str, text: &str) -> Result<()> {
        let url = format!("{}{}/sendMessage", TG_API, self.token);
        let body = json!({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });

        let resp = self.client.post(&url).json(&body).send().await
            .map_err(AppError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_text = resp.text().await.unwrap_or_default();
            // Rate limit: wait and retry once
            if status.as_u16() == 429 {
                warn!("Telegram rate limit hit, waiting 5s...");
                sleep(Duration::from_secs(5)).await;
                let retry = self.client.post(&url).json(&body).send().await
                    .map_err(AppError::Http)?;
                if !retry.status().is_success() {
                    return Err(AppError::Internal(format!("Telegram error: {}", retry.status())));
                }
            } else {
                return Err(AppError::Internal(format!("Telegram {}: {}", status, err_text)));
            }
        }

        Ok(())
    }

    fn split_message(text: &str) -> Vec<String> {
        if text.len() <= MAX_MESSAGE_LEN {
            return vec![text.to_string()];
        }
        text.chars()
            .collect::<Vec<char>>()
            .chunks(MAX_MESSAGE_LEN)
            .map(|c| c.iter().collect())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_short_message() {
        let msg = "Hello";
        assert_eq!(TelegramPusher::split_message(msg), vec!["Hello"]);
    }

    #[test]
    fn test_split_long_message() {
        let msg = "x".repeat(5000);
        let chunks = TelegramPusher::split_message(&msg);
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.len() <= MAX_MESSAGE_LEN));
    }
}
```

**Step 3: Implement formatter.rs**

```rust
// src/telegram/formatter.rs
use crate::services::limit_up::LimitUpSummary;
use crate::services::market::MarketOverview;
use crate::services::sector::SectorRank;

pub fn format_daily_report(
    overview: &MarketOverview,
    limit_up: &LimitUpSummary,
    top_sectors: &[SectorRank],
    bottom_sectors: &[SectorRank],
    scan_results: Option<&serde_json::Value>,
) -> String {
    let mut msg = String::new();

    msg.push_str(&format!(
        "📊 <b>每日市场报告 {}</b>\n\n",
        overview.date.format("%Y-%m-%d")
    ));

    // Indices
    msg.push_str("📈 <b>指数表现</b>\n");
    for idx in &overview.indices {
        let arrow = if idx.change_pct >= 0.0 { "🔺" } else { "🔻" };
        msg.push_str(&format!(
            "{} {} {}{:.2}%\n",
            arrow, idx.name, if idx.change_pct >= 0.0 { "+" } else { "" }, idx.change_pct
        ));
    }

    // Breadth
    msg.push_str(&format!(
        "\n🔢 上涨 {} | 下跌 {} | 涨停 {}\n",
        overview.up_count, overview.down_count, overview.limit_up_count
    ));

    // Limit-up summary
    msg.push_str(&format!(
        "\n🎯 <b>涨停板</b>\n总计: {} | 封: {} | 炸: {} | 炸板率: {:.1}%\n",
        limit_up.total, limit_up.sealed, limit_up.burst, limit_up.burst_rate
    ));

    // Top sectors
    if !top_sectors.is_empty() {
        msg.push_str("\n🏆 <b>板块涨幅榜</b>\n");
        for (i, s) in top_sectors.iter().enumerate() {
            msg.push_str(&format!("{}. {} +{:.2}%\n", i+1, s.name, s.change_pct));
        }
    }

    // Bottom sectors
    if !bottom_sectors.is_empty() {
        msg.push_str("\n📉 <b>板块跌幅榜</b>\n");
        for s in bottom_sectors {
            msg.push_str(&format!("• {} {:.2}%\n", s.name, s.change_pct));
        }
    }

    // Signal scan summary
    if let Some(results) = scan_results {
        if let Some(obj) = results.as_object() {
            let non_empty: Vec<(&String, usize)> = obj.iter()
                .filter_map(|(k, v)| {
                    v.as_array().map(|a| (k, a.len())).filter(|(_, n)| *n > 0)
                })
                .collect();

            if !non_empty.is_empty() {
                msg.push_str("\n📡 <b>信号扫描</b>\n");
                for (signal_id, count) in &non_empty {
                    msg.push_str(&format!("• {}: {} 只\n", signal_id, count));
                }
            }
        }
    }

    msg
}

pub fn format_signal_hits(signal_name: &str, hits: &[crate::services::scanner::SignalHit]) -> String {
    let mut msg = format!("📡 <b>{}</b> — {} 只股票\n\n", signal_name, hits.len());
    for h in hits.iter().take(20) {
        msg.push_str(&format!("{} {} {}\n", h.icon, h.code, h.name));
    }
    if hits.len() > 20 {
        msg.push_str(&format!("... 还有 {} 只\n", hits.len() - 20));
    }
    msg
}
```

**Step 4: Create mod.rs**

```rust
// src/telegram/mod.rs
pub mod pusher;
pub mod formatter;
pub use pusher::TelegramPusher;
```

**Step 5: Add to main.rs and run tests**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test telegram
git add src/telegram/
git commit -m "feat: add TelegramPusher with rate-limit retry and message formatter"
```

---

### Task 19: REST API (Axum routes)

**Files:**
- Create: `src/api/mod.rs`
- Create: `src/api/routes.rs`

**Step 1: Create module**

```bash
mkdir -p src/api
```

**Step 2: Implement routes.rs**

```rust
// src/api/routes.rs
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::signals::registry::SignalRegistry;
use crate::state::AppState;
use crate::storage::postgres;

type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;

fn api_error(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": msg})))
}

fn check_auth(headers: &HeaderMap, api_key: Option<&str>) -> bool {
    match api_key {
        None => true, // No key configured = open
        Some(key) => headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == format!("Bearer {}", key))
            .unwrap_or(false),
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/signals", get(list_signals))
        .route("/api/scan/latest", get(get_scan_latest))
        .route("/api/scan/trigger", post(trigger_scan))
        .route("/api/report/daily", get(get_daily_report))
        .route("/api/market/overview", get(market_overview_stub))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "service": "qbot"}))
}

async fn list_signals() -> Json<Value> {
    let signals: Vec<Value> = SignalRegistry::get_enabled()
        .iter()
        .map(|s| json!({
            "id": s.signal_id(),
            "name": s.display_name(),
            "icon": s.icon(),
            "group": s.group(),
        }))
        .collect();
    Json(json!({"signals": signals, "count": signals.len()}))
}

async fn get_scan_latest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }

    let mut cache = crate::storage::redis_cache::RedisCache::new(state.redis.clone());
    match cache.get_scan_results().await {
        Ok(Some(results)) => Ok(Json(results)),
        Ok(None) => Ok(Json(json!({"status": "no_scan_results"}))),
        Err(e) => Err(api_error(&e.to_string())),
    }
}

async fn trigger_scan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }

    // Spawn background scan
    let state_clone = state.clone();
    tokio::spawn(async move {
        let scanner = crate::services::scanner::ScannerService::new(state_clone);
        if let Err(e) = scanner.run_full_scan().await {
            tracing::warn!("Manual scan failed: {}", e);
        }
    });

    Ok(Json(json!({"status": "scan_started"})))
}

async fn get_daily_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }

    match postgres::get_latest_report(&state.db, "daily").await {
        Ok(Some(content)) => Ok(Json(json!({"content": content}))),
        Ok(None) => Ok(Json(json!({"status": "no_report_yet"}))),
        Err(e) => Err(api_error(&e.to_string())),
    }
}

async fn market_overview_stub() -> Json<Value> {
    Json(json!({"status": "coming_soon"}))
}
```

**Step 3: Create mod.rs**

```rust
// src/api/mod.rs
pub mod routes;
pub use routes::build_router;
```

**Step 4: Add to main.rs and compile check**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo check
git add src/api/
git commit -m "feat: add Axum REST API (health, signals, scan, report endpoints)"
```

---

## Phase 7: Scheduler and Main Boot

### Task 20: Scheduler (cron jobs)

**Files:**
- Create: `src/scheduler/mod.rs`

**Step 1: Implement**

```bash
mkdir -p src/scheduler
```

```rust
// src/scheduler/mod.rs
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, warn};

use crate::data::tushare::TushareClient;
use crate::services::{
    limit_up::LimitUpService,
    market::MarketService,
    market_report::MarketReportService,
    scanner::ScannerService,
    sector::SectorService,
    stock_history::StockHistoryService,
};
use crate::state::AppState;
use crate::telegram::pusher::TelegramPusher;

pub async fn start_scheduler(
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
    pusher: Arc<TelegramPusher>,
) -> anyhow::Result<JobScheduler> {
    let sched = JobScheduler::new().await?;

    // 15:05 weekdays — fetch daily OHLCV + limit-up data
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 5 15 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move {
                info!("⏰ 15:05 — Fetching daily data");
                let today = chrono::Local::now().naive_local().date();
                let history_svc = StockHistoryService::new(s.clone(), p.clone());
                if let Err(e) = history_svc.update_today().await {
                    warn!("Daily data fetch failed: {}", e);
                }
                let limit_svc = LimitUpService::new(s.clone(), p.clone());
                if let Ok(stocks) = limit_svc.fetch_and_save(today).await {
                    info!("Limit-up: {} stocks", stocks.len());
                }
                let sector_svc = SectorService::new(s.clone(), p.clone());
                if let Err(e) = sector_svc.fetch_and_save(today).await {
                    warn!("Sector data failed: {}", e);
                }
            })
        })?).await?;
    }

    // 15:35 weekdays — run full signal scan
    {
        let s = state.clone();
        sched.add(Job::new_async("0 35 15 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone();
            Box::pin(async move {
                info!("⏰ 15:35 — Running full scan");
                let scanner = ScannerService::new(s);
                if let Err(e) = scanner.run_full_scan().await {
                    warn!("Scan failed: {}", e);
                }
            })
        })?).await?;
    }

    // 16:00 weekdays — generate and push daily report
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 0 16 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move {
                info!("⏰ 16:00 — Generating daily report");
                let today = chrono::Local::now().naive_local().date();
                let market_svc = Arc::new(MarketService::new(s.clone(), p.clone()));
                let limit_svc = Arc::new(LimitUpService::new(s.clone(), p.clone()));
                let sector_svc = Arc::new(SectorService::new(s.clone(), p.clone()));
                let scanner_svc = Arc::new(ScannerService::new(s.clone()));
                let report_svc = MarketReportService::new(
                    s.clone(), market_svc, limit_svc, sector_svc, scanner_svc
                );
                match report_svc.generate_daily(today).await {
                    Ok(report) => {
                        if let Some(channel) = &s.config.report_channel {
                            let _ = push.push(channel, &report).await;
                        }
                    }
                    Err(e) => warn!("Daily report failed: {}", e),
                }
            })
        })?).await?;
    }

    // 20:00 Friday — weekly report
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 0 20 * * Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move {
                info!("⏰ Friday 20:00 — Weekly report");
                let market_svc = Arc::new(MarketService::new(s.clone(), p.clone()));
                let limit_svc = Arc::new(LimitUpService::new(s.clone(), p.clone()));
                let sector_svc = Arc::new(SectorService::new(s.clone(), p.clone()));
                let scanner_svc = Arc::new(ScannerService::new(s.clone()));
                let report_svc = MarketReportService::new(
                    s.clone(), market_svc, limit_svc, sector_svc, scanner_svc
                );
                match report_svc.generate_weekly().await {
                    Ok(report) => {
                        if let Some(channel) = &s.config.report_channel {
                            let _ = push.push(channel, &report).await;
                        }
                    }
                    Err(e) => warn!("Weekly report failed: {}", e),
                }
            })
        })?).await?;
    }

    sched.start().await?;
    info!("✅ Scheduler started with 4 jobs");
    Ok(sched)
}
```

**Step 2: Commit**

```bash
git add src/scheduler/
git commit -m "feat: add cron scheduler (15:05 fetch, 15:35 scan, 16:00 report, Friday weekly)"
```

---

### Task 21: Final main.rs wiring

**Files:**
- Modify: `src/main.rs`

**Step 1: Full main.rs**

```rust
// src/main.rs
mod api;
mod config;
mod data;
mod error;
mod scheduler;
mod services;
mod signals;
mod state;
mod storage;
mod telegram;

use anyhow::Result;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "qbot=info,sqlx=warn,tokio_cron_scheduler=warn".into()),
        )
        .init();

    info!("🚀 qbot starting...");

    let config = config::Config::from_env()?;
    let api_port = config.api_port;
    info!("✅ Config loaded (port: {})", api_port);

    // PostgreSQL
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;
    storage::postgres::run_migrations(&db).await?;
    info!("✅ PostgreSQL connected + migrations applied");

    // Redis
    let redis_client = redis::Client::open(config.redis_url.as_str())?;
    let redis = redis::aio::ConnectionManager::new(redis_client).await?;
    info!("✅ Redis connected");

    let state = Arc::new(state::AppState {
        config: Arc::new(config.clone()),
        db,
        redis,
    });

    // Initialize signal registry
    signals::registry::SignalRegistry::init();

    // Data provider and Telegram pusher
    let provider = Arc::new(data::tushare::TushareClient::new(
        config.tushare_token.clone(),
        config.data_proxy.as_deref(),
    ));
    let pusher = Arc::new(telegram::TelegramPusher::new(config.telegram_bot_token.clone()));

    // Check if first-run backfill needed
    {
        let history_svc = services::stock_history::StockHistoryService::new(
            state.clone(), provider.clone()
        );
        if !history_svc.has_today_data().await {
            info!("📥 First run detected — starting 3-year backfill in background");
            let state_clone = state.clone();
            let provider_clone = provider.clone();
            tokio::spawn(async move {
                let svc = services::stock_history::StockHistoryService::new(state_clone, provider_clone);
                if let Err(e) = svc.backfill(3).await {
                    tracing::warn!("Backfill failed: {}", e);
                }
            });
        }
    }

    // Start scheduler
    let _sched = scheduler::start_scheduler(state.clone(), provider.clone(), pusher.clone()).await?;
    info!("✅ Scheduler started");

    // Start Axum REST API
    let router = api::build_router(state.clone());
    let addr = format!("0.0.0.0:{}", api_port).parse::<std::net::SocketAddr>()?;
    info!("✅ API server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
        })
        .await?;

    Ok(())
}
```

**Step 2: Full compile check**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo build 2>&1 | head -50
```

Fix any compile errors. Expected: `Finished dev`.

**Step 3: Run tests**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test
```

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: complete main.rs boot sequence with all services wired up"
```

---

## Phase 8: Deployment

### Task 22: Docker Compose (PostgreSQL + Redis)

**Files:**
- Create: `deploy/docker-compose.yml`
- Create: `deploy/setup.sh`
- Create: `deploy/qbot.service`

**Step 1: Create deploy directory**

```bash
mkdir -p deploy
```

**Step 2: docker-compose.yml**

```yaml
# deploy/docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    environment:
      POSTGRES_USER: qbot
      POSTGRES_PASSWORD: qbot
      POSTGRES_DB: qbot
    volumes:
      - postgres-data:/var/lib/postgresql/data
    ports:
      - "127.0.0.1:5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U qbot -d qbot"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    restart: unless-stopped
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes
    ports:
      - "127.0.0.1:6379:6379"

volumes:
  postgres-data:
  redis-data:
```

**Step 3: systemd service**

```ini
# deploy/qbot.service
[Unit]
Description=qbot A-Share Analysis Service
Documentation=https://github.com/yourname/qbot
After=network.target docker.service
Wants=docker.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/qbot
ExecStart=/opt/qbot/qbot
Restart=always
RestartSec=10
EnvironmentFile=/opt/qbot/.env
StandardOutput=journal
StandardError=journal
SyslogIdentifier=qbot

[Install]
WantedBy=multi-user.target
```

**Step 4: setup.sh**

```bash
#!/bin/bash
# deploy/setup.sh — First-run VPS bootstrap
set -e

echo "🚀 qbot VPS Setup"

# Install Rust if not present
if ! command -v cargo &>/dev/null && [ ! -f "/usr/lib/rust-1.80/bin/cargo" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi

# Install Docker if not present
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | sh
    sudo systemctl enable docker
    sudo systemctl start docker
fi

# Create deployment directory
sudo mkdir -p /opt/qbot
sudo chown ubuntu:ubuntu /opt/qbot

# Copy .env (user must fill this in first)
if [ ! -f /opt/qbot/.env ]; then
    cp /opt/qbot/.env.example /opt/qbot/.env
    echo "⚠️  Please fill in /opt/qbot/.env before continuing"
    exit 1
fi

# Start database services
cd /opt/qbot
docker compose -f deploy/docker-compose.yml up -d
echo "⏳ Waiting for PostgreSQL..."
sleep 10

# Build and install binary
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo build --release
cp target/release/qbot /opt/qbot/qbot

# Install systemd service
sudo cp deploy/qbot.service /etc/systemd/system/qbot.service
sudo systemctl daemon-reload
sudo systemctl enable qbot
sudo systemctl start qbot

echo "✅ qbot deployed! Check: sudo journalctl -u qbot -f"
```

**Step 5: Commit**

```bash
git add deploy/
git commit -m "feat: add Docker Compose (PG+Redis), systemd service, setup.sh"
```

---

### Task 23: GitHub Actions workflows

**Files:**
- Create: `.github/workflows/test.yml`
- Create: `.github/workflows/deploy.yml`

**Step 1: test.yml**

```yaml
# .github/workflows/test.yml
name: Test

on:
  push:
    branches: ["**"]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.80"
          components: clippy

      - name: Cache cargo
        uses: actions/cache@v4
        with:
          path: |
            ~/.cargo/registry
            ~/.cargo/git
            target
          key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}

      - name: Cargo check
        run: cargo check

      - name: Clippy
        run: cargo clippy -- -D warnings

      - name: Tests (no DB needed)
        run: cargo test --lib
```

**Step 2: deploy.yml**

```yaml
# .github/workflows/deploy.yml
name: Deploy

on:
  push:
    branches: [main]
  workflow_dispatch:
    inputs:
      force_rebuild:
        description: "Force cargo build --release"
        type: boolean
        default: false

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: VPS

    steps:
      - uses: actions/checkout@v4

      - name: Check if deployment enabled
        id: check
        run: |
          if [ "${{ secrets.DEPLOY_ENABLED }}" != "true" ]; then
            echo "skip=true" >> $GITHUB_OUTPUT
          else
            echo "skip=false" >> $GITHUB_OUTPUT
          fi

      - name: Deploy to VPS
        if: steps.check.outputs.skip != 'true'
        uses: appleboy/ssh-action@v1.0.3
        with:
          host: ${{ secrets.VPS_HOST }}
          username: ${{ secrets.VPS_USER }}
          key: ${{ secrets.VPS_SSH_KEY }}
          script: |
            set -e
            cd /opt/qbot
            git pull origin main

            export PATH="/usr/lib/rust-1.80/bin:$PATH"
            cargo build --release

            cp target/release/qbot /opt/qbot/qbot
            sudo systemctl restart qbot
            sleep 3
            sudo systemctl is-active qbot && echo "✅ qbot restarted successfully"

      - name: Health check
        if: steps.check.outputs.skip != 'true'
        uses: appleboy/ssh-action@v1.0.3
        with:
          host: ${{ secrets.VPS_HOST }}
          username: ${{ secrets.VPS_USER }}
          key: ${{ secrets.VPS_SSH_KEY }}
          script: |
            curl -sf http://localhost:${{ secrets.API_PORT || 8080 }}/health || exit 1
            echo "✅ Health check passed"
```

**Step 3: Required GitHub Secrets**

Configure these in GitHub → Settings → Environments → VPS:
- `DEPLOY_ENABLED` = `true`
- `VPS_HOST` = your VPS IP
- `VPS_USER` = `ubuntu`
- `VPS_SSH_KEY` = private SSH key

**Step 4: Commit**

```bash
mkdir -p .github/workflows
git add .github/
git commit -m "feat: add GitHub Actions test and deploy workflows"
```

---

## Phase 9: Final Verification

### Task 24: End-to-end compile and test

**Step 1: Full clean build**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo clean
cargo build 2>&1
```

Expected: `Finished dev [unoptimized + debuginfo]`

**Step 2: All unit tests**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo test --lib -- --nocapture 2>&1
```

Expected: All tests pass. Key ones:
- `config::tests::test_config_defaults`
- `signals::base::tests::test_sma`
- `data::tushare::tests::test_tushare_code_convert`
- `data::sina::tests::test_sina_code_convert`
- `telegram::pusher::tests::test_split_short_message`
- `telegram::pusher::tests::test_split_long_message`

**Step 3: Clippy clean**

```bash
export PATH="/usr/lib/rust-1.80/bin:$PATH"
cargo clippy 2>&1
```

Expected: No errors (warnings allowed).

**Step 4: Final commit**

```bash
git add -A
git commit -m "feat: complete qbot implementation — all services, signals, API, scheduler, deploy"
```

---

## Quick Reference: Running locally

```bash
# 1. Start databases
cd deploy && docker compose up -d && cd ..

# 2. Set env
cp .env.example .env
# Fill in TUSHARE_TOKEN, TELEGRAM_BOT_TOKEN, etc.

# 3. Run
export PATH="/usr/lib/rust-1.80/bin:$PATH"
DATABASE_URL=postgresql://qbot:qbot@localhost/qbot cargo run

# 4. Test API
curl http://localhost:8080/health
curl http://localhost:8080/api/signals
```

---

## Extending: Adding a new signal

1. Create `src/signals/<group>/my_signal.rs`
2. Implement `SignalDetector` trait
3. Add `Box::new(MySignal)` to `SignalRegistry::init()` in `src/signals/registry.rs`
4. Run `cargo test signals` — that's it.

## Extending: Adding a new service

1. Create `src/services/my_service.rs` with a struct holding `Arc<AppState>`
2. Add `pub mod my_service;` to `src/services/mod.rs`
3. Wire into scheduler in `src/scheduler/mod.rs` or call from existing services
