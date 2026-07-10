# Point-in-Time Data Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the minimum point-in-time market-data foundation required for trustworthy pattern research and event replay, without producing new stock candidates.

**Architecture:** Introduce a new `analysis::market_snapshot` deep module backed by a focused `market_repository`. Extend Tushare through a separate point-in-time provider interface rather than broadening every fallback provider. Persist provenance and `available_at`; reject incomplete research dates instead of silently imputing critical fields.

**Tech Stack:** Rust 2021, SQLx/PostgreSQL, Tokio, Axum, Tushare provider.

## Global Constraints

- This phase must not create pattern candidates or event analysis.
- Existing bars with unknown historical availability remain `NULL`; do not invent precise availability timestamps.
- Research queries exclude records whose `available_at` is unknown unless an explicit backfill marks them `estimated`.
- Rust must not spawn Python.
- New SQL belongs in `src/storage/market_repository.rs`, not in the existing `postgres.rs`.
- Existing scanner behavior must remain unchanged.
- Every point-in-time query accepts an explicit `as_of` timestamp.
- Migration number is `013`.

---

### Task 1: Add point-in-time market-data schema

**Files:**
- Create: `migrations/013_point_in_time_market_data.sql`
- Test: `src/storage/market_repository.rs`

**Interfaces:**
- Produces tables used by all later tasks.
- Does not change existing repository function signatures.

- [x] **Step 1: Create the migration**

```sql
CREATE TABLE IF NOT EXISTS stock_daily_bar_versions (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    open                 NUMERIC(10,3),
    high                 NUMERIC(10,3),
    low                  NUMERIC(10,3),
    close                NUMERIC(10,3),
    volume               BIGINT,
    amount               NUMERIC(18,2),
    turnover             NUMERIC(8,4),
    pe                   NUMERIC(12,4),
    pb                   NUMERIC(8,4),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS stock_daily_basic_versions (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    turnover_rate        NUMERIC(10,4),
    volume_ratio         NUMERIC(10,4),
    pe                   NUMERIC(14,4),
    pb                   NUMERIC(14,4),
    ps                   NUMERIC(14,4),
    total_share          NUMERIC(20,4),
    float_share          NUMERIC(20,4),
    total_mv             NUMERIC(20,4),
    circ_mv              NUMERIC(20,4),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS security_master_versions (
    code                 VARCHAR(12) NOT NULL,
    name                 VARCHAR(100) NOT NULL,
    market               VARCHAR(20),
    exchange             VARCHAR(20),
    list_status          VARCHAR(10) NOT NULL,
    list_date            DATE,
    delist_date          DATE,
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, available_at)
);

CREATE TABLE IF NOT EXISTS corporate_action_versions (
    source               VARCHAR(32) NOT NULL,
    action_key           VARCHAR(200) NOT NULL,
    code                 VARCHAR(12) NOT NULL,
    action_type          VARCHAR(40) NOT NULL,
    announcement_date    DATE,
    record_date          DATE,
    ex_date              DATE,
    pay_date             DATE,
    cash_dividend        NUMERIC(18,8),
    stock_ratio          NUMERIC(18,8),
    rights_ratio         NUMERIC(18,8),
    rights_price         NUMERIC(18,8),
    raw_payload          JSONB NOT NULL DEFAULT '{}',
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, action_key, available_at)
);

CREATE TABLE IF NOT EXISTS sector_daily_versions (
    code                 VARCHAR(20) NOT NULL,
    name                 VARCHAR(100),
    sector_type          VARCHAR(20),
    change_pct           NUMERIC(8,4),
    amount               NUMERIC(18,2),
    trade_date           DATE NOT NULL,
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS limit_up_stock_versions (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    name                 VARCHAR(50),
    streak               INT,
    limit_time           VARCHAR(10),
    seal_amount          NUMERIC(18,2),
    burst_count          INT,
    score                NUMERIC(5,2),
    board_type           VARCHAR(20),
    close                NUMERIC(10,3),
    pct_chg              NUMERIC(8,4),
    strth                NUMERIC(8,4),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS stock_adjustment_factors (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    adj_factor           NUMERIC(18,8) NOT NULL,
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS security_daily_status (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    listed_days          INT,
    is_st                BOOLEAN NOT NULL DEFAULT FALSE,
    is_suspended         BOOLEAN NOT NULL DEFAULT FALSE,
    price_limit_pct      NUMERIC(8,4),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS index_daily_bars (
    code                 VARCHAR(20) NOT NULL,
    trade_date           DATE NOT NULL,
    close                NUMERIC(14,4) NOT NULL,
    change_pct           NUMERIC(10,4),
    volume               BIGINT,
    amount               NUMERIC(20,2),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS stock_sector_membership (
    code                 VARCHAR(12) NOT NULL,
    sector_code          VARCHAR(20) NOT NULL,
    sector_name          VARCHAR(100) NOT NULL,
    sector_type          VARCHAR(20) NOT NULL,
    valid_from           DATE NOT NULL,
    valid_to             DATE,
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, sector_code, valid_from, available_at)
);

CREATE TABLE IF NOT EXISTS market_daily_snapshots (
    trade_date        DATE NOT NULL,
    snapshot_version  VARCHAR(32) NOT NULL,
    available_at      TIMESTAMPTZ NOT NULL,
    data_complete     BOOLEAN NOT NULL,
    metrics           JSONB NOT NULL,
    missing_inputs    JSONB NOT NULL DEFAULT '[]',
    input_fingerprint VARCHAR(64) NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, snapshot_version)
);

CREATE TABLE IF NOT EXISTS analysis_data_runs (
    run_id            UUID PRIMARY KEY,
    run_type          VARCHAR(50) NOT NULL,
    trade_date        DATE,
    status            VARCHAR(20) NOT NULL,
    input_fingerprint VARCHAR(64),
    details           JSONB NOT NULL DEFAULT '{}',
    error_message     TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_bar_versions_available
    ON stock_daily_bar_versions(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_daily_basic_versions_available
    ON stock_daily_basic_versions(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_security_master_available
    ON security_master_versions(code, available_at);

CREATE INDEX IF NOT EXISTS idx_corporate_actions_available
    ON corporate_action_versions(code, available_at, ex_date);

CREATE INDEX IF NOT EXISTS idx_sector_versions_available
    ON sector_daily_versions(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_limit_versions_available
    ON limit_up_stock_versions(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_adjustment_available
    ON stock_adjustment_factors(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_security_status_available
    ON security_daily_status(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_index_daily_available
    ON index_daily_bars(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_sector_membership_effective
    ON stock_sector_membership(code, valid_from, valid_to, available_at);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_date
    ON market_daily_snapshots(trade_date DESC, snapshot_version);
```

- [x] **Step 2: Add a migration smoke test**

Append to the future `src/storage/market_repository.rs` test module:

```rust
#[sqlx::test(migrations = "./migrations")]
async fn point_in_time_tables_exist(pool: PgPool) -> sqlx::Result<()> {
    let tables: Vec<(String,)> = sqlx::query_as(
        r#"SELECT table_name
           FROM information_schema.tables
           WHERE table_schema = 'public'
             AND table_name = ANY($1)
           ORDER BY table_name"#,
    )
    .bind(vec![
        "analysis_data_runs".to_string(),
        "corporate_action_versions".to_string(),
        "index_daily_bars".to_string(),
        "limit_up_stock_versions".to_string(),
        "market_daily_snapshots".to_string(),
        "security_daily_status".to_string(),
        "security_master_versions".to_string(),
        "sector_daily_versions".to_string(),
        "stock_adjustment_factors".to_string(),
        "stock_daily_bar_versions".to_string(),
        "stock_daily_basic_versions".to_string(),
        "stock_sector_membership".to_string(),
    ])
    .fetch_all(&pool)
    .await?;

    assert_eq!(tables.len(), 12);
    Ok(())
}
```

- [x] **Step 3: Run the migration test**

Run:

```bash
cargo test point_in_time_tables_exist -- --nocapture
```

Expected: PASS when SQLx can create the test database.

- [x] **Step 4: Commit**

```bash
git add migrations/013_point_in_time_market_data.sql src/storage/market_repository.rs
git commit -m "feat: add point-in-time market data schema"
```

---

### Task 2: Add point-in-time domain contracts

**Files:**
- Create: `src/analysis/mod.rs`
- Create: `src/analysis/contracts.rs`
- Create: `src/analysis/market_snapshot/mod.rs`
- Create: `src/analysis/market_snapshot/contracts.rs`
- Modify: `src/main.rs`

**Interfaces:**
- Produces shared Rust types for repository, provider, and snapshot tasks.
- No database calls in contract files.

- [x] **Step 1: Write contract unit tests**

Create `src/analysis/market_snapshot/contracts.rs` with tests first:

```rust
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
```

- [x] **Step 2: Verify the test fails**

Run:

```bash
cargo test point_in_time_context_rejects_data_available_after_cutoff
```

Expected: FAIL because the module and types do not exist.

- [x] **Step 3: Implement the contracts**

```rust
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
```

Create module exports:

```rust
// src/analysis/mod.rs
pub mod contracts;
pub mod market_snapshot;

// src/analysis/market_snapshot/mod.rs
pub mod contracts;
pub use contracts::*;
```

Add to `src/main.rs`:

```rust
mod analysis;
```

- [x] **Step 4: Run unit tests**

Run:

```bash
cargo test point_in_time_context_rejects_data_available_after_cutoff
```

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/analysis src/main.rs
git commit -m "feat: add point-in-time analysis contracts"
```

---

### Task 3: Add a focused market repository

**Files:**
- Create: `src/storage/market_repository.rs`
- Modify: `src/storage/mod.rs`
- Test: `src/storage/market_repository.rs`

**Interfaces:**
- Produces `MarketRepository::new(PgPool)`.
- Produces point-in-time reads and upserts.
- Consumes contracts from Task 2.

- [x] **Step 1: Write a failing point-in-time query test**

```rust
#[sqlx::test(migrations = "./migrations")]
async fn latest_adjustment_factor_respects_as_of(pool: PgPool) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO stock_adjustment_factors
           (code, trade_date, adj_factor, available_at, availability_quality, source)
           VALUES
           ('600000.SH', '2026-07-09', 1.1, '2026-07-09T10:00:00Z', 'observed', 'test'),
           ('600000.SH', '2026-07-10', 1.2, '2026-07-10T10:00:00Z', 'observed', 'test')"#,
    )
    .execute(&pool)
    .await?;

    let repo = MarketRepository::new(pool);
    let value = repo
        .latest_adjustment_factor(
            "600000.SH",
            chrono::NaiveDate::from_ymd_opt(2026, 7, 10).unwrap(),
            chrono::DateTime::parse_from_rfc3339("2026-07-10T09:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        )
        .await
        .unwrap();

    assert_eq!(value.unwrap().trade_date.to_string(), "2026-07-09");
    Ok(())
}
```

- [x] **Step 2: Verify the test fails**

Run:

```bash
cargo test latest_adjustment_factor_respects_as_of
```

Expected: FAIL because `MarketRepository` does not exist.

- [x] **Step 3: Implement the repository skeleton**

```rust
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgPool;

use crate::analysis::market_snapshot::{
    AdjustmentFactor, AvailabilityQuality, IndexDailyBar, MarketSnapshot,
    SecurityDailyStatus, SectorMembership,
};
use crate::error::Result;

#[derive(Clone)]
pub struct MarketRepository {
    pool: PgPool,
}

impl MarketRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn latest_adjustment_factor(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Option<AdjustmentFactor>> {
        let row: Option<(
            String,
            NaiveDate,
            f64,
            DateTime<Utc>,
            DateTime<Utc>,
            String,
            String,
        )> = sqlx::query_as(
                r#"SELECT code, trade_date, adj_factor::float8, available_at,
                          ingested_at, availability_quality, source
                   FROM stock_adjustment_factors
                   WHERE code = $1
                     AND trade_date <= $2
                     AND available_at <= $3
                   ORDER BY trade_date DESC, available_at DESC
                   LIMIT 1"#,
            )
            .bind(code)
            .bind(trade_date)
            .bind(as_of)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(
            |(code, trade_date, adj_factor, available_at, ingested_at, quality, source)| {
                AdjustmentFactor {
                    code,
                    trade_date,
                    adj_factor,
                    available_at,
                    ingested_at,
                    availability_quality: parse_quality(&quality),
                    source,
                }
            },
        ))
    }
}

fn parse_quality(value: &str) -> AvailabilityQuality {
    match value {
        "observed" => AvailabilityQuality::Observed,
        _ => AvailabilityQuality::Estimated,
    }
}
```

Add:

```rust
// src/storage/mod.rs
pub mod market_repository;
```

- [x] **Step 4: Add remaining repository methods**

Implement:

```rust
pub async fn append_daily_basics(&self, rows: &[DailyBasicSnapshot]) -> Result<usize>;
pub async fn append_security_master_versions(&self, rows: &[SecurityMasterVersion]) -> Result<usize>;
pub async fn append_corporate_actions(&self, rows: &[CorporateAction]) -> Result<usize>;
pub async fn append_adjustment_factors(&self, rows: &[AdjustmentFactor]) -> Result<usize>;
pub async fn append_security_statuses(&self, rows: &[SecurityDailyStatus]) -> Result<usize>;
pub async fn append_index_bars(&self, rows: &[IndexDailyBar]) -> Result<usize>;
pub async fn append_sector_memberships(&self, rows: &[SectorMembership]) -> Result<usize>;
pub async fn security_master(&self, code: &str, as_of: DateTime<Utc>) -> Result<Option<SecurityMasterVersion>>;
pub async fn daily_basic(&self, code: &str, trade_date: NaiveDate, as_of: DateTime<Utc>) -> Result<Option<DailyBasicSnapshot>>;
pub async fn corporate_actions(&self, code: &str, end: NaiveDate, as_of: DateTime<Utc>) -> Result<Vec<CorporateAction>>;
pub async fn security_status(&self, code: &str, trade_date: NaiveDate, as_of: DateTime<Utc>) -> Result<Option<SecurityDailyStatus>>;
pub async fn active_sector_memberships(&self, code: &str, trade_date: NaiveDate, as_of: DateTime<Utc>) -> Result<Vec<SectorMembership>>;
pub async fn index_history(&self, code: &str, end: NaiveDate, as_of: DateTime<Utc>, limit: i64) -> Result<Vec<IndexDailyBar>>;
pub async fn save_market_snapshot(&self, snapshot: &MarketSnapshot) -> Result<()>;
pub async fn market_snapshot(&self, trade_date: NaiveDate, version: &str) -> Result<Option<MarketSnapshot>>;
```

Version tables are append-only by `(entity, trade_date, available_at)`. Use `ON CONFLICT ... DO NOTHING`; never update an earlier observation. The legacy current-state tables continue to use their existing upsert behavior.

- [x] **Step 5: Run repository tests**

Run:

```bash
cargo test storage::market_repository -- --nocapture
```

Expected: PASS.

- [x] **Step 6: Commit**

```bash
git add src/storage/market_repository.rs src/storage/mod.rs
git commit -m "feat: add point-in-time market repository"
```

---

### Task 4: Probe source capabilities and add a separate point-in-time provider interface

**Files:**
- Create: `src/data/point_in_time_provider.rs`
- Modify: `src/data/mod.rs`
- Modify: `src/data/tushare.rs`
- Modify: `src/state.rs`
- Modify: `src/main.rs`
- Test: `src/data/tushare.rs`

**Interfaces:**
- Produces `PointInTimeDataProvider`.
- `TushareClient` implements only capabilities verified for the configured account.
- Existing fallback providers remain unchanged.
- Missing critical capabilities block Phase 1 instead of triggering inferred history.

- [x] **Step 1: Add the capability and provider contracts**

```rust
use async_trait::async_trait;
use chrono::NaiveDate;
use std::collections::BTreeMap;

use crate::analysis::market_snapshot::{
    AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar,
    SecurityDailyStatus, SecurityMasterVersion, SectorMembership,
};
use crate::error::Result;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PointInTimeCapabilities {
    pub security_master_history: bool,
    pub corporate_actions: bool,
    pub adjustment_factors: bool,
    pub daily_basic: bool,
    pub daily_security_status: bool,
    pub historical_index_bars: bool,
    pub historical_sector_membership: bool,
    pub details: BTreeMap<String, String>,
}

#[async_trait]
pub trait PointInTimeDataProvider: Send + Sync {
    async fn probe_capabilities(&self) -> Result<PointInTimeCapabilities>;

    async fn get_security_master_versions(
        &self,
    ) -> Result<Vec<SecurityMasterVersion>>;

    async fn get_corporate_actions(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<CorporateAction>>;

    async fn get_adjustment_factors(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<AdjustmentFactor>>;

    async fn get_daily_basics(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<DailyBasicSnapshot>>;

    async fn get_security_statuses(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<SecurityDailyStatus>>;

    async fn get_index_daily_range(
        &self,
        codes: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<IndexDailyBar>>;

    async fn get_sector_memberships(
        &self,
        as_of_date: NaiveDate,
    ) -> Result<Vec<SectorMembership>>;
}
```

Export from `src/data/mod.rs`:

```rust
pub mod point_in_time_provider;
```

- [x] **Step 2: Add bounded capability probes and parser tests**

The probe performs small sample requests and records unsupported or unauthorized capabilities without substituting current-state data.

Add fixture-based parser tests for security master history, corporate actions, daily basics, adjustment factors, and security status:

```rust
assert_eq!(rows[0].code, "600000.SH");
assert_eq!(rows[0].adj_factor, 1.2345);
assert_eq!(rows[0].availability_quality, AvailabilityQuality::Observed);
```

Add a capability test asserting an unauthorized historical-membership endpoint returns:

```rust
assert!(!capabilities.historical_sector_membership);
assert!(capabilities.details["historical_sector_membership"].contains("unauthorized"));
```

- [x] **Step 3: Implement verified Tushare methods**

Use private Tushare calls for capabilities confirmed by the probe. Map `available_at` to the actual QBot fetch time:

```rust
let fetched_at = chrono::Utc::now();
```

Live fetches use `AvailabilityQuality::Observed`. Historical backfills whose true first publication time is unknown use `AvailabilityQuality::Estimated`.

If security master history, corporate actions, daily basics, historical membership, or daily status is unsupported, return an explicit `AppError::DataProvider` from that method. Never derive historical membership from current `stock_info.industry`, never infer historical ST state from the current security name, and never approximate market cap from price alone.

- [x] **Step 4: Wire the dedicated provider into state**

Add to `AppState`:

```rust
pub point_in_time_provider:
    Arc<dyn crate::data::point_in_time_provider::PointInTimeDataProvider>,
```

Change main initialization:

```rust
let tushare_client = Arc::new(data::tushare::TushareClient::new(
    config.tushare_token.clone(),
    config.data_proxy.as_deref(),
));
let tushare_provider: Arc<dyn DataProvider> = tushare_client.clone();
let point_in_time_provider:
    Arc<dyn data::point_in_time_provider::PointInTimeDataProvider> =
    tushare_client.clone();
```

Pass `point_in_time_provider` into `AppState`.

- [x] **Step 5: Persist and expose capability status**

Store the latest probe result in `analysis_data_runs.details` with `run_type='point_in_time_capability_probe'`. Add it to `/api/analysis/data-status` so Phase 1 can show exactly which prerequisite is missing.

- [x] **Step 6: Run tests**

Run:

```bash
cargo test data::tushare -- --nocapture
cargo test config::tests::test_config_defaults
```

Expected: PASS, including explicit unsupported-capability behavior.

- [x] **Step 7: Commit**

```bash
git add src/data src/state.rs src/main.rs
git commit -m "feat: probe point-in-time data capabilities"
```

---

### Task 5: Preserve availability metadata during daily ingestion

**Files:**
- Modify: `src/storage/postgres.rs`
- Modify: `src/storage/market_repository.rs`
- Modify: `src/services/stock_history.rs`
- Modify: `src/services/sector.rs`
- Modify: `src/services/limit_up.rs`
- Test: `src/storage/postgres.rs`
- Test: `src/storage/market_repository.rs`

**Interfaces:**
- Existing function names remain available.
- New live observations use observed timestamps.
- Historical backfills append estimated observations without overwriting them later.

- [x] **Step 1: Write failing current-view and version-history tests**

Add SQLx tests proving that:

- a second legacy upsert updates `turnover`, `pe`, and `pb`, not just OHLCV;
- two observations for the same `(code, trade_date)` with different `available_at` values both remain in `stock_daily_bar_versions`;
- a repeated observation with the same `(code, trade_date, available_at)` is idempotent.

- [x] **Step 2: Verify failure**

Run:

```bash
cargo test daily_bar_upsert_updates_fundamentals append_daily_bar_versions_is_point_in_time_safe
```

Expected: FAIL because the current legacy `ON CONFLICT` clause omits fundamental fields and the append-only version function does not exist.

- [x] **Step 3: Extend the upsert signature**

Keep the existing legacy signature and current-state table behavior:

```rust
pub async fn upsert_daily_bars(
    pool: &PgPool,
    bars: &[(String, Candle)],
) -> Result<usize>
```

Add an append-only method to `MarketRepository`:

```rust
pub async fn append_daily_bar_versions(
    &self,
    bars: &[(String, Candle)],
    available_at: chrono::DateTime<chrono::Utc>,
    availability_quality: &str,
    source: &str,
) -> Result<usize>
```

The legacy table remains the current operational view. Research and replay read only `stock_daily_bar_versions`.

- [x] **Step 4: Update call sites**

Daily live ingestion:

```rust
postgres::upsert_daily_bars(&self.state.db, &bars).await?;
MarketRepository::new(self.state.db.clone())
    .append_daily_bar_versions(
        &bars,
        chrono::Utc::now(),
        "observed",
        self.provider.name(),
    )
    .await?;
```

Historical backfill:

```rust
postgres::upsert_daily_bars(&self.state.db, &bars).await?;
MarketRepository::new(self.state.db.clone())
    .append_daily_bar_versions(
        &bars,
        chrono::Utc::now(),
        "estimated",
        self.provider.name(),
    )
    .await?;
```

Add `MarketRepository::append_sector_versions` and `MarketRepository::append_limit_up_versions` with the same append-only rule. Existing sector and limit-up tables remain current operational views.

- [x] **Step 5: Run tests**

Run:

```bash
cargo test daily_bar_upsert_updates_fundamentals append_daily_bar_versions_is_point_in_time_safe -- --nocapture
cargo test --all --locked
```

Expected: all non-environment-dependent tests PASS.

- [x] **Step 6: Commit**

```bash
git add src/storage/postgres.rs src/storage/market_repository.rs src/services/stock_history.rs src/services/sector.rs src/services/limit_up.rs
git commit -m "feat: persist market data availability metadata"
```

---

### Task 6: Backfill and refresh point-in-time supporting data

**Files:**
- Create: `src/analysis/market_snapshot/ingestion.rs`
- Modify: `src/analysis/market_snapshot/mod.rs`
- Modify: `src/scheduler/mod.rs`
- Test: `src/analysis/market_snapshot/ingestion.rs`

**Interfaces:**
- Produces `PointInTimeIngestion`.
- Consumes the capability-checked provider and append-only repository methods.
- Records partial and failed runs in `analysis_data_runs`.

- [x] **Step 1: Write fake-provider tests**

Create a fake `PointInTimeDataProvider` and assert:

- unsupported critical capability returns a failed readiness result;
- a repeated refresh with identical `available_at` is idempotent;
- a later provider observation appends a new version;
- a failure in corporate actions does not erase successfully fetched daily basics;
- the run status becomes `partial` when a non-critical category fails.

- [x] **Step 2: Define the ingestion interface**

```rust
#[derive(Clone)]
pub struct PointInTimeIngestion {
    provider: Arc<dyn PointInTimeDataProvider>,
    repo: MarketRepository,
}

impl PointInTimeIngestion {
    pub fn new(
        provider: Arc<dyn PointInTimeDataProvider>,
        pool: PgPool,
    ) -> Self;

    pub async fn refresh_reference_data(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<PointInTimeRefreshResult>;

    pub async fn refresh_trade_date(
        &self,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<PointInTimeRefreshResult>;

    pub async fn backfill_range(
        &self,
        start: NaiveDate,
        end: NaiveDate,
        observed_at: DateTime<Utc>,
    ) -> Result<PointInTimeRefreshResult>;
}
```

- [x] **Step 3: Implement reference-data refresh**

`refresh_reference_data` fetches and appends:

```text
security_master_versions
corporate_action_versions
stock_sector_membership
```

It must compare source records and avoid appending a new version when the normalized payload is unchanged.

- [x] **Step 4: Implement trade-date refresh**

`refresh_trade_date` fetches and appends:

```text
stock_daily_basic_versions
security_daily_status
index_daily_bars
stock_adjustment_factors
```

It also verifies that daily bar, sector, and limit-up version writes from Task 5 exist for the date.

- [x] **Step 5: Implement historical backfill semantics**

Historical backfill records:

```text
available_at = conservative_estimated_availability(record)
ingested_at = observed_at
availability_quality = estimated
```

Conservative estimation rules:

- daily bars, daily basics, security status, index bars, sector performance, and limit-up data become available at 09:00 Asia/Shanghai on the next trading day;
- adjustment factors become available at 09:00 on the next trading day after their effective trade date;
- corporate actions require an announcement date and become available at 09:00 on the next trading day after announcement;
- security master changes require a source effective date and become available on the next trading day;
- sector membership requires `valid_from`/`valid_to` or equivalent source dates and becomes available on the next trading day;
- records without enough source dates remain excluded from historical replay.

Dataset construction may include estimated records but must report estimated-row counts and support a sensitivity run that excludes them. It must never use `ingested_at` as the historical decision cutoff.

- [x] **Step 6: Add scheduler job functions without enabling a new cron**

```rust
pub async fn run_point_in_time_reference_refresh_job(state: Arc<AppState>);
pub async fn run_point_in_time_trade_date_refresh_job(state: Arc<AppState>);
```

Call both from `--run-now` after the legacy fetch job. Production schedules are added in Task 9 after completeness reporting exists.

- [x] **Step 7: Run tests and commit**

```bash
cargo test analysis::market_snapshot::ingestion -- --nocapture
git add src/analysis/market_snapshot/ingestion.rs src/analysis/market_snapshot/mod.rs src/scheduler/mod.rs
git commit -m "feat: refresh point-in-time supporting data"
```

---

### Task 7: Implement adjusted-price calculation

**Files:**
- Create: `src/analysis/market_snapshot/adjustment.rs`
- Modify: `src/analysis/market_snapshot/mod.rs`
- Test: `src/analysis/market_snapshot/adjustment.rs`

**Interfaces:**
- Produces `adjust_candles`.
- Consumes raw `Candle` values and dated adjustment factors.

- [x] **Step 1: Write failing tests**

```rust
#[test]
fn adjusts_ohlc_to_latest_factor_without_changing_volume() {
    let bars = vec![
        candle("2026-07-08", 10.0, 11.0, 9.0, 10.5, 1000),
        candle("2026-07-09", 11.0, 12.0, 10.0, 11.5, 1200),
    ];
    let factors = vec![
        factor("2026-07-08", 1.0),
        factor("2026-07-09", 2.0),
    ];

    let adjusted = adjust_candles(&bars, &factors).unwrap();

    assert_eq!(adjusted[0].close, 5.25);
    assert_eq!(adjusted[0].volume, 1000);
    assert_eq!(adjusted[1].close, 11.5);
}

#[test]
fn rejects_missing_factor_for_a_bar() {
    let error = adjust_candles(
        &[candle("2026-07-08", 10.0, 11.0, 9.0, 10.5, 1000)],
        &[],
    )
    .unwrap_err();

    assert!(error.to_string().contains("missing adjustment factor"));
}
```

- [x] **Step 2: Verify failure**

Run:

```bash
cargo test adjustment::tests -- --nocapture
```

Expected: FAIL.

- [x] **Step 3: Implement**

```rust
pub fn adjust_candles(
    bars: &[Candle],
    factors: &[AdjustmentFactor],
) -> Result<Vec<Candle>> {
    let latest = factors
        .iter()
        .max_by_key(|row| row.trade_date)
        .ok_or_else(|| AppError::Internal("missing adjustment factors".into()))?
        .adj_factor;

    let by_date: std::collections::HashMap<_, _> = factors
        .iter()
        .map(|row| (row.trade_date, row.adj_factor))
        .collect();

    bars.iter()
        .map(|bar| {
            let factor = by_date.get(&bar.trade_date).ok_or_else(|| {
                AppError::Internal(format!(
                    "missing adjustment factor for {}",
                    bar.trade_date
                ))
            })?;
            let ratio = factor / latest;
            Ok(Candle {
                open: bar.open * ratio,
                high: bar.high * ratio,
                low: bar.low * ratio,
                close: bar.close * ratio,
                ..bar.clone()
            })
        })
        .collect()
}
```

- [x] **Step 4: Run tests**

Run:

```bash
cargo test adjustment::tests
```

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/analysis/market_snapshot
git commit -m "feat: add adjusted-price calculation"
```

---

### Task 8: Build and persist daily market snapshots

**Files:**
- Create: `src/analysis/market_snapshot/builder.rs`
- Modify: `src/analysis/market_snapshot/mod.rs`
- Modify: `src/scheduler/mod.rs`
- Test: `src/analysis/market_snapshot/builder.rs`

**Interfaces:**
- Produces `MarketSnapshotModule::build_trade_date`.
- Persists one versioned snapshot per trade date.
- Does not alter existing market report output yet.

- [ ] **Step 1: Write a failing pure metrics test**

Create a fixture with four securities and assert:

```rust
assert_eq!(metrics.up_count, 2);
assert_eq!(metrics.down_count, 1);
assert_eq!(metrics.flat_count, 1);
assert_eq!(metrics.above_ma20_count, 2);
assert_eq!(metrics.new_high_20_count, 1);
assert_eq!(metrics.new_low_20_count, 1);
```

- [ ] **Step 2: Implement pure calculation types**

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MarketBreadthMetrics {
    pub up_count: usize,
    pub down_count: usize,
    pub flat_count: usize,
    pub above_ma20_count: usize,
    pub new_high_20_count: usize,
    pub new_low_20_count: usize,
    pub limit_up_count: usize,
    pub limit_down_count: usize,
    pub total_amount: f64,
}
```

Implement `calculate_market_breadth`.

- [ ] **Step 3: Implement the deep module**

```rust
#[derive(Clone)]
pub struct MarketSnapshotModule {
    repo: MarketRepository,
}

impl MarketSnapshotModule {
    pub fn new(pool: PgPool) -> Self {
        Self {
            repo: MarketRepository::new(pool),
        }
    }

    pub async fn build_trade_date(
        &self,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<MarketSnapshotBuildResult>;
}
```

The builder must:

1. Load bars available by `as_of`.
2. Check security status.
3. Load persisted index bars.
4. Calculate breadth.
5. Record missing adjustment/status/index inputs.
6. Set `data_complete = false` if any critical category is missing.
7. Calculate `input_fingerprint` from sorted source IDs and timestamps.
8. Persist `snapshot_version = "market-v1"`.

- [ ] **Step 4: Add a scheduler job function without enabling a new cron yet**

```rust
pub async fn run_market_snapshot_job(state: Arc<AppState>) {
    let trade_date = match postgres::latest_stock_trade_date(&state.db).await {
        Ok(Some(value)) => value,
        _ => return,
    };
    let module = MarketSnapshotModule::new(state.db.clone());
    if let Err(error) = module.build_trade_date(trade_date, chrono::Utc::now()).await {
        warn!("Market snapshot failed: {}", error);
    }
}
```

Call it from `--run-now` after `run_fetch_job` and before `run_scan_job`. Do not add the production cron until Task 9 adds completeness behavior.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test analysis::market_snapshot -- --nocapture
cargo test scheduler::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/analysis/market_snapshot src/scheduler/mod.rs src/main.rs
git commit -m "feat: build daily market snapshots"
```

---

### Task 9: Add data-completeness API and production schedule

**Files:**
- Create: `src/api/analysis_routes.rs`
- Modify: `src/api/mod.rs`
- Modify: `src/api/routes.rs`
- Modify: `src/scheduler/mod.rs`
- Modify: `src/state.rs`
- Modify: `src/main.rs`
- Modify: `README.md`
- Test: `src/api/analysis_routes.rs`
- Test: `src/scheduler/mod.rs`

**Interfaces:**
- Produces `analysis_router`.
- Adds `GET /api/analysis/data-status`.
- Adds explicit refresh and snapshot job endpoints.
- Schedules point-in-time refresh before the market snapshot.

- [ ] **Step 1: Add router tests**

Test that `analysis_router` contains:

```text
GET  /api/analysis/data-status
POST /api/jobs/analysis/point-in-time/refresh
POST /api/jobs/analysis/point-in-time/reference-refresh
POST /api/jobs/analysis/snapshot
```

Use an Axum `Router` smoke test or a direct handler unit test with a SQLx database.

- [ ] **Step 2: Implement handlers**

Response:

```json
{
  "tradeDate": "2026-07-10",
  "snapshotVersion": "market-v1",
  "dataComplete": false,
  "missingInputs": ["adjustment_factors"],
  "availableAt": "2026-07-10T09:30:00Z",
  "inputFingerprint": "..."
}
```

The handlers call `run_point_in_time_trade_date_refresh_job`, `run_point_in_time_reference_refresh_job`, and `run_market_snapshot_job` respectively.

- [ ] **Step 3: Mount the sub-router**

```rust
// src/api/mod.rs
pub mod analysis_routes;
pub mod routes;

pub use routes::build_router;
```

In `build_router`:

```rust
.merge(crate::api::analysis_routes::analysis_router(state.clone()))
```

- [ ] **Step 4: Add the production cron**

Add:

```rust
const POINT_IN_TIME_TRADE_DATE_JOB_CRON: &str = "0 10 17 * * Mon,Tue,Wed,Thu,Fri";
const MARKET_SNAPSHOT_JOB_CRON: &str = "0 20 17 * * Mon,Tue,Wed,Thu,Fri";
const POINT_IN_TIME_REFERENCE_JOB_CRON: &str = "0 30 20 * * Fri";
```

Add to `AppState`:

```rust
pub analysis_job_lock: Arc<tokio::sync::Mutex<()>>,
```

Initialize it in `main.rs`. Register daily trade-date refresh after the legacy fetch, market snapshot after the refresh, and reference refresh on Friday evening. All three jobs acquire this shared lock so overlapping manual triggers cannot run concurrently.

Update scheduler tests to assert the exact schedules and order.

- [ ] **Step 5: Document environment and endpoints**

README must state:

- Existing current-state rows are not point-in-time history until copied through an explicit estimated backfill.
- Pattern research is blocked until security master, daily basics, corporate actions, adjustment factors, status, indices, and sector membership are complete.
- The new endpoint reports capability failures, completeness, and estimated-row counts without guessing missing inputs.

- [ ] **Step 6: Verify**

Run:

```bash
cargo fmt --all -- --check
cargo test --all --locked
git diff --check
```

Expected: formatting and non-environment-dependent tests PASS; SQLx failures must be reported if `DATABASE_URL` is unavailable.

- [ ] **Step 7: Commit**

```bash
git add src/api src/scheduler/mod.rs src/state.rs src/main.rs README.md
git commit -m "feat: expose point-in-time data status"
```

---

## Phase Completion Checklist

- [ ] Migration `013` applies cleanly.
- [ ] Historical unknown availability is not silently fabricated.
- [ ] Daily ingestion records observed availability.
- [ ] Security master history retains delisted names and dates.
- [ ] Daily market-cap and valuation data are queryable by explicit `as_of`.
- [ ] Corporate actions, adjustment factors, and status data are queryable by explicit `as_of`.
- [ ] Historical sector membership has effective dates.
- [ ] Index history is persisted.
- [ ] Capability probe reports unsupported provider features.
- [ ] Daily and weekly refresh jobs are scheduled before snapshot generation.
- [ ] Market snapshot reports missing critical inputs.
- [ ] Existing scanner output is unchanged.
- [ ] No pattern or event candidate table exists yet.
- [ ] Workspace is clean after verification.
