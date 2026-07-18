# Daily Bar Data Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make incremental OHLCV updates reject malformed provider data and repair every persisted traded daily bar whose volume was incorrectly stored as zero.

**Architecture:** Keep the existing provider fallback and current-state table, but validate provider batches before accepting them, bulk-upsert daily bars with non-destructive conflict rules, and add a resumable repair mode that selects only still-invalid trade dates. GitHub Actions invokes repair mode after the normal deployment; each repaired date commits independently, so interruption and rerun are safe.

**Tech Stack:** Rust, Tokio, SQLx/PostgreSQL, Axum service bootstrap, GitHub Actions, systemd-managed VPS deployment.

## Global Constraints

- Do not modify the Telegram feature surface or dashboard authentication.
- Do not edit production files or production data manually over SSH; deployment and repair execute through GitHub Actions.
- Keep dashboard reads responsive while repair runs by committing one trade date at a time.
- Never replace a valid positive OHLCV/amount value with an invalid zero from a provider.
- The repair must be idempotent and resume by querying rows that remain invalid.

---

### Task 1: Validate and preserve daily OHLCV data

**Files:**
- Modify: `src/data/fallback.rs`
- Modify: `src/storage/postgres.rs`
- Test: inline unit tests in `src/data/fallback.rs`
- Test: SQLx tests in `src/storage/postgres.rs`

**Interfaces:**
- Produces: `daily_bar_batch_is_usable(bars: &[(String, Candle)]) -> bool`
- Produces: bulk, guarded `upsert_daily_bars_in_tx(...) -> Result<usize>`

- [ ] **Step 1: Write failing provider-quality tests**

```rust
#[test]
fn rejects_traded_rows_with_missing_volume() {
    let bars = vec![("600000.SH".into(), candle(10.0, 0, 10_000.0))];
    assert!(!daily_bar_batch_is_usable(&bars));
}

#[test]
fn accepts_suspended_zero_activity_rows_beside_valid_trades() {
    let bars = vec![
        ("600000.SH".into(), candle(10.0, 1_000, 10_000.0)),
        ("600001.SH".into(), candle(10.0, 0, 0.0)),
    ];
    assert!(daily_bar_batch_is_usable(&bars));
}
```

- [ ] **Step 2: Run the provider tests and verify RED**

Run: `CARGO_TARGET_DIR=/dev/shm/qbot-target cargo test daily_bar_batch -- --nocapture`

Expected: compile failure because `daily_bar_batch_is_usable` does not exist.

- [ ] **Step 3: Implement provider validation and fallback continuation**

```rust
fn daily_bar_batch_is_usable(bars: &[(String, Candle)]) -> bool {
    !bars.is_empty() && bars.iter().all(|(_, bar)| {
        bar.open > 0.0
            && bar.high > 0.0
            && bar.low > 0.0
            && bar.close > 0.0
            && (bar.amount <= 0.0 || bar.volume > 0)
    })
}
```

Use this predicate in `FallbackDataProvider::get_daily_bars_by_date`; an invalid non-empty batch must continue to the next provider.

- [ ] **Step 4: Write a failing guarded-upsert SQLx test**

Seed a valid row, upsert the same key with zero volume and zero amount, and assert the original positive values remain. Then upsert a corrected positive row and assert it replaces the old values.

- [ ] **Step 5: Replace row-at-a-time writes with chunked SQLx `QueryBuilder` upserts**

Use chunks of 1,000 rows. The conflict clause must use `CASE`/`COALESCE` so positive stored OHLCV and amount values survive invalid zero inputs while corrected positive inputs replace them.

- [ ] **Step 6: Run focused tests and commit**

Run: `DATABASE_URL=postgres://postgres:qbot@127.0.0.1:55432/qbot_test CARGO_TARGET_DIR=/dev/shm/qbot-target cargo test storage::postgres::tests -- --nocapture`

Commit: `fix: harden incremental daily bar writes`

### Task 2: Correct incremental date selection and add resumable repair

**Files:**
- Modify: `src/services/stock_history.rs`
- Modify: `src/storage/postgres.rs`
- Test: inline tests in `src/services/stock_history.rs`
- Test: SQLx tests in `src/storage/postgres.rs`

**Interfaces:**
- Produces: `postgres::trade_dates_with_invalid_volume(pool: &PgPool) -> Result<Vec<NaiveDate>>`
- Produces: `StockHistoryService::update_latest_trading_day() -> Result<NaiveDate>`
- Produces: `StockHistoryService::repair_invalid_daily_bars() -> Result<DailyBarRepairReport>`

- [ ] **Step 1: Write a failing weekend incremental-sync test**

Configure a fake provider with Friday as its latest trading date while the clock date is Saturday. Assert the service fetches and persists Friday, not Saturday.

- [ ] **Step 2: Implement latest-trading-day resolution**

Query provider trading dates over the previous 14 calendar days, filter out future dates, select the maximum, and fetch that date. Keep `update_today()` as a compatibility wrapper calling the corrected method.

- [ ] **Step 3: Write failing invalid-date discovery and repair tests**

Seed two dates with `amount > 0 AND volume = 0`, one already-valid date, and assert only the invalid dates are returned in ascending order. Run repair against a fake provider and assert both dates become valid while the valid date is untouched.

- [ ] **Step 4: Implement resumable repair**

```rust
pub struct DailyBarRepairReport {
    pub attempted_dates: usize,
    pub repaired_dates: usize,
    pub failed_dates: Vec<(NaiveDate, String)>,
}
```

For each invalid date, fetch through the validated fallback provider, bulk-upsert in its own transaction, recheck invalid rows for that date, record the result, and wait 200 ms between provider calls. Continue after per-date failures and return an error only when the final report contains failed dates.

- [ ] **Step 5: Run focused tests and commit**

Run: `CARGO_TARGET_DIR=/dev/shm/qbot-target cargo test services::stock_history::tests -- --nocapture`

Commit: `feat: add resumable OHLCV repair`

### Task 3: Run the repair through GitHub Actions

**Files:**
- Modify: `src/main.rs`
- Modify: `.github/workflows/deploy.yml`
- Modify: `README.md`
- Test: `web/dashboard/tests/deployment.test.mjs`

**Interfaces:**
- Consumes: `StockHistoryService::repair_invalid_daily_bars()`
- Produces: executable mode `qbot --repair-daily-bars`

- [ ] **Step 1: Write a failing deployment-contract test**

Assert the deploy workflow invokes `--repair-daily-bars`, has a six-hour timeout, and runs only after the new binary passes health checks.

- [ ] **Step 2: Add repair-only process mode**

Parse `--repair-daily-bars` once at startup. After the application state and validated provider exist—but before Telegram registration, schedulers, webhooks, and HTTP binding—run repair, log its report, and return from `main`.

- [ ] **Step 3: Add the GitHub Actions repair step**

After deploy health checks, invoke the installed binary over the existing SSH action with `command_timeout: 6h`. Load `/opt/qbot/.env` through a temporary systemd unit or the existing service environment without printing secrets. The workflow must fail if any dates remain unrepaired.

- [ ] **Step 4: Document operations and verification**

Document the idempotent command, the invalid-row query (`amount > 0 AND COALESCE(volume, 0) <= 0`), expected runtime, safe rerun behavior, and how to read the repair logs.

- [ ] **Step 5: Run complete verification and deploy**

Run:

```bash
cargo fmt --all -- --check
npm run check
npx playwright test
npm audit --audit-level=high
git diff --check
```

Push to `main`, monitor the Deploy workflow through repair completion, then verify on the VPS that the invalid-row count is zero, the deployed commit matches, and `qbot` is active.

Commit: `ci: repair persisted OHLCV data after deploy`

