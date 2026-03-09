# Standalone Limit-Up Reports Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add standalone `limitup` and `strong` reports to the Rust qbot, with persistence, Telegram push, and latest-report retrieval APIs.

**Architecture:** Extend the existing report pipeline instead of creating a parallel service. `MarketReportService` will generate three artifacts during the daily report window: the existing `daily` market report, a standalone `limitup` report based on latest limit-up rows, and a standalone `strong` report based on recent strong-stock ranking. All three are stored in `reports`, while the two standalone reports are pushed to `STOCK_ALERT_CHANNEL` with fallback to `REPORT_CHANNEL`.

**Tech Stack:** Rust, SQLx, Axum, PostgreSQL, Telegram webhook push

### Task 1: Formatter-first tests

**Files:**
- Modify: `src/telegram/formatter.rs`
- Test: `src/telegram/formatter.rs`

**Step 1: Write the failing tests**

- Add a unit test proving the limit-up report includes the date, stock count, and top stock names.
- Add a unit test proving the strong-stock report includes the rolling window label and limit-up counts.

**Step 2: Run test to verify it fails**

Run: `cargo test formatter::tests -- --nocapture`

**Step 3: Write minimal implementation**

- Upgrade `format_limit_up_report`.
- Add `format_strong_stock_report`.

**Step 4: Run tests to verify they pass**

Run: `cargo test formatter::tests -- --nocapture`

### Task 2: Data/query plumbing

**Files:**
- Modify: `src/storage/postgres.rs`
- Modify: `src/services/limit_up.rs`

**Step 1: Write the failing test**

- Reuse formatter tests as the red bar for the missing report inputs.

**Step 2: Implement**

- Add a query to load limit-up rows by trade date.
- Reuse existing strong-stock ranking query.
- Add service helpers for report generation.

**Step 3: Verify**

Run: `cargo check`

### Task 3: Generate/persist/push standalone reports

**Files:**
- Modify: `src/services/market_report.rs`
- Modify: `src/scheduler/mod.rs`

**Step 1: Write the failing tests**

- Add route/helper tests covering latest report retrieval for `limitup` and `strong`.

**Step 2: Run test to verify it fails**

Run: `cargo test api::routes::tests -- --nocapture`

**Step 3: Implement**

- Generate `limitup` and `strong` reports during the daily report job.
- Save them with report types `limitup` and `strong`.
- Push them to `STOCK_ALERT_CHANNEL`, falling back to `REPORT_CHANNEL` if needed.

**Step 4: Run tests to verify it passes**

Run: `cargo test api::routes::tests -- --nocapture`

### Task 4: Expose latest standalone reports

**Files:**
- Modify: `src/api/routes.rs`
- Modify: `src/main.rs`
- Modify: `README.md`

**Step 1: Write the failing tests**

- Add unit tests that assert the new report commands and routes are surfaced.

**Step 2: Implement**

- Add `/api/report/limitup` and `/api/report/strong`.
- Add `/limitup_report` and `/strong_report`.
- Update help text and bot command registration.

**Step 3: Verify**

Run: `cargo test api::routes::tests -- --nocapture`

### Task 5: Final verification

**Files:**
- No additional files expected

**Step 1: Run targeted verification**

Run: `cargo fmt --check`

Run: `cargo check`

Run: `cargo test formatter::tests api::routes::tests -- --nocapture`
