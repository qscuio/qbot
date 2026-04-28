# Horizon-First Stock Picking Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make short-, mid-, and long-horizon picking the primary scanner and bot workflow while preserving existing pool IDs and raw signal compatibility.

**Architecture:** Keep `ScannerService` as the scan entry point and `scan_ranker` as the classification layer. The ranker evaluates every stock with enough bars, treats old signal hits as evidence, and emits the existing six `pool_*` buckets. Telegram exposes `/pick` and a horizon-first menu that routes to those same buckets.

**Tech Stack:** Rust, Axum, Telegram inline keyboard callbacks, serde JSON metadata, existing unit tests in `src/api/routes.rs` and `src/services/scan_ranker.rs`.

---

## Chunk 1: Ranker Evaluates Horizon Structures Directly

### Task 1: Remove Raw Signal Hard Gate

**Files:**
- Modify: `src/services/scanner.rs`
- Modify: `src/services/scan_ranker.rs`
- Test: `src/services/scan_ranker.rs`

- [ ] **Step 1: Write failing test**

Add a test proving a structure-only short breakout can enter `pool_short_a` or `pool_short_b` with `hits: vec![]`.

- [ ] **Step 2: Verify RED**

Run: `cargo test rank_scan_inputs_emits_short_pool_without_raw_signal_hits -- --nocapture`
Expected: FAIL because empty raw hits currently reject ranking.

- [ ] **Step 3: Implement minimal ranker change**

Remove `input.hits.is_empty()` from the hard reject. Let structure-only setup checks pass when price, volume, and trend conditions qualify.

- [ ] **Step 4: Feed all scanned stocks into ranking**

In `ScannerService::run_full_scan`, push a `RankInput` for every stock with enough bars, even if `stock_hits` is empty.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test rank_scan_inputs_emits_short_pool_without_raw_signal_hits -- --nocapture`
Expected: PASS.

## Chunk 2: Old Signals Become Evidence And Adders

### Task 2: Add Evidence Bonuses And Matched Setups

**Files:**
- Modify: `src/services/scan_ranker.rs`
- Test: `src/services/scan_ranker.rs`

- [ ] **Step 1: Write failing test**

Add a test proving the same valid mid-horizon structure scores higher when it includes old mid evidence such as `ma_bullish`.

- [ ] **Step 2: Verify RED**

Run: `cargo test rank_scan_inputs_rewards_old_signal_evidence -- --nocapture`
Expected: FAIL because old signals do not add an explicit evidence bonus.

- [ ] **Step 3: Implement evidence helpers**

Add short, mid, and long evidence helper functions. Add a small evidence factor to each candidate when matching old signals exist. Keep `supporting_signals` unchanged.

- [ ] **Step 4: Add `matched_setups` metadata**

Preserve all candidate setup IDs and names that match for a stock in the emitted pool metadata.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test rank_scan_inputs_rewards_old_signal_evidence -- --nocapture`
Expected: PASS.

## Chunk 3: Bot Horizon Command And Menu

### Task 3: Add `/pick` Command Routing

**Files:**
- Modify: `src/api/routes.rs`
- Test: `src/api/routes.rs`

- [ ] **Step 1: Write failing tests**

Add tests for `/pick` help text and parsing `short`, `mid`, and `long` to the correct pool IDs.

- [ ] **Step 2: Verify RED**

Run: `cargo test pick -- --nocapture`
Expected: FAIL because `/pick` command logic does not exist.

- [ ] **Step 3: Implement command parsing**

Add helpers that map `short`, `mid`, and `long` to `pool_short_a`, `pool_mid_a`, and `pool_long_a`. `/pick` without args sends the horizon menu.

- [ ] **Step 4: Route callbacks and command**

Use the existing cached scan page sender for pool buttons. Keep menu buttons wired to `scan:s:<pool_id>`.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test pick -- --nocapture`
Expected: PASS.

## Chunk 4: Verification

### Task 4: Targeted And Broad Checks

**Files:**
- Modify only files changed in previous chunks.

- [ ] **Step 1: Format check**

Run: `rustfmt --edition 2021 --check src/api/routes.rs src/services/scanner.rs src/services/scan_ranker.rs`
Expected: PASS.

- [ ] **Step 2: Targeted tests**

Run: `cargo test scan_ranker pick menu -- --nocapture`
Expected: Use valid filters separately if Cargo rejects multiple names.

- [ ] **Step 3: Broad tests without DB integration**

Run: `cargo test -- --skip storage::postgres::tests::startup_watchlist_rebuild_keeps_only_single_recent_limit_up --skip storage::postgres::tests::strong_limit_up_query_ranks_recent_stocks`
Expected: PASS.
