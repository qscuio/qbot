# Scan Signal Architecture Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ranked short-, mid-, and long-horizon scan pools with `A/B` tiers while preserving the existing raw signal scanner.

**Architecture:** Keep the legacy signal detector registry unchanged, add a new ranking layer that evaluates tradability, structural triggers, factor scores, and risk penalties, then publish the ranked pools back into scanner output as additional scan buckets. Update route metadata so the new pools are visible through existing Telegram and API surfaces.

**Tech Stack:** Rust, Axum, SQLx, serde_json, existing scanner and route modules

---

## Chunk 1: Ranking Module

### Task 1: Create failing tests for ranked pool generation

**Files:**
- Modify: `src/services/scanner.rs`
- Create: `src/services/scan_ranker.rs`

- [ ] **Step 1: Write the failing tests**

Add unit tests that prove:
- a strong short-line pattern lands in `pool_short_a`
- a trend breakout lands in `pool_mid_a` or `pool_mid_b`
- a low-quality stock is rejected by shared hard filters

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test scan_ranker -- --nocapture`
Expected: FAIL because the new ranking module and pool ids do not exist yet

- [ ] **Step 3: Write minimal implementation**

Create `src/services/scan_ranker.rs` with:
- shared pool id constants
- ranked candidate structs
- shared filter helpers
- line trigger logic
- score assembly
- conversion from ranked candidates into `SignalHit`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test scan_ranker -- --nocapture`
Expected: PASS

### Task 2: Inject ranked pools into scanner output

**Files:**
- Modify: `src/services/scanner.rs`
- Modify: `src/services/mod.rs`

- [ ] **Step 1: Write the failing test**

Add a scanner-focused test that verifies ranked pool buckets are appended to the scan result map when candidates qualify.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test scanner -- --nocapture`
Expected: FAIL because the scanner does not append ranked pools

- [ ] **Step 3: Write minimal implementation**

Update `ScannerService::run_full_scan` to:
- call the new ranking layer after raw signal detection
- append `pool_short_a`, `pool_short_b`, `pool_mid_a`, `pool_mid_b`, `pool_long_a`, `pool_long_b`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test scanner -- --nocapture`
Expected: PASS

## Chunk 2: Route and UI Metadata

### Task 3: Add pool metadata and menu visibility

**Files:**
- Modify: `src/api/routes.rs`

- [ ] **Step 1: Write the failing test**

Add tests that prove:
- scan status/meta includes the six new pool ids
- the formatted scan status can distinguish ranked pools from raw signals

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test api::routes::tests -- --nocapture`
Expected: FAIL because route metadata does not know the new pool ids

- [ ] **Step 3: Write minimal implementation**

Update route helpers to:
- register pool labels/icons
- optionally surface pool buttons in scan menus
- preserve backward-compatible behavior for legacy signals

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test api::routes::tests -- --nocapture`
Expected: PASS

## Chunk 3: Verification

### Task 4: Run targeted verification

**Files:**
- No file changes

- [ ] **Step 1: Run focused scan-related tests**

Run: `cargo test services::signal_auto_trading::tests -- --nocapture`
Expected: PASS, proving the new pools do not break adjacent logic

- [ ] **Step 2: Run new scanner/routing tests**

Run: `cargo test scan_ranker scanner api::routes::tests::signal_auto_status_groups_prestart_daban_and_strong_accounts -- --nocapture`
Expected: PASS

- [ ] **Step 3: Review diff**

Run: `git diff -- src/services/scanner.rs src/services/scan_ranker.rs src/api/routes.rs src/services/mod.rs`
Expected: only ranked-pool changes

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-04-22-scan-signal-architecture-design.md \
        docs/superpowers/plans/2026-04-22-scan-signal-architecture.md \
        src/services/scan_ranker.rs src/services/scanner.rs src/services/mod.rs src/api/routes.rs
git commit -m "Add ranked scan pools for short mid and long setups"
```
