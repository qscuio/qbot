# Limit-Up Strong/Startup Tracking Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the missing strong-stock tracking and startup-watch tracking flows from `../qubot` to the Rust `qbot`.

**Architecture:** Reuse `limit_up_stocks` as the source of truth, derive `startup_watchlist` from the latest 30-day limit-up history, and expose both datasets through `LimitUpService` plus Telegram commands/menu entries. Keep the first version query-driven instead of introducing a new background service.

**Tech Stack:** Rust, Axum, SQLx, PostgreSQL, Telegram webhook callbacks

### Task 1: Persist startup-watch derived state

**Files:**
- Create: `migrations/010_startup_watchlist.sql`
- Modify: `src/storage/postgres.rs`
- Test: `src/storage/postgres.rs`

**Step 1: Write the failing tests**

- Add a SQLx test proving strong stocks are ranked by recent limit-up count.
- Add a SQLx test proving startup watchlist keeps only stocks with exactly one limit-up in the last 30 days.

**Step 2: Run tests to verify they fail**

Run: `cargo test strong_limit_up -- --nocapture`

**Step 3: Write minimal implementation**

- Add `startup_watchlist` table migration.
- Add storage queries for strong-stock ranking, startup-watch rebuild, and startup-watch listing.

**Step 4: Run tests to verify they pass**

Run: `cargo test strong_limit_up -- --nocapture`

### Task 2: Expose data through the limit-up service

**Files:**
- Modify: `src/services/limit_up.rs`
- Test: `src/storage/postgres.rs`

**Step 1: Write the failing test**

- Use the storage tests as the red bar for the missing service-backed behavior.

**Step 2: Implement**

- Refresh `startup_watchlist` after each `fetch_and_save`.
- Add service methods for strong stocks and startup watchlist retrieval.

**Step 3: Verify**

Run: `cargo test strong_limit_up startup_watchlist -- --nocapture`

### Task 3: Wire Telegram help/menu/commands

**Files:**
- Modify: `src/api/routes.rs`
- Modify: `src/main.rs`
- Test: `src/api/routes.rs`

**Step 1: Write the failing tests**

- Add unit tests proving help text mentions `/strong` and `/startup`.
- Add a unit test proving the main menu exposes a limit-up entry.

**Step 2: Run tests to verify they fail**

Run: `cargo test telegram_help_text_mentions_limit_up_commands menu_content_exposes_limit_up_entry -- --nocapture`

**Step 3: Implement**

- Add a limit-up submenu with summary, strong stocks, startup watchlist, and manual sync.
- Add `/strong`, `/startup`, `/limitup`, `/limitup_sync` command handling.
- Register the new Telegram commands.

**Step 4: Verify**

Run: `cargo test telegram_help_text_mentions_limit_up_commands menu_content_exposes_limit_up_entry -- --nocapture`

### Task 4: Final verification

**Files:**
- Modify: `README.md` (optional follow-up only if command table is stale)

**Step 1: Run targeted verification**

Run: `cargo test limit_up -- --nocapture`

**Step 2: Run broader regression coverage**

Run: `cargo test`
