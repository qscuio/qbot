# Task 8 Report

## Implementation summary

- Added [`src/analysis/market_snapshot/builder.rs`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/src/analysis/market_snapshot/builder.rs) with:
  - `MarketBreadthMetrics`
  - `calculate_market_breadth`
  - `MarketSnapshotModule::build_trade_date`
  - focused unit/integration tests for breadth, missing inputs, and fingerprinting
- Exported the builder module from [`src/analysis/market_snapshot/mod.rs`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/src/analysis/market_snapshot/mod.rs).
- Added narrow point-in-time repository helpers in [`src/storage/market_repository.rs`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/src/storage/market_repository.rs):
  - `daily_bar_history_as_of`
  - `adjustment_factors_as_of`
  - `security_statuses_as_of`
  - `security_status_universe_as_of`
  - `index_bars_as_of`
- Fixed Task 8 review issues:
  - `MarketSnapshotModule::build_trade_date` now always builds from the requested `(trade_date, as_of)` and returns that freshly built snapshot instead of returning an already persisted `market-v1` row first.
  - `save_market_snapshot` now upserts the single `(trade_date, snapshot_version)` row so a rebuilt `market-v1` snapshot replaces stale metrics, completeness, missing-inputs, and fingerprint fields.
  - Snapshot fingerprints now include loaded bar, adjustment, status, and index provenance even for securities later excluded from breadth because a critical input is missing.
  - Trade-date status rows now act as the known-universe signal for current-day bar completeness. A code with a trade-date status row but no current trade-date bar available as-of records `stock_daily_bar_versions:<code>:<trade_date>` without guessing securities that have neither bars nor status.
- Added `run_market_snapshot_job` in [`src/scheduler/mod.rs`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/src/scheduler/mod.rs).
- Inserted market snapshot execution into `--run-now` in [`src/main.rs`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/src/main.rs) after the point-in-time refreshes and before the scan.
- Added `sha2` in [`Cargo.toml`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/Cargo.toml) and updated [`Cargo.lock`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/Cargo.lock) for deterministic `input_fingerprint` hashing.

## TDD evidence

- Red:
  - Wrote the pure breadth test first in `builder.rs`.
  - Ran `cargo test analysis::market_snapshot -- --nocapture`.
  - Confirmed failure at `analysis::market_snapshot::builder::tests::calculates_market_breadth_from_four_securities` before implementation.
  - For review fixes, applied only the new regression tests to an isolated `HEAD` copy at `/tmp/qbot-task8-red.sKnizi` and ran:
    - `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test build_trade_date_rebuilds_for_later_as_of_and_updates_saved_snapshot -- --nocapture`
      - Failed as expected because the second build returned `available_at = 2026-07-20T18:30:00Z` instead of the requested `2026-07-20T19:30:00Z`.
    - `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test build_trade_date_fingerprint_includes_loaded_inputs_for_excluded_security -- --nocapture`
      - Failed as expected because the fingerprint omitted loaded inputs for the excluded security.
    - `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test build_trade_date_marks_status_universe_codes_missing_current_bar -- --nocapture`
      - Failed as expected because `data_complete` stayed true when a trade-date status row existed without a current trade-date bar.
    - `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test market_snapshot_save_upserts_latest_snapshot -- --nocapture`
      - Failed as expected because the original persisted snapshot was kept instead of upserting the rebuilt row.
- Green:
  - Implemented `MarketBreadthMetrics` and `calculate_market_breadth`.
  - Implemented `MarketSnapshotModule::build_trade_date` and repository helpers.
  - Added focused builder integration tests for:
    - missing adjustment/status/index inputs => `data_complete = false`
    - deterministic fingerprint from sorted source IDs and timestamps
    - same trade date with different `as_of` rebuild/upsert behavior
    - fingerprint completeness for excluded securities
    - trade-date status universe missing-current-bar completeness

## Verification results

- `cargo test analysis::market_snapshot -- --nocapture`
  - PASS
- `cargo test scheduler::tests -- --nocapture`
  - PASS
- `cargo fmt --all -- --check`
  - PASS after running `cargo fmt --all`
- `git diff --check`
  - PASS

`DATABASE_URL` was exported as `postgresql://qbot:qbot@127.0.0.1:5432/qbot` in the verification shell so the SQLx tests could run.

## Files changed

- `Cargo.toml`
- `Cargo.lock`
- `src/analysis/market_snapshot/builder.rs`
- `src/analysis/market_snapshot/mod.rs`
- `src/storage/market_repository.rs`
- `src/scheduler/mod.rs`
- `src/main.rs`
- `.superpowers/sdd/task-8-report.md`

## Concerns

- The builder currently records missing bar-history gaps as `stock_daily_bar_versions:<code>:previous_close`, `stock_daily_bar_versions:<code>:lookback_20`, or `stock_daily_bar_versions:<code>:<trade_date>` when point-in-time inputs show the security should have the required bars. This is intentionally explicit, but no downstream consumer reads those strings yet.
