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
  - `index_bars_as_of`
- Added `run_market_snapshot_job` in [`src/scheduler/mod.rs`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/src/scheduler/mod.rs).
- Inserted market snapshot execution into `--run-now` in [`src/main.rs`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/src/main.rs) after the point-in-time refreshes and before the scan.
- Added `sha2` in [`Cargo.toml`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/Cargo.toml) and updated [`Cargo.lock`](/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation/Cargo.lock) for deterministic `input_fingerprint` hashing.

## TDD evidence

- Red:
  - Wrote the pure breadth test first in `builder.rs`.
  - Ran `cargo test analysis::market_snapshot -- --nocapture`.
  - Confirmed failure at `analysis::market_snapshot::builder::tests::calculates_market_breadth_from_four_securities` before implementation.
- Green:
  - Implemented `MarketBreadthMetrics` and `calculate_market_breadth`.
  - Implemented `MarketSnapshotModule::build_trade_date` and repository helpers.
  - Added focused builder integration tests for:
    - missing adjustment/status/index inputs => `data_complete = false`
    - deterministic fingerprint from sorted source IDs and timestamps

## Verification results

- `cargo test analysis::market_snapshot -- --nocapture`
  - PASS
- `cargo test scheduler::tests -- --nocapture`
  - PASS
- `cargo fmt --all -- --check`
  - PASS after running `cargo fmt --all`
- `git diff --check`
  - PASS

`DATABASE_URL` was exported as `postgresql://qbot:qbot@127.0.0.1/qbot` in the verification shell so the SQLx tests could run.

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

- The builder currently records missing bar-history gaps as `stock_daily_bar_versions:<code>:previous_close` or `stock_daily_bar_versions:<code>:lookback_20` when the status row says the security should have the required history. This is intentionally explicit, but no downstream consumer reads those strings yet.
