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
  - `daily_bar_history_as_of` now treats `open`, `high`, `low`, `close`, `amount`, and `volume` as critical daily bar fields. It no longer fabricates zero-valued candles for NULL critical fields; incomplete rows retain loaded provenance and field-level missing metadata.
  - `MarketSnapshotModule::build_trade_date` now records `stock_daily_bar_versions:<code>:<trade_date>:<field>` for NULL critical daily bar fields and excludes securities with incomplete bar history from breadth so snapshots persist with `data_complete = false`.
  - Trade-date status rows now act as the known-universe signal for current-day bar completeness. A code with a trade-date status row but no current trade-date bar available as-of records `stock_daily_bar_versions:<code>:<trade_date>` without guessing securities that have neither bars nor status.
  - Point-in-time repository helpers now break equal-`available_at` ties deterministically with `ingested_at DESC, source ASC` in the Task 8 window queries for daily bars, adjustment factors, security statuses, status universe, and index bars.
  - Added direct `security_statuses_as_of` test coverage for equal-`available_at` tie selection.
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
    - `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test build_trade_date_uses_deterministic_equal_available_at_versions -- --nocapture`
      - Failed as expected before the SQL tie-breaker fix because the snapshot selected the wrong equal-`available_at` status version and produced `limit_up_count = 1` instead of the expected `0`.
    - `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::market_snapshot::builder::tests::build_trade_date_marks_null_critical_bar_field_incomplete_and_excludes_breadth -- --nocapture`
      - Failed as expected because `snapshot.data_complete` stayed true when a current daily bar had `close = NULL`, demonstrating the old NULL-to-zero conversion path.
- Green:
  - Implemented `MarketBreadthMetrics` and `calculate_market_breadth`.
  - Implemented `MarketSnapshotModule::build_trade_date` and repository helpers.
  - Added focused builder integration tests for:
    - missing adjustment/status/index inputs => `data_complete = false`
    - deterministic fingerprint from sorted source IDs and timestamps
    - same trade date with different `as_of` rebuild/upsert behavior
    - fingerprint completeness for excluded securities
    - trade-date status universe missing-current-bar completeness
    - equal-`available_at` duplicate candidates selecting the later `ingested_at` and source-ascending point-in-time winners, with deterministic snapshot metrics and fingerprint
    - NULL critical daily bar fields producing field-level `missing_inputs`, incomplete snapshots, and no fabricated breadth contribution
  - Added direct repository coverage for `security_statuses_as_of` equal-`available_at` ties choosing the later `ingested_at`, then source-ascending row.

## Verification results

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::market_snapshot -- --nocapture`
  - PASS: 24 passed, 0 failed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test storage::market_repository::tests::security_statuses_as_of_breaks_equal_available_at_ties_deterministically -- --nocapture`
  - PASS: 1 passed, 0 failed
- `cargo test scheduler::tests -- --nocapture`
  - PASS: 2 passed, 0 failed
- `cargo fmt --all -- --check`
  - PASS
- `git diff --check`
  - PASS

The equal-`available_at` regression was run first in isolation to capture RED evidence, then rerun after the SQL fix and passed.

## Files changed

- `Cargo.toml`
- `Cargo.lock`
- `src/analysis/market_snapshot/builder.rs`
- `src/analysis/market_snapshot/mod.rs`
- `src/storage/market_repository.rs`
- `src/scheduler/mod.rs`
- `src/main.rs`
- `.superpowers/sdd/task-8-report.md`

Additional Task 8 re-review fix changed only:

- `src/analysis/market_snapshot/builder.rs`
- `src/storage/market_repository.rs`
- `.superpowers/sdd/task-8-report.md`

## Concerns

- The builder currently records missing bar-history gaps as `stock_daily_bar_versions:<code>:previous_close`, `stock_daily_bar_versions:<code>:lookback_20`, or `stock_daily_bar_versions:<code>:<trade_date>` when point-in-time inputs show the security should have the required bars. This is intentionally explicit, but no downstream consumer reads those strings yet.
