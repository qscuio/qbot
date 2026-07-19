# Historical Chips Task 2 Report

## Scope

Implemented the pure, chronological `ChipModelV2` estimator and exposed it from the services module. No database, network, dashboard, Telegram, mini-app, scheduler, signal, or trading code was changed.

## TDD evidence

### RED

1. Added the complete pure model test suite before the model implementation and ran:

   `cargo test --locked services::chip_model::tests -- --nocapture`

   The build failed with `E0432` because `ChipModelV2` and `CHIP_MODEL_VERSION` did not exist. This was the expected missing-feature failure.

2. During refactoring, added the existing-domain-compatible top-five concentration expectation before changing the implementation. The focused test failed with `expected 100, got 50`, proving it distinguished top-five concentration from dominant-bucket weight.

### GREEN

Ran:

`cargo test --locked services::chip_model::tests -- --nocapture`

Result: 11 passed, 0 failed, 561 filtered out.

Coverage includes first-day zero-turnover bootstrap, normalization, later zero turnover, 100% replacement, flat bars, factor 1→2 halving and 2→1 doubling, adaptive conservative rebinning, deterministic peak ties and metrics, retention beyond 120 days, invalid/out-of-order/cross-stock atomicity, JSON serialize/restore/resume across an immediate factor change, malformed restore rejection, and deterministic repeatability.

Additional verification:

- `cargo fmt --check` — passed
- `git diff --check` — passed

The test build emitted only the repository's existing unused-code and future-incompatibility warnings.

## Files

- Created `src/services/chip_model.rs`
- Modified `src/services/mod.rs`
- Created `.superpowers/sdd/historical-chip-task-2-report.md`

## Implementation notes

- Persists the previous adjustment factor and rebases retained prices with `last_adjustment_factor / current_adjustment_factor`.
- Maintains exactly the configured bucket count with an adaptive grid spanning retained adjusted history and the current bar.
- Conservatively redistributes old point mass between adjacent grid prices, then adds normalized triangular replacement mass.
- Uses a deterministic trade-date midnight timestamp, source `qbot_estimate`, version `qbot-chip-v2`, and `validated=false`.
- Winner rate is mass at or below close; concentration retains the existing product meaning of mass in the five heaviest buckets; exact dominant-weight ties select the lowest price.
- Validates all inputs before computation and commits the next state only after every derived invariant passes.

## Concerns

The estimator intentionally exposes an estimate, not an official chip distribution. Calibration and canonical-source selection remain the responsibility of the later validation task.

## Independent-review fixes

Two Important review findings were fixed in a separate follow-up commit.

### RED

- Added a restored three-bucket `1..1000` history followed by a `10..11` current bar. Before the fix, the expected 50% replacement mass inside the current bar was absent: `expected 0.5, got -0`.
- Added constructor boundaries for bucket counts `0`, `1`, and `2`, plus acceptance of `3`. Before the fix, the test reached count `1` and failed because it was accepted. A normalized two-bucket restored state is also rejected.

### GREEN

- The adaptive grid now always contains the day's deterministic weighted typical price while preserving the retained-history bounds and exact bucket count. This gives even a narrow current bar an in-range representation instead of collapsing replacement mass onto a distant old grid price.
- Conservative rebinning now interpolates over adjacent prices on the resulting non-uniform grid, preserving retained mass and its weighted cost.
- Both constructors and restored state enforce a minimum of three buckets.
- `cargo test --locked services::chip_model::tests -- --nocapture`: 13 passed, 0 failed, 561 filtered out.
- `cargo test --locked analysis::market_snapshot::adjustment::tests::adjusts_ohlc_to_latest_factor_preserving_non_price_fields -- --nocapture`: 1 passed, 0 failed, 573 filtered out.
- `cargo fmt --check`: passed.
- `git diff --check`: passed.
