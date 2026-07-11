# Gate 0 Task 7 Report: Adjusted-Price Calculation

## Implemented

- Added and finalized `src/analysis/market_snapshot/adjustment.rs`.
- `adjust_candles` now:
  - selects the latest factor by `trade_date`,
  - maps each date to a single `adj_factor` and errors on duplicate dates,
  - scales `open`, `high`, `low`, and `close` by `factor / latest_factor`,
  - preserves all non-price fields (`trade_date`, volume, amount, turnover, pe, pb) via clone-update semantics.

## Test coverage updates

- Added/kept duplicate-date coverage via `rejects_duplicate_adjustment_factors_for_same_trade_date`.
- Added assertions in the success path for `adjusted[1].open`, `adjusted[1].high`, and `adjusted[1].low` so both adjusted bars now have full OHLC coverage.
- This is a test-strengthening change only; no production logic was changed.

## Verification

- `cargo test adjustment::tests -- --nocapture`: PASS, **4 passed**.
- `cargo fmt --all -- --check`: PASS.
- `git diff --check`: PASS.

## Notes

- The test set now fully covers OHLC scaling behavior for the first and latest-factor bars.
- The remaining warnings in broader `cargo test` runs are pre-existing and unrelated to this task.
