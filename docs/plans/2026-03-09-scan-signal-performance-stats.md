# Scan Signal Performance Stats Plan

## Goal

Add a minimal forward-return stats view for scanner hits so each signal can be evaluated by sample count, average return, and win rate after 1/3/5/10 trading days.

## Scope

- Add a pure aggregation module in `src/services/scanner_stats.rs`
- Query deduped scan hits from `scan_results` and future closes from `stock_daily_bars`
- Expose stats through `GET /api/scan/stats`
- Expose a Telegram command `/scan_stats`

## Data Rules

- Use `scan_results` as the event table
- Deduplicate by `signal_id + code + scan_date`
- Treat scan date close as the entry price
- Compute forward closes at the next 1/3/5/10 trading sessions
- Aggregate per signal:
  - total samples
  - horizon sample size
  - average return %
  - win rate %

## Verification

- `cargo test scanner_stats::tests -- --nocapture`
- `cargo test api::routes::tests -- --nocapture`
- `cargo fmt --check`
- `cargo check`
