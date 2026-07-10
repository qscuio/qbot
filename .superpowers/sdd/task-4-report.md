# Gate 0 Task 4 Report

## What I implemented

- Added `src/data/point_in_time_provider.rs` with the required `PointInTimeCapabilities` struct and `PointInTimeDataProvider` trait.
- Exported the point-in-time provider module from `src/data/mod.rs`.
- Extended `TushareClient` with bounded point-in-time capability probing and cached probe results.
- Implemented Tushare point-in-time methods for:
  - security master versions
  - corporate actions
  - adjustment factors
  - daily basics
  - daily security statuses
  - historical index bars
- Kept historical sector membership unsupported unless a verified historical as-of source exists; no current-state membership fallback is used.
- Added explicit `AppError::DataProvider` blocking when a required capability is unsupported.
- Added fixture-based parser tests for security master, corporate actions, daily basics, adjustment factors, and security status.
- Added a capability test asserting unauthorized historical sector membership is recorded as unsupported with an unauthorized detail.
- Wired a shared `Arc<TushareClient>` into both `DataProvider` and `PointInTimeDataProvider` while leaving the existing fallback provider chain unchanged.
- Added `point_in_time_provider` to `AppState`.
- Persisted the latest startup probe to `analysis_data_runs.details` with `run_type='point_in_time_capability_probe'`.
- Added protected `GET /api/analysis/data-status` exposing the latest probe status and exact missing capabilities.

## Tests run and results

- `cargo test data::tushare -- --nocapture` - PASS, 8 passed.
- `cargo test config::tests::test_config_defaults` - PASS, 1 passed.
- `cargo fmt --all -- --check` - PASS.
- `git diff --check` - PASS.

The test commands emit existing dead-code warnings from the binary test build, including the new point-in-time trait methods being unused until later tasks call them.

## TDD evidence

### RED

Command:

```bash
cargo test data::tushare -- --nocapture
```

Initial output excerpt:

```text
error[E0432]: unresolved import `crate::data::point_in_time_provider`
error[E0599]: no function or associated item named `parse_security_master_versions` found for struct `tushare::TushareClient`
error[E0599]: no function or associated item named `parse_corporate_actions` found for struct `tushare::TushareClient`
error[E0599]: no function or associated item named `parse_daily_basics` found for struct `tushare::TushareClient`
error[E0599]: no function or associated item named `parse_adjustment_factors` found for struct `tushare::TushareClient`
error[E0599]: no function or associated item named `parse_security_statuses` found for struct `tushare::TushareClient`
error[E0599]: no function or associated item named `record_probe_result` found for struct `tushare::TushareClient`
error: could not compile `qbot` (bin "qbot" test) due to 7 previous errors
```

### GREEN

Command:

```bash
cargo test data::tushare -- --nocapture
```

Final output excerpt:

```text
running 8 tests
test data::tushare::tests::parses_security_master_fixture ... ok
test data::tushare::tests::parses_corporate_actions_fixture ... ok
test data::tushare::tests::parses_security_status_fixture ... ok
test data::tushare::tests::parses_daily_basics_fixture ... ok
test data::tushare::tests::parses_adjustment_factors_fixture ... ok
test data::tushare::tests::test_safe_f64 ... ok
test data::tushare::tests::unauthorized_historical_sector_membership_is_reported_as_unsupported ... ok
test data::tushare::tests::test_tushare_code_convert ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 98 filtered out
```

An intermediate GREEN attempt exposed a real parser issue:

```text
assertion `left == right` failed
  left: Some(0.6000000000000001)
 right: Some(0.6)
```

I fixed the parser to normalize summed stock ratios instead of weakening the fixture assertion.

## Files changed

- `src/data/point_in_time_provider.rs`
- `src/data/mod.rs`
- `src/data/tushare.rs`
- `src/state.rs`
- `src/main.rs`
- `src/api/routes.rs`
- `.superpowers/sdd/task-4-report.md`

## Self-review findings

- The existing fallback provider chain remains `tushare -> eastmoney -> tencent -> db`.
- No plan documents, migrations, storage repository code, or fallback providers were modified.
- Historical sector membership is explicitly unsupported when only current Tushare membership is available; this avoids inferred history.
- Live fetched rows use `AvailabilityQuality::Observed` and `chrono::Utc::now()` for `available_at`/`ingested_at`.
- No Rust code spawns Python.
- The status endpoint uses the existing protected-route bearer token behavior.

## Concerns

- `historical_sector_membership` is expected to be reported missing unless the configured Tushare account exposes a verified historical membership endpoint; current `ths_member` data is deliberately not substituted.
- Startup now performs bounded live probe requests to Tushare and records the result. Probe failure is persisted as a failed probe and logged, but does not change existing scanner/fallback behavior.
- The daily security status method uses verified Tushare sources (`daily`, `stk_limit`, `suspend_d`, `stock_basic`, `namechange`) and does not infer ST status from current names; later tasks may want a more direct historical ST endpoint if available for the configured account.

## Task 4 Review Fixes

### What changed

- Moved point-in-time capability probe persistence and latest status lookup SQL into `MarketRepository`.
- Changed startup probing to run in a background task after `AppState` construction so live Tushare probes no longer block service startup or fallback provider availability.
- Kept probe success and failure persistence through `analysis_data_runs`, with logging for both probe failures and persistence failures.
- Replaced the fabricated historical membership probe test with a test that drives the factored probe path and verifies `ths_member` is called and unauthorized responses mark `historical_sector_membership` unsupported.
- Made `daily_security_status` probe all endpoints required by `get_security_statuses()` (`daily`, `stk_limit`, `suspend_d`, `stock_basic`, `namechange`) and report missing/unauthorized dependencies in details.
- Added a repository round-trip test for persisted probe status. The status value for missing capabilities is `missing` so it fits the existing `analysis_data_runs.status VARCHAR(20)` column.

### Test results

- `cargo test data::tushare -- --nocapture` - PASS, 9 passed.
- `export DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot; cargo test storage::market_repository -- --nocapture` - PASS, 11 passed.
- `cargo test config::tests::test_config_defaults` - PASS, 1 passed.
- `cargo fmt --all -- --check` - PASS.
- `git diff --check` - PASS.

### Files changed

- `src/data/tushare.rs`
- `src/storage/market_repository.rs`
- `src/main.rs`
- `src/api/routes.rs`
- `.superpowers/sdd/task-4-report.md`

### Concerns

- The required test commands still emit existing dead-code warnings from the binary test build.
- `analysis_data_runs.status` is limited to 20 characters, so missing point-in-time prerequisites are persisted as status `missing`; exact missing capabilities remain in `details.missing_capabilities`.
