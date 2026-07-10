### Task 9 Report: Add data-completeness API and production schedule

**Implementation summary**
- Created `src/api/analysis_routes.rs` with `analysis_router`.
- Moved `/api/analysis/data-status` out of the main routes file and mounted the analysis sub-router from `build_router`.
- Added protected analysis endpoints:
  - `GET /api/analysis/data-status`
  - `POST /api/jobs/analysis/point-in-time/refresh`
  - `POST /api/jobs/analysis/point-in-time/reference-refresh`
  - `POST /api/jobs/analysis/snapshot`
- The data-status response exposes camelCase snapshot fields: `tradeDate`, `snapshotVersion`, `dataComplete`, `missingInputs`, `availableAt`, and `inputFingerprint`.
- The data-status response also reports `capabilityFailures`, `completeness`, `estimatedRowCounts`, `capabilityStatus`, and `latestRuns`.
- Added `AppState.analysis_job_lock` and initialized it in `main.rs`.
- Serialized scheduled and manual point-in-time refresh/snapshot work by locking inside:
  - `run_point_in_time_trade_date_refresh_job`
  - `run_point_in_time_reference_refresh_job`
  - `run_market_snapshot_job`
- Added production crons exactly as requested:
  - `0 10 17 * * Mon,Tue,Wed,Thu,Fri`
  - `0 20 17 * * Mon,Tue,Wed,Thu,Fri`
  - `0 30 20 * * Fri`
- Registered the daily point-in-time trade-date refresh after the legacy fetch, market snapshot after that refresh, and reference refresh on Friday evening.
- Updated README with the point-in-time/current-state caveat, pattern-research blocking prerequisites, status endpoint behavior, manual endpoints, and schedule entries.

**RED evidence**
- Added `api::analysis_routes::tests`.
- Initial focused run:
  - Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture`
  - Result: failed as expected with 404s for `/api/analysis/data-status`; tests expected 401/200 route behavior.

**GREEN evidence**
- Focused route tests:
  - Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture`
  - Result: PASS, 2 passed.
- Focused scheduler tests:
  - Command: `cargo test --locked scheduler::tests -- --nocapture`
  - Result: PASS, 3 passed.

**Verification results**
- Command: `cargo fmt --all -- --check`
  - Result: PASS.
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --all --locked`
  - Result: FAIL, 121 passed and 22 failed. The failing SQLx tests reported `DATABASE_URL must be set: EnvVar(NotPresent)`.
- Additional command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --all --locked -- --test-threads=1`
  - Result: FAIL with the same 22 SQLx `DATABASE_URL must be set` failures.
- Command: `git diff --check`
  - Result: PASS.

**Files changed**
- `.superpowers/sdd/task-9-report.md`
- `README.md`
- `src/api/analysis_routes.rs`
- `src/api/mod.rs`
- `src/api/routes.rs`
- `src/main.rs`
- `src/scheduler/mod.rs`
- `src/state.rs`

**Concerns**
- The exact full-test command fails because `config::tests::test_config_defaults` removes `DATABASE_URL` from the shared process environment, after which SQLx tests cannot create test databases. This was left unchanged because it is outside Task 9 scope. Focused Task 9 tests and formatting/diff verification pass.

---

### Task 9 Review Fix Follow-up

**Fix summary**
- Scoped `completeness.pointInTimeRefreshComplete` to a successful latest trade-date refresh run whose `analysis_data_runs.trade_date` matches the returned snapshot `tradeDate`.
- Returned explicit JSON `null` for `missingInputs` and `completeness.missingInputCount` when no persisted market snapshot exists.
- Reused the exported `MARKET_SNAPSHOT_VERSION` from the market snapshot builder.
- Updated the README key-file summary from 5 scheduler cron jobs to 8.

**RED evidence**
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture`
- Result: FAIL as expected, 2 passed and 2 failed:
  - `data_status_does_not_guess_missing_inputs_without_snapshot` saw `missingInputs: []` instead of `null`.
  - `data_status_scopes_refresh_completeness_to_snapshot_trade_date` saw `pointInTimeRefreshComplete: true` for a latest refresh run with a different `trade_date`.

**GREEN evidence**
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture`
  - Result: PASS, 4 passed.
- Command: `cargo test --locked scheduler::tests -- --nocapture`
  - Result: PASS, 3 passed.

**Verification results**
- Command: `cargo fmt --all -- --check`
  - Result: PASS.
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --all --locked -- --skip config::tests::test_config_defaults`
  - Result: PASS, 144 passed and 1 filtered out.
- Command: `cargo test --all --locked config::tests::test_config_defaults`
  - Result: PASS, 1 passed and 144 filtered out.
- Command: `git diff --check`
  - Result: PASS.

**Files changed**
- `.superpowers/sdd/task-9-report.md`
- `README.md`
- `src/analysis/market_snapshot/builder.rs`
- `src/api/analysis_routes.rs`

---

### Task 9 Re-review Fix Follow-up

**Fix summary**
- Changed missing capability-probe rows so `capabilityFailures` is empty instead of reporting the `point_in_time_capability_probe` run type as a provider capability failure.
- Added `capabilityProbe` to the data-status response so a missing probe row is explicit: `{"persisted": false, "status": "not_persisted", "completed": false}`.
- Added coverage that the merged `crate::api::routes::build_router` exposes all analysis routes, catching removed or mis-mounted analysis-router merges.

**RED evidence**
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture`
- Result: FAIL as expected, 5 passed and 1 failed:
  - `data_status_reports_missing_capability_probe_without_capability_failure` saw `capabilityFailures: ["point_in_time_capability_probe"]` instead of `[]`.

**GREEN evidence**
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture`
- Result: PASS, 6 passed.

**Verification results**
- Command: `cargo fmt --all -- --check`
  - Result: PASS.
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture`
  - Result: PASS, 6 passed.
- Command: `cargo test --locked scheduler::tests -- --nocapture`
  - Result: PASS, 3 passed.
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --all --locked -- --skip config::tests::test_config_defaults`
  - Result: PASS, 146 passed and 1 filtered out.
- Command: `cargo test --all --locked config::tests::test_config_defaults`
  - Result: PASS, 1 passed and 146 filtered out.
- Command: `git diff --check`
  - Result: PASS.

**Files changed**
- `.superpowers/sdd/task-9-report.md`
- `src/api/analysis_routes.rs`

---

### Task 9 Repository Boundary Fix Follow-up

**Fix summary**
- Moved the `data-status` read SQL for market snapshots and analysis runs out of `src/api/analysis_routes.rs` and into `src/storage/market_repository.rs`.
- Added repository-owned typed read models `DataStatusSnapshot` and `AnalysisRunSummary`.
- Added `MarketRepository::latest_market_snapshot`, `MarketRepository::latest_analysis_run`, and `MarketRepository::latest_analysis_runs` using the existing endpoint SQL.
- Kept `analysis_routes.rs` focused on auth, handler flow, completeness calculation, and JSON shaping, with no inline SQL.
- Preserved the existing response behavior and JSON shape, including `missingInputs: null` when no snapshot exists, empty `capabilityFailures` when the capability probe is absent, snapshot-scoped `pointInTimeRefreshComplete`, and the shared `MARKET_SNAPSHOT_VERSION`.

**RED evidence**
- Added focused repository tests for the new read boundary.
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked latest_market_snapshot_reads_latest_versioned_snapshot -- --nocapture`
- Result: FAIL at compile as expected before the implementation. Rust reported missing `MarketRepository` methods:
  - `latest_market_snapshot`
  - `latest_analysis_run`
  - `latest_analysis_runs`

**GREEN evidence**
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked latest_market_snapshot_reads_latest_versioned_snapshot -- --nocapture`
  - Result: PASS, 1 passed.
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked latest_analysis_run_queries_return_latest_requested_runs -- --nocapture`
  - Result: PASS, 1 passed.

**Verification results**
- Command: `cargo fmt --all -- --check`
  - Result: PASS.
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture`
  - Result: PASS, 6 passed.
- Command: `cargo test --locked scheduler::tests -- --nocapture`
  - Result: PASS, 3 passed.
- Command: `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --all --locked -- --skip config::tests::test_config_defaults`
  - Result: PASS, 148 passed and 1 filtered out.
- Command: `cargo test --all --locked config::tests::test_config_defaults`
  - Result: PASS, 1 passed and 148 filtered out.
- Command: `git diff --check`
  - Result: PASS.

**Files changed**
- `.superpowers/sdd/task-9-report.md`
- `src/api/analysis_routes.rs`
- `src/storage/market_repository.rs`
