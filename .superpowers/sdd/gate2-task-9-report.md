# Gate 2 Task 9 Report

## Implementation

- Added `src/analysis/events/reporting.rs` with deterministic `DailyEventBrief` building and rendering from structured claims and sources only.
- Wired `EventIntelligence::build_daily_brief` to:
  - read latest publishable evidence for the trade date,
  - resolve latest extraction rows across source-item lineage,
  - classify facts into `今日新增事实` and `今日修订`,
  - keep non-published/non-fact claims in `未确认内容`,
  - derive direct companies and industries from structured extraction payload entities,
  - require evidence-backed source references for rendered brief items.
- Implemented `EventIntelligence::process_pending` to:
  - read latest pending evidence before a cutoff,
  - run structured extraction,
  - persist extraction claims with fact-only publication,
  - publish a new append-only evidence version through the existing revision path.
- Added scheduler jobs:
  - `run_event_ingestion_job(state: Arc<AppState>)`
  - `run_event_fact_brief_job(state: Arc<AppState>)`
- Scheduled:
  - event ingestion: `0 5 9-17 * * Mon,Tue,Wed,Thu,Fri`
  - fact brief: `0 50 17 * * Mon,Tue,Wed,Thu,Fri`
- Event ingestion uses a concrete Redis provider cursor key: `market_event:provider_cursor:<source_id>`.
- Failure isolation is preserved: event ingestion and fact-brief failures log and return; they do not panic and do not run inside `run_daily_report_job`.

## Extra Files / Scope Notes

- Modified `src/storage/event_repository.rs` beyond the planned list because the brief needed real repository reads for:
  - latest pending evidence,
  - latest extraction per evidence lineage.
- Added this report file as requested.

## Verification

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::reporting -- --nocapture`
  - Passed: 3 tests
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test scheduler -- --nocapture`
  - Passed: 11 tests
- `cargo fmt --all -- --check`
  - Passed
- `git diff --check`
  - Passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --all --locked -- --skip config::tests::test_config_defaults`
  - Passed: 313 tests
- `cargo test --all --locked config::tests::test_config_defaults`
  - Passed: 1 test

## Known Concerns

- The new path is verified and passes the requested suites, but the crate still emits pre-existing/adjacent warning noise for unused items outside this task’s required scope.
