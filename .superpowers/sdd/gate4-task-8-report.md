# Gate 4 Task 8 Report

## Status

Completed on worktree `/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation`.

Commit: `a5987d2` (`feat: schedule daily decision-support build`)

## Changed Files

- `src/scheduler/mod.rs`
  - Added `DECISION_SUPPORT_JOB_CRON = "0 55 17 * * Mon,Tue,Wed,Thu,Fri"`.
  - Added `run_decision_support_job(state: Arc<AppState>)`.
  - Registered the DecisionSupport job after event market observation and before the daily report.
  - Added scheduler tests for cron ordering, persistence, degradation, incomplete snapshot handling, and failure isolation.
- `README.md`
  - Documented the 17:55 weekday DecisionSupport build in the scheduler table.
  - Added manual job/read examples and API table entries for DecisionSupport endpoints.
- `src/analysis/decision_support/builder.rs`
  - Extra file beyond the requested ownership surface.
  - Needed to satisfy the required degradation behavior exercised by the scheduler job:
    - persist missing-pattern status while falling back to scan-ranker baseline,
    - persist `dataStatus` in the stored DecisionSupport brief payload,
    - withhold `A` tier assignments when the market snapshot is incomplete.

## Verification

1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test scheduler::tests -- --nocapture`
   - Passed: `30 passed; 0 failed`
2. `cargo fmt --all -- --check`
   - Passed
3. `git diff --check`
   - Passed

## Self-Review

- Trade-date resolution prefers `latest_stock_trade_date` and falls back to the latest persisted market snapshot trade date, matching the plan requirement for consistency with analysis jobs.
- The scheduler job only takes `analysis_job_lock`; it does not take `daily_report_job_lock`.
- Failures are isolated to warning logs and early return paths.
- No trading-table writes were introduced; the test suite also guards `signal_strategy_candidates` from accidental mutation.
- The persisted DecisionSupport brief now carries `dataStatus`, which gives the read path a stable artifact-backed view of incomplete snapshot state.

## Concerns

- The added degradation behavior lives in the builder because the scheduler is required to call `DecisionSupport::build_daily(..., persist_run=true)`. That keeps the persisted artifacts internally consistent, but it also means any other caller using `persist_run=true` will inherit the same missing-pattern and incomplete-snapshot behavior, which is intentional here.
- The test command still emits pre-existing warnings in unrelated modules (`analysis/events`, `signals`, `storage`, `telegram`). They do not block this task, but they remain in the branch state.

---

## Gate 4 Task 8 Review Fix

### Status

Completed on worktree `/home/ubuntu/work/qbot/.worktrees/point-in-time-data-foundation`.

Fix commit: `a280684` (`fix: scope decision support patterns to latest set`)

### Changed Files

- `src/analysis/decision_support/builder.rs`
  - Resolved `latest_published_set` before loading shadow candidates.
  - Scoped pattern candidate loading to the latest set’s `pattern_set_id`.
- `src/storage/pattern_repository.rs`
  - Added `list_shadow_candidates_for_set(trade_date, pattern_set_id)`.
  - Added a repository regression test for same-date rows across multiple published sets.
- `src/scheduler/mod.rs`
  - Added a scheduler regression test covering stale rows in an older published set plus a newer published set with no rows.
  - Added narrow test helpers to seed published sets and per-set shadow rows.

### Verification

1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test scheduler::tests -- --nocapture`
   - Passed: `31 passed; 0 failed`
2. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test storage::pattern_repository -- --nocapture`
   - Passed: `12 passed; 0 failed`
3. `cargo fmt --all -- --check`
   - Passed
4. `git diff --check`
   - Passed

### Self-Review

- The builder now keeps pattern evidence and persisted pattern-set metadata aligned to the same latest published set.
- When the latest published set exists but has no rows for the trade date, decision support falls back to scan-ranker baseline only and still records missing-pattern status.
- The regression test proves stale rows from an older published set no longer leak into persisted decision-support candidates.
