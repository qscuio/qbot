# Gate 4 Task 9 Report

## Status
- Completed
- Commit: `0c22914` (`refactor: route market overview through decision support`)

## Changed Files
- `src/services/ai_analysis.rs`
  - Replaced legacy provider/SQL/LLM market fact generation with DecisionSupport compatibility mode.
  - Marked `aiNarrative` as deprecated in source comments and now render it from structured DecisionSupport brief content.
  - Added a persisted-artifact regression test proving `market_overview` succeeds without LLM access.
- `src/api/routes.rs`
  - Updated Telegram `/ai_analysis` status text to reflect DecisionSupport-backed compatibility output.
- `src/services/mod.rs`
  - Registered a small internal compatibility helper module.
- `src/services/decision_support_compat.rs`
  - Added the deterministic load/build helper required by the brief:
    - load persisted DecisionSupport artifact for the requested date when present
    - otherwise build DecisionSupport with `persist_run = true`
    - load market snapshot metrics without touching trading tables
- `src/main.rs`
  - Removed the scheduled free-form AI loop startup.
  - Left an info log clarifying that `ENABLE_AI_ANALYSIS` no longer starts a loop because DecisionSupport scheduling owns daily generation.

## Verification
1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test services::ai_analysis -- --nocapture`
   - Passed
   - Result: `1 passed; 0 failed`
2. `cargo fmt --all -- --check`
   - Failed once on formatting in `src/services/ai_analysis.rs`
   - Ran `cargo fmt --all`
   - Re-ran `cargo fmt --all -- --check`
   - Passed
3. `git diff --check`
   - Passed

## Self-Review
- `market_overview` and `generate_daily_report` no longer make direct LLM calls.
- `/api/market/overview` still returns the existing compatibility fields.
- `aiNarrative` is explicitly deprecated in source comments and now comes from the structured DecisionSupport brief.
- The old 15:30 scheduled free-form loop is removed from runtime startup.
- The added test uses:
  - a persisted DecisionSupport artifact
  - a persisted market snapshot
  - a failing local LLM endpoint trap
  - panic providers
  - and proves no LLM request is made while `market_overview` still succeeds.

## Concerns
- `topSectors` and `bottomSectors` now serialize as empty arrays in compatibility mode because persisted DecisionSupport artifacts do not currently store sector leaderboard data.
- `topStock` is now mapped to the highest-ranked DecisionSupport candidate rather than a separately computed top-gainer/trend result. This keeps the field available, but it is compatibility output, not the legacy semantic source.

---

## Fix: market overview compatibility semantics

### Changed Files
- `src/services/decision_support_compat.rs`
  - Loaded factual sector leaderboards from `sector_daily`.
  - Loaded factual legacy `topStock` from `stock_daily_bars` + `stock_info`.
  - Reused `TrendAnalyzer::analyze` for factual trend computation from stored bar history.
- `src/services/ai_analysis.rs`
  - Kept legacy factual `topStock` separate from DecisionSupport candidate data.
  - Exposed DecisionSupport candidate data under `topDecisionCandidate`.
  - Serialized compatibility metadata only when factual legacy fields are unavailable.
  - Added regression coverage proving factual sector/top-stock compatibility output and zero LLM calls.

### Verification
1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test services::ai_analysis -- --nocapture`
   - Passed
   - Result: `2 passed; 0 failed`
2. `cargo fmt --all -- --check`
   - Passed
3. `git diff --check`
   - Passed

### Concerns
- None.

---

## Follow-up Fix

### Status
- Completed
- Commit: `9f2704a` (`fix: preserve market overview compatibility semantics`)

### Changed Files
- `src/services/ai_analysis.rs`
  - Restored honest legacy compatibility semantics for `topSectors`, `bottomSectors`, and `topStock`.
  - Added `topDecisionCandidate` so DecisionSupport candidate data is no longer mislabeled as the legacy top-gainer field.
  - Added serialized `compatibility` markers when legacy fields are unavailable or only partially populated.
  - Reused factual top-stock trend data instead of leaving `TrendAnalyzer::analyze` unused.
  - Added regression coverage for both unavailable-field markers and factual legacy-field preservation.
- `src/services/decision_support_compat.rs`
  - Loaded factual sector leaderboards and factual top-stock/trend data into the compatibility context from stored market tables.
  - Fixed a duplicate import in the current branch state so the target tests compile and run.

### Verification
1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test services::ai_analysis -- --nocapture`
   - Passed
   - Result: `2 passed; 0 failed`
2. `cargo fmt --all -- --check`
   - Initially failed on formatting only
   - Ran `cargo fmt --all`
   - Re-ran check and it passed
3. `git diff --check`
   - Passed

### Self-Review
- `market_overview` and `generate_daily_report` still avoid direct LLM calls.
- The scheduled 15:30 free-form AI loop remains removed.
- No trading-table writes were added.
- Legacy market overview fields now come from stored factual tables when available:
  - sector movers from `sector_daily`
  - top gainer and trend from `stock_daily_bars` plus `TrendAnalyzer`
- When those legacy facts are unavailable, the response now says so explicitly instead of returning silent false defaults.

---

## Re-review Fix: legacy top stock compatibility

### Changed Files
- `src/services/decision_support_compat.rs`
  - Restored legacy `LEFT JOIN stock_info` behavior for factual top-stock lookup.
  - Restored `COALESCE(i.name, b.code)` so missing metadata falls back to the stock code instead of dropping the row.
  - Filtered to `prev_close > 0` before computing `change_pct`, preventing nullable decode failures for zero previous close rows.
- `src/services/ai_analysis.rs`
  - Added regression coverage for a top gainer with missing `stock_info` metadata.
  - Added regression coverage proving `/api/market/overview` stays successful when candidate rows have zero previous close.

### Verification
1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test services::ai_analysis -- --nocapture`
   - Passed
   - Result: `4 passed; 0 failed`
2. `cargo fmt --all -- --check`
   - Passed
3. `git diff --check`
   - Passed

### Concerns
- None.
