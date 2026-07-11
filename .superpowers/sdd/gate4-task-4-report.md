## Gate 4 Task 4 Report

### Summary

Implemented the first candidate fusion step for `analysis::decision_support` by adding a read-only pattern adapter and wiring `build_daily` to merge archived scan-ranker baselines with persisted shadow pattern candidates.

### Files Changed

- `src/analysis/decision_support/pattern_adapter.rs`
  - Added candidate ranking, `(code, horizon)` merge logic, percentile normalization, support-tier reconciliation, explanation assembly, risk flag flattening, and invalidation flattening.
- `src/analysis/decision_support/builder.rs`
  - Wired `build_daily` to load scan-ranker baselines and shadow pattern candidates, then build merged `DecisionCandidate` values.
  - Added builder-level tests for merge rules and reject-vs-A disagreement handling.
- `src/analysis/decision_support/mod.rs`
  - Exposed `pattern_adapter`.

### Implementation Notes

- Merge source rules:
  - scan-ranker only -> `base_source = scan_ranker`
  - pattern only -> `base_source = pattern_shadow`
  - both -> `base_source = combined`
- Candidates are joined by `(code, horizon)`.
- Source raw scores are not averaged.
  - `base_score` uses the max of source rank percentiles.
  - `pattern_score` stores the pattern percentile when present.
  - Raw scan-ranker score and raw pattern final score are retained in support statements rather than mixed numerically.
- Support-tier handling is conservative for direct disagreement:
  - if a positive scan-ranker tier is paired with pattern `reject`, the merged candidate is capped below `A`
  - `scan_pattern_disagreement` is added to `risk_flags`
  - both scan and pattern explanations remain attached
- Event adjustment remains `0.0`.
- No writes were added to `signal_strategy_candidates` or any trading path.

### Verification Run

1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::decision_support::builder -- --nocapture`
   - Result: passed
   - Tests:
     - `classifies_support_statements_into_reason_buckets`
     - `build_decision_candidates_merges_scan_and_pattern_sources_without_rescaling_raw_scores`
     - `build_decision_candidates_adds_disagreement_risk_when_pattern_rejects_scan_a`
     - `build_daily_returns_read_only_daily_support_context`

2. `cargo fmt --all -- --check`
   - Result: passed

3. `git diff --check`
   - Result: passed

### Concerns

- Rank percentile calculation uses deterministic ordinal ranking over the loaded candidate lists because no shared percentile config or persisted percentile field exists yet. If later tasks need a different percentile convention, this adapter will need to align with that contract.
- The required verification command still emits existing unrelated repository warnings during compilation.
