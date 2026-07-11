# Gate 3 Task 5 Fix 3 Report

## Findings fixed

- Enforced strict-superset frozen-graph evolution in `FrozenImpactHypothesis::evolve` by rebuilding the candidate graph, verifying every prior node payload and edge payload still exists unchanged, and rejecting evolutions that mutate/remove old graph content or fail to add new graph payload.
- Rejected stock-code-only/list labels before stock-code stripping in linked target classification and company-template fallback/source-subject handling, so pure stock-code lists cannot degrade into placeholder impact targets.
- Added regressions for mutated prior node payload, removed prior edge payload, linked non-company stock-code-list paths, direct non-company stock-code-list classification, and company fallback/source stock-code-list subjects.

## Files changed

- `src/analysis/events/hypotheses.rs`
- `.superpowers/sdd/gate3-task-5-fix3-report.md`

## Commit hash

- Code fix commit: `b1b7a46`

## Commands run

### RED

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::frozen_hypotheses_reject_mutated_prior_node_payload_when_evolving -- --nocapture
```

- Exit code: `101`
- Summary: failed as expected because `evolve` still accepted preserved claim IDs with mutated prior graph payload.

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::frozen_hypotheses_reject_removed_prior_edge_payload_when_evolving -- --nocapture
```

- Exit code: `101`
- Summary: failed as expected because `evolve` still accepted preserved claim IDs with removed prior graph edges/targets.

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::linked_non_company_stock_code_lists_fall_back_instead_of_emitting_placeholder_targets -- --nocapture
```

- Exit code: `101`
- Summary: failed as expected because linked non-company stock-code lists still emitted placeholder impact labels before the fix.

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::company_template_fallback_does_not_emit_stock_code_list_source_subjects -- --nocapture
```

- Exit code: `101`
- Summary: failed as expected because company fallback/source paths still emitted stock-code-derived impact labels before the fix.

### GREEN

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses -- --nocapture
```

- Exit code: `0`
- Summary: `16 passed; 0 failed; 355 filtered out`

```bash
cargo fmt --all -- --check
```

- Exit code: `0`

```bash
git diff --check
```

- Exit code: `0`

## Concerns

- The report records the code-fix commit hash (`b1b7a46`). The final branch tip also includes this report in a follow-up commit because a report file cannot self-embed the hash of the exact commit that introduces it.
