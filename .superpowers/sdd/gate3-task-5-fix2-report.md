# Gate 3 Task 5 Fix 2 Report

## Findings fixed

- Moved the non-direct beneficiary/list filter ahead of all target classification in `target_from_claim_node`, so beneficiary/list labels cannot leak through `DemandFact`, `SupplyFact`, `PriceFact`, `PolicyFact`, `RegulatoryFact`, or generic/source-label fallback classification.
- Applied the same beneficiary/list filter to the company-template fallback path, so source labels such as beneficiary baskets cannot become `RevenueImpact` or `MarginImpact` nodes.
- Added focused regression coverage for non-company fact target classification and company-template fallback behavior.

## Files changed

- `src/analysis/events/hypotheses.rs`
- `.superpowers/sdd/gate3-task-5-fix2-report.md`

## Commit hash

- Final commit hash is reported in the completion response. A committed report file cannot stably embed the hash of the commit that contains that same file.

## Commands run

### RED

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::target_from_claim_node_rejects_non_direct_beneficiary_labels_across_non_company_paths -- --nocapture
```

- Exit code: `101`
- Summary: failed as expected because `DemandFact` beneficiary/list labels still produced targets.

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::company_template_fallback_does_not_emit_beneficiary_list_source_labels -- --nocapture
```

- Exit code: `101`
- Summary: failed as expected because company-template fallback still emitted beneficiary/list source labels as impact targets.

### Focused GREEN

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::target_from_claim_node_rejects_non_direct_beneficiary_labels_across_non_company_paths -- --nocapture
```

- Exit code: `0`
- Summary: `1 passed; 0 failed`

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::company_template_fallback_does_not_emit_beneficiary_list_source_labels -- --nocapture
```

- Exit code: `0`
- Summary: `1 passed; 0 failed`

### Final verification

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses -- --nocapture
```

- Exit code: `0`
- Summary: `11 passed; 0 failed; 355 filtered out`

```bash
cargo fmt --all -- --check
```

- Exit code: `0`

```bash
git diff --check
```

- Exit code: `0`

## Concerns

- None beyond the unavoidable self-reference limitation for embedding the containing commit hash inside the committed report file.
