# Gate 2 Task 9 Fix 2 Report

## Changed Files

- `src/analysis/events/mod.rs`
- `.superpowers/sdd/gate2-task-9-fix2-report.md`

## Summary

- Added a regression test proving `build_brief_entity_records` must omit `beneficiary` entities from `直接涉及公司与行业` while retaining direct `subject` entities such as issuers and industries.
- Restricted brief entity extraction to the existing direct-role vocabulary already present in the extraction schema by requiring `role == "subject"` before mapping supported entity types.

## RED Evidence

### `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`

- Failed: `analysis::events::tests::brief_entity_records_exclude_beneficiaries_and_keep_direct_subjects`
- Assertion failure showed `Beneficiary Holdings` was incorrectly included in `left`:
  - `left`: `... BriefEntityRecord { entity_id: "000001.SZ", display_name: "Beneficiary Holdings" }`
  - `right`: omitted that beneficiary entity as expected
- Result: `63 passed; 1 failed`

## GREEN Evidence

### `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`

- Passed: `64 passed; 0 failed`

### `cargo fmt --all -- --check`

- Passed

### `git diff --check`

- Passed

## Tests Run

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
- `cargo fmt --all -- --check`
- `git diff --check`

## Commit Hash

- `PENDING`

## Concerns

- None.
