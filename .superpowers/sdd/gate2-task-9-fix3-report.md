# Gate 2 Task 9 Fix 3 Report

## Changed Files

- `src/analysis/events/mod.rs`
- `.superpowers/sdd/gate2-task-9-fix3-report.md`

## Scope

Addressed the remaining revision-mapping review findings by:

1. Preserving brief claim order from `extracted_payload.claims` instead of stored `claim_id` order.
2. Assigning `previous_fact_id` values only while iterating published fact claims.

`src/storage/event_repository.rs` was not changed. The ordering fix is derived from structured extraction payload order inside `src/analysis/events/mod.rs`.

## RED Evidence

Command:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture
```

Observed failures:

- `analysis::events::tests::brief_claim_records_only_assign_previous_ids_to_published_facts`
  - non-fact claim incorrectly received `Some(previous_fact_id)` instead of `None`
- `analysis::events::tests::revision_mapping_follows_structured_claim_order_for_multi_fact_updates`
  - rendered revision order was `Revised fact B`, `Revised fact A` instead of structured extraction order `Revised fact A`, `Revised fact B`

## GREEN Evidence

Command:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture
```

Result:

- `test result: ok. 66 passed; 0 failed; 0 ignored; 0 measured; 255 filtered out; finished in 8.46s`

Additional required verification:

```bash
cargo fmt --all -- --check
git diff --check
```

Both commands exited successfully.

## Tests Run

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
- `cargo fmt --all -- --check`
- `git diff --check`

Not run:

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test storage::event_repository -- --nocapture`
  - `src/storage/event_repository.rs` was not changed

## Commit

- Commit hash: `2087f01`

## Concerns

- None.
