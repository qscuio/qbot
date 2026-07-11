## Implementation summary

Implemented Gate 1 / Phase 1 Task 1 as a shadow-only schema and repository contract:

- Added migration `migrations/014_pattern_research_and_shadow.sql` with the pattern research tables from the brief:
  - `analysis_dataset_manifests`
  - `analysis_training_runs`
  - `analysis_pattern_versions`
  - `analysis_pattern_sets`
  - `analysis_pattern_set_members`
  - `analysis_pattern_examples`
  - `analysis_shadow_candidates`
  - plus `idx_pattern_versions_status` and `idx_shadow_candidates_date`
- Added `src/storage/pattern_repository.rs` with:
  - `PatternVersionRow`
  - `PatternSetRow`
  - `ShadowCandidateRow`
  - `PatternRepository`
  - `list_published_patterns`
  - `upsert_shadow_candidates`
  - `latest_published_set`
  - `list_shadow_candidates`
- Updated `src/storage/mod.rs` to export `pattern_repository`.
- Kept the scope shadow-only. No writes were added to `signal_strategy_candidates` or any trading table.

## TDD RED and GREEN evidence

### RED

Added the SQLx tests first in `src/storage/pattern_repository.rs` and ran:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture
```

Observed expected failures before implementation:

- `draft_model_insertion_round_trips`: failed with `relation "analysis_dataset_manifests" does not exist`
- `published_model_query_returns_only_published_members_from_published_set`: failed with `relation "analysis_dataset_manifests" does not exist`
- `pattern_set_rejects_duplicate_members_and_member_order`: failed with `relation "analysis_dataset_manifests" does not exist`
- `duplicate_shadow_candidate_upserts_are_deterministic`: failed with `relation "analysis_dataset_manifests" does not exist`
- `latest_published_set_returns_most_recent_publication`: failed with `relation "analysis_pattern_sets" does not exist`

This confirmed the new schema/repository contract was absent and the tests were exercising the intended missing functionality.

### GREEN

After adding migration `014` and implementing the repository, reran the focused target:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture
```

Result:

- `5 passed`
- `0 failed`

Covered cases:

- draft model insertion
- published-only member query from a published set
- duplicate member rejection
- duplicate member-order rejection
- deterministic duplicate shadow candidate upsert
- latest published set selection

## Commands run and outcomes

1. Initial RED run:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture
```

Outcome: failed as expected because the new tables did not exist yet.

2. First GREEN attempt after implementation:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture
```

Outcome: compile failure due to SQLx tuple `FromRow` limit on the 17-column published-pattern query.

3. Fixed the query by switching `list_published_patterns` to explicit row mapping.

4. Focused repository tests after fix:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture
```

Outcome: all 5 tests passed.

5. Formatting check:

```bash
cargo fmt --all -- --check
```

Outcome: initially failed on formatting in `src/storage/pattern_repository.rs`.

6. Applied formatting:

```bash
cargo fmt --all
```

Outcome: succeeded.

7. Required formatting re-check:

```bash
cargo fmt --all -- --check
```

Outcome: succeeded.

8. Required focused repository test re-run:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture
```

Outcome: succeeded, `5 passed; 0 failed`.

9. Required diff hygiene check:

```bash
git diff --check
```

Outcome: succeeded.

10. Broader suite:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --all --locked -- --skip config::tests::test_config_defaults
```

Outcome: succeeded, `155 passed; 0 failed`.

11. Split config-defaults test:

```bash
cargo test --all --locked config::tests::test_config_defaults
```

Outcome: succeeded, `1 passed; 0 failed`.

## Files changed

- `migrations/014_pattern_research_and_shadow.sql`
- `src/storage/pattern_repository.rs`
- `src/storage/mod.rs`
- `.superpowers/sdd/gate1-task-1-report.md`

## Self-review findings

- Repository behavior matches the brief and remains shadow-only.
- `list_published_patterns` correctly excludes:
  - draft members inside a published set
  - published members inside a draft set
- `analysis_pattern_set_members` uniqueness constraints are enforced by the schema and covered by tests.
- `upsert_shadow_candidates` is implemented as deterministic sequential upserts in a transaction, which avoids same-statement duplicate-conflict ambiguity and preserves last-write-wins behavior.
- Numeric shadow scores are stored as `NUMERIC` in PostgreSQL and surfaced as `f64` in the repository via explicit `::float8` casts for reads.

## Concerns

No task-specific concerns.

The broader test runs emitted pre-existing compiler warnings in unrelated modules, but they did not block this task and no new warnings specific to this change remained after implementation.

## Review fix

### What changed

- Added database publication-contract checks in `014`:
  - published pattern versions now require `horizon IN ('week', 'month')`
  - published pattern versions now require non-null `approved_by`
  - published pattern versions now require non-null `published_at`
  - published pattern sets now require non-null `published_at`
- Added relational enforcement for shadow candidates:
  - `(pattern_version_id, horizon, pattern_type)` must match a real pattern version
  - `(pattern_set_id, pattern_version_id)` must match real set membership
- Tightened repository reads:
  - `list_published_patterns` only returns published week/month pattern versions with approval metadata from a published set with `published_at`
  - `latest_published_set` ignores published-set rows missing `published_at`
- Tightened repository writes:
  - `upsert_shadow_candidates` now inserts through published set membership joins and returns `AppError::Internal` when a row does not match a published week/month pattern in a published set
- Added negative SQLx tests for forbidden publication states, missing approval metadata, reader filtering, and inconsistent shadow candidate metadata/set membership.

### RED evidence for the new tests

Command run before the fix:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture
```

Observed failures before implementation:

- `published_pattern_versions_reject_forbidden_horizons_and_manual_metadata`
  - failed at `assert!(forbidden_quarter.is_err())`
- `published_pattern_sets_require_published_at`
  - failed at `assert!(result.is_err())`
- `published_readers_ignore_rows_that_break_manual_publish_invariants`
  - failed with `left: 2 right: 1`, showing invalid published rows were surfaced
- `upsert_shadow_candidates_rejects_inconsistent_metadata_and_set_membership`
  - failed because `upsert_shadow_candidates` returned `Ok(1)` for invalid input instead of an error

### GREEN evidence with commands/results

1. Focused repository tests after the fix:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture
```

Result: `9 passed; 0 failed`

2. Required formatting check:

```bash
cargo fmt --all -- --check
```

Result: passed

3. Required diff hygiene check:

```bash
git diff --check
```

Result: passed

### Files changed

- `migrations/014_pattern_research_and_shadow.sql`
- `src/storage/pattern_repository.rs`

## Second review fix

### What changed

- Added `duplicate_shadow_candidate_upserts_within_one_batch_are_deterministic` in `src/storage/pattern_repository.rs`.
- The new SQLx test exercises two duplicate shadow candidate rows in a single `upsert_shadow_candidates(&[...])` call and asserts that the second row wins deterministically.
- No production code changed. The existing sequential transactional upsert already had the desired last-write-wins behavior, and the new test passed immediately.

### Verification

- `cargo fmt --all -- --check` - passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::pattern_repository -- --nocapture` - passed
- `git diff --check` - passed
- `.superpowers/sdd/gate1-task-1-report.md`
