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
