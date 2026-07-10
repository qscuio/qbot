## Summary of code changes

- Added `src/analysis/events/dedup.rs` with a conservative duplicate decision module that:
  - returns `DuplicateDecision::Exact` for same source ID + source item ID + version, canonical URL matches, or content-hash matches
  - computes near-duplicate confidence from title-token Jaccard plus normalized content-prefix similarity
  - enforces the conservative floor of `0.92` for automatic near-duplicate decisions
  - returns `ReviewRequired` when a lower configured threshold would otherwise cause an automatic near-duplicate match
- Exported only `DuplicateDecision` from `src/analysis/events/mod.rs`.
- Promoted `canonicalize_source_url` in `src/analysis/events/evidence.rs` to `pub(crate)` so dedup can reuse the existing URL normalization logic.
- Added dedup tests for exact matches, conservative near-duplicate behavior, independent classification, and locked `independent`/duplicate relations through `EventRepository::save_duplicate_group`.

## Red test evidence

- Command:
  - `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::dedup -- --nocapture`
- Result before implementation:
  - `3 passed; 5 failed`
- Failing tests:
  - `analysis::events::dedup::tests::returns_exact_for_matching_canonical_url`
  - `analysis::events::dedup::tests::returns_exact_for_matching_content_hash`
  - `analysis::events::dedup::tests::returns_exact_for_matching_source_item_and_version`
  - `analysis::events::dedup::tests::returns_near_duplicate_when_similarity_meets_conservative_threshold`
  - `analysis::events::dedup::tests::returns_review_required_when_lower_threshold_would_drive_auto_match`
- Representative failure:
  - expected `Exact` / `NearDuplicate` / `ReviewRequired`, got `Independent` from the stub decider

## Green verification evidence

- `cargo fmt --all -- --check`
  - passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::dedup -- --nocapture`
  - passed, `8 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
  - passed, `25 passed; 0 failed`
- `cargo test --all --locked config::tests::test_config_defaults`
  - passed, `1 passed; 0 failed`
- `git diff --check`
  - passed

## Commit hash

- `f8484400deff82aec4e229d0a3f31837be2d8c78`

## Any concerns

- The required verification set passed.
- The branch still emits pre-existing dead-code/unused warnings in the event module area because this feature adds a newly exported public type before downstream consumers use it.

## Fix follow-up: review findings addressed on 2026-07-10

### Review issues and changes

- Issue 1: automatic `NearDuplicate` now requires the configured threshold itself to be at least `0.92`.
  - Updated `DuplicateDecider::decide` in `src/analysis/events/dedup.rs` so thresholds below `0.92` always return `ReviewRequired` for candidates at or above the configured threshold, including scores above `0.92`.
- Issue 2: exact URL matching now requires canonical parse success on both sides.
  - Removed the invalid-URL lowercase fallback from `same_canonical_url` in `src/analysis/events/dedup.rs`; invalid URL text no longer produces `Exact`.
- Issue 3: locked relation coverage now uses the real deterministic duplicate-group identity path.
  - Reworked the locked `independent` and locked duplicate tests in `src/analysis/events/dedup.rs` to create the duplicate group through `EventRepository::insert_manual_evidence`, then lock it, then prove a later reprocessing-style duplicate insert cannot overwrite user-locked relation metadata or members.
- Issue 4: Task 4 warning noise cleanup.
  - Removed the unused `DuplicateDecision` re-export from `src/analysis/events/mod.rs`.
  - Removed the now-dead dedup test helper.
  - Wired the dedup decider into `ManualEvidenceIngestor` representative selection in `src/analysis/events/evidence.rs` so the production duplicate path consumes the Task 4 logic without changing the public manual-submission behavior.

### Red test evidence

- Command:
  - `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::dedup -- --nocapture`
- Result before the fix:
  - `7 passed; 2 failed`
- Failing tests:
  - `analysis::events::dedup::tests::does_not_treat_matching_invalid_url_text_as_exact`
    - actual: `Exact { ... }`
    - expected: `Independent`
  - `analysis::events::dedup::tests::returns_review_required_when_lower_threshold_would_otherwise_auto_match_above_floor`
    - actual: `NearDuplicate { confidence: 0.9375, ... }`
    - expected: `ReviewRequired { ... }`

### Green verification

- `cargo fmt --all -- --check`
  - passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::dedup -- --nocapture`
  - passed, `9 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
  - passed, `26 passed; 0 failed`
- `cargo test --all --locked config::tests::test_config_defaults`
  - passed, `1 passed; 0 failed`
- `git diff --check`
  - passed
- Warning-focused verification:
  - `cargo check --tests --locked`
  - passed
  - Task 4-specific warning noise is gone on the test-target verification surface; the removed unused `dedup::DuplicateDecision` export and dead dedup helper no longer warn. Pre-existing event-module dead-code warnings remain outside Task 4.

### Commit hash

- `51f7e9f10ec7d9642ae9d39c54a72a153da9f8e4`
