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

- `4482e96dadad804476d5e262c1b8e5d39466c760`

## Fix follow-up: live near-duplicate ingestion wired on 2026-07-10

### Review issue and what changed

- Remaining issue: the live manual-ingestion path only queried `content_hash` matches inside `EventRepository::insert_manual_evidence`, then persisted an `"exact"` duplicate group before `DuplicateDecider` ran. That made `NearDuplicate` and `ReviewRequired` unreachable in production ingestion.
- Changed `EventRepository::insert_manual_evidence` to return a broader, deterministic candidate set scoped by `effective_trade_date`, while preserving the existing content-hash advisory lock used by the concurrent manual duplicate path.
- Moved duplicate-group persistence behind `DuplicateDecision` in `ManualEvidenceIngestor`. The decision now drives whether a group is saved, its relation type, confidence, representative, members, and whether the public result is `Inserted` or `Existing`.
- Expanded `DuplicateDecision` in `src/analysis/events/dedup.rs` so exact/near/review decisions carry the candidate IDs needed to persist the chosen duplicate representation deterministically.
- Added the missing live-path regression test for a non-hash near duplicate and a repository-boundary test proving broader candidate discovery.

### Red test evidence

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test manual_insert_surfaces_same_trade_date_near_duplicate_candidates -- --nocapture`
  - failed
  - `assertion left == right failed`
  - left: `[]`
  - right: `[existing evidence id]`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test near_duplicate_manual_submission_reaches_live_ingest_path -- --nocapture`
  - failed
  - `expected duplicate relation, got inserted <submitted evidence id>`

### Green verification

- `cargo fmt --all -- --check`
  - passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::dedup -- --nocapture`
  - passed, `9 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
  - passed, `27 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test storage::event_repository -- --nocapture`
  - passed, `12 passed; 0 failed`
- `cargo test --all --locked config::tests::test_config_defaults`
  - passed, `1 passed; 0 failed`
- `git diff --check`
  - passed
- `cargo check --tests --locked`
  - passed
  - remaining warnings are pre-existing dead-code/unused warnings outside this Task 4 fix

### Commit hash

- `387bae5ce8aea840c9bf43dd3d0b52c5bc693bd6`

## Fix follow-up: final rereview issues addressed on 2026-07-10

### Review issues and changes

- Issue 1: exact duplicate discovery now spans the required global scope.
  - `src/storage/event_repository.rs` now discovers manual duplicate candidates from the union of:
    - same `effective_trade_date` rows for near-duplicate review
    - global exact-match rows by `content_hash`, canonical `source_url`, and `source_id` + `source_item_id` + `version`
  - candidate expansion also pulls the current representative row for any matched duplicate-group member so live ingestion can resolve to the right existing representative.
- Issue 2: ingestion now appends into unlocked duplicate groups instead of replacing members.
  - kept `save_duplicate_group` replacement semantics unchanged for the explicit repository tests
  - added a separate ingestion path, `append_duplicate_group`, that merges new members into the existing unlocked group, preserves older members, and retains the representative correctly.
- Issue 3: `ReviewRequired` is now driven by an injectable ingestion threshold instead of a single hardcoded live-path value.
  - `ManualEvidenceIngestor` now accepts an injected auto-near-duplicate threshold through `with_auto_near_duplicate_threshold`
  - the live-path test proves a threshold below `0.92` persists a `review_required` duplicate group while still returning the existing duplicate relation.
- Issue 4: manual-ingestion locking now serializes the same scope used for near-duplicate discovery.
  - replaced the content-hash-only advisory lock with a deterministic transaction-scoped advisory lock keyed by the manual duplicate discovery scope (`source_tier` + `effective_trade_date`)
  - added a deterministic live-path concurrency test with an insertion delay after candidate discovery so different-hash near duplicates cannot both return inserted.
- Issue 5: `DuplicateDecision` is back to the briefed public shape.
  - restored the public enum variants to:
    - `Exact { representative_id: Uuid }`
    - `NearDuplicate { representative_id: Uuid, confidence: f64 }`
    - `Independent`
    - `ReviewRequired { candidate_ids: Vec<Uuid> }`
  - moved persistence metadata into the private/internal `DuplicateResolution` + `DuplicateMatch` helpers used only inside Task 4 ingestion flow.

### Red test evidence

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test manual_insert_surfaces_cross_trade_date_exact_duplicate_candidates -- --nocapture`
  - failed before the fix
  - `left: []`
  - `right: [existing evidence id]`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test exact_duplicate_manual_submission_detects_matching_content_hash_across_trade_dates -- --nocapture`
  - failed before the fix
  - `expected duplicate relation, got inserted <submitted evidence id>`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test appending_duplicate_through_ingestion_preserves_older_unlocked_group_members -- --nocapture`
  - failed before the fix
  - `expected duplicate relation, got inserted <submitted evidence id>`
- `cargo test duplicate_decision_public_shape_matches_task_brief -- --nocapture`
  - failed before the fix
  - assertion showed `DuplicateDecision::Exact` still exposed `candidate_ids`

### Green verification

- `cargo fmt --all -- --check`
  - passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::dedup -- --nocapture`
  - passed, `10 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
  - passed, `32 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test storage::event_repository -- --nocapture`
  - passed, `13 passed; 0 failed`
- `cargo test --all --locked config::tests::test_config_defaults`
  - passed, `1 passed; 0 failed`
- `git diff --check`
  - passed
- `cargo check --tests --locked`
  - passed
  - remaining warnings are pre-existing unused/dead-code warnings outside Task 4's touched surface

### Commit hash

- `acabc7a2c5bdb0facf3565ec6244492be4069e0b`

## Fix follow-up: canonical URL candidate discovery rereview addressed on 2026-07-10

### Review issues and changes

- Issue 1: global canonical-URL exact-duplicate discovery now runs under a repository-owned canonical URL invariant.
  - `src/storage/event_repository.rs` now canonicalizes `source_url` on every evidence insert and on the manual duplicate lookup input before candidate discovery runs.
  - the repository lookup still uses exact equality in SQL, but it now operates on canonical URL values at the storage boundary instead of raw caller-provided strings.
  - `src/analysis/events/evidence.rs` now delegates its manual-ingestion URL canonicalization to the shared repository helper so the live path and storage path cannot drift.
  - added a repository regression proving cross-trade exact duplicate candidate discovery works when the stored URL and submitted URL differ syntactically but canonicalize to the same value.
  - added a live manual-ingestion regression proving canonical-URL exact duplicates still resolve to `Existing` across trade dates in that same scenario.
- Issue 2: the brittle `duplicate_decision_public_shape_matches_task_brief` source parser was replaced with a type-level shape assertion.
  - `src/analysis/events/dedup.rs` no longer parses Rust source with a nonexistent `impl DuplicateDecision` delimiter.
  - the test now constructs and destructures each public variant directly, so extra/missing fields fail at compile time instead of relying on string parsing boundaries.

### Red test evidence

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test manual_insert_surfaces_cross_trade_date_canonical_url_exact_duplicate_candidates -- --nocapture`
  - failed before the fix
  - `left: []`
  - `right: [existing evidence id]`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test exact_duplicate_manual_submission_detects_matching_canonical_url_across_trade_dates -- --nocapture`
  - failed before the fix
  - `expected duplicate relation, got inserted <submitted evidence id>`
- `cargo test duplicate_decision_public_shape_matches_task_brief -- --nocapture`
  - no red behavior failure was applicable
  - the public enum shape already matched the brief; the defect was that the old test parsed past a nonexistent source boundary and could silently keep passing for the wrong reason

### Green verification

- `cargo fmt --all -- --check`
  - passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::dedup -- --nocapture`
  - passed, `10 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
  - passed, `33 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test storage::event_repository -- --nocapture`
  - passed, `14 passed; 0 failed`
- `cargo test --all --locked config::tests::test_config_defaults`
  - passed, `1 passed; 0 failed`
- `git diff --check`
  - passed
- `cargo check --tests --locked`
  - passed
  - remaining warnings are the same pre-existing unused/dead-code warnings outside this Task 4 touch surface

### Commit hash

- `e48af44a3d3b2485f65e94ef4748c7c348534a37`

## Fix follow-up: atomic duplicate-group persistence rereview addressed on 2026-07-10

### Review issue and what changed

- Issue: `EventRepository::insert_manual_evidence()` committed the inserted evidence row before duplicate classification and duplicate-group append finished in `ManualEvidenceIngestor::submit_at()`. That let a concurrent near-duplicate submission discover a fresh evidence row before its duplicate-group membership existed, derive a different representative, and split the duplicate set across multiple groups.
- `src/storage/event_repository.rs` now owns a new transaction-scoped manual-ingestion callback path that:
  - canonicalizes the submitted row
  - acquires the manual duplicate discovery advisory lock
  - discovers duplicate candidates
  - hands analysis an owned candidate context
  - inserts the submitted row
  - persists the optional duplicate group with `append_duplicate_group_in_tx()`
  - commits only after both the evidence row and duplicate-group membership are durable
- `src/analysis/events/evidence.rs` now classifies duplicates and constructs the optional `DuplicateGroupRow` inside that repository callback through `build_manual_submission_effect()`, preserving the analysis/storage separation while moving persistence into the locked transaction scope.
- Added a deterministic regression proving concurrent different-hash near-duplicate submissions around an existing base event collapse into exactly one duplicate group with one representative, even when the first submission is deliberately paused at the old stale-state boundary.

### Red test evidence

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test concurrent_different_hash_near_duplicates_share_one_duplicate_group_and_representative -- --nocapture`
  - failed before the fix
  - membership counts showed split group attachment for the same duplicate set:
    - left: one submitted row in `1` group, base representative in `2` groups, first concurrent submission in `2` groups
    - right: expected all three evidence rows to belong to exactly `1` duplicate group

### Green verification

- `cargo fmt --all -- --check`
  - passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::dedup -- --nocapture`
  - passed, `10 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
  - passed, `34 passed; 0 failed`
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test storage::event_repository -- --nocapture`
  - passed, `14 passed; 0 failed`
- `cargo test --all --locked config::tests::test_config_defaults`
  - passed, `1 passed; 0 failed`
- `git diff --check`
  - passed
- `cargo check --tests --locked`
  - passed
  - remaining warnings are the same pre-existing unused/dead-code warnings outside this Task 4 fix surface

### Commit hash

- `PENDING-AMEND`
