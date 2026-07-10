# Gate 2 Task 5 Report: Add one official-source adapter

## What I implemented

- Added `src/analysis/adapters/mod.rs` with the new adapter surface:
  - `EventSource`
  - `ContentRetentionPolicy`
  - `FetchBatch`
  - `FetchedEvent`
- Added `src/analysis/adapters/official_event_source.rs` with the first official adapter:
  - env-driven construction from `Config`
  - explicit retention policy selection
  - local JSON feed parsing into `source_item_id`, `published_at`, `title`, `content` or permitted summary, `source_url`, and `raw_payload`
  - retention enforcement that removes full `content` from `raw_payload` when `OFFICIAL_EVENT_STORE_FULL_CONTENT=false`
  - `until` filtering
  - `fetch()` implementation using `reqwest`
- Registered the adapter module in `src/analysis/mod.rs`.
- Extended `Config` and `.env.example` with:
  - `OFFICIAL_EVENT_FEED_URL`
  - `OFFICIAL_EVENT_FEED_API_KEY`
  - `OFFICIAL_EVENT_SOURCE_ID`
  - `OFFICIAL_EVENT_STORE_FULL_CONTENT`
- Updated existing test-only `Config` initializers that broke once the new fields were added.

## TDD RED/GREEN evidence

### RED

Wrote tests first in:
- `src/analysis/adapters/official_event_source.rs`
- `src/config.rs`

Initial RED commands and results:

1. `cargo test analysis::adapters::official_event_source -- --nocapture`
2. `cargo test --all --locked config::tests::test_config_defaults -- --nocapture`

Initial RED result:
- both commands failed at compile time with `E0063`
- failure cause: existing test `Config { ... }` initializers were missing the new official event fields
- representative failures:
  - `src/api/analysis_routes.rs:587`
  - `src/api/pattern_routes.rs:506`
  - `src/scheduler/mod.rs:990`
  - `src/services/stock_history.rs:327`

This established that the new config contract was not yet implemented across the current branch state.

### GREEN

Implemented the adapter/config changes, then reran the required commands.

Passing targeted adapter test:
- `cargo test analysis::adapters::official_event_source -- --nocapture`
- result: `5 passed; 0 failed`

Passing config default test:
- `cargo test --all --locked config::tests::test_config_defaults -- --nocapture`
- result: `1 passed; 0 failed`

Additional adapter coverage included:
- full-content retention path
- summary-only retention path
- `until` cutoff filtering
- `from_config()` env-driven construction
- loopback HTTP `fetch()` test with no live network calls

## Files changed

- `.env.example`
- `src/analysis/mod.rs`
- `src/analysis/adapters/mod.rs`
- `src/analysis/adapters/official_event_source.rs`
- `src/config.rs`
- `src/api/analysis_routes.rs`
- `src/api/pattern_routes.rs`
- `src/scheduler/mod.rs`
- `src/services/stock_history.rs`

## Verification

Required verification run on the final tree:

- `cargo fmt --all -- --check` -> pass
- `cargo test analysis::adapters::official_event_source -- --nocapture` -> pass
- `cargo test --all --locked config::tests::test_config_defaults -- --nocapture` -> pass
- `git diff --check` -> pass

Observed warning noise during test runs:
- crate still emits 25 warnings in the targeted test runs, mostly pre-existing dead-code/unused-item warnings under `analysis/events`, `signals`, `storage`, and `telegram`
- cargo also reports future-incompatibility notices for `redis v0.25.4` and `sqlx-postgres v0.7.4`

## Self-review findings

- Fixed one issue found during self-review: `OfficialEventSource::from_config()` was initially leaking the env `source_id` before passing it into `new()`, which already performs the one required leak to satisfy the brief’s `fn source_id(&self) -> &'static str` contract. Final state only leaks once per adapter instance.
- Retention behavior is enforced on both the mapped `content` field and persisted `raw_payload`.
- No live network calls are made in tests; the `fetch()` path is exercised via a loopback HTTP server.

## Issues/concerns

- The brief’s combination of env-driven `official_event_source_id: String` and `EventSource::source_id() -> &'static str` requires process-lifetime ownership for the selected source id. The implementation uses a single `Box::leak` per constructed adapter instance to satisfy that contract.
- Warning noise remains in the crate, but the required verification commands passed.
