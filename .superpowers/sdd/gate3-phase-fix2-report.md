# Gate 3 Phase Fix 2 Report

## Findings fixed

- Persisted mention-cluster links now drive endpoint and repository readback.
  - `EventRepository::find_latest_cluster_for_evidence` now resolves cluster membership through `market_event_mentions` and then reads the latest version for the linked cluster id.
  - Added repository and API regressions where the requested evidence is neither `primary_evidence_id` nor in `representative_ids`, but is linked only by `market_event_mentions`, and persisted delta/hypothesis/market-observation rows are still returned.
- Market observation persistence now uses real point-in-time market inputs and never writes placeholder `not_observed` rows for missing data.
  - Added point-in-time stock-bar and sector-version reads in `MarketRepository`.
  - `run_event_market_observation_job` now loads adjusted stock return, market index return, and industry return from stored PIT data before calling `observe_market_alignment`.
  - If eligible market inputs are missing, the scheduler logs the skip and inserts nothing, so future runs are not blocked by a placeholder primary-key row.
  - Added scheduler regressions proving real seeded PIT inputs persist a `market_aligned` row and missing inputs persist no row.
- Scheduled event ingestion now includes GDELT when configured and keeps sources isolated.
  - `run_event_ingestion_job` now wires both `OfficialEventSource::from_config` and `GdeltEventSource::from_config` into a shared multi-source ingestion path.
  - Each source uses its own cursor key and independent fetch/ingest/cursor-update flow.
  - Per-source config failures are logged and skipped so another valid configured source still ingests.
  - Added scheduler coverage proving GDELT ingestion actually persists rows, preserves adapter `sourceRole` metadata, and preserves the GDELT cursor on item failure while the official cursor still advances.
- Preserved Gate 3 constraints.
  - No event score/ranking integration was added.
  - No market causality claims were added.
  - No indirect beneficiary stock lists were added.
  - No placeholder/fallback bypass behavior was added.

## Files changed

- `src/api/event_routes.rs`
- `src/scheduler/mod.rs`
- `src/storage/event_repository.rs`
- `src/storage/market_repository.rs`

## Commit hash(es)

- `d874e37` - `fix: complete gate3 event ingestion and observation wiring`

## Commands run

1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test storage::event_repository -- --nocapture`
   - PASS
   - Summary: `26 passed; 0 failed; 369 filtered out`
2. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test api::event_routes -- --nocapture`
   - PASS
   - Summary: `18 passed; 0 failed; 377 filtered out`
3. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test scheduler -- --nocapture`
   - PASS
   - Summary: `20 passed; 0 failed; 375 filtered out`
4. `cargo fmt --all -- --check`
   - FAIL on first run
   - Summary: rustfmt reported formatting diffs in `src/api/event_routes.rs` and `src/scheduler/mod.rs`
5. `cargo fmt --all`
   - PASS
   - Summary: applied rustfmt to the touched files
6. `cargo fmt --all -- --check`
   - PASS
   - Summary: no formatting diffs
7. `git diff --check`
   - PASS
   - Summary: no whitespace or conflict-marker issues

## Concerns

- Focused verification still emits pre-existing unused-code/dead-code warnings in unrelated modules and the existing future-incompatibility notices for `redis v0.25.4` and `sqlx-postgres v0.7.4`. These were not introduced by this fix.
