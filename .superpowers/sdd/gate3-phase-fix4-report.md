# Gate 3 Phase Fix 4 Report

## Findings fixed

- Added structured direct observation entities to frozen impact hypotheses so production-generated sanitized hypothesis payloads preserve direct company observation targets without reintroducing stock-code labels or indirect beneficiary lists.
- Updated scheduled market observation entity discovery to read structured hypothesis metadata instead of scraping stock codes from node labels.
- Enforced supplementary-only publication behavior for company fact auto-publication. Evidence marked by `source_tier = supplement`, `sourceRole = macro_supplement`, or `companyFactEligible = false` now keeps company fact claims in `draft` and out of daily fact briefs unless supported by non-supplementary evidence.
- Added coverage proving:
  - sanitized generated hypotheses still yield direct observation entities and persist market observations when PIT inputs exist;
  - supplementary/GDELT-style evidence does not auto-publish company facts or daily-brief facts by itself;
  - official evidence still auto-publishes company facts as before.
- Preserved Gate 3 constraints:
  - no event score or ranking integration;
  - no market causality claims;
  - no indirect beneficiary stock lists;
  - GDELT remains supplementary/idempotent and scheduled ingestion stays wired.

## Files changed

- `src/analysis/events/hypotheses.rs`
- `src/analysis/events/mod.rs`
- `src/scheduler/mod.rs`

## Commit hash(es)

- `35cf798` - `fix: enforce gate3 observation and supplement rules`

## Commands run

### Targeted red/green loop

1. `cargo test structured_direct_observation_entities_survive_label_sanitization -- --nocapture`
   - FAIL before fix: `direct_observation_entities` was `null`
   - PASS after fix: `1 passed; 0 failed`
2. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test supplementary_company_fact_extraction_stays_draft_and_out_of_daily_facts -- --nocapture`
   - FAIL before fix: stored claim review status remained `published`
   - PASS after fix: `1 passed; 0 failed`
3. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test event_market_observation_job_reads_structured_entities_from_generated_hypotheses -- --nocapture`
   - FAIL before fix: `RowNotFound` because no observation row was persisted
   - PASS after fix: `1 passed; 0 failed`
4. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test official_company_fact_extraction_still_publishes_daily_facts -- --nocapture`
   - PASS: `1 passed; 0 failed`

### Focused verification on final formatted tree

1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
   - PASS
   - Final summary: `120 passed; 0 failed; 282 filtered out`
2. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test scheduler -- --nocapture`
   - PASS
   - Final summary: `24 passed; 0 failed; 378 filtered out`
3. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test api::event_routes -- --nocapture`
   - PASS
   - Final summary: `18 passed; 0 failed; 384 filtered out`
4. `cargo fmt --all -- --check`
   - FAIL on first run
   - Summary: rustfmt required formatting changes in the touched files
5. `cargo fmt --all`
   - PASS
   - Summary: applied rustfmt to the touched files
6. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
   - PASS
   - Final summary: `120 passed; 0 failed; 282 filtered out`
7. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test scheduler -- --nocapture`
   - PASS
   - Final summary: `24 passed; 0 failed; 378 filtered out`
8. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test api::event_routes -- --nocapture`
   - PASS
   - Final summary: `18 passed; 0 failed; 384 filtered out`
9. `cargo fmt --all -- --check`
   - PASS
10. `git diff --check`
   - PASS

## Concerns

- Focused verification still emits pre-existing unused-code warnings in unrelated modules plus the existing future-incompatibility notices for `redis v0.25.4` and `sqlx-postgres v0.7.4`. These were not introduced by this fix.
