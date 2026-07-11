# Gate 3 Phase Fix 5 Report

## Findings fixed

- Enforced the Gate 3 fact auto-publication boundary at source eligibility instead of extractor entity metadata.
- Evidence with `source_tier == "supplement"`, `raw_payload.sourceRole == "macro_supplement"`, or `raw_payload.companyFactEligible == false` now keeps extracted `ClaimType::Fact` claims in `draft` even when the extractor omits entities or mis-roles them.
- Preserved official/manual/non-supplement fact auto-publication behavior.
- Added focused regressions proving supplementary/GDELT-like fact extractions with no entities or mis-rolled entities stay draft and out of daily facts.
- Preserved Gate 3 constraints:
  - no event score or ranking integration;
  - no market causality claims;
  - no indirect beneficiary stock lists.

## Files changed

- `src/analysis/events/mod.rs`
- `.superpowers/sdd/gate3-phase-fix5-report.md`

## Commit hash(es)

- `d3d8a1a` - `fix: block supplementary fact autopublish by source eligibility`

## Commands run

### Targeted red/green loop

1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test supplementary_company_fact_extraction_without_entities_stays_draft_and_out_of_daily_facts -- --nocapture`
   - FAIL before fix
   - Summary: stored fact claim review status was `published` instead of `draft`
2. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test supplementary_company_fact_extraction_with_misrolled_entities_stays_draft_and_out_of_daily_facts -- --nocapture`
   - FAIL before fix
   - Summary: stored fact claim review status was `published` instead of `draft`
3. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test supplementary_company_fact_extraction_without_entities_stays_draft_and_out_of_daily_facts -- --nocapture`
   - PASS
   - Final summary: `1 passed; 0 failed`
4. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test supplementary_company_fact_extraction_with_misrolled_entities_stays_draft_and_out_of_daily_facts -- --nocapture`
   - PASS
   - Final summary: `1 passed; 0 failed`
5. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test official_company_fact_extraction_still_publishes_daily_facts -- --nocapture`
   - PASS
   - Final summary: `1 passed; 0 failed`

### Final verification on the requested commands

1. `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events -- --nocapture`
   - PASS
   - Final summary: `122 passed; 0 failed; 282 filtered out`
2. `cargo fmt --all -- --check`
   - PASS
3. `git diff --check`
   - PASS

## Concerns

- Verification still emits pre-existing unused-code warnings in unrelated modules and the existing future-incompatibility notices for `redis v0.25.4` and `sqlx-postgres v0.7.4`. These were not introduced by this fix.
