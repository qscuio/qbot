# SDD Progress

Task 1: complete (commits 811e16c..47a54d7, review clean; verification: point_in_time_tables_exist passed, fmt check passed, diff check passed, config test and DB suite passed separately)
Task 2: complete (commits 0811f59..748271c, review clean; verification: focused contract test passed, fmt check passed, diff check passed)
Task 3: complete (commits 49bf36a..51e094a, review clean after coverage fix; verification: storage::market_repository 10/10 passed, fmt check passed, diff check passed)
Task 4: complete (commits 84db741..e08c087, review clean after fixes; verification: data::tushare 10/10 passed, storage::market_repository 11/11 passed, config default test passed, fmt check passed, diff check passed)
Task 5: complete (commits d98d265..3189712, review clean after atomicity fix; verification: focused tests passed, storage::market_repository 14/14 passed, full suite 115 passed with config test split, fmt check passed, diff check passed)
Task 6: complete (commits 88e1d26..3793d2b, review clean after backfill auditability fix; verification: ingestion 10/10 passed, storage::market_repository 14/14 passed, fmt check passed, diff check passed)
Task 7: complete (commits 6c9275e..4d7ee59, review clean after coverage and duplicate-factor determinism fixes; verification: adjustment::tests 4/4 passed, fmt check passed, diff check passed)
Task 8: complete (commits f77fa4f..b673980, review clean after PIT rebuild/upsert, provenance, completeness, tie-break, and null-field fixes; verification: analysis::market_snapshot 24/24 passed with DATABASE_URL, security_statuses_as_of tie test 1/1 passed, scheduler::tests 2/2 passed, fmt check passed, diff check passed)
Task 9: complete (commits 05a965e..98a68f3, review clean after data-status completeness, missing-probe, and repository-boundary fixes; verification: api::analysis_routes::tests 6/6 passed, repository boundary tests 2/2 passed, scheduler::tests 3/3 passed, full suite split 148/148 + config 1/1 passed, fmt check passed, diff check passed)

Final branch review: complete (commits 811e16c..f090cad, final review clean after legacy-backfill, reference-refresh, schedule, latestRuns tradeDate, and docs alignment fixes; verification: stock_history 1/1 passed, ingestion 11/11 passed, api::analysis_routes 6/6 passed, scheduler 3/3 passed, full suite split 150/150 + config 1/1 passed, fmt check passed, diff check passed)

## Final Review Fix

### Fix summary
- Critical 1: removed PIT version writes from legacy `StockHistoryService::backfill_range`; legacy/full backfill now only upserts current-state `stock_daily_bars`, while daily incremental `update_today` still records observed PIT versions.
- Important 1: `refresh_reference_data_inner` no longer returns before supported reference categories persist; unsupported critical capabilities now record readiness failure plus category-specific missing capability results, while supported security master and corporate actions still persist.
- Important 2: moved `POINT_IN_TIME_REFERENCE_JOB_CRON` to a pre-snapshot Friday slot (`0 15 17 * * Fri`) and registered it before snapshot generation. This changed the Task 9 hard-coded cron string because the later Phase Completion Checklist and final review required the weekly reference refresh itself to run before Friday snapshot generation.
- Minor 1: added `tradeDate` to `analysis_run_summary_json`, including `latestRuns[*].tradeDate` with `null` for run types without a trade date.

### Red evidence / explanation
- Critical 1: legacy backfill previously called `MarketRepository::append_daily_bar_versions_in_tx(..., Utc::now(), "estimated", ...)` inside `src/services/stock_history.rs`, so first-run startup `backfill_full()` polluted PIT history outside explicit audited PIT backfill.
- Important 1: `refresh_reference_data_inner` previously returned immediately after adding a failed readiness category when either `security_master_history` or `historical_sector_membership` was unsupported, so supported security master and corporate action categories never persisted under the current provider.
- Important 2: scheduler tests and constants showed the weekly PIT reference refresh at `20:30 Fri`, after the Friday `17:20` snapshot job, contradicting the phase checklist ordering.
- Minor 1: `analysis_run_summary_json` omitted `tradeDate`, so `latestRuns` entries dropped that field even when the underlying `AnalysisRunSummary` had it.

### Green / verification
- `cargo fmt --all -- --check` -> passed
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked stock_history -- --nocapture` -> passed (`1 passed`)
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked analysis::market_snapshot::ingestion::tests -- --nocapture` -> passed (`11 passed`)
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked api::analysis_routes::tests -- --nocapture` -> passed (`6 passed`)
- `cargo test --locked scheduler::tests -- --nocapture` -> passed (`3 passed`)
- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --all --locked -- --skip config::tests::test_config_defaults` -> passed (`150 passed; 1 filtered out`)
- `cargo test --all --locked config::tests::test_config_defaults` -> passed (`1 passed`)

### Files changed
- `src/services/stock_history.rs`
- `src/analysis/market_snapshot/ingestion.rs`
- `src/scheduler/mod.rs`
- `src/api/analysis_routes.rs`

## Gate 1: Strong-Stock Pattern Shadow Engine

Gate 1 Task 1: complete (commits 34dd225..2f55ba1, review clean after publication-contract and duplicate-batch coverage fixes; verification: storage::pattern_repository 10/10 passed, fmt check passed, diff check passed)
Gate 1 Task 2: complete (commits 26089ac..05734ab, review clean after CLI scaffold fix; verification: research pytest 8/8 passed, ruff passed, mypy passed, installed qbot-research train-all scaffold command booted, diff check passed)
Gate 1 Task 3: complete (commits ede9235..b66e2f9, review clean after public-path, fingerprint, schema, and publish-horizon fixes; verification: research datasets pytest 14/14 passed, ruff passed, mypy passed, diff check passed)
Gate 1 Task 4: complete (commits 87ded34..5bad209, review clean after market-cap, bucket-coverage, month-horizon, benchmark-data, and tradable-state fixes; verification: research labels/controls pytest 11/11 passed, ruff passed, mypy passed, diff check passed)
Gate 1 Task 5: complete (commits de6c577..025b143, review clean after rejection-contract and GMM covariance-unit fixes; verification: research archetypes pytest 12/12 passed, ruff passed, mypy passed, diff check passed)
Gate 1 Task 6: complete (commits 19e87d2..093e417, review clean after validation-window, no-fallback input, strict-bool, scan-ranker null, and split coverage fixes; verification: research validation/baselines pytest 14/14 passed, ruff passed, mypy passed, diff check passed)
Gate 1 Task 7: complete (commits 3b1ddbb..68783b6, review clean after as-of cutoff, UUID contract, immutable payload, JSON error, empty-plan, and CLI parameter fixes; verification: research pytest 76/76 passed, ruff passed, mypy passed, diff check passed)
Gate 1 Task 8: complete (commits c4bbcf2..e1a8d37, review clean after feature coverage, schema-version, full metric parameters, risk tiering, direct engine coverage, tier persistence, name output, and additive scoring fixes; verification: analysis::patterns 30/30 passed, storage::pattern_repository 11/11 passed with DATABASE_URL, research contract/export pytest 39/39 passed, ruff passed, mypy passed, diff check passed)
Gate 1 Task 9: complete (commits 18c603c..e6372a7, review clean after trading-table safety coverage fix; verification: scheduler pattern shadow job tests 4/4 passed with DATABASE_URL, pattern routes 4/4 passed with DATABASE_URL, analysis matcher focused tests passed, fmt check passed, diff check passed)

## Gate 2: Event Evidence MVP

Gate 2 Task 1: complete (commits 80b62a6..d2b0298, review clean after immutability, locked duplicate, published-claim evidence, duplicate-member replacement, and lookup-index fixes; verification: storage::event_repository 11/11 passed with DATABASE_URL, full suite split 211/211 + config 1/1 passed, fmt check passed, diff check passed)
Gate 2 Task 2: complete (commits ad20abe..ee01b59, review clean after public API boundary fix; verification: analysis::events::contracts 3/3 passed, analysis::events 5/5 passed, config test 1/1 passed, fmt check passed, diff check passed)
Gate 2 Task 3: complete (commits 9b65fab..e0407ea, review clean after public duplicate outcome, concurrency lock, public constructor wiring, public contract boundary, and test-barrier isolation fixes; verification: analysis::events 17/17 passed with DATABASE_URL, analysis::events::evidence 7/7 passed with DATABASE_URL, analysis::events::time 4/4 passed, config test 1/1 passed, fmt check passed, diff check passed)
Gate 2 Task 4: complete (commits 034eb0a..cbcefb8, review clean after conservative rule, live ingestion, canonical URL, atomic duplicate persistence, overlap, lock-scope, stale-barrier, and historical raw-URL fixes; verification: analysis::events::dedup 10/10 passed with DATABASE_URL, analysis::events 35/35 passed with DATABASE_URL, storage::event_repository 15/15 passed with DATABASE_URL, config 1/1 passed, fmt check passed, diff check passed; cargo output still has pre-existing warning noise outside the Task 4 surface)
Gate 2 Task 5: complete (commits ab77aec..05dffe7, review clean after source-id validation, proxy-aware client, blank proxy/env normalization, and env-test isolation fixes; verification: analysis::adapters::official_event_source 10/10 passed, config 1/1 passed, fmt check passed, diff check passed; cargo output still has pre-existing warning noise outside the Task 5 surface)
Gate 2 Task 6: complete (commits a1dec47..3eb2d11, review clean after value-based amount/date validation and exact direct stock-code validation fixes; verification: analysis::events::extraction 13/13 passed, analysis::adapters::llm_event_extractor 4/4 passed, fmt check passed, diff check passed; cargo output still has pre-existing warning noise outside the Task 6 surface)
Gate 2 Task 7: complete (commits 22948fc..2e0c043, review clean after restricting official industry linking to industry/sector entity types; verification: analysis::events::entity_linking 7/7 passed, analysis::events::claims 3/3 passed, fmt check passed, diff check passed; cargo output still has pre-existing warning noise outside the Task 7 surface)
Gate 2 Task 8: complete (commits 9990c3b..575eec0, review clean after external collected status, known/null source readability and review flags, explicit review actions, command-specific usage, atomic review persistence, latest-trade-date brief lookup, and content-only manual submission fixes; verification: api::event_routes 15/15 passed with DATABASE_URL, analysis::events 59/59 passed with DATABASE_URL, storage::event_repository 18/18 passed with DATABASE_URL, fmt check passed, diff check passed; cargo output still has pre-existing warning noise outside the Task 8 surface)
Gate 2 Task 9: complete (commits dfb9fd3..1a1075e, review clean after cursor idempotency, fact-brief push, lock isolation, source-less non-fact handling, direct-entity role filtering, and revision-order fixes; verification: analysis::events 66/66 passed with DATABASE_URL, scheduler 14/14 passed with DATABASE_URL, full suite split 320/320 + config 1/1 passed before the phase checklist fix, fmt check passed, diff check passed; forbidden-feature scan only matched the beneficiary regression fixture)
Gate 2 phase checklist gap: complete (commit a703326, review clean after enforcing fact-only ClaimGraph node types; verification: analysis::events::claims 4/4 passed with DATABASE_URL, full suite split 321/321 + config 1/1 passed, fmt check passed, diff check passed; forbidden-feature scan only matched the beneficiary regression fixture)

Gate 3 Task 1: complete (commits ada917a..7ec4dd4, review clean; verification: storage::event_repository 24/24 passed with DATABASE_URL, fmt check passed, diff check passed)
Gate 3 Task 2: complete (commits 7ec4dd4..a7fd512, review clean after locked-split and refinement-eligibility fixes; verification: analysis::events::clustering 18/18 passed with DATABASE_URL, fmt check passed, diff check passed)
Gate 3 Task 3: complete (commits a7fd512..eee07e2, review clean after real GDELT ArtList parsing fix; verification: analysis::adapters::gdelt 5/5 passed with DATABASE_URL, fmt check passed, diff check passed)
Gate 3 Task 4: complete (commits eee07e2..97d7773, review clean; verification: analysis::events::deltas 3/3 passed with DATABASE_URL, fmt check passed, diff check passed)
Gate 3 Task 5: complete (commits 97d7773..27836dd, review clean after freeze payload, EventDelta evolution, and stock-list filtering fixes; verification: analysis::events::hypotheses 19/19 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 3 Task 6: complete (commits cce5739..4f735dc, review clean; verification: analysis::events::market_observation 6/6 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 3 Task 7: complete (commits aee0b23..7b4a02a, review clean; verification: analysis::events::event_statistics 3/3 passed, fmt check passed, diff check passed)

Gate 3 Task 8: complete (commits e3bbee6..f1f5e6d, review clean; verification: reporting 5/5, scheduler 16/16, event_routes 16/16 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 3 phase review: complete (commits ada917a..df8f73c, phase review clean after persisted read path, GDELT ingestion, scheduled clustering/observation, supplementary fact autopublish, and atomic cluster+mention persistence fixes; verification: analysis::events 122/122 passed, scheduler::tests 24/24 passed, repository transactional regression passed, fmt check passed, diff check passed)

Gate 4 Task 1: complete (commits c91fc59..a4814eb, review clean after candidate insert deduplication; verification: storage::decision_support_repository 3/3 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 4 Task 2: complete (commits 31380b3..c4c09ac, review clean after decision_support re-export warning fix; verification: analysis::decision_support 2/2 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 4 Task 3: complete (commits 3aa038c..fe351b2, review clean after latest-run archive filtering fix; verification: scan_ranker_adapter 3/3 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 4 Task 4: complete (commits 058601f..e0b2169, review clean; verification: analysis::decision_support::builder 4/4 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 4 Task 5: complete (commits 26197da..6c7aa85, review clean; verification: event_adapter 4/4 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 4 Task 6: complete (commits eae4c87..0ee87b0, review clean; verification: decision_support::builder 10/10 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 4 Task 7: complete (commits 534e8ba..05a28a1, review clean after atomic persistence and duplicate-path fixes; verification: api::decision_support_routes 6/6 passed with DATABASE_URL, storage::decision_support_repository 4/4 passed with DATABASE_URL, builder atomic rollback regression 1/1 passed with DATABASE_URL, fmt check passed, diff check passed)

Gate 4 Task 8: complete (commits 87861c0..e18db95, review clean after latest-pattern-set scoping fix; verification: scheduler::tests 31/31 passed with DATABASE_URL, storage::pattern_repository 12/12 passed with DATABASE_URL, fmt check passed, diff check passed)
