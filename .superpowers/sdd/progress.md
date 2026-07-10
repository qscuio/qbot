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
