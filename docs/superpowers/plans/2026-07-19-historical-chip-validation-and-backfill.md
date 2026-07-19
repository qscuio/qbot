# Historical Chip Validation and Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate QBot's chip estimator against official samples, select the canonical source from objective gates, backfill every available trading day, update at 18:00 Beijing time, and expose historical chips in the stock sidebar.

**Architecture:** A pure stateful chip model transforms chronological bars into daily distributions. A separate validation module compares local snapshots with Tushare samples and records one source decision per model version. The repair workflow then writes canonical snapshots from the chosen source; the dashboard reads only canonical PostgreSQL rows.

**Tech Stack:** Rust, Reqwest/Tushare `cyq_perf` and `cyq_chips`, SQLx/PostgreSQL, Axum, ES modules, Lightweight Charts, Node test runner, Playwright.

## Global Constraints

- Treat all chip data as inferred market-cost distributions, never registered holdings.
- Label every response `qbot_estimate` or `tushare`; include the estimator version for estimates.
- Benchmark about 200 stratified stocks, with at least 24 performance dates per stock and a 50-stock by 12-date full-distribution subset.
- Pass only when median average-cost and peak-price error are at most 3%, mean winner-rate error is at most 5 percentage points, P90 cost error is at most 8%, and no subgroup has material bias.
- Use QBot estimates for all history after a pass. After a failure, use official data from 2018 onward and labeled estimates before 2018.
- Backfill through checkpoints; process bounded batches; preserve dashboard responsiveness.
- Start daily work at 18:00 Beijing time and retry when upstream data is not ready.
- Do not modify Telegram code or `web/miniapp/chart`.
- Run every production change through a failing test first.

---

### Task 1: Extend chip storage for source, model state, and validation

**Files:**
- Create: `migrations/021_historical_chip_intelligence.sql`
- Create: `src/data/chip.rs`
- Modify: `src/data/mod.rs`
- Create: `src/storage/chip_repository.rs`
- Modify: `src/storage/mod.rs`
- Test: `src/storage/chip_repository.rs`

**Interfaces:**
- Produces: `ChipRepository::{upsert_snapshot, snapshot_at_or_before, latest_snapshot, save_model_state}`
- Produces: `ChipRepository::{save_validation_run, latest_validation_decision}`
- Produces: shared `ChipBucket`, `ChipSnapshot`, `ChipDayInput`, and `ChipSourceDecision` contracts

- [ ] **Step 1: Write failing SQLx tests**

```rust
repo.upsert_snapshot(&snapshot(date(2026, 7, 17), "qbot_estimate", "v2")).await?;
let resolved = repo.snapshot_at_or_before("600519.SH", date(2026, 7, 19)).await?.unwrap();
assert_eq!(resolved.trade_date, date(2026, 7, 17));
assert_eq!(resolved.source, "qbot_estimate");

repo.save_validation_run(&passing_run()).await?;
assert_eq!(repo.latest_validation_decision("v2").await?, Some(ChipSourceDecision::Estimate));
```

Also test idempotent snapshot replacement for the same canonical date and preservation of a newer model state.

- [ ] **Step 2: Verify the tests fail**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::chip_repository::tests -- --nocapture
```

- [ ] **Step 3: Define shared chip records and create the schema**

Define `ChipBucket`, `ChipSnapshot`, `ChipDayInput`, and `ChipSourceDecision` in `src/data/chip.rs`, then export the module from `src/data/mod.rs` before using those types in repository signatures.

Alter `chip_distribution` to add `source`, `model_version`, `dominant_peak_price`, `validated`, and `source_updated_at`. Keep `(code, trade_date)` as the canonical primary key.

Create `chip_model_states(code, model_version, through_date, distribution, updated_at)`. Create `chip_model_validation_runs(run_id, model_version, sample_definition, aggregate_metrics, subgroup_metrics, decision, started_at, completed_at, error_summary)`. Add indexes for validation lookup and latest stock snapshot.

- [ ] **Step 4: Implement repository methods**

Decode the existing JSON buckets into typed `ChipBucket` values. `snapshot_at_or_before` must order by `trade_date DESC LIMIT 1` and return requested and resolved dates separately at the service boundary.

- [ ] **Step 5: Run tests and commit**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::chip_repository::tests -- --nocapture
git add migrations/021_historical_chip_intelligence.sql src/data/chip.rs src/data/mod.rs src/storage/chip_repository.rs src/storage/mod.rs
git commit -m "feat: add historical chip storage"
```

### Task 2: Build the sequential full-history estimator

**Files:**
- Create: `src/services/chip_model.rs`
- Modify: `src/services/mod.rs`
- Test: `src/services/chip_model.rs`

**Interfaces:**
- Produces: `ChipModelV2::new(bucket_count: usize)`
- Produces: `ChipModelV2::restore(state)` and `update(input: ChipDayInput) -> ChipSnapshot`
- Produces: constant `CHIP_MODEL_VERSION: &str = "qbot-chip-v2"`

- [ ] **Step 1: Write failing pure unit tests**

```rust
let mut model = ChipModelV2::new(30);
let first = model.update(day("2026-07-16", 10.0, 12.0, 9.0, 11.0, 20.0, 1.0));
let second = model.update(day("2026-07-17", 11.0, 13.0, 10.0, 12.0, 10.0, 1.0));
assert!((second.distribution.iter().map(|b| b.weight).sum::<f64>() - 1.0).abs() < 1e-9);
assert!(second.average_cost > first.average_cost);
assert!(second.winner_rate >= 0.0 && second.winner_rate <= 100.0);
```

Add tests for zero turnover, 100% turnover, a 2:1 adjustment-factor change, adaptive grid rebasing, deterministic restore/resume, dominant peak, and a history longer than 120 days retaining old mass.

- [ ] **Step 2: Verify the tests fail**

```bash
cargo test --locked services::chip_model::tests -- --nocapture
```

- [ ] **Step 3: Implement the model**

Maintain normalized probability mass over 30 price buckets. Before each day, rebase prices by the adjustment-factor ratio. Decay existing mass by `1 - turnover_rate`, distribute replacement mass across `[low, high]` with a triangular density centered on the day's weighted typical price, normalize, and compute average cost, winner rate, concentration, and dominant peak.

```rust
let retained = (1.0 - input.turnover_rate / 100.0).clamp(0.0, 1.0);
for bucket in &mut self.buckets { bucket.weight *= retained; }
self.allocate_triangular(1.0 - retained, input.low, input.high, input.typical_price());
self.normalize();
```

- [ ] **Step 4: Run pure tests and commit**

```bash
cargo test --locked services::chip_model::tests -- --nocapture
git add src/services/chip_model.rs src/services/mod.rs
git commit -m "feat: add full-history chip estimator"
```

### Task 3: Add official chip provider contracts and parsing

**Files:**
- Modify: `src/data/chip.rs`
- Modify: `src/data/tushare.rs`
- Test: `src/data/tushare.rs`

**Interfaces:**
- Consumes: shared chip records from Task 1
- Produces: `OfficialChipProvider::{chip_performance, chip_distribution}`
- Produces: `OfficialChipPerformance` and `OfficialChipBucket`

- [ ] **Step 1: Write failing fixture tests**

```rust
let perf = TushareClient::parse_chip_performance(&cyq_perf_fixture());
assert_eq!(perf[0].average_cost, 1512.40);
assert_eq!(perf[0].winner_rate, 72.5);
let chips = TushareClient::parse_official_chip_distribution(&cyq_chips_fixture());
assert!((chips.iter().map(|b| b.weight).sum::<f64>() - 1.0).abs() < 1e-6);
```

Cover reordered fields, percentages represented as strings, null values, and multiple dates.

- [ ] **Step 2: Verify the tests fail**

```bash
cargo test --locked parses_official_chip_fixtures -- --nocapture
```

- [ ] **Step 3: Implement provider calls**

Call `cyq_perf` in bounded code/date windows and `cyq_chips` for the distribution sample or canonical post-2018 backfill. Reject a response whose latest returned trading date is older than the requested daily update date.

- [ ] **Step 4: Run and commit**

```bash
cargo test --locked data::tushare::tests -- --nocapture
git add src/data/chip.rs src/data/mod.rs src/data/tushare.rs
git commit -m "feat: fetch official chip samples"
```

### Task 4: Implement benchmark sampling, metrics, and the source gate

**Files:**
- Create: `src/services/chip_validation.rs`
- Modify: `src/services/mod.rs`
- Test: `src/services/chip_validation.rs`

**Interfaces:**
- Produces: `build_validation_sample(universe, bars, corporate_actions) -> ChipValidationSample`
- Produces: `compare_chip_snapshots(local, official) -> ChipComparison`
- Produces: `decide_chip_source(report) -> ChipSourceDecision`

- [ ] **Step 1: Write failing metric and gate tests**

```rust
let comparison = compare_chip_snapshots(&local, &official);
assert_close(comparison.average_cost_relative_error, 0.02);
assert_close(comparison.winner_rate_absolute_error, 4.0);
assert_eq!(decide_chip_source(&report_at_limits()), ChipSourceDecision::Estimate);
assert_eq!(decide_chip_source(&report_with_biased_high_turnover_group()), ChipSourceDecision::Official);
```

Test deterministic stratification by exchange, market value, turnover, volatility, and corporate-action history; test median, mean absolute error, percentile interpolation, dominant peak, and normalized Wasserstein distribution distance.

- [ ] **Step 2: Verify the tests fail**

```bash
cargo test --locked services::chip_validation::tests -- --nocapture
```

- [ ] **Step 3: Implement the benchmark**

Use a stable hash of code and model version to select about 200 stocks inside strata. Select 24 evenly distributed performance dates per stock and 12 dates for 50 full-distribution stocks. Apply the approved global thresholds and fail when any subgroup's median cost or winner-rate error exceeds twice the global limit.

- [ ] **Step 4: Run and commit**

```bash
cargo test --locked services::chip_validation::tests -- --nocapture
git add src/services/chip_validation.rs src/services/mod.rs
git commit -m "feat: validate chip estimator accuracy"
```

### Task 5: Extend repair with validation and canonical chip backfill

**Files:**
- Modify: `src/services/company_intelligence.rs`
- Modify: `src/main.rs`
- Test: `src/services/company_intelligence.rs`

**Interfaces:**
- Produces: repair phases `chip_benchmark` and `chip_backfill`
- Consumes: model, official provider, validation decision, and checkpoint repository

- [ ] **Step 1: Write failing orchestration tests**

Test one benchmark per model version, estimate selection after a pass, official selection after a fail, estimate-before-2018 behavior, per-stock resume, 250-date transaction batches, and recovery after provider failure.

```rust
service.run_chip_benchmark().await?;
service.backfill_chips().await?;
assert_eq!(repo.snapshot(date(2017, 1, 3)).await?.source, "qbot_estimate");
assert_eq!(repo.snapshot(date(2020, 1, 3)).await?.source, expected_post_2018_source);
```

- [ ] **Step 2: Verify the tests fail**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked chip_backfill -- --nocapture
```

- [ ] **Step 3: Implement benchmark and backfill phases**

Run prerequisites, financials, dividends, benchmark, then canonical chip backfill from `--repair-company-intelligence`. Stream chronological bars and adjustment factors by stock. Commit 250 snapshots per transaction and advance the checkpoint only after commit. If official access is unavailable after an estimator failure, record a failed phase and leave estimates explicitly unvalidated rather than mislabeling them.

- [ ] **Step 4: Run and commit**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked company_intelligence -- --nocapture
git add src/services/company_intelligence.rs src/main.rs
git commit -m "feat: backfill validated historical chips"
```

### Task 6: Expose and render historical chip snapshots

**Files:**
- Modify: `src/services/dashboard_company.rs`
- Modify: `src/api/dashboard_routes.rs`
- Modify: `web/dashboard/js/api.js`
- Modify: `web/dashboard/js/company-panels.js`
- Modify: `web/dashboard/js/chart.js`
- Modify: `web/dashboard/js/app.js`
- Modify: `web/dashboard/css/dashboard.css`
- Modify: `web/dashboard/tests/company-panels.test.mjs`
- Modify: `web/dashboard/tests/chart.test.mjs`
- Modify: `web/dashboard/tests/browser/dashboard.spec.mjs`

**Interfaces:**
- Produces: `GET /api/dashboard/stocks/:code/chips?date=YYYY-MM-DD`
- Produces: `mountChart(..., { onCandleSelect })`

- [ ] **Step 1: Write failing backend tests**

Assert authentication, exact-date lookup, closest-earlier fallback, requested/resolved dates, source, model version, validation label, and 404 only when no earlier snapshot exists.

- [ ] **Step 2: Write failing frontend tests**

```js
assert.match(chipPanel(snapshot), /平均成本/);
assert.match(chipPanel(snapshot), /QBot 估算/);
assert.equal(selectedChipDate({ time: "2026-07-17" }), "2026-07-17");
```

In Playwright, move the crosshair and assert no request; click a candle and assert one request; click `Latest` and assert the latest request. Weekly/monthly clicks must use the candle's final trading date.

- [ ] **Step 3: Verify the tests fail**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked dashboard_chip -- --nocapture
cd web/dashboard && npm test
```

- [ ] **Step 4: Implement route and interaction**

Return distribution buckets ordered high-to-low. Subscribe to Lightweight Charts click events, ignore crosshair movement, and pass the clicked candle time to `onCandleSelect`. Cache chip payloads by code and requested date. Render horizontal price bars, current price, dominant peak, average cost, winner rate, concentration, resolved date, source, and model version.

- [ ] **Step 5: Run and commit**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked dashboard_chip -- --nocapture
cd web/dashboard && npm run check && npm run test:browser
git add src/services/dashboard_company.rs src/api/dashboard_routes.rs web/dashboard/js/api.js web/dashboard/js/company-panels.js web/dashboard/js/chart.js web/dashboard/js/app.js web/dashboard/css/dashboard.css web/dashboard/tests/company-panels.test.mjs web/dashboard/tests/chart.test.mjs web/dashboard/tests/browser/dashboard.spec.mjs
git commit -m "feat: show historical chip distributions"
```

### Task 7: Move daily chip work to 18:00 with readiness retries

**Files:**
- Modify: `src/services/chip_dist.rs`
- Modify: `src/services/company_intelligence.rs`
- Modify: `src/main.rs`
- Modify: `.github/workflows/deploy.yml`
- Modify: `web/dashboard/index.html`
- Modify: `web/dashboard/js/app.js`
- Modify: `web/dashboard/tests/deployment.test.mjs`
- Test: `src/services/chip_dist.rs`

**Interfaces:**
- Produces: `next_chip_update_attempt(now, expected_date, observed_date, attempts) -> UpdateDecision`
- Preserves: `ENABLE_CHIP_DIST` feature flag

- [ ] **Step 1: Write failing schedule tests**

```rust
assert_eq!(next_chip_update_attempt(bj(17, 59), today, None, 0), UpdateDecision::Wait);
assert_eq!(next_chip_update_attempt(bj(18, 0), today, None, 0), UpdateDecision::Run);
assert_eq!(next_chip_update_attempt(bj(18, 30), today, Some(yesterday), 1), UpdateDecision::Retry);
assert_eq!(next_chip_update_attempt(bj(20, 1), today, Some(yesterday), 5), UpdateDecision::StopForDay);
```

- [ ] **Step 2: Verify the tests fail**

```bash
cargo test --locked next_chip_update_attempt -- --nocapture
```

- [ ] **Step 3: Replace the 15:30 loop**

Start one company-intelligence cycle at 18:00 Beijing time. Run `CompanyIntelligenceService::update_latest()` for new financial revisions and dividends, then update chips. When the canonical chip source is official, verify the returned trade date and retry at bounded intervals through 20:00. When the source is estimated, require complete daily bar and turnover prerequisites before advancing model state. Save each dataset independently, so one failed category does not roll back another.

- [ ] **Step 4: Update deployment tests and assets**

Assert the detached repair command includes chip phases and can resume. Bump all six dashboard asset query versions together.

- [ ] **Step 5: Run final verification**

```bash
cargo fmt --check
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot CARGO_INCREMENTAL=0 cargo test --locked
cd web/dashboard && npm run check && npm audit --audit-level=high && npm run test:browser
git diff --check
```

Expected: all commands exit zero.

- [ ] **Step 6: Commit**

```bash
git add src/services/chip_dist.rs src/services/company_intelligence.rs src/main.rs .github/workflows/deploy.yml web/dashboard/index.html web/dashboard/js/app.js web/dashboard/tests/deployment.test.mjs
git commit -m "feat: schedule validated chip updates at 18:00"
```
