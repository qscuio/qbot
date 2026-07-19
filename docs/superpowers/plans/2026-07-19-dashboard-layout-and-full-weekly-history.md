# Dashboard Layout and Full Weekly History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed scan sidebar with a filter dropdown, add a draggable and collapsible stock-information shell, and return weekly candles for the complete stored daily history.

**Architecture:** PostgreSQL aggregates weekly OHLCV before data reaches the service. The frontend keeps filter and sidebar preferences in small pure state helpers, while `app.js` renders the workspace and owns DOM bindings. The information sidebar ships with an Overview tab and stable extension points for the financial, dividend, and chip plans.

**Tech Stack:** Rust, Axum, SQLx/PostgreSQL, browser-native ES modules, CSS Grid, Node test runner, Playwright, Lightweight Charts 5.0.8.

## Global Constraints

- Do not modify `web/miniapp/chart`, Telegram routes, Telegram messages, signal logic, or trading logic.
- Keep the document body and editor fixed to the viewport with no body overflow.
- Keep daily initial display behavior unchanged.
- Default the right sidebar to 380px, clamp it to 300px through 50% of the viewport, persist its width and collapsed state, and use an overlay drawer on narrow screens.
- Run each code change through a failing test before implementation.
- Bump every dashboard asset query version together before deployment.

---

### Task 1: Aggregate complete weekly history in PostgreSQL

**Files:**
- Modify: `src/storage/dashboard_repository.rs`
- Test: `src/storage/dashboard_repository.rs`

**Interfaces:**
- Produces: `DashboardRepository::weekly_history(&self, code: &str) -> Result<Vec<Candle>>`
- Consumes: `stock_daily_bars` rows ordered by `trade_date`

- [ ] **Step 1: Write the failing SQLx test**

Insert more than 500 daily candles for one code, call `weekly_history`, and assert the first and final stored weeks are present. Add a second three-row week whose expected open, high, low, close, volume, amount, turnover, PE, and PB are explicit.

```rust
let weekly = DashboardRepository::new(pool).weekly_history("600519.SH").await.unwrap();
assert!(weekly.len() > 100);
assert_eq!(weekly.first().unwrap().trade_date, date(2024, 1, 5));
assert_eq!(weekly.last().unwrap().volume, 600);
assert_eq!(weekly.last().unwrap().amount, 6_000.0);
```

- [ ] **Step 2: Verify the test fails**

Run:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked weekly_history_aggregates_every_stored_daily_bar -- --nocapture
```

Expected: compilation fails because `weekly_history` does not exist.

- [ ] **Step 3: Implement the aggregate query**

Group by `date_trunc('week', trade_date)`, use the first date and open, maximum high, minimum low, last close, and summed volume and amount. Return a `Candle` per group in chronological order. Carry the last non-null turnover, PE, and PB from the week.

```rust
pub async fn weekly_history(&self, code: &str) -> Result<Vec<Candle>> {
    let rows = sqlx::query_as::<_, WeeklyCandleRow>(
        r#"SELECT MAX(trade_date) AS trade_date,
                  (ARRAY_AGG(open::float8 ORDER BY trade_date))[1] AS open,
                  MAX(high)::float8 AS high,
                  MIN(low)::float8 AS low,
                  (ARRAY_AGG(close::float8 ORDER BY trade_date DESC))[1] AS close,
                  SUM(volume)::int8 AS volume,
                  SUM(amount)::float8 AS amount,
                  (ARRAY_AGG(turnover::float8 ORDER BY trade_date DESC)
                     FILTER (WHERE turnover IS NOT NULL))[1] AS turnover,
                  (ARRAY_AGG(pe::float8 ORDER BY trade_date DESC)
                     FILTER (WHERE pe IS NOT NULL))[1] AS pe,
                  (ARRAY_AGG(pb::float8 ORDER BY trade_date DESC)
                     FILTER (WHERE pb IS NOT NULL))[1] AS pb
           FROM stock_daily_bars
           WHERE code = $1
           GROUP BY date_trunc('week', trade_date)
           ORDER BY trade_date"#,
    )
    .bind(code)
    .fetch_all(&self.pool)
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}
```

- [ ] **Step 4: Run the focused test and repository tests**

Run the focused command from Step 2, then:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::dashboard_repository::tests -- --nocapture
```

Expected: all dashboard repository tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/storage/dashboard_repository.rs
git commit -m "feat: aggregate complete weekly history"
```

### Task 2: Route weekly requests through the complete aggregate

**Files:**
- Modify: `src/services/dashboard.rs`
- Test: `src/services/dashboard.rs`

**Interfaces:**
- Consumes: `DashboardRepository::weekly_history`
- Preserves: `DashboardService::stock_detail(raw_code, period, requested_days)`

- [ ] **Step 1: Write a failing service-level selection test**

Extract the period lookup behind a focused private method and assert that Weekly uses the repository aggregate without a 500-day budget, Daily clamps `days` to `30..=5_000`, and Monthly keeps the existing monthly query.

```rust
assert_eq!(history_source(DashboardPeriod::Weekly, Some(30)), HistorySource::CompleteWeekly);
assert_eq!(history_source(DashboardPeriod::Daily, None), HistorySource::RecentDaily(500));
assert_eq!(history_source(DashboardPeriod::Monthly, None), HistorySource::CompleteMonthly);
```

- [ ] **Step 2: Verify the test fails**

```bash
cargo test --locked dashboard_history_source_keeps_weekly_unbounded -- --nocapture
```

Expected: failure because `HistorySource` and `history_source` do not exist.

- [ ] **Step 3: Implement the three explicit branches**

Add the small selector used by the test, then make `stock_detail` execute its result:

```rust
#[derive(Debug, PartialEq, Eq)]
enum HistorySource { RecentDaily(usize), CompleteWeekly, CompleteMonthly }

fn history_source(period: DashboardPeriod, requested_days: Option<usize>) -> HistorySource {
    match period {
        DashboardPeriod::Daily => HistorySource::RecentDaily(requested_days.unwrap_or(500).clamp(30, 5_000)),
        DashboardPeriod::Weekly => HistorySource::CompleteWeekly,
        DashboardPeriod::Monthly => HistorySource::CompleteMonthly,
    }
}
```

```rust
let period_bars = match history_source(period, requested_days) {
    HistorySource::RecentDaily(days) => postgres::get_stock_history(
        &self.state.db,
        &code,
        days,
    ).await?,
    HistorySource::CompleteWeekly => self.repo.weekly_history(&code).await?,
    HistorySource::CompleteMonthly => {
        postgres::get_stock_monthly_history(&self.state.db, &code).await?
    }
};
```

Remove the Weekly call to `resample_dashboard_bars`; retain the pure resampler for existing tests and any other consumers.

- [ ] **Step 4: Run dashboard service tests**

```bash
cargo test --locked services::dashboard::tests -- --nocapture
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/services/dashboard.rs
git commit -m "fix: return full weekly dashboard history"
```

### Task 3: Add pure filter-menu and sidebar preference state

**Files:**
- Modify: `web/dashboard/js/state.js`
- Modify: `web/dashboard/tests/state.test.mjs`

**Interfaces:**
- Produces: `activeFilterCount(filters) -> number`
- Produces: `clampInspectorWidth(width, viewportWidth) -> number`
- Produces: `loadInspectorPreferences(storage, viewportWidth) -> { width, collapsed }`
- Produces: `saveInspectorPreferences(storage, preferences) -> void`

- [ ] **Step 1: Write failing state tests**

```js
test("counts only active scan filters", () => {
  assert.equal(activeFilterCount({ search: "茅台", group: "trend", signal: "", rankedOnly: false, sort: "ranked", direction: "desc" }), 2);
});

test("clamps and persists inspector preferences", () => {
  assert.equal(clampInspectorWidth(120, 1600), 300);
  assert.equal(clampInspectorWidth(1200, 1600), 800);
  const storage = memoryStorage();
  saveInspectorPreferences(storage, { width: 440, collapsed: true });
  assert.deepEqual(loadInspectorPreferences(storage, 1600), { width: 440, collapsed: true });
});
```

- [ ] **Step 2: Verify the tests fail**

```bash
cd web/dashboard && node --test tests/state.test.mjs
```

Expected: imports fail because the new helpers are missing.

- [ ] **Step 3: Implement the pure helpers**

Count only search, group, signal, and rankedOnly. Treat ranked sorting and descending direction as defaults. Use the key `qbot.dashboard.inspector.v1`, a 380px default, 300px minimum, and `Math.floor(viewportWidth * 0.5)` maximum. Catch storage parse and quota errors and return safe defaults.

- [ ] **Step 4: Run state tests**

```bash
cd web/dashboard && node --test tests/state.test.mjs
```

Expected: all state tests pass.

- [ ] **Step 5: Commit**

```bash
git add web/dashboard/js/state.js web/dashboard/tests/state.test.mjs
git commit -m "feat: add dashboard layout preferences"
```

### Task 4: Replace the fixed sidebar with the filter dropdown

**Files:**
- Modify: `web/dashboard/js/app.js`
- Modify: `web/dashboard/css/dashboard.css`
- Modify: `web/dashboard/tests/browser/dashboard.spec.mjs`

**Interfaces:**
- Consumes: `activeFilterCount`
- Produces: `#filter-menu`, `#filter-toggle`, and the existing `#filters` form

- [ ] **Step 1: Write the failing browser test**

Open the scan page and assert `.sidebar` is absent, click `Filters`, change two filters, and assert the trigger shows `Filters (2)`. Confirm Escape and an outside click close the menu without clearing state. Assert the four latest-run metrics remain visible in a compact summary strip above the result grid.

```js
await expect(page.locator(".sidebar")).toHaveCount(0);
await page.getByRole("button", { name: /^Filters/ }).click();
await page.getByLabel("Stock search").fill("茅台");
await page.getByLabel("Signal group").selectOption("trend");
await expect(page.getByRole("button", { name: "Filters (2)" })).toBeVisible();
```

- [ ] **Step 2: Verify the browser test fails**

```bash
cd web/dashboard && npx playwright test --grep "filter dropdown replaces"
```

Expected: `.sidebar` still exists.

- [ ] **Step 3: Implement the dropdown and grid change**

Remove `sidebarTemplate()` from the workspace grid. Render the existing form inside an anchored `.filter-popover` in `scanTemplate()`, and move Unique stocks, Total hits, Active signals, and Ranked candidates into a compact summary strip. Change `.workspace` to `grid-template-columns: 48px minmax(0, 1fr)`. Bind the same filter inputs, clear action, Escape, and outside-click behavior. Remove the obsolete mobile drawer rules.

- [ ] **Step 4: Run the focused browser test and frontend checks**

```bash
cd web/dashboard
npx playwright test --grep "filter dropdown replaces"
npm run check
```

Expected: focused browser test and all unit checks pass.

- [ ] **Step 5: Commit**

```bash
git add web/dashboard/js/app.js web/dashboard/css/dashboard.css web/dashboard/tests/browser/dashboard.spec.mjs
git commit -m "feat: move scan filters into toolbar"
```

### Task 5: Add the draggable and collapsible information sidebar shell

**Files:**
- Modify: `web/dashboard/js/app.js`
- Modify: `web/dashboard/js/chart.js`
- Modify: `web/dashboard/css/dashboard.css`
- Modify: `web/dashboard/tests/browser/dashboard.spec.mjs`

**Interfaces:**
- Consumes: inspector preference helpers from Task 3
- Produces: `.stock-inspector`, `.inspector-resizer`, `.inspector-toggle`, and tab buttons with `data-inspector-tab`

- [ ] **Step 1: Write failing browser tests**

Test default width, pointer-drag clamping, double-click reset, collapse/reopen persistence after reload, no document overflow, chart resize, and overlay behavior below 700px.

```js
await expect(page.locator(".stock-inspector")).toHaveCSS("width", "380px");
await page.locator(".inspector-toggle").click();
await expect(page.locator(".stock-workspace")).toHaveClass(/inspector-collapsed/);
await page.reload();
await expect(page.locator(".stock-workspace")).toHaveClass(/inspector-collapsed/);
```

- [ ] **Step 2: Verify the tests fail**

```bash
cd web/dashboard && npx playwright test --grep "resizable stock information sidebar"
```

Expected: `.stock-inspector` is absent.

- [ ] **Step 3: Implement the sidebar shell**

Wrap chart and inspector in `.stock-workspace`. Set `--inspector-width` from saved preferences. Use pointer capture on the divider, clamp through `clampInspectorWidth`, and call `chartHandle.resize?.()` during and after drag. Render tabs for Overview, Financials, Dividends, and Chips; Task 5 fills Overview with data already available in the stock response and renders stable empty placeholders for later plans.

Extend `mountChart` to expose:

```js
return {
  resize: () => chart.resize(container.clientWidth, container.clientHeight),
  destroy: () => { cancelInitialFit(); chart.remove(); },
};
```

Use an overlay drawer below 700px and preserve keyboard access to toggle and reset controls.

- [ ] **Step 4: Run browser and frontend suites**

```bash
cd web/dashboard
npm run check
npm run test:browser
```

Expected: all unit and browser tests pass with no body overflow.

- [ ] **Step 5: Commit**

```bash
git add web/dashboard/js/app.js web/dashboard/js/chart.js web/dashboard/css/dashboard.css web/dashboard/tests/browser/dashboard.spec.mjs
git commit -m "feat: add resizable stock information sidebar"
```

### Task 6: Version assets and verify the phase

**Files:**
- Modify: `web/dashboard/index.html`
- Modify: `web/dashboard/js/app.js`
- Test: `web/dashboard/tests/deployment.test.mjs`

**Interfaces:**
- Produces: one new shared dashboard asset version

- [ ] **Step 1: Update the asset-version expectation first**

Add an assertion that the collected asset version differs from `20260719.1`, then run:

```bash
cd web/dashboard && node --test tests/deployment.test.mjs
```

Expected: failure while assets still use `20260719.1`.

- [ ] **Step 2: Bump all six query versions together**

Update `dashboard.css`, `lightweight-charts.js`, `app.js`, `api.js`, `chart.js`, and `state.js` references to the same new value.

- [ ] **Step 3: Run phase verification**

```bash
cargo fmt --check
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot CARGO_INCREMENTAL=0 cargo test --locked dashboard -- --nocapture
cd web/dashboard && npm run check && npm run test:browser
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 4: Commit**

```bash
git add web/dashboard/index.html web/dashboard/js/app.js web/dashboard/tests/deployment.test.mjs
git commit -m "chore: version dashboard workspace assets"
```
