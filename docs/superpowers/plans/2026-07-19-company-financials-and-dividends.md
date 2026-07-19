# Company Financials and Dividends Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Backfill, synchronize, serve, and display complete available company financial and dividend histories.

**Architecture:** A focused provider maps Tushare responses into normalized contracts. A repository owns versioned persistence and checkpoints; a synchronization service owns date windows and resume behavior. Dashboard routes read PostgreSQL only, and the browser lazy-loads each sidebar tab.

**Tech Stack:** Rust, async-trait, Reqwest, Tushare APIs, SQLx/PostgreSQL, Axum, ES modules, Node test runner, Playwright.

## Global Constraints

- Preserve all available source history and all financial revisions.
- Show the latest revision for each reporting period by default.
- Keep optional company requests independent of bootstrap and OHLCV.
- Keep every new route authenticated and read-only.
- Do not modify Telegram code or `web/miniapp/chart`.
- Make every repair write idempotent and resumable by stock and date window.
- Run every production change through a failing test first.

---

### Task 1: Add versioned storage and repair checkpoints

**Files:**
- Create: `migrations/020_company_intelligence.sql`
- Create: `src/data/company.rs`
- Modify: `src/data/mod.rs`
- Create: `src/storage/company_repository.rs`
- Modify: `src/storage/mod.rs`
- Test: `src/storage/company_repository.rs`

**Interfaces:**
- Produces: `CompanyRepository::{upsert_financial_reports, upsert_dividends, financial_history, dividend_history}`
- Produces: `CompanyRepository::{claim_checkpoint, complete_checkpoint, fail_checkpoint, checkpoint}`
- Produces: shared `FinancialFrequency`, `FinancialReport`, and `DividendRecord` contracts

- [ ] **Step 1: Write failing SQLx tests**

```rust
repo.upsert_financial_reports(&[original.clone(), revision.clone()]).await?;
let page = repo.financial_history("600519.SH", FinancialFrequency::Annual, 100, None).await?;
assert_eq!(page.items.len(), 1);
assert_eq!(page.items[0].revision_count, 2);

repo.claim_checkpoint("financials", "600519.SH", Some(date(1998, 1, 1)), Some(date(2026, 12, 31))).await?;
repo.fail_checkpoint("financials", "600519.SH", "timeout").await?;
assert_eq!(repo.checkpoint("financials", "600519.SH").await?.unwrap().attempts, 1);
```

- [ ] **Step 2: Run the test and confirm the missing-module failure**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::company_repository::tests -- --nocapture
```

- [ ] **Step 3: Define shared records and create the schema**

Define `FinancialFrequency`, `FinancialReport`, and `DividendRecord` in `src/data/company.rs` before implementing repository signatures. Export the module from `src/data/mod.rs`.

Create `stock_financial_report_versions` with source, code, end date, announcement date, report type, frequency, source revision, typed numeric metrics, raw payload, available time, and ingestion time. Its primary key is `(source, code, end_date, report_type, source_revision)`.

```sql
CREATE TABLE stock_financial_report_versions (
    source VARCHAR(32) NOT NULL, code VARCHAR(12) NOT NULL,
    end_date DATE NOT NULL, announcement_date DATE,
    report_type VARCHAR(16) NOT NULL, frequency VARCHAR(16) NOT NULL,
    source_revision VARCHAR(64) NOT NULL,
    total_revenue NUMERIC(24,4), revenue NUMERIC(24,4),
    operating_profit NUMERIC(24,4), total_profit NUMERIC(24,4),
    net_profit_parent NUMERIC(24,4), deducted_net_profit NUMERIC(24,4),
    basic_eps NUMERIC(18,6), diluted_eps NUMERIC(18,6),
    roe NUMERIC(18,6), gross_margin NUMERIC(18,6), net_margin NUMERIC(18,6),
    revenue_yoy NUMERIC(18,6), net_profit_yoy NUMERIC(18,6),
    raw_payload JSONB NOT NULL DEFAULT '{}',
    available_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, code, end_date, report_type, source_revision)
);
```

Add `implementation_status`, `cash_dividend_tax`, and `source_revision VARCHAR(64) NOT NULL DEFAULT 'legacy'` to `corporate_action_versions`. Add `company_data_repair_checkpoints` with `(phase, code)` as primary key and start/end date, status, attempts, bounded error, and timestamps. Add indexes for latest financial revision, dividend date, and pending checkpoints.

- [ ] **Step 4: Implement repository methods**

Use idempotent upserts for identical source revisions. Select latest revisions with `ROW_NUMBER() OVER (PARTITION BY end_date, report_type ORDER BY announcement_date DESC NULLS LAST, available_at DESC)`. Clamp pages to 100 items and use end date plus report type as the cursor.

- [ ] **Step 5: Run tests and commit**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked storage::company_repository::tests -- --nocapture
git add migrations/020_company_intelligence.sql src/data/company.rs src/data/mod.rs src/storage/company_repository.rs src/storage/mod.rs
git commit -m "feat: add versioned company intelligence storage"
```

### Task 2: Add provider contracts and Tushare parsers

**Files:**
- Modify: `src/data/company.rs`
- Modify: `src/data/tushare.rs`
- Test: `src/data/tushare.rs`

**Interfaces:**
- Consumes: `FinancialFrequency`, `FinancialReport`, and `DividendRecord` from Task 1
- Produces: `CompanyDataProvider`
- Produces: Tushare implementations of `financial_reports` and `dividends`

- [ ] **Step 1: Write failing field-order-independent parser tests**

```rust
let rows = TushareClient::parse_financial_reports(&income_fixture(), &indicator_fixture(), fetched_at);
assert_eq!(rows[0].frequency, FinancialFrequency::Annual);
assert_eq!(rows[0].net_profit_parent, Some(86_240_000_000.0));
assert_eq!(rows[0].roe, Some(31.2));

let rows = TushareClient::parse_dividend_records(&dividend_fixture(), fetched_at);
assert_eq!(rows[0].implementation_status, "implemented");
assert_eq!(rows[0].cash_dividend, Some(2.76));
```

Fixtures cover nulls, reordered fields, quarters, revisions, proposals, implementation, and cash-plus-stock records.

- [ ] **Step 2: Run the test and confirm missing contracts**

```bash
cargo test --locked parses_company_financial_and_dividend_fixtures -- --nocapture
```

- [ ] **Step 3: Define the provider boundary**

```rust
#[async_trait]
pub trait CompanyDataProvider: Send + Sync {
    async fn financial_reports(&self, code: &str, start: NaiveDate, end: NaiveDate) -> Result<Vec<FinancialReport>>;
    async fn dividends(&self, code: &str, start: NaiveDate, end: NaiveDate) -> Result<Vec<DividendRecord>>;
}
```

Classify `1231` as annual and `0331`, `0630`, and `0930` as quarterly. Preserve source report type and raw payload.

- [ ] **Step 4: Implement bounded Tushare calls**

Call `income` and `fina_indicator` for the same code/window and join on code, end date, and report type. Preserve unmatched rows with null metrics. Call `dividend` by code/window and normalize progress to `proposed`, `approved`, `implemented`, or `unknown`.

- [ ] **Step 5: Run parser tests and commit**

```bash
cargo test --locked data::tushare::tests -- --nocapture
git add src/data/company.rs src/data/mod.rs src/data/tushare.rs
git commit -m "feat: parse Tushare company reports"
```

### Task 3: Implement resumable synchronization

**Files:**
- Create: `src/services/company_intelligence.rs`
- Modify: `src/services/mod.rs`
- Test: `src/services/company_intelligence.rs`

**Interfaces:**
- Produces: `CompanyIntelligenceService<P: CompanyDataProvider>`
- Produces: `backfill_financials()`, `backfill_dividends()`, and `update_latest()`

- [ ] **Step 1: Write failing tests with a recording fake provider**

```rust
let first = service.backfill_financials().await;
assert!(first.is_err());
provider.clear_failure("000002.SZ");
service.backfill_financials().await.unwrap();
assert_eq!(provider.calls_for("000001.SZ"), first_run_calls);
assert_eq!(repo.checkpoint("financials", "000002.SZ").await?.unwrap().status, "completed");
```

Assert yearly windows, resume after failure, completed-window skipping, and independent financial/dividend completion.

- [ ] **Step 2: Run the test and confirm the missing service**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked services::company_intelligence::tests -- --nocapture
```

- [ ] **Step 3: Implement the service**

Read current codes and listing dates, process one stock and one calendar year per request, claim before fetching, and complete only after the transaction commits. Continue other stocks after failures and return completed, failed, and pending counts. `update_latest()` requests the current and prior fiscal years to capture revisions.

- [ ] **Step 4: Run related tests and commit**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked company_intelligence -- --nocapture
git add src/services/company_intelligence.rs src/services/mod.rs
git commit -m "feat: add resumable company data sync"
```

### Task 4: Add the repair CLI

**Files:**
- Modify: `src/main.rs`
- Test: `src/main.rs`

**Interfaces:**
- Produces: `--repair-company-intelligence`
- Consumes: `Arc<TushareClient>` through `CompanyDataProvider`

- [ ] **Step 1: Write the failing flag test**

```rust
assert!(repair_company_intelligence_requested(["qbot", "--repair-company-intelligence"].map(str::to_string)));
assert!(!repair_company_intelligence_requested(["qbot", "--repair-daily-bars"].map(str::to_string)));
```

- [ ] **Step 2: Verify it fails**

```bash
cargo test --locked repair_company_intelligence_requested -- --nocapture
```

- [ ] **Step 3: Implement the repair branch**

Parse the flag before Telegram registration. After state creation, run financials then dividends, log bounded counts, and exit. Repair mode must not register Telegram commands or start HTTP/background loops.

- [ ] **Step 4: Run and commit**

```bash
cargo test --locked main::tests -- --nocapture
git add src/main.rs
git commit -m "feat: add company intelligence repair command"
```

### Task 5: Expose authenticated dashboard routes

**Files:**
- Create: `src/services/dashboard_company.rs`
- Modify: `src/services/mod.rs`
- Modify: `src/api/dashboard_routes.rs`
- Test: `src/services/dashboard_company.rs`
- Test: `src/api/dashboard_routes.rs`

**Interfaces:**
- Produces: `DashboardCompanyService::{company, financials, dividends}`
- Produces: `/api/dashboard/stocks/:code/{company,financials,dividends}`

- [ ] **Step 1: Write failing serialization and authorization tests**

```rust
assert_eq!(payload.items[0].end_date, date(2025, 12, 31));
assert_eq!(payload.items[0].revision_count, 2);
assert_eq!(unauthenticated_status("/api/dashboard/stocks/600519.SH/financials").await, StatusCode::UNAUTHORIZED);
```

Also assert cursor behavior, dividend ordering, 400 for invalid frequency/cursor, and 404 for unknown current codes.

- [ ] **Step 2: Verify the routes are missing**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked dashboard_company -- --nocapture
```

- [ ] **Step 3: Implement view models and handlers**

Company joins stock info, latest security master, and latest daily valuation. Financial and dividend responses expose `items` and `nextCursor`, never raw payloads. Handlers reuse `authorized` and map bad input to 400 and unknown codes to 404.

- [ ] **Step 4: Run and commit**

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test --locked dashboard -- --nocapture
git add src/services/dashboard_company.rs src/services/mod.rs src/api/dashboard_routes.rs
git commit -m "feat: expose dashboard company data"
```

### Task 6: Populate Overview, Financials, and Dividends

**Files:**
- Create: `web/dashboard/js/company-panels.js`
- Create: `web/dashboard/tests/company-panels.test.mjs`
- Modify: `web/dashboard/js/api.js`
- Modify: `web/dashboard/js/app.js`
- Modify: `web/dashboard/css/dashboard.css`
- Modify: `web/dashboard/tests/api.test.mjs`
- Modify: `web/dashboard/tests/browser/dashboard.spec.mjs`

**Interfaces:**
- Produces: API methods `company`, `financials`, and `dividends`
- Produces: renderers `companyPanel`, `financialPanel`, and `dividendPanel`

- [ ] **Step 1: Write failing unit tests**

```js
assert.equal(formatCurrency(86_240_000_000), "862.40亿");
assert.match(financialPanel(payload), /净利润/);
assert.match(dividendPanel({ items: [], nextCursor: null }), /暂无分红记录/);
```

Test URL encoding, annual/quarterly selection, revision labels, status labels, empty states, and cursor appending.

- [ ] **Step 2: Verify the tests fail**

```bash
cd web/dashboard && node --test tests/api.test.mjs tests/company-panels.test.mjs
```

- [ ] **Step 3: Implement lazy panels**

Load Overview after the chart mounts. Load other tabs on first activation and cache by stock, frequency, and cursor. Render compact financial trends and virtualized tables. Keep loading, empty, error, and retry state inside the affected tab.

- [ ] **Step 4: Add browser tests and verify**

Mock delayed and failed optional routes. Assert chart-first rendering, Annual/Quarterly requests, dividend pagination, local retry, and no document overflow.

```bash
cd web/dashboard && npm run check && npm run test:browser
```

- [ ] **Step 5: Commit**

```bash
git add web/dashboard/js/company-panels.js web/dashboard/tests/company-panels.test.mjs web/dashboard/js/api.js web/dashboard/js/app.js web/dashboard/css/dashboard.css web/dashboard/tests/api.test.mjs web/dashboard/tests/browser/dashboard.spec.mjs
git commit -m "feat: show company financials and dividends"
```

### Task 7: Start repair from deployment and verify

**Files:**
- Modify: `.github/workflows/deploy.yml`
- Modify: `web/dashboard/index.html`
- Modify: `web/dashboard/js/app.js`
- Modify: `web/dashboard/tests/deployment.test.mjs`

**Interfaces:**
- Produces: detached systemd repair unit and one shared asset version

- [ ] **Step 1: Write the failing deployment test**

Assert the workflow starts `--repair-company-intelligence` after health with `EnvironmentFile=/opt/qbot/.env` and `systemd-run --no-block`.

- [ ] **Step 2: Verify it fails**

```bash
cd web/dashboard && node --test tests/deployment.test.mjs
```

- [ ] **Step 3: Implement the repair launch and asset bump**

Use a stable unit name. Skip launch if active; otherwise start it detached. Bump all six dashboard asset query versions together.

- [ ] **Step 4: Run full phase verification**

```bash
cargo fmt --check
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot CARGO_INCREMENTAL=0 cargo test --locked
cd web/dashboard && npm run check && npm run test:browser
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/deploy.yml web/dashboard/index.html web/dashboard/js/app.js web/dashboard/tests/deployment.test.mjs
git commit -m "deploy: start company intelligence backfill"
```
