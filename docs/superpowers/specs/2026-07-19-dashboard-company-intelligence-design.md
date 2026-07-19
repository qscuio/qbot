# Dashboard Company Intelligence Design

**Date:** 2026-07-19  
**Status:** Approved for planning

## 1. Purpose

Extend the private QBot dashboard with company fundamentals, full available financial history, dividend history, and historical chip distributions. The work also replaces the fixed scan filter sidebar with a compact filter menu, adds a resizable stock-information sidebar, and makes weekly charts cover the complete stored price history.

The dashboard remains read-only. Existing Telegram routes, jobs, messages, and the Telegram chart miniapp remain unchanged.

## 2. Goals

The feature will:

- show company identity, valuation, financial performance, dividends, and chip distribution beside the chart;
- preserve all available financial and dividend history;
- make historical chip distributions available for any stored trading day;
- validate QBot's chip estimator against Tushare before choosing the long-term data source;
- backfill new datasets through a resumable repair workflow;
- update market-derived data after 18:00 Beijing time with data-readiness retries;
- aggregate weekly bars from the stock's full daily history; and
- load company information without delaying the initial candlestick chart.

## 3. Non-goals

This release will not:

- treat an estimated chip distribution as actual shareholder holdings;
- change signal detection, ranking, trading, or Telegram behavior;
- add financial forecasts, analyst consensus, or AI-generated investment advice;
- place complete financial datasets in the dashboard bootstrap response; or
- pre-render every historical financial or dividend row in the browser.

## 4. Workspace Layout

### 4.1 Scan filters

The dashboard removes the fixed left filter sidebar. A `Filters` button in the scan toolbar opens a dropdown containing:

- stock code or name search;
- signal group and signal selectors;
- ranked-pool membership;
- sort field; and
- sort direction.

The button shows the number of active filters. The filter state survives normal workspace navigation. Clearing filters remains one action.

### 4.2 Stock information sidebar

Stock pages gain a right sidebar with four tabs:

1. **Overview** — company identity, industry, listing date, latest quote, and available valuation measures.
2. **Financials** — revenue, net profit attributable to the parent, deducted net profit, growth, EPS, ROE, gross margin, and net margin.
3. **Dividends** — cash dividends, stock dividends, transfers, announcement date, record date, ex-date, payment date, and implementation status.
4. **Chips** — price distribution, dominant chip peak, average cost, winner rate, concentration, date, source, and model version.

The sidebar starts at 380 pixels. The user can drag its divider between 300 pixels and 50 percent of the viewport. Double-clicking the divider restores 380 pixels. A toolbar control collapses or reopens the sidebar. The browser stores the last width and collapsed state locally.

Resizing the sidebar reallocates space between the chart and sidebar without creating horizontal page scrolling. On narrow screens, the sidebar becomes an overlay drawer and disables drag resizing.

### 4.3 Long histories

Financial and dividend views show a compact summary followed by a trend chart and a virtualized table. Users can switch financials between annual and quarterly periods. Virtualization renders only visible rows while preserving access to the complete returned history.

The document body and editor stay fixed to the viewport. Long sidebar tabs scroll internally without changing the page layout.

## 5. Chart Interaction

Daily, weekly, and monthly controls keep their current behavior. Daily charts retain the existing initial 500-bar presentation for fast loading.

Weekly charts aggregate the complete daily history in PostgreSQL and return only weekly bars. Monthly charts continue reading the complete monthly series. The chart initially frames the latest bars, but users can pan through every returned period.

Clicking a daily candle locks the selected trading date. The Chips tab loads that date's snapshot. Moving the crosshair previews dates but sends no requests. A `Latest` action clears the locked date. On weekly or monthly charts, a click resolves to the final trading day in that period.

## 6. Dashboard API

The existing stock-detail route remains focused on OHLCV and scan hits. New authenticated, read-only routes isolate optional data:

- `GET /api/dashboard/stocks/:code/company`
- `GET /api/dashboard/stocks/:code/financials?frequency=annual|quarterly&cursor=...`
- `GET /api/dashboard/stocks/:code/dividends?cursor=...`
- `GET /api/dashboard/stocks/:code/chips?date=YYYY-MM-DD`

Each route resolves the code through the current stock master and returns a bounded view model. Financial and dividend routes paginate their histories. The chip route returns the closest stored trading day at or before the requested date and reports both requested and resolved dates.

The frontend loads Overview after the candlestick chart becomes usable. It loads the other routes only when the user opens their tabs. Requests for the same stock, tab, period, and date share an in-memory cache.

## 7. Financial Data

### 7.1 Sources

Tushare `income` supplies profit-statement values. Tushare `fina_indicator` supplies normalized indicators and growth rates. The backfill requests bounded date windows so the 100-row financial-indicator limit cannot truncate a company's history.

### 7.2 Storage

A financial-report version table stores one normalized record per source, stock, reporting period, report type, announcement date, and source revision. It includes:

- report end date and announcement date;
- annual, quarterly, or other report frequency;
- total revenue and operating revenue;
- operating profit, total profit, and net profit attributable to the parent;
- deducted net profit;
- basic and diluted EPS;
- ROE, gross margin, and net margin;
- reported growth measures; and
- source, source revision, ingestion time, and raw source payload.

The database keeps revisions for auditability. Dashboard queries select the latest available revision for each reporting period and expose a revision marker when applicable.

## 8. Dividend Data

The implementation reuses `corporate_action_versions` as the dividend source of truth. A migration adds any source fields required to distinguish proposal, approval, and implementation records and to preserve per-share cash and stock ratios without collapsing revisions.

The repair workflow requests the full available Tushare `dividend` history for every current stock. The dashboard defaults to implemented actions but can retain announced actions with an explicit status. Stable source keys and revision-aware writes make retries idempotent.

## 9. Chip Distribution

### 9.1 Meaning and limits

Chip distributions remain inferred cost distributions, not registered shareholder positions. Every response identifies its source as `qbot_estimate` or `tushare` and includes the estimator version when relevant.

### 9.2 Estimator

The historical estimator processes each stock sequentially from its first usable daily bar. It carries the prior distribution forward, applies turnover-based decay, allocates new volume across the day's traded range, handles price-grid rebasing, and applies adjustment and corporate-action inputs. It stores one snapshot per stock and trading day.

The full-history model replaces the current independent 120-day calculation during backfill. Daily updates resume from the latest stored model state instead of recomputing the stock's full history.

### 9.3 Official comparison

Before choosing a permanent source, the repair workflow runs a bounded benchmark:

- about 200 stocks stratified by exchange, market value, turnover, volatility, and corporate-action history;
- at least 24 distributed dates per stock for average cost and winner-rate comparisons against `cyq_perf`; and
- a distribution subset of at least 50 stocks and 12 dates per stock against `cyq_chips`.

The estimator passes only when all of these conditions hold:

- median relative average-cost error is at most 3 percent;
- median relative dominant-peak-price error is at most 3 percent;
- mean absolute winner-rate error is at most 5 percentage points;
- the 90th-percentile cost error is at most 8 percent; and
- no market, turnover, volatility, or corporate-action subgroup shows a material systematic bias.

The benchmark stores its sample definition, estimator version, aggregate metrics, subgroup metrics, timestamp, and decision. It may discard official sample distributions after recording the reproducible comparison results.

If the estimator passes, QBot uses it for the full available history and stops routine official chip synchronization. If it fails, QBot uses Tushare chip data from 2018 onward and labeled estimates before 2018.

## 10. Repair and Incremental Synchronization

### 10.1 Resumable repair

The data-repair command gains explicit phases for:

1. complete daily, monthly, adjustment, and turnover prerequisites;
2. financial history;
3. dividend history;
4. chip-model benchmark; and
5. canonical historical chip backfill.

A checkpoint records the phase, stock, date window, status, attempts, bounded error, and update time. Each phase writes idempotently. Restarting the command resumes incomplete work and never deletes a successful earlier phase.

The chip backfill works by stock and commits bounded date batches. It avoids an all-market in-memory calculation and limits concurrency so API traffic and dashboard reads remain responsive.

### 10.2 Daily updates

The scheduler starts the new market-data update at 18:00 Beijing time on trading days. Because upstream chip data may arrive between 18:00 and 19:00, the job checks the returned trading date and retries with bounded backoff until the expected date appears or the evening retry window expires.

Daily work fetches new financial revisions and dividends, then creates the latest canonical chip snapshot. Each dataset advances independently; one upstream failure does not roll back successful datasets.

## 11. Performance and Caching

The chart and scan bootstrap remain independent of company-intelligence queries. Complete weekly aggregation happens in PostgreSQL, which returns only weekly rows. Financial and dividend endpoints paginate and use indexed latest-revision queries. Chip snapshots use the existing stock-and-date lookup pattern.

The browser caches loaded panels for the active session. The server may add short private-cache headers or Redis caching, but PostgreSQL remains the source of truth. No company response enters the public Cloudflare cache.

## 12. Failure and Empty States

Each right-sidebar tab owns its loading, empty, stale, and error state. A failed optional route leaves the chart and other tabs usable. Retrying a tab repeats only its failed request.

The UI distinguishes:

- no report published for the selected company;
- no dividend history;
- insufficient bars for an estimated chip distribution;
- pending historical backfill;
- upstream official data unavailable; and
- an estimate that failed or has not completed validation.

The dashboard never labels an estimate as official. A missing selected-date chip snapshot may return the closest earlier completed trading day, but the UI must show the resolved date.

## 13. Testing

Backend tests will cover:

- full-history weekly OHLCV aggregation and volume sums;
- financial parsing, report-frequency classification, revision selection, and pagination;
- dividend status parsing, revision preservation, and idempotent writes;
- sequential chip calculation, corporate-action handling, date resolution, and incremental resume;
- benchmark metrics, subgroup gates, pass/fail source selection, and estimator-version invalidation;
- repair checkpoints, interrupted resumes, and isolated phase failures; and
- authentication on every new dashboard route.

Frontend tests will cover:

- filter dropdown behavior and active-filter counts;
- sidebar drag limits, reset, persistence, collapse, and narrow-screen drawer behavior;
- lazy tab requests and request caching;
- annual and quarterly financial history;
- dividend virtualization; and
- candle-click date selection, `Latest` reset, source labels, and independent tab errors.

Browser tests will confirm that the page has no body overflow, chart resizing remains stable, long histories stay responsive, and failed optional data does not replace the chart.

## 14. Deployment and Operations

GitHub Actions remains the only production code-deployment path. CI runs the Rust, frontend, browser, migration, and repair-resume tests before deployment. After service health checks pass, the workflow starts or resumes the repair command without holding the web-service restart open indefinitely.

Production inspection remains read-only from the development environment. Operators can query repair progress, latest dataset dates, benchmark decisions, and per-phase errors without reading secrets or changing server files.

## 15. Acceptance Criteria

The feature is complete when:

1. The scan page has no fixed left filter sidebar and retains every existing filter in a dropdown.
2. The stock page has a draggable, collapsible right sidebar whose width persists and never creates body overflow.
3. Overview, Financials, Dividends, and Chips load independently after the chart.
4. Financials and dividends expose the complete available source history with correct revisions and status labels.
5. Weekly charts expose all weeks derivable from stored daily bars rather than only the latest 500 daily bars.
6. Clicking a candle loads the matching or closest earlier chip snapshot without crosshair request storms.
7. The recorded benchmark selects the estimator only when every approved accuracy gate passes.
8. Historical repair resumes after interruption and daily updates begin at 18:00 Beijing time with readiness retries.
9. Estimates and official data remain visibly distinguishable.
10. Existing Telegram behavior and its chart miniapp remain unchanged.
