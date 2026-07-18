# Professional Scan Dashboard Design

**Date:** 2026-07-18  
**Status:** Approved for planning

## 1. Purpose

QBot needs a private, desktop-first web dashboard for reviewing the latest market scan and investigating one stock at a time on an interactive K-line chart. The dashboard will run at `https://dash.qscuio.com` and deploy through the repository's GitHub Actions workflow.

The existing Telegram miniapp in `web/miniapp/chart` remains unchanged. The new dashboard is a separate application with its own routes, authentication, layout, and assets.

## 2. Goals

The first release will:

- show the latest persisted scan results in one searchable, filterable workspace;
- group multiple signal hits for the same stock into one result row;
- distinguish ranked pools from raw detector signals;
- open a dedicated stock view with daily, weekly, and monthly candlesticks;
- show volume, moving averages, scan markers, and readable signal evidence;
- provide explicit loading, empty, stale, partial-data, and error states;
- protect all dashboard pages and data with a private login;
- deploy the application and its Nginx configuration through GitHub Actions; and
- preserve every Telegram miniapp and webhook behavior.

## 3. Non-goals

The first release will not:

- modify or replace `web/miniapp/chart`;
- trigger scans or scheduled jobs;
- place simulated or real trades;
- expose portfolio, watchlist, chip-distribution, or Telegram controls;
- provide historical signal playback or animation;
- add multi-user roles, invitations, or account administration; or
- change signal detection, ranking, or trading logic.

## 4. System Boundaries

### 4.1 Frontend

The new static application will live in `web/dashboard`. It will use focused HTML, CSS, and JavaScript modules without a runtime Node.js service. Axum will serve the directory under `/dashboard`, following the existing miniapp pattern. This keeps deployment aligned with the current Rust service and avoids a second production process.

The dashboard will use a maintained candlestick-chart library for rendering only. QBot remains responsible for data normalization, authentication, and authorization.

### 4.2 Backend

Dedicated Rust routes under `/api/dashboard/*` will serve dashboard view models. The handlers will call QBot services and repositories directly; they will not make HTTP requests back into QBot's existing API.

Dashboard code will have two clear backend boundaries:

- `DashboardAuth` validates credentials, creates sessions, and protects dashboard routes.
- `DashboardService` reads and normalizes scan, signal, stock, and OHLCV data.

The existing `/api/scan/*`, `/api/chart/*`, `/telegram/*`, and `/miniapp/chart/*` contracts will not change.

### 4.3 Public Routing

Nginx will give `dash.qscuio.com` its own virtual host. It will map browser routes to the new static application and proxy `/api/dashboard/*` to QBot on `127.0.0.1:8080`.

The existing webhook host will retain its own Nginx virtual host. Adding the dashboard host must not replace the current webhook configuration.

## 5. User Experience

### 5.1 Visual language

The interface will use a Cursor/VS Code-inspired dark workspace:

- a near-black title and activity bar;
- an explorer-style filter sidebar;
- editor tabs for scan results and opened stocks;
- a central editor surface for the grid or chart;
- an evidence panel beside the stock chart;
- a bottom status bar for API health, market state, scan time, and result count;
- muted borders and text with a blue focus accent; and
- tabular or monospace numerals for codes, prices, percentages, and dates.

A-share market colors apply only to market values: red indicates an increase, and green indicates a decrease. Decorative gradients, oversized cards, glass effects, and dense icon decoration are excluded.

### 5.2 Login

An unauthenticated visitor sees a focused sign-in screen branded for QBot. Successful login opens the scan workspace. Invalid credentials return one generic message. Expired sessions return the user to sign-in without exposing stale dashboard data.

### 5.3 Scan workspace

The main workspace contains:

- QBot health, market state, latest scan time, and a manual refresh control;
- summary values for unique stocks, total hits, active signals, and ranked-pool candidates;
- search by stock code or name;
- filters for signal group, signal, and ranked pool;
- sorting by code, name, hit count, price change, or ranked-pool priority; and
- a dense result grid with one row per stock.

Each row shows the stock code and name, latest close and daily change when available, signal badges, hit count, and ranked-pool membership. Filters update the visible rows and status-bar count without a full page reload.

### 5.4 Stock workspace

Selecting a result opens a stable stock route and a new editor tab. The tab preserves chart period and visible range while the user switches among opened stocks.

The stock workspace contains:

- stock identity, latest OHLC values, daily change, and trade date;
- daily, weekly, and monthly period controls;
- an interactive candlestick chart;
- volume and MA5, MA10, MA20, and MA60 overlays;
- markers for signals present in the selected scan;
- readable evidence cards for every triggered signal;
- a collapsible raw-metadata view for diagnostics; and
- previous and next controls based on the active filtered result set.

The layout uses the evidence panel beside the chart on wide screens and below it on narrow screens.

## 6. Data Contracts

### 6.1 Authentication routes

- `POST /api/dashboard/auth/login` accepts a username and password, then creates a dashboard session.
- `POST /api/dashboard/auth/logout` revokes the current session and clears its cookie.
- `GET /api/dashboard/auth/session` returns the current authentication state without returning credentials or secrets.

### 6.2 Bootstrap route

`GET /api/dashboard/bootstrap` returns:

- backend health and server time;
- Beijing market state;
- the latest persisted scan run identifier and timestamp;
- scan freshness state;
- the signal catalog and group labels;
- summary counts; and
- normalized result rows.

The backend groups scan rows by stock code. Each stock contains its complete hit list, while each hit retains its signal identifier, display name, icon, category, ranked-pool flag, and metadata. The response may omit optional price enrichment for a stock that lacks a current bar; it must mark that stock as partially enriched.

### 6.3 Stock-detail route

`GET /api/dashboard/stocks/:code?period=daily|weekly|monthly&days=N` returns:

- normalized stock identity and latest price values;
- chronologically ordered OHLCV bars;
- scan hits from the bootstrap run;
- chart markers aligned to known trade dates; and
- a partial-data warning when price history or signal metadata cannot be enriched.

The server will clamp `days` to a documented safe range and reject unsupported periods. It will resolve short and exchange-qualified stock codes through the existing repository logic.

### 6.4 Source of truth

PostgreSQL provides the persisted scan timestamp and OHLCV history. A new scan-run ledger will record each run's identifier, status, start and completion times, stocks checked, hit count, and bounded error summary. The scanner will create a running record before work begins and mark it completed or failed when work ends. This ledger lets the dashboard distinguish no scan, an active scan, a successful zero-hit scan, a failed scan, and a completed scan with hits without changing signal logic.

The existing signal registry provides raw-signal names, icons, and groups. The dashboard catalog will also define `multi_signal` and the six ranked pools so every stored signal has a stable display label. Existing `scan_results` rows remain the source of individual hits and retain their current contract.

Redis may accelerate reads, but the dashboard must still show the latest persisted run after the current 24-hour scan cache expires.

## 7. Authentication and Security

The dashboard uses one administrative credential in the first release. GitHub environment secrets provide:

- `DASHBOARD_USERNAME`;
- `DASHBOARD_PASSWORD_HASH`, encoded with Argon2id;
- `DASHBOARD_SESSION_SECRET`; and
- the Cloudflare origin certificate and private key used by Nginx.

The browser never receives the QBot API key, password hash, session secret, or origin private key.

On successful login, the server creates a random opaque session, stores an HMAC digest of the identifier with an expiry, and sends the identifier in a `Secure`, `HttpOnly`, `SameSite=Strict` cookie. `DASHBOARD_SESSION_SECRET` keys the digest. A session lasts 12 hours. Logout revokes it immediately. The login route applies per-IP failure throttling and logs neither submitted passwords nor session identifiers.

All dashboard data routes require a valid session. Since the first release is read-only, its only state-changing requests are login and logout. These requests accept JSON only, validate the request origin, and reject oversized bodies.

## 8. Error and Freshness Handling

The interface will represent these states explicitly:

- **Never scanned:** no persisted run exists.
- **Empty scan:** a run exists but contains no hits.
- **Stale scan:** the latest persisted scan is older than the most recent expected completed trading-day scan.
- **Partial enrichment:** scan hits exist, but one or more stocks lack current OHLCV data.
- **Unavailable chart:** the selected stock has no usable bars.
- **Backend unavailable:** the dashboard API cannot complete the request.
- **Unauthorized:** the session is missing, invalid, revoked, or expired.

The bootstrap request fails only when its core scan payload cannot be produced. Optional price enrichment failures remain attached to affected rows. The stock workspace keeps its identity and evidence visible when chart history fails.

## 9. Deployment

The existing GitHub Actions deployment remains the only production write path. The Nerd SSH key is used from this development environment only for read-only inspection.

On each deployment from `main`, GitHub Actions will:

1. run Rust tests and frontend checks;
2. build the release binary and verify the dashboard static assets;
3. connect with the existing VPS deployment secrets;
4. write dashboard runtime secrets to the protected QBot environment file;
5. install the Cloudflare origin certificate and key with restricted permissions;
6. install a separate Nginx virtual host for `dash.qscuio.com`;
7. validate Nginx configuration before reload;
8. install the binary, keep the checked-out `web/dashboard` assets in place, and restart QBot; and
9. verify health, unauthenticated rejection, and the dashboard shell.

The Cloudflare DNS record remains proxied with the orange cloud. Cloudflare SSL/TLS mode must remain `Full (strict)`. The origin must accept Cloudflare traffic on HTTPS and present a certificate valid for `dash.qscuio.com`.

Deployment will preserve the current webhook virtual host and Telegram routes. A failed test, build, Nginx validation, service restart, or health check stops the workflow.

## 10. Testing

Backend tests will cover:

- valid, invalid, expired, and revoked sessions;
- login throttling and generic failure responses;
- latest-run selection;
- scan-run lifecycle transitions, including successful zero-hit and failed runs;
- grouping multiple hits under one stock;
- raw-signal, multi-signal, and ranked-pool catalog entries;
- freshness classification;
- optional price-enrichment failures;
- stock-code resolution, period validation, and day limits; and
- protection of every dashboard data route.

Frontend tests will cover:

- result normalization and stable sorting;
- text and signal filtering;
- summary and visible-row counts;
- daily, weekly, and monthly chart transformations;
- signal-marker placement;
- editor-tab state; and
- rendering for loading, empty, stale, partial, unauthorized, and error states.

A browser smoke test will exercise login, scan display, filtering, stock selection, K-line rendering, tab switching, logout, and responsive layout at desktop and narrow widths. Deployment smoke checks will verify that `dash.qscuio.com` redirects HTTP to HTTPS, serves a valid origin-backed TLS response through Cloudflare, rejects unauthenticated dashboard data requests, and leaves the Telegram health and webhook routes reachable.

## 11. Acceptance Criteria

The release is complete when:

1. `https://dash.qscuio.com` shows the private login screen through Cloudflare.
2. Valid credentials open the latest scan workspace; invalid credentials reveal no dashboard data.
3. Search, signal filters, ranked-pool filters, and sorting produce correct visible rows.
4. A stock with several hits appears once and shows every hit.
5. Selecting a stock opens a dedicated workspace with working daily, weekly, and monthly K-lines.
6. Signal evidence and chart markers match the selected persisted scan run.
7. Missing and stale data appear as explicit states rather than fabricated values.
8. Session expiry and logout remove access to dashboard data.
9. The GitHub Actions workflow deploys the dashboard and validates the service without manual server changes.
10. The Telegram miniapp, webhook, and existing API contracts remain unchanged.
