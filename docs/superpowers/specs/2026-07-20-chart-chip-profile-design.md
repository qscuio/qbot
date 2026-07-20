# Chart-Aligned Chip Profile Design

**Date:** 2026-07-20
**Status:** Approved
**Scope:** Private scan dashboard and company-intelligence repair worker only

## Problem

The dashboard currently renders chip distribution as a list in the stock information sidebar. That presentation is disconnected from the K-line price scale and does not match professional Chinese market terminals, where a horizontal chip profile is displayed beside the price chart and changes with the inspected candle.

The current production failure is not only a presentation problem. The dashboard chip endpoint returns `404 stock not found` when no canonical chip snapshot exists. Production currently has zero rows in `chip_distribution` because the single company-intelligence repair process runs financial history before dividends, chip validation, and chip backfill. Financial repair is still running, so chip repair has never started.

## Goals

- Display the chip profile at the right edge of the K-line price pane, aligned to the same price coordinates as candles and moving averages.
- Automatically follow the candle under the crosshair without flooding the API or allowing stale responses to overwrite a newer selection.
- Restore the latest chip snapshot when the pointer leaves the chart.
- Keep daily, weekly, and monthly behavior consistent by resolving the latest canonical snapshot at or before the selected candle date.
- Keep chart rendering responsive during resizing, zooming, panning, and inspector resizing.
- Replace the large sidebar error card with a quiet, non-blocking unavailable state.
- Make chip validation/backfill independently resumable so financial repair cannot prevent chip data from being produced.
- Preserve the Telegram mini app and its routes, messages, signal, and trading behavior unchanged.

## Non-Goals

- Replacing Lightweight Charts.
- Calculating an ad-hoc distribution in the browser from only the visible bars.
- Treating estimated data as official data.
- Exposing repair controls to dashboard users.
- Manually deploying or mutating the production server outside GitHub Actions.

## Chosen Approach

Use a DOM/SVG overlay mounted inside the dashboard chart pane, backed by the existing authenticated dashboard chip API. The overlay maps each normalized chip bucket through the candlestick series' `priceToCoordinate` function and draws horizontal bars from the right edge toward the left. It is presentation-only and does not alter the chart's time series.

This is preferable to keeping the sidebar-only list because the distribution remains visually tied to price. It is also preferable to client-side estimation because the server already owns the canonical model, provenance, validation status, and historical snapshots.

## User Interface

The separate `Chips` inspector tab is removed. The stock inspector retains Overview, Financials, and Dividends.

The chart legend receives a compact chip-profile status and visibility control. The profile is enabled by default. Its visibility preference is stored locally so a user may hide it without changing server state.

The profile occupies an adaptive portion of the chart's right side and overlays the plot instead of adding a new document column. Its width is bounded so it remains useful on both narrow and wide layouts. It has `pointer-events: none`, so chart drag, zoom, and crosshair behavior remain uninterrupted.

Each bucket is positioned by price, not by list order. Bar width is normalized against the largest bucket in the returned snapshot. Buckets below or equal to the selected close use the dashboard's Chinese-market profit/up color; buckets above it use the loss/down color. The dominant peak receives a stronger edge or label. A compact header shows the resolved date, average cost, and winner rate without covering the chart toolbar.

When no canonical snapshot exists, the chart remains fully usable. The overlay is omitted and the compact status reads `筹码待回填`; the application does not show `stock not found`, a retry card, or an empty sidebar panel.

## Automatic Date Selection

On stock open, the dashboard requests the latest canonical snapshot.

Crosshair movement identifies the candle date using the candle series data. Selection is debounced by approximately 200 milliseconds. Requests are cached by `code + date`, and a new selection aborts or invalidates the prior request. Only the newest request generation may update the overlay.

Pointer movement within the same candle does not issue another request. Leaving the chart restores the cached latest snapshot, requesting it only if it is not already cached.

Daily bars use their trading date. Weekly and monthly bars use their final represented trading date, already supplied by the dashboard chart payload. The backend continues resolving `trade_date <= requested_date`, so weekends, holidays, and sparse snapshot histories resolve safely to the nearest prior canonical snapshot.

Crosshair movement must never switch inspector tabs or cause a full workspace render.

## Rendering and Layout Synchronization

The chart handle owns the overlay lifecycle. It receives chip-profile data through an update method and redraws when:

- the snapshot changes;
- the chart container resizes;
- the visible price range changes after zooming or panning;
- the inspector is resized or collapsed;
- the chart period or stock changes.

The renderer obtains vertical coordinates from the candlestick series for every bucket. Buckets outside the current price pane are skipped, not clamped to an edge. Adjacent buckets that map to the same pixel row may be coalesced to avoid visual noise. The number of DOM/SVG elements remains bounded by the canonical distribution ceiling.

Destroying a chart unsubscribes crosshair, click, range, and resize handlers and cancels pending timers. A response from a destroyed chart, signed-out session, prior stock, or prior date is ignored.

## API and Error Semantics

The existing endpoint remains:

`GET /api/dashboard/stocks/:code/chips?date=YYYY-MM-DD`

No date means latest. A date means the latest canonical normalized snapshot at or before that date. Responses retain provenance fields, requested and resolved dates, current price, average cost, winner rate, concentration, dominant peak, and normalized buckets.

The frontend treats a missing chip snapshot as an optional-data state. Authentication and malformed-request errors retain their existing behavior. The API may continue using its current not-found response internally; the dashboard must not surface the raw message as a chart failure.

## Independent Chip Repair

Add a dedicated repair mode for chip validation and backfill. The combined company-intelligence repair remains available, but deployment starts chip repair in its own transient systemd unit after the application health check. The chip unit is not sequenced behind financial or dividend history.

The chip repair mode:

1. obtains or reuses the persisted model validation decision;
2. runs the resumable canonical chip backfill;
3. preserves checkpoint and lease safety;
4. reports incomplete, pending, and failed stocks as a failed unit result rather than silently succeeding;
5. never labels estimates as official or validated unless the persisted benchmark supports that claim.

The worker uses bounded database/provider concurrency already enforced by the service and does not run inside a web request. GitHub Actions creates or replaces the transient unit idempotently. Deployment remains the only production mutation path.

## Data Availability During Backfill

The dashboard can display a stock as soon as its first canonical snapshot is committed. Historical requests continue resolving at or before the requested date. Stocks not yet reached by the worker show the quiet pending state and become available on a later request without a frontend deployment.

No Cloudflare cache is added to authenticated chip responses. They remain private and `no-store`; the frontend's in-memory per-session cache prevents repeated crosshair requests.

## Testing

Frontend unit tests cover date extraction, cache keys, normalized profile geometry, price-coordinate filtering, color classification, bounded elements, and quiet missing-data behavior.

Browser tests cover:

- latest snapshot on stock open;
- debounced crosshair-driven historical requests;
- no duplicate request while remaining on one candle;
- stale response suppression during rapid movement;
- restoration of latest data on pointer leave;
- weekly and monthly final-candle dates;
- profile redraw after chart/inspector resize;
- absence of the Chips inspector tab and large error card;
- cleanup across stock changes and sign-out.

Backend tests cover the dedicated repair argument, phase isolation, persisted validation reuse, failure reporting, and GitHub Actions transient-unit configuration. Existing repository and route tests continue proving at-or-before resolution and canonical-only selection.

## Deployment and Verification

Changes are committed and pushed through the repository. GitHub Actions runs Rust, frontend unit, browser, and deployment-contract tests, deploys the dashboard, restarts the application through the existing workflow, and starts the independent chip repair unit.

Post-deployment checks are read-only: application health, workflow status, repair unit status, repair logs, and growth of canonical `chip_distribution` rows. No manual binary copy, service restart, database write, or Cloudflare mutation is performed.
