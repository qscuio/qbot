# Chart-Aligned Chip Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render a price-aligned chip profile inside the dashboard K-line chart and automatically switch its snapshot with the crosshair.

**Architecture:** Add a focused pure-geometry/rendering module beside the existing Lightweight Charts adapter. The chart owns overlay placement and event subscriptions; the application owns authenticated fetching, per-session caching, debouncing, and stale-response suppression.

**Tech Stack:** Browser ES modules, Lightweight Charts 5.0.8, SVG/DOM, Node test runner, Playwright.

## Global Constraints

- Preserve the Telegram mini app and its routes, messages, signal, and trading behavior unchanged.
- Keep authenticated chip responses private and `no-store`; cache only in the current browser session.
- Crosshair movement must never switch inspector tabs or rerender the full workspace.
- A missing snapshot must not display `stock not found`, a retry card, or a chart failure.
- Deploy only through GitHub Actions.

---

### Task 1: Pure Chip Profile Geometry

**Files:**
- Create: `web/dashboard/js/chip-profile.js`
- Create: `web/dashboard/tests/chip-profile.test.mjs`

**Interfaces:**
- Consumes: dashboard chip payload fields `distribution`, `currentPrice`, `dominantPeakPrice`, `resolvedDate`, `averageCost`, and `winnerRate`.
- Produces: `chipProfileRows(snapshot, priceToCoordinate, chartWidth, chartHeight)` and `chipProfileSummary(snapshot)`.

- [ ] **Step 1: Write failing geometry tests**

```js
test("chip profile maps prices to bounded right-aligned rows", () => {
  const rows = chipProfileRows(snapshot, (price) => 300 - price * 10, 1000, 500);
  assert.equal(rows.length, 3);
  assert.equal(Math.max(...rows.map((row) => row.width)), 140);
  assert.equal(rows.find((row) => row.price === 11).tone, "loss");
  assert.equal(rows.find((row) => row.price === 9).tone, "profit");
});

test("chip profile drops invalid and off-pane buckets and marks the dominant peak", () => {
  const rows = chipProfileRows({
    ...snapshot,
    dominantPeakPrice: 10,
    distribution: [...snapshot.distribution, { price: -1, weight: 1 }],
  }, (price) => price === 11 ? -2 : 100, 600, 300);
  assert.equal(rows.length, 2);
  assert.equal(rows.find((row) => row.price === 10).dominant, true);
});
```

- [ ] **Step 2: Run the test and verify the missing module failure**

Run: `cd web/dashboard && node --test tests/chip-profile.test.mjs`

Expected: FAIL with `ERR_MODULE_NOT_FOUND` for `js/chip-profile.js`.

- [ ] **Step 3: Implement bounded normalized geometry and summary formatting**

```js
export const MAX_CHIP_PROFILE_ROWS = 60;

export function chipProfileRows(snapshot, priceToCoordinate, chartWidth, chartHeight) {
  const current = Number(snapshot?.currentPrice);
  const dominant = Number(snapshot?.dominantPeakPrice);
  const maxWidth = Math.min(180, Math.max(110, Number(chartWidth) * 0.14));
  const buckets = (Array.isArray(snapshot?.distribution) ? snapshot.distribution : [])
    .map((bucket) => ({ price: Number(bucket?.price), weight: Number(bucket?.weight) }))
    .filter(({ price, weight }) => Number.isFinite(price) && price > 0 && Number.isFinite(weight) && weight >= 0)
    .slice(0, MAX_CHIP_PROFILE_ROWS);
  const peak = Math.max(...buckets.map(({ weight }) => weight), 0);
  return buckets.flatMap(({ price, weight }) => {
    const y = Number(priceToCoordinate(price));
    if (!Number.isFinite(y) || y < 0 || y > chartHeight) return [];
    return [{
      price, weight, y,
      width: peak > 0 ? weight / peak * maxWidth : 0,
      tone: Number.isFinite(current) && price <= current ? "profit" : "loss",
      dominant: Number.isFinite(dominant) && Math.abs(price - dominant) < 0.000001,
    }];
  });
}
```

- [ ] **Step 4: Run focused tests**

Run: `cd web/dashboard && node --test tests/chip-profile.test.mjs`

Expected: PASS.

- [ ] **Step 5: Commit the pure module**

```bash
git add web/dashboard/js/chip-profile.js web/dashboard/tests/chip-profile.test.mjs
git commit -m "feat: add chip profile geometry"
```

### Task 2: Lightweight Charts Overlay Lifecycle

**Files:**
- Modify: `web/dashboard/js/chart.js`
- Modify: `web/dashboard/tests/chart.test.mjs`
- Modify: `web/dashboard/css/dashboard.css`

**Interfaces:**
- Consumes: `chipProfileRows` and `chipProfileSummary` from Task 1.
- Produces: `mountChart(..., { onChipDateChange })` returning `setChipProfile(snapshot, state)`, `resize()`, and `destroy()`.

- [ ] **Step 1: Replace click-only tests with crosshair lifecycle tests**

```js
test("mounted chart emits each crosshair candle once, restores latest on leave, and cleans up", () => {
  const selected = [];
  const handle = mountChart(container, bars, [], {
    onChipDateChange: (date) => selected.push(date),
  });
  crosshairHandler(candleParam("2026-07-17"));
  crosshairHandler(candleParam("2026-07-17"));
  crosshairHandler(candleParam("2026-07-18"));
  crosshairHandler({ point: undefined });
  assert.deepEqual(selected, ["2026-07-17", "2026-07-18", null]);
  handle.destroy();
  assert.equal(unsubscribedCrosshair, crosshairHandler);
});
```

- [ ] **Step 2: Run the chart test and verify it fails**

Run: `cd web/dashboard && node --test tests/chart.test.mjs`

Expected: FAIL because `mountChart` does not subscribe to crosshair movement or expose `setChipProfile`.

- [ ] **Step 3: Mount an SVG overlay and subscribe to chart changes**

```js
const overlay = document.createElement("div");
overlay.className = "chip-profile-overlay";
overlay.innerHTML = '<div class="chip-profile-meta"></div><svg aria-label="筹码峰"></svg>';
container.append(overlay);

let lastChipDate;
const handleCrosshair = (param) => {
  const date = param?.point && param.seriesData?.has?.(candleSeries)
    ? selectedChipDate(param)
    : null;
  if (date === lastChipDate) return;
  lastChipDate = date;
  onChipDateChange?.(date);
};
chart.subscribeCrosshairMove(handleCrosshair);
```

Implement one animation-frame-scheduled redraw using `candleSeries.priceToCoordinate`. Invoke it from `setChipProfile`, resize, visible logical range changes, wheel, and pointer-up. Destroy must remove every subscription, listener, timer, and overlay.

- [ ] **Step 4: Add non-intercepting adaptive overlay styles**

```css
.chip-profile-overlay { position:absolute; inset:28px 58px 26px auto; width:clamp(110px,14%,180px); pointer-events:none; z-index:3; }
.chip-profile-overlay svg { width:100%; height:100%; overflow:visible; }
.chip-profile-row.profit { stroke:rgba(239,83,80,.68); }
.chip-profile-row.loss { stroke:rgba(38,166,154,.68); }
.chip-profile-row.dominant { stroke-width:3; filter:brightness(1.35); }
```

- [ ] **Step 5: Run chart and asset tests**

Run: `cd web/dashboard && npm test && node scripts/check-assets.mjs`

Expected: PASS.

- [ ] **Step 6: Commit chart integration**

```bash
git add web/dashboard/js/chart.js web/dashboard/tests/chart.test.mjs web/dashboard/css/dashboard.css
git commit -m "feat: align chip profile with price chart"
```

### Task 3: Debounced Fetching and Quiet Optional State

**Files:**
- Modify: `web/dashboard/js/api.js`
- Modify: `web/dashboard/js/app.js`
- Modify: `web/dashboard/js/company-panels.js`
- Modify: `web/dashboard/tests/api.test.mjs`
- Modify: `web/dashboard/tests/company-panels.test.mjs`
- Modify: `web/dashboard/tests/browser/dashboard.spec.mjs`
- Modify: `web/dashboard/index.html`

**Interfaces:**
- Consumes: `chartHandle.setChipProfile(payload, state)` and `onChipDateChange(date)` from Task 2.
- Produces: per-session cache keyed by `code + requested date`, a 200ms debounce, abort/stale suppression, latest restoration, and a local visibility preference.

- [ ] **Step 1: Write failing browser coverage for automatic switching**

```js
test("chip profile loads latest, follows a debounced crosshair, and restores latest on leave", async ({ page }) => {
  const requests = [];
  await mockApi(page, true, hits, [], { chips: { requested: ({ url }) => requests.push(url.searchParams.get("date")) } });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await expect(page.locator(".chip-profile-overlay")).toHaveAttribute("data-state", "ready");
  expect(requests).toEqual([null]);
  await page.locator("#stock-chart").hover({ position: { x: 350, y: 180 } });
  await expect.poll(() => requests.length).toBe(2);
  expect(requests[1]).toMatch(/^2026-/);
  await page.mouse.move(10, 10);
  await expect.poll(() => requests.at(-1)).toBe(null);
  await expect(page.getByRole("tab", { name: "Chips" })).toHaveCount(0);
});
```

Add cases for rapid stale responses, same-candle deduplication, weekly/monthly dates, 404 quiet state, sign-out cleanup, and inspector resizing.

- [ ] **Step 2: Run browser tests and verify the old sidebar behavior fails**

Run: `cd web/dashboard && npx playwright test tests/browser/dashboard.spec.mjs`

Expected: FAIL because latest is lazy, crosshair is silent, and the Chips tab still exists.

- [ ] **Step 3: Allow request cancellation and add chart-profile state**

```js
chips: (code, date = null, { signal } = {}) => {
  const suffix = date === null ? "" : `?date=${encodeURIComponent(date)}`;
  return request(`/api/dashboard/stocks/${encodeURIComponent(code)}/chips${suffix}`, { signal });
},
```

In `app.js`, add `chipProfileCache`, one active `AbortController`, a request generation, and one debounce timer. `onChipDateChange(date)` clears the old timer and schedules `loadChipProfile(code, date)` after 200ms; `null` restores latest. A 404 calls `setChipProfile(null, "pending")`; authentication errors still cross the existing auth boundary.

- [ ] **Step 4: Remove obsolete sidebar rendering**

Remove the Chips inspector tab, `chipPanel` import/export, list CSS, click-to-open behavior, and obsolete list tests. Add a compact chart legend button with `aria-pressed`, persist `qbot.dashboard.chip-profile-visible`, and call `chartHandle.setChipProfileVisible` without rerendering.

- [ ] **Step 5: Bump dashboard asset versions consistently**

Change all dashboard entry/module query strings from `v=20260719.7` to the shared value `v=20260720.1`, including `index.html` and JavaScript imports.

- [ ] **Step 6: Run the full frontend suite**

Run: `cd web/dashboard && npm test && npx playwright test && node scripts/check-assets.mjs`

Expected: all unit, browser, and asset tests PASS.

- [ ] **Step 7: Commit automatic profile loading**

```bash
git add web/dashboard
git commit -m "feat: auto-switch chart chip profiles"
```

### Task 4: Frontend Regression Verification

**Files:**
- Test only; no production file changes expected.

**Interfaces:**
- Consumes: completed Tasks 1-3.
- Produces: verified dashboard frontend ready to pair with independent data repair.

- [ ] **Step 1: Run all frontend checks from a clean install state**

Run: `cd web/dashboard && npm ci && npm test && npx playwright test && node scripts/check-assets.mjs`

Expected: every unit and browser test passes and the asset checker reports no missing or stale assets.

- [ ] **Step 2: Confirm Telegram files are untouched**

Run: `git diff 3a52436 -- web/miniapp src/telegram.rs src/api/routes.rs`

Expected: no output.

- [ ] **Step 3: Record any test-only corrections and commit**

```bash
git add web/dashboard
git diff --cached --quiet || git commit -m "test: cover chart chip profile interactions"
```
