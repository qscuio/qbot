import { test, expect } from "@playwright/test";

const bootstrap = {
  serverTime: "2026-07-18T16:00:00+08:00",
  marketOpen: false,
  runId: "d138a95e-6ac0-41e7-9a2e-4ee97d01af4d",
  scannedAt: "2026-07-18T15:15:00+08:00",
  freshness: "fresh",
  summary: { uniqueStocks: 2, totalHits: 3, activeSignals: 3, rankedCandidates: 1 },
  catalog: [
    { id: "volume_surge", name: "Volume surge", icon: "V", group: "volume", isRankedPool: false },
    { id: "ma_bullish", name: "MA bullish", icon: "M", group: "trend", isRankedPool: false },
    { id: "top20", name: "Top 20", icon: "20", group: "ranked_pool", isRankedPool: true },
  ],
  results: [
    { code: "600519", name: "贵州茅台", close: 1488.5, changePct: 1.25, tradeDate: "2026-07-18", partial: false, hits: [
      { signalId: "volume_surge", name: "Volume surge", icon: "V", group: "volume", isRankedPool: false, metadata: { ratio: 2.1 } },
      { signalId: "top20", name: "Top 20", icon: "20", group: "ranked_pool", isRankedPool: true, metadata: { rank: 4 } },
    ] },
    { code: "000001", name: "平安银行", close: 12.2, changePct: -0.4, tradeDate: "2026-07-18", partial: false, hits: [
      { signalId: "ma_bullish", name: "MA bullish", icon: "M", group: "trend", isRankedPool: false, metadata: {} },
    ] },
  ],
};

const bars = Array.from({ length: 90 }, (_, index) => {
  const close = 1400 + index;
  const date = new Date(Date.UTC(2026, 0, 1 + index)).toISOString().slice(0, 10);
  return { time: date, open: close - 3, high: close + 8, low: close - 10, close, volume: 1000000 + index * 1000, amount: 1 };
});

async function mockApi(page, initiallyAuthenticated = true, stockHits = bootstrap.results[0].hits, requestedPeriods = [], gates = {}) {
  let authenticated = initiallyAuthenticated;
  let bootstrapRequests = 0;
  await page.route("**/api/dashboard/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/auth/session")) {
      return route.fulfill({ status: authenticated ? 200 : 401, json: authenticated ? { authenticated: true } : { error: "unauthorized" } });
    }
    if (url.pathname.endsWith("/auth/login")) {
      authenticated = true;
      return route.fulfill({ status: 200, json: { authenticated: true } });
    }
    if (url.pathname.endsWith("/auth/logout")) return route.fulfill({ status: 204, body: "" });
    if (!authenticated) return route.fulfill({ status: 401, json: { error: "unauthorized" } });
    if (url.pathname.endsWith("/bootstrap")) {
      bootstrapRequests += 1;
      if (gates.bootstrap && bootstrapRequests > (gates.bootstrap.afterRequests ?? 0)) {
        gates.bootstrap.requested?.();
        await gates.bootstrap.release;
      }
      return route.fulfill({
        status: 200,
        json: {
          ...bootstrap,
          results: bootstrap.results.map((row) => row.code === "600519" ? { ...row, hits: stockHits } : row),
        },
      });
    }
    const period = url.searchParams.get("period") || "daily";
    requestedPeriods.push(period);
    if (gates.stock) {
      gates.stock.requested?.();
      await gates.stock.release;
    }
    const periodBars = period === "daily"
      ? bars
      : bars.filter((_, index) => (index + 1) % (period === "weekly" ? 5 : 20) === 0);
    return route.fulfill({ status: 200, json: {
      runId: bootstrap.runId,
      code: "600519",
      name: "贵州茅台",
      period,
      partial: false,
      latest: periodBars.at(-1),
      bars: periodBars,
      hits: stockHits,
    } });
  });
}

async function installLifecycleProbe(page) {
  await page.addInitScript(() => {
    const NativeResizeObserver = window.ResizeObserver;
    const nativeAddEventListener = window.addEventListener;
    const nativeRemoveEventListener = window.removeEventListener;
    const resizeListeners = new Set();
    window.__dashboardLifecycleProbe = { callbacks: 0, disconnects: 0, observes: 0, resizeListeners };
    window.ResizeObserver = class extends NativeResizeObserver {
      constructor(callback) {
        super((...args) => {
          window.__dashboardLifecycleProbe.callbacks += 1;
          callback(...args);
        });
      }
      observe(...args) {
        window.__dashboardLifecycleProbe.observes += 1;
        return super.observe(...args);
      }
      disconnect() {
        window.__dashboardLifecycleProbe.disconnects += 1;
        return super.disconnect();
      }
    };
    window.addEventListener = function addEventListener(type, listener, options) {
      if (type === "resize") resizeListeners.add(listener);
      return nativeAddEventListener.call(this, type, listener, options);
    };
    window.removeEventListener = function removeEventListener(type, listener, options) {
      if (type === "resize") resizeListeners.delete(listener);
      return nativeRemoveEventListener.call(this, type, listener, options);
    };
  });
}

test("login, filter, stock chart, periods, and logout", async ({ page }) => {
  const requestedPeriods = [];
  await mockApi(page, false, bootstrap.results[0].hits, requestedPeriods);
  await page.goto("/dashboard/");
  await expect(page.getByRole("heading", { name: "QBot Market Intelligence" })).toBeVisible();
  await page.getByLabel("Username").fill("analyst");
  await page.getByLabel("Password").fill("secret");
  await page.getByRole("button", { name: "Sign in" }).click();

  await expect(page.getByRole("heading", { name: "Latest scan" })).toBeVisible();
  await expect(page.locator("tbody tr")).toHaveCount(2);
  await page.getByRole("button", { name: /^Filters/ }).click();
  await page.getByLabel("Stock search").fill("茅台");
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await page.locator("tbody tr").click();
  await expect(page.getByRole("heading", { name: /贵州茅台/ })).toBeVisible();
  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();
  await expect(page.locator(".chart-period")).toHaveText("Daily · 90 bars · 2 signals");
  await page.getByRole("button", { name: "Weekly" }).click();
  await expect(page.getByRole("button", { name: "Weekly" })).toHaveClass(/active/);
  await expect(page.locator(".chart-period")).toHaveText("Weekly · 18 bars · 2 signals");
  expect(requestedPeriods).toContain("weekly");
  await page.getByRole("button", { name: "Settings" }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
});

test("filter dropdown replaces the fixed sidebar", async ({ page }) => {
  await mockApi(page);
  await page.goto("/dashboard/");

  await expect(page.locator(".sidebar")).toHaveCount(0);
  await page.getByRole("button", { name: /^Filters/ }).click();
  await expect(page.locator("#filter-menu")).toBeVisible();
  await page.getByLabel("Stock search").fill("茅台");
  await page.getByLabel("Signal group").selectOption("trend");
  await expect(page.getByRole("button", { name: "Filters (2)" })).toBeVisible();

  await page.keyboard.press("Escape");
  await expect(page.locator("#filter-menu")).toBeHidden();
  await page.getByRole("button", { name: "Filters (2)" }).click();
  await expect(page.getByLabel("Stock search")).toHaveValue("茅台");
  await expect(page.getByLabel("Signal group")).toHaveValue("trend");
  await page.getByRole("heading", { name: "Latest scan" }).click();
  await expect(page.locator("#filter-menu")).toBeHidden();
  await page.getByRole("button", { name: "Filters (2)" }).click();
  await expect(page.getByLabel("Stock search")).toHaveValue("茅台");
  await expect(page.getByLabel("Signal group")).toHaveValue("trend");

  const summary = page.locator(".summary-strip");
  await expect(summary.getByText("Unique stocks")).toBeVisible();
  await expect(summary.getByText("Total hits")).toBeVisible();
  await expect(summary.getByText("Active signals")).toBeVisible();
  await expect(summary.getByText("Ranked candidates")).toBeVisible();
});

test("top chrome is consolidated into the left settings menu", async ({ page }) => {
  await mockApi(page);
  await page.goto("/dashboard/");

  await expect(page.locator(".titlebar")).toHaveCount(0);
  await page.getByRole("button", { name: "Settings" }).click();
  await expect(page.getByText("Market intelligence · read only")).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
});

test("stock workspace teardown disconnects chart and inspector resize resources on logout", async ({ page }) => {
  await installLifecycleProbe(page);
  await mockApi(page);
  await page.goto("/dashboard/");
  const baselineListeners = await page.evaluate(() => window.__dashboardLifecycleProbe.resizeListeners.size);
  await page.locator("tbody tr").first().click();
  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();
  const mounted = await page.evaluate(() => ({
    listeners: window.__dashboardLifecycleProbe.resizeListeners.size,
    observes: window.__dashboardLifecycleProbe.observes,
  }));
  expect(mounted.listeners).toBeGreaterThan(baselineListeners);
  expect(mounted.observes).toBeGreaterThan(0);

  await page.getByRole("button", { name: "Settings" }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  const afterLogout = await page.evaluate(() => ({
    callbacks: window.__dashboardLifecycleProbe.callbacks,
    disconnects: window.__dashboardLifecycleProbe.disconnects,
    listeners: window.__dashboardLifecycleProbe.resizeListeners.size,
  }));
  expect(afterLogout.disconnects).toBeGreaterThan(0);
  expect(afterLogout.listeners).toBe(baselineListeners);
  await page.setViewportSize({ width: 900, height: 500 });
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  expect(await page.evaluate(() => window.__dashboardLifecycleProbe.callbacks)).toBe(afterLogout.callbacks);
});

test("delayed stock response cannot remount a protected workspace after logout", async ({ page }) => {
  await installLifecycleProbe(page);
  let releaseStock;
  const stockRelease = new Promise((resolve) => { releaseStock = resolve; });
  let markStockRequested;
  const stockRequested = new Promise((resolve) => { markStockRequested = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    stock: { release: stockRelease, requested: markStockRequested },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await stockRequested;

  await page.getByRole("button", { name: "Settings" }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  const authLifecycle = await page.evaluate(() => ({
    listeners: window.__dashboardLifecycleProbe.resizeListeners.size,
    observes: window.__dashboardLifecycleProbe.observes,
  }));
  const stockResponse = page.waitForResponse((response) => response.url().includes("/api/dashboard/stocks/"));
  releaseStock();
  await stockResponse;
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));

  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  await expect(page.locator(".stock-inspector")).toHaveCount(0);
  await expect(page.locator("#stock-chart")).toHaveCount(0);
  expect(await page.evaluate(() => ({
    listeners: window.__dashboardLifecycleProbe.resizeListeners.size,
    observes: window.__dashboardLifecycleProbe.observes,
  }))).toEqual(authLifecycle);
});

test("delayed quiet bootstrap cannot replace the auth root after logout", async ({ page }) => {
  await installLifecycleProbe(page);
  let releaseBootstrap;
  const bootstrapRelease = new Promise((resolve) => { releaseBootstrap = resolve; });
  let markBootstrapRequested;
  const bootstrapRequested = new Promise((resolve) => { markBootstrapRequested = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    bootstrap: { afterRequests: 1, release: bootstrapRelease, requested: markBootstrapRequested },
  });
  await page.goto("/dashboard/");
  await page.getByRole("button", { name: "Refresh" }).click();
  await bootstrapRequested;

  await page.getByRole("button", { name: "Settings" }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  const authLifecycle = await page.evaluate(() => ({
    listeners: window.__dashboardLifecycleProbe.resizeListeners.size,
    observes: window.__dashboardLifecycleProbe.observes,
  }));
  const bootstrapResponse = page.waitForResponse((response) => response.url().endsWith("/api/dashboard/bootstrap"));
  releaseBootstrap();
  await bootstrapResponse;
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));

  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  await expect(page.locator(".shell")).toHaveCount(0);
  expect(await page.evaluate(() => ({
    listeners: window.__dashboardLifecycleProbe.resizeListeners.size,
    observes: window.__dashboardLifecycleProbe.observes,
  }))).toEqual(authLifecycle);
});

test("stock detail fills the viewport without an evidence sidebar", async ({ page }) => {
  await page.setViewportSize({ width: 1883, height: 937 });
  const denseHits = Array.from({ length: 10 }, (_, index) => ({
    ...bootstrap.results[0].hits[index % bootstrap.results[0].hits.length],
    signalId: `signal-${index}`,
    name: `Signal ${index + 1}`,
  }));
  await mockApi(page, true, denseHits);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await expect(page.locator(".chart-activity")).toHaveText("VOL");

  const layout = await page.evaluate(() => {
    const root = document.scrollingElement;
    const editor = document.querySelector(".editor-body");
    return {
      page: { client: root.clientHeight, scroll: root.scrollHeight },
      editor: { client: editor.clientHeight, scroll: editor.scrollHeight },
    };
  });

  expect(layout.page.scroll).toBe(layout.page.client);
  expect(layout.editor.scroll).toBe(layout.editor.client);
  await expect(page.locator(".evidence-pane")).toHaveCount(0);
  await expect(page.locator(".statusbar")).toHaveCSS("background-color", "rgb(24, 24, 24)");
});

test("resizable stock information sidebar persists its desktop layout", async ({ page }) => {
  await page.setViewportSize({ width: 1200, height: 800 });
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();

  const inspector = page.locator(".stock-inspector");
  const workspace = page.locator(".stock-workspace");
  const resizer = page.locator(".inspector-resizer");
  await expect(inspector).toHaveCSS("width", "380px");
  await expect(page.locator("[data-inspector-tab]")).toHaveCount(4);
  await expect(page.locator('[data-inspector-panel="overview"]')).toContainText("1,489.00");

  const chartSurface = page.locator("#stock-chart > div").first();
  const initialChartWidth = (await chartSurface.boundingBox()).width;
  const divider = await resizer.boundingBox();
  await page.mouse.move(divider.x + divider.width / 2, divider.y + 40);
  await page.mouse.down();
  await page.mouse.move(-100, divider.y + 40, { steps: 3 });
  await page.mouse.up();
  await expect(inspector).toHaveCSS("width", "600px");
  expect((await chartSurface.boundingBox()).width).toBeLessThan(initialChartWidth);
  await page.reload();
  await expect(inspector).toHaveCSS("width", "600px");

  const maxDivider = await resizer.boundingBox();
  await page.mouse.move(maxDivider.x + maxDivider.width / 2, maxDivider.y + 40);
  await page.mouse.down();
  await page.mouse.move(1190, maxDivider.y + 40, { steps: 3 });
  await page.mouse.up();
  await expect(inspector).toHaveCSS("width", "300px");

  await resizer.dblclick();
  await expect(inspector).toHaveCSS("width", "380px");
  await expect(resizer).toHaveAttribute("role", "separator");
  await resizer.focus();
  await page.keyboard.press("ArrowLeft");
  await expect(inspector).toHaveCSS("width", "400px");
  await page.keyboard.press("ArrowRight");
  await expect(inspector).toHaveCSS("width", "380px");

  const overviewTab = page.locator('[data-inspector-tab="overview"]');
  const financialsTab = page.locator('[data-inspector-tab="financials"]');
  await expect(overviewTab).toHaveAttribute("tabindex", "0");
  await expect(financialsTab).toHaveAttribute("tabindex", "-1");
  await overviewTab.focus();
  await page.keyboard.press("ArrowRight");
  await expect(financialsTab).toBeFocused();
  await expect(financialsTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator('[data-inspector-panel="financials"]')).toBeVisible();

  await page.locator(".inspector-toggle").click();
  await expect(workspace).toHaveClass(/inspector-collapsed/);
  await page.reload();
  await expect(workspace).toHaveClass(/inspector-collapsed/);
  await page.locator(".inspector-toggle").click();
  await expect(workspace).not.toHaveClass(/inspector-collapsed/);
  await page.reload();
  await expect(workspace).not.toHaveClass(/inspector-collapsed/);
  await expect(inspector).toHaveCSS("width", "380px");

  const overflow = await page.evaluate(() => ({
    width: [document.documentElement.clientWidth, document.documentElement.scrollWidth],
    height: [document.documentElement.clientHeight, document.documentElement.scrollHeight],
  }));
  expect(overflow.width[1]).toBe(overflow.width[0]);
  expect(overflow.height[1]).toBe(overflow.height[0]);
});

test("resizable stock information sidebar reclamps and resizes its chart on live viewport changes", async ({ page }) => {
  await page.setViewportSize({ width: 1200, height: 800 });
  await page.addInitScript(() => localStorage.setItem("qbot.dashboard.inspector.v1", JSON.stringify({ width: 600, collapsed: false })));
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();

  const inspector = page.locator(".stock-inspector");
  await expect(inspector).toHaveCSS("width", "600px");
  const initialSurfaceWidth = (await page.locator("#stock-chart > div").boundingBox()).width;
  await page.setViewportSize({ width: 900, height: 700 });
  await expect.soft(inspector).toHaveCSS("width", "450px");
  await expect.poll(() => page.evaluate(() => ({
    host: document.querySelector("#stock-chart").getBoundingClientRect().width,
    surface: document.querySelector("#stock-chart > div").getBoundingClientRect().width,
  })).then(({ host, surface }) => ({ matches: Math.abs(host - surface) <= 1, shrank: surface < initialSurfaceWidth }))).toEqual({ matches: true, shrank: true });
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem("qbot.dashboard.inspector.v1")).width)).toBe(600);

  await page.setViewportSize({ width: 1400, height: 800 });
  await expect(inspector).toHaveCSS("width", "600px");
  await expect.poll(() => page.evaluate(() => {
    const host = document.querySelector("#stock-chart").getBoundingClientRect().width;
    const surface = document.querySelector("#stock-chart > div").getBoundingClientRect().width;
    return Math.abs(host - surface);
  })).toBeLessThanOrEqual(1);
});

test("mobile inspector actions preserve the preferred desktop width", async ({ page }) => {
  await page.setViewportSize({ width: 680, height: 420 });
  await page.addInitScript(() => {
    if (!localStorage.getItem("qbot.dashboard.inspector.v1")) {
      localStorage.setItem("qbot.dashboard.inspector.v1", JSON.stringify({ width: 520, collapsed: false }));
    }
  });
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();

  await expect(page.locator(".stock-inspector")).toHaveCSS("width", "340px");
  await page.locator(".inspector-toggle").click();
  await page.locator(".inspector-toggle").click();
  await page.keyboard.press("Escape");
  await expect(page.locator(".stock-workspace")).toHaveClass(/inspector-collapsed/);
  await page.setViewportSize({ width: 1200, height: 800 });
  await page.reload();
  await expect(page.locator(".stock-inspector")).toHaveCSS("width", "520px");
});

test("resizable stock information sidebar cleans up lost pointer capture", async ({ page }) => {
  await page.setViewportSize({ width: 1200, height: 800 });
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();

  const resizer = page.locator(".inspector-resizer");
  const divider = await resizer.boundingBox();
  await page.mouse.move(divider.x + divider.width / 2, divider.y + 40);
  await page.mouse.down();
  await expect(resizer).toHaveClass(/dragging/);
  await resizer.dispatchEvent("pointerup", { pointerId: 999 });
  await expect(resizer).toHaveClass(/dragging/);
  await resizer.dispatchEvent("lostpointercapture", { pointerId: 1 });
  await expect(resizer).not.toHaveClass(/dragging/);
  await page.mouse.up();
});

test("resizable stock information sidebar becomes a narrow overlay drawer", async ({ page }) => {
  await page.setViewportSize({ width: 680, height: 420 });
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();

  const workspace = page.locator(".stock-workspace");
  const inspector = page.locator(".stock-inspector");
  const chartPane = page.locator(".chart-pane");
  const openChartWidth = (await chartPane.boundingBox()).width;
  expect((await chartPane.boundingBox()).height).toBeLessThanOrEqual((await workspace.boundingBox()).height);
  expect((await page.locator("#stock-chart").boundingBox()).height).toBeLessThanOrEqual((await chartPane.boundingBox()).height - 28);
  const verticalLayout = await page.evaluate(() => {
    const editor = document.querySelector(".editor-body");
    const stockWorkspace = document.querySelector(".stock-workspace");
    return {
      editor: { client: editor.clientHeight, scroll: editor.scrollHeight },
      workspaceBottom: stockWorkspace.getBoundingClientRect().bottom,
      editorBottom: editor.getBoundingClientRect().bottom,
    };
  });
  expect(verticalLayout.editor.scroll).toBe(verticalLayout.editor.client);
  expect(verticalLayout.workspaceBottom).toBeLessThanOrEqual(verticalLayout.editorBottom);
  await expect(inspector).toBeVisible();
  await expect(inspector).toHaveCSS("position", "absolute");
  await expect(page.locator(".inspector-resizer")).toBeHidden();

  const toggle = page.locator(".inspector-toggle");
  await toggle.click();
  await expect(workspace).toHaveClass(/inspector-collapsed/);
  await expect(inspector).toBeHidden();
  expect((await chartPane.boundingBox()).width).toBe(openChartWidth);

  await toggle.click();
  await expect(inspector).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(workspace).toHaveClass(/inspector-collapsed/);
  await expect(toggle).toBeFocused();

  const overflow = await page.evaluate(() => ({
    width: [document.documentElement.clientWidth, document.documentElement.scrollWidth],
    height: [document.documentElement.clientHeight, document.documentElement.scrollHeight],
  }));
  expect(overflow.width[1]).toBe(overflow.width[0]);
  expect(overflow.height[1]).toBe(overflow.height[0]);
});

test("stock chart fits a short non-mobile landscape workspace", async ({ page }) => {
  await page.setViewportSize({ width: 800, height: 420 });
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();

  await expect(page.locator(".inspector-resizer")).toBeVisible();
  await expect(page.locator(".stock-inspector")).toHaveCSS("position", "static");
  const layout = await page.evaluate(() => {
    const root = document.scrollingElement;
    const editor = document.querySelector(".editor-body");
    const workspace = document.querySelector(".stock-workspace");
    const pane = document.querySelector(".chart-pane");
    const chart = document.querySelector("#stock-chart");
    return {
      document: { client: root.clientHeight, scroll: root.scrollHeight },
      editor: { client: editor.clientHeight, scroll: editor.scrollHeight },
      heights: {
        workspace: workspace.getBoundingClientRect().height,
        pane: pane.getBoundingClientRect().height,
        chart: chart.getBoundingClientRect().height,
      },
    };
  });
  expect(layout.heights.pane).toBeLessThanOrEqual(layout.heights.workspace);
  expect(layout.heights.chart).toBeLessThanOrEqual(layout.heights.pane - 28);
  expect(layout.document.scroll).toBe(layout.document.client);
  expect(layout.editor.scroll).toBe(layout.editor.client);
});

test("browser back and forward restore scan and stock pages", async ({ page }) => {
  await mockApi(page);
  await page.goto("/dashboard/");

  await page.locator("tbody tr").nth(0).click();
  await expect(page).toHaveURL(/#stock\/600519$/);
  await page.getByRole("tab", { name: /Latest scan/ }).click();
  await expect(page).toHaveURL(/#scan$/);
  await page.locator("tbody tr").nth(1).click();
  await expect(page).toHaveURL(/#stock\/000001$/);

  await page.goBack();
  await expect(page.getByRole("heading", { name: "Latest scan" })).toBeVisible();
  await page.goBack();
  await expect(page).toHaveURL(/#stock\/600519$/);
  await page.goForward();
  await expect(page.getByRole("heading", { name: "Latest scan" })).toBeVisible();
  await page.goForward();
  await expect(page).toHaveURL(/#stock\/000001$/);
});

test("stale stock links return to the scan without showing a chart error", async ({ page }) => {
  const stockRequests = [];
  await mockApi(page, true, bootstrap.results[0].hits, stockRequests);

  await page.goto("/dashboard/#stock/000618.SZ");

  await expect(page).toHaveURL(/#scan$/);
  await expect(page.getByRole("heading", { name: "Latest scan" })).toBeVisible();
  await expect(page.getByText("Chart unavailable")).toHaveCount(0);
  expect(stockRequests).toEqual([]);
});
