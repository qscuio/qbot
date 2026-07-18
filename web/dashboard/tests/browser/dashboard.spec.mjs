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

async function mockApi(page, initiallyAuthenticated = true, stockHits = bootstrap.results[0].hits, requestedPeriods = []) {
  let authenticated = initiallyAuthenticated;
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
    if (url.pathname.endsWith("/bootstrap")) return route.fulfill({
      status: 200,
      json: {
        ...bootstrap,
        results: bootstrap.results.map((row) => row.code === "600519" ? { ...row, hits: stockHits } : row),
      },
    });
    const period = url.searchParams.get("period") || "daily";
    requestedPeriods.push(period);
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

test("narrow layout exposes the filter drawer", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.getByRole("button", { name: "Filters", exact: true }).click();
  await expect(page.locator("#sidebar")).toHaveClass(/open/);
  await expect(page.getByLabel("Stock search")).toBeVisible();
});

test("top chrome is consolidated into the left settings menu", async ({ page }) => {
  await mockApi(page);
  await page.goto("/dashboard/");

  await expect(page.locator(".titlebar")).toHaveCount(0);
  await page.getByRole("button", { name: "Settings" }).click();
  await expect(page.getByText("Market intelligence · read only")).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
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
