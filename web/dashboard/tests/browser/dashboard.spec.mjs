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

async function mockApi(page, initiallyAuthenticated = true) {
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
    if (url.pathname.endsWith("/bootstrap")) return route.fulfill({ status: 200, json: bootstrap });
    return route.fulfill({ status: 200, json: {
      runId: bootstrap.runId,
      code: "600519",
      name: "贵州茅台",
      period: url.searchParams.get("period") || "daily",
      partial: false,
      latest: bars.at(-1),
      bars,
      hits: bootstrap.results[0].hits,
    } });
  });
}

test("login, filter, stock chart, periods, and logout", async ({ page }) => {
  await mockApi(page, false);
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
  await page.getByRole("button", { name: "W" }).click();
  await expect(page.getByRole("button", { name: "W" })).toHaveClass(/active/);
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
