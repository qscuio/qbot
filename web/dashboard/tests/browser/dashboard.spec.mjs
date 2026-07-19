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

const company = {
  code: "600519.SH",
  name: "贵州茅台",
  industry: "白酒",
  market: "主板",
  exchange: "SSE",
  listDate: "2001-08-27",
  quote: { tradeDate: "2026-07-17", close: "1488.50", volume: 123456, amount: "321000000" },
  valuation: { pe: "22.1", pb: "7.2", ps: "12.4", totalMarketValue: "1862400000000" },
};

const annualFinancials = {
  items: [{ endDate: "2025-12-31", announcementDate: "2026-03-31", frequency: "annual", reportType: "1", revenue: "120000000000", netProfitParent: "86240000000", deductedNetProfit: "85000000000", revenueYoy: "12.3", netProfitYoy: "14.2", basicEps: "3.21", roe: "31.2", grossMargin: "91.2", revisionCount: 2 }],
  nextCursor: null,
};

const dividendFirstPage = {
  items: [{ announcementDate: "2026-04-01", recordDate: "2026-06-20", exDate: "2026-06-21", payDate: "2026-06-25", implementationStatus: "implemented", cashDividend: "2.76", stockRatio: "0", revisionCount: 1 }],
  nextCursor: "older page",
};

const chipSnapshot = {
  code: "600519.SH",
  requestedDate: "2026-02-15",
  resolvedDate: "2026-02-13",
  currentPrice: 1488.5,
  distribution: [
    { price: 1500, weight: 0.6, percentage: 60 },
    { price: 1400, weight: 0.4, percentage: 40 },
  ],
  averageCost: 1460,
  winnerRate: 70,
  concentration: 82,
  dominantPeakPrice: 1500,
  source: "qbot_estimate",
  sourceLabel: "QBot 估算",
  modelVersion: "qbot-chip-v2",
  validated: true,
  validationLabel: "已验证",
  sourceUpdatedAt: "2026-07-18T10:00:00Z",
};

async function mockApi(page, initiallyAuthenticated = true, stockHits = bootstrap.results[0].hits, requestedPeriods = [], gates = {}) {
  let authenticated = initiallyAuthenticated;
  let sessionRequests = 0;
  let bootstrapRequests = 0;
  let stockRequests = 0;
  const optionalRequests = { company: 0, financials: 0, dividends: 0, chips: 0 };
  await page.route("**/api/dashboard/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/auth/session")) {
      sessionRequests += 1;
      gates.session?.requested?.(sessionRequests);
      if (sessionRequests <= (gates.session?.failRequests ?? 0)) {
        return route.fulfill({ status: 500, json: { error: "temporary session failure" } });
      }
      return route.fulfill({ status: authenticated ? 200 : 401, json: authenticated ? { authenticated: true } : { error: "unauthorized" } });
    }
    if (url.pathname.endsWith("/auth/login")) {
      authenticated = true;
      return route.fulfill({ status: 200, json: { authenticated: true } });
    }
    if (url.pathname.endsWith("/auth/logout")) {
      if (gates.logout) {
        gates.logout.requested?.();
        await gates.logout.release;
      }
      authenticated = false;
      return route.fulfill({ status: 204, body: "" });
    }
    if (!authenticated) return route.fulfill({ status: 401, json: { error: "unauthorized" } });
    if (url.pathname.endsWith("/bootstrap")) {
      bootstrapRequests += 1;
      const response = gates.bootstrap?.responses?.[bootstrapRequests];
      if (response) {
        response.requested?.();
        if (response.release) await response.release;
      } else if (gates.bootstrap && bootstrapRequests > (gates.bootstrap.afterRequests ?? 0)) {
        gates.bootstrap.requested?.();
        await gates.bootstrap.release;
      }
      if (response?.status && response.status !== 200) {
        return route.fulfill({ status: response.status, json: response.payload ?? { error: "request failed" } });
      }
      const responseBootstrap = response?.payload ?? bootstrap;
      return route.fulfill({
        status: 200,
        json: {
          ...responseBootstrap,
          results: responseBootstrap.results.map((row) => row.code === "600519" ? { ...row, hits: stockHits } : row),
        },
      });
    }
    const optionalKind = url.pathname.endsWith("/company")
      ? "company"
      : url.pathname.endsWith("/financials")
        ? "financials"
        : url.pathname.endsWith("/dividends")
          ? "dividends"
          : url.pathname.endsWith("/chips") ? "chips" : null;
    if (optionalKind) {
      optionalRequests[optionalKind] += 1;
      const configuration = gates[optionalKind];
      const response = configuration?.responses?.[optionalRequests[optionalKind]];
      configuration?.requested?.({ request: optionalRequests[optionalKind], url });
      response?.requested?.({ request: optionalRequests[optionalKind], url });
      if (response?.release) await response.release;
      else if (configuration?.release) await configuration.release;
      if (response?.status && response.status !== 200) {
        return route.fulfill({ status: response.status, json: response.payload ?? { error: `${optionalKind} request failed` } });
      }
      const payload = response?.payload ?? (optionalKind === "company"
        ? company
        : optionalKind === "financials"
          ? annualFinancials
          : optionalKind === "dividends" ? dividendFirstPage : chipSnapshot);
      return route.fulfill({ status: 200, json: payload });
    }
    const period = url.searchParams.get("period") || "daily";
    requestedPeriods.push(period);
    stockRequests += 1;
    const stockResponse = gates.stock?.responses?.[stockRequests];
    if (stockResponse) {
      stockResponse.requested?.();
      if (stockResponse.release) await stockResponse.release;
    } else if (gates.stock) {
      gates.stock.requested?.();
      await gates.stock.release;
    }
    if (stockResponse?.status && stockResponse.status !== 200) {
      return route.fulfill({ status: stockResponse.status, json: stockResponse.payload ?? { error: "stock request failed" } });
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
      ...(stockResponse?.payload ?? {}),
    } });
  });
}

test("chips stay lazy, crosshair movement is silent, and Latest loads without a date", async ({ page }) => {
  const requests = [];
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    chips: { requested: ({ url }) => requests.push(url.search) },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();

  await page.locator("#stock-chart").hover({ position: { x: 300, y: 180 } });
  await page.mouse.move(500, 240, { steps: 8 });
  expect(requests).toEqual([]);

  await page.getByRole("tab", { name: "Chips" }).click();
  await expect(page.locator('[data-inspector-panel="chips"]')).toContainText("QBot 估算");
  expect(requests).toEqual([""]);
  await page.locator("[data-chip-latest]").click();
  await page.waitForTimeout(20);
  expect(requests).toEqual([""]);
});

test("candle click selects Chips once, resolves fallback, and Latest requests the newest snapshot", async ({ page }) => {
  const requests = [];
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    chips: { requested: ({ url }) => requests.push(url.searchParams.get("date")) },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  const chart = page.locator("#stock-chart");
  await expect(chart.locator("canvas").first()).toBeVisible();
  await chart.click({ position: { x: 380, y: 180 } });

  await expect(page.getByRole("tab", { name: "Chips" })).toHaveAttribute("aria-selected", "true");
  await expect(page.locator('[data-inspector-panel="chips"]')).toContainText("请求 2026-02-15");
  await expect(page.locator('[data-inspector-panel="chips"]')).toContainText("实际 2026-02-13");
  expect(requests).toHaveLength(1);
  expect(requests[0]).toMatch(/^2026-\d{2}-\d{2}$/);

  await page.locator("[data-chip-latest]").click();
  await expect.poll(() => requests.length).toBe(2);
  expect(requests[1]).toBeNull();
});

test("rapid chip selections keep the newest response and use weekly/monthly final candle dates", async ({ page }) => {
  const requests = [];
  let releaseFirst;
  const firstRelease = new Promise((resolve) => { releaseFirst = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    chips: {
      requested: ({ url }) => requests.push(url.searchParams.get("date")),
      responses: {
        1: { release: firstRelease, payload: { ...chipSnapshot, requestedDate: "older", resolvedDate: "older-response" } },
        2: { payload: { ...chipSnapshot, requestedDate: "newer", resolvedDate: "newer-response" } },
      },
    },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  const chart = page.locator("#stock-chart");
  await chart.click({ position: { x: 280, y: 180 } });
  await expect.poll(() => requests.length).toBe(1);
  await page.getByRole("button", { name: "Weekly" }).click();
  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();
  await page.locator("#stock-chart").click({ position: { x: 200, y: 180 } });
  await expect.poll(() => requests.length).toBe(2);
  expect(bars.filter((_, index) => (index + 1) % 5 === 0).map((bar) => bar.time)).toContain(requests[1]);
  await expect(page.locator('[data-inspector-panel="chips"]')).toContainText("newer-response");
  releaseFirst();
  await page.waitForTimeout(30);
  await expect(page.locator('[data-inspector-panel="chips"]')).toContainText("newer-response");
  await expect(page.locator('[data-inspector-panel="chips"]')).not.toContainText("older-response");

  await page.getByRole("button", { name: "Monthly" }).click();
  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();
  const before = requests.length;
  await page.locator("#stock-chart").click({ position: { x: 500, y: 180 } });
  await expect.poll(() => requests.length).toBe(before + 1);
  expect(bars.filter((_, index) => (index + 1) % 20 === 0).map((bar) => bar.time)).toContain(requests.at(-1));
});

test("a chip response from a signed-out generation cannot overwrite the next session", async ({ page }) => {
  let releaseOld;
  const oldRelease = new Promise((resolve) => { releaseOld = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    chips: { responses: {
      1: { release: oldRelease, payload: { ...chipSnapshot, resolvedDate: "old-session" } },
      2: { payload: { ...chipSnapshot, resolvedDate: "fresh-session" } },
    } },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();
  await page.getByRole("tab", { name: "Chips" }).click();
  await page.locator("#settings").click();
  await page.locator("#logout").click();
  await expect(page.locator("#login-form")).toBeVisible();
  releaseOld();
  await page.waitForTimeout(30);
  await expect(page.locator("#login-form")).toBeVisible();
  await page.locator("#username").fill("analyst");
  await page.locator("#password").fill("secret");
  await page.locator("#login-form button").click();
  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();
  await page.getByRole("tab", { name: "Chips" }).click();
  await expect(page.locator('[data-inspector-panel="chips"]')).toContainText("fresh-session");
  await expect(page.locator('[data-inspector-panel="chips"]')).not.toContainText("old-session");
});

test("company overview waits for chart mount while optional data stays local", async ({ page }) => {
  let releaseCompany;
  const companyRelease = new Promise((resolve) => { releaseCompany = resolve; });
  let markCompanyRequested;
  const companyRequested = new Promise((resolve) => { markCompanyRequested = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    company: { release: companyRelease, requested: markCompanyRequested },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await companyRequested;

  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();
  await expect(page.locator('[data-inspector-panel="overview"]')).toContainText("Loading overview");
  releaseCompany();
  await expect(page.locator('[data-inspector-panel="overview"]')).toContainText("贵州茅台");
  await expect(page.locator('[data-inspector-panel="overview"]')).toContainText("市盈率");
});

test("financials lazy-load once per frequency and ignore stale frequency responses", async ({ page }) => {
  const requests = [];
  let releaseAnnual;
  const annualRelease = new Promise((resolve) => { releaseAnnual = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    financials: {
      requested: ({ url }) => requests.push(url.searchParams.get("frequency")),
      responses: {
        1: { release: annualRelease, payload: annualFinancials },
        2: { payload: { items: [{ ...annualFinancials.items[0], endDate: "2026-03-31", frequency: "quarterly", netProfitParent: "21000000000" }], nextCursor: null } },
      },
    },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await page.getByRole("tab", { name: "Financials" }).click();
  await page.getByRole("button", { name: "季度" }).click();

  await expect(page.locator('[data-inspector-panel="financials"]')).toContainText("2026-03-31");
  releaseAnnual();
  await page.evaluate(() => new Promise((resolve) => setTimeout(resolve, 20)));
  await expect(page.locator('[data-inspector-panel="financials"]')).toContainText("2026-03-31");
  await expect(page.locator('[data-inspector-panel="financials"]')).not.toContainText("2025-12-31");
  await page.getByRole("button", { name: "年度" }).click();
  await expect(page.locator('[data-inspector-panel="financials"]')).toContainText("2025-12-31");
  expect(requests).toEqual(["annual", "quarterly"]);
});

test("dividend failure retries locally and pagination appends without duplicates", async ({ page }) => {
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    dividends: { responses: {
      1: { status: 500, payload: { error: "dividend source unavailable" } },
      2: { payload: dividendFirstPage },
      3: { payload: { items: [dividendFirstPage.items[0], { ...dividendFirstPage.items[0], announcementDate: "2025-04-01", exDate: "2025-06-21", cashDividend: "2.10" }], nextCursor: null } },
    } },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await page.getByRole("tab", { name: "Dividends" }).click();

  await expect(page.locator('[data-inspector-panel="dividends"]')).toContainText("dividend source unavailable");
  await page.locator('[data-inspector-panel="dividends"] [data-panel-retry]').click();
  await expect(page.locator('[data-inspector-panel="dividends"]')).toContainText("2.76");
  await page.locator('[data-inspector-panel="dividends"] [data-load-more="dividends"]').click();
  await expect(page.locator('[data-inspector-panel="dividends"]')).toContainText("2.10");
  await expect(page.locator('[data-inspector-panel="dividends"] .dividend-card')).toHaveCount(2);
});

test("active company tab follows stock navigation without stale panel writes", async ({ page }) => {
  const requestedCodes = [];
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    financials: { requested: ({ url }) => requestedCodes.push(url.pathname.split("/").at(-2)) },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await page.getByRole("tab", { name: "Financials" }).click();
  await expect.poll(() => requestedCodes.length).toBe(1);
  await page.getByRole("tab", { name: /Latest scan/ }).click();
  await page.locator("tbody tr").nth(1).click();

  await expect.poll(() => requestedCodes).toEqual(["600519", "000001"]);
  await expect(page.getByRole("tab", { name: "Financials" })).toHaveAttribute("aria-selected", "true");
});

test("an optional route 401 uses the protected session lifecycle", async ({ page }) => {
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    financials: { responses: { 1: { status: 401, payload: { error: "session expired" } } } },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await page.getByRole("tab", { name: "Financials" }).click();

  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  await expect(page.locator(".shell, #stock-chart, .stock-inspector")).toHaveCount(0);
});

test("old company response cannot cross logout and login generations", async ({ page }) => {
  let releaseOld;
  const oldRelease = new Promise((resolve) => { releaseOld = resolve; });
  let oldRequested;
  const requested = new Promise((resolve) => { oldRequested = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    company: { responses: {
      1: { release: oldRelease, requested: oldRequested, payload: { ...company, name: "Old generation" } },
      2: { payload: { ...company, name: "Fresh generation" } },
    } },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await requested;
  await page.getByRole("button", { name: "Settings" }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  await page.getByLabel("Username").fill("analyst");
  await page.getByLabel("Password").fill("secret");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.locator('[data-inspector-panel="overview"]')).toContainText("Fresh generation");

  releaseOld();
  await page.evaluate(() => new Promise((resolve) => setTimeout(resolve, 20)));
  await expect(page.locator('[data-inspector-panel="overview"]')).toContainText("Fresh generation");
  await expect(page.locator('[data-inspector-panel="overview"]')).not.toContainText("Old generation");
});

test("loaded company tables remain inside the viewport on desktop and narrow screens", async ({ page }) => {
  await page.setViewportSize({ width: 960, height: 420 });
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await page.getByRole("tab", { name: "Financials" }).click();
  await expect(page.locator(".financial-table")).toBeVisible();

  const desktop = await page.evaluate(() => ({
    width: [document.documentElement.clientWidth, document.documentElement.scrollWidth],
    height: [document.documentElement.clientHeight, document.documentElement.scrollHeight],
  }));
  expect(desktop.width[1]).toBe(desktop.width[0]);
  expect(desktop.height[1]).toBe(desktop.height[0]);
  await page.setViewportSize({ width: 680, height: 420 });
  const narrow = await page.evaluate(() => ({
    width: [document.documentElement.clientWidth, document.documentElement.scrollWidth],
    height: [document.documentElement.clientHeight, document.documentElement.scrollHeight],
  }));
  expect(narrow.width[1]).toBe(narrow.width[0]);
  expect(narrow.height[1]).toBe(narrow.height[0]);
});

test("empty stock history still loads overview and the active optional tab", async ({ page }) => {
  const companyCodes = [];
  const financialCodes = [];
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    stock: { responses: { 2: { payload: { bars: [], latest: null } } } },
    company: { requested: ({ url }) => companyCodes.push(url.pathname.split("/").at(-2)) },
    financials: { requested: ({ url }) => financialCodes.push(url.pathname.split("/").at(-2)) },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await page.getByRole("tab", { name: "Financials" }).click();
  await page.getByRole("tab", { name: /Latest scan/ }).click();
  await page.locator("tbody tr").nth(1).click();

  await expect(page.locator("#stock-chart")).toHaveCount(0);
  await expect(page.getByText("No usable chart history")).toBeVisible();
  await expect.poll(() => companyCodes).toEqual(["600519", "000001"]);
  await expect.poll(() => financialCodes).toEqual(["600519", "000001"]);
  await expect(page.locator('[data-inspector-panel="financials"]')).toContainText("2025-12-31");
});

test("virtual history shifts retain keyboard focus and clamp the last window", async ({ page }) => {
  const financialItems = Array.from({ length: 180 }, (_, index) => ({
    ...annualFinancials.items[0], endDate: `financial-${index}`, reportType: String(index),
  }));
  const dividendItems = Array.from({ length: 180 }, (_, index) => ({
    ...dividendFirstPage.items[0], announcementDate: `dividend-${index}`, exDate: `dividend-${index}`,
  }));
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    financials: { responses: { 1: { payload: { items: financialItems, nextCursor: null } } } },
    dividends: { responses: { 1: { payload: { items: dividendItems, nextCursor: null } } } },
  });
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await page.getByRole("tab", { name: "Financials" }).click();
  const financialScroller = page.locator('[data-history-kind="financials"]');
  await expect(financialScroller).toHaveAttribute("data-history-start", "0");
  await expect(financialScroller).toContainText("financial-0");
  await expect(financialScroller).not.toContainText("financial-60");
  await financialScroller.focus();
  await financialScroller.evaluate((element) => {
    element.scrollTop = 90 * 42;
    element.dispatchEvent(new Event("scroll"));
  });
  await expect(financialScroller).toHaveAttribute("data-history-start", "80");
  await expect(financialScroller.locator("[data-history-row]").first()).toContainText("financial-80");
  const financialGeometry = await financialScroller.evaluate((element) => ({
    spacer: element.querySelector(".history-spacer td").getBoundingClientRect().height,
    stride: element.querySelectorAll("[data-history-row]")[1].getBoundingClientRect().top - element.querySelectorAll("[data-history-row]")[0].getBoundingClientRect().top,
  }));
  expect(financialGeometry.spacer).toBe(3360);
  expect(financialGeometry.stride).toBeCloseTo(42, 1);
  await expect(financialScroller).toBeFocused();
  await financialScroller.evaluate((element) => {
    element.scrollTop = element.scrollHeight;
    element.dispatchEvent(new Event("scroll"));
  });
  await expect(financialScroller).toHaveAttribute("data-history-start", "120");
  await expect(financialScroller).toBeFocused();
  await expect(financialScroller.locator("[data-history-row]")).toHaveCount(60);
  await expect(financialScroller).toContainText("financial-179");

  await page.getByRole("tab", { name: "Dividends" }).click();
  const dividendScroller = page.locator('[data-history-kind="dividends"]');
  await expect(dividendScroller).toHaveAttribute("data-history-start", "0");
  await expect(dividendScroller).toContainText("dividend-0");
  await expect(dividendScroller).not.toContainText("dividend-60");
  await dividendScroller.focus();
  await dividendScroller.evaluate((element) => {
    element.scrollTop = 90 * 101;
    element.dispatchEvent(new Event("scroll"));
  });
  await expect(dividendScroller).toHaveAttribute("data-history-start", "80");
  await expect(dividendScroller.locator(".dividend-card").first()).toContainText("dividend-80");
  expect(await dividendScroller.evaluate((element) => ({
    spacer: element.querySelector(".history-spacer").getBoundingClientRect().height,
    stride: element.querySelectorAll(".dividend-card")[1].getBoundingClientRect().top - element.querySelectorAll(".dividend-card")[0].getBoundingClientRect().top,
  }))).toEqual({ spacer: 8080, stride: 101 });
  await expect(dividendScroller).toBeFocused();
  await dividendScroller.evaluate((element) => {
    element.scrollTop = element.scrollHeight;
    element.dispatchEvent(new Event("scroll"));
  });
  await expect(dividendScroller).toHaveAttribute("data-history-start", "120");
  await expect(dividendScroller).toBeFocused();
  await expect(dividendScroller.locator(".dividend-card")).toHaveCount(60);
  await expect(dividendScroller).toContainText("dividend-179");
});

test("repeated tab activation does not accumulate panel action handlers", async ({ page }) => {
  await page.addInitScript(() => {
    const native = EventTarget.prototype.addEventListener;
    window.__panelBindings = new WeakMap();
    EventTarget.prototype.addEventListener = function dashboardBindingProbe(type, listener, options) {
      if (this instanceof Element && ((type === "scroll" && this.matches("[data-history-kind]")) || (type === "click" && this.matches("[data-financial-frequency]")))) {
        window.__panelBindings.set(this, (window.__panelBindings.get(this) || 0) + 1);
      }
      return native.call(this, type, listener, options);
    };
  });
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await page.getByRole("tab", { name: "Financials" }).click();
  await expect(page.locator('[data-history-kind="financials"]')).toBeVisible();
  for (let index = 0; index < 8; index += 1) {
    await page.getByRole("tab", { name: "Overview" }).click();
    await page.getByRole("tab", { name: "Financials" }).click();
  }

  const bindings = await page.evaluate(() => ({
    scroll: window.__panelBindings.get(document.querySelector('[data-history-kind="financials"]')),
    annual: window.__panelBindings.get(document.querySelector('[data-financial-frequency="annual"]')),
    quarterly: window.__panelBindings.get(document.querySelector('[data-financial-frequency="quarterly"]')),
  }));
  expect(bindings).toEqual({ scroll: 1, annual: 1, quarterly: 1 });
});

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

test("history navigation cannot restore cached protected views after logout", async ({ page }) => {
  await installLifecycleProbe(page);
  await mockApi(page);
  await page.goto("/dashboard/");
  await page.locator("tbody tr").first().click();
  await expect(page.locator("#stock-chart canvas").first()).toBeVisible();

  await page.getByRole("button", { name: "Settings" }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  const authLifecycle = await page.evaluate(() => ({
    listeners: window.__dashboardLifecycleProbe.resizeListeners.size,
    observes: window.__dashboardLifecycleProbe.observes,
  }));

  await page.goBack();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  await expect(page.locator(".shell, .stock-inspector, #stock-chart")).toHaveCount(0);
  await page.goForward();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  await expect(page.locator(".shell, .stock-inspector, #stock-chart")).toHaveCount(0);
  expect(await page.evaluate(() => ({
    listeners: window.__dashboardLifecycleProbe.resizeListeners.size,
    observes: window.__dashboardLifecycleProbe.observes,
  }))).toEqual(authLifecycle);
});

test("held logout hides login until session cookie expiry settles", async ({ page }) => {
  let releaseLogout;
  const logoutRelease = new Promise((resolve) => { releaseLogout = resolve; });
  let markLogoutRequested;
  const logoutRequested = new Promise((resolve) => { markLogoutRequested = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    logout: { release: logoutRelease, requested: markLogoutRequested },
  });
  await page.goto("/dashboard/");

  await page.getByRole("button", { name: "Settings" }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  await logoutRequested;
  await expect(page.getByText("Signing out…", { exact: true })).toBeVisible();
  await expect(page.locator("#login-form")).toHaveCount(0);
  await expect(page.locator(".shell, .stock-inspector, #stock-chart")).toHaveCount(0);

  const logoutResponse = page.waitForResponse((response) => response.url().endsWith("/api/dashboard/auth/logout"));
  releaseLogout();
  await logoutResponse;
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  await page.getByLabel("Username").fill("analyst");
  await page.getByLabel("Password").fill("secret");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Latest scan" })).toBeVisible();
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));

  await expect(page.getByRole("heading", { name: "Latest scan" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign in" })).toHaveCount(0);
});

test("startup session failure retry reruns session initialization", async ({ page }) => {
  const sessionRequests = [];
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    session: { failRequests: 1, requested: (request) => sessionRequests.push(request) },
  });
  await page.goto("/dashboard/");

  await expect(page.getByText("QBot is unavailable")).toBeVisible();
  await page.getByRole("button", { name: "Retry" }).click();

  await expect(page.getByRole("heading", { name: "Latest scan" })).toBeVisible();
  expect(sessionRequests).toEqual([1, 2]);
});

test("bootstrap 401 returns to login without scheduling refresh", async ({ page }) => {
  await page.addInitScript(() => {
    const nativeSetInterval = window.setInterval.bind(window);
    window.__dashboardRefreshIntervals = 0;
    window.setInterval = (handler, timeout, ...args) => {
      if (timeout === 5 * 60 * 1000) window.__dashboardRefreshIntervals += 1;
      return nativeSetInterval(handler, timeout, ...args);
    };
  });
  await mockApi(page, false, bootstrap.results[0].hits, [], {
    bootstrap: { responses: { 1: { status: 401, payload: { error: "unauthorized" } } } },
  });
  await page.goto("/dashboard/");
  await page.getByLabel("Username").fill("analyst");
  await page.getByLabel("Password").fill("secret");
  const bootstrapResponse = page.waitForResponse((response) => response.url().endsWith("/api/dashboard/bootstrap"));
  await page.getByRole("button", { name: "Sign in" }).click();
  await bootstrapResponse;

  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  expect(await page.evaluate(() => window.__dashboardRefreshIntervals)).toBe(0);
});

test("newer quiet bootstrap wins when an earlier refresh resolves last", async ({ page }) => {
  let releaseOlder;
  const olderRelease = new Promise((resolve) => { releaseOlder = resolve; });
  let markOlderRequested;
  const olderRequested = new Promise((resolve) => { markOlderRequested = resolve; });
  let markNewerRequested;
  const newerRequested = new Promise((resolve) => { markNewerRequested = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    bootstrap: {
      responses: {
        2: { release: olderRelease, requested: markOlderRequested, payload: { ...bootstrap, runId: "older-refresh" } },
        3: { requested: markNewerRequested, payload: { ...bootstrap, runId: "newer-refresh" } },
      },
    },
  });
  await page.goto("/dashboard/");

  await page.getByRole("button", { name: "Refresh" }).click();
  await olderRequested;
  await page.getByRole("button", { name: "Refresh" }).click();
  await newerRequested;
  await expect(page.locator(".view-heading p")).toHaveText("Run newer-refresh");

  const olderResponse = page.waitForResponse((response) => response.url().endsWith("/api/dashboard/bootstrap"));
  releaseOlder();
  await olderResponse;
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));

  await expect(page.locator(".view-heading p")).toHaveText("Run newer-refresh");
});

test("newer same-key stock success wins when an older success resolves last", async ({ page }) => {
  let releaseOlder;
  const olderRelease = new Promise((resolve) => { releaseOlder = resolve; });
  let markOlderRequested;
  const olderRequested = new Promise((resolve) => { markOlderRequested = resolve; });
  let releaseNewer;
  const newerRelease = new Promise((resolve) => { releaseNewer = resolve; });
  let markNewerRequested;
  const newerRequested = new Promise((resolve) => { markNewerRequested = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    stock: {
      responses: {
        1: { release: olderRelease, requested: markOlderRequested, payload: { name: "Older snapshot" } },
        2: { release: newerRelease, requested: markNewerRequested, payload: { name: "Newer snapshot" } },
      },
    },
  });
  await page.goto("/dashboard/");

  await page.locator("tbody tr").first().click();
  await olderRequested;
  await page.locator("[data-close]").click();
  await page.locator("tbody tr").first().click();
  await newerRequested;

  const newerResponse = page.waitForResponse((response) => response.url().includes("/api/dashboard/stocks/"));
  releaseNewer();
  await newerResponse;
  await expect(page.getByRole("heading", { name: /Newer snapshot/ })).toBeVisible();
  const olderResponse = page.waitForResponse((response) => response.url().includes("/api/dashboard/stocks/"));
  releaseOlder();
  await olderResponse;
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));

  await expect(page.getByRole("heading", { name: /Newer snapshot/ })).toBeVisible();
  await expect(page.getByRole("heading", { name: /Older snapshot/ })).toHaveCount(0);
});

test("newer same-key stock success wins when an older error resolves last", async ({ page }) => {
  let releaseOlder;
  const olderRelease = new Promise((resolve) => { releaseOlder = resolve; });
  let markOlderRequested;
  const olderRequested = new Promise((resolve) => { markOlderRequested = resolve; });
  let releaseNewer;
  const newerRelease = new Promise((resolve) => { releaseNewer = resolve; });
  let markNewerRequested;
  const newerRequested = new Promise((resolve) => { markNewerRequested = resolve; });
  await mockApi(page, true, bootstrap.results[0].hits, [], {
    stock: {
      responses: {
        1: { release: olderRelease, requested: markOlderRequested, status: 500, payload: { error: "older request failed" } },
        2: { release: newerRelease, requested: markNewerRequested, payload: { name: "Current snapshot" } },
      },
    },
  });
  await page.goto("/dashboard/");

  await page.locator("tbody tr").first().click();
  await olderRequested;
  await page.locator("[data-close]").click();
  await page.locator("tbody tr").first().click();
  await newerRequested;

  const newerResponse = page.waitForResponse((response) => response.url().includes("/api/dashboard/stocks/") && response.status() === 200);
  releaseNewer();
  await newerResponse;
  await expect(page.getByRole("heading", { name: /Current snapshot/ })).toBeVisible();
  const olderResponse = page.waitForResponse((response) => response.url().includes("/api/dashboard/stocks/") && response.status() === 500);
  releaseOlder();
  await olderResponse;
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));

  await expect(page.getByRole("heading", { name: /Current snapshot/ })).toBeVisible();
  await expect(page.getByText("Chart unavailable")).toHaveCount(0);
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
  await expect(page.locator('[data-inspector-panel="overview"]')).toContainText("1,488.50");

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
