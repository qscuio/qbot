import test from "node:test";
import assert from "node:assert/strict";

import { dashboardApi } from "../js/api.js";

test("stock history lets the server choose a period-specific data budget", async () => {
  let requestedPath = "";
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (path) => {
    requestedPath = path;
    return new Response(JSON.stringify({ bars: [] }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    await dashboardApi.stock("600519.SH", "monthly");
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(requestedPath, "/api/dashboard/stocks/600519.SH?period=monthly");
});

test("company intelligence API safely encodes stock, frequency, and cursor", async () => {
  const requestedPaths = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (path) => {
    requestedPaths.push(path);
    return new Response(JSON.stringify({ items: [], nextCursor: null }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    await dashboardApi.company("60/0519.SH");
    await dashboardApi.financials("600519.SH", "quarterly", "end/date+next=");
    await dashboardApi.dividends("600519.SH", "cash/action?next=1");
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.deepEqual(requestedPaths, [
    "/api/dashboard/stocks/60%2F0519.SH/company",
    "/api/dashboard/stocks/600519.SH/financials?frequency=quarterly&cursor=end%2Fdate%2Bnext%3D",
    "/api/dashboard/stocks/600519.SH/dividends?cursor=cash%2Faction%3Fnext%3D1",
  ]);
});

test("company intelligence API omits absent cursors", async () => {
  const requestedPaths = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (path) => {
    requestedPaths.push(path);
    return new Response(JSON.stringify({ items: [], nextCursor: null }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    await dashboardApi.financials("600519.SH", "annual");
    await dashboardApi.dividends("600519.SH");
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.deepEqual(requestedPaths, [
    "/api/dashboard/stocks/600519.SH/financials?frequency=annual",
    "/api/dashboard/stocks/600519.SH/dividends",
  ]);
});

test("historical chips API omits latest query and encodes an exact requested date", async () => {
  const requestedPaths = [];
  const requestedSignals = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (path, options) => {
    requestedPaths.push(path);
    requestedSignals.push(options.signal);
    return new Response(JSON.stringify({ distribution: [] }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    await dashboardApi.chips("60/0519.SH");
    const controller = new AbortController();
    await dashboardApi.chips("600519.SH", "2026-07-17", { signal: controller.signal });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.deepEqual(requestedPaths, [
    "/api/dashboard/stocks/60%2F0519.SH/chips",
    "/api/dashboard/stocks/600519.SH/chips?date=2026-07-17",
  ]);
  assert.equal(requestedSignals[0], undefined);
  assert.equal(requestedSignals[1] instanceof AbortSignal, true);
});
