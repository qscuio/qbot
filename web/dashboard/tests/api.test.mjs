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
