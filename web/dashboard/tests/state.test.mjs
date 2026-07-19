import test from "node:test";
import assert from "node:assert/strict";

import {
  activeFilterCount,
  applyFilters,
  clampInspectorWidth,
  createWorkspaceState,
  loadInspectorPreferences,
  normalizeRows,
  openStockTab,
  saveInspectorPreferences,
  sortRows,
} from "../js/state.js";

function memoryStorage() {
  const values = new Map();
  return {
    getItem(key) {
      return values.get(key) ?? null;
    },
    setItem(key, value) {
      values.set(key, value);
    },
  };
}

const rows = normalizeRows([
  {
    code: "600519",
    name: "贵州茅台",
    changePct: 1.2,
    hits: [
      { signalId: "volume_surge", group: "volume", isRankedPool: false },
      { signalId: "top20", group: "ranked", isRankedPool: true },
    ],
  },
  {
    code: "000001",
    name: "平安银行",
    changePct: -0.4,
    hits: [{ signalId: "ma_bullish", group: "trend", isRankedPool: false }],
  },
]);

test("normalizes scan rows with derived filter fields", () => {
  assert.equal(rows[0].hitCount, 2);
  assert.deepEqual(rows[0].signalIds, ["volume_surge", "top20"]);
  assert.equal(rows[0].ranked, true);
});

test("filters by text, group, signal, and ranked membership", () => {
  assert.deepEqual(applyFilters(rows, { search: "茅台" }).map((row) => row.code), ["600519"]);
  assert.deepEqual(applyFilters(rows, { group: "trend" }).map((row) => row.code), ["000001"]);
  assert.deepEqual(applyFilters(rows, { signal: "top20" }).map((row) => row.code), ["600519"]);
  assert.deepEqual(applyFilters(rows, { rankedOnly: true }).map((row) => row.code), ["600519"]);
});

test("sorts deterministically and keeps codes as strings", () => {
  assert.deepEqual(sortRows(rows, "change", "asc").map((row) => row.code), ["000001", "600519"]);
  assert.deepEqual(sortRows(rows, "code", "asc").map((row) => row.code), ["000001", "600519"]);
});

test("counts only active scan filters", () => {
  assert.equal(activeFilterCount({
    search: "茅台",
    group: "trend",
    signal: "",
    rankedOnly: false,
    sort: "ranked",
    direction: "desc",
  }), 2);
  assert.equal(activeFilterCount({ signal: "top20", rankedOnly: true }), 2);
  assert.equal(activeFilterCount({ search: "   " }), 0);
});

test("clamps and persists inspector preferences", () => {
  assert.equal(clampInspectorWidth(120, 1600), 300);
  assert.equal(clampInspectorWidth(1200, 1600), 800);
  const storage = memoryStorage();
  saveInspectorPreferences(storage, { width: 440, collapsed: true });
  assert.deepEqual(loadInspectorPreferences(storage, 1600), { width: 440, collapsed: true });
});

test("uses safe inspector defaults for missing or invalid storage", () => {
  assert.deepEqual(loadInspectorPreferences(memoryStorage(), 1600), {
    width: 380,
    collapsed: false,
  });
  assert.deepEqual(loadInspectorPreferences({ getItem: () => "not json" }, 600), {
    width: 300,
    collapsed: false,
  });
  assert.deepEqual(loadInspectorPreferences({ getItem: () => { throw new Error("blocked"); } }, 1600), {
    width: 380,
    collapsed: false,
  });
});

test("ignores inspector storage quota errors", () => {
  assert.doesNotThrow(() => saveInspectorPreferences({
    setItem() {
      throw new Error("quota exceeded");
    },
  }, { width: 440, collapsed: true }));
});

test("opening a stock creates a stable editor tab", () => {
  const state = createWorkspaceState();
  const opened = openStockTab(state, { code: "600519", name: "贵州茅台" });
  const reopened = openStockTab(opened, { code: "600519", name: "贵州茅台" });

  assert.equal(reopened.tabs.length, 2);
  assert.equal(reopened.activeTab, "stock:600519");
  assert.equal(reopened.tabs[1].period, "daily");
});
