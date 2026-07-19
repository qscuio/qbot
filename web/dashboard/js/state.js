const collator = new Intl.Collator("zh-CN", { numeric: true, sensitivity: "base" });
const inspectorPreferencesKey = "qbot.dashboard.inspector.v1";
const defaultInspectorWidth = 380;
const minimumInspectorWidth = 300;

export function activeFilterCount(filters = {}) {
  return [String(filters.search ?? "").trim(), filters.group, filters.signal, filters.rankedOnly]
    .filter(Boolean)
    .length;
}

export function clampInspectorWidth(width, viewportWidth) {
  const maximumWidth = Math.max(minimumInspectorWidth, Math.floor(viewportWidth * 0.5));
  const preferredWidth = Number.isFinite(width) ? width : defaultInspectorWidth;
  return Math.min(Math.max(preferredWidth, minimumInspectorWidth), maximumWidth);
}

export function loadInspectorPreferences(storage, viewportWidth) {
  try {
    const stored = JSON.parse(storage.getItem(inspectorPreferencesKey));
    return {
      width: clampInspectorWidth(stored?.width, viewportWidth),
      collapsed: stored?.collapsed === true,
    };
  } catch {
    return {
      width: clampInspectorWidth(defaultInspectorWidth, viewportWidth),
      collapsed: false,
    };
  }
}

export function saveInspectorPreferences(storage, preferences) {
  try {
    storage.setItem(inspectorPreferencesKey, JSON.stringify(preferences));
  } catch {
    // Browser storage may be unavailable or over quota.
  }
}

export function normalizeRows(results = []) {
  return results.map((result) => {
    const hits = Array.isArray(result.hits) ? result.hits : [];
    return {
      ...result,
      code: String(result.code ?? ""),
      name: result.name || "Unknown security",
      hits,
      hitCount: hits.length,
      signalIds: hits.map((hit) => hit.signalId),
      groups: [...new Set(hits.map((hit) => hit.group).filter(Boolean))],
      ranked: hits.some((hit) => hit.isRankedPool),
      searchText: `${result.code ?? ""} ${result.name ?? ""}`.toLocaleLowerCase("zh-CN"),
    };
  });
}

export function applyFilters(rows, filters = {}) {
  const search = String(filters.search || "").trim().toLocaleLowerCase("zh-CN");
  return rows.filter((row) => {
    if (search && !row.searchText.includes(search)) return false;
    if (filters.group && !row.groups.includes(filters.group)) return false;
    if (filters.signal && !row.signalIds.includes(filters.signal)) return false;
    if (filters.rankedOnly && !row.ranked) return false;
    return true;
  });
}

export function sortRows(rows, key = "ranked", direction = "desc") {
  const multiplier = direction === "asc" ? 1 : -1;
  const valueFor = (row) => {
    switch (key) {
      case "name": return row.name;
      case "hits": return row.hitCount;
      case "change": return row.changePct ?? Number.NEGATIVE_INFINITY;
      case "code": return row.code;
      case "ranked": return Number(row.ranked) * 1000 + row.hitCount;
      default: return row.code;
    }
  };
  return [...rows].sort((left, right) => {
    const a = valueFor(left);
    const b = valueFor(right);
    const compared = typeof a === "number" && typeof b === "number"
      ? a - b
      : collator.compare(String(a), String(b));
    if (compared !== 0) return compared * multiplier;
    return collator.compare(left.code, right.code);
  });
}

export function createWorkspaceState() {
  return {
    activeTab: "scan",
    tabs: [{ id: "scan", type: "scan", label: "Latest scan", closable: false }],
  };
}

export function openStockTab(state, stock) {
  const id = `stock:${stock.code}`;
  if (state.tabs.some((tab) => tab.id === id)) return { ...state, activeTab: id };
  return {
    ...state,
    activeTab: id,
    tabs: [
      ...state.tabs,
      {
        id,
        type: "stock",
        code: stock.code,
        label: `${stock.code} ${stock.name}`,
        period: "daily",
        closable: true,
      },
    ],
  };
}

export function closeTab(state, id) {
  const index = state.tabs.findIndex((tab) => tab.id === id);
  if (index < 0 || !state.tabs[index].closable) return state;
  const tabs = state.tabs.filter((tab) => tab.id !== id);
  const activeTab = state.activeTab === id
    ? (tabs[Math.max(0, index - 1)]?.id || "scan")
    : state.activeTab;
  return { ...state, tabs, activeTab };
}

export function updateTab(state, id, patch) {
  return {
    ...state,
    tabs: state.tabs.map((tab) => tab.id === id ? { ...tab, ...patch } : tab),
  };
}
