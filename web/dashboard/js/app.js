import { dashboardApi, ApiError } from "./api.js?v=20260719.7";
import { activitySeries, mountChart } from "./chart.js?v=20260719.7";
import { chipPanel, companyPanel, dividendPanel, financialPanel } from "./company-panels.js?v=20260719.7";
import {
  activeFilterCount,
  applyFilters,
  closeTab,
  clampInspectorWidth,
  createWorkspaceState,
  DEFAULT_INSPECTOR_WIDTH,
  loadInspectorPreferences,
  maximumInspectorWidth,
  MINIMUM_INSPECTOR_WIDTH,
  normalizeRows,
  openStockTab,
  saveInspectorPreferences,
  sortRows,
  updateTab,
} from "./state.js?v=20260719.7";

const app = document.querySelector("#app");
let bootstrap = null;
let rows = [];
let workspace = createWorkspaceState();
let filters = { search: "", group: "", signal: "", rankedOnly: false, sort: "ranked", direction: "desc" };
const details = new Map();
const stockRequestSequences = new Map();
const companyPanelStates = new Map();
const optionalRequestCache = new Map();
const optionalRequestSequences = new Map();
const financialFrequencies = new Map();
const chipRequestedDates = new Map();
let chartHandle = null;
let inspectorCleanup = null;
let closeInspectorOverlay = null;
let authenticated = false;
let protectedViewGeneration = 0;
let bootstrapRequestSequence = 0;
let refreshTimer = null;
let filterMenuOpen = false;
let inspectorTab = "overview";
let inspectorPreferences = loadInspectorPreferences(window.localStorage, Number.POSITIVE_INFINITY);

function effectiveInspectorWidth() {
  return clampInspectorWidth(inspectorPreferences.width, window.innerWidth);
}

function teardownWorkspaceView() {
  inspectorCleanup?.();
  inspectorCleanup = null;
  closeInspectorOverlay = null;
  chartHandle?.destroy();
  chartHandle = null;
}

function invalidateProtectedView() {
  protectedViewGeneration += 1;
  teardownWorkspaceView();
  return protectedViewGeneration;
}

function enterAuthenticationBoundary() {
  authenticated = false;
  bootstrap = null;
  rows = [];
  workspace = createWorkspaceState();
  details.clear();
  stockRequestSequences.clear();
  companyPanelStates.clear();
  optionalRequestCache.clear();
  optionalRequestSequences.clear();
  financialFrequencies.clear();
  chipRequestedDates.clear();
  return invalidateProtectedView();
}

function scheduleRefresh() {
  clearInterval(refreshTimer);
  refreshTimer = setInterval(() => loadBootstrap({ quiet: true }), 5 * 60 * 1000);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function formatTime(value) {
  if (!value) return "Never";
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short", timeZone: "Asia/Shanghai" }).format(new Date(value));
}

function changeClass(value) {
  if (value > 0) return "up";
  if (value < 0) return "down";
  return "";
}

function visibleRows() {
  return sortRows(applyFilters(rows, filters), filters.sort, filters.direction);
}

function renderLogin(error = "") {
  clearInterval(refreshTimer);
  const generation = enterAuthenticationBoundary();
  app.innerHTML = `
    <main class="login-screen">
      <form class="login-card" id="login-form">
        <div class="brand-mark">QB</div>
        <h1>QBot Market Intelligence</h1>
        <p>Sign in to the private scan workspace.</p>
        <div class="field"><label for="username">Username</label><input class="control" id="username" name="username" autocomplete="username" required autofocus></div>
        <div class="field"><label for="password">Password</label><input class="control" id="password" name="password" type="password" autocomplete="current-password" required></div>
        <button class="primary-button" type="submit">Sign in</button>
        <p class="form-error" role="alert">${escapeHtml(error)}</p>
      </form>
    </main>`;
  app.querySelector("#login-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = event.currentTarget.querySelector("button");
    const errorNode = event.currentTarget.querySelector(".form-error");
    button.disabled = true;
    button.textContent = "Signing in…";
    errorNode.textContent = "";
    const generation = protectedViewGeneration;
    try {
      await dashboardApi.login(event.currentTarget.username.value, event.currentTarget.password.value);
      if (generation !== protectedViewGeneration) return;
      authenticated = true;
      if ((await loadBootstrap()) === true) scheduleRefresh();
    } catch (error) {
      if (generation !== protectedViewGeneration) return;
      errorNode.textContent = error.status === 429 ? "Too many attempts. Try again later." : "Invalid username or password.";
      button.disabled = false;
      button.textContent = "Sign in";
    }
  });
}

function renderSigningOut() {
  clearInterval(refreshTimer);
  const generation = enterAuthenticationBoundary();
  app.innerHTML = `<div class="boot-screen"><span class="spinner"></span><span>Signing out…</span></div>`;
  return generation;
}

async function loadBootstrap({ quiet = false } = {}) {
  if (!authenticated) return false;
  const requestSequence = ++bootstrapRequestSequence;
  const generation = quiet ? protectedViewGeneration : invalidateProtectedView();
  if (!quiet) {
    app.innerHTML = `<div class="boot-screen"><span class="spinner"></span><span>Loading latest scan…</span></div>`;
  }
  try {
    const payload = await dashboardApi.bootstrap();
    if (!authenticated || generation !== protectedViewGeneration || requestSequence !== bootstrapRequestSequence) return false;
    bootstrap = payload;
    rows = normalizeRows(bootstrap.results);
    restoreLocation();
    return true;
  } catch (error) {
    if (!authenticated || generation !== protectedViewGeneration || requestSequence !== bootstrapRequestSequence) return false;
    if (error instanceof ApiError && error.status === 401) {
      renderLogin();
      return false;
    }
    if (quiet) {
      showTransientError("Refresh failed. Existing results are unchanged.");
      return false;
    }
    renderFailure(error.message);
    return false;
  }
}

function renderFailure(message, retry = () => loadBootstrap()) {
  invalidateProtectedView();
  app.innerHTML = `<div class="boot-screen"><strong>QBot is unavailable</strong><span>${escapeHtml(message)}</span><button class="outline-button" id="retry">Retry</button></div>`;
  app.querySelector("#retry").addEventListener("click", () => retry());
}

function freshnessBanner() {
  if (bootstrap.freshness === "never_scanned") return `<div class="state-banner">No completed scan has been persisted yet.</div>`;
  if (bootstrap.freshness === "stale") return `<div class="state-banner">The latest scan is older than the latest trading data. Review it as stale.</div>`;
  if (rows.some((row) => row.partial)) return `<div class="state-banner">Some results could not be enriched with current market data.</div>`;
  return "";
}

function shellTemplate(body) {
  const visible = visibleRows();
  return `
    <div class="shell">
      <div class="workspace">
        <nav class="activitybar" aria-label="Workspace"><button class="activity-button active" title="Scan explorer">⌁</button><span class="activity-spacer"></span><button class="activity-button" id="settings" aria-label="Settings" aria-expanded="false">⚙</button></nav>
        <div class="settings-popover hidden" id="settings-menu"><strong>QBot</strong><span>Market intelligence · read only</span><button class="outline-button" id="logout">Sign out</button></div>
        <main class="editor"><div class="tabs" role="tablist"><button class="ghost-button mobile-settings" id="mobile-settings" aria-label="Settings">⚙</button>${tabsTemplate()}</div><div class="editor-body">${body}</div></main>
      </div>
      <footer class="statusbar">
        <span><i class="status-dot"></i>API connected</span>
        <span>${bootstrap.marketOpen ? "Market open" : "Market closed"}</span>
        <div class="statusbar-right"><span>Scan ${escapeHtml(formatTime(bootstrap.scannedAt))}</span><span>${visible.length} / ${rows.length} stocks</span></div>
      </footer>
    </div>`;
}

function filterFormTemplate() {
  const groups = [...new Set(bootstrap.catalog.map((item) => item.group))].sort();
  return `<form class="filter-form" id="filters">
      <label>Stock search<input class="control" name="search" value="${escapeHtml(filters.search)}" placeholder="Code or name"></label>
      <label>Signal group<select class="control" name="group"><option value="">All groups</option>${groups.map((group) => `<option value="${escapeHtml(group)}" ${filters.group === group ? "selected" : ""}>${escapeHtml(group)}</option>`).join("")}</select></label>
      <label>Signal<select class="control" name="signal"><option value="">All signals</option>${bootstrap.catalog.map((signal) => `<option value="${escapeHtml(signal.id)}" ${filters.signal === signal.id ? "selected" : ""}>${escapeHtml(signal.name)}</option>`).join("")}</select></label>
      <label>Sort<select class="control" name="sort"><option value="ranked">Ranked priority</option><option value="code">Stock code</option><option value="name">Stock name</option><option value="hits">Hit count</option><option value="change">Price change</option></select></label>
      <label>Direction<select class="control" name="direction"><option value="desc">Descending</option><option value="asc">Ascending</option></select></label>
      <label class="checkbox"><input type="checkbox" name="rankedOnly" ${filters.rankedOnly ? "checked" : ""}> Ranked pools only</label>
      <button class="outline-button" type="button" id="clear-filters">Clear filters</button>
    </form>`;
}

function tabsTemplate() {
  return workspace.tabs.map((tab) => `<button class="tab ${workspace.activeTab === tab.id ? "active" : ""}" data-tab="${escapeHtml(tab.id)}" role="tab"><span class="tab-icon">${tab.type === "scan" ? "⌁" : "K"}</span><span class="tab-label">${escapeHtml(tab.label)}</span>${tab.closable ? `<span class="tab-close" data-close="${escapeHtml(tab.id)}" title="Close">×</span>` : ""}</button>`).join("");
}

function scanTemplate() {
  const visible = visibleRows();
  const filterCount = activeFilterCount(filters);
  return `<section class="scan-view">
    <div class="view-heading"><div><h1>Latest scan</h1><p>${escapeHtml(bootstrap.runId ? `Run ${bootstrap.runId}` : "Awaiting the first scan")}</p></div><div class="view-heading-actions"><div class="filter-anchor"><button class="outline-button" id="filter-toggle" type="button" aria-controls="filter-menu" aria-expanded="${filterMenuOpen}">Filters${filterCount ? ` (${filterCount})` : ""}</button><div class="filter-popover ${filterMenuOpen ? "" : "hidden"}" id="filter-menu"><div class="filter-popover-title">Scan filters</div>${filterFormTemplate()}</div></div><button class="outline-button" id="refresh">↻ Refresh</button></div></div>
    ${freshnessBanner()}
    <section class="summary-strip" aria-label="Latest run summary">
      <div class="summary-metric"><span>Unique stocks</span><strong>${bootstrap.summary.uniqueStocks}</strong></div>
      <div class="summary-metric"><span>Total hits</span><strong>${bootstrap.summary.totalHits}</strong></div>
      <div class="summary-metric"><span>Active signals</span><strong>${bootstrap.summary.activeSignals}</strong></div>
      <div class="summary-metric"><span>Ranked candidates</span><strong>${bootstrap.summary.rankedCandidates}</strong></div>
    </section>
    ${visible.length ? `<table class="data-grid"><thead><tr><th>Security</th><th>Close</th><th>Change</th><th>Signals</th><th>Hits</th></tr></thead><tbody>${visible.map((row) => `<tr data-stock="${escapeHtml(row.code)}"><td><div class="stock-cell"><span class="stock-code mono">${escapeHtml(row.code)}</span><span class="stock-name">${escapeHtml(row.name)}</span></div></td><td class="number">${formatNumber(row.close)}</td><td class="number ${changeClass(row.changePct)}">${row.changePct == null ? "—" : `${row.changePct > 0 ? "+" : ""}${formatNumber(row.changePct)}%`}</td><td><div class="badges">${row.hits.map((hit) => `<span class="badge ${hit.isRankedPool ? "ranked" : ""}" title="${escapeHtml(hit.name)}">${escapeHtml(hit.icon)} ${escapeHtml(hit.name)}</span>`).join("")}</div></td><td class="number">${row.hitCount}</td></tr>`).join("")}</tbody></table>` : `<div class="empty-state"><strong>${rows.length ? "No results match these filters" : "The latest scan contains no hits"}</strong><span>${rows.length ? "Clear or adjust the explorer filters." : "A successful zero-hit scan is valid."}</span></div>`}
  </section>`;
}

function stockLoadingTemplate(tab) {
  return `<div class="loading-panel"><span class="spinner"></span><span>Loading ${escapeHtml(tab.code)} market history…</span></div>`;
}

function panelStateKey(kind, code, frequency = "") {
  return `${protectedViewGeneration}:${kind}:${code}:${frequency}`;
}

function currentFinancialFrequency(code) {
  return financialFrequencies.get(code) || "annual";
}

function chipSelectionKey(code) {
  return `${protectedViewGeneration}:${code}`;
}

function currentChipRequest(code) {
  return chipRequestedDates.get(chipSelectionKey(code)) ?? "latest";
}

function chipControls() {
  return `<div class="panel-section-toolbar chip-toolbar"><strong>筹码分布</strong><button type="button" class="outline-button" data-chip-latest>Latest</button></div>`;
}

function getPanelState(kind, code, frequency = "") {
  const key = panelStateKey(kind, code, frequency);
  if (!companyPanelStates.has(key)) {
    companyPanelStates.set(key, { status: "idle", items: [], nextCursor: null, error: "", failedCursor: null, windowStart: 0 });
  }
  return companyPanelStates.get(key);
}

function financialControls(frequency) {
  return `<div class="panel-section-toolbar"><div class="segmented-control" role="group" aria-label="财务周期"><button type="button" data-financial-frequency="annual" aria-pressed="${frequency === "annual"}" class="${frequency === "annual" ? "active" : ""}">年度</button><button type="button" data-financial-frequency="quarterly" aria-pressed="${frequency === "quarterly"}" class="${frequency === "quarterly" ? "active" : ""}">季度</button></div></div>`;
}

function panelLoading(label, frequency = null) {
  return `${frequency ? financialControls(frequency) : ""}<div class="panel-local-state" role="status"><span class="spinner"></span><span>Loading ${escapeHtml(label)}…</span></div>`;
}

function panelError(kind, message, frequency = null) {
  return `${frequency ? financialControls(frequency) : ""}<div class="panel-local-state panel-error" role="alert"><strong>Unable to load this section</strong><span>${escapeHtml(message)}</span><button type="button" class="outline-button" data-panel-retry="${escapeHtml(kind)}">Retry</button></div>`;
}

function panelMarkup(kind, code) {
  if (kind === "overview") {
    const state = getPanelState("company", code);
    if (state.status === "loaded") return companyPanel(state.payload);
    if (state.status === "error") return panelError("company", state.error);
    return panelLoading("overview");
  }
  if (kind === "financials") {
    const frequency = currentFinancialFrequency(code);
    const state = getPanelState("financials", code, frequency);
    if (state.status === "loaded" || state.items.length) {
      const rendered = financialPanel(
        { items: state.items, nextCursor: state.nextCursor },
        { frequency, windowStart: state.windowStart },
      );
      return `${rendered}${state.status === "error" ? `<div class="panel-inline-error" role="alert">${escapeHtml(state.error)} <button type="button" data-panel-retry="financials">Retry</button></div>` : ""}`;
    }
    if (state.status === "error") return panelError("financials", state.error, frequency);
    return panelLoading(`${frequency} financials`, frequency);
  }
  if (kind === "dividends") {
    const state = getPanelState("dividends", code);
    if (state.status === "loaded" || state.items.length) {
      const rendered = dividendPanel(
        { items: state.items, nextCursor: state.nextCursor },
        { windowStart: state.windowStart },
      );
      return `${rendered}${state.status === "error" ? `<div class="panel-inline-error" role="alert">${escapeHtml(state.error)} <button type="button" data-panel-retry="dividends">Retry</button></div>` : ""}`;
    }
    if (state.status === "error") return panelError("dividends", state.error);
    return panelLoading("dividends");
  }
  if (kind === "chips") {
    const requestDate = currentChipRequest(code);
    const state = getPanelState("chips", code, requestDate);
    if (state.status === "loaded") return chipPanel(state.payload);
    if (state.status === "error") return `${chipControls()}${panelError("chips", state.error)}`;
    return `${chipControls()}${panelLoading("chips")}`;
  }
  return "";
}

function currentStockTab() {
  const tab = workspace.tabs.find((item) => item.id === workspace.activeTab);
  return tab?.type === "stock" ? tab : null;
}

function renderInspectorPanel(kind, code) {
  const tab = currentStockTab();
  if (!tab || tab.code !== code || inspectorTab !== kind) return;
  const panel = app.querySelector(`[data-inspector-panel="${kind}"]`);
  if (!panel) return;
  const scroller = app.querySelector(".inspector-panels");
  const scrollTop = scroller?.scrollTop || 0;
  const previousHistoryScroller = panel.querySelector("[data-history-kind]");
  const historyScrollTop = previousHistoryScroller?.scrollTop || 0;
  const restoreHistoryFocus = document.activeElement === previousHistoryScroller;
  panel.innerHTML = panelMarkup(kind, code);
  bindPanelActions(panel, kind, code);
  if (scroller) scroller.scrollTop = scrollTop;
  const historyScroller = panel.querySelector("[data-history-kind]");
  if (historyScroller) {
    if (restoreHistoryFocus) historyScroller.focus({ preventScroll: true });
    historyScroller.scrollTop = historyScrollTop;
  }
}

function rowIdentity(kind, item) {
  if (kind === "financials") return [item.source, item.endDate, item.reportType].join("|");
  return [item.source, item.announcementDate, item.recordDate, item.exDate, item.payDate, item.implementationStatus, item.cashDividend, item.stockRatio].join("|");
}

function appendUnique(kind, current, incoming) {
  const seen = new Set(current.map((item) => rowIdentity(kind, item)));
  return [...current, ...incoming.filter((item) => {
    const key = rowIdentity(kind, item);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  })];
}

async function loadPanelPage(kind, code, { frequency = "", cursor = null, retry = false } = {}) {
  if (!authenticated) return;
  const generation = protectedViewGeneration;
  const baseKey = panelStateKey(kind, code, frequency);
  const state = getPanelState(kind, code, frequency);
  const requestKey = `${baseKey}:${cursor || "first"}`;
  if (retry) optionalRequestCache.delete(requestKey);
  if (optionalRequestCache.has(requestKey)) return optionalRequestCache.get(requestKey);
  state.status = "loading";
  state.error = "";
  state.failedCursor = null;
  renderInspectorPanel(kind === "company" ? "overview" : kind, code);
  const sequence = (optionalRequestSequences.get(requestKey) || 0) + 1;
  optionalRequestSequences.set(requestKey, sequence);
  const isCurrent = () => authenticated
    && generation === protectedViewGeneration
    && optionalRequestSequences.get(requestKey) === sequence;
  const operation = (async () => {
    try {
      const payload = kind === "company"
        ? await dashboardApi.company(code)
        : kind === "financials"
          ? await dashboardApi.financials(code, frequency, cursor)
          : kind === "dividends"
            ? await dashboardApi.dividends(code, cursor)
            : await dashboardApi.chips(code, frequency === "latest" ? null : frequency);
      if (!isCurrent()) return;
      if (kind === "company" || kind === "chips") state.payload = payload;
      else state.items = cursor
        ? appendUnique(kind, state.items, Array.isArray(payload.items) ? payload.items : [])
        : (Array.isArray(payload.items) ? payload.items : []);
      state.nextCursor = payload?.nextCursor || null;
      state.status = "loaded";
      state.error = "";
      optionalRequestCache.set(requestKey, Promise.resolve(payload));
    } catch (error) {
      if (!isCurrent()) return;
      optionalRequestCache.delete(requestKey);
      if (error instanceof ApiError && error.status === 401) {
        renderLogin("Your session expired. Please sign in again.");
        return;
      }
      state.status = "error";
      state.error = error.message;
      state.failedCursor = cursor;
    }
    if (!isCurrent()) return;
    renderInspectorPanel(kind === "company" ? "overview" : kind, code);
  })();
  optionalRequestCache.set(requestKey, operation);
  return operation;
}

function ensureInspectorPanel(kind, code) {
  if (kind === "overview") return loadPanelPage("company", code);
  if (kind === "financials") return loadPanelPage("financials", code, { frequency: currentFinancialFrequency(code) });
  if (kind === "dividends") return loadPanelPage("dividends", code);
  if (kind === "chips") return loadPanelPage("chips", code, { frequency: currentChipRequest(code) });
  return null;
}

function bindPanelActions(panel, kind, code) {
  panel.querySelectorAll("[data-financial-frequency]").forEach((button) => button.addEventListener("click", () => {
    const frequency = button.dataset.financialFrequency;
    financialFrequencies.set(code, frequency);
    renderInspectorPanel("financials", code);
    loadPanelPage("financials", code, { frequency });
  }));
  panel.querySelector("[data-panel-retry]")?.addEventListener("click", () => {
    const requestKind = kind === "overview" ? "company" : kind;
    const frequency = requestKind === "financials"
      ? currentFinancialFrequency(code)
      : requestKind === "chips" ? currentChipRequest(code) : "";
    const state = getPanelState(requestKind, code, frequency);
    loadPanelPage(requestKind, code, { frequency, cursor: state.failedCursor, retry: true });
  });
  panel.querySelector("[data-chip-latest]")?.addEventListener("click", () => {
    chipRequestedDates.set(chipSelectionKey(code), "latest");
    renderInspectorPanel("chips", code);
    loadPanelPage("chips", code, { frequency: "latest" });
  });
  panel.querySelector("[data-load-more]")?.addEventListener("click", () => {
    const requestKind = panel.querySelector("[data-load-more]").dataset.loadMore;
    const frequency = requestKind === "financials" ? currentFinancialFrequency(code) : "";
    const state = getPanelState(requestKind, code, frequency);
    if (state.nextCursor) loadPanelPage(requestKind, code, { frequency, cursor: state.nextCursor });
  });
  const historyScroller = panel.querySelector("[data-history-kind]");
  if (historyScroller) {
    let scrollFrame = null;
    historyScroller.addEventListener("scroll", () => {
      if (scrollFrame !== null) return;
      scrollFrame = window.requestAnimationFrame(() => {
        scrollFrame = null;
        const requestKind = historyScroller.dataset.historyKind;
        const frequency = requestKind === "financials" ? currentFinancialFrequency(code) : "";
        const state = getPanelState(requestKind, code, frequency);
        const rowHeight = Number(historyScroller.dataset.historyRowHeight) || 42;
        const total = Number(historyScroller.dataset.historyTotal) || 0;
        const maximumStart = Math.max(0, total - 60);
        const nextStart = Math.min(maximumStart, Math.max(0, Math.floor(historyScroller.scrollTop / rowHeight) - 10));
        if (Math.abs(nextStart - state.windowStart) < 10) return;
        state.windowStart = nextStart;
        renderInspectorPanel(kind, code);
      });
    }, { passive: true });
  }
}

function inspectorTemplate(detail) {
  const tabs = [
    ["overview", "Overview"],
    ["financials", "Financials"],
    ["dividends", "Dividends"],
    ["chips", "Chips"],
  ];
  const tabButtons = tabs.map(([id, label]) => `<button type="button" role="tab" data-inspector-tab="${id}" id="inspector-tab-${id}" aria-controls="inspector-panel-${id}" aria-selected="${inspectorTab === id}" tabindex="${inspectorTab === id ? "0" : "-1"}" class="inspector-tab ${inspectorTab === id ? "active" : ""}">${label}</button>`).join("");
  return `<aside class="stock-inspector" id="stock-inspector" aria-label="Stock information">
    <div class="inspector-heading"><strong>Information</strong><span class="mono">${escapeHtml(detail.code)}</span></div>
    <div class="inspector-tabs" role="tablist" aria-label="Stock information sections">${tabButtons}</div>
    <div class="inspector-panels">
      ${tabs.map(([id]) => `<section class="inspector-panel" role="tabpanel" data-inspector-panel="${id}" id="inspector-panel-${id}" aria-labelledby="inspector-tab-${id}" ${inspectorTab === id ? "" : "hidden"}>${panelMarkup(id, detail.code)}</section>`).join("")}
    </div>
  </aside>`;
}

function stockTemplate(tab, detail) {
  if (detail.error) return `<div class="loading-panel"><strong>Chart unavailable</strong><span>${escapeHtml(detail.error)}</span><button class="outline-button" id="retry-stock">Retry</button></div>`;
  const latest = detail.latest;
  const previous = detail.bars.at(-2);
  const change = latest && previous && previous.close ? (latest.close - previous.close) / previous.close * 100 : null;
  const activeRows = visibleRows();
  const index = activeRows.findIndex((row) => row.code === tab.code);
  const periods = [["daily", "D", "Daily"], ["weekly", "W", "Weekly"], ["monthly", "M", "Monthly"]];
  const periodName = periods.find(([period]) => period === detail.period)?.[2] || "Daily";
  const activity = activitySeries(detail.bars);
  const inspectorWidth = effectiveInspectorWidth();
  return `<section class="stock-view">
    <header class="stock-toolbar"><div class="stock-identity"><h1>${escapeHtml(detail.name)}<span>${escapeHtml(detail.code)}</span></h1><div class="muted mono">${latest ? escapeHtml(latest.time) : "No market history"}${detail.partial ? " · partial data" : ""}</div></div><div class="stock-quote"><span class="stock-price">${formatNumber(latest?.close)}</span><span class="number ${changeClass(change)}">${change == null ? "—" : `${change > 0 ? "+" : ""}${formatNumber(change)}%`}</span></div><div class="periods" aria-label="Chart period">${periods.map(([period, label, name]) => `<button class="period-button ${tab.period === period ? "active" : ""}" data-period="${period}" aria-label="${name}" title="${name}">${label}</button>`).join("")}</div><div class="nav-buttons"><button class="ghost-button" data-neighbor="${escapeHtml(activeRows[index - 1]?.code || "")}" ${index <= 0 ? "disabled" : ""}>← Prev</button><button class="ghost-button" data-neighbor="${escapeHtml(activeRows[index + 1]?.code || "")}" ${index < 0 || index >= activeRows.length - 1 ? "disabled" : ""}>Next →</button></div><button class="ghost-button inspector-toggle" type="button" aria-controls="stock-inspector" aria-expanded="${!inspectorPreferences.collapsed}" aria-label="${inspectorPreferences.collapsed ? "Show" : "Hide"} stock information">ⓘ</button></header>
    <div class="stock-workspace ${inspectorPreferences.collapsed ? "inspector-collapsed" : ""}" style="--inspector-width:${inspectorWidth}px"><div class="stock-content">${detail.bars.length ? `<div class="chart-pane"><div class="chart-legend"><strong class="chart-period">${periodName} · ${detail.bars.length} bars · ${detail.hits.length} signals</strong><span class="chart-activity">${escapeHtml(activity.label)}</span><span class="ma5">MA5</span><span class="ma10">MA10</span><span class="ma20">MA20</span><span class="ma60">MA60</span></div><div class="chart" id="stock-chart"></div><a class="chart-watermark" href="https://www.tradingview.com/" target="_blank" rel="noopener">Charts by TradingView</a></div>` : `<div class="empty-state"><strong>No usable chart history</strong><span>No historical bars are available for this period.</span></div>`}</div><div class="inspector-resizer" role="separator" tabindex="0" aria-controls="stock-inspector" aria-label="Resize stock information panel" aria-orientation="vertical" aria-valuemin="${MINIMUM_INSPECTOR_WIDTH}" aria-valuemax="${maximumInspectorWidth(window.innerWidth)}" aria-valuenow="${inspectorWidth}" title="Drag or use arrow keys to resize · Press Enter to reset"></div>${inspectorTemplate(detail)}</div>
  </section>`;
}

function renderWorkspace() {
  if (!authenticated || !bootstrap) return;
  teardownWorkspaceView();
  const tab = workspace.tabs.find((item) => item.id === workspace.activeTab) || workspace.tabs[0];
  workspace = { ...workspace, activeTab: tab.id };
  const detail = tab.type === "stock" ? details.get(`${tab.code}:${tab.period}`) : null;
  const body = tab.type === "scan" ? scanTemplate() : (detail ? stockTemplate(tab, detail) : stockLoadingTemplate(tab));
  app.innerHTML = shellTemplate(body);
  bindShell(tab);
  if (tab.type === "stock") {
    if (!detail) loadStock(tab);
    else if (!detail.error) {
      if (detail.bars.length) {
        const chartGeneration = protectedViewGeneration;
        chartHandle = mountChart(app.querySelector("#stock-chart"), detail.bars, detail.hits, {
          onCandleSelect: (date) => {
            const activeStock = currentStockTab();
            if (!authenticated || chartGeneration !== protectedViewGeneration || activeStock?.code !== tab.code) return;
            chipRequestedDates.set(chipSelectionKey(tab.code), date);
            app.querySelector('[data-inspector-tab="chips"]')?.click();
          },
        });
      }
      queueMicrotask(() => {
        if (currentStockTab()?.code !== tab.code) return;
        ensureInspectorPanel("overview", tab.code);
        if (inspectorTab !== "overview") ensureInspectorPanel(inspectorTab, tab.code);
      });
    }
  }
}

function bindShell(activeTab) {
  app.querySelector("#logout").addEventListener("click", async () => {
    const logoutGeneration = renderSigningOut();
    try {
      await dashboardApi.logout();
      if (logoutGeneration !== protectedViewGeneration || authenticated) return;
      renderLogin();
    } catch {
      if (logoutGeneration !== protectedViewGeneration || authenticated) return;
      renderLogin("Sign out could not be confirmed. Please sign in again.");
    }
  });
  const settingsMenu = app.querySelector("#settings-menu");
  const toggleSettings = () => {
    settingsMenu.classList.toggle("hidden");
    app.querySelector("#settings").setAttribute("aria-expanded", String(!settingsMenu.classList.contains("hidden")));
  };
  app.querySelector("#settings").addEventListener("click", toggleSettings);
  app.querySelector("#mobile-settings")?.addEventListener("click", toggleSettings);
  app.querySelector("#filter-toggle")?.addEventListener("click", () => setFilterMenuOpen(!filterMenuOpen));
  app.querySelector("#filters")?.addEventListener("input", (event) => {
    const form = event.currentTarget;
    const changedField = event.target.name;
    filters = { ...filters, search: form.search.value, group: form.group.value, signal: form.signal.value, sort: form.sort.value, direction: form.direction.value, rankedOnly: form.rankedOnly.checked };
    renderWorkspace();
    if (changedField === "search") {
      const search = app.querySelector('#filters [name="search"]');
      search.focus();
      search.setSelectionRange(search.value.length, search.value.length);
    }
  });
  const filterForm = app.querySelector("#filters");
  if (filterForm) {
    filterForm.elements.sort.value = filters.sort;
    filterForm.elements.direction.value = filters.direction;
  }
  app.querySelector("#clear-filters")?.addEventListener("click", () => { filters = { search: "", group: "", signal: "", rankedOnly: false, sort: "ranked", direction: "desc" }; renderWorkspace(); });
  app.querySelectorAll("[data-tab]").forEach((button) => button.addEventListener("click", (event) => {
    if (event.target.closest("[data-close]")) return;
    workspace = { ...workspace, activeTab: button.dataset.tab };
    const tab = workspace.tabs.find((item) => item.id === button.dataset.tab);
    pushRoute(tab?.type === "stock" ? stockRoute(tab.code) : "#scan");
    renderWorkspace();
  }));
  app.querySelectorAll("[data-close]").forEach((button) => button.addEventListener("click", (event) => {
    event.stopPropagation();
    workspace = closeTab(workspace, button.dataset.close);
    const active = workspace.tabs.find((tab) => tab.id === workspace.activeTab);
    pushRoute(active?.type === "stock" ? stockRoute(active.code) : "#scan");
    renderWorkspace();
  }));
  app.querySelector("#refresh")?.addEventListener("click", () => loadBootstrap({ quiet: true }));
  app.querySelectorAll("[data-stock]").forEach((row) => row.addEventListener("click", () => openStock(row.dataset.stock)));
  app.querySelectorAll("[data-period]").forEach((button) => button.addEventListener("click", () => {
    workspace = updateTab(workspace, activeTab.id, { period: button.dataset.period });
    renderWorkspace();
  }));
  app.querySelectorAll("[data-neighbor]").forEach((button) => button.addEventListener("click", () => button.dataset.neighbor && openStock(button.dataset.neighbor)));
  app.querySelector("#retry-stock")?.addEventListener("click", () => {
    details.delete(`${activeTab.code}:${activeTab.period}`);
    renderWorkspace();
  });
  inspectorCleanup = bindInspector();
}

function bindInspector() {
  const stockWorkspace = app.querySelector(".stock-workspace");
  const inspector = app.querySelector(".stock-inspector");
  const toggle = app.querySelector(".inspector-toggle");
  if (!stockWorkspace || !inspector || !toggle) return null;

  let resizeFrame = null;
  const resizeChart = () => {
    if (resizeFrame !== null) return;
    resizeFrame = window.requestAnimationFrame(() => {
      resizeFrame = null;
      chartHandle?.resize?.();
    });
  };
  const resizer = app.querySelector(".inspector-resizer");
  const applyEffectiveWidth = () => {
    const width = effectiveInspectorWidth();
    stockWorkspace.style.setProperty("--inspector-width", `${width}px`);
    resizer?.setAttribute("aria-valuemax", String(maximumInspectorWidth(window.innerWidth)));
    resizer?.setAttribute("aria-valuenow", String(width));
    resizeChart();
  };
  const setCollapsed = (collapsed, { restoreFocus = false } = {}) => {
    inspectorPreferences = { ...inspectorPreferences, collapsed };
    saveInspectorPreferences(window.localStorage, inspectorPreferences);
    stockWorkspace.classList.toggle("inspector-collapsed", collapsed);
    toggle.setAttribute("aria-expanded", String(!collapsed));
    toggle.setAttribute("aria-label", `${collapsed ? "Show" : "Hide"} stock information`);
    if (restoreFocus) toggle.focus();
    resizeChart();
  };
  const setWidth = (width, { persist = false } = {}) => {
    inspectorPreferences = {
      ...inspectorPreferences,
      width: clampInspectorWidth(width, window.innerWidth),
    };
    if (persist) saveInspectorPreferences(window.localStorage, inspectorPreferences);
    applyEffectiveWidth();
  };

  toggle.addEventListener("click", () => setCollapsed(!inspectorPreferences.collapsed));
  const closeOverlay = () => {
    if (!stockWorkspace.classList.contains("inspector-collapsed")) {
      setCollapsed(true, { restoreFocus: true });
    }
  };
  closeInspectorOverlay = closeOverlay;
  const tabButtons = [...app.querySelectorAll("[data-inspector-tab]")];
  const activateTab = (button, { focus = false } = {}) => {
    inspectorTab = button.dataset.inspectorTab;
    tabButtons.forEach((candidate) => {
      const active = candidate === button;
      candidate.classList.toggle("active", active);
      candidate.setAttribute("aria-selected", String(active));
      candidate.tabIndex = active ? 0 : -1;
    });
    app.querySelectorAll("[data-inspector-panel]").forEach((panel) => {
      panel.hidden = panel.dataset.inspectorPanel !== inspectorTab;
    });
    const activeStock = currentStockTab();
    if (activeStock) {
      ensureInspectorPanel(inspectorTab, activeStock.code);
    }
    if (focus) button.focus();
  };
  tabButtons.forEach((button, index) => {
    button.addEventListener("click", () => activateTab(button));
    button.addEventListener("keydown", (event) => {
      let nextIndex = null;
      if (event.key === "ArrowRight") nextIndex = (index + 1) % tabButtons.length;
      else if (event.key === "ArrowLeft") nextIndex = (index - 1 + tabButtons.length) % tabButtons.length;
      else if (event.key === "Home") nextIndex = 0;
      else if (event.key === "End") nextIndex = tabButtons.length - 1;
      if (nextIndex === null) return;
      event.preventDefault();
      activateTab(tabButtons[nextIndex], { focus: true });
    });
  });
  const activeStock = currentStockTab();
  if (activeStock) {
    app.querySelectorAll("[data-inspector-panel]").forEach((panel) => {
      bindPanelActions(panel, panel.dataset.inspectorPanel, activeStock.code);
    });
  }

  if (resizer) {
    let drag = null;
    const finishDrag = (event) => {
      if (!drag || event.pointerId !== drag.pointerId) return;
      drag = null;
      resizer.classList.remove("dragging");
      saveInspectorPreferences(window.localStorage, inspectorPreferences);
      resizeChart();
    };
    const resetWidth = () => setWidth(DEFAULT_INSPECTOR_WIDTH, { persist: true });
    resizer.addEventListener("pointerdown", (event) => {
      if (event.button !== 0 || drag) return;
      event.preventDefault();
      drag = { pointerId: event.pointerId, startX: event.clientX, startWidth: effectiveInspectorWidth() };
      resizer.setPointerCapture(event.pointerId);
      resizer.classList.add("dragging");
    });
    resizer.addEventListener("pointermove", (event) => {
      if (!drag || event.pointerId !== drag.pointerId) return;
      setWidth(drag.startWidth + drag.startX - event.clientX);
    });
    resizer.addEventListener("pointerup", finishDrag);
    resizer.addEventListener("pointercancel", finishDrag);
    resizer.addEventListener("lostpointercapture", finishDrag);
    resizer.addEventListener("dblclick", (event) => {
      event.preventDefault();
      resetWidth();
    });
    resizer.addEventListener("keydown", (event) => {
      const width = effectiveInspectorWidth();
      let nextWidth = null;
      if (event.key === "ArrowLeft") nextWidth = width + 20;
      else if (event.key === "ArrowRight") nextWidth = width - 20;
      else if (event.key === "Home") nextWidth = maximumInspectorWidth(window.innerWidth);
      else if (event.key === "End") nextWidth = MINIMUM_INSPECTOR_WIDTH;
      else if (event.key === "Enter" || event.key === " ") nextWidth = DEFAULT_INSPECTOR_WIDTH;
      if (nextWidth === null) return;
      event.preventDefault();
      setWidth(nextWidth, { persist: true });
    });
  }
  const handleViewportResize = () => applyEffectiveWidth();
  window.addEventListener("resize", handleViewportResize);
  return () => {
    window.removeEventListener("resize", handleViewportResize);
    if (resizeFrame !== null) window.cancelAnimationFrame(resizeFrame);
    if (closeInspectorOverlay === closeOverlay) closeInspectorOverlay = null;
  };
}

function setFilterMenuOpen(open) {
  filterMenuOpen = open;
  app.querySelector("#filter-menu")?.classList.toggle("hidden", !open);
  app.querySelector("#filter-toggle")?.setAttribute("aria-expanded", String(open));
}

document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  if (filterMenuOpen) {
    setFilterMenuOpen(false);
    app.querySelector("#filter-toggle")?.focus();
    return;
  }
  if (window.matchMedia("(max-width: 700px)").matches) closeInspectorOverlay?.();
});

document.addEventListener("click", (event) => {
  if (filterMenuOpen && event.target instanceof Element && !event.target.closest(".filter-anchor")) {
    setFilterMenuOpen(false);
  }
});

function stockRoute(code) {
  return `#stock/${encodeURIComponent(code)}`;
}

function pushRoute(route) {
  if (location.hash !== route) history.pushState(null, "", route);
}

function openStock(code, { updateHistory = true } = {}) {
  if (!authenticated || !bootstrap) return;
  const row = rows.find((item) => item.code === code);
  if (!row) {
    workspace = { ...workspace, activeTab: "scan" };
    if (location.hash !== "#scan") history.replaceState(null, "", "#scan");
    renderWorkspace();
    return;
  }
  workspace = openStockTab(workspace, row);
  if (updateHistory) pushRoute(stockRoute(code));
  renderWorkspace();
}

async function loadStock(tab) {
  if (!authenticated) return;
  const key = `${tab.code}:${tab.period}`;
  const generation = protectedViewGeneration;
  const requestSequence = (stockRequestSequences.get(key) ?? 0) + 1;
  stockRequestSequences.set(key, requestSequence);
  const isCurrentRequest = () => authenticated
    && generation === protectedViewGeneration
    && stockRequestSequences.get(key) === requestSequence;
  try {
    const detail = await dashboardApi.stock(tab.code, tab.period);
    if (!isCurrentRequest()) return;
    details.set(key, detail);
  } catch (error) {
    if (!isCurrentRequest()) return;
    if (error.status === 401) return renderLogin("Your session expired. Please sign in again.");
    details.set(key, { error: error.message });
  }
  if (!isCurrentRequest()) return;
  if (workspace.activeTab === tab.id && workspace.tabs.find((item) => item.id === tab.id)?.period === tab.period) renderWorkspace();
}

function restoreLocation() {
  if (!authenticated || !bootstrap) return;
  const match = location.hash.match(/^#stock\/(.+)$/);
  if (match) {
    openStock(decodeURIComponent(match[1]), { updateHistory: false });
    return;
  }
  workspace = { ...workspace, activeTab: "scan" };
  if (location.hash !== "#scan") history.replaceState(null, "", "#scan");
  renderWorkspace();
}

function showTransientError(message) {
  const scan = app.querySelector(".scan-view");
  if (!scan) return;
  const banner = document.createElement("div");
  banner.className = "state-banner error";
  banner.textContent = message;
  scan.prepend(banner);
  setTimeout(() => banner.remove(), 5000);
}

async function start() {
  try {
    await dashboardApi.session();
    authenticated = true;
    if ((await loadBootstrap()) === true) scheduleRefresh();
  } catch (error) {
    if (error.status === 401 || error.status === 503) renderLogin(error.status === 503 ? "Dashboard authentication is not configured." : "");
    else renderFailure(error.message, start);
  }
}

start();
window.addEventListener("popstate", () => {
  if (authenticated && bootstrap) restoreLocation();
});
