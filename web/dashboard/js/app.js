import { dashboardApi, ApiError } from "./api.js?v=20260719.1";
import { activitySeries, mountChart } from "./chart.js?v=20260719.1";
import {
  applyFilters,
  closeTab,
  createWorkspaceState,
  normalizeRows,
  openStockTab,
  sortRows,
  updateTab,
} from "./state.js?v=20260719.1";

const app = document.querySelector("#app");
let bootstrap = null;
let rows = [];
let workspace = createWorkspaceState();
let filters = { search: "", group: "", signal: "", rankedOnly: false, sort: "ranked", direction: "desc" };
const details = new Map();
let chartHandle = null;
let refreshTimer = null;

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
    try {
      await dashboardApi.login(event.currentTarget.username.value, event.currentTarget.password.value);
      await loadBootstrap();
      scheduleRefresh();
    } catch (error) {
      errorNode.textContent = error.status === 429 ? "Too many attempts. Try again later." : "Invalid username or password.";
      button.disabled = false;
      button.textContent = "Sign in";
    }
  });
}

async function loadBootstrap({ quiet = false } = {}) {
  if (!quiet) app.innerHTML = `<div class="boot-screen"><span class="spinner"></span><span>Loading latest scan…</span></div>`;
  try {
    bootstrap = await dashboardApi.bootstrap();
    rows = normalizeRows(bootstrap.results);
    restoreLocation();
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) return renderLogin();
    if (quiet) return showTransientError("Refresh failed. Existing results are unchanged.");
    renderFailure(error.message);
  }
}

function renderFailure(message) {
  app.innerHTML = `<div class="boot-screen"><strong>QBot is unavailable</strong><span>${escapeHtml(message)}</span><button class="outline-button" id="retry">Retry</button></div>`;
  app.querySelector("#retry").addEventListener("click", () => loadBootstrap());
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
        ${sidebarTemplate()}
        <main class="editor"><div class="tabs" role="tablist"><button class="ghost-button mobile-filter" id="mobile-filter">Filters</button><button class="ghost-button mobile-settings" id="mobile-settings" aria-label="Settings">⚙</button>${tabsTemplate()}</div><div class="editor-body">${body}</div></main>
      </div>
      <footer class="statusbar">
        <span><i class="status-dot"></i>API connected</span>
        <span>${bootstrap.marketOpen ? "Market open" : "Market closed"}</span>
        <div class="statusbar-right"><span>Scan ${escapeHtml(formatTime(bootstrap.scannedAt))}</span><span>${visible.length} / ${rows.length} stocks</span></div>
      </footer>
    </div>`;
}

function sidebarTemplate() {
  const groups = [...new Set(bootstrap.catalog.map((item) => item.group))].sort();
  return `<aside class="sidebar" id="sidebar">
    <div class="sidebar-title">Scan explorer</div>
    <div class="section-title">Filters</div>
    <form class="filter-form" id="filters">
      <label>Stock search<input class="control" name="search" value="${escapeHtml(filters.search)}" placeholder="Code or name"></label>
      <label>Signal group<select class="control" name="group"><option value="">All groups</option>${groups.map((group) => `<option value="${escapeHtml(group)}" ${filters.group === group ? "selected" : ""}>${escapeHtml(group)}</option>`).join("")}</select></label>
      <label>Signal<select class="control" name="signal"><option value="">All signals</option>${bootstrap.catalog.map((signal) => `<option value="${escapeHtml(signal.id)}" ${filters.signal === signal.id ? "selected" : ""}>${escapeHtml(signal.name)}</option>`).join("")}</select></label>
      <label>Sort<select class="control" name="sort"><option value="ranked">Ranked priority</option><option value="code">Stock code</option><option value="name">Stock name</option><option value="hits">Hit count</option><option value="change">Price change</option></select></label>
      <label>Direction<select class="control" name="direction"><option value="desc">Descending</option><option value="asc">Ascending</option></select></label>
      <label class="checkbox"><input type="checkbox" name="rankedOnly" ${filters.rankedOnly ? "checked" : ""}> Ranked pools only</label>
      <button class="outline-button" type="button" id="clear-filters">Clear filters</button>
    </form>
    <div class="section-title">Latest run</div>
    <div class="metrics-list">
      <div class="metric-row"><span>Unique stocks</span><strong>${bootstrap.summary.uniqueStocks}</strong></div>
      <div class="metric-row"><span>Total hits</span><strong>${bootstrap.summary.totalHits}</strong></div>
      <div class="metric-row"><span>Active signals</span><strong>${bootstrap.summary.activeSignals}</strong></div>
      <div class="metric-row"><span>Ranked candidates</span><strong>${bootstrap.summary.rankedCandidates}</strong></div>
    </div>
  </aside>`;
}

function tabsTemplate() {
  return workspace.tabs.map((tab) => `<button class="tab ${workspace.activeTab === tab.id ? "active" : ""}" data-tab="${escapeHtml(tab.id)}" role="tab"><span class="tab-icon">${tab.type === "scan" ? "⌁" : "K"}</span><span class="tab-label">${escapeHtml(tab.label)}</span>${tab.closable ? `<span class="tab-close" data-close="${escapeHtml(tab.id)}" title="Close">×</span>` : ""}</button>`).join("");
}

function scanTemplate() {
  const visible = visibleRows();
  return `<section class="scan-view">
    <div class="view-heading"><div><h1>Latest scan</h1><p>${escapeHtml(bootstrap.runId ? `Run ${bootstrap.runId}` : "Awaiting the first scan")}</p></div><div class="view-heading-actions"><button class="outline-button" id="refresh">↻ Refresh</button></div></div>
    ${freshnessBanner()}
    ${visible.length ? `<table class="data-grid"><thead><tr><th>Security</th><th>Close</th><th>Change</th><th>Signals</th><th>Hits</th></tr></thead><tbody>${visible.map((row) => `<tr data-stock="${escapeHtml(row.code)}"><td><div class="stock-cell"><span class="stock-code mono">${escapeHtml(row.code)}</span><span class="stock-name">${escapeHtml(row.name)}</span></div></td><td class="number">${formatNumber(row.close)}</td><td class="number ${changeClass(row.changePct)}">${row.changePct == null ? "—" : `${row.changePct > 0 ? "+" : ""}${formatNumber(row.changePct)}%`}</td><td><div class="badges">${row.hits.map((hit) => `<span class="badge ${hit.isRankedPool ? "ranked" : ""}" title="${escapeHtml(hit.name)}">${escapeHtml(hit.icon)} ${escapeHtml(hit.name)}</span>`).join("")}</div></td><td class="number">${row.hitCount}</td></tr>`).join("")}</tbody></table>` : `<div class="empty-state"><strong>${rows.length ? "No results match these filters" : "The latest scan contains no hits"}</strong><span>${rows.length ? "Clear or adjust the explorer filters." : "A successful zero-hit scan is valid."}</span></div>`}
  </section>`;
}

function stockLoadingTemplate(tab) {
  return `<div class="loading-panel"><span class="spinner"></span><span>Loading ${escapeHtml(tab.code)} market history…</span></div>`;
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
  return `<section class="stock-view">
    <header class="stock-toolbar"><div class="stock-identity"><h1>${escapeHtml(detail.name)}<span>${escapeHtml(detail.code)}</span></h1><div class="muted mono">${latest ? escapeHtml(latest.time) : "No market history"}${detail.partial ? " · partial data" : ""}</div></div><div class="stock-quote"><span class="stock-price">${formatNumber(latest?.close)}</span><span class="number ${changeClass(change)}">${change == null ? "—" : `${change > 0 ? "+" : ""}${formatNumber(change)}%`}</span></div><div class="periods" aria-label="Chart period">${periods.map(([period, label, name]) => `<button class="period-button ${tab.period === period ? "active" : ""}" data-period="${period}" aria-label="${name}" title="${name}">${label}</button>`).join("")}</div><div class="nav-buttons"><button class="ghost-button" data-neighbor="${escapeHtml(activeRows[index - 1]?.code || "")}" ${index <= 0 ? "disabled" : ""}>← Prev</button><button class="ghost-button" data-neighbor="${escapeHtml(activeRows[index + 1]?.code || "")}" ${index < 0 || index >= activeRows.length - 1 ? "disabled" : ""}>Next →</button></div></header>
    ${detail.bars.length ? `<div class="stock-content"><div class="chart-pane"><div class="chart-legend"><strong class="chart-period">${periodName} · ${detail.bars.length} bars · ${detail.hits.length} signals</strong><span class="chart-activity">${escapeHtml(activity.label)}</span><span class="ma5">MA5</span><span class="ma10">MA10</span><span class="ma20">MA20</span><span class="ma60">MA60</span></div><div class="chart" id="stock-chart"></div><a class="chart-watermark" href="https://www.tradingview.com/" target="_blank" rel="noopener">Charts by TradingView</a></div></div>` : `<div class="empty-state"><strong>No usable chart history</strong><span>No historical bars are available for this period.</span></div>`}
  </section>`;
}

function renderWorkspace() {
  chartHandle?.destroy();
  chartHandle = null;
  const tab = workspace.tabs.find((item) => item.id === workspace.activeTab) || workspace.tabs[0];
  workspace = { ...workspace, activeTab: tab.id };
  const detail = tab.type === "stock" ? details.get(`${tab.code}:${tab.period}`) : null;
  const body = tab.type === "scan" ? scanTemplate() : (detail ? stockTemplate(tab, detail) : stockLoadingTemplate(tab));
  app.innerHTML = shellTemplate(body);
  bindShell(tab);
  if (tab.type === "stock") {
    if (!detail) loadStock(tab);
    else if (!detail.error && detail.bars.length) chartHandle = mountChart(app.querySelector("#stock-chart"), detail.bars, detail.hits);
  }
}

function bindShell(activeTab) {
  app.querySelector("#logout").addEventListener("click", async () => {
    try { await dashboardApi.logout(); } finally { renderLogin(); }
  });
  const settingsMenu = app.querySelector("#settings-menu");
  const toggleSettings = () => {
    settingsMenu.classList.toggle("hidden");
    app.querySelector("#settings").setAttribute("aria-expanded", String(!settingsMenu.classList.contains("hidden")));
  };
  app.querySelector("#settings").addEventListener("click", toggleSettings);
  app.querySelector("#mobile-settings")?.addEventListener("click", toggleSettings);
  app.querySelector("#mobile-filter")?.addEventListener("click", () => app.querySelector("#sidebar").classList.toggle("open"));
  app.querySelector("#filters").addEventListener("input", (event) => {
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
  app.querySelector("#filters select[name=sort]").value = filters.sort;
  app.querySelector("#filters select[name=direction]").value = filters.direction;
  app.querySelector("#clear-filters").addEventListener("click", () => { filters = { search: "", group: "", signal: "", rankedOnly: false, sort: "ranked", direction: "desc" }; renderWorkspace(); });
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
}

function stockRoute(code) {
  return `#stock/${encodeURIComponent(code)}`;
}

function pushRoute(route) {
  if (location.hash !== route) history.pushState(null, "", route);
}

function openStock(code, { updateHistory = true } = {}) {
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
  const key = `${tab.code}:${tab.period}`;
  try {
    details.set(key, await dashboardApi.stock(tab.code, tab.period));
  } catch (error) {
    if (error.status === 401) return renderLogin("Your session expired. Please sign in again.");
    details.set(key, { error: error.message });
  }
  if (workspace.activeTab === tab.id && workspace.tabs.find((item) => item.id === tab.id)?.period === tab.period) renderWorkspace();
}

function restoreLocation() {
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
    await loadBootstrap();
    scheduleRefresh();
  } catch (error) {
    if (error.status === 401 || error.status === 503) renderLogin(error.status === 503 ? "Dashboard authentication is not configured." : "");
    else renderFailure(error.message);
  }
}

start();
window.addEventListener("popstate", () => {
  if (bootstrap) restoreLocation();
});
