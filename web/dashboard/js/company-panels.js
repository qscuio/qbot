function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function numeric(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function fixed(value, digits = 2) {
  const parsed = numeric(value);
  return parsed === null
    ? "—"
    : parsed.toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function formatCurrency(value) {
  const parsed = numeric(value);
  if (parsed === null) return "—";
  const absolute = Math.abs(parsed);
  if (absolute >= 100_000_000) return `${(parsed / 100_000_000).toFixed(2)}亿`;
  if (absolute >= 10_000) return `${(parsed / 10_000).toFixed(2)}万`;
  return parsed.toFixed(2);
}

function metric(label, value, className = "") {
  return `<div class="company-metric ${className}"><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd></div>`;
}

function percent(value) {
  const parsed = numeric(value);
  return parsed === null ? "—" : `${parsed > 0 ? "+" : ""}${fixed(parsed)}%`;
}

function unsignedPercent(value) {
  const parsed = numeric(value);
  return parsed === null ? "—" : `${fixed(parsed)}%`;
}

function date(value) {
  return value ? escapeHtml(value) : "—";
}

function revision(count) {
  return numeric(count) > 1 ? `<span class="revision-badge">修订 ${escapeHtml(count)}</span>` : "";
}

// The canonical estimator currently emits 30 buckets. Keep a defensive ceiling
// so malformed or future payloads cannot create an unbounded inspector DOM.
const MAX_CHIP_BUCKETS = 60;

export function chipPanel(payload = {}) {
  const distribution = (Array.isArray(payload.distribution) ? payload.distribution : [])
    .map((bucket) => ({
      price: numeric(bucket?.price),
      weight: numeric(bucket?.weight),
      percentage: numeric(bucket?.percentage),
    }))
    .filter((bucket) => bucket.price !== null && bucket.price > 0 && bucket.weight !== null && bucket.weight >= 0)
    .sort((left, right) => right.price - left.price)
    .slice(0, MAX_CHIP_BUCKETS);
  const maxWeight = Math.max(...distribution.map((bucket) => bucket.weight), 0);
  const rows = distribution.map((bucket) => {
    const width = maxWeight > 0 ? Math.max(1, Math.min(100, bucket.weight / maxWeight * 100)) : 0;
    const percentage = bucket.percentage === null ? bucket.weight * 100 : bucket.percentage;
    return `<div class="chip-bucket" data-chip-bucket><span class="chip-price mono">${escapeHtml(fixed(bucket.price))}</span><span class="chip-bar-track"><span class="chip-bar" style="--chip-width:${escapeHtml(Number(width.toFixed(2)))}%"></span></span><span class="chip-weight mono">${escapeHtml(fixed(percentage))}%</span></div>`;
  }).join("");
  const requested = payload.requestedDate
    ? `请求 ${escapeHtml(payload.requestedDate)} · 实际 ${date(payload.resolvedDate)}`
    : `最新 · ${date(payload.resolvedDate)}`;
  const model = payload.modelVersion || "—";
  return `<div class="chip-panel">
    <div class="panel-section-toolbar chip-toolbar"><strong>筹码分布</strong><button type="button" class="outline-button" data-chip-latest>Latest</button></div>
    <div class="chip-provenance"><strong>${escapeHtml(payload.sourceLabel || "来源未知")}</strong><span>${escapeHtml(payload.validationLabel || "未验证")}</span><span class="mono">${escapeHtml(model)}</span></div>
    <div class="chip-date-line">${requested}</div>
    <dl class="company-metric-grid chip-summary">
      ${metric("当前价", fixed(payload.currentPrice))}${metric("主峰", fixed(payload.dominantPeakPrice))}${metric("平均成本", fixed(payload.averageCost))}${metric("获利比例", unsignedPercent(payload.winnerRate))}${metric("集中度", unsignedPercent(payload.concentration))}${metric("更新时间", payload.sourceUpdatedAt ? date(payload.sourceUpdatedAt) : "—")}
    </dl>
    <section class="chip-distribution" aria-label="价格筹码分布">${rows || `<div class="panel-empty"><strong>暂无筹码分布</strong><span>该交易日没有可展示的规范化筹码桶。</span></div>`}</section>
  </div>`;
}

export function companyPanel(company = {}) {
  const quote = company.quote || {};
  const valuation = company.valuation || {};
  const identity = [company.industry, company.market, company.exchange].filter(Boolean).map(escapeHtml).join(" · ") || "暂无行业分类";
  return `<div class="company-overview">
    <section class="company-identity-card">
      <div><strong>${escapeHtml(company.name || company.code || "公司概览")}</strong><span>${identity}</span></div>
      <span class="company-code mono">${escapeHtml(company.code || "")}</span>
      <dl>${metric("上市日期", date(company.listDate))}${metric("最新交易日", date(quote.tradeDate))}</dl>
    </section>
    <section aria-labelledby="quote-heading"><h3 id="quote-heading">行情快照</h3><dl class="company-metric-grid">
      ${metric("收盘", fixed(quote.close))}${metric("成交量", fixed(quote.volume, 0))}${metric("成交额", formatCurrency(quote.amount))}${metric("换手率", percent(valuation.turnoverRate))}
    </dl></section>
    <section aria-labelledby="valuation-heading"><h3 id="valuation-heading">估值</h3><dl class="company-metric-grid">
      ${metric("市盈率", fixed(valuation.pe))}${metric("市净率", fixed(valuation.pb))}${metric("市销率", fixed(valuation.ps))}${metric("量比", fixed(valuation.volumeRatio))}${metric("总市值", formatCurrency(valuation.totalMarketValue))}${metric("流通市值", formatCurrency(valuation.circulatingMarketValue))}
    </dl></section>
  </div>`;
}

function financialTrend(items) {
  const chronological = [...items].reverse().slice(-12);
  const values = chronological.map((item) => numeric(item.netProfitParent)).filter((value) => value !== null);
  const max = Math.max(...values.map(Math.abs), 1);
  if (!values.length) return "";
  return `<section class="financial-trend" aria-label="净利润趋势"><h3>净利润趋势</h3><div class="trend-bars">${chronological.map((item) => {
    const value = numeric(item.netProfitParent);
    const height = value === null ? 0 : Math.max(4, Math.round(Math.abs(value) / max * 46));
    return `<div class="trend-point" title="${escapeHtml(item.endDate)} · ${escapeHtml(formatCurrency(value))}"><span class="trend-value ${value < 0 ? "negative" : ""}" style="height:${height}px"></span><small>${escapeHtml(String(item.endDate || "").slice(0, 4))}</small></div>`;
  }).join("")}</div></section>`;
}

function historyWindow(items, start = 0, size = 60) {
  const windowSize = Math.max(1, Math.min(60, Number(size) || 60));
  const maximumStart = Math.max(0, items.length - windowSize);
  const windowStart = Math.max(0, Math.min(maximumStart, Number(start) || 0));
  return { windowStart, windowSize, visible: items.slice(windowStart, windowStart + windowSize) };
}

export function financialPanel(payload = {}, { frequency = "annual", windowStart = 0, windowSize = 60 } = {}) {
  const items = Array.isArray(payload.items) ? payload.items : [];
  const quarterly = frequency === "quarterly";
  const window = historyWindow(items, windowStart, windowSize);
  const topHeight = window.windowStart * 42;
  const bottomHeight = Math.max(0, items.length - window.windowStart - window.visible.length) * 42;
  const table = items.length ? `<div class="panel-table-window" tabindex="0" aria-label="财务历史表格" data-history-kind="financials" data-history-total="${items.length}" data-history-start="${window.windowStart}" data-history-row-height="42"><table class="panel-table financial-table"><thead><tr><th>报告期</th><th>营收</th><th>净利润</th><th>同比</th><th>ROE</th></tr></thead><tbody>${topHeight ? `<tr class="history-spacer"><td colspan="5" style="height:${topHeight}px"></td></tr>` : ""}${window.visible.map((item) => `<tr data-history-row><td><strong>${date(item.endDate)}</strong><span>${date(item.announcementDate)} ${revision(item.revisionCount)}</span></td><td>${escapeHtml(formatCurrency(item.revenue ?? item.totalRevenue))}<span>${escapeHtml(percent(item.revenueYoy))}</span></td><td>${escapeHtml(formatCurrency(item.netProfitParent))}<span>扣非净利润 ${escapeHtml(formatCurrency(item.deductedNetProfit))}</span></td><td class="${numeric(item.netProfitYoy) < 0 ? "down" : "up"}">${escapeHtml(percent(item.netProfitYoy))}<span>EPS ${escapeHtml(fixed(item.basicEps))}</span></td><td>${escapeHtml(percent(item.roe))}<span>毛利 ${escapeHtml(percent(item.grossMargin))}</span></td></tr>`).join("")}${bottomHeight ? `<tr class="history-spacer"><td colspan="5" style="height:${bottomHeight}px"></td></tr>` : ""}</tbody></table></div>` : `<div class="panel-empty"><strong>暂无${quarterly ? "季度" : "年度"}财务记录</strong><span>数据将在下一次公司资料同步后显示。</span></div>`;
  return `<div class="financial-panel">
    <div class="panel-section-toolbar"><div class="segmented-control" role="group" aria-label="财务周期"><button type="button" data-financial-frequency="annual" aria-pressed="${!quarterly}" class="${!quarterly ? "active" : ""}">年度</button><button type="button" data-financial-frequency="quarterly" aria-pressed="${quarterly}" class="${quarterly ? "active" : ""}">季度</button></div><span>${items.length} 条记录</span></div>
    ${financialTrend(items)}${table}
    ${payload.nextCursor ? `<button class="panel-load-more" type="button" data-load-more="financials">加载更早记录</button>` : ""}
  </div>`;
}

const statusLabels = {
  implemented: "已实施",
  approved: "已批准",
  proposed: "预案",
  unknown: "状态未知",
};

export function dividendPanel(payload = {}, { windowStart = 0, windowSize = 60 } = {}) {
  const items = Array.isArray(payload.items) ? payload.items : [];
  const window = historyWindow(items, windowStart, windowSize);
  const topHeight = window.windowStart * 101;
  const bottomHeight = Math.max(0, items.length - window.windowStart - window.visible.length) * 101;
  const content = items.length ? `<div class="dividend-list" tabindex="0" data-history-kind="dividends" data-history-total="${items.length}" data-history-start="${window.windowStart}" data-history-row-height="101">${topHeight ? `<div class="history-spacer" style="height:${topHeight}px"></div>` : ""}${window.visible.map((item) => {
    const status = item.implementationStatus || "unknown";
    return `<article class="dividend-card"><header><strong>${date(item.exDate || item.announcementDate)}</strong><span class="status-badge status-${escapeHtml(status)}">${escapeHtml(statusLabels[status] || statusLabels.unknown)}</span>${revision(item.revisionCount)}</header><div class="dividend-values"><span>现金 ${escapeHtml(fixed(item.cashDividend))}</span><span>送转 ${escapeHtml(fixed(item.stockRatio))}</span></div><dl><div><dt>公告</dt><dd>${date(item.announcementDate)}</dd></div><div><dt>股权登记</dt><dd>${date(item.recordDate)}</dd></div><div><dt>除权除息</dt><dd>${date(item.exDate)}</dd></div><div><dt>派付</dt><dd>${date(item.payDate)}</dd></div></dl></article>`;
  }).join("")}${bottomHeight ? `<div class="history-spacer" style="height:${bottomHeight}px"></div>` : ""}</div>` : `<div class="panel-empty"><strong>暂无分红记录</strong><span>已同步的历史中没有可展示的分红方案。</span></div>`;
  return `<div class="dividend-panel"><div class="panel-section-toolbar"><strong>历史分红</strong><span>${items.length} 条记录</span></div>${content}${payload.nextCursor ? `<button class="panel-load-more" type="button" data-load-more="dividends">加载更早记录</button>` : ""}</div>`;
}
