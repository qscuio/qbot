import test from "node:test";
import assert from "node:assert/strict";

import {
  chipPanel,
  companyPanel,
  dividendPanel,
  financialPanel,
  formatCurrency,
} from "../js/company-panels.js";

test("chipPanel renders escaped provenance, summary metrics, and bounded normalized bars", () => {
  const html = chipPanel({
    code: "600519.SH<script>",
    requestedDate: "2026-07-19<bad>",
    resolvedDate: "2026-07-17",
    currentPrice: 1488.5,
    averageCost: 1475,
    winnerRate: 72.5,
    concentration: 80,
    dominantPeakPrice: 1500,
    sourceLabel: "QBot 估算<img>",
    validationLabel: "已验证",
    modelVersion: "qbot-chip-v2<script>",
    distribution: Array.from({ length: 100 }, (_, index) => ({
      price: 1600 - index,
      weight: index === 0 ? 0.2 : 0.1,
      percentage: index === 0 ? 20 : 10,
    })),
  });

  assert.match(html, /平均成本/);
  assert.match(html, /当前价/);
  assert.match(html, /主峰/);
  assert.match(html, /获利比例/);
  assert.match(html, /集中度/);
  assert.match(html, /请求 2026-07-19&lt;bad&gt;/);
  assert.match(html, /QBot 估算&lt;img&gt;/);
  assert.match(html, /qbot-chip-v2&lt;script&gt;/);
  assert.doesNotMatch(html, /<script>|<img>/);
  assert.equal((html.match(/data-chip-bucket/g) || []).length, 60);
  assert.match(html, /style="--chip-width:100%"/);
  assert.match(html, /data-chip-latest/);
});

test("chipPanel handles missing and invalid distributions without throwing", () => {
  const empty = chipPanel({ distribution: [] });
  const invalid = chipPanel({ distribution: [{ price: "bad", weight: -1 }, null] });
  assert.match(empty, /暂无筹码分布/);
  assert.match(invalid, /暂无筹码分布/);
});

test("formatCurrency uses exact Chinese market units", () => {
  assert.equal(formatCurrency(86_240_000_000), "862.40亿");
  assert.equal(formatCurrency("12345678.9"), "1234.57万");
  assert.equal(formatCurrency(998.5), "998.50");
  assert.equal(formatCurrency(null), "—");
});

test("companyPanel renders compact identity and valuation without unsafe markup", () => {
  const html = companyPanel({
    code: "600519.SH",
    name: "贵州<茅台>",
    industry: "白酒",
    market: "主板",
    exchange: "SSE",
    listDate: "2001-08-27",
    quote: { tradeDate: "2026-07-17", close: "1488.50", volume: 123456 },
    valuation: { pe: "22.1", pb: "7.2", totalMarketValue: "1862400000000" },
  });

  assert.match(html, /贵州&lt;茅台&gt;/);
  assert.match(html, /市盈率/);
  assert.match(html, /18624\.00亿/);
  assert.doesNotMatch(html, /贵州<茅台>/);
});

test("financialPanel renders selection, revisions, and professional metrics", () => {
  const html = financialPanel({
    items: [{
      endDate: "2025-12-31",
      announcementDate: "2026-03-31",
      reportType: "1",
      frequency: "annual",
      revenue: "120000000000",
      netProfitParent: "86240000000",
      deductedNetProfit: "85000000000",
      revenueYoy: "12.3",
      netProfitYoy: "14.2",
      basicEps: "3.21",
      roe: "31.2",
      revisionCount: 2,
    }],
    nextCursor: "older",
  }, { frequency: "annual" });

  assert.match(html, /aria-pressed="true"[^>]*>年度/);
  assert.match(html, /净利润/);
  assert.match(html, /扣非净利润/);
  assert.match(html, /修订 2/);
  assert.match(html, /data-financial-frequency="quarterly"/);
  assert.match(html, /data-load-more="financials"/);
});

test("financialPanel supports quarterly selection and empty state", () => {
  const html = financialPanel({ items: [], nextCursor: null }, { frequency: "quarterly" });
  assert.match(html, /aria-pressed="true"[^>]*>季度/);
  assert.match(html, /暂无季度财务记录/);
});

test("dividendPanel renders status, revision, and pagination labels", () => {
  const html = dividendPanel({
    items: [{
      announcementDate: "2026-04-01",
      recordDate: "2026-06-20",
      exDate: "2026-06-21",
      payDate: "2026-06-25",
      implementationStatus: "implemented",
      cashDividend: "2.76",
      stockRatio: "0.1",
      revisionCount: 3,
    }],
    nextCursor: "next page",
  });

  assert.match(html, /已实施/);
  assert.match(html, /修订 3/);
  assert.match(html, /现金 2\.76/);
  assert.match(html, /data-load-more="dividends"/);
});

test("dividendPanel explains an empty history", () => {
  assert.match(dividendPanel({ items: [], nextCursor: null }), /暂无分红记录/);
});

test("long financial and dividend histories render bounded DOM windows", () => {
  const financialItems = Array.from({ length: 180 }, (_, index) => ({
    endDate: `${2025 - Math.floor(index / 4)}-12-31`,
    reportType: String(index),
    netProfitParent: index,
  }));
  const dividendItems = Array.from({ length: 180 }, (_, index) => ({
    announcementDate: `${2025 - Math.floor(index / 12)}-01-01`,
    exDate: `row-${index}`,
    implementationStatus: "implemented",
  }));

  const financial = financialPanel({ items: financialItems }, { windowStart: 80 });
  const dividends = dividendPanel({ items: dividendItems }, { windowStart: 80 });
  assert.ok((financial.match(/<tr data-history-row/g) || []).length <= 60);
  assert.ok((dividends.match(/<article class="dividend-card"/g) || []).length <= 60);
  assert.match(financial, /data-history-total="180"/);
  assert.match(dividends, /data-history-total="180"/);
  assert.match(dividends, /row-80/);
});

test("history windows expose exact first, middle, and end spacer geometry", () => {
  const financialItems = Array.from({ length: 180 }, (_, index) => ({
    endDate: `financial-${index}`,
    reportType: String(index),
  }));
  const dividendItems = Array.from({ length: 180 }, (_, index) => ({
    announcementDate: `dividend-${index}`,
    exDate: `dividend-${index}`,
    implementationStatus: "implemented",
  }));

  const financialFirst = financialPanel({ items: financialItems }, { windowStart: 0 });
  const financialMiddle = financialPanel({ items: financialItems }, { windowStart: 80 });
  const financialEnd = financialPanel({ items: financialItems }, { windowStart: 999 });
  assert.match(financialFirst, /financial-0/);
  assert.doesNotMatch(financialFirst, /financial-60/);
  assert.match(financialMiddle, /data-history-start="80"/);
  assert.match(financialMiddle, /height:3360px/);
  assert.match(financialMiddle, /height:1680px/);
  assert.match(financialEnd, /data-history-start="120"/);
  assert.match(financialEnd, /financial-179/);

  const dividendFirst = dividendPanel({ items: dividendItems }, { windowStart: 0 });
  const dividendMiddle = dividendPanel({ items: dividendItems }, { windowStart: 80 });
  const dividendEnd = dividendPanel({ items: dividendItems }, { windowStart: 999 });
  assert.match(dividendFirst, /dividend-0/);
  assert.doesNotMatch(dividendFirst, /dividend-60/);
  assert.match(dividendMiddle, /data-history-start="80"/);
  assert.match(dividendMiddle, /height:8080px/);
  assert.match(dividendMiddle, /height:4040px/);
  assert.match(dividendEnd, /data-history-start="120"/);
  assert.match(dividendEnd, /dividend-179/);
  assert.doesNotMatch(dividendMiddle, /display: grid/);
});
