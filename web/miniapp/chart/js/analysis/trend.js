/**
 * Viewport-Aware Multi-Scale Channel System v6
 * 
 * v6 adds:
 * 1. Dual-scale ZigZag: short-scale swing channels + long-scale main trend
 * 2. Main trend channel (1 only) with relaxed filtering  
 * 3. Short swing channels (MAX_CHANNELS) for local structure
 * 4. Range boxes for horizontal consolidation
 */

import { state, charts } from '../core/state.js';
import { INDICATOR_COLORS } from '../core/config.js';
import { calculateATR } from '../chart/indicators.js';
import { tipsEngine } from './tips.js';

// ============================================================================
// Configuration
// ============================================================================

const DEBOUNCE_MS = 80;

// Short-scale swing channels
const MAX_SWING_CHANNELS = 2;
const SWING_CONTAINMENT = 0.75;

// Long-scale main trend channel
const MAIN_CONTAINMENT = 0.70;  // More relaxed for main trend

// Range boxes
const MAX_RANGES = 1;
const RANGE_WIN_FRAC = 0.28;
const RANGE_MIN_WIN = 35;
const RANGE_STEP_FRAC = 0.20;
const RANGE_CONTAINMENT = 0.88;

// Shared
const RECENT_TOUCH_FRAC = 0.25;
const MIN_SEG_BARS_FRAC = 1 / 18;
const MIN_SEG_BARS_ABS = 12;

// ============================================================================
// Utilities
// ============================================================================

const typicalPrice = (b) => (b.high + b.low + b.close) / 3;

function quantile(arr, q) {
    if (!arr.length) return 0;
    const a = arr.slice().sort((x, y) => x - y);
    const pos = (a.length - 1) * q;
    const base = Math.floor(pos);
    const rest = pos - base;
    return a[base + 1] !== undefined ? a[base] + rest * (a[base + 1] - a[base]) : a[base];
}

function linearRegression(xs, ys) {
    const n = xs.length;
    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    for (let i = 0; i < n; i++) {
        sumX += xs[i]; sumY += ys[i]; sumXY += xs[i] * ys[i]; sumX2 += xs[i] * xs[i];
    }
    const denom = n * sumX2 - sumX * sumX;
    if (Math.abs(denom) < 1e-12) return { slope: 0, intercept: ys[0] ?? 0 };
    const slope = (n * sumXY - sumX * sumY) / denom;
    const intercept = (sumY - slope * sumX) / n;
    return { slope, intercept };
}

// ============================================================================
// Adaptive Parameters (Dual-Scale)
// ============================================================================

function makeParams(N, visibleData) {
    const atr = calculateATR(visibleData, Math.min(14, Math.max(2, visibleData.length - 1)));
    const avgPrice = visibleData.reduce((s, d) => s + d.close, 0) / visibleData.length;

    // Short-scale: for swing channels
    const kShort = N < 120 ? 1.2 : N < 260 ? 1.6 : 2.0;
    const revShort = Math.max(kShort * atr, avgPrice * 0.008);

    // Long-scale: for main trend (fewer turning points)
    const kLong = N < 160 ? 2.6 : N < 320 ? 3.2 : 3.8;
    const revLong = Math.max(kLong * atr, avgPrice * 0.015);

    const useLog = state.isLogScale;

    const minSegBarsShort = Math.max(MIN_SEG_BARS_ABS, Math.floor(N * MIN_SEG_BARS_FRAC));
    const minSegBarsLong = Math.max(30, Math.floor(N * 0.22));

    const tol = Math.min(atr * 0.35, avgPrice * 0.006);

    return { atr, avgPrice, revShort, revLong, useLog, minSegBarsShort, minSegBarsLong, tol };
}

// ============================================================================
// ZigZag Segmentation
// ============================================================================

function zigzag(visibleData, baseIdx, rev) {
    const priceAt = (i) => typicalPrice(visibleData[i]);
    const pts = [];
    let lastLocal = 0, lastPrice = priceAt(0), dir = 0;
    let extLocal = 0, extPrice = lastPrice;

    for (let i = 1; i < visibleData.length; i++) {
        const p = priceAt(i);
        if (dir === 0) {
            if (p - lastPrice >= rev) {
                dir = 1; extLocal = i; extPrice = p;
                pts.push({ globalIdx: baseIdx + lastLocal, localIdx: lastLocal, price: lastPrice, type: 'L', time: visibleData[lastLocal].time });
            } else if (lastPrice - p >= rev) {
                dir = -1; extLocal = i; extPrice = p;
                pts.push({ globalIdx: baseIdx + lastLocal, localIdx: lastLocal, price: lastPrice, type: 'H', time: visibleData[lastLocal].time });
            }
            continue;
        }
        if (dir === 1) {
            if (p > extPrice) { extPrice = p; extLocal = i; }
            if (extPrice - p >= rev) {
                pts.push({ globalIdx: baseIdx + extLocal, localIdx: extLocal, price: extPrice, type: 'H', time: visibleData[extLocal].time });
                lastLocal = extLocal; lastPrice = extPrice; dir = -1; extLocal = i; extPrice = p;
            }
        } else {
            if (p < extPrice) { extPrice = p; extLocal = i; }
            if (p - extPrice >= rev) {
                pts.push({ globalIdx: baseIdx + extLocal, localIdx: extLocal, price: extPrice, type: 'L', time: visibleData[extLocal].time });
                lastLocal = extLocal; lastPrice = extPrice; dir = 1; extLocal = i; extPrice = p;
            }
        }
    }
    pts.push({ globalIdx: baseIdx + extLocal, localIdx: extLocal, price: extPrice, type: dir === 1 ? 'H' : 'L', time: visibleData[extLocal].time });

    const cleaned = [];
    for (const p of pts) {
        const last = cleaned[cleaned.length - 1];
        if (!last || last.type !== p.type) { cleaned.push(p); continue; }
        if (p.type === 'H' ? p.price >= last.price : p.price <= last.price) cleaned[cleaned.length - 1] = p;
    }
    return cleaned;
}

function buildSegments(points, baseIdx, minSegBars, minAmp) {
    const segs = [];
    for (let i = 0; i < points.length - 1; i++) {
        const a = points[i], b = points[i + 1];
        const bars = b.globalIdx - a.globalIdx;
        if (bars < minSegBars || Math.abs(b.price - a.price) < minAmp) continue;
        segs.push({
            startGlobalIdx: a.globalIdx, endGlobalIdx: b.globalIdx,
            startLocal: a.localIdx, endLocal: b.localIdx,
            dir: b.price > a.price ? 1 : -1,
            startTime: a.time, endTime: b.time,
            bars,
        });
    }

    // Merge same-direction adjacent segments
    const merged = [];
    for (const s of segs) {
        const prev = merged[merged.length - 1];
        if (prev && prev.dir === s.dir && s.startLocal <= prev.endLocal + Math.floor(minSegBars / 2)) {
            prev.endGlobalIdx = s.endGlobalIdx;
            prev.endLocal = s.endLocal;
            prev.endTime = s.endTime;
            prev.bars = prev.endLocal - prev.startLocal;
        } else {
            merged.push({ ...s });
        }
    }
    return merged;
}

// ============================================================================
// Channel Fitting (supports mode: 'swing' | 'main')
// ============================================================================

function fitChannelForSegment(visibleData, baseIdx, seg, params, mode = 'swing') {
    const { useLog, atr, avgPrice, tol } = params;
    const toY = useLog ? (p) => Math.log(Math.max(p, 1e-9)) : (p) => p;
    const fromY = useLog ? (y) => Math.exp(y) : (y) => y;

    const xs = [], ys = [];
    for (let i = seg.startLocal; i <= seg.endLocal; i++) {
        xs.push(baseIdx + i);
        ys.push(toY(typicalPrice(visibleData[i])));
    }
    if (xs.length < 6) return null;

    const { slope, intercept } = linearRegression(xs, ys);
    const midY = (x) => slope * x + intercept;

    const resH = [], resL = [];
    for (let i = seg.startLocal; i <= seg.endLocal; i++) {
        const x = baseIdx + i;
        resH.push(toY(visibleData[i].high) - midY(x));
        resL.push(toY(visibleData[i].low) - midY(x));
    }

    const bars = seg.endLocal - seg.startLocal + 1;
    const qUp = bars < 30 ? 0.90 : 0.95;
    const qDn = bars < 30 ? 0.10 : 0.05;
    const offUp = quantile(resH, qUp);
    const offDn = quantile(resL, qDn);

    const getMid = (gx) => fromY(midY(gx));
    const getUp = (gx) => fromY(midY(gx) + offUp);
    const getDn = (gx) => fromY(midY(gx) + offDn);

    // Scoring with mode-specific thresholds
    const isMain = mode === 'main';
    const MINC = isMain ? MAIN_CONTAINMENT : SWING_CONTAINMENT;
    const maxWidthAtr = isMain ? 12.0 : 10.0;
    const minRecentTouches = isMain ? 0 : 1;  // Main trend doesn't require recent touches

    const n = bars;
    const recentStart = seg.endLocal - Math.max(8, Math.floor(n * RECENT_TOUCH_FRAC));
    let contain = 0, touchesUp = 0, touchesDn = 0;

    const midLocal = Math.floor((seg.startLocal + seg.endLocal) / 2);
    const width = getUp(baseIdx + midLocal) - getDn(baseIdx + midLocal);
    if (width < 0.6 * atr || width > maxWidthAtr * atr) return null;

    for (let i = seg.startLocal; i <= seg.endLocal; i++) {
        const gx = baseIdx + i;
        const b = visibleData[i];
        if (b.high <= getUp(gx) + tol && b.low >= getDn(gx) - tol) contain++;
        if (i >= recentStart) {
            if (Math.abs(b.high - getUp(gx)) <= tol) touchesUp++;
            if (Math.abs(b.low - getDn(gx)) <= tol) touchesDn++;
        }
    }

    const containment = contain / n;
    const recentTouches = touchesUp + touchesDn;
    if (containment < MINC || recentTouches < minRecentTouches) return null;

    // Score: main trend favors span, swing favors recent touches
    const score = containment * 100 + (isMain ? bars * 0.15 : recentTouches * 12) - Math.abs(width / atr - 3) * 4;

    return {
        regime: mode, startGlobalIdx: seg.startGlobalIdx, endGlobalIdx: seg.endGlobalIdx,
        startLocal: seg.startLocal, endLocal: seg.endLocal, dir: seg.dir,
        getMid, getUp, getDn, score, containment, recentTouches, bars,
    };
}

// ============================================================================
// Pick Main Trend Segment (longest + rightmost)
// ============================================================================

function pickMainTrendSegment(segs, baseIdx, N) {
    if (!segs.length) return null;
    const rightEdge = baseIdx + N - 1;
    let best = null, bestScore = -Infinity;

    for (const s of segs) {
        const span = s.bars;
        const rightness = 1 - (rightEdge - s.endGlobalIdx) / Math.max(1, N);
        const score = span + rightness * span * 0.6;
        if (score > bestScore) { bestScore = score; best = s; }
    }
    return best;
}

// ============================================================================
// Compute Channels
// ============================================================================

function computeSwingChannels(visibleData, baseIdx, params) {
    if (visibleData.length < 40) return [];
    const points = zigzag(visibleData, baseIdx, params.revShort);
    const segs = buildSegments(points, baseIdx, params.minSegBarsShort, params.revShort * 1.0);

    const channels = [];
    for (const seg of segs) {
        const ch = fitChannelForSegment(visibleData, baseIdx, seg, params, 'swing');
        if (ch) channels.push(ch);
    }

    channels.sort((a, b) => b.score - a.score);
    return channels.slice(0, MAX_SWING_CHANNELS).sort((a, b) => a.startGlobalIdx - b.startGlobalIdx);
}

function computeMainTrendChannel(visibleData, baseIdx, params) {
    if (visibleData.length < 60) return null;
    const points = zigzag(visibleData, baseIdx, params.revLong);
    const segs = buildSegments(points, baseIdx, params.minSegBarsLong, params.revLong * 0.9);

    const mainSeg = pickMainTrendSegment(segs, baseIdx, visibleData.length);
    if (!mainSeg) return null;

    return fitChannelForSegment(visibleData, baseIdx, mainSeg, params, 'main');
}

// ============================================================================
// Range Detection
// ============================================================================

function detectRanges(visibleData, baseIdx, params) {
    const N = visibleData.length;
    if (N < 60) return [];

    const { atr, tol } = params;
    const W = Math.max(RANGE_MIN_WIN, Math.floor(N * RANGE_WIN_FRAC));
    const step = Math.max(5, Math.floor(W * RANGE_STEP_FRAC));
    const slopeThr = atr * 0.15;
    const minWidth = atr * 0.8, maxWidth = atr * 4.0;

    const candidates = [];

    for (let start = 0; start + W <= N; start += step) {
        const end = start + W - 1;
        const highs = [], lows = [], xs = [], ys = [];

        for (let i = start; i <= end; i++) {
            highs.push(visibleData[i].high);
            lows.push(visibleData[i].low);
            xs.push(baseIdx + i);
            ys.push(typicalPrice(visibleData[i]));
        }

        const top = quantile(highs, 0.92);
        const bot = quantile(lows, 0.08);
        const width = top - bot;
        if (!(width > minWidth && width < maxWidth)) continue;

        const { slope } = linearRegression(xs, ys);
        if (Math.abs(slope) > slopeThr / 10) continue;

        let contain = 0;
        for (let i = start; i <= end; i++) {
            const b = visibleData[i];
            if (b.high <= top + tol && b.low >= bot - tol) contain++;
        }
        if (contain / W < RANGE_CONTAINMENT) continue;

        const recentStart = end - Math.max(8, Math.floor(W * 0.25));
        let tTop = 0, tBot = 0;
        for (let i = recentStart; i <= end; i++) {
            const b = visibleData[i];
            if (Math.abs(b.high - top) <= tol) tTop++;
            if (Math.abs(b.low - bot) <= tol) tBot++;
        }
        if (tTop + tBot < 2) continue;

        const score = (contain / W) * 100 + (tTop + tBot) * 10 - (Math.abs(slope) / slopeThr) * 20;
        candidates.push({
            regime: 'range', startLocal: start, endLocal: end,
            startGlobalIdx: baseIdx + start, endGlobalIdx: baseIdx + end,
            top, bot, mid: (top + bot) / 2, score,
        });
    }

    candidates.sort((a, b) => b.score - a.score);
    const picked = [];
    for (const c of candidates) {
        const overlap = picked.some(p => {
            const L = Math.max(p.startGlobalIdx, c.startGlobalIdx);
            const R = Math.min(p.endGlobalIdx, c.endGlobalIdx);
            return R > L && (R - L) / (c.endGlobalIdx - c.startGlobalIdx) > 0.5;
        });
        if (!overlap) picked.push(c);
        if (picked.length >= MAX_RANGES) break;
    }

    return picked.sort((a, b) => a.startGlobalIdx - b.startGlobalIdx);
}

// ============================================================================
// Series Pools
// ============================================================================

class ChannelSeriesPool {
    constructor() { this.items = []; }
    ensure() {
        while (this.items.length < MAX_SWING_CHANNELS) {
            this.items.push({
                up: charts.main.addLineSeries({ color: INDICATOR_COLORS.resistance, lineWidth: 2, lineStyle: 0, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
                mid: charts.main.addLineSeries({ color: INDICATOR_COLORS.trendLine, lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
                dn: charts.main.addLineSeries({ color: INDICATOR_COLORS.support, lineWidth: 2, lineStyle: 0, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
            });
        }
    }
    clearAll() { this.items.forEach(it => { it.up.setData([]); it.mid.setData([]); it.dn.setData([]); }); }
    render(channels, visibleData, baseIdx) {
        this.ensure(); this.clearAll();
        if (!state.showPriceLines) return;
        channels.forEach((ch, k) => {
            const it = this.items[k];
            const upD = [], midD = [], dnD = [];
            for (let i = Math.max(0, ch.startLocal); i <= Math.min(visibleData.length - 1, ch.endLocal); i++) {
                const gx = baseIdx + i, t = visibleData[i].time;
                upD.push({ time: t, value: ch.getUp(gx) });
                midD.push({ time: t, value: ch.getMid(gx) });
                dnD.push({ time: t, value: ch.getDn(gx) });
            }
            it.up.setData(upD); it.mid.setData(midD); it.dn.setData(dnD);
        });
    }
}

// Main trend: separate series (dashed, thinner)
let mainTrendSeries = null;

function initMainTrendSeries() {
    if (!charts.main || mainTrendSeries) return;
    mainTrendSeries = {
        up: charts.main.addLineSeries({ color: '#ff6b6b', lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
        mid: charts.main.addLineSeries({ color: '#ffd93d', lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
        dn: charts.main.addLineSeries({ color: '#6bcb77', lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
    };
}

function clearMainTrendSeries() {
    if (!mainTrendSeries) return;
    mainTrendSeries.up.setData([]);
    mainTrendSeries.mid.setData([]);
    mainTrendSeries.dn.setData([]);
}

function renderMainTrendChannel(ch, visibleData, baseIdx) {
    initMainTrendSeries();
    clearMainTrendSeries();
    if (!state.showPriceLines || !ch) return;

    const upD = [], midD = [], dnD = [];
    for (let i = Math.max(0, ch.startLocal); i <= Math.min(visibleData.length - 1, ch.endLocal); i++) {
        const gx = baseIdx + i, t = visibleData[i].time;
        upD.push({ time: t, value: ch.getUp(gx) });
        midD.push({ time: t, value: ch.getMid(gx) });
        dnD.push({ time: t, value: ch.getDn(gx) });
    }
    mainTrendSeries.up.setData(upD);
    mainTrendSeries.mid.setData(midD);
    mainTrendSeries.dn.setData(dnD);
}

class RangeSeriesPool {
    constructor() { this.items = []; }
    ensure() {
        while (this.items.length < MAX_RANGES) {
            this.items.push({
                top: charts.main.addLineSeries({ color: INDICATOR_COLORS.resistance, lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
                bot: charts.main.addLineSeries({ color: INDICATOR_COLORS.support, lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
                mid: charts.main.addLineSeries({ color: INDICATOR_COLORS.trendLine, lineWidth: 1, lineStyle: 3, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false }),
            });
        }
    }
    clearAll() { this.items.forEach(it => { it.top.setData([]); it.bot.setData([]); it.mid.setData([]); }); }
    render(ranges, visibleData, baseIdx) {
        this.ensure(); this.clearAll();
        if (!state.showPriceLines) return;
        ranges.forEach((r, k) => {
            const it = this.items[k];
            const topD = [], botD = [], midD = [];
            for (let i = Math.max(0, r.startLocal); i <= Math.min(visibleData.length - 1, r.endLocal); i++) {
                const t = visibleData[i].time;
                topD.push({ time: t, value: r.top });
                botD.push({ time: t, value: r.bot });
                midD.push({ time: t, value: r.mid });
            }
            it.top.setData(topD); it.bot.setData(botD); it.mid.setData(midD);
        });
    }
}

const swingChannelPool = new ChannelSeriesPool();
const rangePool = new RangeSeriesPool();

// ============================================================================
// Public API
// ============================================================================

export function updateDynamicAnalysis() {
    if (state.dynamicLineTimeout) clearTimeout(state.dynamicLineTimeout);

    state.dynamicLineTimeout = setTimeout(() => {
        const range = charts.main?.timeScale().getVisibleLogicalRange();
        if (!range) return;

        const startIdx = Math.max(0, Math.floor(range.from));
        const endIdx = Math.min(state.rawData.length - 1, Math.ceil(range.to));
        if (startIdx >= endIdx || endIdx - startIdx < 40) {
            swingChannelPool.clearAll();
            clearMainTrendSeries();
            rangePool.clearAll();
            return;
        }

        const visibleData = state.rawData.slice(startIdx, endIdx + 1);
        const baseIdx = startIdx;
        const params = makeParams(visibleData.length, visibleData);

        // Short-scale: swing channels
        const swingChannels = computeSwingChannels(visibleData, baseIdx, params);

        // Long-scale: main trend channel
        const mainChannel = computeMainTrendChannel(visibleData, baseIdx, params);

        // Range boxes
        const ranges = detectRanges(visibleData, baseIdx, params);

        // Render all
        swingChannelPool.render(swingChannels, visibleData, baseIdx);
        renderMainTrendChannel(mainChannel, visibleData, baseIdx);
        rangePool.render(ranges, visibleData, baseIdx);

        // Trigger Trend Tips
        if (mainChannel) {
            if (mainChannel.dir === 1) {
                tipsEngine.showRandomTip(['UPTREND_CHASE', 'UPTREND_PULLBACK', 'UPTREND_RISK', 'PREDICT_TOP', 'PREDICT_REVERSAL', 'POSITION_CONFIRMATION', 'MARKET_VALIDATION', 'POSITION_REWARD', 'POSITION_PURPOSE']);
            } else if (mainChannel.dir === -1) {
                tipsEngine.showRandomTip(['DOWNTREND_KNIFE', 'DOWNTREND_WAIT', 'DOWNTREND_RETEST', 'DOWNTREND_DONT_ADD', 'DOWNTREND_CUT_LOSS', 'DOWNTREND_NO_HOPE', 'PREDICT_BOTTOM', 'PREDICT_REVERSAL', 'SURVIVAL_FIRST', 'NO_AVERAGING_DOWN', 'OPPORTUNITY_FILTER', 'POSITION_PURPOSE', 'POSITION_REWARD']);
            }
        }

    }, DEBOUNCE_MS);
}

export function togglePriceLines() {
    state.showPriceLines = !state.showPriceLines;
    updateDynamicAnalysis();
    const btn = document.querySelector('[data-action="lines"]');
    if (btn) btn.classList.toggle('active', state.showPriceLines);
}

export function resetTrendState() {
    swingChannelPool.clearAll();
    clearMainTrendSeries();
    rangePool.clearAll();
}
