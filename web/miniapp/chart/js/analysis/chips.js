/**
 * Chip distribution calculation and rendering
 */

import { state, charts, mainSeries, updateState } from '../core/state.js';
import { CHART_CONFIG } from '../core/config.js';

let chipDebounceTimer = null;

/**
 * Debounced chip rendering
 * @param {number} idx - Data index
 */
export function debouncedRenderChips(idx) {
    if (idx === state.lastChipIdx && idx !== -1) return;
    updateState({ lastChipIdx: idx });

    if (chipDebounceTimer) clearTimeout(chipDebounceTimer);
    chipDebounceTimer = setTimeout(() => {
        renderSidePanelChips(idx);
    }, 20);
}

/**
 * Calculate chip distribution for a given index
 * @param {Array} data - Raw OHLCV data
 * @param {number} upToIndex - Index to calculate up to
 * @param {Object|null} priceRange - Optional price range to sync with
 * @returns {Object} Chip distribution data
 */
export function calculateChipDistribution(data, upToIndex, priceRange = null) {
    if (!data || data.length === 0) {
        return { distribution: [], avgCost: 0, profitRatio: 0, concentration: 0, priceRange: null };
    }

    const endIndex = upToIndex >= 0 ? upToIndex : data.length - 1;
    const currentPrice = data[endIndex].close;
    const lookback = Math.min(CHART_CONFIG.chipLookback, endIndex + 1);
    const startIndex = endIndex - lookback + 1;

    // Calculate price range
    let chipMinPrice = Infinity, chipMaxPrice = -Infinity;
    for (let i = startIndex; i <= endIndex; i++) {
        if (data[i].low < chipMinPrice) chipMinPrice = data[i].low;
        if (data[i].high > chipMaxPrice) chipMaxPrice = data[i].high;

    }

    let minPrice, maxPrice;
    if (priceRange && priceRange.min !== undefined && priceRange.max !== undefined) {
        minPrice = priceRange.min;
        maxPrice = priceRange.max;
    } else {
        const padding = (chipMaxPrice - chipMinPrice) * 0.1;
        minPrice = Math.max(0, chipMinPrice - padding);
        maxPrice = chipMaxPrice + padding;
    }

    const numBuckets = CHART_CONFIG.chipBuckets;
    const bucketSize = (maxPrice - minPrice) / numBuckets;
    const buckets = new Array(numBuckets).fill(0);

    for (let i = startIndex; i <= endIndex; i++) {
        const d = data[i];
        const decay = 1 - (endIndex - i) * 0.02;
        const vol = d.volume * Math.max(0.1, decay);
        const lowB = Math.floor((d.low - minPrice) / bucketSize);
        const highB = Math.floor((d.high - minPrice) / bucketSize);
        for (let b = Math.max(0, lowB); b <= Math.min(numBuckets - 1, highB); b++) {
            buckets[b] += vol / (highB - lowB + 1);
        }
    }

    const maxB = Math.max(...buckets);
    const dist = [];
    let profitV = 0, lossV = 0, wSum = 0, tVol = 0;

    for (let i = 0; i < numBuckets; i++) {
        const price = minPrice + (i + 0.5) * bucketSize;
        const pct = maxB > 0 ? (buckets[i] / maxB) * 100 : 0;
        const isP = price <= currentPrice;
        dist.push({ price, percentage: pct, isProfit: isP });

        if (buckets[i] > 0) {
            if (isP) profitV += buckets[i]; else lossV += buckets[i];
            wSum += price * buckets[i];
            tVol += buckets[i];
        }
    }

    const avgCost = tVol > 0 ? wSum / tVol : currentPrice;
    const profitRatio = (profitV + lossV) > 0 ? (profitV / (profitV + lossV)) * 100 : 0;

    const sorted = [...buckets].sort((a, b) => b - a).slice(0, Math.floor(numBuckets * 0.2));
    const topSum = sorted.reduce((a, b) => a + b, 0);
    const conc = tVol > 0 ? (topSum / tVol) * 100 : 0;

    return {
        distribution: dist.reverse(),
        avgCost,
        profitRatio,
        concentration: conc,
        currentPrice,
        priceRange: { min: minPrice, max: maxPrice },
    };
}

/**
 * Render chip distribution in side panel
 * @param {number} dayIndex - Data index (-1 for latest)
 */
export function renderSidePanelChips(dayIndex) {
    const idx = dayIndex >= 0 ? dayIndex : state.rawData.length - 1;
    const target = state.rawData[idx];
    if (!target) return;

    document.getElementById('chip-date').textContent = target.time;

    const canvas = document.getElementById('chip-canvas');
    const container = document.getElementById('chip-chart-area');
    if (!canvas || !container) return;

    const rect = container.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;

    // Align with main chart
    const mainChartEl = document.getElementById('main-chart');
    const mainRect = mainChartEl.getBoundingClientRect();
    const mainHeight = mainRect.height;

    const sidePanel = document.getElementById('side-panel');
    if (sidePanel) {
        sidePanel.style.setProperty('--chip-height', `${mainHeight}px`);
    }

    const chipRect = container.getBoundingClientRect();
    const yOffset = mainRect.top - chipRect.top;

    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = rect.height + 'px';

    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, rect.width, rect.height);

    const chips = calculateChipDistribution(state.rawData, idx);
    if (!mainSeries.candle) return;

    const maxPct = Math.max(...chips.distribution.map(d => d.percentage));
    const currentPrice = chips.currentPrice;

    // Draw bars
    chips.distribution.forEach(d => {
        const yMain = mainSeries.candle.priceToCoordinate(d.price);
        if (yMain === null) return;
        if (yMain < 0 || yMain > mainHeight) return;

        const y = yMain + yOffset;
        if (y < 0 || y > rect.height) return;

        const barWidth = (d.percentage / maxPct) * (rect.width - 40);
        const barHeight = 2;

        const isProfit = d.price <= currentPrice;
        ctx.fillStyle = isProfit ? '#ef4444' : '#3b82f6';
        ctx.globalAlpha = 0.6;

        ctx.fillRect(0, y - barHeight / 2, barWidth, barHeight);

        if (d.percentage === maxPct) {
            ctx.fillStyle = '#f59e0b';
            ctx.fillRect(0, y - barHeight / 2, barWidth + 2, barHeight);
        }
    });
    ctx.globalAlpha = 1.0;

    // Draw current price line
    const currYMain = mainSeries.candle.priceToCoordinate(currentPrice);
    const currY = currYMain !== null ? currYMain + yOffset : null;
    if (currYMain !== null && currYMain >= 0 && currYMain <= mainHeight && currY !== null && currY >= 0 && currY <= rect.height) {
        ctx.strokeStyle = '#fbbf24';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 2]);
        ctx.beginPath();
        ctx.moveTo(0, currY);
        ctx.lineTo(rect.width, currY);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.fillStyle = '#fbbf24';
        ctx.font = '10px monospace';
        ctx.fillText(currentPrice.toFixed(2), rect.width - 35, currY - 2);
    }

    // Update stats
    const pRatioEl = document.getElementById('p-ratio');
    pRatioEl.textContent = chips.profitRatio.toFixed(1) + '%';
    pRatioEl.style.color = chips.profitRatio >= 50 ? 'var(--up-color)' : 'var(--down-color)';
    document.getElementById('avg-cost').textContent = chips.avgCost.toFixed(2);
    document.getElementById('conc').textContent = chips.concentration.toFixed(1) + '%';
}

/**
 * Toggle chips panel visibility
 */
export function toggleChipsPanel() {
    const panel = document.getElementById('side-panel');
    const backdrop = document.getElementById('backdrop');
    panel.classList.toggle('open');

    if (window.innerWidth < 768) {
        backdrop.classList.toggle('open');
    } else {
        setTimeout(() => {
            charts.main?.resize(0, 0);
            charts.sub?.resize(0, 0);
        }, 300);
    }
}
