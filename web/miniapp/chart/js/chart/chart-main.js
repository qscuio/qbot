/**
 * Main chart setup and management
 */

import { CHART_CONFIG, INDICATOR_COLORS } from '../core/config.js';
import { state, charts, mainSeries, updateState } from '../core/state.js';
import { getColors } from '../core/utils.js';
import { updateOverlays } from './overlays.js';
import { updateSubChart } from './chart-sub.js';
import { updateMainLegend, updateSubLegend } from '../ui/legend.js';
import { updateHeaderValues, restoreHeaderValues } from '../ui/header.js';
import { debouncedRenderChips, renderSidePanelChips } from '../analysis/chips.js';
import { updateDynamicAnalysis } from '../analysis/trend.js';
import { detectBuySellSignals } from '../analysis/signals.js';
import { updateActionBanner } from '../ui/action-banner.js';
import { calculateMA } from './indicators.js';

/**
 * Initialize main chart
 */
export function initMainChart() {
    const mainCont = document.getElementById('main-chart');
    const subCont = document.getElementById('sub-chart');
    const c = getColors();

    const chartOpts = {
        layout: {
            background: { type: 'solid', color: 'transparent' },
            textColor: c.text,
            fontFamily: "'Inter', sans-serif",
        },
        grid: {
            vertLines: { color: c.grid },
            horzLines: { color: c.grid },
        },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        localization: {
            locale: 'zh-CN',
            dateFormat: 'yyyy-MM-dd',
        },
        timeScale: {
            borderColor: c.grid,
            timeVisible: false,
            rightOffset: 5,
            barSpacing: 8,
            minBarSpacing: 3,
            fixLeftEdge: false,
            fixRightEdge: true,
            shiftVisibleRangeOnNewBar: true,
        },
        rightPriceScale: {
            borderColor: c.grid,
            minimumWidth: 80,
            autoScale: true,
        },
        autoSize: true,
    };

    charts.main = LightweightCharts.createChart(mainCont, chartOpts);
    charts.sub = LightweightCharts.createChart(subCont, chartOpts);

    // Main series
    mainSeries.candle = charts.main.addCandlestickSeries({
        upColor: c.up,
        downColor: c.down,
        borderUpColor: c.up,
        borderDownColor: c.down,
        wickUpColor: c.up,
        wickDownColor: c.down,
    });

    // MA series (hidden by default until overlay is active)
    mainSeries.ma5 = charts.main.addLineSeries({ color: INDICATOR_COLORS.ma5, lineWidth: 1, visible: false });
    mainSeries.ma10 = charts.main.addLineSeries({ color: INDICATOR_COLORS.ma10, lineWidth: 1, visible: false });
    mainSeries.ma20 = charts.main.addLineSeries({ color: INDICATOR_COLORS.ma20, lineWidth: 1, visible: false });

    // BOLL series
    mainSeries.bollUp = charts.main.addLineSeries({ color: INDICATOR_COLORS.bollUp, lineWidth: 1, lineStyle: 2, visible: false });
    mainSeries.bollMid = charts.main.addLineSeries({ color: INDICATOR_COLORS.bollMid, lineWidth: 1, visible: false });
    mainSeries.bollLow = charts.main.addLineSeries({ color: INDICATOR_COLORS.bollLow, lineWidth: 1, lineStyle: 2, visible: false });

    // Sync time scales
    charts.main.timeScale().subscribeVisibleLogicalRangeChange(r => {
        charts.sub.timeScale().setVisibleLogicalRange(r);
        requestAnimationFrame(() => renderSidePanelChips(state.lastChipIdx));
        updateDynamicAnalysis();
        updateDateRangeDisplay(r);
    });

    charts.sub.timeScale().subscribeVisibleLogicalRangeChange(r => {
        charts.main.timeScale().setVisibleLogicalRange(r);
        requestAnimationFrame(() => renderSidePanelChips(state.lastChipIdx));
    });

    // Crosshair move handlers
    charts.main.subscribeCrosshairMove(param => {
        updateMainLegend(param);

        // Crosshair sync to sub chart
        const subSeriesRef = mainSeries.candle;
        if (param.time && subSeriesRef) {
            charts.sub.setCrosshairPosition(0, param.time, subSeriesRef);
        }

        // Update chip distribution on hover
        if (param.time && state.rawData.length > 0) {
            const idx = state.rawData.findIndex(d => d.time === param.time);
            if (idx !== -1) {
                debouncedRenderChips(idx);
                const d = state.rawData[idx];
                const prev = idx > 0 ? state.rawData[idx - 1] : d;
                updateHeaderValues(d, prev);
            }
        } else {
            restoreHeaderValues();
        }
    });

    charts.sub.subscribeCrosshairMove(param => {
        updateSubLegend(param);
        if (param.time && mainSeries.candle) {
            charts.main.setCrosshairPosition(0, param.time, mainSeries.candle);
        }

        if (param.time && state.rawData.length > 0) {
            const idx = state.rawData.findIndex(d => d.time === param.time);
            if (idx !== -1) {
                debouncedRenderChips(idx);
            }
        }
    });

    // Click handler for chip lock
    charts.main.subscribeClick(param => {
        if (param.time) {
            const idx = state.rawData.findIndex(d => d.time === param.time);
            if (idx !== -1) {
                updateState({ lastChipIdx: idx });
                renderSidePanelChips(idx);
            }
        }
    });

    applyChartTheme();
}

/**
 * Apply theme colors to charts
 */
export function applyChartTheme() {
    const c = getColors();
    const opts = {
        layout: { textColor: c.text },
        grid: { vertLines: { color: c.grid }, horzLines: { color: c.grid } },
        timeScale: { borderColor: c.grid },
        rightPriceScale: { borderColor: c.grid },
    };

    charts.main?.applyOptions(opts);
    charts.sub?.applyOptions(opts);

    mainSeries.candle?.applyOptions({
        upColor: c.up,
        downColor: c.down,
        borderUpColor: c.up,
        borderDownColor: c.down,
        wickUpColor: c.up,
        wickDownColor: c.down,
    });
}

/**
 * Process data and update all chart elements
 */
export function processData() {
    // Main chart candle data
    mainSeries.candle.setData(state.rawData.map(d => ({
        time: d.time,
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
    })));

    // Update time scale settings
    const isIntraday = !['daily', 'weekly', 'monthly'].includes(state.timeFrame);
    const tsOpts = { timeScale: { timeVisible: isIntraday } };
    charts.main.applyOptions(tsOpts);
    charts.sub.applyOptions(tsOpts);

    // Overlays and sub chart
    updateOverlays();
    updateSubChart();

    // Signals
    if (state.showSignals) {
        const ma5 = calculateMA(state.rawData, 5);
        const ma10 = calculateMA(state.rawData, 10);
        const signals = detectBuySellSignals(state.rawData, ma5, ma10);
        mainSeries.candle.setMarkers(signals);
        updateActionBanner(ma5, ma10);
    } else {
        mainSeries.candle.setMarkers([]);
    }

    // Dynamic analysis (trend lines, S/R)
    updateDynamicAnalysis();

    // Set initial visible range
    const totalBars = state.rawData.length;
    const visibleBars = CHART_CONFIG.defaultVisibleBars;
    if (totalBars > visibleBars) {
        charts.main.timeScale().setVisibleLogicalRange({
            from: totalBars - visibleBars,
            to: totalBars + 2,
        });
    } else {
        charts.main.timeScale().fitContent();
    }
    // Scroll to latest data with a small buffer (positive value = bars from right edge)
    charts.main.timeScale().scrollToPosition(2, false);

    charts.main.priceScale('right').applyOptions({
        autoScale: true,
        scaleMargins: { top: 0.1, bottom: 0.1 },
    });
}

/**
 * Resize charts (call on window resize or layout change)
 */
export function resizeCharts() {
    charts.main?.resize(0, 0);
    charts.sub?.resize(0, 0);
}

/**
 * Update the date range display based on visible logical range
 * @param {Object} range - {from, to}
 */
function updateDateRangeDisplay(range) {
    if (!range || !state.rawData || state.rawData.length === 0) return;

    const len = state.rawData.length;
    let fromIdx = Math.round(range.from);
    let toIdx = Math.round(range.to);

    // Clamp indices
    if (fromIdx < 0) fromIdx = 0;
    if (toIdx >= len) toIdx = len - 1;

    // Safety check if range is completely out of bounds or invalid
    if (fromIdx > toIdx) return;
    if (fromIdx >= len || toIdx < 0) return;

    const start = state.rawData[fromIdx];
    const end = state.rawData[toIdx];

    const el = document.getElementById('date-range');
    if (el && start && end) {
        el.textContent = `${start.time} ~ ${end.time}`;
    }
}
