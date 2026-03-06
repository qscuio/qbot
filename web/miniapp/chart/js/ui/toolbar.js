/**
 * Toolbar interactions
 */

import { state, charts, updateState } from '../core/state.js';
import { loadData } from '../core/api.js';
import { toggleOverlays } from '../chart/overlays.js';
import { setSubChart as setSubChartType } from '../chart/chart-sub.js';
import { toggleSignals } from '../analysis/signals.js';
import { togglePriceLines, resetTrendState } from '../analysis/trend.js';
import { toggleChipsPanel, renderSidePanelChips } from '../analysis/chips.js';
import { tipsEngine } from '../analysis/tips.js';
import { resizeCharts } from '../chart/chart-main.js';

/**
 * Set chart period (daily, weekly, monthly)
 * @param {string} period 
 * @param {HTMLElement} btn 
 */
export function setPeriod(period, btn) {
    if (state.timeFrame === period) return;

    updateState({ timeFrame: period });
    btn.parentNode.querySelectorAll('.t-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    resetTrendState();
    tipsEngine.showTip('TIMEFRAME_CHANGE');
    loadData();
}

/**
 * Set sub chart type
 * @param {string} type 
 * @param {HTMLElement} btn 
 */
export function setSubChart(type, btn) {
    setSubChartType(type, btn);
}

/**
 * Toggle log scale
 */
export function toggleLogScale() {
    state.isLogScale = !state.isLogScale;
    document.getElementById('btn-log').classList.toggle('active', state.isLogScale);

    const mode = state.isLogScale ? 1 : 0;
    charts.main?.priceScale('right').applyOptions({ mode });
}

/**
 * Toggle landscape mode
 */
export function toggleLandscape() {
    state.isLandscape = !state.isLandscape;
    const body = document.body;
    const btn = document.getElementById('btn-landscape');

    if (state.isLandscape) {
        body.classList.add('force-landscape');
        btn.classList.add('active');
    } else {
        body.classList.remove('force-landscape');
        btn.classList.remove('active');
    }

    // Trigger resize after transition
    setTimeout(() => {
        resizeCharts();
        renderSidePanelChips(state.lastChipIdx);
    }, 100);
}

/**
 * Handle orientation change
 */
export function handleOrientationChange() {
    // If physically rotated to landscape, remove forced landscape
    if (window.orientation === 90 || window.orientation === -90) {
        if (state.isLandscape) toggleLandscape();
    }
}

/**
 * Handle resize for orientation detection
 */
export function handleResize() {
    if (window.innerWidth > window.innerHeight && state.isLandscape) {
        toggleLandscape();
    }
    resizeCharts();
    setTimeout(() => renderSidePanelChips(state.lastChipIdx), 100);
}

// Re-export other toggle functions
export { toggleOverlays, toggleSignals, togglePriceLines, toggleChipsPanel };
