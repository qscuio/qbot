/**
 * Header UI updates
 */

import { state, updateState } from '../core/state.js';
import { formatVolume } from '../core/utils.js';

/**
 * Update stock header with new data
 * @param {Object} data - API response data
 */
export function updateStockHeader(data) {
    document.getElementById('name').textContent = data.name || state.code;
    document.getElementById('code').textContent = state.code;

    const validData = state.rawData.filter(d => d && d.close !== undefined && d.close !== null);
    if (validData.length === 0) {
        document.getElementById('price').textContent = '--';
        return;
    }

    const last = validData[validData.length - 1];
    const prev = validData[validData.length > 1 ? validData.length - 2 : 0];

    updateState({
        latestStockData: last,
        latestPrevData: prev,
    });

    updateHeaderValues(last, prev);
}

/**
 * Restore header to latest values (after crosshair leaves)
 */
export function restoreHeaderValues() {
    if (state.latestStockData && state.latestPrevData) {
        updateHeaderValues(state.latestStockData, state.latestPrevData);
    }
}

/**
 * Update header with specific data values
 * @param {Object} current - Current data point
 * @param {Object} prev - Previous data point
 */
export function updateHeaderValues(current, prev) {
    if (!current) return;

    // Price
    document.getElementById('price').textContent = current.close.toFixed(2);

    // Change percentage
    let pct = 0;
    if (prev && prev.close > 0) {
        pct = ((current.close - prev.close) / prev.close) * 100;
    }

    const chgEl = document.getElementById('change');
    chgEl.textContent = (pct > 0 ? '+' : '') + pct.toFixed(2) + '%';
    chgEl.className = `price-change ${pct > 0 ? 'up' : pct < 0 ? 'down' : 'flat'}`;

    // Amplitude
    let amp = current.amplitude;
    if (amp === undefined || amp === null) {
        if (prev && prev.close > 0) {
            amp = ((current.high - current.low) / prev.close) * 100;
        } else {
            amp = 0;
        }
    }
    document.getElementById('amplitude').textContent = amp.toFixed(2) + '%';

    // Turnover Rate
    const turnover = current.turnover_rate || 0;
    document.getElementById('turnover').textContent = turnover.toFixed(2) + '%';

    // Volume Ratio
    const vr = current.volume_ratio;
    const vrEl = document.getElementById('vol-ratio');
    if (vr !== undefined && vr !== null) {
        vrEl.textContent = vr.toFixed(2);
        vrEl.style.color = vr > 2 ? 'var(--up-color)' : 'var(--text-primary)';
    } else {
        vrEl.textContent = '--';
        vrEl.style.color = 'var(--text-primary)';
    }

    // Volume
    document.getElementById('vol-stat').textContent = formatVolume(current.volume);
}
