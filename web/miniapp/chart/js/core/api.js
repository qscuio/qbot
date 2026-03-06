/**
 * API communication module with authenticated requests
 */

import { API_BASE, CHART_CONFIG, isTradingTime } from './config.js';
import { state, updateState } from './state.js';
import { processData } from '../chart/chart-main.js';
import { updateStockHeader } from '../ui/header.js';
import { renderSidePanelChips } from '../analysis/chips.js';
import { updateSectorBanner } from '../ui/sector.js';

/**
 * Make an authenticated fetch request with Telegram initData
 * @param {string} url 
 * @param {Object} options 
 * @returns {Promise<Response>}
 */
export async function authenticatedFetch(url, options = {}) {
    const headers = options.headers || {};

    // Add Telegram initData if available
    if (window.Telegram?.WebApp?.initData) {
        headers['X-Telegram-Init-Data'] = window.Telegram.WebApp.initData;
    }

    const res = await fetch(url, { ...options, headers });

    if (res.status === 401 || res.status === 403) {
        document.body.innerHTML = `
            <div style="display:flex;justify-content:center;align-items:center;height:100vh;background:#000;color:#fff;flex-direction:column;text-align:center;padding:20px;">
                <h2 style="color: #ef4444; margin-bottom: 10px;">Access Denied</h2>
                <p style="color: #9ca3af;">You are not authorized to view this chart.</p>
                <p style="font-size: 12px; color: #525252; margin-top: 20px;">ID: ${window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 'Unknown'}</p>
            </div>
        `;
        throw new Error('Access Denied');
    }
    return res;
}

/**
 * Load chart data from the API
 * @param {boolean} isUpdate - Whether this is a background update
 */
export async function loadData(isUpdate = false) {
    try {
        let days = CHART_CONFIG.defaultDays;
        if (state.timeFrame === 'weekly') days = CHART_CONFIG.defaultWeeklyDays;
        if (state.timeFrame === 'monthly') days = CHART_CONFIG.defaultMonthlyDays;
        const res = await authenticatedFetch(
            `${API_BASE}/api/chart/data/${state.code}?days=${days}&period=${state.timeFrame}`,
        );

        if (!res.ok) throw new Error('Network error');
        const json = await res.json();

        if (!json.data || !json.data.length) throw new Error('No Data');

        // Filter out invalid data entries
        const rawData = json.data.filter(d =>
            d &&
            d.close !== undefined && d.close !== null && !isNaN(d.close) &&
            d.open !== undefined && d.open !== null &&
            d.high !== undefined && d.high !== null &&
            d.low !== undefined && d.low !== null,
        );

        if (rawData.length === 0) throw new Error('No valid data');

        updateState({ rawData });
        updateStockHeader(json);
        updateSectorBanner(json.sector_info);
        processData();

        // Only reset zoom on first load
        if (!isUpdate) {
            resetChipCache();
            renderSidePanelChips(-1);
        }

        document.getElementById('loading').style.display = 'none';

    } catch (e) {
        if (!isUpdate) {
            document.getElementById('loading').innerHTML = `<div class="error">${e.message}</div>`;
        } else {
            console.warn('Background update failed:', e);
        }
    }
}

/**
 * Reset chip cache
 */
function resetChipCache() {
    updateState({ lastChipIdx: -1 });
}

/**
 * Start auto-refresh timer for trading hours
 */
export function startAutoRefresh() {
    if (state.refreshTimer) {
        clearInterval(state.refreshTimer);
    }

    state.refreshTimer = setInterval(() => {
        if (document.hidden) return;
        if (state.timeFrame !== 'daily') return;

        if (isTradingTime()) {
            loadData(true);
        }
    }, CHART_CONFIG.autoRefreshInterval);
}

/**
 * Stop auto-refresh timer
 */
export function stopAutoRefresh() {
    if (state.refreshTimer) {
        clearInterval(state.refreshTimer);
        state.refreshTimer = null;
    }
}
