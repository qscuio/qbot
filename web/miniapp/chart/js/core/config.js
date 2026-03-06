/**
 * Configuration constants for the chart application
 */

export const API_BASE = window.location.origin;
export const DEFAULT_STOCK_CODE = 'sh000001';

export const CHART_CONFIG = {
    defaultVisibleBars: 60,
    autoRefreshInterval: 5000,
    defaultDays: 250,
    defaultWeeklyDays: 100,
    defaultMonthlyDays: 60,
    chipLookback: 60,
    chipBuckets: 120,
};

export const INDICATOR_COLORS = {
    ma5: '#f59e0b',
    ma10: '#3b82f6',
    ma20: '#a855f7',
    ma60: '#22c55e',
    bollUp: '#f97316',
    bollMid: '#3b82f6',
    bollLow: '#22c55e',
    trendLine: '#ec4899',
    support: '#22c55e',
    resistance: '#ef4444',
};

/**
 * Parse URL parameters and Telegram start params
 * @returns {Object} Parsed parameters
 */
export function parseUrlParams() {
    const params = new URLSearchParams(window.location.search);

    // Check for Telegram Mini App startapp parameter in multiple places
    const tgStartParam = window.Telegram?.WebApp?.initDataUnsafe?.start_param
        || params.get('tgWebAppStartParam')
        || params.get('startapp');

    const parsed = parseStartParam(tgStartParam);
    const parsedCodeParam = parseStartParam(params.get('code'));

    return {
        code: parsedCodeParam.code || parsed.code || DEFAULT_STOCK_CODE,
        context: params.get('context') || parsedCodeParam.context || parsed.context,
        debugUserId: params.get('debug_user_id'),
    };
}

/**
 * Parse a combined start parameter (code_context format)
 * @param {string|null} startParam 
 * @returns {Object}
 */
function parseStartParam(startParam) {
    if (!startParam) {
        return { code: null, context: null };
    }
    const idx = startParam.indexOf('_');
    if (idx === -1) {
        return { code: startParam, context: null };
    }
    return {
        code: startParam.substring(0, idx) || null,
        context: startParam.substring(idx + 1) || null,
    };
}

/**
 * Check if current time is within China market trading hours
 * @returns {boolean}
 */
export function isTradingTime() {
    const now = new Date();
    // Convert to China time (UTC+8)
    const utc = now.getTime() + now.getTimezoneOffset() * 60000;
    const chinaTime = new Date(utc + (3600000 * 8));

    const day = chinaTime.getDay();
    if (day === 0 || day === 6) return false; // Weekend

    const h = chinaTime.getHours();
    const m = chinaTime.getMinutes();
    const t = h * 60 + m;

    // 9:15 - 11:30 (555 - 690)
    // 13:00 - 15:00 (780 - 900)
    return (t >= 555 && t <= 690) || (t >= 780 && t <= 900);
}

/**
 * Get current China time info for market status
 * @returns {Object}
 */
export function getChinaTimeInfo() {
    const now = new Date();
    const chinaTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Shanghai' }));
    const hour = chinaTime.getHours();
    const mins = chinaTime.getMinutes();
    const dayOfWeek = chinaTime.getDay();
    const timeMins = hour * 60 + mins;
    const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
    const isMarketOpen = !isWeekend && ((timeMins >= 570 && timeMins <= 690) || (timeMins >= 780 && timeMins <= 900));

    return {
        chinaTime,
        hour,
        mins,
        timeMins,
        dayOfWeek,
        isWeekend,
        isMarketOpen,
    };
}
