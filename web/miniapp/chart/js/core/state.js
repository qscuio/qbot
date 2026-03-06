/**
 * Centralized application state management
 */

export const state = {
    // Stock identification
    code: '',
    navContext: null,
    userId: null,

    // Data
    rawData: [],
    latestStockData: null,
    latestPrevData: null,

    // Chart settings
    timeFrame: 'daily',
    activeOverlay: 'MA',  // 'MA', 'BOLL', 'NONE'
    activeSub: 'VOL',     // 'VOL', 'MACD', 'KDJ', 'RSI'

    // Display toggles
    showSignals: true,
    showPriceLines: true,
    showChipOverlay: true,
    isLandscape: false,
    isLogScale: false,

    // Navigation
    prevCode: null,
    nextCode: null,

    // Chip state
    lastChipIdx: -1,
    chipDebounceTimer: null,

    // Dynamic analysis
    dynamicLineTimeout: null,
    priceLines: [],
    trendMarkers: [],

    // Auto-refresh
    refreshTimer: null,
};

/**
 * Chart instances - kept separate for clarity
 */
export const charts = {
    main: null,
    sub: null,
};

/**
 * Series references for main chart
 */
export const mainSeries = {
    candle: null,
    ma5: null,
    ma10: null,
    ma20: null,
    bollUp: null,
    bollMid: null,
    bollLow: null,
    trendLine: null,
    trendUpper: null,
    trendLower: null,
    trendUpper2: null,  // Secondary resistance line
    trendLower2: null,  // Secondary support line
};

/**
 * Series references for sub chart
 */
export const subSeries = {
    vol: null,
    macdHist: null,
    macdLine: null,
    macdSig: null,
    k: null,
    d: null,
    j: null,
    rsi: null,
};

/**
 * Update state with new values
 * @param {Object} updates - Key-value pairs to update
 */
export function updateState(updates) {
    Object.assign(state, updates);
}

/**
 * Reset chart-related state (for stock navigation)
 */
export function resetChartState() {
    state.rawData = [];
    state.lastChipIdx = -1;
    state.latestStockData = null;
    state.latestPrevData = null;
    state.priceLines = [];
    state.trendMarkers = [];
}

/**
 * Resolve user ID from Telegram or debug param
 * @param {string|null} debugUserId 
 * @returns {number|null}
 */
export function resolveUserId(debugUserId = null) {
    let resolved = window.Telegram?.WebApp?.initDataUnsafe?.user?.id;
    if (!resolved && debugUserId && /^\d+$/.test(debugUserId)) {
        resolved = Number(debugUserId);
    }
    if (!resolved) {
        console.warn('No Telegram User ID found; watchlist disabled.');
    }
    state.userId = resolved || null;
    return state.userId;
}
