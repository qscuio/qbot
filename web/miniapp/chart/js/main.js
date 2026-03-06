/**
 * Main entry point for the chart application
 */

import { parseUrlParams } from './core/config.js';
import { state, updateState, resolveUserId } from './core/state.js';
import { loadData, startAutoRefresh } from './core/api.js';
import { initMainChart, resizeCharts } from './chart/chart-main.js';
import { initTheme, toggleTheme } from './ui/theme.js';
import {
    setPeriod,
    setSubChart,
    toggleOverlays,
    toggleSignals,
    togglePriceLines,
    toggleLogScale,
    toggleLandscape,
    toggleChipsPanel,
    handleOrientationChange,
    handleResize,
} from './ui/toolbar.js';
import { tipsEngine } from './analysis/tips.js';
import { initTelegram } from './integrations/telegram.js';
import { checkWatchlistStatus, toggleWatchlist } from './integrations/watchlist.js';
import { initNavigation, navigateTo, navigateToHome } from './integrations/navigation.js';
import { renderSidePanelChips } from './analysis/chips.js';
import { initSearch } from './ui/search.js';
import { initWatchlistDropdown } from './ui/watchlist-dropdown.js';

/**
 * Initialize the application
 */
function init() {
    // Parse URL parameters
    const params = parseUrlParams();
    updateState({
        code: params.code,
        navContext: params.context,
    });
    resolveUserId(params.debugUserId);

    // Initialize components
    initTelegram();
    initTheme();
    initMainChart();
    tipsEngine.init();
    initSearch();
    initWatchlistDropdown();

    // Load data
    loadData();
    startAutoRefresh();

    // Initialize features
    checkWatchlistStatus();
    initNavigation();

    // Mobile check for chips button
    if (window.innerWidth < 768) {
        const chipsToggle = document.getElementById('chips-toggle');
        if (chipsToggle) chipsToggle.style.display = 'block';
    }
}

/**
 * Set up event listeners
 */
function setupEventListeners() {
    // Period buttons
    document.querySelectorAll('[data-period]').forEach(btn => {
        btn.addEventListener('click', () => setPeriod(btn.dataset.period, btn));
    });

    // Overlay buttons
    document.getElementById('btn-ma')?.addEventListener('click', () => toggleOverlays('MA'));
    document.getElementById('btn-boll')?.addEventListener('click', () => toggleOverlays('BOLL'));

    // Sub chart buttons
    document.querySelectorAll('[data-subchart]').forEach(btn => {
        btn.addEventListener('click', () => setSubChart(btn.dataset.subchart, btn));
    });

    // Log scale
    document.getElementById('btn-log')?.addEventListener('click', toggleLogScale);

    // Signals
    document.getElementById('btn-signals')?.addEventListener('click', toggleSignals);

    // Price lines
    document.getElementById('btn-lines')?.addEventListener('click', togglePriceLines);

    // Chips panel
    document.getElementById('btn-chips')?.addEventListener('click', toggleChipsPanel);
    document.getElementById('chips-toggle')?.addEventListener('click', toggleChipsPanel);
    document.getElementById('backdrop')?.addEventListener('click', toggleChipsPanel);
    document.querySelector('.close-btn')?.addEventListener('click', toggleChipsPanel);

    // Landscape
    document.getElementById('btn-landscape')?.addEventListener('click', toggleLandscape);

    // Theme
    document.getElementById('theme-btn')?.addEventListener('click', toggleTheme);

    // Watchlist
    document.getElementById('btn-watchlist')?.addEventListener('click', toggleWatchlist);

    // Navigation
    document.getElementById('float-btn-prev')?.addEventListener('click', () => navigateTo('prev'));
    document.getElementById('float-btn-next')?.addEventListener('click', () => navigateTo('next'));
    document.getElementById('btn-home')?.addEventListener('click', navigateToHome);

    // Window events
    window.addEventListener('resize', () => {
        handleResize();
        setTimeout(() => renderSidePanelChips(state.lastChipIdx), 100);
    });
    window.addEventListener('orientationchange', handleOrientationChange);
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    init();
    setupEventListeners();
});
