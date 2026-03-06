/**
 * Watchlist Dropdown UI
 */

import { API_BASE } from '../core/config.js';
import { state, updateState, resetChartState } from '../core/state.js';
import { authenticatedFetch, loadData } from '../core/api.js';
import { checkWatchlistStatus } from '../integrations/watchlist.js';
import { checkNavigation } from '../integrations/navigation.js';

/**
 * Initialize watchlist dropdown
 */
export function initWatchlistDropdown() {
    const btn = document.getElementById('btn-my-list');
    const dropdown = document.getElementById('watchlist-dropdown');
    const closeBtn = document.getElementById('close-watchlist');

    if (!btn || !dropdown) return;

    // Toggle dropdown
    btn.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleDropdown();
    });

    // Close button
    closeBtn?.addEventListener('click', (e) => {
        e.stopPropagation();
        dropdown.classList.remove('open');
    });

    // Close when clicking outside
    document.addEventListener('click', (e) => {
        if (!dropdown.contains(e.target) && e.target !== btn) {
            dropdown.classList.remove('open');
        }
    });
}

/**
 * Toggle dropdown visibility and load data
 */
async function toggleDropdown() {
    const dropdown = document.getElementById('watchlist-dropdown');
    const isOpen = dropdown.classList.contains('open');

    if (isOpen) {
        dropdown.classList.remove('open');
    } else {
        dropdown.classList.add('open');
        await loadWatchlist();
    }
}

/**
 * Load watchlist data from API
 */
async function loadWatchlist() {
    const container = document.getElementById('watchlist-items');
    if (!state.userId) {
        container.innerHTML = '<div class="wl-empty">请先在 Telegram 中打开</div>';
        return;
    }

    container.innerHTML = '<div class="wl-loading">加载中...</div>';

    try {
        const res = await authenticatedFetch(`${API_BASE}/api/chart/watchlist/list?user_id=${state.userId}`);
        const data = await res.json();

        if (!data.watchlist || data.watchlist.length === 0) {
            container.innerHTML = '<div class="wl-empty">暂无自选股</div>';
            return;
        }

        renderWatchlist(data.watchlist);
    } catch (e) {
        console.error('Failed to load watchlist:', e);
        container.innerHTML = '<div class="wl-error">加载失败</div>';
    }
}

/**
 * Render watchlist items
 * @param {Array} items 
 */
function renderWatchlist(items) {
    const container = document.getElementById('watchlist-items');
    container.innerHTML = '';

    items.forEach(item => {
        const div = document.createElement('div');
        div.className = 'wl-item';
        if (item.code === state.code) {
            div.classList.add('active');
        }

        div.innerHTML = `
            <span class="wl-name">${item.name || item.code}</span>
            <span class="wl-code">${item.code}</span>
        `;

        div.addEventListener('click', () => {
            selectWatchlistItem(item.code);
        });

        container.appendChild(div);
    });
}

/**
 * Select a stock from watchlist
 * @param {string} code 
 */
function selectWatchlistItem(code) {
    const dropdown = document.getElementById('watchlist-dropdown');
    dropdown.classList.remove('open');

    if (code === state.code) return;

    // Update URL
    const url = new URL(window.location.href);
    url.searchParams.set('code', code);
    url.searchParams.set('context', 'watchlist');
    window.history.pushState({}, '', url);

    // Update state
    updateState({
        code,
        navContext: 'watchlist',
    });

    // Reset and reload
    resetChartState();
    document.getElementById('loading').style.display = 'flex';

    loadData();
    checkWatchlistStatus();
    checkNavigation();
}
