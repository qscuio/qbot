/**
 * Stock navigation functionality
 */

import { API_BASE } from '../core/config.js';
import { state, updateState, resetChartState } from '../core/state.js';
import { authenticatedFetch, loadData } from '../core/api.js';
import { checkWatchlistStatus } from './watchlist.js';

/**
 * Initialize navigation
 */
export function initNavigation() {
    if (state.navContext) {
        checkNavigation();
    }
}

/**
 * Check available prev/next stocks
 */
export async function checkNavigation() {
    if (!state.navContext) return;

    try {
        let url = `${API_BASE}/api/chart/navigation?code=${state.code}&context=${state.navContext}`;
        if (state.userId) url += `&user_id=${state.userId}`;

        const res = await authenticatedFetch(url);
        const data = await res.json();

        updateState({
            prevCode: data.prev,
            nextCode: data.next,
        });

        const btnPrev = document.getElementById('float-btn-prev');
        const btnNext = document.getElementById('float-btn-next');
        const navContainer = document.getElementById('floating-nav');

        if (state.prevCode || state.nextCode) {
            navContainer.style.display = 'flex';
        } else {
            navContainer.style.display = 'none';
        }

        if (state.prevCode) {
            btnPrev.style.visibility = 'visible';
            btnPrev.title = state.prevCode;
        } else {
            btnPrev.style.visibility = 'hidden';
        }

        if (state.nextCode) {
            btnNext.style.visibility = 'visible';
            btnNext.title = state.nextCode;
        } else {
            btnNext.style.visibility = 'hidden';
        }
    } catch (e) {
        console.error('Nav check failed', e);
    }
}

/**
 * Navigate to prev or next stock
 * @param {string} dir - 'prev' or 'next'
 */
export function navigateTo(dir) {
    const target = dir === 'prev' ? state.prevCode : state.nextCode;
    if (!target) return;

    // Update URL
    const url = new URL(window.location.href);
    url.searchParams.set('code', target);
    if (state.navContext) {
        url.searchParams.set('context', state.navContext);
    }

    // Update state
    updateState({ code: target });
    window.history.pushState({}, '', url);

    // Reset and reload
    resetChartState();
    document.getElementById('loading').style.display = 'flex';

    loadData();
    checkWatchlistStatus();
    checkNavigation();
}

/**
 * Navigate to Home (Default Index)
 */
/**
 * Navigate to Home (Default Index)
 */
export function navigateToHome() {
    const target = '000001'; // Shanghai Composite Index

    // Update URL
    const url = new URL(window.location.href);
    url.searchParams.set('code', target);
    url.searchParams.delete('context');
    window.history.pushState({}, '', url);

    // Update state
    updateState({
        code: target,
        navContext: null
    });

    // Reset and reload
    resetChartState();
    document.getElementById('loading').style.display = 'flex';

    loadData();
    checkWatchlistStatus();
    checkNavigation();
}
