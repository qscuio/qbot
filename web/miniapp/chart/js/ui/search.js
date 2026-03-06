/**
 * Search functionality
 */

import { API_BASE } from '../core/config.js';
import { updateState, resetChartState } from '../core/state.js';
import { authenticatedFetch, loadData } from '../core/api.js';
import { checkWatchlistStatus } from '../integrations/watchlist.js';
import { checkNavigation } from '../integrations/navigation.js';

let searchDebounceTimer = null;

/**
 * Initialize search module
 */
export function initSearch() {
    const input = document.getElementById('search-input');
    const results = document.getElementById('search-results');
    const container = document.querySelector('.search-container');
    const icon = container?.querySelector('.search-icon');

    // Click on search icon to focus input (for mobile)
    icon?.addEventListener('click', () => {
        input.focus();
    });

    // Search input
    input.addEventListener('input', e => {
        const query = e.target.value.trim();
        if (searchDebounceTimer) clearTimeout(searchDebounceTimer);

        if (query.length < 1) {
            results.classList.remove('open');
            results.innerHTML = '';
            return;
        }

        searchDebounceTimer = setTimeout(() => performSearch(query), 300);
    });

    // Focus shows results if query exists
    input.addEventListener('focus', () => {
        if (input.value.trim().length >= 1 && results.children.length > 0) {
            results.classList.add('open');
        }
    });

    // Click outside to close
    document.addEventListener('click', e => {
        if (!input.contains(e.target) && !results.contains(e.target)) {
            results.classList.remove('open');
        }
    });
}

/**
 * Perform search API call
 * @param {string} query 
 */
async function performSearch(query) {
    const resultsContainer = document.getElementById('search-results');
    resultsContainer.classList.add('open');
    resultsContainer.innerHTML = '<div class="search-loading">Searching...</div>';

    try {
        const res = await authenticatedFetch(`${API_BASE}/api/chart/search?q=${encodeURIComponent(query)}`);
        const data = await res.json();

        if (!data.results || data.results.length === 0) {
            resultsContainer.innerHTML = '<div class="search-empty">No results found</div>';
            return;
        }

        renderResults(data.results);
    } catch (e) {
        console.error('Search failed:', e);
        resultsContainer.innerHTML = '<div class="search-error">Search failed</div>';
    }
}

/**
 * Render search results
 * @param {Array} results 
 */
function renderResults(results) {
    const container = document.getElementById('search-results');
    container.innerHTML = '';

    results.forEach(item => {
        const div = document.createElement('div');
        div.className = 'search-item';
        div.innerHTML = `
            <span class="code">${item.code}</span>
            <span class="name">${item.name}</span>
        `;
        div.addEventListener('click', () => selectStock(item.code));
        container.appendChild(div);
    });
}

/**
 * Select a stock from results
 * @param {string} code 
 */
function selectStock(code) {
    const results = document.getElementById('search-results');
    const input = document.getElementById('search-input');

    results.classList.remove('open');
    input.value = ''; // Clear input after selection

    // Update URL
    const url = new URL(window.location.href);
    url.searchParams.set('code', code);
    // Reset context when searching
    url.searchParams.delete('context');
    window.history.pushState({}, '', url);

    // Update state
    updateState({
        code,
        navContext: null,
    });

    // Reset and reload
    resetChartState();
    document.getElementById('loading').style.display = 'flex';

    loadData();
    checkWatchlistStatus();
    checkNavigation();
}
