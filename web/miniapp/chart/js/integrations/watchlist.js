/**
 * Watchlist functionality
 */

import { API_BASE } from '../core/config.js';
import { state } from '../core/state.js';
import { authenticatedFetch } from '../core/api.js';
import { showToast } from '../ui/toast.js';

/**
 * Check if current stock is in watchlist
 */
export async function checkWatchlistStatus() {
    const btn = document.getElementById('btn-watchlist');

    if (!state.userId) {
        btn.disabled = true;
        btn.style.opacity = '0.5';
        btn.title = 'Open in Telegram to use watchlist';
        return;
    }

    btn.disabled = false;
    btn.style.opacity = '1';
    btn.title = '';

    try {
        const res = await authenticatedFetch(
            `${API_BASE}/api/chart/watchlist/status?user_id=${state.userId}&code=${state.code}`,
        );
        const data = await res.json();
        updateWatchlistBtn(data.in_watchlist);
    } catch (e) {
        console.error('Failed to check watchlist:', e);
    }
}

/**
 * Update watchlist button appearance
 * @param {boolean} isIn - Whether stock is in watchlist
 */
export function updateWatchlistBtn(isIn) {
    const btn = document.getElementById('btn-watchlist');
    if (isIn) {
        btn.textContent = '★';
        btn.style.color = '#f59e0b';
        btn.classList.add('active');
    } else {
        btn.textContent = '☆';
        btn.style.color = 'var(--text-secondary)';
        btn.classList.remove('active');
    }
}

/**
 * Toggle watchlist status
 */
export async function toggleWatchlist() {
    const btn = document.getElementById('btn-watchlist');

    if (!state.userId) {
        showToast('Open in Telegram to use watchlist');
        return;
    }

    const isAdding = btn.textContent === '☆';

    // Optimistic update
    updateWatchlistBtn(isAdding);

    try {
        const endpoint = isAdding ? '/api/chart/watchlist/add' : '/api/chart/watchlist/remove';
        const res = await authenticatedFetch(`${API_BASE}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                user_id: state.userId,
                code: state.code,
                name: document.getElementById('name').textContent,
            }),
        });

        const data = await res.json();
        if (isAdding && !data.success && !data.code) {
            updateWatchlistBtn(false);
            showToast('Failed to add');
        } else if (!isAdding && !data.success) {
            updateWatchlistBtn(true);
            showToast('Failed to remove');
        } else {
            showToast(isAdding ? 'Added to Watchlist' : 'Removed from Watchlist');
        }
    } catch (e) {
        console.error('Watchlist toggle error:', e);
        updateWatchlistBtn(!isAdding);
        showToast('Error updating watchlist');
    }
}
