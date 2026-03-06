/**
 * Theme management
 */

import { applyChartTheme } from '../chart/chart-main.js';

/**
 * Initialize theme from localStorage or system preference
 */
export function initTheme() {
    const theme = localStorage.getItem('pro-chart-theme')
        || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    document.documentElement.setAttribute('data-theme', theme);
    updateThemeIcon(theme);
    syncTelegramTheme();
}

/**
 * Toggle between light and dark theme
 */
export function toggleTheme() {
    const cur = document.documentElement.getAttribute('data-theme');
    const next = cur === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('pro-chart-theme', next);
    updateThemeIcon(next);
    applyChartTheme();
    syncTelegramTheme();
}

/**
 * Update theme toggle button icon
 * @param {string} theme - 'dark' or 'light'
 */
function updateThemeIcon(theme) {
    const btn = document.getElementById('theme-btn');
    if (btn) {
        btn.textContent = theme === 'dark' ? '☀️' : '🌙';
    }
}

/**
 * Sync theme colors with Telegram WebApp
 */
export function syncTelegramTheme() {
    if (window.Telegram?.WebApp) {
        requestAnimationFrame(() => {
            const style = getComputedStyle(document.documentElement);
            const headerColor = style.getPropertyValue('--bg-secondary').trim();
            const bgColor = style.getPropertyValue('--bg-primary').trim();

            try {
                Telegram.WebApp.setHeaderColor(headerColor);
                Telegram.WebApp.setBackgroundColor(bgColor);
            } catch (e) {
                console.warn('Failed to set Telegram colors', e);
            }
        });
    }
}
