/**
 * Telegram WebApp integration
 */

import { initTheme } from '../ui/theme.js';

/**
 * Initialize Telegram WebApp
 */
export function initTelegram() {
    if (!window.Telegram?.WebApp) return;

    Telegram.WebApp.ready();

    try {
        const platform = Telegram.WebApp.platform || 'unknown';

        // Only request fullscreen on mobile devices
        if (['android', 'ios'].includes(platform)) {
            if (Telegram.WebApp.requestFullscreen) {
                Telegram.WebApp.requestFullscreen();
            }
        }

        // Expand to full height
        Telegram.WebApp.expand();
    } catch (e) {
        console.warn('Telegram WebApp setup failed:', e);
    }

    // Listen for theme changes
    Telegram.WebApp.onEvent('themeChanged', initTheme);
}
