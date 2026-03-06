/**
 * Utility functions for the chart application
 */

/**
 * Format volume for display (e.g., 10000 -> 1.0万)
 * @param {number} vol 
 * @returns {string}
 */
export function formatVolume(vol) {
    if (vol > 100000000) {
        return (vol / 100000000).toFixed(2) + '亿';
    } else if (vol > 10000) {
        return (vol / 10000).toFixed(1) + '万';
    }
    return vol.toString();
}

/**
 * Debounce a function call
 * @param {Function} func 
 * @param {number} wait 
 * @returns {Function}
 */
export function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Get CSS color values from current theme
 * @returns {Object}
 */
export function getColors() {
    const style = getComputedStyle(document.body);
    return {
        bg: style.getPropertyValue('--bg-primary').trim(),
        grid: style.getPropertyValue('--grid-color').trim(),
        text: style.getPropertyValue('--text-secondary').trim(),
        up: style.getPropertyValue('--up-color').trim(),
        down: style.getPropertyValue('--down-color').trim(),
    };
}

/**
 * Safely remove a series from a chart
 * @param {Object} chart - Chart instance
 * @param {Object} series - Series to remove
 */
export function removeSeries(chart, series) {
    if (series) {
        try {
            chart.removeSeries(series);
        } catch (_e) {
            // Ignore - series may already be removed
        }
    }
}

/**
 * Clamp a number between min and max
 * @param {number} num 
 * @param {number} min 
 * @param {number} max 
 * @returns {number}
 */
export function clamp(num, min, max) {
    return Math.min(Math.max(num, min), max);
}
