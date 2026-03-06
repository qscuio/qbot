/**
 * Buy/Sell signal detection
 */

import { state, mainSeries } from '../core/state.js';

/**
 * Event types for trend state machine
 */
export const EVENT_TYPES = {
    TOUCH_TL: 'TOUCH_TL',
    BREAK_TL: 'BREAK_TL',
    FAILED_BREAKDOWN: 'FAILED_BREAKDOWN',
    FAILED_BREAKOUT: 'FAILED_BREAKOUT',
    BREAKOUT_ACCEPTED: 'BREAKOUT_ACCEPTED',
    RISKY_NTH_TOUCH: 'RISKY_NTH_TOUCH',
};

/**
 * Detect buy/sell signals based on MA crossovers
 * @param {Array} data - Raw OHLCV data
 * @param {Array} ma5 - MA5 data
 * @param {Array} ma10 - MA10 data
 * @returns {Array} Signal markers
 */
export function detectBuySellSignals(data, ma5, ma10) {
    const signals = [];

    for (let i = 1; i < data.length; i++) {
        const t = data[i].time;
        const tp = data[i - 1].time;
        const c5 = ma5.find(m => m.time === t)?.value;
        const c10 = ma10.find(m => m.time === t)?.value;
        const p5 = ma5.find(m => m.time === tp)?.value;
        const p10 = ma10.find(m => m.time === tp)?.value;

        if (c5 && c10 && p5 && p10) {
            // Golden cross (MA5 crosses above MA10)
            if (p5 <= p10 && c5 > c10) {
                signals.push({
                    time: t,
                    position: 'belowBar',
                    color: '#ef4444',
                    shape: 'arrowUp',
                    text: 'B',
                });
            }
            // Death cross (MA5 crosses below MA10)
            if (p5 >= p10 && c5 < c10) {
                signals.push({
                    time: t,
                    position: 'aboveBar',
                    color: '#22c55e',
                    shape: 'arrowDown',
                    text: 'S',
                });
            }
        }
    }

    return signals;
}

/**
 * Update trend markers on chart
 * @param {Array} data - Raw OHLCV data
 * @param {Object} trendLine - Current trend line
 * @param {number} epsilon - ATR-based tolerance
 * @param {Array} currentEvents - Events from state machine
 */
export function updateTrendMarkers(data, trendLine, epsilon, currentEvents) {
    const markers = [...(mainSeries.candle?.markers() || [])];

    currentEvents.forEach(ev => {
        const bar = data[ev.idx];
        let marker = null;

        if (ev.type === EVENT_TYPES.FAILED_BREAKDOWN) {
            marker = {
                time: bar.time,
                position: 'belowBar',
                color: '#22c55e',
                shape: 'arrowUp',
                text: 'FBD',
            };
        } else if (ev.type === EVENT_TYPES.FAILED_BREAKOUT) {
            marker = {
                time: bar.time,
                position: 'aboveBar',
                color: '#ef4444',
                shape: 'arrowDown',
                text: 'FBO',
            };
        } else if (ev.type === EVENT_TYPES.BREAKOUT_ACCEPTED) {
            marker = {
                time: bar.time,
                position: 'belowBar',
                color: '#3b82f6',
                shape: 'circle',
                text: 'ACC',
            };
        } else if (ev.type === EVENT_TYPES.RISKY_NTH_TOUCH) {
            marker = {
                time: bar.time,
                position: 'aboveBar',
                color: '#f59e0b',
                shape: 'square',
                text: `⚠${ev.count}`,
            };
        }

        if (marker && !markers.some(m => m.time === marker.time && m.text === marker.text)) {
            markers.push(marker);
        }
    });

    state.trendMarkers = markers;
}

/**
 * Toggle signals display
 */
export function toggleSignals() {
    state.showSignals = !state.showSignals;
    document.getElementById('btn-signals').classList.toggle('active', state.showSignals);

    if (state.rawData.length) {
        // Re-import to avoid circular dependency at runtime
        import('../chart/chart-main.js').then(({ processData }) => processData());
    }
}
