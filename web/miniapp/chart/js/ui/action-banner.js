/**
 * Action banner UI for buy/sell recommendations
 */

import { state } from '../core/state.js';
import { getChinaTimeInfo } from '../core/config.js';

/**
 * Update action banner with current signals
 * @param {Array} ma5 - MA5 data
 * @param {Array} ma10 - MA10 data
 */
export function updateActionBanner(ma5, ma10) {
    if (state.rawData.length < 2 || ma5.length < 2 || ma10.length < 2) return;

    const lastIdx = state.rawData.length - 1;
    const lastTime = state.rawData[lastIdx].time;
    const prevTime = state.rawData[lastIdx - 1].time;

    const curr5 = ma5.find(m => m.time === lastTime)?.value;
    const curr10 = ma10.find(m => m.time === lastTime)?.value;
    const prev5 = ma5.find(m => m.time === prevTime)?.value;
    const prev10 = ma10.find(m => m.time === prevTime)?.value;

    if (!curr5 || !curr10) return;

    const badge = document.getElementById('action-badge');
    const text = document.getElementById('action-text');
    const timeEl = document.getElementById('action-time');

    const { chinaTime, timeMins, isWeekend, isMarketOpen } = getChinaTimeInfo();

    // Calculate trend
    const maDiff = curr5 - curr10;
    const maDiffPrev = prev5 && prev10 ? prev5 - prev10 : 0;
    const trendDir = maDiff > 0 ? 'bullish' : 'bearish';
    const crossingUp = maDiffPrev <= 0 && maDiff > 0;
    const crossingDown = maDiffPrev >= 0 && maDiff < 0;

    let action = 'hold';
    let actionText = '';
    let timeLabel = '';

    if (crossingUp) {
        action = 'buy';
        actionText = isMarketOpen
            ? '<strong>BUY Signal</strong> - MA5 crossed above MA10. Consider entry now.'
            : '<strong>BUY on Open</strong> - MA5 crossed above MA10. Watch at market open.';
    } else if (crossingDown) {
        action = 'sell';
        actionText = isMarketOpen
            ? '<strong>SELL Signal</strong> - MA5 crossed below MA10. Consider exit now.'
            : '<strong>SELL on Open</strong> - MA5 crossed below MA10. Watch at market open.';
    } else if (trendDir === 'bullish') {
        action = 'hold';
        actionText = '<strong>HOLD Long</strong> - MA5 > MA10, uptrend intact. Hold or add on dips.';
    } else {
        action = 'hold';
        actionText = '<strong>HOLD/Wait</strong> - MA5 < MA10, downtrend. Wait for buy signal.';
    }

    // Time label
    if (isMarketOpen) {
        timeLabel = `Market Open • ${chinaTime.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}`;
    } else if (isWeekend) {
        timeLabel = 'Market Closed (Weekend) • Action applies to next open';
    } else if (timeMins < 570) {
        timeLabel = 'Pre-Market • Action applies to today\'s open';
    } else {
        timeLabel = 'After Hours • Action applies to next trading day';
    }

    // Update UI
    badge.className = 'action-badge ' + action;
    badge.textContent = action.toUpperCase();
    text.innerHTML = actionText;
    timeEl.textContent = timeLabel;
}
