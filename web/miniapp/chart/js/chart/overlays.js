/**
 * Chart overlay management (MA, BOLL)
 */

import { state, mainSeries } from '../core/state.js';
import { calculateMA, calculateBOLL } from './indicators.js';

/**
 * Update overlay visibility and data
 */
export function updateOverlays() {
    const isMa = state.activeOverlay === 'MA';
    const isBoll = state.activeOverlay === 'BOLL';

    // Toggle visibility
    mainSeries.ma5?.applyOptions({ visible: isMa });
    mainSeries.ma10?.applyOptions({ visible: isMa });
    mainSeries.ma20?.applyOptions({ visible: isMa });

    mainSeries.bollUp?.applyOptions({ visible: isBoll });
    mainSeries.bollMid?.applyOptions({ visible: isBoll });
    mainSeries.bollLow?.applyOptions({ visible: isBoll });

    const maLegend = document.getElementById('ma-legend');
    maLegend.innerHTML = '';

    if (isMa) {
        mainSeries.ma5?.setData(calculateMA(state.rawData, 5));
        mainSeries.ma10?.setData(calculateMA(state.rawData, 10));
        mainSeries.ma20?.setData(calculateMA(state.rawData, 20));
        maLegend.innerHTML = `
            <span class="leg-item c-ma5">MA5:<span class="leg-val" id="l-ma5">--</span></span>
            <span class="leg-item c-ma10">MA10:<span class="leg-val" id="l-ma10">--</span></span>
            <span class="leg-item c-ma20">MA20:<span class="leg-val" id="l-ma20">--</span></span>
        `;
    } else if (isBoll) {
        const boll = calculateBOLL(state.rawData);
        mainSeries.bollUp?.setData(boll.map(b => ({ time: b.time, value: b.upper })));
        mainSeries.bollMid?.setData(boll.map(b => ({ time: b.time, value: b.mid })));
        mainSeries.bollLow?.setData(boll.map(b => ({ time: b.time, value: b.lower })));
        maLegend.innerHTML = `
            <span class="leg-item" style="color:#f97316">UP:<span class="leg-val" id="l-b1">--</span></span>
            <span class="leg-item" style="color:#3b82f6">MID:<span class="leg-val" id="l-b2">--</span></span>
            <span class="leg-item" style="color:#22c55e">LOW:<span class="leg-val" id="l-b3">--</span></span>
        `;
    }
}

/**
 * Toggle between overlay types
 * @param {string} type - 'MA' or 'BOLL'
 */
export function toggleOverlays(type) {
    if (state.activeOverlay === type) {
        state.activeOverlay = 'NONE';
    } else {
        state.activeOverlay = type;
    }

    document.getElementById('btn-ma').classList.toggle('active', state.activeOverlay === 'MA');
    document.getElementById('btn-boll').classList.toggle('active', state.activeOverlay === 'BOLL');

    updateOverlays();
}
