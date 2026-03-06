/**
 * Legend UI updates for main and sub charts
 */

import { state, mainSeries, subSeries } from '../core/state.js';

/**
 * Update main chart legend on crosshair move
 * @param {Object} param - Crosshair move event parameter
 */
export function updateMainLegend(param) {
    if (!param.time || !param.seriesData) return;

    const c = param.seriesData.get(mainSeries.candle);
    if (c) {
        document.getElementById('l-o').textContent = c.open.toFixed(2);
        document.getElementById('l-h').textContent = c.high.toFixed(2);
        document.getElementById('l-l').textContent = c.low.toFixed(2);
        document.getElementById('l-c').textContent = c.close.toFixed(2);
    }

    if (state.activeOverlay === 'MA') {
        const v5 = param.seriesData.get(mainSeries.ma5);
        const v10 = param.seriesData.get(mainSeries.ma10);
        const v20 = param.seriesData.get(mainSeries.ma20);
        const el5 = document.getElementById('l-ma5');
        const el10 = document.getElementById('l-ma10');
        const el20 = document.getElementById('l-ma20');
        if (v5 && el5) el5.textContent = v5.value.toFixed(2);
        if (v10 && el10) el10.textContent = v10.value.toFixed(2);
        if (v20 && el20) el20.textContent = v20.value.toFixed(2);
    }

    if (state.activeOverlay === 'BOLL') {
        const u = param.seriesData.get(mainSeries.bollUp);
        const m = param.seriesData.get(mainSeries.bollMid);
        const l = param.seriesData.get(mainSeries.bollLow);
        const el1 = document.getElementById('l-b1');
        const el2 = document.getElementById('l-b2');
        const el3 = document.getElementById('l-b3');
        if (u && el1) el1.textContent = u.value.toFixed(2);
        if (m && el2) el2.textContent = m.value.toFixed(2);
        if (l && el3) el3.textContent = l.value.toFixed(2);
    }
}

/**
 * Update sub chart legend on crosshair move
 * @param {Object} param - Crosshair move event parameter
 */
export function updateSubLegend(param) {
    if (!param.time || !param.seriesData) return;

    if (state.activeSub === 'VOL') {
        const v = param.seriesData.get(subSeries.vol);
        const el = document.getElementById('l-vol');
        if (v && el) el.textContent = (v.value / 10000).toFixed(1) + '万';
    } else if (state.activeSub === 'MACD') {
        const diff = param.seriesData.get(subSeries.macdLine);
        const dea = param.seriesData.get(subSeries.macdSig);
        const hist = param.seriesData.get(subSeries.macdHist);
        const elDif = document.getElementById('l-dif');
        const elDea = document.getElementById('l-dea');
        const elMacd = document.getElementById('l-macd');
        if (diff && elDif) elDif.textContent = diff.value.toFixed(3);
        if (dea && elDea) elDea.textContent = dea.value.toFixed(3);
        if (hist && elMacd) elMacd.textContent = hist.value.toFixed(3);
    } else if (state.activeSub === 'KDJ') {
        const k = param.seriesData.get(subSeries.k);
        const d = param.seriesData.get(subSeries.d);
        const j = param.seriesData.get(subSeries.j);
        const elK = document.getElementById('l-k');
        const elD = document.getElementById('l-d');
        const elJ = document.getElementById('l-j');
        if (k && elK) elK.textContent = k.value.toFixed(2);
        if (d && elD) elD.textContent = d.value.toFixed(2);
        if (j && elJ) elJ.textContent = j.value.toFixed(2);
    } else if (state.activeSub === 'RSI') {
        const r = param.seriesData.get(subSeries.rsi);
        const el = document.getElementById('l-rsi');
        if (r && el) el.textContent = r.value.toFixed(2);
    }
}
