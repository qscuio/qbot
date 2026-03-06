/**
 * Sub-chart management (VOL, MACD, KDJ, RSI)
 */

import { state, charts, subSeries } from '../core/state.js';
import { getColors, removeSeries } from '../core/utils.js';
import { calculateMACD, calculateKDJ, calculateRSI } from './indicators.js';

/**
 * Update sub chart with selected indicator
 */
export function updateSubChart() {
    const chart = charts.sub;
    if (!chart) return;

    // Clear previous series
    removeSeries(chart, subSeries.vol);
    removeSeries(chart, subSeries.macdHist);
    removeSeries(chart, subSeries.macdLine);
    removeSeries(chart, subSeries.macdSig);
    removeSeries(chart, subSeries.k);
    removeSeries(chart, subSeries.d);
    removeSeries(chart, subSeries.j);
    removeSeries(chart, subSeries.rsi);

    // Reset references
    subSeries.vol = null;
    subSeries.macdHist = null;
    subSeries.macdLine = null;
    subSeries.macdSig = null;
    subSeries.k = null;
    subSeries.d = null;
    subSeries.j = null;
    subSeries.rsi = null;

    const subLeg = document.getElementById('sub-legend');
    subLeg.innerHTML = '';
    const colors = getColors();

    if (state.activeSub === 'VOL') {
        subSeries.vol = chart.addHistogramSeries({ priceFormat: { type: 'volume' } });
        const volData = state.rawData
            .filter(d => d.volume !== undefined && d.volume !== null && d.volume > 0)
            .map(d => ({
                time: d.time,
                value: d.volume,
                color: d.close >= d.open ? colors.up : colors.down,
            }));
        if (volData.length > 0) {
            subSeries.vol.setData(volData);
        }
        subLeg.innerHTML = '<span class="leg-item">Vol:<span class="leg-val" id="l-vol">--</span></span>';
    } else if (state.activeSub === 'MACD') {
        const macd = calculateMACD(state.rawData);

        subSeries.macdHist = chart.addHistogramSeries({
            color: '#2962ff',
            lineWidth: 2,
            priceFormat: { type: 'price' },
        });
        subSeries.macdLine = chart.addLineSeries({ color: '#f59e0b', lineWidth: 1 });
        subSeries.macdSig = chart.addLineSeries({ color: '#3b82f6', lineWidth: 1 });

        subSeries.macdLine.setData(macd.map(m => ({ time: m.time, value: m.diff })));
        subSeries.macdSig.setData(macd.map(m => ({ time: m.time, value: m.dea })));
        subSeries.macdHist.setData(macd.map(m => ({
            time: m.time,
            value: m.hist,
            color: m.hist > 0 ? colors.up : colors.down,
        })));

        subLeg.innerHTML = `
            <span class="leg-item" style="color:#f59e0b">DIF:<span class="leg-val" id="l-dif">--</span></span>
            <span class="leg-item" style="color:#3b82f6">DEA:<span class="leg-val" id="l-dea">--</span></span>
            <span class="leg-item">MACD:<span class="leg-val" id="l-macd">--</span></span>
        `;
    } else if (state.activeSub === 'KDJ') {
        const kdj = calculateKDJ(state.rawData);

        subSeries.k = chart.addLineSeries({ color: '#f59e0b', lineWidth: 1 });
        subSeries.d = chart.addLineSeries({ color: '#3b82f6', lineWidth: 1 });
        subSeries.j = chart.addLineSeries({ color: '#a855f7', lineWidth: 1 });

        subSeries.k.setData(kdj.map(x => ({ time: x.time, value: x.k })));
        subSeries.d.setData(kdj.map(x => ({ time: x.time, value: x.d })));
        subSeries.j.setData(kdj.map(x => ({ time: x.time, value: x.j })));

        subLeg.innerHTML = `
            <span class="leg-item" style="color:#f59e0b">K:<span class="leg-val" id="l-k">--</span></span>
            <span class="leg-item" style="color:#3b82f6">D:<span class="leg-val" id="l-d">--</span></span>
            <span class="leg-item" style="color:#a855f7">J:<span class="leg-val" id="l-j">--</span></span>
        `;
    } else if (state.activeSub === 'RSI') {
        const rsi = calculateRSI(state.rawData);

        subSeries.rsi = chart.addLineSeries({ color: '#a855f7', lineWidth: 1 });
        subSeries.rsi.setData(rsi);

        subLeg.innerHTML = '<span class="leg-item" style="color:#a855f7">RSI(14):<span class="leg-val" id="l-rsi">--</span></span>';
    }
}

/**
 * Set sub chart type
 * @param {string} type - 'VOL', 'MACD', 'KDJ', 'RSI'
 * @param {HTMLElement} btn - Button element
 */
export function setSubChart(type, btn) {
    if (state.activeSub === type) return;

    state.activeSub = type;
    btn.parentNode.querySelectorAll('.t-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    updateSubChart();
}
