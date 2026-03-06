/**
 * Technical indicator calculations
 */

/**
 * Calculate Simple Moving Average
 * @param {Array} data - Raw OHLCV data
 * @param {number} n - Period
 * @returns {Array}
 */
export function calculateMA(data, n) {
    return data.map((d, i, arr) => {
        if (i < n - 1) return { time: d.time, value: NaN };
        const sum = arr.slice(i - n + 1, i + 1).reduce((a, b) => a + b.close, 0);
        return { time: d.time, value: sum / n };
    }).filter(d => !isNaN(d.value));
}

/**
 * Calculate Bollinger Bands
 * @param {Array} data - Raw OHLCV data
 * @param {number} n - Period (default 20)
 * @param {number} k - Standard deviation multiplier (default 2)
 * @returns {Array}
 */
export function calculateBOLL(data, n = 20, k = 2) {
    return data.map((d, i, arr) => {
        if (i < n - 1) return null;
        const slice = arr.slice(i - n + 1, i + 1);
        const avg = slice.reduce((a, b) => a + b.close, 0) / n;
        const variance = slice.reduce((a, b) => a + Math.pow(b.close - avg, 2), 0) / n;
        const std = Math.sqrt(variance);
        return {
            time: d.time,
            mid: avg,
            upper: avg + k * std,
            lower: avg - k * std,
        };
    }).filter(Boolean);
}

/**
 * Calculate MACD
 * @param {Array} data - Raw OHLCV data
 * @param {number} short - Short EMA period (default 12)
 * @param {number} long - Long EMA period (default 26)
 * @param {number} sig - Signal period (default 9)
 * @returns {Array}
 */
export function calculateMACD(data, short = 12, long = 26, sig = 9) {
    let emaShort = 0, emaLong = 0, emaSig = 0;
    const res = [];

    data.forEach((d, i) => {
        if (i === 0) {
            emaShort = d.close;
            emaLong = d.close;
        } else {
            emaShort = (2 * d.close + (short - 1) * emaShort) / (short + 1);
            emaLong = (2 * d.close + (long - 1) * emaLong) / (long + 1);
        }
        const diff = emaShort - emaLong;
        if (i === 0) {
            emaSig = diff;
        } else {
            emaSig = (2 * diff + (sig - 1) * emaSig) / (sig + 1);
        }

        res.push({ time: d.time, diff, dea: emaSig, hist: 2 * (diff - emaSig) });
    });

    return res;
}

/**
 * Calculate KDJ
 * @param {Array} data - Raw OHLCV data
 * @param {number} n - Period (default 9)
 * @param {number} m1 - K smoothing (default 3)
 * @param {number} m2 - D smoothing (default 3)
 * @returns {Array}
 */
export function calculateKDJ(data, n = 9, m1 = 3, m2 = 3) {
    let k = 50, d = 50;

    return data.map((item, i) => {
        let low = item.low, high = item.high;
        for (let j = 0; j < n; j++) {
            if (i - j >= 0) {
                low = Math.min(low, data[i - j].low);
                high = Math.max(high, data[i - j].high);
            }
        }
        const rsv = (high === low) ? 50 : (item.close - low) / (high - low) * 100;
        k = (1 * rsv + (m1 - 1) * k) / m1;
        d = (1 * k + (m2 - 1) * d) / m2;
        const jVal = 3 * k - 2 * d;
        return { time: item.time, k, d, j: jVal };
    });
}

/**
 * Calculate RSI
 * @param {Array} data - Raw OHLCV data
 * @param {number} n - Period (default 14)
 * @returns {Array}
 */
export function calculateRSI(data, n = 14) {
    let up = 0, down = 0;
    const res = [];

    for (let i = 1; i < data.length; i++) {
        const diff = data[i].close - data[i - 1].close;
        const u = diff > 0 ? diff : 0;
        const d = diff < 0 ? -diff : 0;

        if (i <= n) {
            up += u;
            down += d;
            if (i === n) {
                up /= n;
                down /= n;
            }
        } else {
            up = (up * (n - 1) + u) / n;
            down = (down * (n - 1) + d) / n;
        }

        if (i >= n) {
            const rs = down === 0 ? 100 : up / down;
            res.push({ time: data[i].time, value: 100 - (100 / (1 + rs)) });
        }
    }

    return res;
}

/**
 * Calculate ATR (Average True Range)
 * @param {Array} data - Raw OHLCV data
 * @param {number} period - Period (default 14)
 * @returns {number}
 */
export function calculateATR(data, period = 14) {
    if (data.length < 2) return 0;
    const trs = [];

    for (let i = 1; i < data.length; i++) {
        const tr = Math.max(
            data[i].high - data[i].low,
            Math.abs(data[i].high - data[i - 1].close),
            Math.abs(data[i].low - data[i - 1].close),
        );
        trs.push(tr);
    }

    const slice = trs.slice(-period);
    return slice.reduce((a, b) => a + b, 0) / slice.length;
}

/**
 * Calculate volume ratio (current vs average)
 * @param {Array} data - Raw OHLCV data
 * @param {number} lookback - Lookback period (default 20)
 * @returns {number}
 */
export function calculateVolRatio(data, lookback = 20) {
    if (data.length < 2) return 1;
    const currentVol = data[data.length - 1].volume;
    const avgVol = data.slice(-lookback - 1, -1).reduce((a, b) => a + b.volume, 0) / lookback;
    return avgVol > 0 ? currentVol / avgVol : 1;
}
