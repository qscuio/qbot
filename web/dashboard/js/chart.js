const COLORS = {
  up: "#ef5350",
  down: "#26a69a",
  grid: "#252526",
  text: "#a9a9a9",
};

export const CHART_INTERACTION_OPTIONS = {
  handleScroll: {
    pressedMouseMove: true,
    horzTouchDrag: true,
  },
  handleScale: {
    axisPressedMouseMove: { time: false, price: true },
  },
};

export function movingAverage(bars, window) {
  const values = [];
  let sum = 0;
  bars.forEach((bar, index) => {
    sum += Number(bar.close);
    if (index >= window) sum -= Number(bars[index - window].close);
    if (index >= window - 1) {
      values.push({ time: bar.time, value: Number((sum / window).toFixed(4)) });
    }
  });
  return values;
}

export function signalMarkers(bars, hits) {
  const time = bars.at(-1)?.time;
  if (!time) return [];
  return hits.map((hit, index) => ({
    time,
    position: index % 2 === 0 ? "aboveBar" : "belowBar",
    color: index % 2 === 0 ? "#4ea1ff" : "#c586c0",
    shape: index % 2 === 0 ? "arrowDown" : "arrowUp",
    text: hit.name || hit.signalId,
  }));
}

export function activitySeries(bars) {
  const usableVolume = bars.filter((bar) => Number(bar.volume) > 0).length;
  const metric = bars.length > 0 && usableVolume / bars.length >= 0.8 ? "volume" : "amount";
  const label = metric === "volume" ? "VOL" : "AMOUNT · volume unavailable";
  return {
    metric,
    label,
    data: bars.map((bar) => ({
      time: bar.time,
      value: Math.max(0, Number(bar[metric]) || 0),
      color: Number(bar.close) >= Number(bar.open)
        ? "rgba(239,83,80,.45)"
        : "rgba(38,166,154,.45)",
    })),
  };
}

function addSeries(chart, type, options) {
  if (typeof chart.addSeries === "function") {
    return chart.addSeries(window.LightweightCharts[type], options);
  }
  const legacy = {
    CandlestickSeries: "addCandlestickSeries",
    HistogramSeries: "addHistogramSeries",
    LineSeries: "addLineSeries",
  };
  return chart[legacy[type]](options);
}

export function fitChartAfterLayout(
  timeScale,
  scheduleFrame = (callback) => window.requestAnimationFrame(callback),
  barCount = 0,
) {
  let active = true;
  scheduleFrame(() => scheduleFrame(() => {
    if (!active) return;
    if (barCount > 120) {
      timeScale.setVisibleLogicalRange({ from: barCount - 120, to: barCount - 1 });
    } else {
      timeScale.fitContent();
    }
  }));
  return () => { active = false; };
}

export function mountChart(container, bars, hits = []) {
  if (!window.LightweightCharts || !container) return { destroy() {} };
  container.replaceChildren();
  const chart = window.LightweightCharts.createChart(container, {
    autoSize: true,
    ...CHART_INTERACTION_OPTIONS,
    layout: { background: { color: "#1e1e1e" }, textColor: COLORS.text },
    grid: {
      vertLines: { color: COLORS.grid },
      horzLines: { color: COLORS.grid },
    },
    rightPriceScale: { borderColor: "#333333" },
    timeScale: {
      borderColor: "#333333",
      timeVisible: false,
      rightOffset: 0,
      fixLeftEdge: true,
      fixRightEdge: true,
    },
    crosshair: { mode: 0 },
    localization: { locale: "zh-CN" },
  });
  const candleSeries = addSeries(chart, "CandlestickSeries", {
    upColor: COLORS.up,
    downColor: COLORS.down,
    wickUpColor: COLORS.up,
    wickDownColor: COLORS.down,
    borderVisible: false,
  });
  candleSeries.setData(bars.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })));

  const activity = activitySeries(bars);
  const activityHistogram = addSeries(chart, "HistogramSeries", {
    priceFormat: { type: "volume" },
    priceScaleId: "activity",
  });
  activityHistogram.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
  activityHistogram.setData(activity.data);

  [[5, "#dcdcaa"], [10, "#4ec9b0"], [20, "#569cd6"], [60, "#c586c0"]].forEach(([windowSize, color]) => {
    const series = addSeries(chart, "LineSeries", {
      color,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
    series.setData(movingAverage(bars, windowSize));
  });

  const markers = signalMarkers(bars, hits);
  if (markers.length) {
    if (typeof window.LightweightCharts.createSeriesMarkers === "function") {
      window.LightweightCharts.createSeriesMarkers(candleSeries, markers);
    } else if (typeof candleSeries.setMarkers === "function") {
      candleSeries.setMarkers(markers);
    }
  }
  const cancelInitialFit = fitChartAfterLayout(chart.timeScale(), undefined, bars.length);
  return {
    destroy: () => {
      cancelInitialFit();
      chart.remove();
    },
  };
}
