import { chipProfileRows, chipProfileSummary } from "./chip-profile.js?v=20260720.1";

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

export function selectedChipDate(param) {
  const time = param?.time;
  let value = null;
  if (typeof time === "string" && /^\d{4}-\d{2}-\d{2}$/.test(time)) {
    value = time;
  } else if (time && Number.isInteger(time.year) && Number.isInteger(time.month) && Number.isInteger(time.day)) {
    value = `${String(time.year).padStart(4, "0")}-${String(time.month).padStart(2, "0")}-${String(time.day).padStart(2, "0")}`;
  }
  if (!value) return null;
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isNaN(parsed.valueOf()) || parsed.toISOString().slice(0, 10) !== value ? null : value;
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

function createChipProfileOverlay(container) {
  const ownerDocument = container.ownerDocument || document;
  const root = ownerDocument.createElement("div");
  root.className = "chip-profile-overlay";
  root.dataset.state = "loading";
  const meta = ownerDocument.createElement("div");
  meta.className = "chip-profile-meta";
  const summary = ownerDocument.createElement("span");
  summary.className = "chip-profile-summary";
  summary.textContent = "筹码加载中";
  const provenance = ownerDocument.createElement("span");
  provenance.className = "chip-profile-provenance";
  meta.append(summary, provenance);
  const svg = ownerDocument.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("aria-label", "筹码峰");
  svg.setAttribute("role", "img");
  root.append(meta, svg);
  container.append(root);
  return { root, meta: summary, provenance, svg, ownerDocument };
}

function drawChipProfile(overlay, snapshot, state, candleSeries, container) {
  overlay.root.dataset.state = state;
  overlay.svg.replaceChildren();
  if (state !== "ready" || !snapshot) {
    overlay.meta.textContent = state === "loading" ? "筹码加载中" : "筹码待回填";
    overlay.provenance.textContent = "";
    overlay.root.removeAttribute?.("title");
    return;
  }

  const profileWidth = Math.min(180, Math.max(110, container.clientWidth * 0.14));
  const profileHeight = container.clientHeight;
  const rows = chipProfileRows(
    snapshot,
    (price) => candleSeries.priceToCoordinate(price),
    container.clientWidth,
    profileHeight,
  );
  const summary = chipProfileSummary(snapshot);
  overlay.root.style.width = `${profileWidth}px`;
  overlay.svg.setAttribute("viewBox", `0 0 ${profileWidth} ${profileHeight}`);
  overlay.svg.setAttribute("preserveAspectRatio", "none");
  overlay.meta.textContent = `${summary.date} · 成本 ${summary.averageCost} · 获利 ${summary.winnerRate}`;
  overlay.provenance.textContent = summary.source;
  overlay.root.setAttribute?.("title", summary.source);
  const lines = rows.map((row) => {
    const line = overlay.ownerDocument.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("class", `chip-profile-row ${row.tone}${row.dominant ? " dominant" : ""}`);
    line.setAttribute("x1", Math.max(0, profileWidth - row.width).toFixed(2));
    line.setAttribute("x2", profileWidth.toFixed(2));
    line.setAttribute("y1", row.y.toFixed(2));
    line.setAttribute("y2", row.y.toFixed(2));
    line.setAttribute("data-price", row.price);
    line.setAttribute("data-weight", row.weight);
    return line;
  });
  overlay.svg.replaceChildren(...lines);
}

export function mountChart(container, bars, hits = [], { onChipDateChange } = {}) {
  if (!window.LightweightCharts || !container) {
    return {
      resize() {},
      setChipProfile() {},
      setChipProfileVisible() {},
      destroy() {},
    };
  }
  container.replaceChildren();
  const chart = window.LightweightCharts.createChart(container, {
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
  const timeScale = chart.timeScale();
  const cancelInitialFit = fitChartAfterLayout(timeScale, undefined, bars.length);
  const overlay = createChipProfileOverlay(container);
  let active = true;
  let chipSnapshot = null;
  let chipState = "loading";
  let redrawFrame = null;
  let lastChipDate = null;
  const redraw = () => {
    redrawFrame = null;
    if (!active || overlay.root.hidden) return;
    drawChipProfile(overlay, chipSnapshot, chipState, candleSeries, container);
  };
  const scheduleRedraw = () => {
    if (!active || redrawFrame !== null) return;
    redrawFrame = window.requestAnimationFrame(redraw);
  };
  const handleCrosshair = (param) => {
    if (!active || typeof onChipDateChange !== "function") return;
    const date = param?.point && param.seriesData?.has?.(candleSeries)
      ? selectedChipDate(param)
      : null;
    if (date === lastChipDate) return;
    lastChipDate = date;
    onChipDateChange(date);
  };
  if (typeof chart.subscribeCrosshairMove === "function") chart.subscribeCrosshairMove(handleCrosshair);
  if (typeof timeScale.subscribeVisibleLogicalRangeChange === "function") {
    timeScale.subscribeVisibleLogicalRangeChange(scheduleRedraw);
  }
  container.addEventListener?.("wheel", scheduleRedraw, { passive: true });
  container.addEventListener?.("pointerup", scheduleRedraw);
  const resize = () => {
    if (!active || !container.clientWidth || !container.clientHeight) return;
    chart.resize(container.clientWidth, container.clientHeight);
    scheduleRedraw();
  };
  const resizeObserver = typeof window.ResizeObserver === "function"
    ? new window.ResizeObserver(resize)
    : null;
  if (resizeObserver) resizeObserver.observe(container);
  else window.addEventListener("resize", resize);
  return {
    resize,
    setChipProfile: (snapshot, state = snapshot ? "ready" : "pending") => {
      chipSnapshot = snapshot;
      chipState = state;
      scheduleRedraw();
    },
    setChipProfileVisible: (visible) => {
      overlay.root.hidden = !visible;
      if (visible) scheduleRedraw();
    },
    destroy: () => {
      active = false;
      if (redrawFrame !== null) window.cancelAnimationFrame?.(redrawFrame);
      if (typeof chart.unsubscribeCrosshairMove === "function") chart.unsubscribeCrosshairMove(handleCrosshair);
      if (typeof timeScale.unsubscribeVisibleLogicalRangeChange === "function") {
        timeScale.unsubscribeVisibleLogicalRangeChange(scheduleRedraw);
      }
      container.removeEventListener?.("wheel", scheduleRedraw);
      container.removeEventListener?.("pointerup", scheduleRedraw);
      resizeObserver?.disconnect();
      if (!resizeObserver) window.removeEventListener("resize", resize);
      cancelInitialFit();
      overlay.root.remove();
      chart.remove();
    },
  };
}
