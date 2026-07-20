import test from "node:test";
import assert from "node:assert/strict";

import * as chartModule from "../js/chart.js";

const { movingAverage, signalMarkers } = chartModule;

test("selectedChipDate accepts exact ISO candle times and rejects invalid selections", () => {
  assert.equal(chartModule.selectedChipDate({ time: "2026-07-17" }), "2026-07-17");
  assert.equal(chartModule.selectedChipDate({ time: { year: 2026, month: 7, day: 17 } }), "2026-07-17");
  assert.equal(chartModule.selectedChipDate({ time: "2026-02-30" }), null);
  assert.equal(chartModule.selectedChipDate({ time: 1784246400 }), null);
  assert.equal(chartModule.selectedChipDate({}), null);
});

test("mounted chart emits each crosshair candle once, restores latest on leave, and cleans up", () => {
  const originalWindow = globalThis.window;
  const originalDocument = globalThis.document;
  let crosshairHandler;
  let unsubscribedCrosshair;
  let visibleRangeHandler;
  let unsubscribedVisibleRange;
  const series = [];
  const frames = [];
  class FakeElement {
    constructor(tagName = "div") {
      this.tagName = tagName;
      this.children = [];
      this.dataset = {};
      this.style = {};
      this.attributes = new Map();
      this.listeners = new Map();
      this.className = "";
      this.classList = { toggle() {} };
      this.clientWidth = 800;
      this.clientHeight = 500;
    }
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) { this.children = [...children]; }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    addEventListener(name, handler) { this.listeners.set(name, handler); }
    removeEventListener(name) { this.listeners.delete(name); }
    remove() { this.removed = true; }
  }
  globalThis.document = {
    createElement: (tagName) => new FakeElement(tagName),
    createElementNS: (_namespace, tagName) => new FakeElement(tagName),
  };
  const timeScale = {
    fitContent() {},
    setVisibleLogicalRange() {},
    subscribeVisibleLogicalRangeChange: (handler) => { visibleRangeHandler = handler; },
    unsubscribeVisibleLogicalRangeChange: (handler) => { unsubscribedVisibleRange = handler; },
  };
  const chart = {
    addSeries: () => {
      const item = {
        setData() {},
        priceScale: () => ({ applyOptions() {} }),
        priceToCoordinate: (price) => price * 10,
      };
      series.push(item);
      return item;
    },
    timeScale: () => timeScale,
    subscribeCrosshairMove: (handler) => { crosshairHandler = handler; },
    unsubscribeCrosshairMove: (handler) => { unsubscribedCrosshair = handler; },
    resize() {},
    remove() {},
  };
  globalThis.window = {
    LightweightCharts: {
      CandlestickSeries: {}, HistogramSeries: {}, LineSeries: {},
      createChart: () => chart,
    },
    requestAnimationFrame: (callback) => { frames.push(callback); return frames.length; },
    cancelAnimationFrame() {},
    addEventListener() {},
    removeEventListener() {},
  };
  const selected = [];
  const container = new FakeElement();

  try {
    const handle = chartModule.mountChart(
      container,
      [{ time: "2026-07-17", open: 10, high: 12, low: 9, close: 11, volume: 5 }],
      [],
      { onChipDateChange: (date) => selected.push(date) },
    );
    const candle = (time) => ({ time, point: { x: 10, y: 10 }, seriesData: new Map([[series[0], {}]]) });
    crosshairHandler(candle("2026-07-17"));
    crosshairHandler(candle("2026-07-17"));
    crosshairHandler(candle("bad"));
    crosshairHandler({ point: undefined });
    assert.deepEqual(selected, ["2026-07-17", null]);
    assert.equal(typeof handle.setChipProfile, "function");
    assert.equal(typeof handle.setChipProfileVisible, "function");
    assert.equal(typeof visibleRangeHandler, "function");
    handle.destroy();
    assert.equal(unsubscribedCrosshair, crosshairHandler);
    assert.equal(unsubscribedVisibleRange, visibleRangeHandler);
    crosshairHandler(candle("2026-07-18"));
    assert.deepEqual(selected, ["2026-07-17", null]);
  } finally {
    globalThis.window = originalWindow;
    globalThis.document = originalDocument;
  }
});

test("moving average begins when the requested window is complete", () => {
  const bars = [1, 2, 3, 4, 5, 6].map((close, index) => ({
    time: `2026-07-0${index + 1}`,
    close,
  }));

  assert.deepEqual(movingAverage(bars, 3), [
    { time: "2026-07-03", value: 2 },
    { time: "2026-07-04", value: 3 },
    { time: "2026-07-05", value: 4 },
    { time: "2026-07-06", value: 5 },
  ]);
});

test("signal markers share the most recent available bar", () => {
  const markers = signalMarkers(
    [{ time: "2026-07-17" }, { time: "2026-07-18" }],
    [{ name: "Volume surge" }, { name: "Top 20" }],
  );

  assert.equal(markers.length, 2);
  assert.equal(markers[0].time, "2026-07-18");
  assert.equal(markers[1].text, "Top 20");
});

test("activity histogram uses amount when stored volumes are missing", () => {
  const activity = chartModule.activitySeries([
    { time: "2026-07-17", open: 10, close: 11, volume: 0, amount: 1_250_000 },
    { time: "2026-07-18", open: 11, close: 10, volume: 0, amount: 980_000 },
  ]);

  assert.equal(activity.metric, "amount");
  assert.equal(activity.label, "AMOUNT · volume unavailable");
  assert.deepEqual(activity.data.map(({ time, value }) => ({ time, value })), [
    { time: "2026-07-17", value: 1_250_000 },
    { time: "2026-07-18", value: 980_000 },
  ]);
});

test("initial chart view waits for layout and shows the most recent 120 bars", () => {
  assert.equal(typeof chartModule.fitChartAfterLayout, "function");
  const frames = [];
  let fits = 0;
  const ranges = [];
  const timeScale = {
    fitContent: () => { fits += 1; },
    setVisibleLogicalRange: (range) => ranges.push(range),
  };

  chartModule.fitChartAfterLayout(timeScale, (callback) => frames.push(callback), 500);

  assert.equal(fits, 0);
  assert.equal(frames.length, 1);
  frames.shift()();
  assert.equal(fits, 0);
  assert.equal(frames.length, 1);
  frames.shift()();
  assert.equal(fits, 0);
  assert.deepEqual(ranges, [{ from: 380, to: 499 }]);
});

test("initial chart view fits all data when fewer than 120 bars exist", () => {
  const frames = [];
  let fits = 0;
  const timeScale = {
    fitContent: () => { fits += 1; },
    setVisibleLogicalRange: () => assert.fail("short histories should fit fully"),
  };

  chartModule.fitChartAfterLayout(timeScale, (callback) => frames.push(callback), 80);
  frames.shift()();
  frames.shift()();

  assert.equal(fits, 1);
});

test("chart interaction pans inside fixed data boundaries", () => {
  assert.deepEqual(chartModule.CHART_INTERACTION_OPTIONS, {
    handleScroll: {
      pressedMouseMove: true,
      horzTouchDrag: true,
    },
    handleScale: {
      axisPressedMouseMove: { time: false, price: true },
    },
  });
});
