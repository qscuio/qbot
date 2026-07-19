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

test("mounted chart emits candle click dates only and unsubscribes on destroy", () => {
  const originalWindow = globalThis.window;
  let clickHandler;
  let unsubscribed;
  let crosshairSubscriptions = 0;
  const series = [];
  const chart = {
    addSeries: () => {
      const item = {
        setData() {},
        priceScale: () => ({ applyOptions() {} }),
      };
      series.push(item);
      return item;
    },
    timeScale: () => ({ fitContent() {}, setVisibleLogicalRange() {} }),
    subscribeClick: (handler) => { clickHandler = handler; },
    unsubscribeClick: (handler) => { unsubscribed = handler; },
    subscribeCrosshairMove: () => { crosshairSubscriptions += 1; },
    resize() {},
    remove() {},
  };
  globalThis.window = {
    LightweightCharts: {
      CandlestickSeries: {}, HistogramSeries: {}, LineSeries: {},
      createChart: () => chart,
    },
    requestAnimationFrame: () => 1,
    addEventListener() {},
    removeEventListener() {},
  };
  const selected = [];
  const container = { replaceChildren() {}, clientWidth: 800, clientHeight: 500 };

  try {
    const handle = chartModule.mountChart(
      container,
      [{ time: "2026-07-17", open: 10, high: 12, low: 9, close: 11, volume: 5 }],
      [],
      { onCandleSelect: (date) => selected.push(date) },
    );
    assert.equal(crosshairSubscriptions, 0);
    clickHandler({ time: "2026-07-17", point: { x: 10, y: 10 }, seriesData: new Map() });
    clickHandler({ time: "bad", point: { x: 10, y: 10 }, seriesData: new Map([[series[0], {}]]) });
    clickHandler({ time: "2026-07-17", point: { x: 10, y: 10 }, seriesData: new Map([[series[0], {}]]) });
    assert.deepEqual(selected, ["2026-07-17"]);
    handle.destroy();
    assert.equal(unsubscribed, clickHandler);
    clickHandler({ time: "2026-07-18", point: { x: 10, y: 10 }, seriesData: new Map([[series[0], {}]]) });
    assert.deepEqual(selected, ["2026-07-17"]);
  } finally {
    globalThis.window = originalWindow;
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
