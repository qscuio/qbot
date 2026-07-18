import test from "node:test";
import assert from "node:assert/strict";

import { movingAverage, signalMarkers } from "../js/chart.js";

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
