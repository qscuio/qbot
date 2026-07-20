import test from "node:test";
import assert from "node:assert/strict";

import {
  chipProfileRows,
  chipProfileSummary,
  MAX_CHIP_PROFILE_ROWS,
} from "../js/chip-profile.js";

const snapshot = {
  resolvedDate: "2026-07-17",
  currentPrice: 10,
  averageCost: 9.62,
  winnerRate: 63.25,
  dominantPeakPrice: 10,
  sourceLabel: "QBot 估算",
  validationLabel: "未验证",
  distribution: [
    { price: 11, weight: 0.25 },
    { price: 10, weight: 0.5 },
    { price: 9, weight: 0.1 },
  ],
};

test("chip profile maps prices to bounded right-aligned rows", () => {
  const rows = chipProfileRows(snapshot, (price) => 300 - price * 10, 1000, 500);

  assert.equal(rows.length, 3);
  assert.equal(Math.max(...rows.map((row) => row.width)), 140);
  assert.equal(rows.find((row) => row.price === 11).tone, "loss");
  assert.equal(rows.find((row) => row.price === 9).tone, "profit");
  assert.equal(rows.find((row) => row.price === 10).dominant, true);
});

test("chip profile drops invalid and off-pane buckets and keeps its DOM bounded", () => {
  const distribution = Array.from({ length: MAX_CHIP_PROFILE_ROWS + 8 }, (_, index) => ({
    price: index + 1,
    weight: index + 1,
  }));
  distribution.unshift({ price: -1, weight: 1 }, { price: 12, weight: -1 });

  const rows = chipProfileRows(
    { ...snapshot, distribution },
    (price) => price === 1 ? -2 : price,
    600,
    300,
  );

  assert.ok(rows.length <= MAX_CHIP_PROFILE_ROWS);
  assert.equal(rows.some((row) => row.price <= 1), false);
});

test("chip profile supports zero weights and formats compact provenance", () => {
  const rows = chipProfileRows(
    { ...snapshot, distribution: [{ price: 10, weight: 0 }] },
    () => 100,
    300,
    200,
  );

  assert.equal(rows[0].width, 0);
  assert.deepEqual(chipProfileSummary(snapshot), {
    date: "2026-07-17",
    averageCost: "9.62",
    winnerRate: "63.25%",
    source: "QBot 估算 · 未验证",
  });
  assert.deepEqual(chipProfileSummary(null), {
    date: "筹码待回填",
    averageCost: "—",
    winnerRate: "—",
    source: "",
  });
});
