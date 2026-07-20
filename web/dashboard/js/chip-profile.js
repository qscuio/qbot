export const MAX_CHIP_PROFILE_ROWS = 60;

const MIN_PROFILE_WIDTH = 110;
const MAX_PROFILE_WIDTH = 180;
const PROFILE_WIDTH_RATIO = 0.14;

function finiteNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function fixed(value, suffix = "") {
  const number = finiteNumber(value);
  return number === null ? "—" : `${number.toFixed(2)}${suffix}`;
}

export function chipProfileRows(snapshot, priceToCoordinate, chartWidth, chartHeight) {
  if (typeof priceToCoordinate !== "function") return [];
  const width = finiteNumber(chartWidth);
  const height = finiteNumber(chartHeight);
  if (width === null || width <= 0 || height === null || height <= 0) return [];

  const currentPrice = finiteNumber(snapshot?.currentPrice);
  const dominantPeakPrice = finiteNumber(snapshot?.dominantPeakPrice);
  const maximumWidth = Math.min(
    MAX_PROFILE_WIDTH,
    Math.max(MIN_PROFILE_WIDTH, width * PROFILE_WIDTH_RATIO),
  );
  const buckets = (Array.isArray(snapshot?.distribution) ? snapshot.distribution : [])
    .map((bucket) => ({
      price: finiteNumber(bucket?.price),
      weight: finiteNumber(bucket?.weight),
    }))
    .filter(({ price, weight }) => price !== null && price > 0 && weight !== null && weight >= 0)
    .slice(0, MAX_CHIP_PROFILE_ROWS);
  const peakWeight = Math.max(...buckets.map(({ weight }) => weight), 0);

  return buckets.flatMap(({ price, weight }) => {
    const y = finiteNumber(priceToCoordinate(price));
    if (y === null || y < 0 || y > height) return [];
    return [{
      price,
      weight,
      y,
      width: peakWeight > 0 ? weight / peakWeight * maximumWidth : 0,
      tone: currentPrice !== null && price <= currentPrice ? "profit" : "loss",
      dominant: dominantPeakPrice !== null && Math.abs(price - dominantPeakPrice) < 0.000001,
    }];
  });
}

export function chipProfileSummary(snapshot) {
  if (!snapshot || typeof snapshot !== "object" || Array.isArray(snapshot)) {
    return { date: "筹码待回填", averageCost: "—", winnerRate: "—", source: "" };
  }
  return {
    date: snapshot.resolvedDate || "筹码待回填",
    averageCost: fixed(snapshot.averageCost),
    winnerRate: fixed(snapshot.winnerRate, "%"),
    source: [snapshot.sourceLabel, snapshot.validationLabel].filter(Boolean).join(" · "),
  };
}
