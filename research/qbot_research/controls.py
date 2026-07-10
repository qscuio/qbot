from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from statistics import pstdev
from typing import Any, Final, Literal

import polars as pl

ControlType = Literal["ordinary", "failed_breakout", "negative_excess"]

CONTROL_TYPE_ORDER: Final[tuple[ControlType, ...]] = (
    "ordinary",
    "failed_breakout",
    "negative_excess",
)
REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "trade_date",
    "code",
    "sector_code",
    "tradable_sample",
    "future_return",
    "future_market_excess",
    "future_max_favorable_excursion",
    "is_positive",
)
EXPLICIT_MARKET_CAP_COLUMNS: Final[tuple[str, ...]] = (
    "market_cap",
    "float_market_cap",
    "free_float_market_cap",
)
PRICE_COLUMNS: Final[tuple[str, ...]] = ("adjusted_close", "close", "bar_close")


@dataclass(frozen=True)
class ControlMatchConfig:
    bucket_count: int = 5
    controls_per_type: int = 1


def match_controls(
    samples: pl.DataFrame,
    candidates: pl.DataFrame,
    config: ControlMatchConfig,
) -> pl.DataFrame:
    if config.bucket_count <= 0:
        raise ValueError("bucket_count must be positive")
    if config.controls_per_type <= 0:
        raise ValueError("controls_per_type must be positive")

    _require_columns(samples, REQUIRED_COLUMNS, "samples")
    _require_columns(candidates, REQUIRED_COLUMNS, "candidates")

    enriched_candidates = _enrich_candidates(candidates, config)
    candidate_rows = list(enriched_candidates.iter_rows(named=True))
    candidate_lookup = {
        (row["trade_date"], str(row["code"])): row for row in candidate_rows
    }

    matches: list[dict[str, Any]] = []
    sample_rows = list(samples.iter_rows(named=True))
    for sample in sample_rows:
        sample_key = (sample["trade_date"], str(sample["code"]))
        enriched_sample = candidate_lookup.get(sample_key)
        if enriched_sample is None:
            raise ValueError(
                "samples must be a subset of candidates when controls derive buckets from history"
            )

        eligible_controls = [
            row
            for row in candidate_rows
            if row["trade_date"] == enriched_sample["trade_date"]
            and str(row["sector_code"]) == str(enriched_sample["sector_code"])
            and bool(row["tradable_sample"]) is bool(enriched_sample["tradable_sample"])
            and int(row["market_cap_bucket"]) == int(enriched_sample["market_cap_bucket"])
            and int(row["price_bucket"]) == int(enriched_sample["price_bucket"])
            and int(row["amount_20d_bucket"]) == int(enriched_sample["amount_20d_bucket"])
            and int(row["volatility_20d_bucket"]) == int(enriched_sample["volatility_20d_bucket"])
            and str(row["code"]) != str(enriched_sample["code"])
            and not bool(row["is_positive"])
        ]

        controls_by_type: dict[ControlType, list[dict[str, Any]]] = defaultdict(list)
        for row in eligible_controls:
            controls_by_type[_classify_control(row)].append(row)

        for control_type in CONTROL_TYPE_ORDER:
            ranked_controls = sorted(
                controls_by_type.get(control_type, []),
                key=lambda row: (
                    _distance(enriched_sample, row),
                    str(row["code"]),
                ),
            )
            for control in ranked_controls[: config.controls_per_type]:
                matches.append(
                    {
                        "sample_trade_date": enriched_sample["trade_date"],
                        "sample_code": str(enriched_sample["code"]),
                        "control_trade_date": control["trade_date"],
                        "control_code": str(control["code"]),
                        "control_type": control_type,
                        "sector_code": str(control["sector_code"]),
                        "tradable_sample": bool(control["tradable_sample"]),
                        "market_cap_bucket": int(control["market_cap_bucket"]),
                        "price_bucket": int(control["price_bucket"]),
                        "amount_20d_bucket": int(control["amount_20d_bucket"]),
                        "volatility_20d_bucket": int(control["volatility_20d_bucket"]),
                        "distance": _distance(enriched_sample, control),
                    }
                )

    if not matches:
        return pl.DataFrame(
            schema={
                "sample_trade_date": pl.Date,
                "sample_code": pl.String,
                "control_trade_date": pl.Date,
                "control_code": pl.String,
                "control_type": pl.String,
                "sector_code": pl.String,
                "tradable_sample": pl.Boolean,
                "market_cap_bucket": pl.Int64,
                "price_bucket": pl.Int64,
                "amount_20d_bucket": pl.Int64,
                "volatility_20d_bucket": pl.Int64,
                "distance": pl.Float64,
            }
        )

    order_by_type = {control_type: index for index, control_type in enumerate(CONTROL_TYPE_ORDER)}
    return (
        pl.DataFrame(matches)
        .with_columns(
            pl.col("control_type")
            .replace_strict(order_by_type, return_dtype=pl.Int64)
            .alias("__control_type_order")
        )
        .sort(["sample_trade_date", "sample_code", "__control_type_order", "control_code"])
        .drop("__control_type_order")
    )


def _enrich_candidates(frame: pl.DataFrame, config: ControlMatchConfig) -> pl.DataFrame:
    price_column = _first_present_column(frame, PRICE_COLUMNS)
    if price_column is None:
        raise ValueError(
            "match_controls requires one of the price columns: "
            f"{', '.join(PRICE_COLUMNS)}"
        )
    market_cap_column = _first_present_column(frame, EXPLICIT_MARKET_CAP_COLUMNS)
    if market_cap_column is None and "turnover" not in frame.columns:
        raise ValueError(
            "match_controls requires a market cap column or the amount/turnover proxy inputs"
        )
    if "amount_20d_avg" not in frame.columns and "amount" not in frame.columns:
        raise ValueError("match_controls requires amount or amount_20d_avg")

    indexed = frame.with_row_index("__row_id")
    sorted_frame = indexed.sort(["code", "trade_date", "__row_id"])
    rows = list(sorted_frame.iter_rows(named=True))

    market_cap_metric: list[float] = [0.0] * len(rows)
    price_metric: list[float] = [0.0] * len(rows)
    amount_20d_metric: list[float] = [0.0] * len(rows)
    volatility_20d_metric: list[float] = [0.0] * len(rows)

    rows_by_code: dict[str, list[int]] = defaultdict(list)
    for row_index, row in enumerate(rows):
        rows_by_code[str(row["code"])].append(row_index)
        price_metric[row_index] = float(row[price_column])
        if market_cap_column is not None and row[market_cap_column] is not None:
            market_cap_metric[row_index] = float(row[market_cap_column])
        else:
            turnover_value = row.get("turnover")
            if turnover_value is None or float(turnover_value) <= 0.0:
                raise ValueError(
                    "match_controls requires positive turnover when market cap is not present"
                )
            market_cap_metric[row_index] = float(row["amount"]) / float(turnover_value)

    for code_indices in rows_by_code.values():
        amount_history: list[float] = []
        return_history: list[float] = []
        previous_close: float | None = None
        for row_index in code_indices:
            row = rows[row_index]
            if "amount_20d_avg" in row and row["amount_20d_avg"] is not None:
                amount_20d_metric[row_index] = float(row["amount_20d_avg"])
            else:
                amount_history.append(float(row["amount"]))
                amount_window = amount_history[-20:]
                amount_20d_metric[row_index] = sum(amount_window) / len(amount_window)

            current_close = float(row[price_column])
            if previous_close is not None and previous_close > 0.0:
                return_history.append(current_close / previous_close - 1.0)
            previous_close = current_close

            if "volatility_20d" in row and row["volatility_20d"] is not None:
                volatility_20d_metric[row_index] = float(row["volatility_20d"])
            else:
                return_window = return_history[-20:]
                volatility_20d_metric[row_index] = (
                    float(pstdev(return_window)) if len(return_window) > 1 else 0.0
                )

    rows_by_trade_date: dict[Any, list[int]] = defaultdict(list)
    for row_index, row in enumerate(rows):
        rows_by_trade_date[row["trade_date"]].append(row_index)

    market_cap_bucket: list[int] = [0] * len(rows)
    price_bucket: list[int] = [0] * len(rows)
    amount_20d_bucket: list[int] = [0] * len(rows)
    volatility_20d_bucket: list[int] = [0] * len(rows)

    for group_indices in rows_by_trade_date.values():
        _assign_buckets(market_cap_metric, group_indices, market_cap_bucket, config.bucket_count)
        _assign_buckets(price_metric, group_indices, price_bucket, config.bucket_count)
        _assign_buckets(amount_20d_metric, group_indices, amount_20d_bucket, config.bucket_count)
        _assign_buckets(
            volatility_20d_metric,
            group_indices,
            volatility_20d_bucket,
            config.bucket_count,
        )

    return (
        sorted_frame.with_columns(
            [
                pl.Series("market_cap_metric", market_cap_metric),
                pl.Series("price_metric", price_metric),
                pl.Series("amount_20d_metric", amount_20d_metric),
                pl.Series("volatility_20d_metric", volatility_20d_metric),
                pl.Series("market_cap_bucket", market_cap_bucket),
                pl.Series("price_bucket", price_bucket),
                pl.Series("amount_20d_bucket", amount_20d_bucket),
                pl.Series("volatility_20d_bucket", volatility_20d_bucket),
            ]
        )
        .sort("__row_id")
        .drop("__row_id")
    )


def _assign_buckets(
    metric_values: list[float],
    group_indices: list[int],
    bucket_output: list[int],
    bucket_count: int,
) -> None:
    unique_values = sorted({metric_values[index] for index in group_indices})
    if len(unique_values) == 1:
        only_value = unique_values[0]
        for index in group_indices:
            if metric_values[index] == only_value:
                bucket_output[index] = 0
        return

    denominator = len(unique_values)
    bucket_by_value = {
        value: min(bucket_count - 1, (rank * bucket_count) // denominator)
        for rank, value in enumerate(unique_values)
    }
    for index in group_indices:
        bucket_output[index] = bucket_by_value[metric_values[index]]


def _classify_control(row: dict[str, Any]) -> ControlType:
    if float(row["future_return"]) <= 0.0 and float(row["future_max_favorable_excursion"]) > 0.0:
        return "failed_breakout"
    if float(row["future_market_excess"]) < 0.0:
        return "negative_excess"
    return "ordinary"


def _distance(sample: dict[str, Any], candidate: dict[str, Any]) -> float:
    return sum(
        [
            _relative_distance(float(sample["market_cap_metric"]), float(candidate["market_cap_metric"])),
            _relative_distance(float(sample["price_metric"]), float(candidate["price_metric"])),
            _relative_distance(float(sample["amount_20d_metric"]), float(candidate["amount_20d_metric"])),
            _relative_distance(
                float(sample["volatility_20d_metric"]),
                float(candidate["volatility_20d_metric"]),
            ),
        ]
    )


def _relative_distance(left: float, right: float) -> float:
    baseline = max(abs(left), abs(right), 1e-9)
    return abs(left - right) / baseline


def _first_present_column(frame: pl.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def _require_columns(
    frame: pl.DataFrame,
    required_columns: tuple[str, ...],
    frame_name: str,
) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"{frame_name} missing required columns: {missing_csv}")
