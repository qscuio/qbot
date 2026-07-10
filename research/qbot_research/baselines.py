from __future__ import annotations

import math
from dataclasses import replace
from typing import Any, Final, Sequence

import polars as pl

from qbot_research.validation import ValidationConfig, validate_archetype

BASELINE_NAMES: Final[tuple[str, ...]] = (
    "relative_strength_20_60",
    "ma20_ma60_trend",
    "volatility_contraction_breakout",
    "scan_ranker_a",
)

SCAN_RANKER_A_POOLS: Final[frozenset[str]] = frozenset(
    {"pool_short_a", "pool_mid_a", "pool_long_a"}
)


def apply_baseline(frame: pl.DataFrame, baseline_name: str) -> pl.DataFrame:
    if baseline_name == "relative_strength_20_60":
        return _relative_strength_20_60(frame)
    if baseline_name == "ma20_ma60_trend":
        return _ma20_ma60_trend(frame)
    if baseline_name == "volatility_contraction_breakout":
        return _volatility_contraction_breakout(frame)
    if baseline_name == "scan_ranker_a":
        return _scan_ranker_a(frame)
    raise ValueError(f"Unsupported baseline: {baseline_name}")


def evaluate_baselines(
    frame: pl.DataFrame,
    config: ValidationConfig,
    baseline_names: Sequence[str] = BASELINE_NAMES,
) -> dict[str, dict[str, Any]]:
    baseline_config = replace(
        config,
        signal_column="baseline_signal",
        score_column="baseline_score",
    )
    results: dict[str, dict[str, Any]] = {}
    for baseline_name in baseline_names:
        baseline_frame = apply_baseline(frame, baseline_name)
        results[baseline_name] = validate_archetype(
            baseline_frame,
            {"archetype_id": baseline_name},
            baseline_config,
        )
    return results


def _relative_strength_20_60(frame: pl.DataFrame) -> pl.DataFrame:
    required_columns = (
        "trade_date",
        "code",
        "return_20d",
        "return_60d",
        "market_return_20d",
        "market_return_60d",
    )
    _require_columns(frame, required_columns, "relative_strength_20_60")
    rows = list(frame.iter_rows(named=True))
    scores = [
        _float_value(row, "return_20d")
        - _float_value(row, "market_return_20d")
        + _float_value(row, "return_60d")
        - _float_value(row, "market_return_60d")
        for row in rows
    ]
    selected_indices: set[int] = set()
    indices_by_date: dict[Any, list[int]] = {}
    for index, row in enumerate(rows):
        indices_by_date.setdefault(row["trade_date"], []).append(index)
    for group_indices in indices_by_date.values():
        top_count = max(1, math.ceil(len(group_indices) * 0.10))
        ordered = sorted(
            group_indices,
            key=lambda index: (-scores[index], str(rows[index]["code"])),
        )
        selected_indices.update(ordered[:top_count])

    return _with_baseline_columns(
        frame,
        "relative_strength_20_60",
        [index in selected_indices for index in range(len(rows))],
        scores,
    )


def _ma20_ma60_trend(frame: pl.DataFrame) -> pl.DataFrame:
    required_columns = ("close", "ma20", "ma60", "ma20_prev")
    _require_columns(frame, required_columns, "ma20_ma60_trend")
    signals: list[bool] = []
    scores: list[float] = []
    for row in frame.iter_rows(named=True):
        close = _float_value(row, "close")
        ma20 = _float_value(row, "ma20")
        ma60 = _float_value(row, "ma60")
        ma20_prev = _float_value(row, "ma20_prev")
        signals.append(close > ma20 > ma60 and ma20 > ma20_prev)
        scores.append((close / ma20 - 1.0) + (ma20 / ma60 - 1.0) + (ma20 / ma20_prev - 1.0))
    return _with_baseline_columns(frame, "ma20_ma60_trend", signals, scores)


def _volatility_contraction_breakout(frame: pl.DataFrame) -> pl.DataFrame:
    required_columns = (
        "close",
        "volatility_5d",
        "volatility_20d",
        "volatility_60d",
        "prior_20d_high",
    )
    _require_columns(frame, required_columns, "volatility_contraction_breakout")
    signals: list[bool] = []
    scores: list[float] = []
    for row in frame.iter_rows(named=True):
        close = _float_value(row, "close")
        volatility_5d = _float_value(row, "volatility_5d")
        volatility_20d = _float_value(row, "volatility_20d")
        volatility_60d = _float_value(row, "volatility_60d")
        prior_20d_high = _float_value(row, "prior_20d_high")
        signals.append(
            volatility_5d < volatility_20d < volatility_60d and close > prior_20d_high
        )
        scores.append(
            (volatility_20d - volatility_5d)
            + (volatility_60d - volatility_20d)
            + (close / prior_20d_high - 1.0)
        )
    return _with_baseline_columns(frame, "volatility_contraction_breakout", signals, scores)


def _scan_ranker_a(frame: pl.DataFrame) -> pl.DataFrame:
    required_columns = ("scan_ranker_pool_id", "scan_ranker_score")
    _require_columns(frame, required_columns, "scan_ranker_a")
    signals: list[bool] = []
    scores: list[float] = []
    for row in frame.iter_rows(named=True):
        pool_id = row["scan_ranker_pool_id"]
        if pool_id is None:
            raise ValueError("scan_ranker_pool_id must be non-null")
        score = _float_value(row, "scan_ranker_score")
        signals.append(pool_id in SCAN_RANKER_A_POOLS)
        scores.append(score)
    return _with_baseline_columns(frame, "scan_ranker_a", signals, scores)


def _with_baseline_columns(
    frame: pl.DataFrame,
    baseline_name: str,
    signals: Sequence[bool],
    scores: Sequence[float],
) -> pl.DataFrame:
    if len(signals) != frame.height or len(scores) != frame.height:
        raise ValueError("baseline signal and score lengths must match frame height")
    return frame.with_columns(
        [
            pl.lit(baseline_name).alias("baseline_name"),
            pl.Series("baseline_signal", list(signals)),
            pl.Series("baseline_score", list(scores)),
        ]
    )


def _require_columns(frame: pl.DataFrame, required_columns: tuple[str, ...], context: str) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"{context} missing required columns: {missing_csv}")


def _float_value(row: dict[str, Any], column: str) -> float:
    value = row[column]
    if value is None:
        raise ValueError(f"{column} must be non-null")
    return float(value)
