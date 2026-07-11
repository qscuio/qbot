from __future__ import annotations

from collections import defaultdict
from typing import Any, Final

import polars as pl

from qbot_research.contracts import Horizon

HORIZON_DAYS: Final[dict[Horizon, int]] = {
    "week": 5,
    "month": 20,
    "quarter": 60,
    "year": 250,
}
PUBLISHABLE_HORIZONS: Final[frozenset[Horizon]] = frozenset({"week", "month"})
RESEARCH_ONLY_HORIZONS: Final[frozenset[Horizon]] = frozenset({"quarter"})
DESCRIPTIVE_ONLY_HORIZONS: Final[frozenset[Horizon]] = frozenset({"year"})
MARKET_RETURN_COLUMNS: Final[tuple[str, ...]] = (
    "sse_change_pct",
    "szse_change_pct",
    "chinext_change_pct",
    "star50_change_pct",
)
REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "trade_date",
    "code",
    "adjusted_close",
    "adjusted_high",
    "adjusted_low",
    "amount",
    "is_st",
    "is_suspended",
    "sector_code",
)


def label_samples(frame: pl.DataFrame, horizon: Horizon) -> pl.DataFrame:
    _validate_horizon(horizon)
    _require_columns(frame, REQUIRED_COLUMNS)

    market_columns = [column for column in MARKET_RETURN_COLUMNS if column in frame.columns]
    if not market_columns:
        raise ValueError(
            "label_samples requires at least one market return column: "
            f"{', '.join(MARKET_RETURN_COLUMNS)}"
        )

    indexed = frame.with_row_index("__row_id")
    sorted_frame = indexed.sort(["code", "trade_date", "__row_id"])
    rows = list(sorted_frame.iter_rows(named=True))
    row_count = len(rows)
    horizon_days = HORIZON_DAYS[horizon]

    future_return: list[float | None] = [None] * row_count
    future_market_excess: list[float | None] = [None] * row_count
    future_industry_excess: list[float | None] = [None] * row_count
    future_max_drawdown: list[float | None] = [None] * row_count
    future_max_favorable_excursion: list[float | None] = [None] * row_count
    tradable_sample: list[bool] = [_is_tradable_row(row) for row in rows]
    strength_score: list[float | None] = [None] * row_count
    is_positive: list[bool] = [False] * row_count

    rows_by_code: dict[str, list[int]] = defaultdict(list)
    for row_index, row in enumerate(rows):
        rows_by_code[str(row["code"])].append(row_index)

    for code_indices in rows_by_code.values():
        for position, row_index in enumerate(code_indices):
            if position + horizon_days >= len(code_indices):
                continue

            base_close = _float_value(rows[row_index], "adjusted_close")
            if base_close <= 0.0:
                continue

            future_indices = code_indices[position + 1 : position + horizon_days + 1]
            target_index = code_indices[position + horizon_days]
            target_close = _float_value(rows[target_index], "adjusted_close")

            future_return[row_index] = target_close / base_close - 1.0
            future_max_drawdown[row_index] = min(
                min(_float_value(rows[index], "adjusted_low") / base_close - 1.0 for index in future_indices),
                0.0,
            )
            future_max_favorable_excursion[row_index] = max(
                max(
                    _float_value(rows[index], "adjusted_high") / base_close - 1.0
                    for index in future_indices
                ),
                0.0,
            )

            market_return = 1.0
            for index in future_indices:
                market_return *= 1.0 + _market_daily_return(rows[index], market_columns)
            row_future_return = future_return[row_index]
            if row_future_return is not None:
                future_market_excess[row_index] = row_future_return - (market_return - 1.0)

    rows_by_trade_date_and_sector: dict[tuple[Any, str], list[int]] = defaultdict(list)
    for row_index, row in enumerate(rows):
        rows_by_trade_date_and_sector[(row["trade_date"], str(row["sector_code"]))].append(row_index)

    for group_indices in rows_by_trade_date_and_sector.values():
        group_returns: list[float] = []
        for index in group_indices:
            row_future_return = future_return[index]
            if row_future_return is not None:
                group_returns.append(row_future_return)
        if not group_returns:
            continue

        industry_average = sum(group_returns) / len(group_returns)
        for index in group_indices:
            row_future_return = future_return[index]
            if row_future_return is None:
                continue
            future_industry_excess[index] = row_future_return - industry_average

    rows_by_trade_date: dict[Any, list[int]] = defaultdict(list)
    for row_index, row in enumerate(rows):
        rows_by_trade_date[row["trade_date"]].append(row_index)

    for group_indices in rows_by_trade_date.values():
        eligible_indices = [
            index
            for index in group_indices
            if future_return[index] is not None
            and future_market_excess[index] is not None
            and future_industry_excess[index] is not None
            and future_max_drawdown[index] is not None
            and future_max_favorable_excursion[index] is not None
        ]
        if not eligible_indices:
            continue

        component_scores = [
            _percentile_scores(eligible_indices, rows, future_return),
            _percentile_scores(eligible_indices, rows, future_market_excess),
            _percentile_scores(eligible_indices, rows, future_industry_excess),
            _percentile_scores(eligible_indices, rows, future_max_drawdown),
            _percentile_scores(eligible_indices, rows, future_max_favorable_excursion),
        ]

        for index in eligible_indices:
            strength_score[index] = sum(scores[index] for scores in component_scores) / len(
                component_scores
            )

    for row_index in range(row_count):
        score = strength_score[row_index]
        if score is None:
            continue
        row_future_return = future_return[row_index]
        row_market_excess = future_market_excess[row_index]
        row_industry_excess = future_industry_excess[row_index]
        if (
            tradable_sample[row_index]
            and row_future_return is not None
            and row_market_excess is not None
            and row_industry_excess is not None
            and row_future_return > 0.0
            and row_market_excess > 0.0
            and row_industry_excess > 0.0
            and score >= 90.0
        ):
            is_positive[row_index] = True

    labeled = (
        sorted_frame.with_columns(
            [
                pl.Series("future_return", future_return),
                pl.Series("future_market_excess", future_market_excess),
                pl.Series("future_industry_excess", future_industry_excess),
                pl.Series("future_max_drawdown", future_max_drawdown),
                pl.Series(
                    "future_max_favorable_excursion",
                    future_max_favorable_excursion,
                ),
                pl.Series("tradable_sample", tradable_sample),
                pl.Series("strength_score", strength_score),
                pl.Series("is_positive", is_positive),
            ]
        )
        .sort("__row_id")
        .drop("__row_id")
    )
    return labeled


def _validate_horizon(horizon: Horizon) -> None:
    if horizon not in HORIZON_DAYS:
        raise ValueError(
            f"Unsupported horizon '{horizon}'. Expected one of: {', '.join(HORIZON_DAYS)}"
        )


def _require_columns(frame: pl.DataFrame, required_columns: tuple[str, ...]) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"label_samples missing required columns: {missing_csv}")


def _float_value(row: dict[str, Any], column: str) -> float:
    value = row[column]
    if value is None:
        raise ValueError(f"label_samples requires non-null values in '{column}'")
    return float(value)


def _market_daily_return(row: dict[str, Any], market_columns: list[str]) -> float:
    values = [float(row[column]) / 100.0 for column in market_columns if row[column] is not None]
    if not values:
        raise ValueError(
            "label_samples requires a market benchmark return for future row "
            f"trade_date={row['trade_date']} code={row['code']}"
        )
    return sum(values) / len(values)


def _is_tradable_row(row: dict[str, Any]) -> bool:
    amount = float(row["amount"]) if row["amount"] is not None else 0.0
    return (
        not bool(row["is_st"])
        and not bool(row["is_suspended"])
        and amount > 0.0
        and float(row["adjusted_close"]) > 0.0
    )


def _percentile_scores(
    eligible_indices: list[int],
    rows: list[dict[str, Any]],
    values: list[float | None],
) -> dict[int, float]:
    ordered_indices = sorted(
        eligible_indices,
        key=lambda index: (
            float("-inf") if values[index] is None else values[index],
            str(rows[index]["code"]),
        ),
    )
    if len(ordered_indices) == 1:
        return {ordered_indices[0]: 100.0}

    denominator = len(ordered_indices) - 1
    return {
        index: 100.0 * rank / denominator for rank, index in enumerate(ordered_indices)
    }
