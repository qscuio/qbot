from __future__ import annotations

import calendar
import statistics
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal, Sequence, cast

import polars as pl


@dataclass(frozen=True)
class Split:
    split_id: str
    train_start: date | None
    train_end: date | None
    validation_start: date
    validation_end: date
    purge_start: date | None
    purge_end: date | None
    embargo_start: date | None
    embargo_end: date | None
    train_dates: tuple[date, ...]
    validation_dates: tuple[date, ...]
    purge_dates: tuple[date, ...]
    embargo_dates: tuple[date, ...]


@dataclass(frozen=True)
class ValidationConfig:
    date_column: str = "trade_date"
    code_column: str = "code"
    signal_column: str = "candidate_signal"
    score_column: str = "candidate_score"
    label_column: str = "is_positive"
    return_column: str = "future_return"
    market_excess_column: str = "future_market_excess"
    drawdown_column: str = "future_max_drawdown"
    amount_column: str = "amount"
    regime_column: str = "regime"
    transaction_cost_bps: float = 0.0
    best_required_baseline_return: float = 0.0
    max_single_stock_contribution: float = 0.35
    max_single_period_contribution: float = 0.35


ConditionOperator = Literal[">=", ">", "<=", "<", "==", "!="]


def purged_walk_forward_splits(
    dates: list[date],
    train_months: int,
    validation_months: int,
    step_months: int,
    horizon_days: int,
) -> list[Split]:
    if train_months <= 0:
        raise ValueError("train_months must be positive")
    if validation_months <= 0:
        raise ValueError("validation_months must be positive")
    if step_months <= 0:
        raise ValueError("step_months must be positive")
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")

    ordered_dates = tuple(sorted(set(dates)))
    if not ordered_dates:
        return []

    splits: list[Split] = []
    anchor = ordered_dates[0]
    split_index = 0
    while True:
        train_start_index = _first_index_on_or_after(ordered_dates, anchor)
        validation_start_boundary = _add_months(anchor, train_months)
        validation_end_boundary = _add_months(validation_start_boundary, validation_months)
        validation_start_index = _first_index_on_or_after(ordered_dates, validation_start_boundary)
        validation_end_index = _first_index_on_or_after(ordered_dates, validation_end_boundary)

        if (
            train_start_index is None
            or validation_start_index is None
            or validation_end_index is None
            or validation_end_index <= validation_start_index
        ):
            break
        if validation_end_index + horizon_days > len(ordered_dates):
            break

        train_end_index = max(train_start_index, validation_start_index - horizon_days)
        train_dates = ordered_dates[train_start_index:train_end_index]
        validation_dates = ordered_dates[validation_start_index:validation_end_index]
        purge_dates = ordered_dates[train_end_index:validation_start_index]
        embargo_dates = ordered_dates[validation_end_index : validation_end_index + horizon_days]
        if train_dates and validation_dates:
            splits.append(
                Split(
                    split_id=f"split_{split_index}",
                    train_start=train_dates[0],
                    train_end=train_dates[-1],
                    validation_start=validation_dates[0],
                    validation_end=validation_dates[-1],
                    purge_start=purge_dates[0] if purge_dates else None,
                    purge_end=purge_dates[-1] if purge_dates else None,
                    embargo_start=embargo_dates[0] if embargo_dates else None,
                    embargo_end=embargo_dates[-1] if embargo_dates else None,
                    train_dates=train_dates,
                    validation_dates=validation_dates,
                    purge_dates=purge_dates,
                    embargo_dates=embargo_dates,
                )
            )
            split_index += 1

        next_anchor = _add_months(anchor, step_months)
        if next_anchor <= anchor:
            raise ValueError("step_months did not advance the split anchor")
        anchor = next_anchor

    return splits


def validate_archetype(
    frame: pl.DataFrame,
    candidate: dict[str, Any],
    config: ValidationConfig,
) -> dict[str, Any]:
    working = _with_candidate_signal_and_score(frame, candidate, config)
    _require_columns(
        working,
        (
            config.date_column,
            config.code_column,
            config.signal_column,
            config.score_column,
            config.label_column,
            config.return_column,
            config.market_excess_column,
            config.drawdown_column,
            config.amount_column,
            config.regime_column,
        ),
        "validate_archetype",
    )

    rows = list(working.iter_rows(named=True))
    metrics = _metrics_for_rows(rows, config)
    metrics["cluster_stability"] = _optional_float(candidate.get("stability_score"))
    majority_windows_positive_lift = _majority_windows_positive_lift(rows, config)

    release_gate_passed = (
        majority_windows_positive_lift
        and metrics["cost_adjusted_return"] > config.best_required_baseline_return
        and metrics["top_stock_contribution"] <= config.max_single_stock_contribution
        and metrics["top_period_contribution"] <= config.max_single_period_contribution
    )

    candidate_id = str(candidate.get("archetype_id", candidate.get("candidate_id", "candidate")))
    result: dict[str, Any] = {
        "candidate_id": candidate_id,
        **metrics,
        "majority_windows_positive_lift": majority_windows_positive_lift,
        "baseline_comparison": {
            "best_required_baseline_return": _finite_float(
                config.best_required_baseline_return,
                "best_required_baseline_return",
            ),
            "cost_adjusted_return_delta": _finite_float(
                metrics["cost_adjusted_return"] - config.best_required_baseline_return,
                "cost_adjusted_return_delta",
            ),
        },
        "release_gate_passed": release_gate_passed,
        "candidate_status": "validated" if release_gate_passed else "draft",
    }
    return result


def _with_candidate_signal_and_score(
    frame: pl.DataFrame,
    candidate: dict[str, Any],
    config: ValidationConfig,
) -> pl.DataFrame:
    has_signal = config.signal_column in frame.columns
    has_score = config.score_column in frame.columns
    if has_signal and has_score:
        return frame

    expressions: list[pl.Series] = []
    if not has_signal:
        conditions = candidate.get("necessary_conditions")
        if not isinstance(conditions, list) or not conditions:
            raise ValueError(
                f"validate_archetype missing required columns: {config.signal_column}"
            )
        expressions.append(pl.Series(config.signal_column, _evaluate_conditions(frame, conditions)))

    if not has_score:
        candidate_score_column = candidate.get("score_column")
        if not isinstance(candidate_score_column, str):
            raise ValueError(f"validate_archetype missing required columns: {config.score_column}")
        _require_columns(frame, (candidate_score_column,), "validate_archetype")
        expressions.append(
            pl.Series(
                config.score_column,
                [_float_value(row, candidate_score_column) for row in frame.iter_rows(named=True)],
            )
        )

    return frame.with_columns(expressions)


def _evaluate_conditions(
    frame: pl.DataFrame,
    conditions: Sequence[Any],
) -> list[bool]:
    parsed_conditions: list[tuple[str, ConditionOperator, float | str | bool]] = []
    for raw_condition in conditions:
        if not isinstance(raw_condition, dict):
            raise ValueError("candidate necessary_conditions must contain objects")
        column = raw_condition.get("column")
        operator = raw_condition.get("operator")
        value = raw_condition.get("value")
        if not isinstance(column, str) or not isinstance(operator, str):
            raise ValueError("candidate condition requires column and operator")
        if operator not in {">=", ">", "<=", "<", "==", "!="}:
            raise ValueError(f"Unsupported candidate condition operator: {operator}")
        _require_columns(frame, (column,), "validate_archetype")
        parsed_conditions.append((column, cast(ConditionOperator, operator), cast(Any, value)))

    signals: list[bool] = []
    for row in frame.iter_rows(named=True):
        signals.append(
            all(
                _condition_matches(row[column], operator, value)
                for column, operator, value in parsed_conditions
            )
        )
    return signals


def _condition_matches(value: Any, operator: ConditionOperator, target: float | str | bool) -> bool:
    if value is None:
        raise ValueError("candidate condition columns must be non-null")
    if operator == "==":
        return bool(value == target)
    if operator == "!=":
        return bool(value != target)
    numeric_value = float(value)
    numeric_target = float(target)
    if operator == ">=":
        return numeric_value >= numeric_target
    if operator == ">":
        return numeric_value > numeric_target
    if operator == "<=":
        return numeric_value <= numeric_target
    return numeric_value < numeric_target


def _metrics_for_rows(rows: list[dict[str, Any]], config: ValidationConfig) -> dict[str, Any]:
    sample_count = len(rows)
    if sample_count == 0:
        raise ValueError("validate_archetype requires at least one sample")

    positive_sample_count = sum(1 for row in rows if _bool_value(row, config.label_column))
    control_sample_count = sample_count - positive_sample_count
    selected_rows = [row for row in rows if _bool_value(row, config.signal_column)]
    selected_count = len(selected_rows)
    true_positive_count = sum(1 for row in selected_rows if _bool_value(row, config.label_column))
    false_positive_count = selected_count - true_positive_count

    base_rate = positive_sample_count / sample_count
    precision = true_positive_count / selected_count if selected_count else 0.0
    lift = precision / base_rate if base_rate > 0.0 else 0.0
    turnover = _turnover(rows, config)
    cost_adjusted_return = (
        _mean([_float_value(row, config.return_column) for row in selected_rows])
        - turnover * config.transaction_cost_bps / 10_000.0
    )

    return {
        "positive_sample_count": positive_sample_count,
        "control_sample_count": control_sample_count,
        "effective_sample_count": float(selected_count),
        "base_rate": _finite_float(base_rate, "base_rate"),
        "precision": _finite_float(precision, "precision"),
        "lift": _finite_float(lift, "lift"),
        "lift_over_base_rate": _finite_float(lift, "lift_over_base_rate"),
        "coverage": _finite_float(selected_count / sample_count, "coverage"),
        "false_positive_rate": _finite_float(
            false_positive_count / control_sample_count if control_sample_count else 0.0,
            "false_positive_rate",
        ),
        "precision_at_10": _finite_float(_precision_at(rows, config, 10), "precision_at_10"),
        "precision_at_50": _finite_float(_precision_at(rows, config, 50), "precision_at_50"),
        "cost_adjusted_return": _finite_float(cost_adjusted_return, "cost_adjusted_return"),
        "max_drawdown": _finite_float(
            min((_float_value(row, config.drawdown_column) for row in selected_rows), default=0.0),
            "max_drawdown",
        ),
        "turnover": _finite_float(turnover, "turnover"),
        "yearly_results": _group_results(rows, config, lambda row: str(_date_value(row, config.date_column).year)),
        "regime_results": _group_results(rows, config, lambda row: str(row[config.regime_column])),
        "top_stock_contribution": _finite_float(
            _top_share(selected_rows, config.code_column),
            "top_stock_contribution",
        ),
        "top_period_contribution": _finite_float(
            _top_share(selected_rows, config.date_column),
            "top_period_contribution",
        ),
        "mean_excess_return": _finite_float(
            _mean([_float_value(row, config.market_excess_column) for row in selected_rows]),
            "mean_excess_return",
        ),
        "median_excess_return": _finite_float(
            _median([_float_value(row, config.market_excess_column) for row in selected_rows]),
            "median_excess_return",
        ),
        "win_rate": _finite_float(
            _mean([1.0 if _float_value(row, config.return_column) > 0.0 else 0.0 for row in selected_rows]),
            "win_rate",
        ),
        "profit_factor": _finite_float(_profit_factor(selected_rows, config), "profit_factor"),
        "max_losing_streak": _max_losing_streak(selected_rows, config),
        "capacity_estimate": _finite_float(
            _median([_float_value(row, config.amount_column) for row in selected_rows]) * 0.01,
            "capacity_estimate",
        ),
        "cluster_stability": None,
        "calibration_error": _finite_float(_calibration_error(rows, config), "calibration_error"),
    }


def _group_results(
    rows: list[dict[str, Any]],
    config: ValidationConfig,
    key_fn: Any,
) -> dict[str, dict[str, float | int]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(key_fn(row)), []).append(row)

    results: dict[str, dict[str, float | int]] = {}
    for key, group_rows in sorted(grouped.items()):
        selected_rows = [row for row in group_rows if _bool_value(row, config.signal_column)]
        positives = sum(1 for row in group_rows if _bool_value(row, config.label_column))
        true_positives = sum(1 for row in selected_rows if _bool_value(row, config.label_column))
        base_rate = positives / len(group_rows)
        precision = true_positives / len(selected_rows) if selected_rows else 0.0
        lift = precision / base_rate if base_rate > 0.0 else 0.0
        results[key] = {
            "sample_count": len(group_rows),
            "selected_count": len(selected_rows),
            "base_rate": _finite_float(base_rate, "base_rate"),
            "precision": _finite_float(precision, "precision"),
            "lift": _finite_float(lift, "lift"),
            "coverage": _finite_float(len(selected_rows) / len(group_rows), "coverage"),
            "cost_adjusted_return": _finite_float(
                _mean([_float_value(row, config.return_column) for row in selected_rows]),
                "cost_adjusted_return",
            ),
        }
    return results


def _majority_windows_positive_lift(rows: list[dict[str, Any]], config: ValidationConfig) -> bool:
    by_date: dict[date, list[dict[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(_date_value(row, config.date_column), []).append(row)
    if not by_date:
        return False
    positive_lift_count = 0
    for group_rows in by_date.values():
        metrics = _group_results(group_rows, config, lambda _: "window")["window"]
        if float(metrics["lift"]) > 1.0:
            positive_lift_count += 1
    return positive_lift_count > len(by_date) / 2.0


def _precision_at(rows: list[dict[str, Any]], config: ValidationConfig, count: int) -> float:
    ordered = sorted(
        rows,
        key=lambda row: (
            -_float_value(row, config.score_column),
            _date_value(row, config.date_column),
            str(row[config.code_column]),
        ),
    )
    top_rows = ordered[: min(count, len(ordered))]
    return _mean([1.0 if _bool_value(row, config.label_column) else 0.0 for row in top_rows])


def _turnover(rows: list[dict[str, Any]], config: ValidationConfig) -> float:
    by_date: dict[date, set[str]] = {}
    for row in rows:
        trade_date = _date_value(row, config.date_column)
        by_date.setdefault(trade_date, set())
        if _bool_value(row, config.signal_column):
            by_date[trade_date].add(str(row[config.code_column]))

    turnovers: list[float] = []
    previous: set[str] | None = None
    for trade_date in sorted(by_date):
        current = by_date[trade_date]
        if previous is None:
            turnovers.append(1.0 if current else 0.0)
        else:
            union = current | previous
            turnovers.append(0.0 if not union else 1.0 - len(current & previous) / len(union))
        previous = current
    return _mean(turnovers)


def _profit_factor(selected_rows: list[dict[str, Any]], config: ValidationConfig) -> float:
    returns = [_float_value(row, config.return_column) for row in selected_rows]
    gross_profit = sum(value for value in returns if value > 0.0)
    gross_loss = abs(sum(value for value in returns if value < 0.0))
    if gross_loss == 0.0:
        return gross_profit
    return gross_profit / gross_loss


def _max_losing_streak(selected_rows: list[dict[str, Any]], config: ValidationConfig) -> int:
    streak = 0
    max_streak = 0
    ordered = sorted(
        selected_rows,
        key=lambda row: (_date_value(row, config.date_column), str(row[config.code_column])),
    )
    for row in ordered:
        if _float_value(row, config.return_column) <= 0.0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def _calibration_error(rows: list[dict[str, Any]], config: ValidationConfig) -> float:
    scores = [_float_value(row, config.score_column) for row in rows]
    if not scores:
        return 0.0
    min_score = min(scores)
    max_score = max(scores)
    denominator = max_score - min_score
    normalized_scores = [0.5 if denominator == 0.0 else (score - min_score) / denominator for score in scores]
    total_error = 0.0
    for bin_index in range(5):
        lower = bin_index / 5.0
        upper = (bin_index + 1) / 5.0
        bin_rows = [
            (row, normalized_score)
            for row, normalized_score in zip(rows, normalized_scores, strict=True)
            if lower <= normalized_score < upper or (bin_index == 4 and normalized_score == 1.0)
        ]
        if not bin_rows:
            continue
        confidence = _mean([score for _, score in bin_rows])
        accuracy = _mean([1.0 if _bool_value(row, config.label_column) else 0.0 for row, _ in bin_rows])
        total_error += len(bin_rows) / len(rows) * abs(confidence - accuracy)
    return total_error


def _top_share(rows: list[dict[str, Any]], column: str) -> float:
    if not rows:
        return 0.0
    counts: dict[Any, int] = {}
    for row in rows:
        counts[row[column]] = counts.get(row[column], 0) + 1
    return max(counts.values()) / len(rows)


def _require_columns(frame: pl.DataFrame, required_columns: tuple[str, ...], context: str) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"{context} missing required columns: {missing_csv}")


def _float_value(row: Any, column: str) -> float:
    value = cast(dict[str, Any], row)[column]
    if value is None:
        raise ValueError(f"validate_archetype requires non-null values in '{column}'")
    return float(value)


def _bool_value(row: dict[str, Any], column: str) -> bool:
    value = row[column]
    if value is None:
        raise ValueError(f"validate_archetype requires non-null values in '{column}'")
    return bool(value)


def _date_value(row: dict[str, Any], column: str) -> date:
    value = row[column]
    if not isinstance(value, date):
        raise ValueError(f"validate_archetype requires date values in '{column}'")
    return value


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.median(values))


def _finite_float(value: float, name: str) -> float:
    if not value == value or value in {float("inf"), float("-inf")}:
        raise ValueError(f"{name} must be finite")
    return float(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return _finite_float(float(value), "cluster_stability")


def _first_index_on_or_after(dates: Sequence[date], target: date) -> int | None:
    for index, candidate in enumerate(dates):
        if candidate >= target:
            return index
    return None


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)
