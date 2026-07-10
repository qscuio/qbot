from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import polars as pl
import pytest

from qbot_research.validation import (
    ValidationConfig,
    purged_walk_forward_splits,
    validate_archetype,
)


def _trading_dates(count: int) -> list[date]:
    start = date(2026, 1, 2)
    return [start + timedelta(days=offset) for offset in range(count)]


def _validation_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    trade_dates = [date(2026, 1, 2), date(2026, 1, 3), date(2026, 2, 2), date(2026, 2, 3)]
    for trade_date in trade_dates:
        for code, signal, score, is_positive, future_return in [
            ("AAA", True, 0.95, True, 0.050),
            ("BBB", True, 0.90, True, 0.030),
            ("CCC", False, 0.20, False, -0.020),
            ("DDD", False, 0.10, False, -0.010),
        ]:
            rows.append(
                {
                    "trade_date": trade_date,
                    "code": code,
                    "candidate_signal": signal,
                    "candidate_score": score,
                    "is_positive": is_positive,
                    "future_return": future_return,
                    "future_market_excess": future_return - 0.005,
                    "future_max_drawdown": -0.040 if signal else -0.070,
                    "amount": 100_000_000.0,
                    "regime": "bull" if trade_date.month == 1 else "bear",
                }
            )
    return pl.DataFrame(rows)


def _validation_config() -> ValidationConfig:
    return ValidationConfig(
        signal_column="candidate_signal",
        score_column="candidate_score",
        best_required_baseline_return=0.010,
        max_single_stock_contribution=0.60,
        max_single_period_contribution=0.60,
        transaction_cost_bps=10.0,
    )


def test_purged_walk_forward_splits_remove_overlapping_labels_and_add_embargo() -> None:
    dates = _trading_dates(150)
    splits = purged_walk_forward_splits(
        dates,
        train_months=2,
        validation_months=1,
        step_months=1,
        horizon_days=20,
    )

    assert splits
    first_split = splits[0]
    date_positions = {trade_date: index for index, trade_date in enumerate(dates)}
    validation_start_position = date_positions[first_split.validation_start]

    assert first_split.train_dates
    assert first_split.validation_dates
    assert first_split.purge_dates
    assert first_split.embargo_dates
    assert len(first_split.embargo_dates) >= 20
    assert first_split.embargo_dates[0] > first_split.validation_end

    for train_date in first_split.train_dates:
        assert date_positions[train_date] + 20 < validation_start_position


def test_validate_archetype_returns_full_metric_payload_and_validates_release_gate() -> None:
    result = validate_archetype(
        _validation_frame(),
        {"archetype_id": "trend:kmeans:k2:c0", "stability_score": 0.82},
        _validation_config(),
    )

    required_metrics = {
        "base_rate",
        "precision",
        "lift",
        "coverage",
        "false_positive_rate",
        "precision_at_10",
        "precision_at_50",
        "cost_adjusted_return",
        "max_drawdown",
        "turnover",
        "yearly_results",
        "regime_results",
        "top_stock_contribution",
        "top_period_contribution",
        "positive_sample_count",
        "control_sample_count",
        "effective_sample_count",
        "lift_over_base_rate",
        "mean_excess_return",
        "median_excess_return",
        "win_rate",
        "profit_factor",
        "max_losing_streak",
        "capacity_estimate",
        "cluster_stability",
        "calibration_error",
    }
    assert required_metrics.issubset(result.keys())
    assert result["base_rate"] == pytest.approx(0.5)
    assert result["precision"] == pytest.approx(1.0)
    assert result["lift"] == pytest.approx(2.0)
    assert result["lift_over_base_rate"] == pytest.approx(2.0)
    assert result["coverage"] == pytest.approx(0.5)
    assert result["majority_windows_positive_lift"] is True
    assert result["release_gate_passed"] is True
    assert result["candidate_status"] == "validated"
    assert result["cluster_stability"] == pytest.approx(0.82)
    json.dumps(result)


def test_validate_archetype_keeps_concentrated_candidate_in_draft_status() -> None:
    frame = _validation_frame().with_columns(
        (pl.col("code") == "AAA").alias("candidate_signal"),
        pl.when(pl.col("code") == "AAA")
        .then(pl.lit(0.99))
        .otherwise(pl.lit(0.10))
        .alias("candidate_score"),
    )
    config = ValidationConfig(
        signal_column="candidate_signal",
        score_column="candidate_score",
        best_required_baseline_return=0.010,
        max_single_stock_contribution=0.40,
        max_single_period_contribution=0.60,
    )

    result = validate_archetype(frame, {"archetype_id": "too-concentrated"}, config)

    assert result["precision"] == pytest.approx(1.0)
    assert result["top_stock_contribution"] == pytest.approx(1.0)
    assert result["release_gate_passed"] is False
    assert result["candidate_status"] == "draft"


def test_validate_archetype_rejects_missing_required_columns() -> None:
    with pytest.raises(ValueError, match="validate_archetype missing required columns.*future_return"):
        validate_archetype(
            _validation_frame().drop("future_return"),
            {"archetype_id": "missing-return"},
            _validation_config(),
        )


def test_validate_archetype_can_score_serializable_candidate_payload_conditions() -> None:
    frame = _validation_frame().with_columns(pl.lit(1.5).alias("relative_strength_20d"))
    candidate: dict[str, Any] = {
        "archetype_id": "condition-only",
        "score_column": "relative_strength_20d",
        "necessary_conditions": [
            {"column": "relative_strength_20d", "operator": ">=", "value": 1.0}
        ],
    }
    result = validate_archetype(
        frame.drop("candidate_signal").drop("candidate_score"),
        candidate,
        ValidationConfig(
            best_required_baseline_return=-1.0,
            max_single_stock_contribution=1.0,
            max_single_period_contribution=1.0,
        ),
    )

    assert result["candidate_id"] == "condition-only"
    assert result["coverage"] == pytest.approx(1.0)
