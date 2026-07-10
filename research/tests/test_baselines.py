from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from qbot_research.baselines import (
    BASELINE_NAMES,
    apply_baseline,
    evaluate_baselines,
)
from qbot_research.validation import ValidationConfig


def _baseline_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    trade_dates = [date(2026, 1, 2), date(2026, 1, 3)]
    for trade_date in trade_dates:
        for index in range(20):
            code = f"S{index:02d}"
            strong = index >= 18
            rows.append(
                {
                    "trade_date": trade_date,
                    "code": code,
                    "return_20d": 0.01 * index,
                    "return_60d": 0.02 * index,
                    "market_return_20d": 0.05,
                    "market_return_60d": 0.10,
                    "close": 12.0 + index,
                    "ma20": 11.0 + index,
                    "ma60": 10.0 + index,
                    "ma20_prev": 10.5 + index,
                    "volatility_5d": 0.020 if strong else 0.060,
                    "volatility_20d": 0.040 if strong else 0.050,
                    "volatility_60d": 0.080,
                    "prior_20d_high": 11.5 + index,
                    "scan_ranker_pool_id": "pool_mid_a" if strong else "pool_mid_b",
                    "scan_ranker_score": 88.0 if strong else 61.0,
                    "is_positive": strong,
                    "future_return": 0.050 if strong else -0.010,
                    "future_market_excess": 0.040 if strong else -0.020,
                    "future_max_drawdown": -0.020 if strong else -0.080,
                    "amount": 100_000_000.0 + index,
                    "regime": "bull",
                }
            )
    return pl.DataFrame(rows)


def test_relative_strength_20_60_selects_cross_sectional_top_decile() -> None:
    baseline = apply_baseline(_baseline_frame(), "relative_strength_20_60")
    selected = baseline.filter(pl.col("baseline_signal")).sort(["trade_date", "code"])

    assert selected.select("code").to_series().to_list() == ["S18", "S19", "S18", "S19"]
    assert selected.select("baseline_name").to_series().to_list() == [
        "relative_strength_20_60",
        "relative_strength_20_60",
        "relative_strength_20_60",
        "relative_strength_20_60",
    ]


def test_ma20_ma60_trend_requires_price_stack_and_rising_ma20() -> None:
    frame = pl.DataFrame(
        [
            {
                "trade_date": date(2026, 1, 2),
                "code": "PASS",
                "close": 12.0,
                "ma20": 11.0,
                "ma60": 10.0,
                "ma20_prev": 10.5,
            },
            {
                "trade_date": date(2026, 1, 2),
                "code": "FAIL",
                "close": 12.0,
                "ma20": 10.0,
                "ma60": 11.0,
                "ma20_prev": 10.5,
            },
        ]
    )

    rows = {
        str(row["code"]): row
        for row in apply_baseline(frame, "ma20_ma60_trend").iter_rows(named=True)
    }
    assert rows["PASS"]["baseline_signal"] is True
    assert rows["FAIL"]["baseline_signal"] is False


def test_volatility_contraction_breakout_requires_three_volatility_windows_and_breakout() -> None:
    frame = pl.DataFrame(
        [
            {
                "trade_date": date(2026, 1, 2),
                "code": "PASS",
                "close": 12.0,
                "volatility_5d": 0.020,
                "volatility_20d": 0.040,
                "volatility_60d": 0.080,
                "prior_20d_high": 11.5,
            },
            {
                "trade_date": date(2026, 1, 2),
                "code": "FAIL",
                "close": 12.0,
                "volatility_5d": 0.050,
                "volatility_20d": 0.040,
                "volatility_60d": 0.080,
                "prior_20d_high": 11.5,
            },
        ]
    )

    rows = {
        str(row["code"]): row
        for row in apply_baseline(frame, "volatility_contraction_breakout").iter_rows(named=True)
    }
    assert rows["PASS"]["baseline_signal"] is True
    assert rows["FAIL"]["baseline_signal"] is False


def test_scan_ranker_a_uses_passed_pool_columns_without_calling_rust() -> None:
    baseline = apply_baseline(_baseline_frame(), "scan_ranker_a")
    selected = baseline.filter(pl.col("baseline_signal")).sort(["trade_date", "code"])

    assert selected.select("code").to_series().to_list() == ["S18", "S19", "S18", "S19"]
    assert selected.select("baseline_score").to_series().to_list() == [88.0, 88.0, 88.0, 88.0]


def test_baseline_metrics_share_validation_result_schema() -> None:
    results = evaluate_baselines(
        _baseline_frame(),
        ValidationConfig(
            signal_column="baseline_signal",
            score_column="baseline_score",
            best_required_baseline_return=0.0,
            max_single_stock_contribution=0.60,
            max_single_period_contribution=0.60,
        ),
    )

    assert set(results) == set(BASELINE_NAMES)
    for baseline_name, result in results.items():
        assert result["candidate_id"] == baseline_name
        assert "precision" in result
        assert "yearly_results" in result
        assert "regime_results" in result
        assert "release_gate_passed" in result


def test_baselines_reject_missing_required_columns() -> None:
    with pytest.raises(ValueError, match="scan_ranker_a missing required columns.*scan_ranker_score"):
        apply_baseline(_baseline_frame().drop("scan_ranker_score"), "scan_ranker_a")
