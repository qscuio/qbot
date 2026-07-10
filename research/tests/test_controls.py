from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from qbot_research.controls import ControlMatchConfig, match_controls


def _make_candidate_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    trade_dates = [date(2026, 3, 1), date(2026, 3, 2), date(2026, 3, 3)]

    close_history = {
        "POS": [19.0, 19.5, 20.0],
        "ORD": [19.0, 19.5, 20.0],
        "FAIL": [19.0, 19.5, 20.0],
        "NEG": [19.0, 19.5, 20.0],
        "OFFSECTOR": [19.0, 19.5, 20.0],
        "OFFDATE": [19.0, 19.5, 20.0],
    }

    for code, closes in close_history.items():
        for idx, trade_date in enumerate(trade_dates):
            if code == "OFFDATE" and trade_date == date(2026, 3, 3):
                continue
            rows.append(
                {
                    "trade_date": trade_date,
                    "code": code,
                    "adjusted_close": closes[idx],
                    "market_cap": 5_000_000_000.0,
                    "amount": 1_000_000.0,
                    "turnover": 0.02,
                    "sector_code": "FIN" if code == "OFFSECTOR" else "TECH",
                    "is_st": False,
                    "is_suspended": False,
                    "tradable_sample": True,
                    "future_return": 0.0,
                    "future_market_excess": 0.0,
                    "future_max_favorable_excursion": 0.0,
                    "is_positive": False,
                }
            )

    frame = pl.DataFrame(rows)
    return frame.with_columns(
        [
            pl.when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "POS"))
            .then(pl.lit(0.2))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "ORD"))
            .then(pl.lit(0.03))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "FAIL"))
            .then(pl.lit(-0.02))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "NEG"))
            .then(pl.lit(-0.05))
            .otherwise(pl.col("future_return"))
            .alias("future_return"),
            pl.when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "POS"))
            .then(pl.lit(0.15))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "ORD"))
            .then(pl.lit(0.01))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "FAIL"))
            .then(pl.lit(0.0))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "NEG"))
            .then(pl.lit(-0.07))
            .otherwise(pl.col("future_market_excess"))
            .alias("future_market_excess"),
            pl.when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "POS"))
            .then(pl.lit(0.25))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "ORD"))
            .then(pl.lit(0.04))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "FAIL"))
            .then(pl.lit(0.08))
            .when((pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "NEG"))
            .then(pl.lit(0.0))
            .otherwise(pl.col("future_max_favorable_excursion"))
            .alias("future_max_favorable_excursion"),
            (
                (pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "POS")
            ).alias("is_positive"),
        ]
    )


def test_match_controls_returns_deterministic_types_without_matching_self() -> None:
    candidates = _make_candidate_frame()
    samples = candidates.filter(
        (pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "POS")
    )

    matches = match_controls(samples, candidates, ControlMatchConfig(bucket_count=3))

    assert matches.select("sample_code").to_series().to_list() == ["POS", "POS", "POS"]
    assert matches.select("control_code").to_series().to_list() == ["ORD", "FAIL", "NEG"]
    assert matches.select("control_type").to_series().to_list() == [
        "ordinary",
        "failed_breakout",
        "negative_excess",
    ]
    assert "POS" not in matches.select("control_code").to_series().to_list()
    assert matches.select("sample_trade_date").to_series().to_list() == [
        date(2026, 3, 3),
        date(2026, 3, 3),
        date(2026, 3, 3),
    ]
    assert matches.select("control_trade_date").to_series().to_list() == [
        date(2026, 3, 3),
        date(2026, 3, 3),
        date(2026, 3, 3),
    ]
    assert matches.select("sector_code").to_series().to_list() == ["TECH", "TECH", "TECH"]
    assert matches.select("tradable_sample").to_series().to_list() == [True, True, True]


def test_match_controls_accepts_precomputed_market_cap_bucket_without_market_cap_metric() -> None:
    candidates = _make_candidate_frame().drop("market_cap", "turnover").with_columns(
        pl.lit(0).alias("market_cap_bucket")
    )
    samples = candidates.filter(
        (pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "POS")
    )

    matches = match_controls(samples, candidates, ControlMatchConfig(bucket_count=3))

    assert matches.select("control_code").to_series().to_list() == ["ORD", "FAIL", "NEG"]
    assert matches.select("market_cap_bucket").to_series().to_list() == [0, 0, 0]


def test_match_controls_requires_real_market_cap_or_precomputed_bucket() -> None:
    candidates = _make_candidate_frame().drop("market_cap")
    samples = candidates.filter(
        (pl.col("trade_date") == date(2026, 3, 3)) & (pl.col("code") == "POS")
    )

    with pytest.raises(ValueError, match="market cap"):
        match_controls(samples, candidates, ControlMatchConfig())
