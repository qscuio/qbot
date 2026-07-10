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


def _make_bucket_filter_candidate_frame() -> pl.DataFrame:
    trade_date = date(2026, 4, 1)

    def row(
        *,
        code: str,
        control_type: str,
        market_cap: float,
        adjusted_close: float,
        amount_20d_avg: float,
        volatility_20d: float,
        market_cap_bucket: int,
        price_bucket: int,
        amount_20d_bucket: int,
        volatility_20d_bucket: int,
        is_positive: bool = False,
    ) -> dict[str, object]:
        future_return = 0.03
        future_market_excess = 0.01
        future_max_favorable_excursion = 0.04
        if control_type == "failed_breakout":
            future_return = -0.02
            future_market_excess = 0.0
            future_max_favorable_excursion = 0.08
        elif control_type == "negative_excess":
            future_return = 0.01
            future_market_excess = -0.05
            future_max_favorable_excursion = 0.0

        return {
            "trade_date": trade_date,
            "code": code,
            "sector_code": "TECH",
            "tradable_sample": True,
            "future_return": 0.2 if is_positive else future_return,
            "future_market_excess": 0.15 if is_positive else future_market_excess,
            "future_max_favorable_excursion": (
                0.25 if is_positive else future_max_favorable_excursion
            ),
            "is_positive": is_positive,
            "market_cap": market_cap,
            "adjusted_close": adjusted_close,
            "amount_20d_avg": amount_20d_avg,
            "volatility_20d": volatility_20d,
            "market_cap_bucket": market_cap_bucket,
            "price_bucket": price_bucket,
            "amount_20d_bucket": amount_20d_bucket,
            "volatility_20d_bucket": volatility_20d_bucket,
        }

    return pl.DataFrame(
        [
            row(
                code="POS",
                control_type="ordinary",
                market_cap=100.0,
                adjusted_close=20.0,
                amount_20d_avg=1_000.0,
                volatility_20d=0.050,
                market_cap_bucket=2,
                price_bucket=3,
                amount_20d_bucket=4,
                volatility_20d_bucket=1,
                is_positive=True,
            ),
            row(
                code="ORD",
                control_type="ordinary",
                market_cap=140.0,
                adjusted_close=28.0,
                amount_20d_avg=1_400.0,
                volatility_20d=0.090,
                market_cap_bucket=2,
                price_bucket=3,
                amount_20d_bucket=4,
                volatility_20d_bucket=1,
            ),
            row(
                code="FAIL",
                control_type="failed_breakout",
                market_cap=142.0,
                adjusted_close=29.0,
                amount_20d_avg=1_420.0,
                volatility_20d=0.091,
                market_cap_bucket=2,
                price_bucket=3,
                amount_20d_bucket=4,
                volatility_20d_bucket=1,
            ),
            row(
                code="NEG",
                control_type="negative_excess",
                market_cap=144.0,
                adjusted_close=30.0,
                amount_20d_avg=1_440.0,
                volatility_20d=0.092,
                market_cap_bucket=2,
                price_bucket=3,
                amount_20d_bucket=4,
                volatility_20d_bucket=1,
            ),
            row(
                code="MCAP_DECOY",
                control_type="ordinary",
                market_cap=101.0,
                adjusted_close=28.0,
                amount_20d_avg=1_400.0,
                volatility_20d=0.090,
                market_cap_bucket=1,
                price_bucket=3,
                amount_20d_bucket=4,
                volatility_20d_bucket=1,
            ),
            row(
                code="PRICE_DECOY",
                control_type="failed_breakout",
                market_cap=142.0,
                adjusted_close=20.1,
                amount_20d_avg=1_420.0,
                volatility_20d=0.091,
                market_cap_bucket=2,
                price_bucket=2,
                amount_20d_bucket=4,
                volatility_20d_bucket=1,
            ),
            row(
                code="AMOUNT_DECOY",
                control_type="negative_excess",
                market_cap=144.0,
                adjusted_close=30.0,
                amount_20d_avg=1_000.1,
                volatility_20d=0.092,
                market_cap_bucket=2,
                price_bucket=3,
                amount_20d_bucket=3,
                volatility_20d_bucket=1,
            ),
            row(
                code="VOL_DECOY",
                control_type="ordinary",
                market_cap=140.0,
                adjusted_close=28.0,
                amount_20d_avg=1_400.0,
                volatility_20d=0.051,
                market_cap_bucket=2,
                price_bucket=3,
                amount_20d_bucket=4,
                volatility_20d_bucket=2,
            ),
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


def test_match_controls_excludes_decoys_that_miss_any_required_bucket() -> None:
    candidates = _make_bucket_filter_candidate_frame()
    samples = candidates.filter(pl.col("code") == "POS")

    matches = match_controls(samples, candidates, ControlMatchConfig(bucket_count=5))

    assert matches.select("control_code").to_series().to_list() == ["ORD", "FAIL", "NEG"]
    assert matches.select("control_type").to_series().to_list() == [
        "ordinary",
        "failed_breakout",
        "negative_excess",
    ]
    assert matches.select("market_cap_bucket").to_series().to_list() == [2, 2, 2]
    assert matches.select("price_bucket").to_series().to_list() == [3, 3, 3]
    assert matches.select("amount_20d_bucket").to_series().to_list() == [4, 4, 4]
    assert matches.select("volatility_20d_bucket").to_series().to_list() == [1, 1, 1]
    assert {
        "MCAP_DECOY",
        "PRICE_DECOY",
        "AMOUNT_DECOY",
        "VOL_DECOY",
    }.isdisjoint(set(matches.select("control_code").to_series().to_list()))


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
