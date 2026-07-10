from __future__ import annotations

from datetime import date, timedelta
from typing import Any, cast

import polars as pl
import pytest

from qbot_research.labels import label_samples


def _make_week_label_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    trade_dates = [date(2026, 1, day) for day in range(1, 7)]

    aaa_close = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
    aaa_high = [10.4, 11.4, 12.4, 13.4, 14.4, 16.0]
    aaa_low = [9.8, 10.0, 11.0, 12.0, 13.0, 14.0]

    bbb_close = [20.0, 20.5, 21.0, 21.0, 21.5, 22.0]
    bbb_high = [20.4, 20.9, 21.4, 21.4, 21.9, 23.0]
    bbb_low = [19.8, 19.0, 20.0, 20.5, 21.0, 21.5]

    ccc_close = [30.0, 29.0, 28.0, 27.0, 26.0, 27.0]
    ccc_high = [30.4, 29.4, 28.4, 27.4, 26.4, 27.4]
    ccc_low = [29.8, 27.0, 26.0, 25.0, 24.0, 23.0]

    for idx, trade_date in enumerate(trade_dates):
        rows.extend(
            [
                {
                    "trade_date": trade_date,
                    "code": "AAA",
                    "adjusted_close": aaa_close[idx],
                    "adjusted_high": aaa_high[idx],
                    "adjusted_low": aaa_low[idx],
                    "amount": 1_000_000.0 + idx,
                    "listed_days": 10 if idx == 0 else 900 + idx,
                    "is_st": False,
                    "is_suspended": idx == 1,
                    "price_limit_pct": 10.0,
                    "sector_code": "TECH" if idx == 0 else "FUTURE-TECH",
                    "sector_name": "Technology",
                    "sector_type": "industry",
                    "sse_change_pct": 1.0,
                    "szse_change_pct": 1.0,
                    "chinext_change_pct": 1.0,
                    "star50_change_pct": 1.0,
                    "dataset_version": "dataset-v1",
                    "horizon": "week",
                    "year": trade_date.year,
                },
                {
                    "trade_date": trade_date,
                    "code": "BBB",
                    "adjusted_close": bbb_close[idx],
                    "adjusted_high": bbb_high[idx],
                    "adjusted_low": bbb_low[idx],
                    "amount": 900_000.0 + idx,
                    "listed_days": 500 + idx,
                    "is_st": False,
                    "is_suspended": False,
                    "price_limit_pct": 10.0,
                    "sector_code": "TECH",
                    "sector_name": "Technology",
                    "sector_type": "industry",
                    "sse_change_pct": 1.0,
                    "szse_change_pct": 1.0,
                    "chinext_change_pct": 1.0,
                    "star50_change_pct": 1.0,
                    "dataset_version": "dataset-v1",
                    "horizon": "week",
                    "year": trade_date.year,
                },
                {
                    "trade_date": trade_date,
                    "code": "CCC",
                    "adjusted_close": ccc_close[idx],
                    "adjusted_high": ccc_high[idx],
                    "adjusted_low": ccc_low[idx],
                    "amount": 800_000.0 + idx,
                    "listed_days": 400 + idx,
                    "is_st": False,
                    "is_suspended": idx == 0,
                    "price_limit_pct": 10.0,
                    "sector_code": "FIN",
                    "sector_name": "Finance",
                    "sector_type": "industry",
                    "sse_change_pct": 1.0,
                    "szse_change_pct": 1.0,
                    "chinext_change_pct": 1.0,
                    "star50_change_pct": 1.0,
                    "dataset_version": "dataset-v1",
                    "horizon": "week",
                    "year": trade_date.year,
                },
            ]
        )

    return pl.DataFrame(rows)


def _make_linear_frame(length: int) -> pl.DataFrame:
    base_date = date(2026, 1, 1)
    rows = []
    for offset in range(length):
        trade_date = base_date + timedelta(days=offset)
        price = 10.0 + float(offset)
        rows.append(
            {
                "trade_date": trade_date,
                "code": "AAA",
                "adjusted_close": price,
                "adjusted_high": price + 0.5,
                "adjusted_low": price - 0.5,
                "amount": 1_000_000.0,
                "listed_days": 300 + offset,
                "is_st": False,
                "is_suspended": False,
                "price_limit_pct": 10.0,
                "sector_code": "TECH",
                "sector_name": "Technology",
                "sector_type": "industry",
                "sse_change_pct": 0.0,
                "szse_change_pct": 0.0,
                "chinext_change_pct": 0.0,
                "star50_change_pct": 0.0,
                "dataset_version": "dataset-v1",
                "horizon": "week",
                "year": trade_date.year,
            }
        )
    return pl.DataFrame(rows)


def test_label_samples_keeps_feature_date_fields_and_adds_expected_label_columns() -> None:
    labeled = label_samples(_make_week_label_frame(), "week")

    expected_columns = {
        "future_return",
        "future_market_excess",
        "future_industry_excess",
        "future_max_drawdown",
        "future_max_favorable_excursion",
        "tradable_sample",
        "strength_score",
        "is_positive",
    }
    assert expected_columns.issubset(set(labeled.columns))

    aaa_row = labeled.filter(
        (pl.col("trade_date") == date(2026, 1, 1)) & (pl.col("code") == "AAA")
    ).row(0, named=True)
    bbb_row = labeled.filter(
        (pl.col("trade_date") == date(2026, 1, 1)) & (pl.col("code") == "BBB")
    ).row(0, named=True)
    ccc_row = labeled.filter(
        (pl.col("trade_date") == date(2026, 1, 1)) & (pl.col("code") == "CCC")
    ).row(0, named=True)

    assert aaa_row["listed_days"] == 10
    assert aaa_row["sector_code"] == "TECH"
    assert aaa_row["tradable_sample"] is True
    assert aaa_row["future_return"] == pytest.approx(0.5)
    assert aaa_row["future_market_excess"] == pytest.approx(0.4489899499000001)
    assert aaa_row["future_industry_excess"] == pytest.approx(0.2)
    assert aaa_row["future_max_drawdown"] == pytest.approx(0.0)
    assert aaa_row["future_max_favorable_excursion"] == pytest.approx(0.6)
    assert aaa_row["strength_score"] == pytest.approx(100.0)
    assert aaa_row["is_positive"] is True

    assert bbb_row["tradable_sample"] is True
    assert bbb_row["is_positive"] is False
    assert ccc_row["tradable_sample"] is False
    assert ccc_row["is_positive"] is False
    assert (
        cast(float, aaa_row["strength_score"])
        > cast(float, bbb_row["strength_score"])
        > cast(float, ccc_row["strength_score"])
    )


@pytest.mark.parametrize(
    ("horizon", "length"),
    [
        ("quarter", 61),
        ("year", 251),
    ],
)
def test_label_samples_supports_research_only_horizons_explicitly(
    horizon: str,
    length: int,
) -> None:
    labeled = label_samples(_make_linear_frame(length), cast(Any, horizon))

    first_row = labeled.row(0, named=True)
    assert first_row["future_return"] is not None
    assert first_row["is_positive"] is False


def test_label_samples_rejects_unknown_horizon() -> None:
    with pytest.raises(ValueError, match="Unsupported horizon"):
        label_samples(_make_linear_frame(6), cast(Any, "day"))
