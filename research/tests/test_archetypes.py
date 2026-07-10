from __future__ import annotations

import json
import math

import polars as pl
import pytest

from qbot_research.archetypes import (
    ArchetypeDiscoveryConfig,
    discover_archetypes,
    oversold_reversal_family,
    trend_family,
    vcp_breakout_family,
)


def _base_row(code: str, *, trend_cluster: int | None = None) -> dict[str, object]:
    if trend_cluster == 0:
        return {
            "code": code,
            "return_20d": 0.12,
            "return_60d": 0.24,
            "price_vs_ma50": 0.08,
            "ma20_vs_ma50": 0.04,
            "relative_strength_20d": 1.20,
            "volatility_20d": 0.045,
            "consolidation_range_20d": 0.24,
            "consolidation_range_60d": 0.28,
            "volume_ratio_20d": 1.05,
            "breakout_return_5d": 0.01,
            "distance_from_20d_low": 0.25,
            "rsi_14": 61.0,
            "reversal_return_5d": 0.0,
        }
    if trend_cluster == 1:
        return {
            "code": code,
            "return_20d": 0.25,
            "return_60d": 0.48,
            "price_vs_ma50": 0.18,
            "ma20_vs_ma50": 0.10,
            "relative_strength_20d": 1.55,
            "volatility_20d": 0.095,
            "consolidation_range_20d": 0.26,
            "consolidation_range_60d": 0.30,
            "volume_ratio_20d": 1.10,
            "breakout_return_5d": 0.02,
            "distance_from_20d_low": 0.35,
            "rsi_14": 72.0,
            "reversal_return_5d": 0.01,
        }
    return {
        "code": code,
        "return_20d": 0.01,
        "return_60d": 0.02,
        "price_vs_ma50": -0.01,
        "ma20_vs_ma50": -0.02,
        "relative_strength_20d": 0.95,
        "volatility_20d": 0.030,
        "consolidation_range_20d": 0.30,
        "consolidation_range_60d": 0.31,
        "volume_ratio_20d": 0.90,
        "breakout_return_5d": 0.00,
        "distance_from_20d_low": 0.20,
        "rsi_14": 50.0,
        "reversal_return_5d": 0.00,
    }


def _trend_training_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for index in range(8):
        row = _base_row(f"LOW_{index}", trend_cluster=0)
        row["return_20d"] = 0.10 + index * 0.004
        row["relative_strength_20d"] = 1.15 + index * 0.01
        rows.append(row)
    for index in range(8):
        row = _base_row(f"HIGH_{index}", trend_cluster=1)
        row["return_20d"] = 0.23 + index * 0.005
        row["relative_strength_20d"] = 1.50 + index * 0.01
        rows.append(row)
    rows.append(_base_row("UNCLASSIFIED"))
    return pl.DataFrame(rows)


def _unstable_trend_training_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for index in range(30):
        angle = 2.0 * math.pi * float(index) / 30.0
        row = _base_row(f"RING_{index}", trend_cluster=0)
        row["return_20d"] = 0.20 + 0.03 * math.cos(angle)
        row["return_60d"] = 0.30 + 0.03 * math.sin(angle)
        row["relative_strength_20d"] = 1.30 + 0.02 * math.cos(angle * 2.0)
        rows.append(row)
    return pl.DataFrame(rows)


def test_family_masks_leave_non_matching_samples_unclassified() -> None:
    frame = pl.DataFrame(
        [
            _base_row("TREND", trend_cluster=0),
            {
                **_base_row("VCP"),
                "return_20d": 0.08,
                "return_60d": 0.16,
                "price_vs_ma50": 0.03,
                "ma20_vs_ma50": 0.01,
                "relative_strength_20d": 1.18,
                "consolidation_range_20d": 0.10,
                "consolidation_range_60d": 0.22,
                "volume_ratio_20d": 1.55,
                "breakout_return_5d": 0.06,
            },
            {
                **_base_row("OVERSOLD"),
                "return_20d": -0.14,
                "return_60d": -0.05,
                "distance_from_20d_low": 0.03,
                "rsi_14": 31.0,
                "volume_ratio_20d": 1.25,
                "reversal_return_5d": 0.05,
            },
            _base_row("UNCLASSIFIED"),
        ]
    )

    classified = frame.select(
        [
            pl.col("code"),
            trend_family(frame).alias("trend"),
            vcp_breakout_family(frame).alias("vcp_breakout"),
            oversold_reversal_family(frame).alias("oversold_reversal"),
        ]
    ).with_columns(
        (pl.col("trend") | pl.col("vcp_breakout") | pl.col("oversold_reversal")).alias(
            "any_family"
        )
    )

    rows = {str(row["code"]): row for row in classified.iter_rows(named=True)}
    assert rows["TREND"]["trend"] is True
    assert rows["VCP"]["vcp_breakout"] is True
    assert rows["OVERSOLD"]["oversold_reversal"] is True
    assert rows["UNCLASSIFIED"]["any_family"] is False


def test_discover_archetypes_compares_kmeans_and_gmm_with_serializable_payloads() -> None:
    config = ArchetypeDiscoveryConfig(
        feature_columns=[
            "return_20d",
            "return_60d",
            "price_vs_ma50",
            "ma20_vs_ma50",
            "relative_strength_20d",
            "volatility_20d",
        ],
        cluster_counts=[2],
        min_family_samples=8,
        min_cluster_size=3,
        random_seed=13,
        stability_iterations=2,
        min_stability_score=0.70,
    )

    first = discover_archetypes(_trend_training_frame(), "trend", config)
    second = discover_archetypes(_trend_training_frame(), "trend", config)

    assert first == second
    assert first["pattern_type"] == "trend"
    assert first["candidate_row_count"] == 16
    assert {candidate["model_type"] for candidate in first["archetypes"]} == {"kmeans", "gmm"}
    assert first["rejections"] == []
    json.dumps(first)

    for candidate in first["archetypes"]:
        assert candidate["random_seed"] == 13
        assert candidate["sample_count"] >= 3
        assert candidate["scaler"]["mean"]
        assert candidate["scaler"]["scale"]
        assert candidate["high_contribution_features"]
        if candidate["model_type"] == "kmeans":
            assert candidate["centroid"]
            assert candidate["silhouette"] > 0.0
        else:
            assert candidate["mixture_mean"]
            assert candidate["mixture_covariance"]
            assert candidate["mixture_weight"] > 0.0
            assert candidate["bic"] < 0.0


def test_discover_archetypes_rejects_tiny_clusters_explicitly() -> None:
    config = ArchetypeDiscoveryConfig(
        feature_columns=["return_20d", "return_60d", "relative_strength_20d"],
        cluster_counts=[2],
        min_family_samples=3,
        min_cluster_size=9,
        random_seed=7,
        stability_iterations=2,
        min_stability_score=0.0,
    )

    result = discover_archetypes(_trend_training_frame(), "trend", config)

    assert result["archetypes"] == []
    assert {rejection["reason"] for rejection in result["rejections"]} == {"tiny_cluster"}


def test_discover_archetypes_rejects_unstable_model_explicitly() -> None:
    config = ArchetypeDiscoveryConfig(
        feature_columns=["return_20d", "return_60d", "relative_strength_20d"],
        cluster_counts=[2],
        min_family_samples=3,
        min_cluster_size=3,
        random_seed=7,
        stability_iterations=3,
        min_stability_score=0.95,
    )

    result = discover_archetypes(_unstable_trend_training_frame(), "trend", config)

    assert result["archetypes"] == []
    assert {rejection["reason"] for rejection in result["rejections"]} == {"unstable_model"}


def test_discover_archetypes_fails_clearly_for_missing_required_columns() -> None:
    frame = _trend_training_frame().drop("relative_strength_20d")
    config = ArchetypeDiscoveryConfig(
        feature_columns=["return_20d", "relative_strength_20d"],
        cluster_counts=[2],
    )

    with pytest.raises(ValueError, match="missing required columns.*relative_strength_20d"):
        discover_archetypes(frame, "trend", config)


def test_discover_archetypes_rejects_unknown_pattern_type() -> None:
    with pytest.raises(ValueError, match="Unsupported pattern_type"):
        discover_archetypes(
            _trend_training_frame(),
            "earnings_gap",  # type: ignore[arg-type]
            ArchetypeDiscoveryConfig(feature_columns=["return_20d"], cluster_counts=[2]),
        )
