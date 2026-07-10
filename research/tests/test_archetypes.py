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


def _vcp_training_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for index in range(6):
        row = _base_row(f"VCP_TIGHT_{index}")
        row.update(
            {
                "return_20d": 0.04 + index * 0.001,
                "return_60d": 0.09 + index * 0.001,
                "price_vs_ma50": 0.02,
                "ma20_vs_ma50": 0.01,
                "relative_strength_20d": 1.08 + index * 0.005,
                "consolidation_range_20d": 0.08 + index * 0.002,
                "consolidation_range_60d": 0.20 + index * 0.002,
                "volume_ratio_20d": 1.35 + index * 0.02,
                "breakout_return_5d": 0.04 + index * 0.002,
            }
        )
        rows.append(row)
    for index in range(6):
        row = _base_row(f"VCP_LOOSE_{index}")
        row.update(
            {
                "return_20d": 0.05 + index * 0.001,
                "return_60d": 0.10 + index * 0.001,
                "price_vs_ma50": 0.03,
                "ma20_vs_ma50": 0.02,
                "relative_strength_20d": 1.24 + index * 0.005,
                "consolidation_range_20d": 0.13 + index * 0.002,
                "consolidation_range_60d": 0.28 + index * 0.002,
                "volume_ratio_20d": 1.65 + index * 0.02,
                "breakout_return_5d": 0.07 + index * 0.002,
            }
        )
        rows.append(row)
    rows.append(_base_row("TREND_ONLY", trend_cluster=0))
    rows.append(_base_row("UNCLASSIFIED"))
    return pl.DataFrame(rows)


def _oversold_training_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for index in range(6):
        row = _base_row(f"OVERSOLD_SHALLOW_{index}")
        row.update(
            {
                "return_20d": -0.09 - index * 0.002,
                "return_60d": -0.04,
                "relative_strength_20d": 0.92 + index * 0.004,
                "distance_from_20d_low": 0.06 - index * 0.001,
                "rsi_14": 38.0 - index * 0.4,
                "volume_ratio_20d": 1.05 + index * 0.02,
                "reversal_return_5d": 0.025 + index * 0.001,
            }
        )
        rows.append(row)
    for index in range(6):
        row = _base_row(f"OVERSOLD_DEEP_{index}")
        row.update(
            {
                "return_20d": -0.17 - index * 0.002,
                "return_60d": -0.08,
                "relative_strength_20d": 0.82 + index * 0.004,
                "distance_from_20d_low": 0.025 - index * 0.001,
                "rsi_14": 30.0 - index * 0.4,
                "volume_ratio_20d": 1.30 + index * 0.02,
                "reversal_return_5d": 0.05 + index * 0.001,
            }
        )
        rows.append(row)
    rows.append(_base_row("TREND_ONLY", trend_cluster=0))
    rows.append(_base_row("UNCLASSIFIED"))
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


def test_discover_archetypes_rejects_overlarge_cluster_counts_and_continues() -> None:
    config = ArchetypeDiscoveryConfig(
        feature_columns=["return_20d", "return_60d", "relative_strength_20d"],
        cluster_counts=[16, 2],
        min_family_samples=8,
        min_cluster_size=3,
        random_seed=13,
        stability_iterations=2,
        min_stability_score=0.0,
    )

    result = discover_archetypes(_trend_training_frame(), "trend", config)

    assert {candidate["cluster_count"] for candidate in result["archetypes"]} == {2}
    unsupported = [
        rejection for rejection in result["rejections"]
        if rejection["reason"] == "unsupported_cluster_count"
    ]
    assert {rejection["model_type"] for rejection in unsupported} == {"kmeans", "gmm"}
    assert {rejection["cluster_count"] for rejection in unsupported} == {16}


def test_discover_archetypes_rejects_degenerate_fits_before_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    class DegenerateKMeans:
        def __init__(self, **_: object) -> None:
            pass

        def fit_predict(self, scaled_matrix: object) -> list[int]:
            return [0] * len(scaled_matrix)  # type: ignore[arg-type]

    class DegenerateGmm:
        def __init__(self, **_: object) -> None:
            pass

        def fit_predict(self, scaled_matrix: object) -> list[int]:
            return [0] * len(scaled_matrix)  # type: ignore[arg-type]

        def bic(self, _: object) -> float:
            raise AssertionError("BIC must not be computed for degenerate labels")

    def fail_silhouette(*_: object) -> float:
        raise AssertionError("silhouette must not be computed for degenerate labels")

    monkeypatch.setattr("qbot_research.archetypes.KMeans", DegenerateKMeans)
    monkeypatch.setattr("qbot_research.archetypes.GaussianMixture", DegenerateGmm)
    monkeypatch.setattr("qbot_research.archetypes.silhouette_score", fail_silhouette)
    config = ArchetypeDiscoveryConfig(
        feature_columns=["return_20d", "return_60d", "relative_strength_20d"],
        cluster_counts=[2],
        min_family_samples=8,
        min_cluster_size=3,
        random_seed=13,
        stability_iterations=2,
        min_stability_score=0.0,
    )

    result = discover_archetypes(_trend_training_frame(), "trend", config)

    assert result["archetypes"] == []
    degenerate = [
        rejection for rejection in result["rejections"]
        if rejection["reason"] == "degenerate_labels"
    ]
    assert {rejection["model_type"] for rejection in degenerate} == {"kmeans", "gmm"}
    assert all(rejection["distinct_label_count"] == 1 for rejection in degenerate)


def test_discover_archetypes_requires_multiple_stability_iterations() -> None:
    config = ArchetypeDiscoveryConfig(
        feature_columns=["return_20d", "return_60d", "relative_strength_20d"],
        cluster_counts=[2],
        min_family_samples=8,
        min_cluster_size=3,
        stability_iterations=1,
    )

    with pytest.raises(ValueError, match="stability_iterations must be at least 2"):
        discover_archetypes(_trend_training_frame(), "trend", config)


def test_discover_archetypes_applies_vcp_breakout_family_gate() -> None:
    config = ArchetypeDiscoveryConfig(
        feature_columns=[
            "consolidation_range_20d",
            "consolidation_range_60d",
            "volume_ratio_20d",
            "breakout_return_5d",
            "relative_strength_20d",
        ],
        cluster_counts=[2],
        min_family_samples=8,
        min_cluster_size=3,
        random_seed=5,
        stability_iterations=2,
        min_stability_score=0.0,
    )

    result = discover_archetypes(_vcp_training_frame(), "vcp_breakout", config)

    assert result["candidate_row_count"] == 12
    assert {candidate["pattern_type"] for candidate in result["archetypes"]} == {"vcp_breakout"}
    assert result["rejections"] == []


def test_discover_archetypes_applies_oversold_reversal_family_gate() -> None:
    config = ArchetypeDiscoveryConfig(
        feature_columns=[
            "return_20d",
            "distance_from_20d_low",
            "rsi_14",
            "volume_ratio_20d",
            "reversal_return_5d",
        ],
        cluster_counts=[2],
        min_family_samples=8,
        min_cluster_size=3,
        random_seed=5,
        stability_iterations=2,
        min_stability_score=0.0,
    )

    result = discover_archetypes(_oversold_training_frame(), "oversold_reversal", config)

    assert result["candidate_row_count"] == 12
    assert {candidate["pattern_type"] for candidate in result["archetypes"]} == {
        "oversold_reversal"
    }
    assert result["rejections"] == []


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
