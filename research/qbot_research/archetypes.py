from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Final, Literal, Sequence, SupportsFloat, cast

import numpy as np
import numpy.typing as npt
import polars as pl
from sklearn.cluster import KMeans  # type: ignore[import-untyped]
from sklearn.metrics import adjusted_rand_score, silhouette_score  # type: ignore[import-untyped]
from sklearn.mixture import GaussianMixture  # type: ignore[import-untyped]

from qbot_research.contracts import PatternType

ModelType = Literal["kmeans", "gmm"]
FloatMatrix = npt.NDArray[np.float64]
IntVector = npt.NDArray[np.int_]

TREND_COLUMNS: Final[tuple[str, ...]] = (
    "return_20d",
    "return_60d",
    "price_vs_ma50",
    "ma20_vs_ma50",
    "relative_strength_20d",
)
VCP_BREAKOUT_COLUMNS: Final[tuple[str, ...]] = (
    "consolidation_range_20d",
    "consolidation_range_60d",
    "volume_ratio_20d",
    "breakout_return_5d",
    "relative_strength_20d",
)
OVERSOLD_REVERSAL_COLUMNS: Final[tuple[str, ...]] = (
    "return_20d",
    "distance_from_20d_low",
    "rsi_14",
    "volume_ratio_20d",
    "reversal_return_5d",
)


@dataclass(frozen=True)
class ArchetypeDiscoveryConfig:
    feature_columns: Sequence[str]
    cluster_counts: Sequence[int] = (2, 3)
    min_family_samples: int = 30
    min_cluster_size: int = 10
    random_seed: int = 0
    high_contribution_feature_count: int = 5
    stability_iterations: int = 3
    min_stability_score: float = 0.75
    covariance_regularization: float = 1e-6


def trend_family(frame: pl.DataFrame) -> pl.Expr:
    _require_columns(frame, TREND_COLUMNS, "trend_family")
    return (
        (pl.col("return_20d") >= 0.10)
        & (pl.col("return_60d") >= 0.18)
        & (pl.col("price_vs_ma50") > 0.0)
        & (pl.col("ma20_vs_ma50") > 0.0)
        & (pl.col("relative_strength_20d") >= 1.10)
    )


def vcp_breakout_family(frame: pl.DataFrame) -> pl.Expr:
    _require_columns(frame, VCP_BREAKOUT_COLUMNS, "vcp_breakout_family")
    return (
        (pl.col("consolidation_range_20d") <= 0.18)
        & (pl.col("consolidation_range_20d") <= pl.col("consolidation_range_60d") * 0.70)
        & (pl.col("volume_ratio_20d") >= 1.20)
        & (pl.col("breakout_return_5d") >= 0.03)
        & (pl.col("relative_strength_20d") >= 1.05)
    )


def oversold_reversal_family(frame: pl.DataFrame) -> pl.Expr:
    _require_columns(frame, OVERSOLD_REVERSAL_COLUMNS, "oversold_reversal_family")
    return (
        (pl.col("return_20d") <= -0.08)
        & (pl.col("distance_from_20d_low") <= 0.08)
        & (pl.col("rsi_14") <= 40.0)
        & (pl.col("volume_ratio_20d") >= 1.0)
        & (pl.col("reversal_return_5d") >= 0.02)
    )


FAMILY_MASKS: Final[dict[PatternType, Callable[[pl.DataFrame], pl.Expr]]] = {
    "trend": trend_family,
    "vcp_breakout": vcp_breakout_family,
    "oversold_reversal": oversold_reversal_family,
}


def discover_archetypes(
    train_frame: pl.DataFrame,
    pattern_type: PatternType,
    config: ArchetypeDiscoveryConfig,
) -> dict[str, Any]:
    _validate_config(config)
    if pattern_type not in FAMILY_MASKS:
        raise ValueError(f"Unsupported pattern_type: {pattern_type}")

    family_mask = FAMILY_MASKS[pattern_type](train_frame)
    _require_columns(train_frame, tuple(config.feature_columns), "discover_archetypes")
    candidates = train_frame.filter(family_mask)

    result: dict[str, Any] = {
        "pattern_type": pattern_type,
        "random_seed": int(config.random_seed),
        "feature_columns": list(config.feature_columns),
        "candidate_row_count": int(candidates.height),
        "archetypes": [],
        "rejections": [],
    }

    if candidates.height < config.min_family_samples:
        result["rejections"].append(
            {
                "pattern_type": pattern_type,
                "reason": "insufficient_family_samples",
                "candidate_row_count": int(candidates.height),
                "min_family_samples": int(config.min_family_samples),
            }
        )
        return result

    matrix = _feature_matrix(candidates, config.feature_columns)
    scaled_matrix, scaler = _scale_matrix(matrix, config.feature_columns)

    for cluster_count in config.cluster_counts:
        if cluster_count >= candidates.height:
            raise ValueError(
                "cluster_counts must be smaller than the candidate family row count "
                "for silhouette scoring"
            )
        _fit_kmeans(
            pattern_type=pattern_type,
            scaled_matrix=scaled_matrix,
            scaler=scaler,
            cluster_count=cluster_count,
            config=config,
            result=result,
        )
        _fit_gmm(
            pattern_type=pattern_type,
            scaled_matrix=scaled_matrix,
            scaler=scaler,
            cluster_count=cluster_count,
            config=config,
            result=result,
        )

    result["archetypes"] = sorted(
        cast(list[dict[str, Any]], result["archetypes"]),
        key=lambda candidate: (
            str(candidate["model_type"]),
            int(candidate["cluster_count"]),
            int(candidate["cluster_id"]),
        ),
    )
    result["rejections"] = sorted(
        cast(list[dict[str, Any]], result["rejections"]),
        key=lambda rejection: (
            str(rejection.get("model_type", "")),
            int(rejection.get("cluster_count", -1)),
            int(rejection.get("cluster_id", -1)),
            str(rejection["reason"]),
        ),
    )
    return result


def _fit_kmeans(
    *,
    pattern_type: PatternType,
    scaled_matrix: FloatMatrix,
    scaler: dict[str, dict[str, float]],
    cluster_count: int,
    config: ArchetypeDiscoveryConfig,
    result: dict[str, Any],
) -> None:
    model = KMeans(n_clusters=cluster_count, random_state=config.random_seed, n_init=10)
    labels = cast(IntVector, model.fit_predict(scaled_matrix))
    silhouette = _silhouette(scaled_matrix, labels)
    stability_score = _kmeans_stability(scaled_matrix, labels, cluster_count, config)
    if _reject_unstable(
        pattern_type=pattern_type,
        model_type="kmeans",
        cluster_count=cluster_count,
        stability_score=stability_score,
        config=config,
        result=result,
    ):
        return

    centers_scaled = cast(FloatMatrix, model.cluster_centers_)
    centers = _inverse_scale(centers_scaled, scaler, config.feature_columns)
    for cluster_id in range(cluster_count):
        sample_count = int(np.count_nonzero(labels == cluster_id))
        if _reject_tiny_cluster(
            pattern_type=pattern_type,
            model_type="kmeans",
            cluster_count=cluster_count,
            cluster_id=cluster_id,
            sample_count=sample_count,
            config=config,
            result=result,
        ):
            continue
        center_scaled = centers_scaled[cluster_id]
        cast(list[dict[str, Any]], result["archetypes"]).append(
            {
                "archetype_id": f"{pattern_type}:kmeans:k{cluster_count}:c{cluster_id}",
                "pattern_type": pattern_type,
                "model_type": "kmeans",
                "cluster_count": int(cluster_count),
                "cluster_id": int(cluster_id),
                "sample_count": sample_count,
                "random_seed": int(config.random_seed),
                "scaler": scaler,
                "centroid": _feature_dict(centers[cluster_id], config.feature_columns),
                "silhouette": silhouette,
                "bic": None,
                "stability_score": stability_score,
                "high_contribution_features": _high_contribution_features(
                    center_scaled,
                    config.feature_columns,
                    config.high_contribution_feature_count,
                ),
            }
        )


def _fit_gmm(
    *,
    pattern_type: PatternType,
    scaled_matrix: FloatMatrix,
    scaler: dict[str, dict[str, float]],
    cluster_count: int,
    config: ArchetypeDiscoveryConfig,
    result: dict[str, Any],
) -> None:
    model = GaussianMixture(
        n_components=cluster_count,
        random_state=config.random_seed,
        covariance_type="full",
        reg_covar=config.covariance_regularization,
    )
    labels = cast(IntVector, model.fit_predict(scaled_matrix))
    silhouette = _silhouette(scaled_matrix, labels)
    bic = _finite_float(model.bic(scaled_matrix), "bic")
    stability_score = _gmm_stability(scaled_matrix, labels, cluster_count, config)
    if _reject_unstable(
        pattern_type=pattern_type,
        model_type="gmm",
        cluster_count=cluster_count,
        stability_score=stability_score,
        config=config,
        result=result,
    ):
        return

    means_scaled = cast(FloatMatrix, model.means_)
    means = _inverse_scale(means_scaled, scaler, config.feature_columns)
    weights = cast(FloatMatrix, model.weights_)
    covariances = cast(npt.NDArray[np.float64], model.covariances_)
    for cluster_id in range(cluster_count):
        sample_count = int(np.count_nonzero(labels == cluster_id))
        if _reject_tiny_cluster(
            pattern_type=pattern_type,
            model_type="gmm",
            cluster_count=cluster_count,
            cluster_id=cluster_id,
            sample_count=sample_count,
            config=config,
            result=result,
        ):
            continue
        mean_scaled = means_scaled[cluster_id]
        cast(list[dict[str, Any]], result["archetypes"]).append(
            {
                "archetype_id": f"{pattern_type}:gmm:k{cluster_count}:c{cluster_id}",
                "pattern_type": pattern_type,
                "model_type": "gmm",
                "cluster_count": int(cluster_count),
                "cluster_id": int(cluster_id),
                "sample_count": sample_count,
                "random_seed": int(config.random_seed),
                "scaler": scaler,
                "mixture_mean": _feature_dict(means[cluster_id], config.feature_columns),
                "mixture_covariance": _matrix_payload(covariances[cluster_id]),
                "mixture_weight": _finite_float(weights[cluster_id], "mixture_weight"),
                "silhouette": silhouette,
                "bic": bic,
                "stability_score": stability_score,
                "high_contribution_features": _high_contribution_features(
                    mean_scaled,
                    config.feature_columns,
                    config.high_contribution_feature_count,
                ),
            }
        )


def _reject_unstable(
    *,
    pattern_type: PatternType,
    model_type: ModelType,
    cluster_count: int,
    stability_score: float,
    config: ArchetypeDiscoveryConfig,
    result: dict[str, Any],
) -> bool:
    if stability_score >= config.min_stability_score:
        return False
    cast(list[dict[str, Any]], result["rejections"]).append(
        {
            "pattern_type": pattern_type,
            "model_type": model_type,
            "cluster_count": int(cluster_count),
            "reason": "unstable_model",
            "stability_score": stability_score,
            "min_stability_score": _finite_float(config.min_stability_score, "min_stability_score"),
        }
    )
    return True


def _reject_tiny_cluster(
    *,
    pattern_type: PatternType,
    model_type: ModelType,
    cluster_count: int,
    cluster_id: int,
    sample_count: int,
    config: ArchetypeDiscoveryConfig,
    result: dict[str, Any],
) -> bool:
    if sample_count >= config.min_cluster_size:
        return False
    cast(list[dict[str, Any]], result["rejections"]).append(
        {
            "pattern_type": pattern_type,
            "model_type": model_type,
            "cluster_count": int(cluster_count),
            "cluster_id": int(cluster_id),
            "reason": "tiny_cluster",
            "sample_count": int(sample_count),
            "min_cluster_size": int(config.min_cluster_size),
        }
    )
    return True


def _kmeans_stability(
    scaled_matrix: FloatMatrix,
    primary_labels: IntVector,
    cluster_count: int,
    config: ArchetypeDiscoveryConfig,
) -> float:
    scores: list[float] = []
    for offset in range(1, config.stability_iterations):
        model = KMeans(n_clusters=cluster_count, random_state=config.random_seed + offset, n_init=10)
        labels = cast(IntVector, model.fit_predict(scaled_matrix))
        scores.append(_finite_float(adjusted_rand_score(primary_labels, labels), "stability_score"))
    return min(scores) if scores else 1.0


def _gmm_stability(
    scaled_matrix: FloatMatrix,
    primary_labels: IntVector,
    cluster_count: int,
    config: ArchetypeDiscoveryConfig,
) -> float:
    scores: list[float] = []
    for offset in range(1, config.stability_iterations):
        model = GaussianMixture(
            n_components=cluster_count,
            random_state=config.random_seed + offset,
            covariance_type="full",
            reg_covar=config.covariance_regularization,
        )
        labels = cast(IntVector, model.fit_predict(scaled_matrix))
        scores.append(_finite_float(adjusted_rand_score(primary_labels, labels), "stability_score"))
    return min(scores) if scores else 1.0


def _validate_config(config: ArchetypeDiscoveryConfig) -> None:
    if not config.feature_columns:
        raise ValueError("feature_columns must not be empty")
    if len(set(config.feature_columns)) != len(config.feature_columns):
        raise ValueError("feature_columns must not contain duplicates")
    if not config.cluster_counts:
        raise ValueError("cluster_counts must not be empty")
    if any(cluster_count < 2 for cluster_count in config.cluster_counts):
        raise ValueError("cluster_counts must be at least 2")
    if config.min_family_samples <= 0:
        raise ValueError("min_family_samples must be positive")
    if config.min_cluster_size <= 0:
        raise ValueError("min_cluster_size must be positive")
    if config.high_contribution_feature_count <= 0:
        raise ValueError("high_contribution_feature_count must be positive")
    if config.stability_iterations <= 0:
        raise ValueError("stability_iterations must be positive")
    if not 0.0 <= config.min_stability_score <= 1.0:
        raise ValueError("min_stability_score must be between 0.0 and 1.0")
    if config.covariance_regularization <= 0.0:
        raise ValueError("covariance_regularization must be positive")


def _require_columns(frame: pl.DataFrame, required_columns: Sequence[str], context: str) -> None:
    missing = sorted(set(required_columns).difference(frame.columns))
    if missing:
        raise ValueError(f"{context} missing required columns: {', '.join(missing)}")


def _feature_matrix(frame: pl.DataFrame, feature_columns: Sequence[str]) -> FloatMatrix:
    feature_frame = frame.select(list(feature_columns))
    null_counts = feature_frame.null_count().row(0, named=True)
    columns_with_nulls = [
        column for column, count in null_counts.items() if isinstance(count, int) and count > 0
    ]
    if columns_with_nulls:
        raise ValueError(
            "discover_archetypes requires non-null feature values: "
            f"{', '.join(sorted(columns_with_nulls))}"
        )
    matrix = np.asarray(feature_frame.to_numpy(), dtype=np.float64)
    if not np.isfinite(matrix).all():
        raise ValueError("discover_archetypes requires finite numeric feature values")
    return matrix


def _scale_matrix(
    matrix: FloatMatrix,
    feature_columns: Sequence[str],
) -> tuple[FloatMatrix, dict[str, dict[str, float]]]:
    means = matrix.mean(axis=0)
    scales = matrix.std(axis=0)
    zero_variance_features = [
        feature_columns[index] for index, scale in enumerate(scales) if float(scale) == 0.0
    ]
    if zero_variance_features:
        raise ValueError(
            "discover_archetypes requires non-zero variance features: "
            f"{', '.join(zero_variance_features)}"
        )

    scaled_matrix = (matrix - means) / scales
    scaler = {
        "mean": _feature_dict(means, feature_columns),
        "scale": _feature_dict(scales, feature_columns),
    }
    return scaled_matrix, scaler


def _inverse_scale(
    scaled_values: FloatMatrix,
    scaler: dict[str, dict[str, float]],
    feature_columns: Sequence[str],
) -> FloatMatrix:
    means = np.array([scaler["mean"][feature] for feature in feature_columns], dtype=np.float64)
    scales = np.array([scaler["scale"][feature] for feature in feature_columns], dtype=np.float64)
    return scaled_values * scales + means


def _silhouette(scaled_matrix: FloatMatrix, labels: IntVector) -> float:
    return _finite_float(silhouette_score(scaled_matrix, labels), "silhouette")


def _feature_dict(values: npt.NDArray[np.float64], feature_columns: Sequence[str]) -> dict[str, float]:
    return {
        feature: _finite_float(values[index], feature) for index, feature in enumerate(feature_columns)
    }


def _matrix_payload(matrix: FloatMatrix) -> list[list[float]]:
    return [
        [_finite_float(value, "matrix_value") for value in row]
        for row in matrix.tolist()
    ]


def _high_contribution_features(
    center_scaled: npt.NDArray[np.float64],
    feature_columns: Sequence[str],
    limit: int,
) -> list[dict[str, float | str]]:
    ranked_indices = sorted(
        range(len(feature_columns)),
        key=lambda index: (-abs(float(center_scaled[index])), feature_columns[index]),
    )
    return [
        {
            "feature": feature_columns[index],
            "scaled_value": _finite_float(center_scaled[index], "scaled_value"),
            "abs_scaled_value": _finite_float(abs(center_scaled[index]), "abs_scaled_value"),
        }
        for index in ranked_indices[:limit]
    ]


def _finite_float(value: SupportsFloat, field_name: str) -> float:
    result = float(value)
    if not np.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result
