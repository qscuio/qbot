from datetime import date, datetime, timezone

import pytest
import typer
from pydantic import ValidationError
from typer.testing import CliRunner

from qbot_research.cli import app
from qbot_research.contracts import (
    AnalysisPatternExamplePayload,
    AnalysisPatternVersionPayload,
    DatasetManifest,
    PatternModelPayload,
)

RUNNER = CliRunner()


def test_dataset_manifest_accepts_valid_values() -> None:
    manifest = DatasetManifest(
        dataset_version="dataset-v1",
        schema_version="1",
        feature_version="1",
        horizon="week",
        data_cutoff=date(2026, 7, 10),
        available_at_cutoff=datetime(2026, 7, 10, 15, 0, tzinfo=timezone.utc),
        row_count=12,
        date_from=date(2026, 7, 1),
        date_to=date(2026, 7, 10),
        files=["dataset.parquet"],
        file_checksums={"dataset.parquet": "abc123"},
        input_fingerprint="fingerprint-v1",
    )

    assert manifest.horizon == "week"
    assert manifest.row_count == 12


def test_dataset_manifest_rejects_invalid_horizon() -> None:
    with pytest.raises(ValidationError, match="horizon"):
        DatasetManifest(
            dataset_version="dataset-v1",
            schema_version="1",
            feature_version="1",
            horizon="day",
            data_cutoff=date(2026, 7, 10),
            available_at_cutoff=datetime(2026, 7, 10, 15, 0, tzinfo=timezone.utc),
            row_count=12,
            date_from=date(2026, 7, 1),
            date_to=date(2026, 7, 10),
            files=["dataset.parquet"],
            file_checksums={"dataset.parquet": "abc123"},
            input_fingerprint="fingerprint-v1",
        )


def test_dataset_manifest_rejects_negative_row_count() -> None:
    with pytest.raises(ValidationError, match="row_count"):
        DatasetManifest(
            dataset_version="dataset-v1",
            schema_version="1",
            feature_version="1",
            horizon="week",
            data_cutoff=date(2026, 7, 10),
            available_at_cutoff=datetime(2026, 7, 10, 15, 0, tzinfo=timezone.utc),
            row_count=-1,
            date_from=date(2026, 7, 1),
            date_to=date(2026, 7, 10),
            files=["dataset.parquet"],
            file_checksums={"dataset.parquet": "abc123"},
            input_fingerprint="fingerprint-v1",
        )


@pytest.mark.parametrize("missing_payload_field", ["scaler_mean", "scaler_scale", "centroid"])
def test_pattern_model_payload_rejects_missing_required_feature_payload(
    missing_payload_field: str,
) -> None:
    scaler_mean = {"close_strength": 1.5, "volume_ratio": 2.5}
    scaler_scale = {"close_strength": 0.4, "volume_ratio": 0.6}
    centroid = {"close_strength": 1.1, "volume_ratio": 3.1}

    if missing_payload_field == "scaler_mean":
        del scaler_mean["volume_ratio"]
    elif missing_payload_field == "scaler_scale":
        del scaler_scale["volume_ratio"]
    else:
        del centroid["volume_ratio"]

    with pytest.raises(ValidationError, match="required_features"):
        PatternModelPayload(
            required_features=["close_strength", "volume_ratio"],
            scaler_mean=scaler_mean,
            scaler_scale=scaler_scale,
            centroid=centroid,
            distance_metric="euclidean",
            cluster_parameters={},
            similarity_thresholds={"shadow_a": 0.9},
            necessary_conditions=[{"field": "trend", "operator": "gte", "value": 1.0}],
            risk_conditions=[{"field": "drawdown", "operator": "lte", "value": 0.1}],
        )


def _pattern_model_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "required_features": ["close_strength", "volume_ratio"],
        "scaler_mean": {"close_strength": 1.5, "volume_ratio": 2.5},
        "scaler_scale": {"close_strength": 0.4, "volume_ratio": 0.6},
        "centroid": {"close_strength": 1.1, "volume_ratio": 3.1},
        "distance_metric": "euclidean",
        "cluster_parameters": {},
        "similarity_thresholds": {"shadow_a": 0.9},
        "necessary_conditions": [{"field": "trend", "operator": "gte", "value": 1.0}],
        "risk_conditions": [{"field": "drawdown", "operator": "lte", "value": 0.1}],
    }
    payload.update(overrides)
    return payload


def test_pattern_model_payload_accepts_empty_euclidean_cluster_parameters() -> None:
    model = PatternModelPayload.model_validate(_pattern_model_payload())

    assert model.cluster_parameters.model_dump(exclude_none=True) == {}


def test_pattern_model_payload_rejects_missing_mahalanobis_covariance() -> None:
    with pytest.raises(ValidationError, match="covariance"):
        PatternModelPayload.model_validate(
            _pattern_model_payload(distance_metric="mahalanobis", cluster_parameters={})
        )


@pytest.mark.parametrize(
    "covariance",
    [
        [],
        [[1.0, 0.0]],
        [[1.0, 0.0], [0.0]],
        [[1.0, float("nan")], [0.0, 1.0]],
    ],
)
def test_pattern_model_payload_rejects_invalid_mahalanobis_covariance(
    covariance: list[list[float]],
) -> None:
    with pytest.raises(ValidationError, match="covariance"):
        PatternModelPayload.model_validate(
            _pattern_model_payload(
                distance_metric="mahalanobis",
                cluster_parameters={"covariance": covariance},
            )
        )


def test_pattern_model_payload_requires_gmm_cluster_parameters() -> None:
    with pytest.raises(ValidationError, match="mixture_mean"):
        PatternModelPayload.model_validate(
            _pattern_model_payload(
                distance_metric="gmm_probability",
                cluster_parameters={
                    "mixture_covariance": [[1.0, 0.0], [0.0, 1.0]],
                    "mixture_weight": 0.7,
                },
            )
        )
    with pytest.raises(ValidationError, match="mixture_covariance"):
        PatternModelPayload.model_validate(
            _pattern_model_payload(
                distance_metric="gmm_probability",
                cluster_parameters={
                    "mixture_mean": {"close_strength": 1.0, "volume_ratio": 2.0},
                    "mixture_weight": 0.7,
                },
            )
        )
    with pytest.raises(ValidationError, match="mixture_weight"):
        PatternModelPayload.model_validate(
            _pattern_model_payload(
                distance_metric="gmm_probability",
                cluster_parameters={
                    "mixture_mean": {"close_strength": 1.0, "volume_ratio": 2.0},
                    "mixture_covariance": [[1.0, 0.0], [0.0, 1.0]],
                },
            )
        )


def test_pattern_model_payload_validates_gmm_cluster_parameters() -> None:
    model = PatternModelPayload.model_validate(
        _pattern_model_payload(
            distance_metric="gmm_probability",
            cluster_parameters={
                "mixture_mean": {"close_strength": 1.0, "volume_ratio": 2.0},
                "mixture_covariance": [[1.0, 0.2], [0.2, 1.5]],
                "mixture_weight": 0.7,
            },
        )
    )

    assert model.cluster_parameters.mixture_weight == 0.7

    with pytest.raises(ValidationError, match="mixture_mean"):
        PatternModelPayload.model_validate(
            _pattern_model_payload(
                distance_metric="gmm_probability",
                cluster_parameters={
                    "mixture_mean": {"close_strength": 1.0},
                    "mixture_covariance": [[1.0, 0.0], [0.0, 1.0]],
                    "mixture_weight": 0.7,
                },
            )
        )
    with pytest.raises(ValidationError, match="mixture_weight"):
        PatternModelPayload.model_validate(
            _pattern_model_payload(
                distance_metric="gmm_probability",
                cluster_parameters={
                    "mixture_mean": {"close_strength": 1.0, "volume_ratio": 2.0},
                    "mixture_covariance": [[1.0, 0.0], [0.0, 1.0]],
                    "mixture_weight": 0.0,
                },
            )
        )


def test_cli_exports_importable_typer_app() -> None:
    assert isinstance(app, typer.Typer)


def test_cli_train_all_requires_explicit_plan_json() -> None:
    result = RUNNER.invoke(app, ["train-all", "--as-of", "2026-07-10"])

    assert result.exit_code != 0
    assert "--plan-json" in result.stdout


def _analysis_pattern_version_payload(pattern_version_id: object) -> dict[str, object]:
    return {
        "pattern_version_id": pattern_version_id,
        "pattern_id": "trend-kmeans-c0",
        "horizon": "week",
        "pattern_type": "trend",
        "status": "validated",
        "schema_version": "1",
        "feature_version": "1",
        "logic_version": "logic-v1",
        "dataset_version": "ptf-v1-week-20260710",
        "model_payload": {
            "required_features": ["return_20d"],
            "scaler_mean": {"return_20d": 0.14},
            "scaler_scale": {"return_20d": 0.04},
            "centroid": {"return_20d": 0.18},
            "distance_metric": "euclidean",
            "cluster_parameters": {},
            "similarity_thresholds": {"shadow_a": 0.88},
            "necessary_conditions": [
                {"column": "return_20d", "operator": ">=", "value": 0.10},
            ],
            "risk_conditions": [
                {"column": "future_max_drawdown", "operator": ">=", "value": -0.08},
            ],
        },
        "validation_payload": {
            "candidate_id": "trend:kmeans:k2:c0",
            "positive_sample_count": 12,
            "control_sample_count": 18,
            "effective_sample_count": 8.0,
            "base_rate": 0.40,
            "precision": 0.75,
            "lift": 1.875,
            "lift_over_base_rate": 1.875,
            "coverage": 0.27,
            "false_positive_rate": 0.11,
            "precision_at_10": 0.70,
            "precision_at_50": 0.62,
            "cost_adjusted_return": 0.032,
            "max_drawdown": -0.045,
            "turnover": 0.20,
            "yearly_results": {"2026": {"sample_count": 30, "precision": 0.75}},
            "regime_results": {"bull": {"sample_count": 18, "precision": 0.80}},
            "top_stock_contribution": 0.20,
            "top_period_contribution": 0.25,
            "mean_excess_return": 0.024,
            "median_excess_return": 0.020,
            "win_rate": 0.72,
            "profit_factor": 2.40,
            "max_losing_streak": 2,
            "capacity_estimate": 1_000_000.0,
            "cluster_stability": 0.86,
            "calibration_error": 0.05,
            "majority_windows_positive_lift": True,
            "baseline_comparison": {
                "best_required_baseline_return": 0.01,
                "cost_adjusted_return_delta": 0.022,
            },
            "release_gate_passed": True,
            "candidate_status": "validated",
        },
        "trained_from": date(2026, 1, 1),
        "trained_until": date(2026, 7, 10),
        "available_at_cutoff": datetime(2026, 7, 10, 23, 0, tzinfo=timezone.utc),
        "approved_by": None,
        "published_at": None,
        "created_at": datetime(2026, 7, 10, 23, 0, tzinfo=timezone.utc),
    }


def test_analysis_pattern_version_requires_uuid_pattern_version_id() -> None:
    payload = _analysis_pattern_version_payload("11111111-1111-4111-8111-111111111111")

    validated = AnalysisPatternVersionPayload.model_validate(payload)

    assert str(validated.pattern_version_id) == "11111111-1111-4111-8111-111111111111"

    with pytest.raises(ValidationError, match="pattern_version_id"):
        AnalysisPatternVersionPayload.model_validate(
            _analysis_pattern_version_payload("not-a-uuid")
        )


def test_analysis_pattern_example_requires_uuid_pattern_version_id() -> None:
    payload = {
        "pattern_version_id": "11111111-1111-4111-8111-111111111111",
        "example_type": "typical_positive",
        "code": "AAA",
        "trade_date": date(2026, 7, 8),
        "similarity": 0.94,
        "metadata": {"rank": 1},
    }

    validated = AnalysisPatternExamplePayload.model_validate(payload)

    assert str(validated.pattern_version_id) == "11111111-1111-4111-8111-111111111111"

    with pytest.raises(ValidationError, match="pattern_version_id"):
        AnalysisPatternExamplePayload.model_validate(
            {
                **payload,
                "pattern_version_id": "not-a-uuid",
            }
        )
