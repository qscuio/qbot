from datetime import date, datetime, timezone

import pytest
import typer
from pydantic import ValidationError

from qbot_research.cli import app
from qbot_research.contracts import DatasetManifest, PatternModelPayload


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


def test_pattern_model_payload_rejects_missing_required_feature_payload() -> None:
    with pytest.raises(ValidationError, match="required_features"):
        PatternModelPayload(
            required_features=["close_strength", "volume_ratio"],
            scaler_mean={"close_strength": 1.5, "volume_ratio": 2.5},
            scaler_scale={"close_strength": 0.4},
            centroid={"close_strength": 1.1, "volume_ratio": 3.1},
            distance_metric="euclidean",
            similarity_thresholds={"shadow_a": 0.9},
            necessary_conditions=[{"field": "trend", "operator": "gte", "value": 1.0}],
            risk_conditions=[{"field": "drawdown", "operator": "lte", "value": 0.1}],
        )


def test_cli_exports_importable_typer_app() -> None:
    assert isinstance(app, typer.Typer)
