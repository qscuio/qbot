from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from qbot_research.cli import app
from qbot_research.contracts import AnalysisPatternVersionPayload
from qbot_research.export import (
    ExportMetadata,
    PatternVersionExport,
    export_pattern_version,
    insert_pattern_version_export,
)

RUNNER = CliRunner()


def dt(year: int, month: int, day: int, hour: int = 12) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def _candidate_payload() -> dict[str, Any]:
    return {
        "required_features": ["return_20d", "relative_strength_20d"],
        "scaler_mean": {"return_20d": 0.14, "relative_strength_20d": 1.25},
        "scaler_scale": {"return_20d": 0.04, "relative_strength_20d": 0.12},
        "centroid": {"return_20d": 0.18, "relative_strength_20d": 1.41},
        "distance_metric": "euclidean",
        "similarity_thresholds": {"shadow_a": 0.88, "shadow_b": 0.80},
        "necessary_conditions": [
            {"column": "return_20d", "operator": ">=", "value": 0.10},
        ],
        "risk_conditions": [
            {"column": "future_max_drawdown", "operator": ">=", "value": -0.08},
        ],
    }


def _validation_payload(status: str = "validated") -> dict[str, Any]:
    return {
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
        "release_gate_passed": status == "validated",
        "candidate_status": status,
    }


def _metadata(**overrides: Any) -> ExportMetadata:
    values: dict[str, Any] = {
        "pattern_version_id": UUID("11111111-1111-4111-8111-111111111111"),
        "pattern_id": "trend-kmeans-c0",
        "horizon": "week",
        "pattern_type": "trend",
        "schema_version": "1",
        "feature_version": "1",
        "logic_version": "logic-v1",
        "dataset_version": "ptf-v1-week-20260710",
        "trained_from": date(2026, 1, 1),
        "trained_until": date(2026, 7, 10),
        "available_at_cutoff": dt(2026, 7, 10, 23),
        "created_at": dt(2026, 7, 10, 23),
    }
    values.update(overrides)
    return ExportMetadata(**values)


def _positive_examples() -> list[dict[str, Any]]:
    return [
        {
            "code": "AAA",
            "trade_date": date(2026, 7, 8),
            "similarity": 0.94,
            "metadata": {"rank": 1, "future_return": 0.05},
        },
    ]


def _failed_examples() -> list[dict[str, Any]]:
    return [
        {
            "code": "BBB",
            "trade_date": date(2026, 7, 9),
            "similarity": 0.89,
            "metadata": {"rank": 2, "failure_reason": "false_positive"},
        },
    ]


def _export(status: str = "validated") -> PatternVersionExport:
    return export_pattern_version(
        candidate_payload=_candidate_payload(),
        validation_payload=_validation_payload(status),
        metadata=_metadata(),
        typical_positive_examples=_positive_examples(),
        failed_examples=_failed_examples(),
    )


def test_export_pattern_version_builds_validated_immutable_rows_and_examples() -> None:
    exported = _export()

    assert exported.version_row.status == "validated"
    assert exported.version_row.approved_by is None
    assert exported.version_row.published_at is None
    assert [example.example_type for example in exported.example_rows] == [
        "typical_positive",
        "failed",
    ]

    payload = AnalysisPatternVersionPayload.model_validate(exported.version_row_payload())
    assert payload.status == "validated"
    assert payload.model_payload.required_features == ["return_20d", "relative_strength_20d"]
    assert payload.validation_payload.candidate_status == "validated"

    with pytest.raises(AttributeError):
        exported.version_row.status = "draft"  # type: ignore[misc]


def test_export_pattern_version_accepts_draft_and_rejects_published_status() -> None:
    draft = _export("draft")

    assert draft.version_row.status == "draft"

    with pytest.raises(ValidationError, match="candidate_status"):
        _export("published")


def test_export_pattern_version_rejects_non_model_export_horizons() -> None:
    with pytest.raises(ValueError, match="only 'week' and 'month'"):
        export_pattern_version(
            candidate_payload=_candidate_payload(),
            validation_payload=_validation_payload("draft"),
            metadata=_metadata(horizon="quarter"),
            typical_positive_examples=_positive_examples(),
            failed_examples=_failed_examples(),
        )


def test_export_pattern_version_requires_positive_and_failed_examples() -> None:
    with pytest.raises(ValueError, match="typical_positive"):
        export_pattern_version(
            candidate_payload=_candidate_payload(),
            validation_payload=_validation_payload("draft"),
            metadata=_metadata(),
            typical_positive_examples=[],
            failed_examples=_failed_examples(),
        )

    with pytest.raises(ValueError, match="failed"):
        export_pattern_version(
            candidate_payload=_candidate_payload(),
            validation_payload=_validation_payload("draft"),
            metadata=_metadata(),
            typical_positive_examples=_positive_examples(),
            failed_examples=[],
        )


@dataclass
class RecordingCursor:
    calls: list[tuple[str, tuple[Any, ...]]]

    def execute(self, sql: str, parameters: tuple[Any, ...]) -> None:
        self.calls.append((sql, parameters))


def test_insert_pattern_version_export_writes_exact_version_and_example_columns() -> None:
    cursor = RecordingCursor(calls=[])
    exported = _export()

    insert_pattern_version_export(cursor, exported)

    assert len(cursor.calls) == 3
    version_sql, version_parameters = cursor.calls[0]
    assert "INSERT INTO analysis_pattern_versions" in version_sql
    assert (
        "pattern_version_id, pattern_id, horizon, pattern_type, status, schema_version, "
        "feature_version, logic_version, dataset_version, model_payload, validation_payload, "
        "trained_from, trained_until, available_at_cutoff, approved_by, published_at, created_at"
    ) in " ".join(version_sql.split())
    assert version_parameters[0] == UUID("11111111-1111-4111-8111-111111111111")
    assert version_parameters[4] == "validated"
    assert version_parameters[14] is None
    assert version_parameters[15] is None

    for _, parameters in cursor.calls[1:]:
        assert parameters[0] == UUID("11111111-1111-4111-8111-111111111111")
    example_types = [parameters[1] for _, parameters in cursor.calls[1:]]
    assert example_types == ["typical_positive", "failed"]
    assert all("INSERT INTO analysis_pattern_examples" in sql for sql, _ in cursor.calls[1:])


def test_train_command_exports_from_explicit_json_inputs(tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidate.json"
    validation_path = tmp_path / "validation.json"
    metadata_path = tmp_path / "metadata.json"
    positive_path = tmp_path / "positive.json"
    failed_path = tmp_path / "failed.json"
    output_path = tmp_path / "export.json"

    candidate_path.write_text(json.dumps(_candidate_payload()), encoding="utf-8")
    validation_path.write_text(json.dumps(_validation_payload("validated")), encoding="utf-8")
    metadata_path.write_text(
        json.dumps(
            {
                "pattern_version_id": "11111111-1111-4111-8111-111111111111",
                "pattern_id": "trend-kmeans-c0",
                "pattern_type": "trend",
                "schema_version": "1",
                "feature_version": "1",
                "logic_version": "logic-v1",
                "trained_from": "2026-01-01",
                "trained_until": "2026-07-10",
                "available_at_cutoff": "2026-07-10T23:00:00+00:00",
                "created_at": "2026-07-10T23:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    positive_path.write_text(json.dumps(_positive_examples(), default=str), encoding="utf-8")
    failed_path.write_text(json.dumps(_failed_examples(), default=str), encoding="utf-8")

    result = RUNNER.invoke(
        app,
        [
            "train",
            "--horizon",
            "week",
            "--dataset-version",
            "ptf-v1-week-20260710",
            "--candidate-json",
            str(candidate_path),
            "--validation-json",
            str(validation_path),
            "--metadata-json",
            str(metadata_path),
            "--positive-examples-json",
            str(positive_path),
            "--failed-examples-json",
            str(failed_path),
            "--output-json",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["version_row"]["status"] == "validated"
    assert payload["version_row"]["dataset_version"] == "ptf-v1-week-20260710"
    assert {row["example_type"] for row in payload["example_rows"]} == {
        "typical_positive",
        "failed",
    }


def test_train_command_rejects_quarter_model_exports(tmp_path: Path) -> None:
    result = RUNNER.invoke(
        app,
        [
            "train",
            "--horizon",
            "quarter",
            "--dataset-version",
            "ptf-v1-quarter-20260710",
            "--candidate-json",
            str(tmp_path / "missing-candidate.json"),
            "--validation-json",
            str(tmp_path / "missing-validation.json"),
            "--metadata-json",
            str(tmp_path / "missing-metadata.json"),
            "--positive-examples-json",
            str(tmp_path / "missing-positive.json"),
            "--failed-examples-json",
            str(tmp_path / "missing-failed.json"),
        ],
    )

    assert result.exit_code != 0
    assert "only 'week' and 'month'" in result.stdout


def test_train_all_requires_explicit_plan_json_and_does_not_pretend_training_ran() -> None:
    result = RUNNER.invoke(app, ["train-all", "--as-of", "2026-07-10"])

    assert result.exit_code != 0
    assert "--plan-json" in result.stdout
    assert "No training ran" in result.stdout


def test_train_all_exports_from_explicit_plan_json(tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.json"
    output_path = tmp_path / "train-all-export.json"
    plan_path.write_text(
        json.dumps(
            {
                "exports": [
                    {
                        "horizon": "month",
                        "dataset_version": "ptf-v1-month-20260710",
                        "metadata": {
                            "pattern_version_id": "11111111-1111-4111-8111-111111111111",
                            "pattern_id": "trend-kmeans-c0",
                            "pattern_type": "trend",
                            "schema_version": "1",
                            "feature_version": "1",
                            "logic_version": "logic-v1",
                            "trained_from": "2026-01-01",
                            "trained_until": "2026-07-10",
                            "available_at_cutoff": "2026-07-10T23:00:00+00:00",
                            "created_at": "2026-07-10T23:00:00+00:00",
                        },
                        "candidate": _candidate_payload(),
                        "validation": _validation_payload("draft"),
                        "typical_positive_examples": _positive_examples(),
                        "failed_examples": _failed_examples(),
                    }
                ]
            },
            default=str,
        ),
        encoding="utf-8",
    )

    result = RUNNER.invoke(
        app,
        [
            "train-all",
            "--as-of",
            "2026-07-10",
            "--plan-json",
            str(plan_path),
            "--output-json",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["exports"][0]["version_row"]["horizon"] == "month"
    assert payload["exports"][0]["version_row"]["status"] == "draft"
