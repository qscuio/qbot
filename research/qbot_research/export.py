from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, Mapping, Protocol, Sequence, cast
from uuid import UUID

from psycopg.types.json import Jsonb

from qbot_research.contracts import (
    AnalysisPatternExamplePayload,
    AnalysisPatternVersionPayload,
    ExampleType,
    ModelExportHorizon,
    PatternModelPayload,
    PatternType,
    PatternVersionStatus,
    ValidationPayload,
)

MODEL_EXPORT_HORIZONS: frozenset[str] = frozenset({"week", "month"})


@dataclass(frozen=True)
class ExportMetadata:
    pattern_version_id: UUID
    pattern_id: str
    horizon: ModelExportHorizon
    pattern_type: PatternType
    schema_version: str
    feature_version: str
    logic_version: str
    dataset_version: str
    trained_from: date
    trained_until: date
    available_at_cutoff: datetime
    created_at: datetime

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, object],
        *,
        horizon: str,
        dataset_version: str,
    ) -> ExportMetadata:
        return cls(
            pattern_version_id=_uuid_value(payload, "pattern_version_id"),
            pattern_id=_string_value(payload, "pattern_id"),
            horizon=validate_model_export_horizon(horizon),
            pattern_type=_pattern_type_value(payload, "pattern_type"),
            schema_version=_string_value(payload, "schema_version"),
            feature_version=_string_value(payload, "feature_version"),
            logic_version=_string_value(payload, "logic_version"),
            dataset_version=dataset_version,
            trained_from=_date_value(payload, "trained_from"),
            trained_until=_date_value(payload, "trained_until"),
            available_at_cutoff=_datetime_value(payload, "available_at_cutoff"),
            created_at=_datetime_value(payload, "created_at"),
        )

    def __post_init__(self) -> None:
        validate_model_export_horizon(self.horizon)
        if self.trained_from > self.trained_until:
            raise ValueError("trained_from must be on or before trained_until")


@dataclass(frozen=True)
class PatternVersionRow:
    pattern_version_id: UUID
    pattern_id: str
    horizon: ModelExportHorizon
    pattern_type: PatternType
    status: PatternVersionStatus
    schema_version: str
    feature_version: str
    logic_version: str
    dataset_version: str
    model_payload: Mapping[str, Any]
    validation_payload: Mapping[str, Any]
    trained_from: date
    trained_until: date
    available_at_cutoff: datetime
    approved_by: None
    published_at: None
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "model_payload", _freeze_json_like(self.model_payload))
        object.__setattr__(
            self,
            "validation_payload",
            _freeze_json_like(self.validation_payload),
        )
        AnalysisPatternVersionPayload.model_validate(self.payload())

    def payload(self) -> dict[str, Any]:
        return {
            "pattern_version_id": str(self.pattern_version_id),
            "pattern_id": self.pattern_id,
            "horizon": self.horizon,
            "pattern_type": self.pattern_type,
            "status": self.status,
            "schema_version": self.schema_version,
            "feature_version": self.feature_version,
            "logic_version": self.logic_version,
            "dataset_version": self.dataset_version,
            "model_payload": _thaw_json_like(self.model_payload),
            "validation_payload": _thaw_json_like(self.validation_payload),
            "trained_from": self.trained_from,
            "trained_until": self.trained_until,
            "available_at_cutoff": self.available_at_cutoff,
            "approved_by": self.approved_by,
            "published_at": self.published_at,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class PatternExampleRow:
    pattern_version_id: UUID
    example_type: ExampleType
    code: str
    trade_date: date
    similarity: float | None
    metadata: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _freeze_json_like(self.metadata))
        AnalysisPatternExamplePayload.model_validate(self.payload())

    def payload(self) -> dict[str, Any]:
        return {
            "pattern_version_id": str(self.pattern_version_id),
            "example_type": self.example_type,
            "code": self.code,
            "trade_date": self.trade_date,
            "similarity": self.similarity,
            "metadata": _thaw_json_like(self.metadata),
        }


@dataclass(frozen=True)
class PatternVersionExport:
    version_row: PatternVersionRow
    example_rows: tuple[PatternExampleRow, ...]

    def version_row_payload(self) -> dict[str, Any]:
        return self.version_row.payload()

    def payload(self) -> dict[str, Any]:
        return {
            "version_row": self.version_row.payload(),
            "example_rows": [example.payload() for example in self.example_rows],
        }


class ExecuteCursor(Protocol):
    def execute(self, sql: str, parameters: tuple[Any, ...]) -> object: ...


def validate_model_export_horizon(value: str) -> ModelExportHorizon:
    if value not in MODEL_EXPORT_HORIZONS:
        raise ValueError("only 'week' and 'month' horizons may export model versions")
    return cast(ModelExportHorizon, value)


def export_pattern_version(
    *,
    candidate_payload: Mapping[str, object],
    validation_payload: Mapping[str, object],
    metadata: ExportMetadata,
    typical_positive_examples: Sequence[Mapping[str, object]],
    failed_examples: Sequence[Mapping[str, object]],
) -> PatternVersionExport:
    if not typical_positive_examples:
        raise ValueError("at least one typical_positive example is required")
    if not failed_examples:
        raise ValueError("at least one failed example is required")

    validated_payload = ValidationPayload.model_validate(validation_payload)
    model_payload_input = dict(candidate_payload)
    model_payload_input["validation_lift"] = validated_payload.lift
    model_payload_input["validation_coverage"] = validated_payload.coverage
    model_payload_input["baseline_comparison"] = dict(validated_payload.baseline_comparison)
    model_payload = PatternModelPayload.model_validate(model_payload_input)
    row = PatternVersionRow(
        pattern_version_id=metadata.pattern_version_id,
        pattern_id=metadata.pattern_id,
        horizon=metadata.horizon,
        pattern_type=metadata.pattern_type,
        status=validated_payload.candidate_status,
        schema_version=metadata.schema_version,
        feature_version=metadata.feature_version,
        logic_version=metadata.logic_version,
        dataset_version=metadata.dataset_version,
        model_payload=model_payload.model_dump(mode="json"),
        validation_payload=validated_payload.model_dump(mode="json"),
        trained_from=metadata.trained_from,
        trained_until=metadata.trained_until,
        available_at_cutoff=metadata.available_at_cutoff,
        approved_by=None,
        published_at=None,
        created_at=metadata.created_at,
    )
    examples = (
        *_example_rows(
            "typical_positive",
            metadata.pattern_version_id,
            typical_positive_examples,
        ),
        *_example_rows("failed", metadata.pattern_version_id, failed_examples),
    )
    return PatternVersionExport(version_row=row, example_rows=examples)


def insert_pattern_version_export(cursor: ExecuteCursor, exported: PatternVersionExport) -> None:
    row = exported.version_row
    row_payload = row.payload()
    cursor.execute(
        """
        INSERT INTO analysis_pattern_versions
        (pattern_version_id, pattern_id, horizon, pattern_type, status, schema_version,
         feature_version, logic_version, dataset_version, model_payload, validation_payload,
         trained_from, trained_until, available_at_cutoff, approved_by, published_at, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row.pattern_version_id,
            row.pattern_id,
            row.horizon,
            row.pattern_type,
            row.status,
            row.schema_version,
            row.feature_version,
            row.logic_version,
            row.dataset_version,
            Jsonb(row_payload["model_payload"]),
            Jsonb(row_payload["validation_payload"]),
            row.trained_from,
            row.trained_until,
            row.available_at_cutoff,
            row.approved_by,
            row.published_at,
            row.created_at,
        ),
    )
    for example in exported.example_rows:
        example_payload = example.payload()
        cursor.execute(
            """
            INSERT INTO analysis_pattern_examples
            (pattern_version_id, example_type, code, trade_date, similarity, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                example.pattern_version_id,
                example.example_type,
                example.code,
                example.trade_date,
                example.similarity,
                Jsonb(example_payload["metadata"]),
            ),
        )


def _example_rows(
    example_type: ExampleType,
    pattern_version_id: UUID,
    examples: Sequence[Mapping[str, object]],
) -> tuple[PatternExampleRow, ...]:
    rows: list[PatternExampleRow] = []
    for example in examples:
        payload = AnalysisPatternExamplePayload.model_validate(
            {
                "pattern_version_id": str(pattern_version_id),
                "example_type": example_type,
                **example,
            }
        )
        rows.append(
            PatternExampleRow(
                pattern_version_id=pattern_version_id,
                example_type=payload.example_type,
                code=payload.code,
                trade_date=payload.trade_date,
                similarity=payload.similarity,
                metadata=payload.metadata,
            )
        )
    return tuple(rows)


def _string_value(payload: Mapping[str, object], field_name: str) -> str:
    value = _required_value(payload, field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _uuid_value(payload: Mapping[str, object], field_name: str) -> UUID:
    value = _required_value(payload, field_name)
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        return UUID(value)
    raise ValueError(f"{field_name} must be a UUID string")


def _date_value(payload: Mapping[str, object], field_name: str) -> date:
    value = _required_value(payload, field_name)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"{field_name} must be an ISO date string")


def _datetime_value(payload: Mapping[str, object], field_name: str) -> datetime:
    value = _required_value(payload, field_name)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise ValueError(f"{field_name} must be an ISO datetime string")


def _pattern_type_value(payload: Mapping[str, object], field_name: str) -> PatternType:
    value = _string_value(payload, field_name)
    valid_pattern_types = {"trend", "vcp_breakout", "oversold_reversal"}
    if value not in valid_pattern_types:
        raise ValueError(f"{field_name} must be one of {sorted(valid_pattern_types)}")
    return cast(PatternType, value)


def _required_value(payload: Mapping[str, object], field_name: str) -> object:
    if field_name not in payload:
        raise ValueError(f"{field_name} is required")
    return payload[field_name]


def _freeze_json_like(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_json_like(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_json_like(item) for item in value)
    return value


def _thaw_json_like(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json_like(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json_like(item) for item in value]
    return value
