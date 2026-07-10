from __future__ import annotations

from datetime import date, datetime
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

Horizon: TypeAlias = Literal["week", "month", "quarter", "year"]
PatternType: TypeAlias = Literal["trend", "vcp_breakout", "oversold_reversal"]
DistanceMetric: TypeAlias = Literal["euclidean", "mahalanobis", "gmm_probability"]
ConditionPayload: TypeAlias = dict[str, object]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DatasetManifest(ContractModel):
    dataset_version: str
    schema_version: str
    feature_version: str
    horizon: Horizon
    data_cutoff: date
    available_at_cutoff: datetime
    row_count: int = Field(ge=0)
    date_from: date
    date_to: date
    files: list[str]
    file_checksums: dict[str, str]
    input_fingerprint: str

    @model_validator(mode="after")
    def validate_manifest_consistency(self) -> DatasetManifest:
        if self.date_from > self.date_to:
            raise ValueError("date_from must be on or before date_to")
        missing_checksums = [path for path in self.files if path not in self.file_checksums]
        if missing_checksums:
            raise ValueError("file_checksums must include every file listed in files")
        return self


class PatternModelPayload(ContractModel):
    required_features: list[str]
    scaler_mean: dict[str, float]
    scaler_scale: dict[str, float]
    centroid: dict[str, float]
    distance_metric: DistanceMetric
    similarity_thresholds: dict[str, float]
    necessary_conditions: list[ConditionPayload]
    risk_conditions: list[ConditionPayload]

    @field_validator("required_features")
    @classmethod
    def validate_required_features(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("required_features must not be empty")
        return value

    @field_validator("scaler_mean", "scaler_scale", "centroid")
    @classmethod
    def validate_feature_payloads(
        cls,
        value: dict[str, float],
        info: ValidationInfo,
    ) -> dict[str, float]:
        required_features = info.data.get("required_features")
        if not isinstance(required_features, list):
            return value
        missing = [feature for feature in required_features if feature not in value]
        if missing:
            missing_csv = ", ".join(missing)
            raise ValueError(
                f"{info.field_name} must include values for required_features: {missing_csv}"
            )
        return value


class ValidationPayload(ContractModel):
    positive_sample_count: int
    control_sample_count: int
    effective_sample_count: float
    base_rate: float
    precision: float
    lift_over_base_rate: float
    coverage: float
    false_positive_rate: float
    cost_adjusted_return: float
    max_drawdown: float
    majority_windows_positive_lift: bool
    baseline_comparison: dict[str, float]
