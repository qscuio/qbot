from __future__ import annotations

from datetime import date, datetime
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

Horizon: TypeAlias = Literal["week", "month", "quarter", "year"]
ModelExportHorizon: TypeAlias = Literal["week", "month"]
PatternType: TypeAlias = Literal["trend", "vcp_breakout", "oversold_reversal"]
DistanceMetric: TypeAlias = Literal["euclidean", "mahalanobis", "gmm_probability"]
ConditionPayload: TypeAlias = dict[str, object]
PatternVersionStatus: TypeAlias = Literal["draft", "validated"]
ExampleType: TypeAlias = Literal["typical_positive", "failed"]


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
    candidate_id: str
    positive_sample_count: int
    control_sample_count: int
    effective_sample_count: float
    base_rate: float
    precision: float
    lift: float
    lift_over_base_rate: float
    coverage: float
    false_positive_rate: float
    precision_at_10: float
    precision_at_50: float
    cost_adjusted_return: float
    max_drawdown: float
    turnover: float
    yearly_results: dict[str, dict[str, float | int]]
    regime_results: dict[str, dict[str, float | int]]
    top_stock_contribution: float
    top_period_contribution: float
    mean_excess_return: float
    median_excess_return: float
    win_rate: float
    profit_factor: float
    max_losing_streak: int
    capacity_estimate: float
    cluster_stability: float | None
    calibration_error: float
    majority_windows_positive_lift: bool
    baseline_comparison: dict[str, float]
    release_gate_passed: bool
    candidate_status: PatternVersionStatus


class AnalysisPatternVersionPayload(ContractModel):
    pattern_version_id: str
    pattern_id: str
    horizon: ModelExportHorizon
    pattern_type: PatternType
    status: PatternVersionStatus
    schema_version: str
    feature_version: str
    logic_version: str
    dataset_version: str
    model_payload: PatternModelPayload
    validation_payload: ValidationPayload
    trained_from: date
    trained_until: date
    available_at_cutoff: datetime
    approved_by: None = None
    published_at: None = None
    created_at: datetime

    @model_validator(mode="after")
    def validate_pattern_version_consistency(self) -> AnalysisPatternVersionPayload:
        if self.trained_from > self.trained_until:
            raise ValueError("trained_from must be on or before trained_until")
        if self.status != self.validation_payload.candidate_status:
            raise ValueError("status must match validation_payload.candidate_status")
        return self


class AnalysisPatternExamplePayload(ContractModel):
    pattern_version_id: str
    example_type: ExampleType
    code: str
    trade_date: date
    similarity: float | None
    metadata: dict[str, object]
