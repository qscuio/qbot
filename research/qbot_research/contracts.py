from __future__ import annotations

from datetime import date, datetime
from math import isfinite
from typing import Literal, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

Horizon: TypeAlias = Literal["week", "month", "quarter", "year"]
ModelExportHorizon: TypeAlias = Literal["week", "month"]
PatternType: TypeAlias = Literal["trend", "vcp_breakout", "oversold_reversal"]
DistanceMetric: TypeAlias = Literal["euclidean", "mahalanobis", "gmm_probability"]
ConditionPayload: TypeAlias = dict[str, object]
PatternVersionStatus: TypeAlias = Literal["draft", "validated"]
ExampleType: TypeAlias = Literal["typical_positive", "failed"]
CovarianceMatrix: TypeAlias = list[list[float]]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ClusterParameters(ContractModel):
    covariance: CovarianceMatrix | None = None
    mixture_mean: dict[str, float] | None = None
    mixture_covariance: CovarianceMatrix | None = None
    mixture_weight: float | None = None


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
    cluster_parameters: ClusterParameters
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

    @model_validator(mode="after")
    def validate_cluster_parameters(self) -> PatternModelPayload:
        dimension = len(self.required_features)
        if self.distance_metric == "euclidean":
            return self

        if self.distance_metric == "mahalanobis":
            if self.cluster_parameters.covariance is None:
                raise ValueError("cluster_parameters.covariance is required for mahalanobis")
            _validate_covariance_matrix(
                "cluster_parameters.covariance",
                self.cluster_parameters.covariance,
                dimension,
            )
            return self

        if self.cluster_parameters.mixture_mean is None:
            raise ValueError("cluster_parameters.mixture_mean is required for gmm_probability")
        if self.cluster_parameters.mixture_covariance is None:
            raise ValueError("cluster_parameters.mixture_covariance is required for gmm_probability")
        if self.cluster_parameters.mixture_weight is None:
            raise ValueError("cluster_parameters.mixture_weight is required for gmm_probability")
        _validate_required_feature_map(
            "cluster_parameters.mixture_mean",
            self.cluster_parameters.mixture_mean,
            self.required_features,
        )
        _validate_covariance_matrix(
            "cluster_parameters.mixture_covariance",
            self.cluster_parameters.mixture_covariance,
            dimension,
        )
        if not isfinite(self.cluster_parameters.mixture_weight):
            raise ValueError("cluster_parameters.mixture_weight must be finite")
        if self.cluster_parameters.mixture_weight <= 0.0:
            raise ValueError("cluster_parameters.mixture_weight must be positive")
        return self


def _validate_required_feature_map(
    field_name: str,
    payload: dict[str, float],
    required_features: list[str],
) -> None:
    missing = [feature for feature in required_features if feature not in payload]
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"{field_name} must include values for required_features: {missing_csv}")
    for feature in required_features:
        if not isfinite(payload[feature]):
            raise ValueError(f"{field_name} must contain finite values: {feature}")


def _validate_covariance_matrix(
    field_name: str,
    matrix: CovarianceMatrix,
    dimension: int,
) -> None:
    if not matrix:
        raise ValueError(f"{field_name} must have positive dimension")
    if len(matrix) != dimension:
        raise ValueError(f"{field_name} dimensions must match required_features")
    for row in matrix:
        if len(row) != len(matrix):
            raise ValueError(f"{field_name} must be square")
        if any(not isfinite(value) for value in row):
            raise ValueError(f"{field_name} must contain finite values")


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
    pattern_version_id: UUID
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
    pattern_version_id: UUID
    example_type: ExampleType
    code: str
    trade_date: date
    similarity: float | None
    metadata: dict[str, object]
