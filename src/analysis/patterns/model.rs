use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{AppError, Result};

pub type FeatureVector = BTreeMap<String, f64>;
pub type ConditionPayload = BTreeMap<String, Value>;
pub type CovarianceMatrix = Vec<Vec<f64>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    Euclidean,
    Mahalanobis,
    GmmProbability,
}

impl DistanceMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Euclidean => "euclidean",
            Self::Mahalanobis => "mahalanobis",
            Self::GmmProbability => "gmm_probability",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterParameters {
    pub covariance: Option<CovarianceMatrix>,
    pub mixture_mean: Option<FeatureVector>,
    pub mixture_covariance: Option<CovarianceMatrix>,
    pub mixture_weight: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatternModelPayload {
    pub required_features: Vec<String>,
    pub scaler_mean: FeatureVector,
    pub scaler_scale: FeatureVector,
    pub centroid: FeatureVector,
    pub distance_metric: DistanceMetric,
    pub cluster_parameters: ClusterParameters,
    pub similarity_thresholds: FeatureVector,
    pub necessary_conditions: Vec<ConditionPayload>,
    pub risk_conditions: Vec<ConditionPayload>,
}

impl PatternModelPayload {
    pub fn from_value(value: Value) -> Result<Self> {
        let model: Self = serde_json::from_value(value)?;
        model.validate()?;
        Ok(model)
    }

    fn validate(&self) -> Result<()> {
        if self.required_features.is_empty() {
            return Err(AppError::Internal(
                "required_features must not be empty".to_string(),
            ));
        }
        validate_feature_payload(
            "scaler_mean",
            &self.required_features,
            &self.scaler_mean,
            false,
        )?;
        validate_feature_payload(
            "scaler_scale",
            &self.required_features,
            &self.scaler_scale,
            true,
        )?;
        validate_feature_payload("centroid", &self.required_features, &self.centroid, false)?;
        self.validate_cluster_parameters()?;
        Ok(())
    }

    fn validate_cluster_parameters(&self) -> Result<()> {
        match self.distance_metric {
            DistanceMetric::Euclidean => Ok(()),
            DistanceMetric::Mahalanobis => {
                let covariance = self.cluster_parameters.covariance.as_ref().ok_or_else(|| {
                    AppError::Internal(
                        "cluster_parameters.covariance is required for mahalanobis".to_string(),
                    )
                })?;
                validate_covariance_matrix(
                    "cluster_parameters.covariance",
                    covariance,
                    self.required_features.len(),
                )?;
                validate_positive_definite("cluster_parameters.covariance", covariance)?;
                Ok(())
            }
            DistanceMetric::GmmProbability => {
                let mixture_mean =
                    self.cluster_parameters
                        .mixture_mean
                        .as_ref()
                        .ok_or_else(|| {
                            AppError::Internal(
                                "cluster_parameters.mixture_mean is required for gmm_probability"
                                    .to_string(),
                            )
                        })?;
                validate_feature_payload(
                    "cluster_parameters.mixture_mean",
                    &self.required_features,
                    mixture_mean,
                    false,
                )?;
                let mixture_covariance = self
                    .cluster_parameters
                    .mixture_covariance
                    .as_ref()
                    .ok_or_else(|| {
                        AppError::Internal(
                            "cluster_parameters.mixture_covariance is required for gmm_probability"
                                .to_string(),
                        )
                    })?;
                validate_covariance_matrix(
                    "cluster_parameters.mixture_covariance",
                    mixture_covariance,
                    self.required_features.len(),
                )?;
                validate_positive_definite(
                    "cluster_parameters.mixture_covariance",
                    mixture_covariance,
                )?;
                let mixture_weight = self.cluster_parameters.mixture_weight.ok_or_else(|| {
                    AppError::Internal(
                        "cluster_parameters.mixture_weight is required for gmm_probability"
                            .to_string(),
                    )
                })?;
                if !mixture_weight.is_finite() {
                    return Err(AppError::Internal(
                        "cluster_parameters.mixture_weight must be finite".to_string(),
                    ));
                }
                if mixture_weight <= 0.0 {
                    return Err(AppError::Internal(
                        "cluster_parameters.mixture_weight must be positive".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }
}

fn validate_feature_payload(
    field_name: &str,
    required_features: &[String],
    payload: &FeatureVector,
    require_non_zero: bool,
) -> Result<()> {
    let missing: Vec<&str> = required_features
        .iter()
        .filter_map(|feature| (!payload.contains_key(feature)).then_some(feature.as_str()))
        .collect();
    if !missing.is_empty() {
        return Err(AppError::Internal(format!(
            "{} must include values for required_features: {}",
            field_name,
            missing.join(", ")
        )));
    }
    for feature in required_features {
        let value = payload
            .get(feature)
            .expect("required feature presence checked before finite validation");
        if !value.is_finite() {
            return Err(AppError::Internal(format!(
                "{} must contain finite values for required_features: {}",
                field_name, feature
            )));
        }
        if require_non_zero && *value == 0.0 {
            return Err(AppError::Internal(format!(
                "{} must contain non-zero values for required_features: {}",
                field_name, feature
            )));
        }
    }
    Ok(())
}

fn validate_covariance_matrix(
    field_name: &str,
    matrix: &CovarianceMatrix,
    dimension: usize,
) -> Result<()> {
    if matrix.is_empty() {
        return Err(AppError::Internal(format!(
            "{} must have positive dimension",
            field_name
        )));
    }
    if matrix.len() != dimension {
        return Err(AppError::Internal(format!(
            "{} dimensions must match required_features",
            field_name
        )));
    }
    for row in matrix {
        if row.len() != matrix.len() {
            return Err(AppError::Internal(format!("{} must be square", field_name)));
        }
        for value in row {
            if !value.is_finite() {
                return Err(AppError::Internal(format!(
                    "{} must contain finite values",
                    field_name
                )));
            }
        }
    }
    Ok(())
}

fn validate_positive_definite(field_name: &str, matrix: &CovarianceMatrix) -> Result<()> {
    cholesky_lower(matrix).map(|_| ()).map_err(|_| {
        AppError::Internal(format!(
            "{} must be symmetric positive definite",
            field_name
        ))
    })
}

pub fn cholesky_lower(matrix: &CovarianceMatrix) -> Result<CovarianceMatrix> {
    let dimension = matrix.len();
    let mut lower = vec![vec![0.0; dimension]; dimension];
    for i in 0..dimension {
        for j in 0..=i {
            let mut sum = matrix[i][j];
            for k in 0..j {
                sum -= lower[i][k] * lower[j][k];
            }
            if i == j {
                if sum <= 0.0 || !sum.is_finite() {
                    return Err(AppError::Internal(
                        "covariance matrix must be positive definite".to_string(),
                    ));
                }
                lower[i][j] = sum.sqrt();
            } else {
                if lower[j][j] == 0.0 {
                    return Err(AppError::Internal(
                        "covariance matrix must be positive definite".to_string(),
                    ));
                }
                lower[i][j] = sum / lower[j][j];
            }
        }
    }
    for i in 0..dimension {
        for j in 0..i {
            if (matrix[i][j] - matrix[j][i]).abs() > 1e-10 {
                return Err(AppError::Internal(
                    "covariance matrix must be symmetric".to_string(),
                ));
            }
        }
    }
    Ok(lower)
}

pub fn mahalanobis_distance_squared(delta: &[f64], covariance: &CovarianceMatrix) -> Result<f64> {
    let lower = cholesky_lower(covariance)?;
    let dimension = delta.len();
    if covariance.len() != dimension {
        return Err(AppError::Internal(
            "covariance dimensions must match feature deltas".to_string(),
        ));
    }

    let mut y = vec![0.0; dimension];
    for i in 0..dimension {
        let mut sum = delta[i];
        for (k, value) in y.iter().enumerate().take(i) {
            sum -= lower[i][k] * value;
        }
        y[i] = sum / lower[i][i];
    }

    let mut x = vec![0.0; dimension];
    for i in (0..dimension).rev() {
        let mut sum = y[i];
        for k in (i + 1)..dimension {
            sum -= lower[k][i] * x[k];
        }
        x[i] = sum / lower[i][i];
    }

    let distance_squared = delta
        .iter()
        .zip(x.iter())
        .map(|(delta_value, solved)| delta_value * solved)
        .sum::<f64>();
    if !distance_squared.is_finite() || distance_squared < 0.0 {
        return Err(AppError::Internal(
            "mahalanobis distance must be finite and non-negative".to_string(),
        ));
    }
    Ok(distance_squared)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateStatus {
    Draft,
    Validated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationPayload {
    pub candidate_id: String,
    pub positive_sample_count: i64,
    pub control_sample_count: i64,
    pub effective_sample_count: f64,
    pub base_rate: f64,
    pub precision: f64,
    pub lift: f64,
    pub lift_over_base_rate: f64,
    pub coverage: f64,
    pub false_positive_rate: f64,
    pub precision_at_10: f64,
    pub precision_at_50: f64,
    pub cost_adjusted_return: f64,
    pub max_drawdown: f64,
    pub turnover: f64,
    pub yearly_results: BTreeMap<String, BTreeMap<String, Value>>,
    pub regime_results: BTreeMap<String, BTreeMap<String, Value>>,
    pub top_stock_contribution: f64,
    pub top_period_contribution: f64,
    pub mean_excess_return: f64,
    pub median_excess_return: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub max_losing_streak: i64,
    pub capacity_estimate: f64,
    pub cluster_stability: Option<f64>,
    pub calibration_error: f64,
    pub majority_windows_positive_lift: bool,
    pub baseline_comparison: BTreeMap<String, f64>,
    pub release_gate_passed: bool,
    pub candidate_status: CandidateStatus,
}

impl ValidationPayload {
    pub fn from_value(value: Value) -> Result<Self> {
        let validation: Self = serde_json::from_value(value)?;
        validation.validate()?;
        Ok(validation)
    }

    fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("effective_sample_count", self.effective_sample_count),
            ("base_rate", self.base_rate),
            ("precision", self.precision),
            ("lift", self.lift),
            ("lift_over_base_rate", self.lift_over_base_rate),
            ("coverage", self.coverage),
            ("false_positive_rate", self.false_positive_rate),
            ("precision_at_10", self.precision_at_10),
            ("precision_at_50", self.precision_at_50),
            ("cost_adjusted_return", self.cost_adjusted_return),
            ("max_drawdown", self.max_drawdown),
            ("turnover", self.turnover),
            ("top_stock_contribution", self.top_stock_contribution),
            ("top_period_contribution", self.top_period_contribution),
            ("mean_excess_return", self.mean_excess_return),
            ("median_excess_return", self.median_excess_return),
            ("win_rate", self.win_rate),
            ("profit_factor", self.profit_factor),
            ("capacity_estimate", self.capacity_estimate),
            ("calibration_error", self.calibration_error),
        ] {
            if !value.is_finite() {
                return Err(AppError::Internal(format!(
                    "validation field {} must be finite",
                    field
                )));
            }
        }
        if let Some(cluster_stability) = self.cluster_stability {
            if !cluster_stability.is_finite() {
                return Err(AppError::Internal(
                    "validation field cluster_stability must be finite".to_string(),
                ));
            }
        }
        validate_nested_numeric("yearly_results", &self.yearly_results)?;
        validate_nested_numeric("regime_results", &self.regime_results)?;
        Ok(())
    }
}

fn validate_nested_numeric(
    field_name: &str,
    payload: &BTreeMap<String, BTreeMap<String, Value>>,
) -> Result<()> {
    for (group, metrics) in payload {
        for (metric, value) in metrics {
            match value.as_f64() {
                Some(number) if number.is_finite() => {}
                _ => {
                    return Err(AppError::Internal(format!(
                        "{}.{}.{} must be numeric and finite",
                        field_name, group, metric
                    )));
                }
            }
        }
    }
    Ok(())
}
