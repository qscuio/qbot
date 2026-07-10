use serde::Serialize;
use serde_json::{json, Value};

use super::model::{ConditionPayload, FeatureVector};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConditionEvaluation {
    pub condition: Value,
    pub feature: Option<String>,
    pub operator: Option<String>,
    pub threshold: Option<f64>,
    pub actual: Option<f64>,
    pub passed: Option<bool>,
    pub status: String,
}

pub fn evaluate_condition(
    condition: &ConditionPayload,
    features: &FeatureVector,
) -> ConditionEvaluation {
    let condition_value = json!(condition);
    let feature = condition
        .get("column")
        .or_else(|| condition.get("field"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let operator = condition
        .get("operator")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let threshold = condition.get("value").and_then(Value::as_f64);

    let Some(feature_name) = feature else {
        return ConditionEvaluation {
            condition: condition_value,
            feature: None,
            operator,
            threshold,
            actual: None,
            passed: None,
            status: "unsupported_condition".to_string(),
        };
    };
    let Some(actual) = features.get(&feature_name).copied() else {
        return ConditionEvaluation {
            condition: condition_value,
            feature: Some(feature_name),
            operator,
            threshold,
            actual: None,
            passed: None,
            status: "missing_feature".to_string(),
        };
    };
    let Some(operator_value) = operator.as_deref() else {
        return ConditionEvaluation {
            condition: condition_value,
            feature: Some(feature_name),
            operator,
            threshold,
            actual: Some(actual),
            passed: None,
            status: "unsupported_operator".to_string(),
        };
    };
    let Some(threshold_value) = threshold else {
        return ConditionEvaluation {
            condition: condition_value,
            feature: Some(feature_name),
            operator,
            threshold,
            actual: Some(actual),
            passed: None,
            status: "non_numeric_value".to_string(),
        };
    };

    let passed = match operator_value {
        ">=" | "gte" => actual >= threshold_value,
        ">" | "gt" => actual > threshold_value,
        "<=" | "lte" => actual <= threshold_value,
        "<" | "lt" => actual < threshold_value,
        "==" | "=" | "eq" => (actual - threshold_value).abs() <= f64::EPSILON,
        "!=" | "ne" => (actual - threshold_value).abs() > f64::EPSILON,
        _ => {
            return ConditionEvaluation {
                condition: condition_value,
                feature: Some(feature_name),
                operator,
                threshold,
                actual: Some(actual),
                passed: None,
                status: "unsupported_operator".to_string(),
            }
        }
    };

    ConditionEvaluation {
        condition: condition_value,
        feature: Some(feature_name),
        operator,
        threshold,
        actual: Some(actual),
        passed: Some(passed),
        status: "evaluated".to_string(),
    }
}
