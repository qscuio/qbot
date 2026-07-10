use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::explanation::{evaluate_condition, ConditionEvaluation};
pub use super::model::FeatureVector;
use super::model::{CandidateStatus, DistanceMetric, PatternModelPayload, ValidationPayload};
use super::ranking::{final_score, rank_candidate, ShadowTier};
use crate::analysis::market_snapshot::SecurityDailyStatus;
use crate::data::types::Candle;
use crate::error::{AppError, Result};
use crate::storage::market_repository::{MarketRepository, PointInTimeDailyBarVersion};
use crate::storage::pattern_repository::{
    PatternRepository, PatternVersionRow, ShadowCandidateRow,
};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Invalidation {
    pub reason: String,
    pub feature: Option<String>,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SimilarityScore {
    pub distance_metric: String,
    pub distance: f64,
    pub similarity: f64,
    pub scaled_features: FeatureVector,
    pub scaled_centroid: FeatureVector,
    pub feature_contributions: FeatureVector,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternEvaluation {
    pub similarity_score: f64,
    pub validated_lift: f64,
    pub final_score: f64,
    pub shadow_tier: ShadowTier,
    pub matched_features: Value,
    pub risk_flags: Value,
    pub supporting_signals: Value,
    pub invalidations: Vec<Invalidation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternCandidate {
    pub trade_date: NaiveDate,
    pub code: String,
    pub horizon: String,
    pub pattern_version_id: Uuid,
    pub pattern_set_id: Uuid,
    pub pattern_type: String,
    pub similarity_score: f64,
    pub validated_lift: f64,
    pub final_score: f64,
    pub shadow_tier: ShadowTier,
    pub matched_features: Value,
    pub risk_flags: Value,
    pub supporting_signals: Value,
    pub invalidations: Value,
    pub input_fingerprint: String,
}

impl PatternCandidate {
    fn to_shadow_row(&self, created_at: DateTime<Utc>) -> ShadowCandidateRow {
        ShadowCandidateRow {
            trade_date: self.trade_date,
            code: self.code.clone(),
            horizon: self.horizon.clone(),
            pattern_version_id: self.pattern_version_id,
            pattern_set_id: self.pattern_set_id,
            pattern_type: self.pattern_type.clone(),
            similarity_score: self.similarity_score,
            validated_lift: self.validated_lift,
            final_score: self.final_score,
            shadow_tier: self.shadow_tier.as_str().to_string(),
            matched_features: self.matched_features.clone(),
            risk_flags: self.risk_flags.clone(),
            supporting_signals: self.supporting_signals.clone(),
            invalidations: self.invalidations.clone(),
            input_fingerprint: self.input_fingerprint.clone(),
            created_at,
        }
    }
}

pub struct PatternEngine {
    pattern_repo: PatternRepository,
    market_repo: MarketRepository,
}

impl PatternEngine {
    pub fn new(pattern_repo: PatternRepository, market_repo: MarketRepository) -> Self {
        Self {
            pattern_repo,
            market_repo,
        }
    }

    pub async fn match_market(
        &self,
        trade_date: NaiveDate,
        pattern_set_id: Uuid,
    ) -> Result<Vec<PatternCandidate>> {
        let pattern_rows = self
            .pattern_repo
            .list_published_patterns(pattern_set_id)
            .await?;
        let patterns = load_published_patterns(pattern_rows)?;
        if patterns.is_empty() {
            return Ok(Vec::new());
        }

        let as_of = trade_date_cutoff(trade_date);
        let lookback = required_lookback(&patterns).max(1);
        let history_rows = self
            .market_repo
            .daily_bar_history_as_of(trade_date, as_of, lookback as i64)
            .await?;
        let statuses = self
            .market_repo
            .security_status_universe_as_of(trade_date, as_of)
            .await?;

        let histories = histories_by_code(history_rows);
        let status_by_code: BTreeMap<String, SecurityDailyStatus> = statuses
            .into_iter()
            .map(|status| (status.code.clone(), status))
            .collect();
        let required_windows = required_feature_windows(&patterns);
        let market_context =
            MarketFeatureContext::from_histories(&histories, trade_date, &required_windows);

        let mut codes: BTreeSet<String> = status_by_code.keys().cloned().collect();
        codes.extend(histories.keys().cloned());

        let mut candidates = Vec::new();
        for code in codes {
            let history = histories.get(&code).map(Vec::as_slice).unwrap_or(&[]);
            let status = status_by_code.get(&code);
            for pattern in &patterns {
                let (features, mut derivation_invalidations) = derive_feature_vector(
                    &pattern.model.required_features,
                    history,
                    &market_context,
                );
                derivation_invalidations.extend(status_invalidations(status));
                let mut evaluation =
                    evaluate_pattern(&pattern.model, &pattern.validation, &features);
                evaluation.invalidations.extend(derivation_invalidations);
                if !evaluation.invalidations.is_empty() {
                    evaluation.shadow_tier = ShadowTier::Reject;
                    evaluation.final_score = 0.0;
                    set_supporting_shadow_tier(
                        &mut evaluation.supporting_signals,
                        ShadowTier::Reject,
                    );
                }

                let invalidations_json = serde_json::to_value(&evaluation.invalidations)?;
                let input_fingerprint = input_fingerprint(
                    trade_date,
                    &code,
                    pattern.pattern_version_id,
                    pattern_set_id,
                    &pattern.horizon,
                    &pattern.pattern_type,
                    &features,
                );
                candidates.push(PatternCandidate {
                    trade_date,
                    code: code.clone(),
                    horizon: pattern.horizon.clone(),
                    pattern_version_id: pattern.pattern_version_id,
                    pattern_set_id,
                    pattern_type: pattern.pattern_type.clone(),
                    similarity_score: evaluation.similarity_score,
                    validated_lift: evaluation.validated_lift,
                    final_score: evaluation.final_score,
                    shadow_tier: evaluation.shadow_tier,
                    matched_features: evaluation.matched_features,
                    risk_flags: evaluation.risk_flags,
                    supporting_signals: evaluation.supporting_signals,
                    invalidations: invalidations_json,
                    input_fingerprint,
                });
            }
        }

        candidates.sort_by(|left, right| {
            right
                .final_score
                .total_cmp(&left.final_score)
                .then_with(|| left.code.cmp(&right.code))
                .then_with(|| left.horizon.cmp(&right.horizon))
                .then_with(|| left.pattern_version_id.cmp(&right.pattern_version_id))
        });
        let created_at = Utc::now();
        let rows: Vec<ShadowCandidateRow> = candidates
            .iter()
            .map(|candidate| candidate.to_shadow_row(created_at))
            .collect();
        self.pattern_repo.upsert_shadow_candidates(&rows).await?;
        Ok(candidates)
    }
}

struct PublishedPattern {
    pattern_version_id: Uuid,
    horizon: String,
    pattern_type: String,
    model: PatternModelPayload,
    validation: ValidationPayload,
}

fn load_published_patterns(rows: Vec<PatternVersionRow>) -> Result<Vec<PublishedPattern>> {
    rows.into_iter()
        .map(|row| {
            Ok(PublishedPattern {
                pattern_version_id: row.pattern_version_id,
                horizon: row.horizon,
                pattern_type: row.pattern_type,
                model: PatternModelPayload::from_value(row.model_payload)?,
                validation: ValidationPayload::from_value(row.validation_payload)?,
            })
        })
        .collect()
}

pub fn similarity(
    model: &PatternModelPayload,
    features: &FeatureVector,
) -> Result<SimilarityScore> {
    if model.distance_metric != DistanceMetric::Euclidean {
        return Err(AppError::Internal(format!(
            "unsupported distance_metric for Rust matcher: {}",
            model.distance_metric.as_str()
        )));
    }

    let mut squared_distance = 0.0;
    let mut scaled_features = FeatureVector::new();
    let mut scaled_centroid = FeatureVector::new();
    let mut feature_contributions = FeatureVector::new();
    for feature in &model.required_features {
        let value = *features.get(feature).ok_or_else(|| {
            AppError::Internal(format!("missing required feature value: {}", feature))
        })?;
        if !value.is_finite() {
            return Err(AppError::Internal(format!(
                "feature value must be finite: {}",
                feature
            )));
        }
        let mean = model.scaler_mean[feature];
        let scale = model.scaler_scale[feature];
        let centroid = model.centroid[feature];
        let scaled_value = (value - mean) / scale;
        let scaled_center = (centroid - mean) / scale;
        let contribution = (scaled_value - scaled_center).powi(2);
        squared_distance += contribution;
        scaled_features.insert(feature.clone(), scaled_value);
        scaled_centroid.insert(feature.clone(), scaled_center);
        feature_contributions.insert(feature.clone(), contribution);
    }

    let distance = squared_distance.sqrt();
    Ok(SimilarityScore {
        distance_metric: model.distance_metric.as_str().to_string(),
        distance,
        similarity: 1.0 / (1.0 + distance),
        scaled_features,
        scaled_centroid,
        feature_contributions,
    })
}

pub fn evaluate_pattern(
    model: &PatternModelPayload,
    validation: &ValidationPayload,
    features: &FeatureVector,
) -> PatternEvaluation {
    let mut invalidations = Vec::new();
    for feature in &model.required_features {
        if !features.contains_key(feature) {
            invalidations.push(Invalidation {
                reason: "missing_required_feature".to_string(),
                feature: Some(feature.clone()),
                detail: format!("feature {} was not derived for candidate", feature),
            });
        }
    }
    if !model.similarity_thresholds.contains_key("shadow_a") {
        invalidations.push(Invalidation {
            reason: "missing_similarity_threshold".to_string(),
            feature: Some("shadow_a".to_string()),
            detail: "model similarity_thresholds must include shadow_a".to_string(),
        });
    }
    if !model.similarity_thresholds.contains_key("shadow_b") {
        invalidations.push(Invalidation {
            reason: "missing_similarity_threshold".to_string(),
            feature: Some("shadow_b".to_string()),
            detail: "model similarity_thresholds must include shadow_b".to_string(),
        });
    }
    if model.distance_metric != DistanceMetric::Euclidean {
        invalidations.push(Invalidation {
            reason: "unsupported_distance_metric".to_string(),
            feature: None,
            detail: format!(
                "distance_metric {} is not supported by the Rust matcher",
                model.distance_metric.as_str()
            ),
        });
    }

    let necessary_conditions = evaluate_conditions(&model.necessary_conditions, features);
    for condition in &necessary_conditions {
        match condition.passed {
            Some(true) => {}
            Some(false) => invalidations.push(Invalidation {
                reason: "necessary_condition_failed".to_string(),
                feature: condition.feature.clone(),
                detail: condition_detail(condition),
            }),
            None => invalidations.push(Invalidation {
                reason: "necessary_condition_not_evaluable".to_string(),
                feature: condition.feature.clone(),
                detail: condition_detail(condition),
            }),
        }
    }
    let risk_conditions = evaluate_conditions(&model.risk_conditions, features);

    let similarity_result = if invalidations.is_empty() {
        similarity(model, features).ok()
    } else {
        None
    };
    let similarity_score = similarity_result
        .as_ref()
        .map(|score| score.similarity)
        .unwrap_or(0.0);
    let final_score = if invalidations.is_empty() {
        final_score(similarity_score, validation.lift)
    } else {
        0.0
    };
    let shadow_tier = match (
        model.similarity_thresholds.get("shadow_a").copied(),
        model.similarity_thresholds.get("shadow_b").copied(),
    ) {
        (Some(shadow_a_threshold), Some(shadow_b_threshold)) => rank_candidate(
            similarity_score,
            validation,
            shadow_a_threshold,
            shadow_b_threshold,
            !invalidations.is_empty(),
        ),
        _ => ShadowTier::Reject,
    };

    PatternEvaluation {
        similarity_score,
        validated_lift: validation.lift,
        final_score,
        shadow_tier,
        matched_features: matched_features_payload(features, similarity_result.as_ref()),
        risk_flags: risk_flags_payload(&risk_conditions),
        supporting_signals: supporting_signals_payload(
            validation,
            &necessary_conditions,
            similarity_result.as_ref(),
            shadow_tier,
        ),
        invalidations,
    }
}

fn evaluate_conditions(
    conditions: &[super::model::ConditionPayload],
    features: &FeatureVector,
) -> Vec<ConditionEvaluation> {
    conditions
        .iter()
        .map(|condition| evaluate_condition(condition, features))
        .collect()
}

fn matched_features_payload(features: &FeatureVector, score: Option<&SimilarityScore>) -> Value {
    match score {
        Some(score) => json!({
            "raw": features,
            "scaled": score.scaled_features,
            "scaled_centroid": score.scaled_centroid,
            "feature_contributions": score.feature_contributions,
            "distance_metric": score.distance_metric,
            "distance": score.distance,
            "similarity": score.similarity,
        }),
        None => json!({
            "raw": features,
        }),
    }
}

fn risk_flags_payload(risk_conditions: &[ConditionEvaluation]) -> Value {
    let triggered: Vec<&ConditionEvaluation> = risk_conditions
        .iter()
        .filter(|condition| condition.passed == Some(true))
        .collect();
    json!({
        "conditions": risk_conditions,
        "triggered": triggered,
    })
}

fn supporting_signals_payload(
    validation: &ValidationPayload,
    necessary_conditions: &[ConditionEvaluation],
    score: Option<&SimilarityScore>,
    shadow_tier: ShadowTier,
) -> Value {
    json!({
        "necessary_conditions": necessary_conditions,
        "score_components": {
            "similarity": score.map(|score| score.similarity),
            "validated_lift": validation.lift,
            "release_gate_passed": validation.release_gate_passed,
            "candidate_status": match validation.candidate_status {
                CandidateStatus::Draft => "draft",
                CandidateStatus::Validated => "validated",
            },
            "shadow_tier": shadow_tier.as_str(),
        }
    })
}

fn set_supporting_shadow_tier(payload: &mut Value, shadow_tier: ShadowTier) {
    if let Some(score_components) = payload
        .as_object_mut()
        .and_then(|payload| payload.get_mut("score_components"))
        .and_then(Value::as_object_mut)
    {
        score_components.insert(
            "shadow_tier".to_string(),
            Value::String(shadow_tier.as_str().to_string()),
        );
    }
}

fn condition_detail(condition: &ConditionEvaluation) -> String {
    format!(
        "status={}, feature={:?}, operator={:?}, threshold={:?}, actual={:?}",
        condition.status,
        condition.feature,
        condition.operator,
        condition.threshold,
        condition.actual
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SupportedFeature {
    Return(usize),
    RelativeStrength(usize),
}

impl SupportedFeature {
    fn parse(feature: &str) -> Option<Self> {
        parse_window_feature(feature, "return_")
            .map(Self::Return)
            .or_else(|| {
                parse_window_feature(feature, "relative_strength_").map(Self::RelativeStrength)
            })
    }

    fn window(self) -> usize {
        match self {
            Self::Return(window) | Self::RelativeStrength(window) => window,
        }
    }
}

fn parse_window_feature(feature: &str, prefix: &str) -> Option<usize> {
    feature
        .strip_prefix(prefix)?
        .strip_suffix('d')?
        .parse::<usize>()
        .ok()
        .filter(|window| *window > 0)
}

fn required_lookback(patterns: &[PublishedPattern]) -> usize {
    required_feature_windows(patterns)
        .into_iter()
        .map(|window| window + 1)
        .max()
        .unwrap_or(1)
}

fn required_feature_windows(patterns: &[PublishedPattern]) -> BTreeSet<usize> {
    patterns
        .iter()
        .flat_map(|pattern| pattern.model.required_features.iter())
        .filter_map(|feature| SupportedFeature::parse(feature))
        .map(SupportedFeature::window)
        .collect()
}

#[derive(Debug, Default)]
struct MarketFeatureContext {
    median_return_ratio_by_window: BTreeMap<usize, f64>,
}

impl MarketFeatureContext {
    fn from_histories(
        histories: &BTreeMap<String, Vec<PointInTimeDailyBarVersion>>,
        trade_date: NaiveDate,
        required_windows: &BTreeSet<usize>,
    ) -> Self {
        let mut median_return_ratio_by_window = BTreeMap::new();
        for window in required_windows {
            let mut ratios: Vec<f64> = histories
                .values()
                .filter_map(|rows| return_ratio(rows, trade_date, *window).ok())
                .filter(|ratio| ratio.is_finite() && *ratio > 0.0)
                .collect();
            ratios.sort_by(|left, right| left.total_cmp(right));
            if let Some(median) = median(&ratios) {
                median_return_ratio_by_window.insert(*window, median);
            }
        }
        Self {
            median_return_ratio_by_window,
        }
    }
}

fn derive_feature_vector(
    required_features: &[String],
    history: &[PointInTimeDailyBarVersion],
    market_context: &MarketFeatureContext,
) -> (FeatureVector, Vec<Invalidation>) {
    let mut features = FeatureVector::new();
    let mut invalidations = Vec::new();
    let trade_date = history.last().map(|row| row.trade_date);

    for feature in required_features {
        match SupportedFeature::parse(feature) {
            Some(SupportedFeature::Return(window)) => {
                match trade_date.and_then(|date| stock_return(history, date, window).ok()) {
                    Some(value) => {
                        features.insert(feature.clone(), value);
                    }
                    None => invalidations.push(Invalidation {
                        reason: "insufficient_bar_history".to_string(),
                        feature: Some(feature.clone()),
                        detail: format!("{} requires {}+1 valid daily bars", feature, window),
                    }),
                }
            }
            Some(SupportedFeature::RelativeStrength(window)) => {
                let stock_ratio =
                    trade_date.and_then(|date| return_ratio(history, date, window).ok());
                let market_ratio = market_context
                    .median_return_ratio_by_window
                    .get(&window)
                    .copied();
                match (stock_ratio, market_ratio) {
                    (Some(stock_ratio), Some(market_ratio))
                        if stock_ratio.is_finite()
                            && market_ratio.is_finite()
                            && market_ratio != 0.0 =>
                    {
                        features.insert(feature.clone(), stock_ratio / market_ratio);
                    }
                    (Some(_), None) => invalidations.push(Invalidation {
                        reason: "missing_market_relative_strength_baseline".to_string(),
                        feature: Some(feature.clone()),
                        detail: format!("{} requires market median return ratio", feature),
                    }),
                    _ => invalidations.push(Invalidation {
                        reason: "insufficient_bar_history".to_string(),
                        feature: Some(feature.clone()),
                        detail: format!("{} requires {}+1 valid daily bars", feature, window),
                    }),
                }
            }
            None => invalidations.push(Invalidation {
                reason: "unsupported_required_feature".to_string(),
                feature: Some(feature.clone()),
                detail: format!(
                    "feature {} cannot be derived from daily bar history",
                    feature
                ),
            }),
        }
    }
    (features, invalidations)
}

fn stock_return(
    history: &[PointInTimeDailyBarVersion],
    trade_date: NaiveDate,
    window: usize,
) -> std::result::Result<f64, ()> {
    Ok(return_ratio(history, trade_date, window)? - 1.0)
}

fn return_ratio(
    history: &[PointInTimeDailyBarVersion],
    trade_date: NaiveDate,
    window: usize,
) -> std::result::Result<f64, ()> {
    if history.len() <= window {
        return Err(());
    }
    let latest = history.last().ok_or(())?;
    if latest.trade_date != trade_date {
        return Err(());
    }
    let current_close = valid_close(latest)?;
    let previous_close = valid_close(&history[history.len() - 1 - window])?;
    if previous_close == 0.0 {
        return Err(());
    }
    let ratio = current_close / previous_close;
    if ratio.is_finite() {
        Ok(ratio)
    } else {
        Err(())
    }
}

fn valid_close(row: &PointInTimeDailyBarVersion) -> std::result::Result<f64, ()> {
    let Candle { close, .. } = row.bar.as_ref().ok_or(())?;
    if close.is_finite() && *close > 0.0 {
        Ok(*close)
    } else {
        Err(())
    }
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let midpoint = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[midpoint - 1] + values[midpoint]) / 2.0)
    } else {
        Some(values[midpoint])
    }
}

fn histories_by_code(
    rows: Vec<PointInTimeDailyBarVersion>,
) -> BTreeMap<String, Vec<PointInTimeDailyBarVersion>> {
    let mut histories: BTreeMap<String, Vec<PointInTimeDailyBarVersion>> = BTreeMap::new();
    for row in rows {
        histories.entry(row.code.clone()).or_default().push(row);
    }
    histories
}

fn status_invalidations(status: Option<&SecurityDailyStatus>) -> Vec<Invalidation> {
    let Some(status) = status else {
        return vec![Invalidation {
            reason: "missing_security_status".to_string(),
            feature: None,
            detail: "security status was not available as of the trade date cutoff".to_string(),
        }];
    };
    let mut invalidations = Vec::new();
    if status.is_suspended {
        invalidations.push(Invalidation {
            reason: "suspended_security".to_string(),
            feature: None,
            detail: "security was suspended on the trade date".to_string(),
        });
    }
    if status.is_st {
        invalidations.push(Invalidation {
            reason: "st_security".to_string(),
            feature: None,
            detail: "security had ST status on the trade date".to_string(),
        });
    }
    invalidations
}

fn input_fingerprint(
    trade_date: NaiveDate,
    code: &str,
    pattern_version_id: Uuid,
    pattern_set_id: Uuid,
    horizon: &str,
    pattern_type: &str,
    features: &FeatureVector,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("trade_date={}\n", trade_date));
    hasher.update(format!("code={}\n", code));
    hasher.update(format!("pattern_version_id={}\n", pattern_version_id));
    hasher.update(format!("pattern_set_id={}\n", pattern_set_id));
    hasher.update(format!("horizon={}\n", horizon));
    hasher.update(format!("pattern_type={}\n", pattern_type));
    for (feature, value) in features {
        hasher.update(format!("feature:{}={:.17}\n", feature, value));
    }
    format!("{:x}", hasher.finalize())
}

fn trade_date_cutoff(trade_date: NaiveDate) -> DateTime<Utc> {
    let cutoff = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999)
        .expect("23:59:59.999999999 is a valid time");
    DateTime::<Utc>::from_naive_utc_and_offset(trade_date.and_time(cutoff), Utc)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::super::model::{PatternModelPayload, ValidationPayload};
    use super::{evaluate_pattern, similarity, FeatureVector};
    use crate::analysis::patterns::ranking::ShadowTier;

    fn fixture_payload() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../tests/fixtures/pattern_model_v1.json"
        ))
        .unwrap()
    }

    fn validation_payload(release_gate_passed: bool, lift: f64) -> ValidationPayload {
        serde_json::from_value(json!({
            "candidate_id": "trend:kmeans:k2:c0",
            "positive_sample_count": 12,
            "control_sample_count": 18,
            "effective_sample_count": 8.0,
            "base_rate": 0.40,
            "precision": 0.75,
            "lift": lift,
            "lift_over_base_rate": lift,
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
            "capacity_estimate": 1000000.0,
            "cluster_stability": 0.86,
            "calibration_error": 0.05,
            "majority_windows_positive_lift": true,
            "baseline_comparison": {
                "best_required_baseline_return": 0.01,
                "cost_adjusted_return_delta": 0.022
            },
            "release_gate_passed": release_gate_passed,
            "candidate_status": "validated"
        }))
        .unwrap()
    }

    #[test]
    fn fixture_model_payload_loads() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();

        assert_eq!(
            model.required_features,
            vec!["return_20d", "relative_strength_20d"]
        );
        assert_eq!(model.distance_metric.as_str(), "euclidean");
    }

    #[test]
    fn unknown_model_payload_field_rejects() {
        let mut payload = fixture_payload();
        payload["schema"] = json!("unexpected-v2");

        let error = PatternModelPayload::from_value(payload).unwrap_err();

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn unknown_distance_metric_rejects() {
        let mut payload = fixture_payload();
        payload["distance_metric"] = json!("unknown_metric");

        let error = PatternModelPayload::from_value(payload).unwrap_err();

        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn missing_required_feature_rejects() {
        let mut payload = fixture_payload();
        payload["scaler_scale"]
            .as_object_mut()
            .unwrap()
            .remove("relative_strength_20d");

        let error = PatternModelPayload::from_value(payload).unwrap_err();

        assert!(error.to_string().contains("required_features"));
    }

    #[test]
    fn fixed_feature_vector_produces_fixed_similarity() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = FeatureVector::from([
            ("return_20d".to_string(), 0.15),
            ("relative_strength_20d".to_string(), 1.10),
        ]);

        let score = similarity(&model, &features).unwrap();

        assert!((score.similarity - 0.309_016_994_374_947_4).abs() < 1e-12);
    }

    #[test]
    fn high_similarity_without_release_gate_is_not_shadow_a() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = FeatureVector::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
        ]);
        let validation = validation_payload(false, 2.0);

        let candidate = evaluate_pattern(&model, &validation, &features);

        assert_ne!(candidate.shadow_tier, ShadowTier::ShadowA);
        assert_eq!(candidate.shadow_tier, ShadowTier::ShadowB);
    }

    #[test]
    fn high_similarity_with_inadequate_lift_is_not_shadow_a() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = FeatureVector::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
        ]);
        let validation = validation_payload(true, 1.0);

        let candidate = evaluate_pattern(&model, &validation, &features);

        assert_ne!(candidate.shadow_tier, ShadowTier::ShadowA);
        assert_eq!(candidate.shadow_tier, ShadowTier::ShadowB);
    }

    #[test]
    fn unsupported_required_feature_invalidates_candidate() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = BTreeMap::from([("return_20d".to_string(), 0.20)]);
        let validation = validation_payload(true, 2.0);

        let candidate = evaluate_pattern(&model, &validation, &features);

        assert_eq!(candidate.shadow_tier, ShadowTier::Reject);
        assert!(candidate
            .invalidations
            .iter()
            .any(|invalidation| invalidation.reason == "missing_required_feature"));
    }
}
