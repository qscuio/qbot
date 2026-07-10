use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::explanation::{evaluate_condition, ConditionEvaluation};
pub use super::model::FeatureVector;
use super::model::{
    mahalanobis_distance_squared, CandidateStatus, DistanceMetric, PatternModelPayload,
    ValidationPayload,
};
use super::ranking::{final_score, rank_candidate, ScoreComponents, ShadowTier};
use crate::analysis::market_snapshot::adjustment::adjust_candles;
use crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION;
use crate::analysis::market_snapshot::{
    AdjustmentFactor, AvailabilityQuality, DailyBasicSnapshot, MarketSnapshot, SectorMembership,
    SecurityDailyStatus, SecurityMasterVersion,
};
use crate::data::types::Candle;
use crate::error::{AppError, Result};
use crate::storage::market_repository::{MarketRepository, PointInTimeDailyBarVersion};
use crate::storage::pattern_repository::{
    PatternRepository, PatternVersionRow, ShadowCandidateRow,
};

const SUPPORTED_PATTERN_SCHEMA_VERSION: &str = "1";
const RISK_TRIGGER_SCORE_MULTIPLIER: f64 = 0.5;

#[async_trait]
pub(crate) trait PatternStore: Send + Sync {
    async fn list_published_patterns(&self, pattern_set_id: Uuid)
        -> Result<Vec<PatternVersionRow>>;

    async fn upsert_shadow_candidates(&self, rows: &[ShadowCandidateRow]) -> Result<usize>;
}

#[async_trait]
impl PatternStore for PatternRepository {
    async fn list_published_patterns(
        &self,
        pattern_set_id: Uuid,
    ) -> Result<Vec<PatternVersionRow>> {
        PatternRepository::list_published_patterns(self, pattern_set_id).await
    }

    async fn upsert_shadow_candidates(&self, rows: &[ShadowCandidateRow]) -> Result<usize> {
        PatternRepository::upsert_shadow_candidates(self, rows).await
    }
}

#[async_trait]
pub(crate) trait MarketSource: Send + Sync {
    async fn daily_bar_history_as_of(
        &self,
        end: NaiveDate,
        as_of: DateTime<Utc>,
        lookback: i64,
    ) -> Result<Vec<PointInTimeDailyBarVersion>>;

    async fn security_status_universe_as_of(
        &self,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SecurityDailyStatus>>;

    async fn adjustment_factors_as_of(
        &self,
        codes: &[String],
        start: NaiveDate,
        end: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<AdjustmentFactor>>;

    async fn security_master(
        &self,
        code: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<SecurityMasterVersion>>;

    async fn active_sector_memberships(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SectorMembership>>;

    async fn market_snapshot(
        &self,
        trade_date: NaiveDate,
        version: &str,
    ) -> Result<Option<MarketSnapshot>>;

    async fn daily_basic(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Option<DailyBasicSnapshot>>;
}

#[async_trait]
impl MarketSource for MarketRepository {
    async fn daily_bar_history_as_of(
        &self,
        end: NaiveDate,
        as_of: DateTime<Utc>,
        lookback: i64,
    ) -> Result<Vec<PointInTimeDailyBarVersion>> {
        MarketRepository::daily_bar_history_as_of(self, end, as_of, lookback).await
    }

    async fn security_status_universe_as_of(
        &self,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SecurityDailyStatus>> {
        MarketRepository::security_status_universe_as_of(self, trade_date, as_of).await
    }

    async fn adjustment_factors_as_of(
        &self,
        codes: &[String],
        start: NaiveDate,
        end: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<AdjustmentFactor>> {
        MarketRepository::adjustment_factors_as_of(self, codes, start, end, as_of).await
    }

    async fn security_master(
        &self,
        code: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<SecurityMasterVersion>> {
        MarketRepository::security_master(self, code, as_of).await
    }

    async fn active_sector_memberships(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SectorMembership>> {
        MarketRepository::active_sector_memberships(self, code, trade_date, as_of).await
    }

    async fn market_snapshot(
        &self,
        trade_date: NaiveDate,
        version: &str,
    ) -> Result<Option<MarketSnapshot>> {
        MarketRepository::market_snapshot(self, trade_date, version).await
    }

    async fn daily_basic(
        &self,
        code: &str,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<Option<DailyBasicSnapshot>> {
        MarketRepository::daily_basic(self, code, trade_date, as_of).await
    }
}

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

#[derive(Debug, Clone, Default)]
pub struct ScoreContext {
    pub sector_memberships: Vec<SectorMembership>,
    pub market_snapshot: Option<MarketSnapshot>,
    pub daily_basic: Option<DailyBasicSnapshot>,
    pub current_bar: Option<Candle>,
    pub data_quality_flags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternCandidate {
    pub trade_date: NaiveDate,
    pub code: String,
    pub name: Option<String>,
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
            name: self.name.clone(),
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

pub struct PatternEngine<P = PatternRepository, M = MarketRepository> {
    pattern_repo: P,
    market_repo: M,
}

impl PatternEngine<PatternRepository, MarketRepository> {
    pub fn new(pattern_repo: PatternRepository, market_repo: MarketRepository) -> Self {
        Self {
            pattern_repo,
            market_repo,
        }
    }
}

impl<P, M> PatternEngine<P, M>
where
    P: PatternStore,
    M: MarketSource,
{
    #[cfg(test)]
    fn with_sources(pattern_repo: P, market_repo: M) -> Self {
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
        let market_snapshot = self
            .market_repo
            .market_snapshot(trade_date, MARKET_SNAPSHOT_VERSION)
            .await?;

        let histories = histories_by_code(history_rows);
        let adjustment_rows = if histories.is_empty() {
            Vec::new()
        } else {
            let codes: Vec<String> = histories.keys().cloned().collect();
            let start = histories
                .values()
                .filter_map(|history| history.first().map(|row| row.trade_date))
                .min()
                .unwrap_or(trade_date);
            self.market_repo
                .adjustment_factors_as_of(&codes, start, trade_date, as_of)
                .await?
        };
        let adjustments = adjustments_by_code(adjustment_rows);
        let (adjusted_histories, adjustment_invalidations) =
            adjusted_histories_by_code(&histories, &adjustments);
        let status_by_code: BTreeMap<String, SecurityDailyStatus> = statuses
            .into_iter()
            .map(|status| (status.code.clone(), status))
            .collect();
        let required_windows = required_feature_windows(&patterns);
        let market_context = MarketFeatureContext::from_histories(
            &adjusted_histories,
            trade_date,
            &required_windows,
        );

        let mut codes: BTreeSet<String> = status_by_code.keys().cloned().collect();
        codes.extend(histories.keys().cloned());

        let mut candidates = Vec::new();
        for code in codes {
            let history = adjusted_histories
                .get(&code)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let status = status_by_code.get(&code);
            let security_master = self.market_repo.security_master(&code, as_of).await?;
            let candidate_name = security_master.as_ref().and_then(|security| {
                let trimmed = security.name.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            });
            let sector_memberships = self
                .market_repo
                .active_sector_memberships(&code, trade_date, as_of)
                .await?;
            let daily_basic = self
                .market_repo
                .daily_basic(&code, trade_date, as_of)
                .await?;
            let data_quality_flags = data_quality_flags(
                history,
                status,
                &adjustments.get(&code).cloned().unwrap_or_default(),
                security_master.as_ref(),
                &sector_memberships,
                daily_basic.as_ref(),
            );
            let score_context = ScoreContext {
                sector_memberships,
                market_snapshot: market_snapshot.clone(),
                daily_basic,
                current_bar: history.last().and_then(|row| row.bar.clone()),
                data_quality_flags,
            };
            for pattern in &patterns {
                let (features, mut derivation_invalidations) = derive_feature_vector(
                    &pattern.model.required_features,
                    history,
                    &market_context,
                );
                if let Some(adjustment_invalidation) = adjustment_invalidations.get(&code) {
                    derivation_invalidations.push(adjustment_invalidation.clone());
                }
                derivation_invalidations.extend(status_invalidations(status));
                if candidate_name.is_none() {
                    derivation_invalidations.push(Invalidation {
                        reason: "missing_security_master_name".to_string(),
                        feature: None,
                        detail:
                            "security master name was not available as of the trade date cutoff"
                                .to_string(),
                    });
                }
                let mut evaluation = evaluate_pattern(
                    &pattern.model,
                    &pattern.validation,
                    &features,
                    &score_context,
                );
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
                    name: candidate_name.clone(),
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

#[derive(Debug)]
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
            if row.schema_version != SUPPORTED_PATTERN_SCHEMA_VERSION {
                return Err(AppError::Internal(format!(
                    "unsupported pattern schema_version {} for pattern_version_id {}; supported schema_version is {}",
                    row.schema_version, row.pattern_version_id, SUPPORTED_PATTERN_SCHEMA_VERSION
                )));
            }
            let model = PatternModelPayload::from_value(row.model_payload)?;
            let validation = ValidationPayload::from_value(row.validation_payload)?;
            validate_model_validation_consistency(&model, &validation, row.pattern_version_id)?;
            Ok(PublishedPattern {
                pattern_version_id: row.pattern_version_id,
                horizon: row.horizon,
                pattern_type: row.pattern_type,
                model,
                validation,
            })
        })
        .collect()
}

fn validate_model_validation_consistency(
    model: &PatternModelPayload,
    validation: &ValidationPayload,
    pattern_version_id: Uuid,
) -> Result<()> {
    if !same_f64(model.validation_lift, validation.lift) {
        return Err(AppError::Internal(format!(
            "model_payload.validation_lift must match validation_payload.lift for pattern_version_id {}",
            pattern_version_id
        )));
    }
    if !same_f64(model.validation_coverage, validation.coverage) {
        return Err(AppError::Internal(format!(
            "model_payload.validation_coverage must match validation_payload.coverage for pattern_version_id {}",
            pattern_version_id
        )));
    }
    if model.baseline_comparison != validation.baseline_comparison {
        return Err(AppError::Internal(format!(
            "model_payload.baseline_comparison must match validation_payload.baseline_comparison for pattern_version_id {}",
            pattern_version_id
        )));
    }
    Ok(())
}

fn same_f64(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-12
}

pub fn similarity(
    model: &PatternModelPayload,
    features: &FeatureVector,
) -> Result<SimilarityScore> {
    let comparison_center = match model.distance_metric {
        DistanceMetric::Euclidean | DistanceMetric::Mahalanobis => &model.centroid,
        DistanceMetric::GmmProbability => model
            .cluster_parameters
            .mixture_mean
            .as_ref()
            .ok_or_else(|| {
                AppError::Internal(
                    "cluster_parameters.mixture_mean is required for gmm_probability".to_string(),
                )
            })?,
    };
    let mut scaled_squared_distance = 0.0;
    let mut raw_feature_deltas = Vec::with_capacity(model.required_features.len());
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
        let centroid = *comparison_center.get(feature).ok_or_else(|| {
            AppError::Internal(format!(
                "comparison center must include required feature: {}",
                feature
            ))
        })?;
        let scaled_value = (value - mean) / scale;
        let scaled_center = (centroid - mean) / scale;
        let contribution = (scaled_value - scaled_center).powi(2);
        scaled_squared_distance += contribution;
        raw_feature_deltas.push(value - centroid);
        scaled_features.insert(feature.clone(), scaled_value);
        scaled_centroid.insert(feature.clone(), scaled_center);
        feature_contributions.insert(feature.clone(), contribution);
    }

    let (distance, similarity) = match model.distance_metric {
        DistanceMetric::Euclidean => {
            let distance = scaled_squared_distance.sqrt();
            (distance, 1.0 / (1.0 + distance))
        }
        DistanceMetric::Mahalanobis => {
            let covariance = model
                .cluster_parameters
                .covariance
                .as_ref()
                .ok_or_else(|| {
                    AppError::Internal(
                        "cluster_parameters.covariance is required for mahalanobis".to_string(),
                    )
                })?;
            let distance_squared = mahalanobis_distance_squared(&raw_feature_deltas, covariance)?;
            let distance = distance_squared.sqrt();
            (distance, 1.0 / (1.0 + distance))
        }
        DistanceMetric::GmmProbability => {
            let covariance = model
                .cluster_parameters
                .mixture_covariance
                .as_ref()
                .ok_or_else(|| {
                    AppError::Internal(
                        "cluster_parameters.mixture_covariance is required for gmm_probability"
                            .to_string(),
                    )
                })?;
            let weight = model.cluster_parameters.mixture_weight.ok_or_else(|| {
                AppError::Internal(
                    "cluster_parameters.mixture_weight is required for gmm_probability".to_string(),
                )
            })?;
            let distance_squared = mahalanobis_distance_squared(&raw_feature_deltas, covariance)?;
            let distance = distance_squared.sqrt();
            // Bounded deterministic GMM score for one exported component:
            // mixture weight times the full-covariance Gaussian kernel.
            (distance, weight * (-0.5 * distance_squared).exp())
        }
    };
    Ok(SimilarityScore {
        distance_metric: model.distance_metric.as_str().to_string(),
        distance,
        similarity,
        scaled_features,
        scaled_centroid,
        feature_contributions,
    })
}

pub fn evaluate_pattern(
    model: &PatternModelPayload,
    validation: &ValidationPayload,
    features: &FeatureVector,
    score_context: &ScoreContext,
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
    let risk_multiplier = risk_score_multiplier(&risk_conditions);

    let similarity_result = if invalidations.is_empty() {
        similarity(model, features).ok()
    } else {
        None
    };
    let similarity_score = similarity_result
        .as_ref()
        .map(|score| score.similarity)
        .unwrap_or(0.0);
    let score_evaluation = evaluate_score_components(
        model,
        features,
        score_context,
        similarity_score,
        risk_multiplier,
    );
    let final_score = score_evaluation
        .components
        .as_ref()
        .filter(|_| invalidations.is_empty())
        .map(final_score)
        .unwrap_or(0.0);
    let base_shadow_tier = match (
        model.similarity_thresholds.get("shadow_a").copied(),
        model.similarity_thresholds.get("shadow_b").copied(),
    ) {
        (Some(shadow_a_threshold), Some(shadow_b_threshold)) => rank_candidate(
            similarity_score,
            validation,
            shadow_a_threshold,
            shadow_b_threshold,
            !invalidations.is_empty(),
            score_evaluation.context_complete,
            score_evaluation.market_state_satisfied,
        ),
        _ => ShadowTier::Reject,
    };
    let shadow_tier = apply_risk_tier(base_shadow_tier, &risk_conditions);

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
            &risk_conditions,
            similarity_result.as_ref(),
            &score_evaluation,
            shadow_tier,
        ),
        invalidations,
    }
}

struct ScoreComponentEvaluation {
    components: Option<ScoreComponents>,
    payload: Value,
    context_complete: bool,
    market_state_satisfied: bool,
}

fn evaluate_score_components(
    model: &PatternModelPayload,
    features: &FeatureVector,
    context: &ScoreContext,
    similarity_score: f64,
    risk_multiplier: f64,
) -> ScoreComponentEvaluation {
    let mut missing_components = Vec::new();
    let relative_strength = relative_strength_component(model, features).or_else(|| {
        missing_components.push("relative_strength");
        None
    });
    let sector_confirmation = (!context.sector_memberships.is_empty()).then_some(1.0);
    if sector_confirmation.is_none() {
        missing_components.push("sector_confirmation");
    }
    let market_regime = market_regime_component(context.market_snapshot.as_ref()).or_else(|| {
        missing_components.push("market_regime");
        None
    });
    let extension_penalty = extension_penalty_component(model, features).or_else(|| {
        missing_components.push("extension_penalty");
        None
    });
    let liquidity_penalty = liquidity_penalty_component(context).or_else(|| {
        missing_components.push("liquidity_penalty");
        None
    });
    let data_quality_penalty = data_quality_penalty_component(context).or_else(|| {
        missing_components.push("data_quality_penalty");
        None
    });
    let context_complete = missing_components.is_empty() && context.data_quality_flags.is_empty();
    let market_state_satisfied = market_regime.is_some_and(|value| value >= 0.5);

    let components = match (
        relative_strength,
        sector_confirmation,
        market_regime,
        extension_penalty,
        liquidity_penalty,
        data_quality_penalty,
    ) {
        (
            Some(relative_strength),
            Some(sector_confirmation),
            Some(market_regime),
            Some(extension_penalty),
            Some(liquidity_penalty),
            Some(data_quality_penalty),
        ) => Some(ScoreComponents {
            validated_pattern_strength: model.validation_lift,
            current_similarity: similarity_score,
            relative_strength,
            sector_confirmation,
            market_regime,
            extension_penalty,
            liquidity_penalty,
            data_quality_penalty,
            risk_adjustment: risk_multiplier,
        }),
        _ => None,
    };

    ScoreComponentEvaluation {
        payload: json!({
            "validated_pattern_strength": model.validation_lift,
            "current_similarity": similarity_score,
            "relative_strength": relative_strength,
            "sector_confirmation": sector_confirmation,
            "market_regime": market_regime,
            "extension_penalty": extension_penalty,
            "liquidity_penalty": liquidity_penalty,
            "data_quality_penalty": data_quality_penalty,
            "risk_adjustment": risk_multiplier,
            "context_complete": context_complete,
            "market_state_satisfied": market_state_satisfied,
            "missing_components": missing_components,
            "data_quality_flags": context.data_quality_flags,
        }),
        components,
        context_complete,
        market_state_satisfied,
    }
}

fn relative_strength_component(
    model: &PatternModelPayload,
    features: &FeatureVector,
) -> Option<f64> {
    model
        .required_features
        .iter()
        .find(|feature| feature.starts_with("relative_strength_"))
        .and_then(|feature| features.get(feature))
        .copied()
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn market_regime_component(snapshot: Option<&MarketSnapshot>) -> Option<f64> {
    let snapshot = snapshot?;
    if !snapshot.data_complete || !snapshot.missing_inputs.is_empty() {
        return None;
    }
    let breadth = snapshot.metrics.get("breadth")?;
    let up_count = breadth.get("up_count")?.as_f64()?;
    let down_count = breadth.get("down_count")?.as_f64()?;
    let flat_count = breadth.get("flat_count")?.as_f64()?;
    let above_ma20_count = breadth.get("above_ma20_count")?.as_f64()?;
    let total = up_count + down_count + flat_count;
    if total > 0.0 && above_ma20_count.is_finite() && above_ma20_count <= total {
        Some(above_ma20_count / total)
    } else {
        None
    }
}

fn extension_penalty_component(
    model: &PatternModelPayload,
    features: &FeatureVector,
) -> Option<f64> {
    let value = model
        .required_features
        .iter()
        .find(|feature| feature.starts_with("distance_from_") && feature.ends_with("d_low"))
        .and_then(|feature| features.get(feature))
        .copied()
        .filter(|value| value.is_finite() && *value >= 0.0)?;
    Some(((value - 0.25) / 0.75).clamp(0.0, 1.0))
}

fn liquidity_penalty_component(context: &ScoreContext) -> Option<f64> {
    if let Some(turnover_rate) = context
        .daily_basic
        .as_ref()
        .and_then(|basic| basic.turnover_rate)
        .filter(|value| value.is_finite() && *value >= 0.0)
    {
        return Some((0.5 - turnover_rate).clamp(0.0, 1.0));
    }
    if let Some(volume_ratio) = context
        .daily_basic
        .as_ref()
        .and_then(|basic| basic.volume_ratio)
        .filter(|value| value.is_finite() && *value >= 0.0)
    {
        return Some((1.0 - volume_ratio).clamp(0.0, 1.0));
    }
    context
        .current_bar
        .as_ref()
        .and_then(|bar| (bar.amount.is_finite() && bar.amount > 0.0).then_some(0.0))
}

fn data_quality_penalty_component(context: &ScoreContext) -> Option<f64> {
    let snapshot = context.market_snapshot.as_ref()?;
    if !snapshot.data_complete || !snapshot.missing_inputs.is_empty() {
        return None;
    }
    Some((context.data_quality_flags.len() as f64 * 0.25).clamp(0.0, 1.0))
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
    let unevaluable: Vec<&ConditionEvaluation> = risk_conditions
        .iter()
        .filter(|condition| condition.passed.is_none())
        .collect();
    let risk_adjustment = risk_score_multiplier(risk_conditions);
    json!({
        "conditions": risk_conditions,
        "has_triggered": !triggered.is_empty(),
        "has_unevaluable": !unevaluable.is_empty(),
        "triggered": triggered,
        "unevaluable": unevaluable,
        "risk_adjustment": risk_adjustment,
    })
}

fn supporting_signals_payload(
    validation: &ValidationPayload,
    necessary_conditions: &[ConditionEvaluation],
    risk_conditions: &[ConditionEvaluation],
    score: Option<&SimilarityScore>,
    score_evaluation: &ScoreComponentEvaluation,
    shadow_tier: ShadowTier,
) -> Value {
    let mut score_components = score_evaluation.payload.clone();
    if let Some(score_components) = score_components.as_object_mut() {
        score_components.insert(
            "similarity".to_string(),
            json!(score.map(|score| score.similarity)),
        );
        score_components.insert("validated_lift".to_string(), json!(validation.lift));
        score_components.insert(
            "release_gate_passed".to_string(),
            json!(validation.release_gate_passed),
        );
        score_components.insert(
            "candidate_status".to_string(),
            json!(match validation.candidate_status {
                CandidateStatus::Draft => "draft",
                CandidateStatus::Validated => "validated",
            }),
        );
        score_components.insert("shadow_tier".to_string(), json!(shadow_tier.as_str()));
    }
    json!({
        "necessary_conditions": necessary_conditions,
        "risk_conditions": risk_conditions,
        "score_components": score_components,
    })
}

fn risk_score_multiplier(risk_conditions: &[ConditionEvaluation]) -> f64 {
    if has_triggered_or_unevaluable_risk(risk_conditions) {
        RISK_TRIGGER_SCORE_MULTIPLIER
    } else {
        1.0
    }
}

fn apply_risk_tier(shadow_tier: ShadowTier, risk_conditions: &[ConditionEvaluation]) -> ShadowTier {
    match (
        has_triggered_or_unevaluable_risk(risk_conditions),
        shadow_tier,
    ) {
        (true, ShadowTier::ShadowA | ShadowTier::ShadowB) => ShadowTier::Watch,
        _ => shadow_tier,
    }
}

fn has_triggered_or_unevaluable_risk(risk_conditions: &[ConditionEvaluation]) -> bool {
    risk_conditions
        .iter()
        .any(|condition| condition.passed == Some(true) || condition.passed.is_none())
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
    PriceVsMa50,
    Ma20VsMa50,
    ConsolidationRange(usize),
    VolumeRatio(usize),
    BreakoutReturn5d,
    DistanceFromLow(usize),
    Rsi(usize),
    ReversalReturn5d,
}

impl SupportedFeature {
    fn parse(feature: &str) -> Option<Self> {
        parse_window_feature(feature, "return_")
            .map(Self::Return)
            .or_else(|| {
                parse_window_feature(feature, "relative_strength_").map(Self::RelativeStrength)
            })
            .or_else(|| (feature == "price_vs_ma50").then_some(Self::PriceVsMa50))
            .or_else(|| (feature == "ma20_vs_ma50").then_some(Self::Ma20VsMa50))
            .or_else(|| {
                parse_window_feature(feature, "consolidation_range_").map(Self::ConsolidationRange)
            })
            .or_else(|| parse_window_feature(feature, "volume_ratio_").map(Self::VolumeRatio))
            .or_else(|| (feature == "breakout_return_5d").then_some(Self::BreakoutReturn5d))
            .or_else(|| parse_distance_from_low_feature(feature).map(Self::DistanceFromLow))
            .or_else(|| parse_rsi_feature(feature).map(Self::Rsi))
            .or_else(|| (feature == "reversal_return_5d").then_some(Self::ReversalReturn5d))
    }

    fn window(self) -> usize {
        match self {
            Self::Return(window) | Self::RelativeStrength(window) => window,
            Self::PriceVsMa50 | Self::Ma20VsMa50 => 50,
            Self::ConsolidationRange(window)
            | Self::VolumeRatio(window)
            | Self::DistanceFromLow(window)
            | Self::Rsi(window) => window,
            Self::BreakoutReturn5d | Self::ReversalReturn5d => 5,
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

fn parse_distance_from_low_feature(feature: &str) -> Option<usize> {
    feature
        .strip_prefix("distance_from_")?
        .strip_suffix("d_low")?
        .parse::<usize>()
        .ok()
        .filter(|window| *window > 0)
}

fn parse_rsi_feature(feature: &str) -> Option<usize> {
    feature
        .strip_prefix("rsi_")?
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
            Some(SupportedFeature::PriceVsMa50) => {
                insert_feature_or_invalidate(
                    feature,
                    moving_average_ratio(history, trade_date, 50),
                    &mut features,
                    &mut invalidations,
                );
            }
            Some(SupportedFeature::Ma20VsMa50) => {
                insert_feature_or_invalidate(
                    feature,
                    moving_average_cross_ratio(history, trade_date, 20, 50),
                    &mut features,
                    &mut invalidations,
                );
            }
            Some(SupportedFeature::ConsolidationRange(window)) => {
                insert_feature_or_invalidate(
                    feature,
                    consolidation_range(history, trade_date, window),
                    &mut features,
                    &mut invalidations,
                );
            }
            Some(SupportedFeature::VolumeRatio(window)) => {
                insert_feature_or_invalidate(
                    feature,
                    volume_ratio(history, trade_date, window),
                    &mut features,
                    &mut invalidations,
                );
            }
            Some(SupportedFeature::BreakoutReturn5d) => {
                insert_feature_or_invalidate(
                    feature,
                    trade_date.and_then(|date| stock_return(history, date, 5).ok()),
                    &mut features,
                    &mut invalidations,
                );
            }
            Some(SupportedFeature::DistanceFromLow(window)) => {
                insert_feature_or_invalidate(
                    feature,
                    distance_from_low(history, trade_date, window),
                    &mut features,
                    &mut invalidations,
                );
            }
            Some(SupportedFeature::Rsi(window)) => {
                insert_feature_or_invalidate(
                    feature,
                    rsi(history, trade_date, window),
                    &mut features,
                    &mut invalidations,
                );
            }
            Some(SupportedFeature::ReversalReturn5d) => {
                insert_feature_or_invalidate(
                    feature,
                    trade_date.and_then(|date| stock_return(history, date, 5).ok()),
                    &mut features,
                    &mut invalidations,
                );
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

fn insert_feature_or_invalidate(
    feature: &str,
    value: Option<f64>,
    features: &mut FeatureVector,
    invalidations: &mut Vec<Invalidation>,
) {
    match value {
        Some(value) if value.is_finite() => {
            features.insert(feature.to_string(), value);
        }
        _ => invalidations.push(Invalidation {
            reason: "insufficient_bar_history".to_string(),
            feature: Some(feature.to_string()),
            detail: format!("{} requires complete valid PIT daily bars", feature),
        }),
    }
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

fn moving_average_ratio(
    history: &[PointInTimeDailyBarVersion],
    trade_date: Option<NaiveDate>,
    window: usize,
) -> Option<f64> {
    let latest_close = trade_date.and_then(|date| close_at_trade_date(history, date).ok())?;
    let average = average_close(window_bars(history, trade_date?, window).ok()?).ok()?;
    finite_nonzero(average).map(|average| latest_close / average - 1.0)
}

fn moving_average_cross_ratio(
    history: &[PointInTimeDailyBarVersion],
    trade_date: Option<NaiveDate>,
    short_window: usize,
    long_window: usize,
) -> Option<f64> {
    let date = trade_date?;
    let short_average = average_close(window_bars(history, date, short_window).ok()?).ok()?;
    let long_average = average_close(window_bars(history, date, long_window).ok()?).ok()?;
    finite_nonzero(long_average).map(|long_average| short_average / long_average - 1.0)
}

fn consolidation_range(
    history: &[PointInTimeDailyBarVersion],
    trade_date: Option<NaiveDate>,
    window: usize,
) -> Option<f64> {
    let date = trade_date?;
    let bars = window_bars(history, date, window).ok()?;
    let current_close = close_at_trade_date(history, date).ok()?;
    let high = bars
        .iter()
        .map(|bar| bar.high)
        .try_fold(f64::NEG_INFINITY, finite_max)?;
    let low = bars
        .iter()
        .map(|bar| bar.low)
        .try_fold(f64::INFINITY, finite_min)?;
    finite_nonzero(current_close).map(|close| (high - low) / close)
}

fn volume_ratio(
    history: &[PointInTimeDailyBarVersion],
    trade_date: Option<NaiveDate>,
    window: usize,
) -> Option<f64> {
    let bars = window_bars(history, trade_date?, window).ok()?;
    let latest_volume = bars.last()?.volume as f64;
    if latest_volume <= 0.0 {
        return None;
    }
    let total_volume = bars.iter().try_fold(0.0, |sum, bar| {
        let volume = bar.volume as f64;
        (volume > 0.0).then_some(sum + volume)
    })?;
    let average = total_volume / window as f64;
    finite_nonzero(average).map(|average| latest_volume / average)
}

fn distance_from_low(
    history: &[PointInTimeDailyBarVersion],
    trade_date: Option<NaiveDate>,
    window: usize,
) -> Option<f64> {
    let date = trade_date?;
    let current_close = close_at_trade_date(history, date).ok()?;
    let low = window_bars(history, date, window)
        .ok()?
        .iter()
        .map(|bar| bar.low)
        .try_fold(f64::INFINITY, finite_min)?;
    finite_nonzero(low).map(|low| current_close / low - 1.0)
}

fn rsi(
    history: &[PointInTimeDailyBarVersion],
    trade_date: Option<NaiveDate>,
    window: usize,
) -> Option<f64> {
    let date = trade_date?;
    let bars = window_bars(history, date, window + 1).ok()?;
    let closes: Vec<f64> = bars
        .iter()
        .map(|bar| finite_positive(bar.close))
        .collect::<Option<Vec<_>>>()?;
    let mut gains = 0.0;
    let mut losses = 0.0;
    for pair in closes.windows(2) {
        let delta = pair[1] - pair[0];
        if delta > 0.0 {
            gains += delta;
        } else {
            losses += delta.abs();
        }
    }
    let average_gain = gains / window as f64;
    let average_loss = losses / window as f64;
    if average_loss == 0.0 {
        if average_gain == 0.0 {
            Some(50.0)
        } else {
            Some(100.0)
        }
    } else {
        let relative_strength = average_gain / average_loss;
        Some(100.0 - (100.0 / (1.0 + relative_strength)))
    }
}

fn close_at_trade_date(
    history: &[PointInTimeDailyBarVersion],
    trade_date: NaiveDate,
) -> std::result::Result<f64, ()> {
    let latest = history.last().ok_or(())?;
    if latest.trade_date != trade_date {
        return Err(());
    }
    valid_close(latest)
}

fn window_bars(
    history: &[PointInTimeDailyBarVersion],
    trade_date: NaiveDate,
    window: usize,
) -> std::result::Result<Vec<Candle>, ()> {
    if history.len() < window {
        return Err(());
    }
    let latest = history.last().ok_or(())?;
    if latest.trade_date != trade_date {
        return Err(());
    }
    history[history.len() - window..]
        .iter()
        .map(valid_bar)
        .collect()
}

fn average_close(bars: Vec<Candle>) -> std::result::Result<f64, ()> {
    if bars.is_empty() {
        return Err(());
    }
    let sum = bars
        .iter()
        .try_fold(0.0, |sum, bar| {
            finite_positive(bar.close).map(|close| sum + close)
        })
        .ok_or(())?;
    Ok(sum / bars.len() as f64)
}

fn valid_close(row: &PointInTimeDailyBarVersion) -> std::result::Result<f64, ()> {
    let Candle { close, .. } = row.bar.as_ref().ok_or(())?;
    if close.is_finite() && *close > 0.0 {
        Ok(*close)
    } else {
        Err(())
    }
}

fn valid_bar(row: &PointInTimeDailyBarVersion) -> std::result::Result<Candle, ()> {
    let bar = row.bar.as_ref().ok_or(())?;
    if finite_positive(bar.open).is_some()
        && finite_positive(bar.high).is_some()
        && finite_positive(bar.low).is_some()
        && finite_positive(bar.close).is_some()
        && bar.volume > 0
        && bar.amount.is_finite()
    {
        Ok(bar.clone())
    } else {
        Err(())
    }
}

fn finite_positive(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0).then_some(value)
}

fn finite_nonzero(value: f64) -> Option<f64> {
    (value.is_finite() && value != 0.0).then_some(value)
}

fn finite_max(current: f64, value: f64) -> Option<f64> {
    value.is_finite().then_some(current.max(value))
}

fn finite_min(current: f64, value: f64) -> Option<f64> {
    value.is_finite().then_some(current.min(value))
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

fn adjustments_by_code(rows: Vec<AdjustmentFactor>) -> BTreeMap<String, Vec<AdjustmentFactor>> {
    let mut adjustments: BTreeMap<String, Vec<AdjustmentFactor>> = BTreeMap::new();
    for row in rows {
        adjustments.entry(row.code.clone()).or_default().push(row);
    }
    adjustments
}

fn adjusted_histories_by_code(
    histories: &BTreeMap<String, Vec<PointInTimeDailyBarVersion>>,
    adjustments: &BTreeMap<String, Vec<AdjustmentFactor>>,
) -> (
    BTreeMap<String, Vec<PointInTimeDailyBarVersion>>,
    BTreeMap<String, Invalidation>,
) {
    let mut adjusted_histories = BTreeMap::new();
    let mut invalidations = BTreeMap::new();
    for (code, history) in histories {
        let factor_rows = adjustments.get(code).map(Vec::as_slice).unwrap_or(&[]);
        match adjust_history(history, factor_rows) {
            Ok(adjusted_history) => {
                adjusted_histories.insert(code.clone(), adjusted_history);
            }
            Err(invalidation) => {
                invalidations.insert(code.clone(), invalidation);
            }
        }
    }
    (adjusted_histories, invalidations)
}

fn adjust_history(
    history: &[PointInTimeDailyBarVersion],
    factors: &[AdjustmentFactor],
) -> std::result::Result<Vec<PointInTimeDailyBarVersion>, Invalidation> {
    let raw_bars: Vec<Candle> = history
        .iter()
        .map(valid_bar)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Invalidation {
            reason: "missing_daily_bar".to_string(),
            feature: None,
            detail: "daily bar history contains missing or invalid OHLCV data".to_string(),
        })?;
    let adjusted_bars = adjust_candles(&raw_bars, factors).map_err(|error| {
        let detail = error.to_string();
        let reason = if detail.contains("ambiguous adjustment factors") {
            "ambiguous_adjustment_factor"
        } else {
            "missing_adjustment_factor"
        };
        Invalidation {
            reason: reason.to_string(),
            feature: None,
            detail,
        }
    })?;
    Ok(history
        .iter()
        .cloned()
        .zip(adjusted_bars)
        .map(|(mut row, bar)| {
            row.bar = Some(bar);
            row
        })
        .collect())
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

fn data_quality_flags(
    history: &[PointInTimeDailyBarVersion],
    status: Option<&SecurityDailyStatus>,
    adjustments: &[AdjustmentFactor],
    security_master: Option<&SecurityMasterVersion>,
    sector_memberships: &[SectorMembership],
    daily_basic: Option<&DailyBasicSnapshot>,
) -> Vec<String> {
    let mut flags = Vec::new();
    for row in history {
        if !row.missing_critical_fields.is_empty() {
            flags.push(format!(
                "daily_bar_missing_critical_fields:{}:{}",
                row.trade_date,
                row.missing_critical_fields.join(",")
            ));
        }
    }
    if status.is_some_and(|row| row.availability_quality == AvailabilityQuality::Estimated) {
        flags.push("security_status_estimated".to_string());
    }
    if security_master.is_some_and(|row| row.availability_quality == AvailabilityQuality::Estimated)
    {
        flags.push("security_master_estimated".to_string());
    }
    if daily_basic.is_some_and(|row| row.availability_quality == AvailabilityQuality::Estimated) {
        flags.push("daily_basic_estimated".to_string());
    }
    if adjustments
        .iter()
        .any(|row| row.availability_quality == AvailabilityQuality::Estimated)
    {
        flags.push("adjustment_factor_estimated".to_string());
    }
    if sector_memberships
        .iter()
        .any(|row| row.availability_quality == AvailabilityQuality::Estimated)
    {
        flags.push("sector_membership_estimated".to_string());
    }
    flags
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
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::{Duration, NaiveDate, TimeZone, Utc};
    use serde_json::{json, Value};
    use uuid::Uuid;

    use super::super::model::{PatternModelPayload, ValidationPayload};
    use super::{
        derive_feature_vector, evaluate_pattern, load_published_patterns, similarity,
        MarketFeatureContext, MarketSource, PatternEngine, PatternStore, ScoreContext,
        SimilarityScore, SupportedFeature,
    };
    use crate::analysis::market_snapshot::{
        AdjustmentFactor, AvailabilityQuality, DailyBasicSnapshot, MarketSnapshot,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
    };
    use crate::analysis::patterns::ranking::ShadowTier;
    use crate::data::types::Candle;
    use crate::error::Result;
    use crate::storage::market_repository::PointInTimeDailyBarVersion;
    use crate::storage::pattern_repository::{PatternVersionRow, ShadowCandidateRow};

    fn fixture_payload() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../tests/fixtures/pattern_model_v1.json"
        ))
        .unwrap()
    }

    fn model_with_metric(metric: &str, cluster_parameters: Value) -> PatternModelPayload {
        let mut payload = fixture_payload();
        payload["distance_metric"] = json!(metric);
        payload["cluster_parameters"] = cluster_parameters;
        PatternModelPayload::from_value(payload).unwrap()
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected}, got {actual}"
        );
    }

    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 1, 1).unwrap() + Duration::days(i64::from(day - 1))
    }

    fn daily_row(
        day: u32,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: i64,
    ) -> PointInTimeDailyBarVersion {
        let timestamp = date(day).and_hms_opt(16, 0, 0).unwrap().and_utc();
        PointInTimeDailyBarVersion {
            code: "600000.SH".to_string(),
            trade_date: date(day),
            bar: Some(Candle {
                trade_date: date(day),
                open,
                high,
                low,
                close,
                volume,
                amount: close * volume as f64,
                turnover: Some(1.0),
                pe: Some(10.0),
                pb: Some(1.5),
            }),
            missing_critical_fields: Vec::new(),
            available_at: timestamp,
            ingested_at: timestamp,
            source: "test".to_string(),
        }
    }

    fn daily_row_for_code(
        code: &str,
        day: u32,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: i64,
    ) -> PointInTimeDailyBarVersion {
        let mut row = daily_row(day, open, high, low, close, volume);
        row.code = code.to_string();
        row
    }

    fn adjustment_factor(code: &str, day: u32) -> AdjustmentFactor {
        let timestamp = date(day).and_hms_opt(16, 0, 0).unwrap().and_utc();
        AdjustmentFactor {
            code: code.to_string(),
            trade_date: date(day),
            adj_factor: 1.0,
            available_at: timestamp,
            ingested_at: timestamp,
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        }
    }

    fn security_master(code: &str, name: &str, day: u32) -> SecurityMasterVersion {
        let timestamp = date(day).and_hms_opt(16, 0, 0).unwrap().and_utc();
        SecurityMasterVersion {
            code: code.to_string(),
            name: name.to_string(),
            market: Some("A".to_string()),
            exchange: Some("SH".to_string()),
            list_status: "L".to_string(),
            list_date: Some(date(1)),
            delist_date: None,
            available_at: timestamp,
            ingested_at: timestamp,
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        }
    }

    fn sector_membership(code: &str, day: u32) -> SectorMembership {
        let timestamp = date(day).and_hms_opt(16, 0, 0).unwrap().and_utc();
        SectorMembership {
            code: code.to_string(),
            sector_code: "801010".to_string(),
            sector_name: "Energy".to_string(),
            sector_type: "sw_l1".to_string(),
            valid_from: date(1),
            valid_to: None,
            available_at: timestamp,
            ingested_at: timestamp,
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        }
    }

    fn daily_basic(code: &str, day: u32) -> DailyBasicSnapshot {
        let timestamp = date(day).and_hms_opt(16, 0, 0).unwrap().and_utc();
        DailyBasicSnapshot {
            code: code.to_string(),
            trade_date: date(day),
            turnover_rate: Some(2.0),
            volume_ratio: Some(1.2),
            pe: Some(10.0),
            pb: Some(1.5),
            ps: Some(2.0),
            total_share: Some(1_000_000.0),
            float_share: Some(800_000.0),
            total_mv: Some(10_000_000.0),
            circ_mv: Some(8_000_000.0),
            available_at: timestamp,
            ingested_at: timestamp,
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        }
    }

    fn market_snapshot(day: u32) -> MarketSnapshot {
        let timestamp = date(day).and_hms_opt(16, 0, 0).unwrap().and_utc();
        MarketSnapshot {
            trade_date: date(day),
            snapshot_version: "market-v1".to_string(),
            available_at: timestamp,
            data_complete: true,
            metrics: json!({
                "breadth": {
                    "up_count": 6,
                    "down_count": 2,
                    "flat_count": 2,
                    "above_ma20_count": 7,
                    "new_high_20_count": 1,
                    "new_low_20_count": 0,
                    "limit_up_count": 0,
                    "limit_down_count": 0,
                    "total_amount": 100000000.0
                }
            }),
            missing_inputs: Vec::new(),
            input_fingerprint: "market-fp".to_string(),
        }
    }

    fn complete_score_context(code: &str, day: u32) -> ScoreContext {
        ScoreContext {
            sector_memberships: vec![sector_membership(code, day)],
            market_snapshot: Some(market_snapshot(day)),
            daily_basic: Some(daily_basic(code, day)),
            current_bar: daily_row_for_code(code, day, 100.0, 101.0, 99.0, 100.0, 10_000).bar,
            data_quality_flags: Vec::new(),
        }
    }

    fn active_status(code: &str, day: u32) -> SecurityDailyStatus {
        let timestamp = date(day).and_hms_opt(16, 0, 0).unwrap().and_utc();
        SecurityDailyStatus {
            code: code.to_string(),
            trade_date: date(day),
            listed_days: Some(120),
            is_st: false,
            is_suspended: false,
            price_limit_pct: Some(0.10),
            available_at: timestamp,
            ingested_at: timestamp,
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        }
    }

    #[derive(Debug)]
    struct FakePatternStore {
        rows: Vec<PatternVersionRow>,
        saved: Mutex<Vec<ShadowCandidateRow>>,
        listed_sets: Mutex<Vec<Uuid>>,
    }

    #[async_trait]
    impl PatternStore for FakePatternStore {
        async fn list_published_patterns(
            &self,
            pattern_set_id: Uuid,
        ) -> Result<Vec<PatternVersionRow>> {
            self.listed_sets.lock().unwrap().push(pattern_set_id);
            Ok(self.rows.clone())
        }

        async fn upsert_shadow_candidates(&self, rows: &[ShadowCandidateRow]) -> Result<usize> {
            self.saved.lock().unwrap().extend_from_slice(rows);
            Ok(rows.len())
        }
    }

    #[derive(Debug)]
    struct FakeMarketSource {
        history: Vec<PointInTimeDailyBarVersion>,
        statuses: Vec<SecurityDailyStatus>,
        adjustments: Vec<AdjustmentFactor>,
        security_master: BTreeMap<String, SecurityMasterVersion>,
        sector_memberships: BTreeMap<String, Vec<SectorMembership>>,
        market_snapshot: Option<MarketSnapshot>,
        daily_basics: BTreeMap<String, DailyBasicSnapshot>,
    }

    #[async_trait]
    impl MarketSource for FakeMarketSource {
        async fn daily_bar_history_as_of(
            &self,
            _end: NaiveDate,
            _as_of: chrono::DateTime<Utc>,
            _lookback: i64,
        ) -> Result<Vec<PointInTimeDailyBarVersion>> {
            Ok(self.history.clone())
        }

        async fn security_status_universe_as_of(
            &self,
            _trade_date: NaiveDate,
            _as_of: chrono::DateTime<Utc>,
        ) -> Result<Vec<SecurityDailyStatus>> {
            Ok(self.statuses.clone())
        }

        async fn adjustment_factors_as_of(
            &self,
            _codes: &[String],
            _start: NaiveDate,
            _end: NaiveDate,
            _as_of: chrono::DateTime<Utc>,
        ) -> Result<Vec<AdjustmentFactor>> {
            Ok(self.adjustments.clone())
        }

        async fn security_master(
            &self,
            code: &str,
            _as_of: chrono::DateTime<Utc>,
        ) -> Result<Option<SecurityMasterVersion>> {
            Ok(self.security_master.get(code).cloned())
        }

        async fn active_sector_memberships(
            &self,
            code: &str,
            _trade_date: NaiveDate,
            _as_of: chrono::DateTime<Utc>,
        ) -> Result<Vec<SectorMembership>> {
            Ok(self
                .sector_memberships
                .get(code)
                .cloned()
                .unwrap_or_default())
        }

        async fn market_snapshot(
            &self,
            _trade_date: NaiveDate,
            _version: &str,
        ) -> Result<Option<MarketSnapshot>> {
            Ok(self.market_snapshot.clone())
        }

        async fn daily_basic(
            &self,
            code: &str,
            _trade_date: NaiveDate,
            _as_of: chrono::DateTime<Utc>,
        ) -> Result<Option<DailyBasicSnapshot>> {
            Ok(self.daily_basics.get(code).cloned())
        }
    }

    fn pattern_row(schema_version: &str) -> PatternVersionRow {
        let timestamp = Utc.with_ymd_and_hms(2026, 7, 10, 16, 0, 0).unwrap();
        PatternVersionRow {
            pattern_version_id: Uuid::nil(),
            pattern_id: "trend:kmeans:k2:c0".to_string(),
            horizon: "week".to_string(),
            pattern_type: "trend".to_string(),
            status: "published".to_string(),
            schema_version: schema_version.to_string(),
            feature_version: "features-v1".to_string(),
            logic_version: "logic-v1".to_string(),
            dataset_version: "dataset-v1".to_string(),
            model_payload: fixture_payload(),
            validation_payload: json!({
                "candidate_id": "trend:kmeans:k2:c0",
                "positive_sample_count": 12,
                "control_sample_count": 18,
                "effective_sample_count": 8.0,
                "base_rate": 0.40,
                "precision": 0.75,
                "lift": 2.0,
                "lift_over_base_rate": 2.0,
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
                "release_gate_passed": true,
                "candidate_status": "validated"
            }),
            trained_from: date(1),
            trained_until: date(9),
            available_at_cutoff: timestamp,
            approved_by: Some("reviewer".to_string()),
            published_at: Some(timestamp),
            created_at: timestamp,
        }
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
        assert_close(model.validation_lift, 2.0);
        assert_close(model.validation_coverage, 0.27);
        assert_eq!(
            model.baseline_comparison["cost_adjusted_return_delta"],
            0.022
        );
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
    fn model_payload_requires_validation_summary_fields() {
        let mut payload = fixture_payload();
        payload.as_object_mut().unwrap().remove("validation_lift");

        let error = PatternModelPayload::from_value(payload).unwrap_err();

        assert!(error.to_string().contains("validation_lift"));
    }

    #[test]
    fn model_payload_baseline_comparison_values_must_be_finite() {
        let mut payload = fixture_payload();
        payload["baseline_comparison"]["cost_adjusted_return_delta"] = json!(f64::INFINITY);

        let error = PatternModelPayload::from_value(payload).unwrap_err();

        assert!(error.to_string().contains("baseline_comparison"));
    }

    #[test]
    fn fixed_feature_vector_produces_fixed_similarity() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.15),
            ("relative_strength_20d".to_string(), 1.10),
        ]);

        let score = similarity(&model, &features).unwrap();

        assert!((score.similarity - 0.309_016_994_374_947_4).abs() < 1e-12);
    }

    #[test]
    fn high_similarity_without_release_gate_is_not_shadow_a() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
        ]);
        let validation = validation_payload(false, 2.0);

        let candidate = evaluate_pattern(
            &model,
            &validation,
            &features,
            &complete_score_context("600000.SH", 21),
        );

        assert_ne!(candidate.shadow_tier, ShadowTier::ShadowA);
        assert_eq!(candidate.shadow_tier, ShadowTier::ShadowB);
    }

    #[test]
    fn high_similarity_with_inadequate_lift_is_not_shadow_a() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
        ]);
        let validation = validation_payload(true, 1.0);

        let candidate = evaluate_pattern(
            &model,
            &validation,
            &features,
            &complete_score_context("600000.SH", 21),
        );

        assert_ne!(candidate.shadow_tier, ShadowTier::ShadowA);
        assert_eq!(candidate.shadow_tier, ShadowTier::ShadowB);
    }

    #[test]
    fn unsupported_required_feature_invalidates_candidate() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = BTreeMap::from([("return_20d".to_string(), 0.20)]);
        let validation = validation_payload(true, 2.0);

        let candidate = evaluate_pattern(
            &model,
            &validation,
            &features,
            &complete_score_context("600000.SH", 21),
        );

        assert_eq!(candidate.shadow_tier, ShadowTier::Reject);
        assert!(candidate
            .invalidations
            .iter()
            .any(|invalidation| invalidation.reason == "missing_required_feature"));
    }
    #[test]
    fn mahalanobis_payload_requires_full_covariance() {
        let mut payload = fixture_payload();
        payload["distance_metric"] = json!("mahalanobis");
        payload["cluster_parameters"] = json!({});

        let error = PatternModelPayload::from_value(payload).unwrap_err();

        assert!(error.to_string().contains("covariance"));
    }

    #[test]
    fn mahalanobis_payload_rejects_singular_covariance() {
        let mut payload = fixture_payload();
        payload["distance_metric"] = json!("mahalanobis");
        payload["cluster_parameters"] = json!({
            "covariance": [[1.0, 1.0], [1.0, 1.0]]
        });

        let error = PatternModelPayload::from_value(payload).unwrap_err();

        assert!(error.to_string().contains("positive definite"));
    }

    #[test]
    fn mahalanobis_similarity_uses_full_covariance() {
        let model = model_with_metric(
            "mahalanobis",
            json!({
                "covariance": [[0.01, 0.004], [0.004, 0.04]]
            }),
        );
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.25),
            ("relative_strength_20d".to_string(), 1.20),
        ]);

        let SimilarityScore {
            distance,
            similarity,
            distance_metric,
            ..
        } = similarity(&model, &features).unwrap();

        assert_eq!(distance_metric, "mahalanobis");
        assert_close(distance, 0.790_569_415_042_094_9);
        assert_close(similarity, 1.0 / (1.0 + 0.790_569_415_042_094_9));
    }

    #[test]
    fn gmm_probability_payload_requires_full_mixture_parameters() {
        let mut payload = fixture_payload();
        payload["distance_metric"] = json!("gmm_probability");
        payload["cluster_parameters"] = json!({
            "mixture_covariance": [[1.0, 0.0], [0.0, 1.0]],
            "mixture_weight": 0.7
        });

        let error = PatternModelPayload::from_value(payload).unwrap_err();

        assert!(error.to_string().contains("mixture_mean"));
    }

    #[test]
    fn gmm_probability_similarity_uses_full_mixture_parameters() {
        let model = model_with_metric(
            "gmm_probability",
            json!({
                "mixture_mean": {
                    "return_20d": 0.20,
                    "relative_strength_20d": 1.20
                },
                "mixture_covariance": [[0.01, 0.004], [0.004, 0.04]],
                "mixture_weight": 0.8
            }),
        );
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.25),
            ("relative_strength_20d".to_string(), 1.20),
        ]);

        let score = similarity(&model, &features).unwrap();

        assert_eq!(score.distance_metric, "gmm_probability");
        assert_close(score.distance, 0.510_310_363_079_828_8);
        assert_close(
            score.similarity,
            0.8 * (-0.5_f64 * 0.260_416_666_666_666_7_f64).exp(),
        );
    }

    #[test]
    fn gmm_probability_similarity_changes_with_covariance_and_weight() {
        let mut payload = fixture_payload();
        payload["distance_metric"] = json!("gmm_probability");
        payload["cluster_parameters"] = json!({
            "mixture_mean": {
                "return_20d": 0.20,
                "relative_strength_20d": 1.20
            },
            "mixture_covariance": [[0.01, 0.004], [0.004, 0.04]],
            "mixture_weight": 0.8
        });
        let base_model = PatternModelPayload::from_value(payload.clone()).unwrap();
        payload["cluster_parameters"] = json!({
            "mixture_mean": {
                "return_20d": 0.20,
                "relative_strength_20d": 1.20
            },
            "mixture_covariance": [[0.04, 0.0], [0.0, 0.04]],
            "mixture_weight": 0.4
        });
        let changed_model = PatternModelPayload::from_value(payload).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.25),
            ("relative_strength_20d".to_string(), 1.20),
        ]);

        let base_score = similarity(&base_model, &features).unwrap();
        let changed_score = similarity(&changed_model, &features).unwrap();

        assert_ne!(base_score.similarity, changed_score.similarity);
        assert_close(
            changed_score.similarity,
            0.4 * (-0.5_f64 * 0.062_5_f64).exp(),
        );
    }

    #[test]
    fn unsupported_pattern_schema_version_rejects_row() {
        let error = match load_published_patterns(vec![pattern_row("2")]) {
            Ok(_) => panic!("unsupported schema version should reject"),
            Err(error) => error,
        };

        assert!(error
            .to_string()
            .contains("unsupported pattern schema_version"));
        assert!(error.to_string().contains("2"));
    }

    #[test]
    fn published_pattern_loading_rejects_model_validation_summary_mismatch() {
        let mut row = pattern_row("1");
        row.model_payload["validation_lift"] = json!(1.99);

        let error = load_published_patterns(vec![row]).unwrap_err();

        assert!(error.to_string().contains("validation_lift"));
        assert!(error.to_string().contains("validation_payload.lift"));
    }

    #[test]
    fn final_score_uses_all_required_score_components() {
        let mut payload = fixture_payload();
        payload["required_features"] = json!([
            "return_20d",
            "relative_strength_20d",
            "distance_from_20d_low"
        ]);
        payload["scaler_mean"]["distance_from_20d_low"] = json!(0.0);
        payload["scaler_scale"]["distance_from_20d_low"] = json!(1.0);
        payload["centroid"]["distance_from_20d_low"] = json!(0.10);
        let model = PatternModelPayload::from_value(payload).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
            ("distance_from_20d_low".to_string(), 0.15),
        ]);
        let validation = validation_payload(true, 2.0);
        let context = complete_score_context("600000.SH", 21);

        let candidate = evaluate_pattern(&model, &validation, &features, &context);

        assert_eq!(candidate.shadow_tier, ShadowTier::ShadowA);
        let components = &candidate.supporting_signals["score_components"];
        for key in [
            "validated_pattern_strength",
            "current_similarity",
            "relative_strength",
            "sector_confirmation",
            "market_regime",
            "extension_penalty",
            "liquidity_penalty",
            "data_quality_penalty",
        ] {
            assert!(
                components.get(key).is_some(),
                "missing score component {key}: {components}"
            );
        }
        assert_eq!(components["context_complete"], json!(true));
        assert_eq!(components["market_state_satisfied"], json!(true));
        let expected = components["validated_pattern_strength"].as_f64().unwrap()
            * components["current_similarity"].as_f64().unwrap()
            * components["relative_strength"].as_f64().unwrap()
            * components["sector_confirmation"].as_f64().unwrap()
            * components["market_regime"].as_f64().unwrap()
            * (1.0 - components["extension_penalty"].as_f64().unwrap())
            * (1.0 - components["liquidity_penalty"].as_f64().unwrap())
            * (1.0 - components["data_quality_penalty"].as_f64().unwrap())
            * components["risk_adjustment"].as_f64().unwrap();
        assert_close(candidate.final_score, expected);
    }

    #[test]
    fn missing_score_context_demotes_shadow_a_without_zero_substitution() {
        let model = PatternModelPayload::from_value(fixture_payload()).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
        ]);
        let validation = validation_payload(true, 2.0);
        let context = ScoreContext {
            sector_memberships: Vec::new(),
            market_snapshot: None,
            daily_basic: None,
            current_bar: None,
            data_quality_flags: vec!["missing_daily_basic".to_string()],
        };

        let candidate = evaluate_pattern(&model, &validation, &features, &context);

        assert_ne!(candidate.shadow_tier, ShadowTier::ShadowA);
        assert_eq!(
            candidate.supporting_signals["score_components"]["context_complete"],
            json!(false)
        );
        assert_eq!(
            candidate.supporting_signals["score_components"]["missing_components"],
            json!([
                "sector_confirmation",
                "market_regime",
                "extension_penalty",
                "liquidity_penalty",
                "data_quality_penalty"
            ])
        );
        assert!(
            candidate.supporting_signals["score_components"]["relative_strength"]
                .as_f64()
                .unwrap()
                > 0.0
        );
    }

    #[test]
    fn shadow_a_requires_each_market_context_component() {
        let mut payload = fixture_payload();
        payload["required_features"] = json!([
            "return_20d",
            "relative_strength_20d",
            "distance_from_20d_low"
        ]);
        payload["scaler_mean"]["distance_from_20d_low"] = json!(0.0);
        payload["scaler_scale"]["distance_from_20d_low"] = json!(1.0);
        payload["centroid"]["distance_from_20d_low"] = json!(0.10);
        let model = PatternModelPayload::from_value(payload).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
            ("distance_from_20d_low".to_string(), 0.15),
        ]);
        let validation = validation_payload(true, 2.0);
        let complete = complete_score_context("600000.SH", 21);
        assert_eq!(
            evaluate_pattern(&model, &validation, &features, &complete).shadow_tier,
            ShadowTier::ShadowA
        );

        let mut missing_sector = complete.clone();
        missing_sector.sector_memberships.clear();
        let mut missing_snapshot = complete.clone();
        missing_snapshot.market_snapshot = None;
        let mut incomplete_snapshot = complete.clone();
        incomplete_snapshot
            .market_snapshot
            .as_mut()
            .unwrap()
            .data_complete = false;
        let mut missing_liquidity = complete.clone();
        missing_liquidity.daily_basic = None;
        missing_liquidity.current_bar = None;
        let mut estimated_quality = complete.clone();
        estimated_quality
            .data_quality_flags
            .push("daily_basic_estimated".to_string());

        for context in [
            missing_sector,
            missing_snapshot,
            incomplete_snapshot,
            missing_liquidity,
            estimated_quality,
        ] {
            let candidate = evaluate_pattern(&model, &validation, &features, &context);
            assert_ne!(candidate.shadow_tier, ShadowTier::ShadowA);
            assert_eq!(
                candidate.supporting_signals["score_components"]["context_complete"],
                json!(false)
            );
        }
    }

    #[tokio::test]
    async fn match_market_loads_patterns_derives_features_and_persists_shadow_candidates() {
        let trade_date = date(21);
        let pattern_set_id = Uuid::new_v4();
        let pattern_version_id = Uuid::new_v4();
        let mut row = pattern_row("1");
        row.pattern_version_id = pattern_version_id;
        row.model_payload["centroid"] = json!({
            "return_20d": 0.20,
            "relative_strength_20d": 1.0
        });
        row.model_payload["necessary_conditions"] = json!([]);
        row.model_payload["risk_conditions"] = json!([]);

        let code = "600000.SH";
        let mut history = Vec::new();
        for day in 1..=21 {
            let close = 100.0 + f64::from(day - 1);
            history.push(daily_row_for_code(
                code,
                day,
                close,
                close + 1.0,
                close - 1.0,
                close,
                10_000 + i64::from(day),
            ));
        }
        history[20].bar.as_mut().unwrap().close = 120.0;

        let market = FakeMarketSource {
            history,
            statuses: vec![active_status(code, 21)],
            adjustments: (1..=21).map(|day| adjustment_factor(code, day)).collect(),
            security_master: BTreeMap::from([(
                code.to_string(),
                security_master(code, "Pudong Bank", 21),
            )]),
            sector_memberships: BTreeMap::from([(
                code.to_string(),
                vec![sector_membership(code, 21)],
            )]),
            market_snapshot: Some(market_snapshot(21)),
            daily_basics: BTreeMap::from([(code.to_string(), daily_basic(code, 21))]),
        };
        let pattern_store = FakePatternStore {
            rows: vec![row],
            saved: Mutex::new(Vec::new()),
            listed_sets: Mutex::new(Vec::new()),
        };
        let engine = PatternEngine::with_sources(pattern_store, market);

        let candidates = engine
            .match_market(trade_date, pattern_set_id)
            .await
            .unwrap();

        assert_eq!(candidates.len(), 1);
        let candidate = &candidates[0];
        assert_eq!(candidate.pattern_version_id, pattern_version_id);
        assert_eq!(candidate.pattern_set_id, pattern_set_id);
        assert_eq!(candidate.code, code);
        assert_eq!(candidate.name.as_deref(), Some("Pudong Bank"));
        assert!(matches!(
            candidate.shadow_tier,
            ShadowTier::ShadowA | ShadowTier::ShadowB | ShadowTier::Watch | ShadowTier::Reject
        ));
        assert_close(
            candidate.matched_features["raw"]["return_20d"]
                .as_f64()
                .unwrap(),
            0.20,
        );
        assert_close(
            candidate.matched_features["raw"]["relative_strength_20d"]
                .as_f64()
                .unwrap(),
            1.0,
        );
        let saved = engine.pattern_repo.saved.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].code, candidate.code);
        assert_eq!(saved[0].name, candidate.name);
        assert_eq!(saved[0].shadow_tier, candidate.shadow_tier.as_str());
        assert_eq!(
            engine.pattern_repo.listed_sets.lock().unwrap().as_slice(),
            &[pattern_set_id]
        );
    }

    #[tokio::test]
    async fn match_market_rejects_missing_security_master_name() {
        let trade_date = date(21);
        let pattern_set_id = Uuid::new_v4();
        let mut row = pattern_row("1");
        row.model_payload["necessary_conditions"] = json!([]);
        row.model_payload["risk_conditions"] = json!([]);

        let code = "600000.SH";
        let mut history = Vec::new();
        for day in 1..=21 {
            history.push(daily_row_for_code(
                code,
                day,
                100.0,
                101.0,
                99.0,
                100.0 + f64::from(day),
                10_000,
            ));
        }
        let market = FakeMarketSource {
            history,
            statuses: vec![active_status(code, 21)],
            adjustments: (1..=21).map(|day| adjustment_factor(code, day)).collect(),
            security_master: BTreeMap::new(),
            sector_memberships: BTreeMap::from([(
                code.to_string(),
                vec![sector_membership(code, 21)],
            )]),
            market_snapshot: Some(market_snapshot(21)),
            daily_basics: BTreeMap::from([(code.to_string(), daily_basic(code, 21))]),
        };
        let pattern_store = FakePatternStore {
            rows: vec![row],
            saved: Mutex::new(Vec::new()),
            listed_sets: Mutex::new(Vec::new()),
        };
        let engine = PatternEngine::with_sources(pattern_store, market);

        let candidates = engine
            .match_market(trade_date, pattern_set_id)
            .await
            .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].shadow_tier, ShadowTier::Reject);
        assert!(candidates[0]
            .invalidations
            .to_string()
            .contains("missing_security_master_name"));
    }

    #[test]
    fn validation_nested_yearly_and_regime_values_must_be_numeric() {
        let mut yearly_payload = json!({
            "candidate_id": "trend:kmeans:k2:c0",
            "positive_sample_count": 12,
            "control_sample_count": 18,
            "effective_sample_count": 8.0,
            "base_rate": 0.40,
            "precision": 0.75,
            "lift": 2.0,
            "lift_over_base_rate": 2.0,
            "coverage": 0.27,
            "false_positive_rate": 0.11,
            "precision_at_10": 0.70,
            "precision_at_50": 0.62,
            "cost_adjusted_return": 0.032,
            "max_drawdown": -0.045,
            "turnover": 0.20,
            "yearly_results": {"2026": {"sample_count": "30"}},
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
            "release_gate_passed": true,
            "candidate_status": "validated"
        });
        let yearly_error = ValidationPayload::from_value(yearly_payload.clone()).unwrap_err();
        assert!(yearly_error.to_string().contains("yearly_results"));

        yearly_payload["yearly_results"] = json!({"2026": {"sample_count": 30}});
        yearly_payload["regime_results"] = json!({"bull": {"precision": {"value": 0.80}}});
        let regime_error = ValidationPayload::from_value(yearly_payload).unwrap_err();
        assert!(regime_error.to_string().contains("regime_results"));
    }

    #[test]
    fn derives_all_archetype_daily_bar_features() {
        let history: Vec<_> = (1..=61)
            .map(|day| {
                let close = day as f64;
                daily_row(
                    day,
                    close - 0.25,
                    close + 1.0,
                    close - 1.0,
                    close,
                    day as i64 * 100,
                )
            })
            .collect();
        let required_features = vec![
            "return_20d".to_string(),
            "return_60d".to_string(),
            "price_vs_ma50".to_string(),
            "ma20_vs_ma50".to_string(),
            "relative_strength_20d".to_string(),
            "consolidation_range_20d".to_string(),
            "consolidation_range_60d".to_string(),
            "volume_ratio_20d".to_string(),
            "breakout_return_5d".to_string(),
            "distance_from_20d_low".to_string(),
            "rsi_14".to_string(),
            "reversal_return_5d".to_string(),
        ];
        let context = MarketFeatureContext {
            median_return_ratio_by_window: BTreeMap::from([(20, 61.0 / 41.0)]),
        };

        let (features, invalidations) =
            derive_feature_vector(&required_features, &history, &context);

        assert!(invalidations.is_empty(), "{invalidations:?}");
        assert_close(features["return_20d"], 61.0 / 41.0 - 1.0);
        assert_close(features["return_60d"], 60.0);
        assert_close(features["price_vs_ma50"], 61.0 / 36.5 - 1.0);
        assert_close(features["ma20_vs_ma50"], 51.5 / 36.5 - 1.0);
        assert_close(features["relative_strength_20d"], 1.0);
        assert_close(features["consolidation_range_20d"], (62.0 - 41.0) / 61.0);
        assert_close(features["consolidation_range_60d"], (62.0 - 1.0) / 61.0);
        assert_close(features["volume_ratio_20d"], 6100.0 / 5150.0);
        assert_close(features["breakout_return_5d"], 61.0 / 56.0 - 1.0);
        assert_close(features["distance_from_20d_low"], 61.0 / 41.0 - 1.0);
        assert_close(features["rsi_14"], 100.0);
        assert_close(features["reversal_return_5d"], 61.0 / 56.0 - 1.0);
    }

    #[test]
    fn supported_feature_parser_rejects_event_features() {
        assert!(SupportedFeature::parse("limit_up_gap_3d").is_none());
    }

    #[test]
    fn missing_bar_history_invalidates_supported_features_without_substitutes() {
        let history = vec![daily_row(10, 10.0, 11.0, 9.0, 10.0, 1000)];
        let required_features = vec!["price_vs_ma50".to_string()];
        let context = MarketFeatureContext::default();

        let (features, invalidations) =
            derive_feature_vector(&required_features, &history, &context);

        assert!(features.is_empty());
        assert_eq!(invalidations[0].reason, "insufficient_bar_history");
        assert_eq!(invalidations[0].feature.as_deref(), Some("price_vs_ma50"));
    }

    #[test]
    fn adjustment_factor_gaps_are_explicit_invalidations() {
        let history = vec![
            daily_row(8, 10.0, 11.0, 9.0, 10.5, 1000),
            daily_row(9, 11.0, 12.0, 10.0, 11.5, 1200),
        ];
        let timestamp = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();
        let factors = vec![AdjustmentFactor {
            code: "600000.SH".to_string(),
            trade_date: date(9),
            adj_factor: 2.0,
            available_at: timestamp,
            ingested_at: timestamp,
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        }];

        let error = super::adjust_history(&history, &factors).unwrap_err();

        assert_eq!(error.reason, "missing_adjustment_factor");
        assert!(error.detail.contains("2026-01-08"));
    }

    #[test]
    fn triggered_risk_condition_demotes_shadow_a_to_watch_and_reduces_score() {
        let mut payload = fixture_payload();
        payload["risk_conditions"] = json!([
            {"column": "return_20d", "operator": ">=", "value": 0.1}
        ]);
        let model = PatternModelPayload::from_value(payload).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
        ]);
        let validation = validation_payload(true, 2.0);

        let candidate = evaluate_pattern(
            &model,
            &validation,
            &features,
            &complete_score_context("600000.SH", 21),
        );

        assert_eq!(candidate.shadow_tier, ShadowTier::Watch);
        assert!(candidate.final_score < 2.0);
        assert_eq!(
            candidate.risk_flags["triggered"][0]["status"],
            Value::String("evaluated".to_string())
        );
        assert_eq!(candidate.risk_flags["has_triggered"], json!(true));
        assert_eq!(candidate.risk_flags["has_unevaluable"], json!(false));
        assert_eq!(candidate.risk_flags["risk_adjustment"], json!(0.5));
        assert_eq!(
            candidate.supporting_signals["score_components"]["risk_adjustment"],
            json!(0.5)
        );
    }

    #[test]
    fn unevaluable_risk_condition_demotes_shadow_a_reduces_score_and_is_explicit_in_payloads() {
        let mut payload = fixture_payload();
        payload["risk_conditions"] = json!([
            {"column": "missing_feature", "operator": ">=", "value": 0.1}
        ]);
        let model = PatternModelPayload::from_value(payload).unwrap();
        let features = BTreeMap::from([
            ("return_20d".to_string(), 0.20),
            ("relative_strength_20d".to_string(), 1.30),
        ]);
        let validation = validation_payload(true, 2.0);

        let candidate = evaluate_pattern(
            &model,
            &validation,
            &features,
            &complete_score_context("600000.SH", 21),
        );

        assert_ne!(candidate.shadow_tier, ShadowTier::ShadowA);
        assert_eq!(candidate.shadow_tier, ShadowTier::Watch);
        assert!(candidate.final_score < 2.0);
        assert_eq!(
            candidate.supporting_signals["score_components"]["risk_adjustment"],
            json!(0.5)
        );
        assert_eq!(
            candidate.risk_flags["unevaluable"][0]["status"],
            Value::String("missing_feature".to_string())
        );
        assert_eq!(candidate.risk_flags["has_triggered"], json!(false));
        assert_eq!(candidate.risk_flags["has_unevaluable"], json!(true));
        assert_eq!(candidate.risk_flags["risk_adjustment"], json!(0.5));
        assert_eq!(
            candidate.supporting_signals["risk_conditions"][0]["status"],
            Value::String("missing_feature".to_string())
        );
    }
}
