use serde::{Deserialize, Serialize};

use super::model::{CandidateStatus, ValidationPayload};

pub const MIN_SHADOW_A_LIFT: f64 = 1.10;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreComponents {
    pub validated_pattern_strength: f64,
    pub current_similarity: f64,
    pub relative_strength: f64,
    pub sector_confirmation: f64,
    pub market_regime: f64,
    pub extension_penalty: f64,
    pub liquidity_penalty: f64,
    pub data_quality_penalty: f64,
    pub risk_adjustment: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowTier {
    ShadowA,
    ShadowB,
    Watch,
    Reject,
}

impl ShadowTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ShadowA => "shadow_a",
            Self::ShadowB => "shadow_b",
            Self::Watch => "watch",
            Self::Reject => "reject",
        }
    }
}

pub fn final_score(components: &ScoreComponents) -> f64 {
    components.validated_pattern_strength
        * components.current_similarity
        * components.relative_strength
        * components.sector_confirmation
        * components.market_regime
        * (1.0 - components.extension_penalty)
        * (1.0 - components.liquidity_penalty)
        * (1.0 - components.data_quality_penalty)
        * components.risk_adjustment
}

pub fn rank_candidate(
    similarity: f64,
    validation: &ValidationPayload,
    shadow_a_threshold: f64,
    shadow_b_threshold: f64,
    has_invalidations: bool,
    score_context_complete: bool,
    market_state_satisfied: bool,
) -> ShadowTier {
    if has_invalidations {
        return ShadowTier::Reject;
    }
    if similarity >= shadow_a_threshold
        && validation.release_gate_passed
        && validation.candidate_status == CandidateStatus::Validated
        && validation.lift >= MIN_SHADOW_A_LIFT
        && score_context_complete
        && market_state_satisfied
    {
        return ShadowTier::ShadowA;
    }
    if similarity >= shadow_b_threshold {
        return ShadowTier::ShadowB;
    }
    ShadowTier::Watch
}
