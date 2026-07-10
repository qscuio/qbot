use serde::{Deserialize, Serialize};

use super::model::{CandidateStatus, ValidationPayload};

pub const MIN_SHADOW_A_LIFT: f64 = 1.10;

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

pub fn final_score(similarity: f64, validation_lift: f64) -> f64 {
    similarity * validation_lift
}

pub fn rank_candidate(
    similarity: f64,
    validation: &ValidationPayload,
    shadow_a_threshold: f64,
    shadow_b_threshold: f64,
    has_invalidations: bool,
) -> ShadowTier {
    if has_invalidations {
        return ShadowTier::Reject;
    }
    if similarity >= shadow_a_threshold
        && validation.release_gate_passed
        && validation.candidate_status == CandidateStatus::Validated
        && validation.lift >= MIN_SHADOW_A_LIFT
    {
        return ShadowTier::ShadowA;
    }
    if similarity >= shadow_b_threshold {
        return ShadowTier::ShadowB;
    }
    ShadowTier::Watch
}
