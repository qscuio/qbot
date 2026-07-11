use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::analysis::decision_support::builder::classify_statements;
use crate::analysis::decision_support::contracts::{DecisionCandidate, SupportStatement};
use crate::analysis::decision_support::scan_ranker_adapter::BaselineCandidate;
use crate::storage::pattern_repository::ShadowCandidateRow;

#[derive(Clone)]
struct RankedScanCandidate {
    candidate: BaselineCandidate,
    scan_ranker_percentile: f64,
}

#[derive(Clone)]
struct RankedPatternCandidate {
    candidate: ShadowCandidateRow,
    pattern_percentile: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SupportTier {
    Reject,
    Watch,
    B,
    A,
}

impl SupportTier {
    fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::Watch => "watch",
            Self::B => "B",
            Self::A => "A",
        }
    }

    fn is_positive(self) -> bool {
        matches!(self, Self::A | Self::B)
    }
}

pub(crate) fn build_decision_candidates(
    scan_candidates: Vec<BaselineCandidate>,
    pattern_candidates: Vec<ShadowCandidateRow>,
) -> Vec<DecisionCandidate> {
    let ranked_scan = rank_scan_candidates(scan_candidates);
    let ranked_pattern = rank_pattern_candidates(pattern_candidates);
    let mut keys = BTreeSet::new();
    let mut scan_by_key = BTreeMap::new();
    let mut pattern_by_key = BTreeMap::new();

    for candidate in ranked_scan {
        let key = candidate_key(&candidate.candidate.code, &candidate.candidate.horizon);
        keys.insert(key.clone());
        scan_by_key.insert(key, candidate);
    }

    for candidate in ranked_pattern {
        let key = candidate_key(&candidate.candidate.code, &candidate.candidate.horizon);
        keys.insert(key.clone());
        pattern_by_key.insert(key, candidate);
    }

    let mut candidates = Vec::with_capacity(keys.len());
    for key in keys {
        let scan_candidate = scan_by_key.get(&key);
        let pattern_candidate = pattern_by_key.get(&key);
        if scan_candidate.is_none() && pattern_candidate.is_none() {
            continue;
        }

        let base_source = match (scan_candidate.is_some(), pattern_candidate.is_some()) {
            (true, true) => "combined",
            (true, false) => "scan_ranker",
            (false, true) => "pattern_shadow",
            (false, false) => continue,
        };
        let name = scan_candidate
            .map(|candidate| candidate.candidate.name.clone())
            .or_else(|| pattern_candidate.and_then(|candidate| candidate.candidate.name.clone()))
            .unwrap_or_else(|| key.0.clone());
        let horizon = scan_candidate
            .map(|candidate| candidate.candidate.horizon.clone())
            .or_else(|| pattern_candidate.map(|candidate| candidate.candidate.horizon.clone()))
            .unwrap_or_else(|| key.1.clone());
        let base_score = scan_candidate
            .map(|candidate| candidate.scan_ranker_percentile)
            .unwrap_or(0.0)
            .max(
                pattern_candidate
                    .map(|candidate| candidate.pattern_percentile)
                    .unwrap_or(0.0),
            );
        let pattern_score = pattern_candidate.map(|candidate| candidate.pattern_percentile);
        let support_tier = merged_support_tier(
            scan_candidate.map(|candidate| &candidate.candidate),
            pattern_candidate.map(|candidate| &candidate.candidate),
        );
        let disagreement = has_reject_disagreement(
            scan_candidate.map(|candidate| &candidate.candidate),
            pattern_candidate.map(|candidate| &candidate.candidate),
        );
        let mut statements = Vec::new();
        let mut risk_flags = Vec::new();
        let mut invalidations = Vec::new();

        if let Some(scan_candidate) = scan_candidate {
            statements.extend(scan_support_statements(scan_candidate));
            risk_flags.extend(scan_candidate.candidate.risk_flags.clone());
        }
        if let Some(pattern_candidate) = pattern_candidate {
            statements.extend(pattern_support_statements(pattern_candidate));
            risk_flags.extend(pattern_risk_flags(&pattern_candidate.candidate.risk_flags));
            invalidations.extend(pattern_invalidations(
                &pattern_candidate.candidate.invalidations,
            ));
        }
        if disagreement {
            risk_flags.push("scan_pattern_disagreement".to_string());
        }

        dedupe_strings(&mut risk_flags);
        dedupe_strings(&mut invalidations);
        let (facts, calculations, inferences, unknowns) = classify_statements(statements);

        candidates.push(DecisionCandidate {
            code: key.0,
            name,
            horizon,
            base_source: base_source.to_string(),
            base_score,
            pattern_score,
            event_adjustment: 0.0,
            risk_adjustment: 0.0,
            final_score: base_score,
            support_tier: support_tier.as_str().to_string(),
            facts,
            calculations,
            inferences,
            unknowns,
            risk_flags,
            invalidations,
            event_score_audit: Vec::new(),
        });
    }

    candidates.sort_by(|left, right| {
        right
            .final_score
            .total_cmp(&left.final_score)
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.horizon.cmp(&right.horizon))
    });
    candidates
}

fn candidate_key(code: &str, horizon: &str) -> (String, String) {
    (code.to_string(), horizon.to_string())
}

fn rank_scan_candidates(mut candidates: Vec<BaselineCandidate>) -> Vec<RankedScanCandidate> {
    candidates.sort_by(|left, right| {
        right
            .base_score
            .total_cmp(&left.base_score)
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.horizon.cmp(&right.horizon))
            .then_with(|| left.pool_id.cmp(&right.pool_id))
            .then_with(|| left.trigger_id.cmp(&right.trigger_id))
    });

    let total = candidates.len();
    candidates
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| RankedScanCandidate {
            candidate,
            scan_ranker_percentile: rank_percentile(index, total),
        })
        .collect()
}

fn rank_pattern_candidates(candidates: Vec<ShadowCandidateRow>) -> Vec<RankedPatternCandidate> {
    let mut best_by_key: BTreeMap<(String, String), ShadowCandidateRow> = BTreeMap::new();

    for candidate in candidates {
        let key = candidate_key(&candidate.code, &candidate.horizon);
        match best_by_key.get(&key) {
            Some(current) if current.final_score >= candidate.final_score => {}
            _ => {
                best_by_key.insert(key, candidate);
            }
        }
    }

    let mut candidates: Vec<ShadowCandidateRow> = best_by_key.into_values().collect();
    candidates.sort_by(|left, right| {
        right
            .final_score
            .total_cmp(&left.final_score)
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.horizon.cmp(&right.horizon))
            .then_with(|| left.pattern_version_id.cmp(&right.pattern_version_id))
    });

    let total = candidates.len();
    candidates
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| RankedPatternCandidate {
            candidate,
            pattern_percentile: rank_percentile(index, total),
        })
        .collect()
}

fn rank_percentile(index: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        ((total - index) as f64 / total as f64) * 100.0
    }
}

fn merged_support_tier(
    scan_candidate: Option<&BaselineCandidate>,
    pattern_candidate: Option<&ShadowCandidateRow>,
) -> SupportTier {
    let scan_tier = scan_candidate.map(|candidate| scan_support_tier(&candidate.tier));
    let pattern_tier =
        pattern_candidate.map(|candidate| pattern_support_tier(&candidate.shadow_tier));

    match (scan_tier, pattern_tier) {
        (Some(scan_tier), Some(pattern_tier))
            if scan_tier.is_positive() && pattern_tier == SupportTier::Reject =>
        {
            SupportTier::Watch
        }
        (Some(scan_tier), Some(pattern_tier)) => scan_tier.max(pattern_tier),
        (Some(scan_tier), None) => scan_tier,
        (None, Some(pattern_tier)) => pattern_tier,
        (None, None) => SupportTier::Reject,
    }
}

fn has_reject_disagreement(
    scan_candidate: Option<&BaselineCandidate>,
    pattern_candidate: Option<&ShadowCandidateRow>,
) -> bool {
    let Some(scan_candidate) = scan_candidate else {
        return false;
    };
    let Some(pattern_candidate) = pattern_candidate else {
        return false;
    };

    scan_support_tier(&scan_candidate.tier).is_positive()
        && pattern_support_tier(&pattern_candidate.shadow_tier) == SupportTier::Reject
}

fn scan_support_tier(tier: &str) -> SupportTier {
    match tier {
        "A" => SupportTier::A,
        "B" => SupportTier::B,
        "watch" => SupportTier::Watch,
        _ => SupportTier::Reject,
    }
}

fn pattern_support_tier(tier: &str) -> SupportTier {
    match tier {
        "shadow_a" => SupportTier::A,
        "shadow_b" => SupportTier::B,
        "watch" => SupportTier::Watch,
        _ => SupportTier::Reject,
    }
}

fn scan_support_statements(candidate: &RankedScanCandidate) -> Vec<SupportStatement> {
    let mut statements = Vec::with_capacity(candidate.candidate.reasons.len() + 2);
    statements.push(SupportStatement::new(
        crate::analysis::decision_support::contracts::SupportStatementKind::OtherCalculation,
        format!(
            "Scan-ranker raw score {:.2} mapped to {} percentile {:.2}.",
            candidate.candidate.base_score, "scan rank", candidate.scan_ranker_percentile
        ),
        vec![
            "scan_ranker".to_string(),
            candidate.candidate.trigger_id.clone(),
        ],
    ));
    statements.push(SupportStatement::new(
        crate::analysis::decision_support::contracts::SupportStatementKind::OtherInference,
        format!(
            "Scan-ranker assigned {} tier for the {} horizon.",
            candidate.candidate.tier, candidate.candidate.horizon
        ),
        vec![
            "scan_ranker".to_string(),
            candidate.candidate.pool_id.clone(),
        ],
    ));
    for reason in &candidate.candidate.reasons {
        statements.push(SupportStatement::new(
            crate::analysis::decision_support::contracts::SupportStatementKind::OtherInference,
            reason.clone(),
            vec![
                "scan_ranker".to_string(),
                candidate.candidate.trigger_id.clone(),
            ],
        ));
    }
    statements
}

fn pattern_support_statements(candidate: &RankedPatternCandidate) -> Vec<SupportStatement> {
    vec![
        SupportStatement::pattern_similarity(format!(
            "Pattern similarity score {:.4} ranked at percentile {:.2}.",
            candidate.candidate.similarity_score, candidate.pattern_percentile
        )),
        SupportStatement::pattern_lift(format!(
            "Pattern validated lift is {:.4} with raw final score {:.4}.",
            candidate.candidate.validated_lift, candidate.candidate.final_score
        )),
        SupportStatement::new(
            crate::analysis::decision_support::contracts::SupportStatementKind::OtherInference,
            format!(
                "Pattern shadow tier is {} for the {} horizon.",
                candidate.candidate.shadow_tier, candidate.candidate.horizon
            ),
            vec![
                "pattern_shadow".to_string(),
                candidate.candidate.pattern_version_id.to_string(),
            ],
        ),
    ]
}

fn pattern_risk_flags(value: &Value) -> Vec<String> {
    let mut flags = Vec::new();

    if let Some(items) = value.as_array() {
        for item in items {
            if let Some(flag) = item.as_str() {
                flags.push(flag.to_string());
            }
        }
        return flags;
    }

    if value
        .get("has_triggered")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        flags.push("pattern_risk_triggered".to_string());
    }
    if value
        .get("has_unevaluable")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        flags.push("pattern_risk_unevaluable".to_string());
    }
    if let Some(items) = value.get("triggered").and_then(Value::as_array) {
        for item in items {
            if let Some(feature) = item.get("feature").and_then(Value::as_str) {
                flags.push(format!("pattern_risk:{feature}"));
            } else if let Some(status) = item.get("status").and_then(Value::as_str) {
                flags.push(format!("pattern_risk:{status}"));
            }
        }
    }
    if let Some(items) = value.get("unevaluable").and_then(Value::as_array) {
        for item in items {
            if let Some(feature) = item.get("feature").and_then(Value::as_str) {
                flags.push(format!("pattern_unevaluable:{feature}"));
            } else if let Some(status) = item.get("status").and_then(Value::as_str) {
                flags.push(format!("pattern_unevaluable:{status}"));
            }
        }
    }

    flags
}

fn pattern_invalidations(value: &Value) -> Vec<String> {
    let Some(items) = value.as_array() else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| {
            item.as_str().map(ToString::to_string).or_else(|| {
                let reason = item.get("reason").and_then(Value::as_str)?;
                let detail = item.get("detail").and_then(Value::as_str).unwrap_or("");
                if detail.is_empty() {
                    Some(reason.to_string())
                } else {
                    Some(format!("{reason}: {detail}"))
                }
            })
        })
        .collect()
}

fn dedupe_strings(values: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    values.retain(|value| seen.insert(value.clone()));
}
