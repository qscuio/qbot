use chrono::NaiveDate;
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::analysis::decision_support::contracts::{
    DailyDecisionSupport, DataStatus, DecisionCandidate, DecisionSupportConfig,
    EventScoreAdjustmentAudit, MarketSummary, StatementBucket, SupportStatement,
};
use crate::analysis::decision_support::event_adapter::apply_event_context;
use crate::analysis::decision_support::pattern_adapter::build_decision_candidates;
use crate::analysis::decision_support::scan_ranker_adapter::load_scan_ranker_baseline;
use crate::analysis::events::DailyEventBrief;
use crate::error::AppError;
use crate::error::Result;
use crate::storage::decision_support_repository::DecisionSupportRepository;
use crate::storage::decision_support_repository::DecisionSupportRunRow;
use crate::storage::decision_support_repository::{DecisionBriefRow, DecisionCandidateRow};
use crate::storage::event_repository::DailyEventBriefRow;
use crate::storage::event_repository::EventRepository;
use crate::storage::market_repository::DataStatusSnapshot;
use crate::storage::market_repository::MarketRepository;
use crate::storage::pattern_repository::PatternRepository;
use crate::storage::pattern_repository::PatternSetRow;

#[derive(Clone)]
pub struct DecisionSupport {
    pool: PgPool,
    market_repo: MarketRepository,
    pattern_repo: PatternRepository,
    event_repo: EventRepository,
    decision_repo: DecisionSupportRepository,
}

impl DecisionSupport {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool: pool.clone(),
            market_repo: MarketRepository::new(pool.clone()),
            pattern_repo: PatternRepository::new(pool.clone()),
            event_repo: EventRepository::new(pool.clone()),
            decision_repo: DecisionSupportRepository::new(pool),
        }
    }

    pub async fn build_daily(
        &self,
        trade_date: NaiveDate,
        config: DecisionSupportConfig,
    ) -> Result<DailyDecisionSupport> {
        let snapshot = self
            .market_repo
            .market_snapshot(trade_date, &config.market_snapshot_version)
            .await?
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "market snapshot not found for {} with version {}",
                    trade_date, config.market_snapshot_version
                ))
            })?;
        let latest_status = self
            .market_repo
            .latest_market_snapshot(&config.market_snapshot_version)
            .await?;
        let scan_candidates = load_scan_ranker_baseline(&self.pool, trade_date).await?;
        let pattern_candidates = self.pattern_repo.list_shadow_candidates(trade_date).await?;
        let pattern_set = self.pattern_repo.latest_published_set().await?;
        let event_brief_row = self.event_repo.find_daily_brief(Some(trade_date)).await?;
        let event_summary = event_brief_row
            .as_ref()
            .map(parse_event_summary)
            .transpose()?;
        let candidates = apply_event_context(
            trade_date,
            build_decision_candidates(scan_candidates, pattern_candidates),
            event_summary.as_ref(),
            &self.event_repo,
            &self.market_repo,
        )
        .await?;
        let data_status = data_status_from_snapshot(
            trade_date,
            &config.market_snapshot_version,
            latest_status.as_ref(),
            &snapshot.input_fingerprint,
            snapshot.available_at,
            snapshot.data_complete,
            &snapshot.missing_inputs,
        );
        let candidates =
            apply_event_score_adjustments(candidates, &config, data_status.data_complete);
        let run_id = if config.persist_run {
            self.persist_run_with_artifacts(
                trade_date,
                &config,
                &snapshot.input_fingerprint,
                pattern_set.as_ref(),
                event_brief_row.as_ref(),
                &candidates,
                event_summary.as_ref(),
            )
            .await?
        } else {
            Uuid::new_v4()
        };

        Ok(DailyDecisionSupport {
            trade_date,
            run_id,
            candidates,
            market_summary: MarketSummary {
                trade_date: snapshot.trade_date,
                snapshot_version: snapshot.snapshot_version,
                available_at: snapshot.available_at,
                data_complete: snapshot.data_complete,
                metrics: snapshot.metrics,
                missing_inputs: snapshot.missing_inputs,
                input_fingerprint: snapshot.input_fingerprint.clone(),
            },
            event_summary,
            data_status,
        })
    }

    async fn persist_run_with_artifacts(
        &self,
        trade_date: NaiveDate,
        config: &DecisionSupportConfig,
        market_input_fingerprint: &str,
        pattern_set: Option<&PatternSetRow>,
        event_brief_row: Option<&DailyEventBriefRow>,
        candidates: &[DecisionCandidate],
        event_summary: Option<&DailyEventBrief>,
    ) -> Result<Uuid> {
        let started_at = Utc::now();
        let run_id = Uuid::new_v4();
        let input_fingerprint = decision_support_input_fingerprint(
            trade_date,
            config,
            market_input_fingerprint,
            pattern_set,
            event_brief_row,
        );
        let candidate_rows = candidates
            .iter()
            .map(|candidate| decision_candidate_row(run_id, candidate, started_at))
            .collect::<Result<Vec<_>>>()?;
        let brief = DecisionBriefRow {
            run_id,
            trade_date,
            content: decision_brief_content(trade_date, candidates, event_summary),
            structured_payload: decision_brief_payload(trade_date, candidates, event_summary),
            created_at: started_at,
        };

        self.decision_repo
            .create_run_with_artifacts(
                &DecisionSupportRunRow {
                    run_id,
                    trade_date,
                    support_version: config.support_version.clone(),
                    market_snapshot_version: config.market_snapshot_version.clone(),
                    pattern_set_id: pattern_set.map(|row| row.pattern_set_id),
                    event_brief_version: event_brief_row.map(|row| row.brief_version.clone()),
                    event_score_enabled: config.event_score_enabled,
                    event_score_limit: config.event_score_limit,
                    status: "completed".to_string(),
                    input_fingerprint,
                    started_at,
                    completed_at: Some(started_at),
                    error_message: None,
                },
                &candidate_rows,
                &brief,
            )
            .await?;

        Ok(run_id)
    }
}

pub(crate) fn classify_statements(
    statements: Vec<SupportStatement>,
) -> (
    Vec<SupportStatement>,
    Vec<SupportStatement>,
    Vec<SupportStatement>,
    Vec<SupportStatement>,
) {
    let mut facts = Vec::new();
    let mut calculations = Vec::new();
    let mut inferences = Vec::new();
    let mut unknowns = Vec::new();

    for statement in statements {
        match statement.bucket() {
            StatementBucket::Fact => facts.push(statement),
            StatementBucket::Calculation => calculations.push(statement),
            StatementBucket::Inference => inferences.push(statement),
            StatementBucket::Unknown => unknowns.push(statement),
        }
    }

    (facts, calculations, inferences, unknowns)
}

fn parse_event_summary(row: &DailyEventBriefRow) -> Result<DailyEventBrief> {
    serde_json::from_value(row.structured_payload.clone()).map_err(Into::into)
}

fn data_status_from_snapshot(
    requested_trade_date: NaiveDate,
    snapshot_version: &str,
    latest_status: Option<&DataStatusSnapshot>,
    fallback_input_fingerprint: &str,
    fallback_available_at: chrono::DateTime<Utc>,
    fallback_data_complete: bool,
    fallback_missing_inputs: &[String],
) -> DataStatus {
    if let Some(latest_status) = latest_status {
        DataStatus {
            requested_trade_date,
            latest_trade_date: Some(latest_status.trade_date),
            snapshot_version: latest_status.snapshot_version.clone(),
            available_at: Some(latest_status.available_at),
            data_complete: latest_status.data_complete,
            missing_inputs: latest_status.missing_inputs.clone(),
            input_fingerprint: Some(latest_status.input_fingerprint.clone()),
        }
    } else {
        DataStatus {
            requested_trade_date,
            latest_trade_date: Some(requested_trade_date),
            snapshot_version: snapshot_version.to_string(),
            available_at: Some(fallback_available_at),
            data_complete: fallback_data_complete,
            missing_inputs: fallback_missing_inputs.to_vec(),
            input_fingerprint: Some(fallback_input_fingerprint.to_string()),
        }
    }
}

fn decision_support_input_fingerprint(
    trade_date: NaiveDate,
    config: &DecisionSupportConfig,
    market_input_fingerprint: &str,
    pattern_set: Option<&PatternSetRow>,
    event_brief_row: Option<&DailyEventBriefRow>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(trade_date.to_string().as_bytes());
    hasher.update(config.support_version.as_bytes());
    hasher.update(config.market_snapshot_version.as_bytes());
    hasher.update(market_input_fingerprint.as_bytes());
    hasher.update(if config.event_score_enabled {
        b"1"
    } else {
        b"0"
    });
    hasher.update(config.event_score_limit.to_string().as_bytes());
    if let Some(pattern_set) = pattern_set {
        hasher.update(pattern_set.pattern_set_id.as_bytes());
        hasher.update(pattern_set.name.as_bytes());
    }
    if let Some(event_brief_row) = event_brief_row {
        hasher.update(event_brief_row.brief_version.as_bytes());
        hasher.update(event_brief_row.input_fingerprint.as_bytes());
    }

    format!("{:x}", hasher.finalize())
}

fn apply_event_score_adjustments(
    mut candidates: Vec<DecisionCandidate>,
    config: &DecisionSupportConfig,
    data_complete: bool,
) -> Vec<DecisionCandidate> {
    let cap = config.event_score_limit.clamp(0.0, 5.0);

    for candidate in &mut candidates {
        let mut total_adjustment = 0.0;
        for audit in &mut candidate.event_score_audit {
            audit.raw_adjustment =
                raw_event_adjustment(audit.market_alignment, audit.causal_confidence);
            audit.applied_adjustment = 0.0;
            audit.cap = if config.event_score_enabled { cap } else { 0.0 };
            audit.reason = event_adjustment_reason(
                audit,
                config.event_score_enabled,
                cap,
                data_complete,
                total_adjustment,
            );
            total_adjustment += audit.applied_adjustment;
        }

        candidate.event_adjustment = total_adjustment;
        candidate.final_score = candidate.base_score + candidate.risk_adjustment + total_adjustment;
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

fn raw_event_adjustment(market_alignment: Option<f64>, causal_confidence: Option<f64>) -> f64 {
    market_alignment.unwrap_or(0.0) * causal_confidence.unwrap_or(0.0) * 10.0
}

fn event_adjustment_reason(
    audit: &mut EventScoreAdjustmentAudit,
    event_score_enabled: bool,
    cap: f64,
    data_complete: bool,
    total_adjustment: f64,
) -> String {
    if !matches!(
        audit.entity_relation.as_str(),
        "direct_entity" | "reviewed_industry"
    ) {
        return "event relation is not eligible for score adjustment".to_string();
    }

    if !event_score_enabled {
        return "event score adjustment disabled by config".to_string();
    }

    if audit.market_alignment.is_none() || audit.causal_confidence.is_none() {
        return "reviewed market alignment or causal confidence is missing".to_string();
    }

    if !data_complete && audit.raw_adjustment > 0.0 {
        return "market data incomplete; positive event adjustment blocked".to_string();
    }

    let applied = capped_event_adjustment(audit.raw_adjustment, cap, total_adjustment);
    audit.applied_adjustment = applied;
    if applied == audit.raw_adjustment {
        "eligible event adjustment applied".to_string()
    } else {
        format!("eligible event adjustment capped at {cap:.2}")
    }
}

fn capped_event_adjustment(raw_adjustment: f64, cap: f64, total_adjustment: f64) -> f64 {
    if cap <= 0.0 {
        return 0.0;
    }

    let max_increase = cap - total_adjustment;
    let max_decrease = -cap - total_adjustment;
    raw_adjustment.clamp(max_decrease, max_increase)
}

fn decision_candidate_row(
    run_id: Uuid,
    candidate: &DecisionCandidate,
    created_at: chrono::DateTime<Utc>,
) -> Result<DecisionCandidateRow> {
    Ok(DecisionCandidateRow {
        run_id,
        code: candidate.code.clone(),
        name: candidate.name.clone(),
        horizon: candidate.horizon.clone(),
        base_source: candidate.base_source.clone(),
        base_score: candidate.base_score,
        pattern_score: candidate.pattern_score,
        event_adjustment: Some(candidate.event_adjustment),
        risk_adjustment: Some(candidate.risk_adjustment),
        final_score: candidate.final_score,
        support_tier: candidate.support_tier.clone(),
        facts: serde_json::to_value(&candidate.facts)?,
        calculations: serde_json::to_value(&candidate.calculations)?,
        inferences: serde_json::to_value(&candidate.inferences)?,
        unknowns: serde_json::to_value(&candidate.unknowns)?,
        risk_flags: serde_json::to_value(&candidate.risk_flags)?,
        invalidations: serde_json::to_value(&candidate.invalidations)?,
        source_refs: serde_json::to_value(candidate_source_refs(candidate))?,
        created_at,
    })
}

fn candidate_source_refs(candidate: &DecisionCandidate) -> Vec<String> {
    let mut refs = Vec::new();
    for statement in candidate
        .facts
        .iter()
        .chain(candidate.calculations.iter())
        .chain(candidate.inferences.iter())
        .chain(candidate.unknowns.iter())
    {
        for source_ref in &statement.source_refs {
            if !refs.iter().any(|existing| existing == source_ref) {
                refs.push(source_ref.clone());
            }
        }
    }
    refs
}

fn decision_brief_content(
    trade_date: NaiveDate,
    candidates: &[DecisionCandidate],
    event_summary: Option<&DailyEventBrief>,
) -> String {
    let top_candidates = candidates
        .iter()
        .take(3)
        .map(|candidate| {
            format!(
                "{} {} {:.2}",
                candidate.code, candidate.horizon, candidate.final_score
            )
        })
        .collect::<Vec<_>>();

    let event_counts = event_summary
        .map(|summary| {
            format!(
                "facts={} revisions={} unconfirmed={}",
                summary.new_facts.len(),
                summary.revisions.len(),
                summary.unconfirmed.len()
            )
        })
        .unwrap_or_else(|| "facts=0 revisions=0 unconfirmed=0".to_string());

    if top_candidates.is_empty() {
        format!(
            "DecisionSupport {} persisted with 0 candidates; event summary {}",
            trade_date, event_counts
        )
    } else {
        format!(
            "DecisionSupport {} persisted with {} candidates; top: {}; event summary {}",
            trade_date,
            candidates.len(),
            top_candidates.join(", "),
            event_counts
        )
    }
}

fn decision_brief_payload(
    trade_date: NaiveDate,
    candidates: &[DecisionCandidate],
    event_summary: Option<&DailyEventBrief>,
) -> serde_json::Value {
    serde_json::json!({
        "tradeDate": trade_date,
        "candidateCount": candidates.len(),
        "topCandidates": candidates.iter().take(5).map(|candidate| serde_json::json!({
            "code": candidate.code,
            "name": candidate.name,
            "horizon": candidate.horizon,
            "supportTier": candidate.support_tier,
            "finalScore": candidate.final_score,
        })).collect::<Vec<_>>(),
        "eventSummary": event_summary.map(|summary| serde_json::json!({
            "newFactCount": summary.new_facts.len(),
            "revisionCount": summary.revisions.len(),
            "unconfirmedCount": summary.unconfirmed.len(),
            "directEntityCount": summary.direct_entities.len(),
            "sourceCount": summary.sources.len(),
            "inputFingerprint": summary.input_fingerprint,
        })),
    })
}

#[cfg(test)]
mod tests {
    use super::{apply_event_score_adjustments, classify_statements};
    use crate::analysis::decision_support::scan_ranker_adapter::BaselineCandidate;
    use crate::analysis::decision_support::{
        pattern_adapter::build_decision_candidates, DecisionSupport, DecisionSupportConfig,
        EventScoreAdjustmentAudit, SupportStatement,
    };
    use crate::analysis::events::DailyEventBrief;
    use crate::services::scan_ranker::POOL_SHORT_A_ID;
    use crate::storage::decision_support_repository::DecisionSupportRepository;
    use crate::storage::event_repository::{DailyEventBriefRow, EventRepository};
    use crate::storage::market_repository::MarketRepository;
    use crate::storage::pattern_repository::ShadowCandidateRow;
    use crate::storage::postgres::{save_daily_signal_scan_results, DailySignalScanRow};
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    fn baseline_candidate(
        code: &str,
        name: &str,
        horizon: &str,
        tier: &str,
        base_score: f64,
        reasons: &[&str],
    ) -> BaselineCandidate {
        BaselineCandidate {
            code: code.to_string(),
            name: name.to_string(),
            horizon: horizon.to_string(),
            line_type: horizon.to_string(),
            pool_id: format!("{horizon}-{tier}-pool"),
            pool_name: format!("{horizon}-{tier} pool"),
            tier: tier.to_string(),
            base_source: "scan_ranker".to_string(),
            base_score,
            trigger_id: "breakout".to_string(),
            trigger_name: "Breakout".to_string(),
            reasons: reasons.iter().map(|reason| reason.to_string()).collect(),
            risk_flags: vec!["thin_volume".to_string()],
            factor_breakdown: vec![("trend".to_string(), 18.0)],
        }
    }

    fn shadow_candidate(
        code: &str,
        name: &str,
        horizon: &str,
        shadow_tier: &str,
        final_score: f64,
        similarity_score: f64,
        validated_lift: f64,
    ) -> ShadowCandidateRow {
        ShadowCandidateRow {
            trade_date: date(2026, 7, 11),
            code: code.to_string(),
            name: Some(name.to_string()),
            horizon: horizon.to_string(),
            pattern_version_id: Uuid::new_v4(),
            pattern_set_id: Uuid::new_v4(),
            pattern_type: "strong_stock".to_string(),
            similarity_score,
            validated_lift,
            final_score,
            shadow_tier: shadow_tier.to_string(),
            matched_features: json!({"raw": {"relative_strength_20d": 1.2}}),
            risk_flags: json!({
                "has_triggered": shadow_tier == "shadow_b",
                "has_unevaluable": false,
                "triggered": if shadow_tier == "shadow_b" {
                    json!([{"feature": "extension_penalty", "status": "evaluated"}])
                } else {
                    json!([])
                },
                "unevaluable": [],
                "risk_adjustment": 0.5
            }),
            supporting_signals: json!({
                "score_components": {
                    "validated_pattern_strength": 0.7,
                    "current_similarity": 0.2,
                    "risk_adjustment": 0.5
                }
            }),
            invalidations: if shadow_tier == "reject" {
                json!([{
                    "reason": "insufficient_bar_history",
                    "feature": "price_vs_ma50",
                    "detail": "need 50 bars"
                }])
            } else {
                json!([])
            },
            input_fingerprint: format!("fp-{code}-{horizon}-{shadow_tier}"),
            created_at: dt(2026, 7, 11, 18),
        }
    }

    fn decision_candidate(
        code: &str,
        support_tier: &str,
        base_score: f64,
        risk_adjustment: f64,
        event_score_audit: Vec<EventScoreAdjustmentAudit>,
    ) -> crate::analysis::decision_support::DecisionCandidate {
        crate::analysis::decision_support::DecisionCandidate {
            code: code.to_string(),
            name: format!("Name {code}"),
            horizon: "short".to_string(),
            base_source: "scan_ranker".to_string(),
            base_score,
            pattern_score: None,
            event_adjustment: 0.0,
            risk_adjustment,
            final_score: base_score + risk_adjustment,
            support_tier: support_tier.to_string(),
            facts: Vec::new(),
            calculations: Vec::new(),
            inferences: Vec::new(),
            unknowns: Vec::new(),
            risk_flags: Vec::new(),
            invalidations: Vec::new(),
            event_score_audit,
        }
    }

    fn audit_entry(
        entity_relation: &str,
        market_alignment: Option<f64>,
        causal_confidence: Option<f64>,
    ) -> EventScoreAdjustmentAudit {
        EventScoreAdjustmentAudit {
            event_id: Uuid::new_v4(),
            entity_relation: entity_relation.to_string(),
            market_alignment,
            causal_confidence,
            raw_adjustment: 0.0,
            applied_adjustment: 0.0,
            cap: 0.0,
            reason: String::new(),
        }
    }

    async fn seed_market_snapshot(pool: &PgPool, trade_date: NaiveDate) {
        MarketRepository::new(pool.clone())
            .save_market_snapshot(&crate::analysis::market_snapshot::MarketSnapshot {
                trade_date,
                snapshot_version: crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION
                    .to_string(),
                available_at: dt(2026, 7, 11, 18),
                data_complete: true,
                metrics: json!({"breadth": {"up_count": 123}}),
                missing_inputs: Vec::new(),
                input_fingerprint: "market-fingerprint".to_string(),
            })
            .await
            .unwrap();
    }

    async fn seed_ranked_pool_candidate(
        pool: &PgPool,
        trade_date: NaiveDate,
        code: &str,
        name: &str,
        score: f64,
        line_type: &str,
    ) {
        save_daily_signal_scan_results(
            pool,
            trade_date,
            Uuid::new_v4(),
            &[DailySignalScanRow {
                code: code.to_string(),
                name: name.to_string(),
                signal_id: POOL_SHORT_A_ID.to_string(),
                signal_name: "短线A档".to_string(),
                icon: "🔥".to_string(),
                metadata: json!({
                    "line_type": line_type,
                    "tier": "A",
                    "trigger_id": "breakout",
                    "trigger_name": "突破信号",
                    "score": score,
                    "reasons": ["突破确认"],
                    "risk_flags": ["量能不足"],
                    "factor_breakdown": [
                        {"name": "trend", "score": 18.5},
                        {"name": "volume", "score": 11.2}
                    ],
                    "supporting_signals": ["breakout"],
                    "matched_setups": [{"id": "breakout", "name": "突破信号"}]
                }),
            }],
        )
        .await
        .unwrap();
    }

    #[test]
    fn classifies_support_statements_into_reason_buckets() {
        let statements = vec![
            SupportStatement::event_fact(
                "Company confirmed a production expansion.",
                vec!["evidence:1".to_string()],
            ),
            SupportStatement::pattern_similarity("Similarity score is 0.82."),
            SupportStatement::pattern_lift("Validated lift is 0.18."),
            SupportStatement::impact_hypothesis(
                "Expansion may tighten sector supply over the next quarter.",
            ),
            SupportStatement::missing_status("Missing security status snapshot for 600000."),
        ];

        let (facts, calculations, inferences, unknowns) = classify_statements(statements);

        assert_eq!(facts.len(), 1);
        assert_eq!(calculations.len(), 2);
        assert_eq!(inferences.len(), 1);
        assert_eq!(unknowns.len(), 1);
        assert_eq!(facts[0].source_refs, vec!["evidence:1".to_string()]);
    }

    #[test]
    fn build_decision_candidates_merges_scan_and_pattern_sources_without_rescaling_raw_scores() {
        let candidates = build_decision_candidates(
            vec![
                baseline_candidate(
                    "600000.SH",
                    "Pudong Bank",
                    "short",
                    "A",
                    91.4,
                    &["Scan sees a short-term breakout."],
                ),
                baseline_candidate(
                    "000001.SZ",
                    "Ping An Bank",
                    "mid",
                    "B",
                    81.2,
                    &["Scan sees follow-through."],
                ),
            ],
            vec![
                shadow_candidate(
                    "600000.SH",
                    "Pudong Bank",
                    "short",
                    "shadow_b",
                    1.72,
                    0.81,
                    1.14,
                ),
                shadow_candidate(
                    "300001.SZ",
                    "Tech One",
                    "week",
                    "shadow_a",
                    2.31,
                    0.93,
                    1.28,
                ),
            ],
        );

        assert_eq!(candidates.len(), 3);

        let combined = candidates
            .iter()
            .find(|candidate| candidate.code == "600000.SH" && candidate.horizon == "short")
            .expect("expected combined candidate");
        assert_eq!(combined.base_source, "combined");
        assert_eq!(combined.base_score, 100.0);
        assert_eq!(combined.pattern_score, Some(50.0));
        assert_eq!(combined.final_score, combined.base_score);
        assert_eq!(combined.support_tier, "A");
        assert_eq!(combined.event_adjustment, 0.0);
        assert!(combined
            .inferences
            .iter()
            .any(|statement| statement.statement.contains("short-term breakout")));
        assert!(combined
            .calculations
            .iter()
            .any(|statement| statement.statement.contains("Pattern similarity score")));

        let scan_only = candidates
            .iter()
            .find(|candidate| candidate.code == "000001.SZ")
            .expect("expected scan-only candidate");
        assert_eq!(scan_only.base_source, "scan_ranker");
        assert_eq!(scan_only.base_score, 50.0);
        assert_eq!(scan_only.pattern_score, None);
        assert_eq!(scan_only.support_tier, "B");

        let pattern_only = candidates
            .iter()
            .find(|candidate| candidate.code == "300001.SZ")
            .expect("expected pattern-only candidate");
        assert_eq!(pattern_only.base_source, "pattern_shadow");
        assert_eq!(pattern_only.base_score, 100.0);
        assert_eq!(pattern_only.pattern_score, Some(100.0));
        assert_eq!(pattern_only.support_tier, "A");
    }

    #[test]
    fn build_decision_candidates_adds_disagreement_risk_when_pattern_rejects_scan_a() {
        let candidates = build_decision_candidates(
            vec![baseline_candidate(
                "600000.SH",
                "Pudong Bank",
                "short",
                "A",
                91.4,
                &["Scan sees a short-term breakout."],
            )],
            vec![shadow_candidate(
                "600000.SH",
                "Pudong Bank",
                "short",
                "reject",
                0.42,
                0.33,
                0.96,
            )],
        );

        let candidate = &candidates[0];
        assert_eq!(candidate.base_source, "combined");
        assert_ne!(candidate.support_tier, "A");
        assert!(candidate
            .risk_flags
            .iter()
            .any(|flag| flag == "scan_pattern_disagreement"));
        assert!(candidate
            .inferences
            .iter()
            .any(|statement| statement.statement.contains("short-term breakout")));
        assert!(candidate
            .calculations
            .iter()
            .any(|statement| statement.statement.contains("validated lift")));
        assert!(candidate
            .invalidations
            .iter()
            .any(|item| item.contains("insufficient_bar_history")));
    }

    #[test]
    fn disabled_event_adjustment_keeps_score_zero_and_records_attempted_audit() {
        let event_id = Uuid::new_v4();
        let candidates = apply_event_score_adjustments(
            vec![decision_candidate(
                "600519.SH",
                "watch",
                82.0,
                -1.5,
                vec![EventScoreAdjustmentAudit {
                    event_id,
                    entity_relation: "direct_entity".to_string(),
                    market_alignment: Some(1.0),
                    causal_confidence: Some(1.0),
                    raw_adjustment: 0.0,
                    applied_adjustment: 0.0,
                    cap: 0.0,
                    reason: String::new(),
                }],
            )],
            &DecisionSupportConfig {
                event_score_enabled: false,
                event_score_limit: 5.0,
                ..DecisionSupportConfig::default()
            },
            true,
        );

        let candidate = &candidates[0];
        let audit = &candidate.event_score_audit[0];
        assert_eq!(candidate.event_adjustment, 0.0);
        assert_eq!(candidate.final_score, 80.5);
        assert_eq!(audit.event_id, event_id);
        assert_eq!(audit.entity_relation, "direct_entity");
        assert_eq!(audit.market_alignment, Some(1.0));
        assert_eq!(audit.causal_confidence, Some(1.0));
        assert_eq!(audit.raw_adjustment, 10.0);
        assert_eq!(audit.applied_adjustment, 0.0);
        assert_eq!(audit.cap, 0.0);
        assert!(audit.reason.contains("disabled"));
    }

    #[test]
    fn configured_event_adjustment_limit_is_hard_capped_at_five_points() {
        let candidates = apply_event_score_adjustments(
            vec![decision_candidate(
                "600519.SH",
                "watch",
                82.0,
                -1.5,
                vec![audit_entry("direct_entity", Some(1.0), Some(1.0))],
            )],
            &DecisionSupportConfig {
                event_score_enabled: true,
                event_score_limit: 10.0,
                ..DecisionSupportConfig::default()
            },
            true,
        );

        let candidate = &candidates[0];
        let audit = &candidate.event_score_audit[0];
        assert_eq!(candidate.event_adjustment, 5.0);
        assert_eq!(candidate.final_score, 85.5);
        assert_eq!(audit.raw_adjustment, 10.0);
        assert_eq!(audit.applied_adjustment, 5.0);
        assert_eq!(audit.cap, 5.0);
    }

    #[test]
    fn reject_candidates_do_not_change_support_tier_because_of_events() {
        let candidates = apply_event_score_adjustments(
            vec![decision_candidate(
                "600519.SH",
                "reject",
                82.0,
                0.0,
                vec![audit_entry("direct_entity", Some(1.0), Some(1.0))],
            )],
            &DecisionSupportConfig {
                event_score_enabled: true,
                event_score_limit: 5.0,
                ..DecisionSupportConfig::default()
            },
            true,
        );

        let candidate = &candidates[0];
        assert_eq!(candidate.support_tier, "reject");
        assert_eq!(candidate.event_adjustment, 5.0);
    }

    #[test]
    fn data_incomplete_candidates_cannot_receive_positive_event_adjustments() {
        let candidates = apply_event_score_adjustments(
            vec![decision_candidate(
                "600519.SH",
                "watch",
                82.0,
                -1.5,
                vec![audit_entry("direct_entity", Some(0.8), Some(1.0))],
            )],
            &DecisionSupportConfig {
                event_score_enabled: true,
                event_score_limit: 5.0,
                ..DecisionSupportConfig::default()
            },
            false,
        );

        let candidate = &candidates[0];
        let audit = &candidate.event_score_audit[0];
        assert_eq!(candidate.event_adjustment, 0.0);
        assert_eq!(candidate.final_score, 80.5);
        assert_eq!(audit.raw_adjustment, 8.0);
        assert_eq!(audit.applied_adjustment, 0.0);
        assert!(audit.reason.contains("data incomplete"));
    }

    #[test]
    fn only_direct_entity_or_reviewed_industry_relations_are_adjustment_eligible() {
        let candidates = apply_event_score_adjustments(
            vec![
                decision_candidate(
                    "600519.SH",
                    "watch",
                    82.0,
                    0.0,
                    vec![audit_entry("industry_match", Some(0.9), Some(0.7))],
                ),
                decision_candidate(
                    "000001.SZ",
                    "watch",
                    70.0,
                    0.0,
                    vec![audit_entry("reviewed_industry", Some(0.9), Some(0.7))],
                ),
            ],
            &DecisionSupportConfig {
                event_score_enabled: true,
                event_score_limit: 5.0,
                ..DecisionSupportConfig::default()
            },
            true,
        );

        let ineligible = &candidates
            .iter()
            .find(|candidate| candidate.code == "600519.SH")
            .unwrap();
        assert_eq!(ineligible.event_adjustment, 0.0);
        assert_eq!(ineligible.event_score_audit[0].applied_adjustment, 0.0);
        assert!(ineligible.event_score_audit[0]
            .reason
            .contains("not eligible"));

        let reviewed = &candidates
            .iter()
            .find(|candidate| candidate.code == "000001.SZ")
            .unwrap();
        assert_eq!(reviewed.event_adjustment, 5.0);
        assert_eq!(reviewed.event_score_audit[0].applied_adjustment, 5.0);
    }

    #[test]
    fn attempted_adjustments_always_retain_full_audit_payload() {
        let event_id = Uuid::new_v4();
        let candidates = apply_event_score_adjustments(
            vec![decision_candidate(
                "600519.SH",
                "watch",
                82.0,
                0.0,
                vec![EventScoreAdjustmentAudit {
                    event_id,
                    entity_relation: "industry_match".to_string(),
                    market_alignment: Some(-0.4),
                    causal_confidence: Some(0.5),
                    raw_adjustment: 0.0,
                    applied_adjustment: 0.0,
                    cap: 0.0,
                    reason: String::new(),
                }],
            )],
            &DecisionSupportConfig {
                event_score_enabled: true,
                event_score_limit: 5.0,
                ..DecisionSupportConfig::default()
            },
            true,
        );

        let audit = &candidates[0].event_score_audit[0];
        assert_eq!(audit.event_id, event_id);
        assert_eq!(audit.entity_relation, "industry_match");
        assert_eq!(audit.market_alignment, Some(-0.4));
        assert_eq!(audit.causal_confidence, Some(0.5));
        assert_eq!(audit.raw_adjustment, -2.0);
        assert_eq!(audit.applied_adjustment, 0.0);
        assert_eq!(audit.cap, 5.0);
        assert!(!audit.reason.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn build_daily_returns_read_only_daily_support_context(pool: PgPool) -> sqlx::Result<()> {
        let market_repo = MarketRepository::new(pool.clone());
        let event_repo = EventRepository::new(pool.clone());
        let decision_repo = DecisionSupportRepository::new(pool.clone());
        let trade_date = date(2026, 7, 11);

        market_repo
            .save_market_snapshot(&crate::analysis::market_snapshot::MarketSnapshot {
                trade_date,
                snapshot_version: crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION
                    .to_string(),
                available_at: dt(2026, 7, 11, 18),
                data_complete: false,
                metrics: json!({"breadth": {"up_count": 123}}),
                missing_inputs: vec!["security_status:600000:2026-07-11".to_string()],
                input_fingerprint: "market-fingerprint".to_string(),
            })
            .await
            .unwrap();

        let event_summary = DailyEventBrief {
            trade_date,
            new_facts: Vec::new(),
            revisions: Vec::new(),
            unconfirmed: Vec::new(),
            direct_entities: Vec::new(),
            sources: Vec::new(),
            input_fingerprint: "event-fingerprint".to_string(),
        };
        event_repo
            .save_daily_brief(&DailyEventBriefRow {
                trade_date,
                brief_version: "daily_event_brief_v1".to_string(),
                content: "brief".to_string(),
                structured_payload: serde_json::to_value(&event_summary).unwrap(),
                input_fingerprint: event_summary.input_fingerprint.clone(),
                generated_at: dt(2026, 7, 11, 19),
            })
            .await
            .unwrap();

        let support = DecisionSupport::new(pool.clone())
            .build_daily(trade_date, DecisionSupportConfig::default())
            .await
            .unwrap();

        assert_eq!(support.trade_date, trade_date);
        assert!(support.candidates.is_empty());
        assert_eq!(support.market_summary.trade_date, trade_date);
        assert_eq!(
            support.market_summary.snapshot_version,
            crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION
        );
        assert_eq!(support.event_summary, Some(event_summary));
        assert_eq!(support.data_status.requested_trade_date, trade_date);
        assert_eq!(support.data_status.latest_trade_date, Some(trade_date));
        assert!(!support.data_status.data_complete);
        assert_eq!(
            decision_repo.latest_run().await.unwrap(),
            None,
            "default build_daily should remain read-only until run persistence is specified"
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn build_daily_rolls_back_run_when_artifact_persistence_fails(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let trade_date = date(2026, 7, 11);
        seed_market_snapshot(&pool, trade_date).await;
        seed_ranked_pool_candidate(&pool, trade_date, "600000.SH", "Alpha Bank", 95.0, "short")
            .await;
        seed_ranked_pool_candidate(
            &pool,
            trade_date,
            "000001.SZ",
            "Broken Horizon",
            80.0,
            "short-horizon-overflow",
        )
        .await;

        let err = DecisionSupport::new(pool.clone())
            .build_daily(
                trade_date,
                DecisionSupportConfig {
                    persist_run: true,
                    ..DecisionSupportConfig::default()
                },
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("value too long")
                || err.to_string().contains("too long for type")
        );

        let repo = DecisionSupportRepository::new(pool);
        assert_eq!(repo.latest_run().await.unwrap(), None);

        Ok(())
    }
}
