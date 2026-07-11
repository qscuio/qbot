use chrono::NaiveDate;
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::analysis::decision_support::contracts::{
    DailyDecisionSupport, DataStatus, DecisionSupportConfig, MarketSummary, StatementBucket,
    SupportStatement,
};
use crate::analysis::decision_support::event_adapter::apply_event_context;
use crate::analysis::decision_support::pattern_adapter::build_decision_candidates;
use crate::analysis::decision_support::scan_ranker_adapter::load_scan_ranker_baseline;
use crate::analysis::events::DailyEventBrief;
use crate::error::AppError;
use crate::error::Result;
use crate::storage::decision_support_repository::DecisionSupportRepository;
use crate::storage::decision_support_repository::DecisionSupportRunRow;
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
        let run_id = if config.persist_run {
            self.persist_run(
                trade_date,
                &config,
                &snapshot.input_fingerprint,
                pattern_set.as_ref(),
                event_brief_row.as_ref(),
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

    async fn persist_run(
        &self,
        trade_date: NaiveDate,
        config: &DecisionSupportConfig,
        market_input_fingerprint: &str,
        pattern_set: Option<&PatternSetRow>,
        event_brief_row: Option<&DailyEventBriefRow>,
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

        self.decision_repo
            .create_run(&DecisionSupportRunRow {
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
            })
            .await
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

#[cfg(test)]
mod tests {
    use super::classify_statements;
    use crate::analysis::decision_support::scan_ranker_adapter::BaselineCandidate;
    use crate::analysis::decision_support::{
        pattern_adapter::build_decision_candidates, DecisionSupport, DecisionSupportConfig,
        SupportStatement,
    };
    use crate::analysis::events::DailyEventBrief;
    use crate::storage::decision_support_repository::DecisionSupportRepository;
    use crate::storage::event_repository::{DailyEventBriefRow, EventRepository};
    use crate::storage::market_repository::MarketRepository;
    use crate::storage::pattern_repository::ShadowCandidateRow;
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
}
