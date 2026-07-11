use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::FrozenImpactHypothesis;
use crate::analysis::market_snapshot::PointInTimeContext;
use crate::error::{AppError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketObservationStatus {
    NotObserved,
    MarketAligned,
    MarketContradicted,
    Ambiguous,
    Confounded,
    Expired,
}

impl MarketObservationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotObserved => "not_observed",
            Self::MarketAligned => "market_aligned",
            Self::MarketContradicted => "market_contradicted",
            Self::Ambiguous => "ambiguous",
            Self::Confounded => "confounded",
            Self::Expired => "expired",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfounderKind {
    Earnings,
    SuspensionOrResumption,
    RegulatoryPenalty,
    MajorCorporateAction,
    HighImportanceEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationEntity {
    pub entity_type: String,
    pub entity_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservedReturn {
    pub value: f64,
    pub available_at: DateTime<Utc>,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationWindow {
    pub label: String,
    pub expires_on: NaiveDate,
}

#[derive(Debug, Clone)]
pub struct MarketSnapshotObservationInput {
    pub context: PointInTimeContext,
    pub hypothesis: FrozenImpactHypothesis,
    pub entity: ObservationEntity,
    pub window: ObservationWindow,
    pub stock_return: Option<ObservedReturn>,
    pub market_return: Option<ObservedReturn>,
    pub industry_return: Option<ObservedReturn>,
    pub snapshot_version: String,
    pub benchmark_id: String,
    pub industry_benchmark_id: String,
    pub causal_inputs: CausalConfidenceInputs,
    pub related_events: Vec<WindowEvent>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CausalConfidenceInputs {
    pub evidence_strength: f64,
    pub timing_quality: f64,
    pub identification_quality: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowEvent {
    pub entity_id: String,
    pub window_label: String,
    pub event_type: String,
    pub importance: EventImportance,
    pub available_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventImportance {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketObservation {
    pub hypothesis_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub trade_date: NaiveDate,
    pub observation_status: MarketObservationStatus,
    pub market_alignment_score: Option<f64>,
    pub causal_confidence: f64,
    pub abnormal_market_return: Option<f64>,
    pub abnormal_industry_return: Option<f64>,
    pub market_metrics: MarketObservationMetrics,
    pub confounding_events: Vec<ObservedConfounder>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketObservationMetrics {
    pub snapshot_version: String,
    pub window_label: String,
    pub benchmark_id: String,
    pub industry_benchmark_id: String,
    pub stock_return: Option<f64>,
    pub market_return: Option<f64>,
    pub industry_return: Option<f64>,
    pub expected_direction: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedConfounder {
    pub kind: ConfounderKind,
    pub event_type: String,
}

pub fn observe_market_alignment(
    input: &MarketSnapshotObservationInput,
) -> Result<MarketObservation> {
    validate_point_in_time_input(input)?;
    validate_causal_inputs(&input.causal_inputs)?;

    let abnormal_market_return = input
        .stock_return
        .as_ref()
        .zip(input.market_return.as_ref())
        .map(|(stock, market)| round_metric(stock.value - market.value));
    let abnormal_industry_return = input
        .stock_return
        .as_ref()
        .zip(input.industry_return.as_ref())
        .map(|(stock, industry)| round_metric(stock.value - industry.value));
    let confounding_events = detect_confounders(input);
    let expected_direction = infer_expected_direction(&input.hypothesis);
    let market_alignment_score = compute_market_alignment_score(
        expected_direction,
        abnormal_market_return,
        abnormal_industry_return,
    );
    let observation_status = classify_status(
        input,
        expected_direction,
        abnormal_market_return,
        abnormal_industry_return,
        &confounding_events,
    );

    Ok(MarketObservation {
        hypothesis_id: input.hypothesis.hypothesis_id(),
        entity_type: input.entity.entity_type.clone(),
        entity_id: input.entity.entity_id.clone(),
        trade_date: input.context.trade_date,
        observation_status,
        market_alignment_score,
        causal_confidence: derive_causal_confidence(&input.causal_inputs, &confounding_events),
        abnormal_market_return,
        abnormal_industry_return,
        market_metrics: MarketObservationMetrics {
            snapshot_version: input.snapshot_version.clone(),
            window_label: input.window.label.clone(),
            benchmark_id: input.benchmark_id.clone(),
            industry_benchmark_id: input.industry_benchmark_id.clone(),
            stock_return: input
                .stock_return
                .as_ref()
                .map(|value| round_metric(value.value)),
            market_return: input
                .market_return
                .as_ref()
                .map(|value| round_metric(value.value)),
            industry_return: input
                .industry_return
                .as_ref()
                .map(|value| round_metric(value.value)),
            expected_direction: expected_direction.as_str().to_string(),
        },
        confounding_events,
        created_at: input.observed_at,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedDirection {
    Positive,
    Negative,
    Ambiguous,
}

impl ExpectedDirection {
    fn sign(self) -> Option<f64> {
        match self {
            Self::Positive => Some(1.0),
            Self::Negative => Some(-1.0),
            Self::Ambiguous => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Positive => "positive",
            Self::Negative => "negative",
            Self::Ambiguous => "ambiguous",
        }
    }
}

fn validate_point_in_time_input(input: &MarketSnapshotObservationInput) -> Result<()> {
    validate_return_availability(&input.context, "stock_return", input.stock_return.as_ref())?;
    validate_return_availability(
        &input.context,
        "market_return",
        input.market_return.as_ref(),
    )?;
    validate_return_availability(
        &input.context,
        "industry_return",
        input.industry_return.as_ref(),
    )?;

    for related in &input.related_events {
        if !input.context.can_use(related.available_at) {
            return Err(AppError::BadRequest(format!(
                "point-in-time market observation cannot use related event `{}` available at {} after as_of {}",
                related.event_type, related.available_at, input.context.as_of
            )));
        }
    }

    Ok(())
}

fn validate_return_availability(
    context: &PointInTimeContext,
    field_name: &str,
    observed: Option<&ObservedReturn>,
) -> Result<()> {
    if let Some(observed) = observed {
        if !context.can_use(observed.available_at) {
            return Err(AppError::BadRequest(format!(
                "point-in-time market observation cannot use {field_name} data available at {} after as_of {}",
                observed.available_at, context.as_of
            )));
        }
    }

    Ok(())
}

fn validate_causal_inputs(inputs: &CausalConfidenceInputs) -> Result<()> {
    for (name, value) in [
        ("evidence_strength", inputs.evidence_strength),
        ("timing_quality", inputs.timing_quality),
        ("identification_quality", inputs.identification_quality),
    ] {
        if !(0.0..=1.0).contains(&value) {
            return Err(AppError::BadRequest(format!("{name} must be within [0,1]")));
        }
    }

    Ok(())
}

fn infer_expected_direction(hypothesis: &FrozenImpactHypothesis) -> ExpectedDirection {
    let mut saw_positive = false;
    let mut saw_negative = false;

    for edge in &hypothesis.graph().edges {
        match relation_direction(&edge.relation) {
            Some(ExpectedDirection::Positive) => saw_positive = true,
            Some(ExpectedDirection::Negative) => saw_negative = true,
            _ => {}
        }
    }

    match (saw_positive, saw_negative) {
        (true, false) => ExpectedDirection::Positive,
        (false, true) => ExpectedDirection::Negative,
        _ => ExpectedDirection::Ambiguous,
    }
}

fn relation_direction(relation: &str) -> Option<ExpectedDirection> {
    match relation {
        "increases" | "may_expand_demand" | "may_lower_risk_premium" => {
            Some(ExpectedDirection::Positive)
        }
        "may_reduce_supply" | "may_compress_margin" => Some(ExpectedDirection::Negative),
        _ => None,
    }
}

fn compute_market_alignment_score(
    expected_direction: ExpectedDirection,
    abnormal_market_return: Option<f64>,
    abnormal_industry_return: Option<f64>,
) -> Option<f64> {
    let sign = expected_direction.sign()?;
    let abnormal_market_return = abnormal_market_return?;
    let abnormal_industry_return = abnormal_industry_return?;

    Some(round_metric(
        ((abnormal_market_return * sign) + (abnormal_industry_return * sign)) / 2.0,
    ))
}

fn classify_status(
    input: &MarketSnapshotObservationInput,
    expected_direction: ExpectedDirection,
    abnormal_market_return: Option<f64>,
    abnormal_industry_return: Option<f64>,
    confounding_events: &[ObservedConfounder],
) -> MarketObservationStatus {
    if abnormal_market_return.is_none() || abnormal_industry_return.is_none() {
        return if input.context.trade_date > input.window.expires_on {
            MarketObservationStatus::Expired
        } else {
            MarketObservationStatus::NotObserved
        };
    }

    if !confounding_events.is_empty() {
        return MarketObservationStatus::Confounded;
    }

    let Some(sign) = expected_direction.sign() else {
        return MarketObservationStatus::Ambiguous;
    };
    let market_signal = abnormal_market_return.unwrap() * sign;
    let industry_signal = abnormal_industry_return.unwrap() * sign;

    if market_signal > 0.0 && industry_signal > 0.0 {
        MarketObservationStatus::MarketAligned
    } else if market_signal < 0.0 && industry_signal < 0.0 {
        MarketObservationStatus::MarketContradicted
    } else {
        MarketObservationStatus::Ambiguous
    }
}

fn detect_confounders(input: &MarketSnapshotObservationInput) -> Vec<ObservedConfounder> {
    input
        .related_events
        .iter()
        .filter(|event| {
            event.entity_id == input.entity.entity_id && event.window_label == input.window.label
        })
        .filter_map(|event| {
            classify_confounder(event).map(|kind| ObservedConfounder {
                kind,
                event_type: event.event_type.clone(),
            })
        })
        .collect()
}

fn classify_confounder(event: &WindowEvent) -> Option<ConfounderKind> {
    match event.event_type.as_str() {
        "earnings" => Some(ConfounderKind::Earnings),
        "trading_suspension" | "trading_resumption" | "suspension" | "resumption" => {
            Some(ConfounderKind::SuspensionOrResumption)
        }
        "regulatory_penalty" => Some(ConfounderKind::RegulatoryPenalty),
        "major_corporate_action" => Some(ConfounderKind::MajorCorporateAction),
        _ if event.importance == EventImportance::High => Some(ConfounderKind::HighImportanceEvent),
        _ => None,
    }
}

fn derive_causal_confidence(
    inputs: &CausalConfidenceInputs,
    confounding_events: &[ObservedConfounder],
) -> f64 {
    let base =
        (inputs.evidence_strength + inputs.timing_quality + inputs.identification_quality) / 3.0;
    let confounder_penalty = if confounding_events.is_empty() {
        1.0
    } else {
        0.5
    };

    round_metric(base * confounder_penalty)
}

fn round_metric(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::events::claims::{ClaimEdge, ClaimGraph, ClaimNode};
    use chrono::TimeZone;
    use uuid::Uuid;

    #[test]
    fn status_enum_uses_repository_supported_values() {
        let actual = [
            MarketObservationStatus::NotObserved.as_str(),
            MarketObservationStatus::MarketAligned.as_str(),
            MarketObservationStatus::MarketContradicted.as_str(),
            MarketObservationStatus::Ambiguous.as_str(),
            MarketObservationStatus::Confounded.as_str(),
            MarketObservationStatus::Expired.as_str(),
        ];

        assert_eq!(
            actual,
            [
                "not_observed",
                "market_aligned",
                "market_contradicted",
                "ambiguous",
                "confounded",
                "expired",
            ]
        );
    }

    #[test]
    fn aligned_returns_produce_required_abnormal_returns_without_changing_causal_confidence() {
        let hypothesis = positive_hypothesis();
        let input = observation_input(hypothesis.clone(), 0.05, 0.02, 0.03);

        let observation = observe_market_alignment(&input).unwrap();

        assert_eq!(observation.abnormal_market_return, Some(0.03));
        assert_eq!(observation.abnormal_industry_return, Some(0.02));
        assert_eq!(
            observation.observation_status,
            MarketObservationStatus::MarketAligned
        );

        let contradicted =
            observe_market_alignment(&observation_input(hypothesis, -0.05, 0.02, 0.03)).unwrap();
        assert_ne!(
            observation.market_alignment_score,
            contradicted.market_alignment_score
        );
        assert_eq!(
            observation.causal_confidence,
            contradicted.causal_confidence
        );
    }

    #[test]
    fn qualifying_same_entity_window_events_mark_observation_confounded() {
        let cases = [
            (
                "earnings",
                EventImportance::Medium,
                ConfounderKind::Earnings,
            ),
            (
                "trading_suspension",
                EventImportance::Medium,
                ConfounderKind::SuspensionOrResumption,
            ),
            (
                "trading_resumption",
                EventImportance::Medium,
                ConfounderKind::SuspensionOrResumption,
            ),
            (
                "regulatory_penalty",
                EventImportance::Medium,
                ConfounderKind::RegulatoryPenalty,
            ),
            (
                "major_corporate_action",
                EventImportance::Medium,
                ConfounderKind::MajorCorporateAction,
            ),
            (
                "issuer_disclosure",
                EventImportance::High,
                ConfounderKind::HighImportanceEvent,
            ),
        ];

        for (event_type, importance, expected_kind) in cases {
            let mut input = observation_input(positive_hypothesis(), 0.05, 0.02, 0.03);
            input.related_events.push(WindowEvent {
                entity_id: input.entity.entity_id.clone(),
                window_label: input.window.label.clone(),
                event_type: event_type.to_string(),
                importance,
                available_at: input.context.as_of,
            });

            let observation = observe_market_alignment(&input).unwrap();

            assert_eq!(
                observation.observation_status,
                MarketObservationStatus::Confounded
            );
            assert_eq!(observation.confounding_events.len(), 1);
            assert_eq!(observation.confounding_events[0].kind, expected_kind);
        }
    }

    #[test]
    fn future_snapshot_inputs_are_rejected_for_point_in_time_observation() {
        let mut input = observation_input(positive_hypothesis(), 0.05, 0.02, 0.03);
        input.stock_return.as_mut().unwrap().available_at =
            Utc.with_ymd_and_hms(2026, 7, 14, 9, 31, 0).unwrap();

        let error = observe_market_alignment(&input).unwrap_err();

        assert!(error
            .to_string()
            .contains("point-in-time market observation cannot use stock_return data"));
    }

    #[test]
    fn expired_window_without_observed_returns_is_marked_expired() {
        let mut input = observation_input(positive_hypothesis(), 0.05, 0.02, 0.03);
        input.window.expires_on = date(2026, 7, 13);
        input.context.trade_date = date(2026, 7, 14);
        input.stock_return = None;
        input.market_return = None;
        input.industry_return = None;

        let observation = observe_market_alignment(&input).unwrap();

        assert_eq!(
            observation.observation_status,
            MarketObservationStatus::Expired
        );
        assert_eq!(observation.market_alignment_score, None);
        assert_eq!(observation.abnormal_market_return, None);
        assert_eq!(observation.abnormal_industry_return, None);
    }

    #[test]
    fn observing_market_alignment_does_not_mutate_frozen_hypothesis() {
        let hypothesis = positive_hypothesis();
        let before = hypothesis.clone();

        let observation =
            observe_market_alignment(&observation_input(hypothesis.clone(), 0.05, 0.02, 0.03))
                .unwrap();

        assert_eq!(hypothesis, before);
        assert_eq!(observation.hypothesis_id, hypothesis.hypothesis_id());
    }

    fn observation_input(
        hypothesis: FrozenImpactHypothesis,
        stock_return: f64,
        market_return: f64,
        industry_return: f64,
    ) -> MarketSnapshotObservationInput {
        let context = PointInTimeContext {
            trade_date: date(2026, 7, 14),
            as_of: Utc.with_ymd_and_hms(2026, 7, 14, 9, 30, 0).unwrap(),
        };

        MarketSnapshotObservationInput {
            context,
            hypothesis,
            entity: ObservationEntity {
                entity_type: "company".to_string(),
                entity_id: "600519.SH".to_string(),
            },
            window: ObservationWindow {
                label: "t+1".to_string(),
                expires_on: date(2026, 7, 15),
            },
            stock_return: Some(observed_return(stock_return)),
            market_return: Some(observed_return(market_return)),
            industry_return: Some(observed_return(industry_return)),
            snapshot_version: "pit-market-snapshot-v1".to_string(),
            benchmark_id: "CSI300".to_string(),
            industry_benchmark_id: "SW-FoodBeverage".to_string(),
            causal_inputs: CausalConfidenceInputs {
                evidence_strength: 0.81,
                timing_quality: 0.72,
                identification_quality: 0.64,
            },
            related_events: Vec::new(),
            observed_at: Utc.with_ymd_and_hms(2026, 7, 14, 9, 30, 0).unwrap(),
        }
    }

    fn observed_return(value: f64) -> ObservedReturn {
        ObservedReturn {
            value,
            available_at: Utc.with_ymd_and_hms(2026, 7, 14, 9, 30, 0).unwrap(),
            source: "unit-test".to_string(),
        }
    }

    fn positive_hypothesis() -> FrozenImpactHypothesis {
        FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "order-1",
                    "CompanyFact",
                    "600519.SH wins major automation order",
                    vec![Uuid::from_u128(101)],
                    0.91,
                )],
                vec![claim_edge(
                    "order-1",
                    "industry-1",
                    "mentions",
                    vec![Uuid::from_u128(102)],
                    0.87,
                )],
            )
            .unwrap(),
            vec![Uuid::from_u128(103)],
            Utc.with_ymd_and_hms(2026, 7, 13, 16, 0, 0).unwrap(),
        )
        .unwrap()
    }

    fn claim_node(
        node_id: &str,
        node_type: &str,
        label: &str,
        evidence_ids: Vec<Uuid>,
        confidence: f64,
    ) -> ClaimNode {
        ClaimNode {
            node_id: node_id.to_string(),
            node_type: node_type.to_string(),
            label: label.to_string(),
            evidence_ids,
            confidence,
        }
    }

    fn claim_edge(
        from: &str,
        to: &str,
        relation: &str,
        evidence_ids: Vec<Uuid>,
        confidence: f64,
    ) -> ClaimEdge {
        ClaimEdge {
            from: from.to_string(),
            to: to.to_string(),
            relation: relation.to_string(),
            evidence_ids,
            confidence,
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }
}
