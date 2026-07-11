use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::claims::{ClaimEdge, ClaimGraph, ClaimNode};
use super::deltas::{EventDelta, ExpectationGap, NormalizedValue, RevisedValue, StatusChange};
use crate::error::{AppError, Result};

pub const IMPACT_HYPOTHESIS_SCHEMA_VERSION: &str = "impact_hypothesis_graph_v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImpactHypothesisGraph {
    pub schema_version: String,
    pub nodes: Vec<HypothesisNode>,
    pub edges: Vec<HypothesisEdge>,
    pub based_on_claim_ids: Vec<Uuid>,
    pub frozen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HypothesisNode {
    pub node_id: String,
    pub node_type: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HypothesisEdge {
    pub from: String,
    pub to: String,
    pub relation: String,
    pub generation_method: String,
    pub logic_rule_id: Option<String>,
    pub confidence: f64,
    pub assumptions: Vec<String>,
    pub expected_horizon: String,
    pub observable_indicators: Vec<String>,
    pub counter_scenario: Vec<String>,
    pub invalidation_conditions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrozenImpactHypothesis {
    hypothesis_id: Uuid,
    hypothesis_version: i32,
    supersedes_hypothesis_id: Option<Uuid>,
    graph: ImpactHypothesisGraph,
}

pub fn build_impact_hypothesis_graph(
    claim_graph: &ClaimGraph,
    based_on_claim_ids: Vec<Uuid>,
    frozen_at: DateTime<Utc>,
) -> Result<ImpactHypothesisGraph> {
    let based_on_claim_ids = canonical_claim_ids(based_on_claim_ids)?;
    let node_index = claim_graph
        .nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node))
        .collect::<BTreeMap<_, _>>();

    let mut nodes_by_id = BTreeMap::<String, HypothesisNode>::new();
    let mut edges = Vec::new();

    for claim_node in &claim_graph.nodes {
        let Some(template) = template_for_claim(claim_node) else {
            continue;
        };

        let source_node = HypothesisNode {
            node_id: format!("{}:source:{}", template.logic_rule_id, claim_node.node_id),
            node_type: template.source_node_type.to_string(),
            label: sanitize_hypothesis_label(&claim_node.label),
        };
        nodes_by_id
            .entry(source_node.node_id.clone())
            .or_insert_with(|| source_node.clone());

        let targets = infer_targets(claim_node, claim_graph, &node_index);
        for (target_index, target) in targets.iter().enumerate() {
            let impact_node = HypothesisNode {
                node_id: format!(
                    "{}:impact:{}:{}",
                    template.logic_rule_id,
                    slugify(&target.label),
                    target_index
                ),
                node_type: target.node_type.clone(),
                label: target.label.clone(),
            };
            nodes_by_id
                .entry(impact_node.node_id.clone())
                .or_insert_with(|| impact_node.clone());

            let indicator_node = HypothesisNode {
                node_id: format!(
                    "{}:indicator:{}:{}",
                    template.logic_rule_id,
                    slugify(&template.observable_indicator),
                    target_index
                ),
                node_type: "ObservableIndicator".to_string(),
                label: template.observable_indicator.to_string(),
            };
            nodes_by_id
                .entry(indicator_node.node_id.clone())
                .or_insert_with(|| indicator_node.clone());

            let invalidation_node = HypothesisNode {
                node_id: format!(
                    "{}:invalidation:{}:{}",
                    template.logic_rule_id,
                    slugify(&template.invalidation_condition),
                    target_index
                ),
                node_type: "InvalidationCondition".to_string(),
                label: template.invalidation_condition.to_string(),
            };
            nodes_by_id
                .entry(invalidation_node.node_id.clone())
                .or_insert_with(|| invalidation_node.clone());

            edges.push(HypothesisEdge {
                from: source_node.node_id.clone(),
                to: impact_node.node_id.clone(),
                relation: template.primary_relation.to_string(),
                generation_method: "domain_rule".to_string(),
                logic_rule_id: Some(template.logic_rule_id.to_string()),
                confidence: target.confidence.min(claim_node.confidence),
                assumptions: vec![template.assumption.to_string()],
                expected_horizon: template.expected_horizon.to_string(),
                observable_indicators: vec![template.observable_indicator.to_string()],
                counter_scenario: vec![template.counter_scenario.to_string()],
                invalidation_conditions: vec![template.invalidation_condition.to_string()],
            });
            edges.push(HypothesisEdge {
                from: impact_node.node_id.clone(),
                to: indicator_node.node_id.clone(),
                relation: "observed_by".to_string(),
                generation_method: "domain_rule".to_string(),
                logic_rule_id: Some(template.logic_rule_id.to_string()),
                confidence: target.confidence.min(claim_node.confidence),
                assumptions: vec![template.assumption.to_string()],
                expected_horizon: template.expected_horizon.to_string(),
                observable_indicators: vec![template.observable_indicator.to_string()],
                counter_scenario: vec![template.counter_scenario.to_string()],
                invalidation_conditions: vec![template.invalidation_condition.to_string()],
            });
            edges.push(HypothesisEdge {
                from: impact_node.node_id.clone(),
                to: invalidation_node.node_id.clone(),
                relation: "invalidated_by".to_string(),
                generation_method: "domain_rule".to_string(),
                logic_rule_id: Some(template.logic_rule_id.to_string()),
                confidence: target.confidence.min(claim_node.confidence),
                assumptions: vec![template.assumption.to_string()],
                expected_horizon: template.expected_horizon.to_string(),
                observable_indicators: vec![template.observable_indicator.to_string()],
                counter_scenario: vec![template.counter_scenario.to_string()],
                invalidation_conditions: vec![template.invalidation_condition.to_string()],
            });
        }
    }

    if nodes_by_id.is_empty() || edges.is_empty() {
        return Err(AppError::BadRequest(
            "claim graph does not match any supported hypothesis template".to_string(),
        ));
    }

    Ok(ImpactHypothesisGraph {
        schema_version: IMPACT_HYPOTHESIS_SCHEMA_VERSION.to_string(),
        nodes: nodes_by_id.into_values().collect(),
        edges,
        based_on_claim_ids,
        frozen_at,
    })
}

impl FrozenImpactHypothesis {
    pub fn hypothesis_id(&self) -> Uuid {
        self.hypothesis_id
    }

    pub fn hypothesis_version(&self) -> i32 {
        self.hypothesis_version
    }

    pub fn supersedes_hypothesis_id(&self) -> Option<Uuid> {
        self.supersedes_hypothesis_id
    }

    pub fn graph(&self) -> &ImpactHypothesisGraph {
        &self.graph
    }

    pub fn initial(
        claim_graph: &ClaimGraph,
        based_on_claim_ids: Vec<Uuid>,
        frozen_at: DateTime<Utc>,
    ) -> Result<Self> {
        Ok(Self {
            hypothesis_id: Uuid::new_v4(),
            hypothesis_version: 1,
            supersedes_hypothesis_id: None,
            graph: build_impact_hypothesis_graph(claim_graph, based_on_claim_ids, frozen_at)?,
        })
    }

    pub fn evolve(
        &self,
        claim_graph: &ClaimGraph,
        event_delta: &EventDelta,
        frozen_at: DateTime<Utc>,
    ) -> Result<Self> {
        let canonical_ids = evolved_claim_ids(&self.graph.based_on_claim_ids, event_delta)?;

        let graph = build_impact_hypothesis_graph(claim_graph, canonical_ids, frozen_at)?;
        if !strictly_preserves_prior_graph_payload(&self.graph, &graph) {
            return Err(AppError::BadRequest(
                "frozen hypotheses must preserve prior graph payload when evolving".to_string(),
            ));
        }

        Ok(Self {
            hypothesis_id: Uuid::new_v4(),
            hypothesis_version: self.hypothesis_version + 1,
            supersedes_hypothesis_id: Some(self.hypothesis_id),
            graph,
        })
    }
}

#[derive(Debug, Clone)]
struct TemplateSpec {
    logic_rule_id: &'static str,
    source_node_type: &'static str,
    fallback_target_type: &'static str,
    fallback_target_label: &'static str,
    primary_relation: &'static str,
    assumption: &'static str,
    expected_horizon: &'static str,
    observable_indicator: &'static str,
    counter_scenario: &'static str,
    invalidation_condition: &'static str,
}

#[derive(Debug, Clone)]
struct InferredTarget {
    node_type: String,
    label: String,
    confidence: f64,
}

const POLICY_SUBSIDY_TEMPLATE: TemplateSpec = TemplateSpec {
    logic_rule_id: "policy_subsidy_v1",
    source_node_type: "PolicyVariable",
    fallback_target_type: "IndustryImpact",
    fallback_target_label: "subsidy-linked industry demand",
    primary_relation: "increases",
    assumption: "subsidy reaches directly mentioned beneficiaries without offsetting restrictions",
    expected_horizon: "medium_term",
    observable_indicator: "industry order intake and revenue guidance improve",
    counter_scenario: "subsidy is delayed or too small to change realized demand",
    invalidation_condition: "implementation guidance excludes the directly mentioned beneficiaries",
};

const SUPPLY_RESTRICTION_TEMPLATE: TemplateSpec = TemplateSpec {
    logic_rule_id: "supply_restriction_v1",
    source_node_type: "SupplyVariable",
    fallback_target_type: "PriceImpact",
    fallback_target_label: "tight-supply spot pricing",
    primary_relation: "may_reduce_supply",
    assumption: "the restriction is material enough to tighten immediately available supply",
    expected_horizon: "short_term",
    observable_indicator: "spot prices and inventory tightness rise",
    counter_scenario: "alternative capacity fills the gap before inventories tighten",
    invalidation_condition: "supply resumes quickly or downstream inventories keep expanding",
};

const DEMAND_SHOCK_TEMPLATE: TemplateSpec = TemplateSpec {
    logic_rule_id: "demand_shock_v1",
    source_node_type: "DemandVariable",
    fallback_target_type: "IndustryImpact",
    fallback_target_label: "demand-sensitive industry exposure",
    primary_relation: "may_expand_demand",
    assumption: "the shock changes realized consumption rather than only expectations",
    expected_horizon: "medium_term",
    observable_indicator: "volume growth and channel sell-through improve",
    counter_scenario: "demand is pulled forward instead of sustainably expanding",
    invalidation_condition: "end-demand data weakens despite the reported shock",
};

const LIQUIDITY_RATE_TEMPLATE: TemplateSpec = TemplateSpec {
    logic_rule_id: "liquidity_rate_v1",
    source_node_type: "LiquidityVariable",
    fallback_target_type: "ValuationImpact",
    fallback_target_label: "rate-sensitive valuation multiples",
    primary_relation: "may_lower_risk_premium",
    assumption: "funding costs and discount rates transmit into equity valuation",
    expected_horizon: "medium_term",
    observable_indicator: "bond yields and growth-style relative strength fall in tandem",
    counter_scenario: "macro stress offsets easier liquidity conditions",
    invalidation_condition: "market rates rise or credit conditions tighten after the policy move",
};

const COMPANY_ORDER_TEMPLATE: TemplateSpec = TemplateSpec {
    logic_rule_id: "company_order_v1",
    source_node_type: "DemandVariable",
    fallback_target_type: "RevenueImpact",
    fallback_target_label: "direct order beneficiary",
    primary_relation: "increases",
    assumption: "the order converts into recognized revenue without major execution slippage",
    expected_horizon: "medium_term",
    observable_indicator: "backlog, revenue guidance, or utilization improves",
    counter_scenario: "the order is canceled, repriced, or already priced in",
    invalidation_condition:
        "management discloses that the order will not translate into incremental revenue",
};

const COMPANY_ACCIDENT_TEMPLATE: TemplateSpec = TemplateSpec {
    logic_rule_id: "company_accident_v1",
    source_node_type: "SupplyVariable",
    fallback_target_type: "MarginImpact",
    fallback_target_label: "operationally exposed company margin",
    primary_relation: "may_compress_margin",
    assumption: "the accident interrupts operations or raises remediation costs",
    expected_horizon: "short_term",
    observable_indicator:
        "production guidance, utilization, or remediation disclosures deteriorate",
    counter_scenario: "operations resume quickly with limited financial damage",
    invalidation_condition: "the site resumes normally and management reports immaterial impact",
};

fn template_for_claim(node: &ClaimNode) -> Option<&'static TemplateSpec> {
    let label = node.label.to_ascii_lowercase();

    match node.node_type.as_str() {
        "PolicyFact" if contains_any(&label, &["subsidy", "rebate", "grant", "support"]) => {
            Some(&POLICY_SUBSIDY_TEMPLATE)
        }
        "SupplyFact"
            if contains_any(
                &label,
                &[
                    "restrict",
                    "restriction",
                    "outage",
                    "shutdown",
                    "halt",
                    "ban",
                    "quota",
                    "cut",
                ],
            ) =>
        {
            Some(&SUPPLY_RESTRICTION_TEMPLATE)
        }
        "DemandFact"
            if contains_any(
                &label,
                &[
                    "demand", "orders", "order", "surge", "boom", "slump", "travel",
                ],
            ) =>
        {
            Some(&DEMAND_SHOCK_TEMPLATE)
        }
        "MacroDataFact"
            if contains_any(
                &label,
                &[
                    "rate",
                    "liquidity",
                    "rrr",
                    "reserve",
                    "yield",
                    "easing",
                    "tightening",
                ],
            ) =>
        {
            Some(&LIQUIDITY_RATE_TEMPLATE)
        }
        "CompanyFact" if contains_any(&label, &["order", "contract", "booking", "bid"]) => {
            Some(&COMPANY_ORDER_TEMPLATE)
        }
        "OperationalFact"
            if contains_any(
                &label,
                &[
                    "accident",
                    "explosion",
                    "fire",
                    "leak",
                    "halt",
                    "shutdown",
                    "incident",
                ],
            ) =>
        {
            Some(&COMPANY_ACCIDENT_TEMPLATE)
        }
        _ => None,
    }
}

fn infer_targets<'a>(
    source_node: &'a ClaimNode,
    claim_graph: &'a ClaimGraph,
    node_index: &BTreeMap<&'a str, &'a ClaimNode>,
) -> Vec<InferredTarget> {
    let mut targets = Vec::new();
    let mut seen = BTreeSet::<(String, String)>::new();

    for edge in claim_graph
        .edges
        .iter()
        .filter(|edge| edge.from == source_node.node_id || edge.to == source_node.node_id)
    {
        let linked_id = if edge.from == source_node.node_id {
            edge.to.as_str()
        } else {
            edge.from.as_str()
        };
        let Some(linked) = node_index.get(linked_id) else {
            continue;
        };
        let Some(target) = target_from_claim_node(linked, edge) else {
            continue;
        };
        if seen.insert((target.node_type.clone(), target.label.clone())) {
            targets.push(target);
        }
    }

    if targets.is_empty() {
        let Some(template) = template_for_claim(source_node) else {
            return targets;
        };
        let (label, node_type) = fallback_target(source_node, template);
        targets.push(InferredTarget {
            node_type,
            label,
            confidence: source_node.confidence,
        });
    }

    targets
}

fn target_from_claim_node(node: &ClaimNode, edge: &ClaimEdge) -> Option<InferredTarget> {
    if rejects_non_direct_target_label(&node.label) {
        return None;
    }

    let sanitized_label = sanitize_hypothesis_label(&node.label);
    if rejects_non_direct_target_label(&sanitized_label) {
        return None;
    }

    let (node_type, label) = match node.node_type.as_str() {
        _ if mentions_archetype(&sanitized_label) => {
            ("StockArchetypeImpact".to_string(), sanitized_label.clone())
        }
        "CompanyFact" if is_direct_company_target(&sanitized_label) => (
            "RevenueImpact".to_string(),
            extract_company_subject(&sanitized_label),
        ),
        "CompanyFact" => return None,
        "SupplyFact" | "DemandFact" | "PriceFact" => {
            ("IndustryImpact".to_string(), sanitized_label.clone())
        }
        "RegulatoryFact" | "PolicyFact" => ("IndustryImpact".to_string(), sanitized_label.clone()),
        _ => ("IndustryImpact".to_string(), sanitized_label.clone()),
    };

    Some(InferredTarget {
        node_type,
        label,
        confidence: node.confidence.min(edge.confidence),
    })
}

fn fallback_target(source_node: &ClaimNode, template: &TemplateSpec) -> (String, String) {
    match template.logic_rule_id {
        "company_order_v1" | "company_accident_v1" => {
            let raw_subject = extract_company_subject(&source_node.label);
            let sanitized_subject = sanitize_hypothesis_label(&raw_subject);
            let label = if rejects_non_direct_target_label(&raw_subject)
                || rejects_non_direct_target_label(&sanitized_subject)
            {
                template.fallback_target_label.to_string()
            } else {
                sanitized_subject
            };

            (label, template.fallback_target_type.to_string())
        }
        _ => (
            template.fallback_target_label.to_string(),
            template.fallback_target_type.to_string(),
        ),
    }
}

fn canonical_claim_ids(mut claim_ids: Vec<Uuid>) -> Result<Vec<Uuid>> {
    if claim_ids.is_empty() {
        return Err(AppError::BadRequest(
            "hypothesis graphs must reference at least one claim id".to_string(),
        ));
    }

    claim_ids.sort_unstable();
    claim_ids.dedup();
    Ok(claim_ids)
}

fn evolved_claim_ids(prior_claim_ids: &[Uuid], event_delta: &EventDelta) -> Result<Vec<Uuid>> {
    if !event_delta.removed_claim_ids.is_empty()
        || !event_delta.revised_values.is_empty()
        || !event_delta.status_changes.is_empty()
        || event_delta.expectation_gap.is_some()
    {
        return Err(AppError::BadRequest(
            "frozen hypotheses only support additive EventDelta claim changes".to_string(),
        ));
    }

    if event_delta.new_claim_ids.is_empty() {
        return Err(AppError::BadRequest(
            "frozen hypotheses require EventDelta new facts to create a new version".to_string(),
        ));
    }

    let mut canonical_ids = prior_claim_ids.to_vec();
    canonical_ids.extend(event_delta.new_claim_ids.iter().copied());
    let canonical_ids = canonical_claim_ids(canonical_ids)?;

    if canonical_ids.len() <= prior_claim_ids.len() {
        return Err(AppError::BadRequest(
            "frozen hypotheses require EventDelta new facts to create a new version".to_string(),
        ));
    }

    Ok(canonical_ids)
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn strictly_preserves_prior_graph_payload(
    prior: &ImpactHypothesisGraph,
    candidate: &ImpactHypothesisGraph,
) -> bool {
    let prior_nodes = prior
        .nodes
        .iter()
        .map(|node| (node.node_id.clone(), serde_json::to_string(node).unwrap()))
        .collect::<BTreeMap<_, _>>();
    let candidate_nodes = candidate
        .nodes
        .iter()
        .map(|node| (node.node_id.clone(), serde_json::to_string(node).unwrap()))
        .collect::<BTreeMap<_, _>>();

    let nodes_preserved = prior_nodes
        .iter()
        .all(|(node_id, payload)| candidate_nodes.get(node_id) == Some(payload));

    let prior_edges = payload_multiset(&prior.edges);
    let candidate_edges = payload_multiset(&candidate.edges);
    let edges_preserved = prior_edges.iter().all(|(payload, count)| {
        candidate_edges
            .get(payload)
            .is_some_and(|candidate_count| candidate_count >= count)
    });

    let graph_grew = candidate_nodes.len() > prior_nodes.len()
        || candidate_edges.len() > prior_edges.len()
        || candidate_edges
            .iter()
            .any(|(payload, count)| candidate_count_exceeds(payload, *count, &prior_edges));

    nodes_preserved && edges_preserved && graph_grew
}

fn payload_multiset<T: Serialize>(values: &[T]) -> BTreeMap<String, usize> {
    let mut payloads = BTreeMap::new();
    for value in values {
        *payloads
            .entry(serde_json::to_string(value).unwrap())
            .or_insert(0) += 1;
    }
    payloads
}

fn candidate_count_exceeds(
    payload: &str,
    candidate_count: usize,
    prior_counts: &BTreeMap<String, usize>,
) -> bool {
    candidate_count > prior_counts.get(payload).copied().unwrap_or_default()
}

fn sanitize_hypothesis_label(value: &str) -> String {
    let filtered = value
        .split_whitespace()
        .filter(|token| !looks_like_stock_code(token))
        .collect::<Vec<_>>()
        .join(" ");

    let trimmed = filtered.trim();
    if trimmed.is_empty() {
        "directly mentioned entity".to_string()
    } else {
        trimmed.to_string()
    }
}

fn extract_company_subject(value: &str) -> String {
    let keywords = [
        "wins",
        "win",
        "secures",
        "secured",
        "receives",
        "received",
        "lands",
        "order",
        "orders",
        "accident",
        "explosion",
        "fire",
        "halt",
        "halts",
        "shutdown",
    ];
    let tokens = value.split_whitespace().collect::<Vec<_>>();
    let split_at = tokens
        .iter()
        .position(|token| keywords.contains(&token.to_ascii_lowercase().as_str()))
        .unwrap_or(tokens.len());
    let subject = tokens[..split_at].join(" ");
    if subject.trim().is_empty() {
        value.to_string()
    } else {
        subject
    }
}

fn mentions_archetype(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    contains_any(
        &lower,
        &[
            "upstream",
            "downstream",
            "supplier",
            "distributor",
            "contractor",
        ],
    )
}

fn is_direct_company_target(value: &str) -> bool {
    !rejects_non_direct_target_label(value)
}

fn is_non_direct_beneficiary_list_label(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    contains_any(
        &lower,
        &["peer", "beneficiar", "indirect", "basket", "list"],
    )
}

fn rejects_non_direct_target_label(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("directly mentioned entity")
        || is_non_direct_beneficiary_list_label(trimmed)
        || is_stock_code_only_list_label(trimmed)
}

fn is_stock_code_only_list_label(value: &str) -> bool {
    let tokens = value
        .split(|ch: char| ch.is_whitespace() || matches!(ch, ',' | ';' | '/'))
        .map(|token| token.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '.'))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();

    !tokens.is_empty() && tokens.iter().all(|token| looks_like_stock_code(token))
}

fn looks_like_stock_code(token: &str) -> bool {
    let mut parts = token
        .trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '.')
        .split('.');
    let Some(left) = parts.next() else {
        return false;
    };
    let Some(right) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }

    (left.len() == 6 && left.chars().all(|ch| ch.is_ascii_digit()))
        && matches!(right, "SH" | "SZ" | "BJ")
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut last_was_sep = false;
    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            Some(ch.to_ascii_lowercase())
        } else {
            None
        };

        match mapped {
            Some(ch) => {
                slug.push(ch);
                last_was_sep = false;
            }
            None if !last_was_sep => {
                slug.push('-');
                last_was_sep = true;
            }
            None => {}
        }
    }

    slug.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;

    #[test]
    fn builds_policy_subsidy_graph_with_company_scope_and_template_metadata() {
        let frozen_at = dt(2026, 7, 11, 9);
        let claim_ids = vec![Uuid::from_u128(101), Uuid::from_u128(102)];
        let graph = build_impact_hypothesis_graph(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![
                    claim_node(
                        "policy-1",
                        "PolicyFact",
                        "New energy vehicle subsidy expanded",
                        vec![evidence_id(1)],
                        0.92,
                    ),
                    claim_node(
                        "company-1",
                        "CompanyFact",
                        "Acme Batteries",
                        vec![evidence_id(2)],
                        0.88,
                    ),
                ],
                vec![claim_edge(
                    "policy-1",
                    "company-1",
                    "applies_to",
                    vec![evidence_id(3)],
                    0.84,
                )],
            )
            .unwrap(),
            claim_ids.clone(),
            frozen_at,
        )
        .unwrap();

        assert_eq!(graph.schema_version, IMPACT_HYPOTHESIS_SCHEMA_VERSION);
        assert_eq!(graph.based_on_claim_ids, claim_ids);
        assert_eq!(graph.frozen_at, frozen_at);
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.node_type == "PolicyVariable"
                && node.label == "New energy vehicle subsidy expanded"));
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.node_type == "RevenueImpact" && node.label == "Acme Batteries"));
        assert!(graph.edges.iter().any(|edge| {
            edge.logic_rule_id.as_deref() == Some("policy_subsidy_v1")
                && edge.generation_method == "domain_rule"
                && edge.relation == "increases"
                && edge.expected_horizon == "medium_term"
        }));
    }

    #[test]
    fn supports_all_required_deterministic_templates() {
        let cases = [
            ("policy_subsidy_v1", "PolicyFact", "Battery subsidy widened"),
            (
                "supply_restriction_v1",
                "SupplyFact",
                "Smelter outage restricts refined copper supply",
            ),
            ("demand_shock_v1", "DemandFact", "Air travel demand surges"),
            (
                "liquidity_rate_v1",
                "MacroDataFact",
                "Central bank cuts policy rate and adds liquidity",
            ),
            (
                "company_order_v1",
                "CompanyFact",
                "Acme Motors wins a multi-year bus order",
            ),
            (
                "company_accident_v1",
                "OperationalFact",
                "Acme Chemicals plant explosion halts production",
            ),
        ];

        for (template_id, node_type, label) in cases {
            let graph = build_impact_hypothesis_graph(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![
                        claim_node("fact-1", node_type, label, vec![evidence_id(11)], 0.9),
                        claim_node(
                            "company-1",
                            "CompanyFact",
                            "Directly Mentioned Company",
                            vec![evidence_id(12)],
                            0.83,
                        ),
                    ],
                    vec![claim_edge(
                        "fact-1",
                        "company-1",
                        "affects",
                        vec![evidence_id(13)],
                        0.82,
                    )],
                )
                .unwrap(),
                vec![Uuid::from_u128(1000)],
                dt(2026, 7, 11, 10),
            )
            .unwrap();

            assert!(
                graph
                    .edges
                    .iter()
                    .any(|edge| edge.logic_rule_id.as_deref() == Some(template_id)),
                "missing template {template_id}"
            );
        }
    }

    #[test]
    fn hypothesis_scope_does_not_generate_indirect_stock_codes() {
        let graph = build_impact_hypothesis_graph(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![
                    claim_node(
                        "order-1",
                        "CompanyFact",
                        "Acme Robotics wins major automation order",
                        vec![evidence_id(21)],
                        0.91,
                    ),
                    claim_node(
                        "peer-1",
                        "CompanyFact",
                        "Peer basket 600519.SH 000001.SZ",
                        vec![evidence_id(22)],
                        0.75,
                    ),
                ],
                vec![claim_edge(
                    "order-1",
                    "peer-1",
                    "impacts",
                    vec![evidence_id(23)],
                    0.7,
                )],
            )
            .unwrap(),
            vec![Uuid::from_u128(2001)],
            dt(2026, 7, 11, 11),
        )
        .unwrap();

        assert!(graph
            .nodes
            .iter()
            .all(|node| !contains_stock_code(&node.label)));
        assert!(graph.edges.iter().all(|edge| {
            edge.observable_indicators
                .iter()
                .all(|indicator| !contains_stock_code(indicator))
                && edge
                    .counter_scenario
                    .iter()
                    .all(|scenario| !contains_stock_code(scenario))
                && edge
                    .invalidation_conditions
                    .iter()
                    .all(|condition| !contains_stock_code(condition))
        }));
    }

    #[test]
    fn hypothesis_scope_omits_indirect_company_targets() {
        let graph = build_impact_hypothesis_graph(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![
                    claim_node(
                        "order-1",
                        "CompanyFact",
                        "Acme Robotics wins major automation order",
                        vec![evidence_id(24)],
                        0.91,
                    ),
                    claim_node(
                        "peer-1",
                        "CompanyFact",
                        "Peer beneficiary basket 600519.SH 000001.SZ",
                        vec![evidence_id(25)],
                        0.75,
                    ),
                    claim_node(
                        "industry-1",
                        "DemandFact",
                        "Industrial automation demand",
                        vec![evidence_id(26)],
                        0.78,
                    ),
                    claim_node(
                        "upstream-1",
                        "SupplyFact",
                        "Upstream servo suppliers",
                        vec![evidence_id(27)],
                        0.74,
                    ),
                ],
                vec![
                    claim_edge("order-1", "peer-1", "impacts", vec![evidence_id(28)], 0.7),
                    claim_edge(
                        "order-1",
                        "industry-1",
                        "impacts",
                        vec![evidence_id(29)],
                        0.77,
                    ),
                    claim_edge(
                        "order-1",
                        "upstream-1",
                        "impacts",
                        vec![evidence_id(30)],
                        0.72,
                    ),
                ],
            )
            .unwrap(),
            vec![Uuid::from_u128(2002)],
            dt(2026, 7, 11, 11),
        )
        .unwrap();

        assert!(graph
            .nodes
            .iter()
            .all(|node| node.label != "Peer beneficiary basket"));
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.node_type == "IndustryImpact"
                && node.label == "Industrial automation demand"));
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.node_type == "StockArchetypeImpact"
                && node.label == "Upstream servo suppliers"));
    }

    #[test]
    fn target_from_claim_node_rejects_non_direct_beneficiary_labels_across_non_company_paths() {
        let edge = claim_edge(
            "source-1",
            "target-1",
            "impacts",
            vec![evidence_id(33)],
            0.81,
        );

        for node_type in [
            "DemandFact",
            "SupplyFact",
            "PriceFact",
            "PolicyFact",
            "RegulatoryFact",
            "SourceLabel",
        ] {
            let target = target_from_claim_node(
                &claim_node(
                    "target-1",
                    node_type,
                    "Peer beneficiary basket 600519.SH 000001.SZ",
                    vec![evidence_id(34)],
                    0.86,
                ),
                &edge,
            );

            assert!(
                target.is_none(),
                "expected {node_type} beneficiary list label to be rejected"
            );
        }
    }

    #[test]
    fn company_template_fallback_does_not_emit_beneficiary_list_source_labels() {
        let order_graph = build_impact_hypothesis_graph(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "order-1",
                    "CompanyFact",
                    "Peer beneficiary basket 600519.SH 000001.SZ wins major automation order",
                    vec![evidence_id(35)],
                    0.91,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(2003)],
            dt(2026, 7, 11, 11),
        )
        .unwrap();

        assert!(order_graph
            .nodes
            .iter()
            .all(|node| !(node.node_type == "RevenueImpact"
                && node.label == "Peer beneficiary basket")));
        assert!(order_graph
            .nodes
            .iter()
            .any(|node| node.node_type == "RevenueImpact"
                && node.label == "direct order beneficiary"));

        let accident_graph = build_impact_hypothesis_graph(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "accident-1",
                    "OperationalFact",
                    "Peer beneficiary basket 600519.SH 000001.SZ accident halts production",
                    vec![evidence_id(36)],
                    0.88,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(2004)],
            dt(2026, 7, 11, 12),
        )
        .unwrap();

        assert!(accident_graph
            .nodes
            .iter()
            .all(|node| !(node.node_type == "MarginImpact"
                && node.label == "Peer beneficiary basket")));
        assert!(accident_graph
            .nodes
            .iter()
            .any(|node| node.node_type == "MarginImpact"
                && node.label == "operationally exposed company margin"));
    }

    #[test]
    fn frozen_hypotheses_require_new_version_when_new_facts_arrive() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "order-1",
                    "CompanyFact",
                    "Acme Robotics wins major automation order",
                    vec![evidence_id(31)],
                    0.91,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(3001)],
            dt(2026, 7, 11, 12),
        )
        .unwrap();

        let evolved = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![
                        claim_node(
                            "order-1",
                            "CompanyFact",
                            "Acme Robotics wins major automation order",
                            vec![evidence_id(31)],
                            0.91,
                        ),
                        claim_node(
                            "order-2",
                            "CompanyFact",
                            "Order amount raised to 5 billion yuan",
                            vec![evidence_id(32)],
                            0.9,
                        ),
                    ],
                    Vec::new(),
                )
                .unwrap(),
                &event_delta().with_new_claim_ids(vec![Uuid::from_u128(3002)]),
                dt(2026, 7, 11, 13),
            )
            .unwrap();

        assert_eq!(initial.hypothesis_version(), 1);
        assert_eq!(evolved.hypothesis_version(), 2);
        assert_eq!(evolved.graph().based_on_claim_ids.len(), 2);
        assert_eq!(
            evolved.supersedes_hypothesis_id(),
            Some(initial.hypothesis_id())
        );
        assert_eq!(
            initial.graph().based_on_claim_ids,
            vec![Uuid::from_u128(3001)]
        );
    }

    #[test]
    fn frozen_hypotheses_reject_rebuild_without_new_facts() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "rate-1",
                    "MacroDataFact",
                    "Central bank adds liquidity",
                    vec![evidence_id(41)],
                    0.89,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(4001)],
            dt(2026, 7, 11, 14),
        )
        .unwrap();

        let error = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![claim_node(
                        "rate-1",
                        "MacroDataFact",
                        "Central bank adds liquidity",
                        vec![evidence_id(41)],
                        0.89,
                    )],
                    Vec::new(),
                )
                .unwrap(),
                &event_delta(),
                dt(2026, 7, 11, 15),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses require EventDelta new facts to create a new version"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_removed_claim_deltas_when_evolving() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "order-1",
                    "CompanyFact",
                    "Acme Robotics wins major automation order",
                    vec![evidence_id(42)],
                    0.91,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(5001), Uuid::from_u128(5002)],
            dt(2026, 7, 11, 16),
        )
        .unwrap();

        let error = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![claim_node(
                        "order-1",
                        "CompanyFact",
                        "Acme Robotics wins major automation order",
                        vec![evidence_id(42)],
                        0.91,
                    )],
                    Vec::new(),
                )
                .unwrap(),
                &event_delta()
                    .with_new_claim_ids(vec![Uuid::from_u128(5003)])
                    .with_removed_claim_ids(vec![Uuid::from_u128(5002)]),
                dt(2026, 7, 11, 17),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses only support additive EventDelta claim changes"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_revised_value_deltas_when_evolving() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "order-1",
                    "CompanyFact",
                    "Acme Robotics wins major automation order",
                    vec![evidence_id(43)],
                    0.91,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(6001), Uuid::from_u128(6002)],
            dt(2026, 7, 11, 18),
        )
        .unwrap();

        let error = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![
                        claim_node(
                            "order-1",
                            "CompanyFact",
                            "Acme Robotics wins major automation order",
                            vec![evidence_id(43)],
                            0.91,
                        ),
                        claim_node(
                            "order-2",
                            "DemandFact",
                            "Battery demand surges",
                            vec![evidence_id(44)],
                            0.85,
                        ),
                    ],
                    Vec::new(),
                )
                .unwrap(),
                &event_delta()
                    .with_new_claim_ids(vec![Uuid::from_u128(6003)])
                    .with_revised_values(vec![RevisedValue {
                        canonical_claim_id: "order_amount".to_string(),
                        previous_claim_id: Uuid::from_u128(6002),
                        current_claim_id: Uuid::from_u128(6004),
                        previous: Some(normalized_value("1 billion", "cny", "1000000000", "cny")),
                        current: Some(normalized_value("1.2 billion", "cny", "1200000000", "cny")),
                    }]),
                dt(2026, 7, 11, 19),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses only support additive EventDelta claim changes"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_status_change_deltas_when_evolving() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "rate-1",
                    "MacroDataFact",
                    "Central bank adds liquidity",
                    vec![evidence_id(61)],
                    0.89,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(6101)],
            dt(2026, 7, 11, 19),
        )
        .unwrap();

        let error = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![
                        claim_node(
                            "rate-1",
                            "MacroDataFact",
                            "Central bank adds liquidity",
                            vec![evidence_id(61)],
                            0.89,
                        ),
                        claim_node(
                            "rate-2",
                            "DemandFact",
                            "Credit demand improves",
                            vec![evidence_id(62)],
                            0.85,
                        ),
                    ],
                    Vec::new(),
                )
                .unwrap(),
                &event_delta()
                    .with_new_claim_ids(vec![Uuid::from_u128(6102)])
                    .with_status_changes(vec![StatusChange {
                        canonical_claim_id: "policy_rate".to_string(),
                        previous_claim_id: Uuid::from_u128(6101),
                        current_claim_id: Uuid::from_u128(6103),
                        previous_status: Some("expected".to_string()),
                        current_status: Some("confirmed".to_string()),
                    }]),
                dt(2026, 7, 11, 20),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses only support additive EventDelta claim changes"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_expectation_gap_deltas_when_evolving() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "policy-1",
                    "PolicyFact",
                    "Battery subsidy widened",
                    vec![evidence_id(63)],
                    0.9,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(6201)],
            dt(2026, 7, 11, 20),
        )
        .unwrap();

        let error = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![
                        claim_node(
                            "policy-1",
                            "PolicyFact",
                            "Battery subsidy widened",
                            vec![evidence_id(63)],
                            0.9,
                        ),
                        claim_node(
                            "policy-2",
                            "DemandFact",
                            "Electric vehicle demand improves",
                            vec![evidence_id(64)],
                            0.84,
                        ),
                    ],
                    Vec::new(),
                )
                .unwrap(),
                &event_delta()
                    .with_new_claim_ids(vec![Uuid::from_u128(6202)])
                    .with_expectation_gap(Some(ExpectationGap {
                        canonical_claim_id: "subsidy_realization".to_string(),
                        expected: Some(normalized_value("10 percent", "pct", "10", "pct")),
                        observed: Some(normalized_value("6 percent", "pct", "6", "pct")),
                        expected_date: None,
                        observed_date: None,
                    })),
                dt(2026, 7, 11, 21),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses only support additive EventDelta claim changes"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_expectation_only_deltas_when_evolving() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "policy-1",
                    "PolicyFact",
                    "Battery subsidy widened",
                    vec![evidence_id(65)],
                    0.9,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(6301)],
            dt(2026, 7, 11, 21),
        )
        .unwrap();

        let error = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![claim_node(
                        "policy-1",
                        "PolicyFact",
                        "Battery subsidy widened",
                        vec![evidence_id(65)],
                        0.9,
                    )],
                    Vec::new(),
                )
                .unwrap(),
                &event_delta().with_expectation_gap(Some(ExpectationGap {
                    canonical_claim_id: "subsidy_realization".to_string(),
                    expected: Some(normalized_value("10 percent", "pct", "10", "pct")),
                    observed: Some(normalized_value("6 percent", "pct", "6", "pct")),
                    expected_date: None,
                    observed_date: None,
                })),
                dt(2026, 7, 11, 22),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses only support additive EventDelta claim changes"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_mutated_prior_node_payload_when_evolving() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![
                    claim_node(
                        "policy-1",
                        "PolicyFact",
                        "New energy vehicle subsidy expanded",
                        vec![evidence_id(45)],
                        0.92,
                    ),
                    claim_node(
                        "company-1",
                        "CompanyFact",
                        "Acme Batteries",
                        vec![evidence_id(46)],
                        0.88,
                    ),
                ],
                vec![claim_edge(
                    "policy-1",
                    "company-1",
                    "applies_to",
                    vec![evidence_id(47)],
                    0.84,
                )],
            )
            .unwrap(),
            vec![Uuid::from_u128(7001)],
            dt(2026, 7, 11, 20),
        )
        .unwrap();

        let error = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![
                        claim_node(
                            "policy-1",
                            "PolicyFact",
                            "New energy vehicle subsidy expanded",
                            vec![evidence_id(45)],
                            0.92,
                        ),
                        claim_node(
                            "company-1",
                            "CompanyFact",
                            "Renamed Batteries",
                            vec![evidence_id(46)],
                            0.88,
                        ),
                        claim_node(
                            "demand-1",
                            "DemandFact",
                            "Battery demand surges",
                            vec![evidence_id(48)],
                            0.86,
                        ),
                    ],
                    vec![claim_edge(
                        "policy-1",
                        "company-1",
                        "applies_to",
                        vec![evidence_id(47)],
                        0.84,
                    )],
                )
                .unwrap(),
                &event_delta().with_new_claim_ids(vec![Uuid::from_u128(7002)]),
                dt(2026, 7, 11, 21),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses must preserve prior graph payload when evolving"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_removed_prior_edge_payload_when_evolving() {
        let initial = FrozenImpactHypothesis::initial(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![
                    claim_node(
                        "policy-1",
                        "PolicyFact",
                        "New energy vehicle subsidy expanded",
                        vec![evidence_id(49)],
                        0.92,
                    ),
                    claim_node(
                        "company-1",
                        "CompanyFact",
                        "Acme Batteries",
                        vec![evidence_id(50)],
                        0.88,
                    ),
                ],
                vec![claim_edge(
                    "policy-1",
                    "company-1",
                    "applies_to",
                    vec![evidence_id(51)],
                    0.84,
                )],
            )
            .unwrap(),
            vec![Uuid::from_u128(7101)],
            dt(2026, 7, 11, 22),
        )
        .unwrap();

        let error = initial
            .evolve(
                &ClaimGraph::new(
                    "claim_graph_v1",
                    vec![
                        claim_node(
                            "policy-1",
                            "PolicyFact",
                            "New energy vehicle subsidy expanded",
                            vec![evidence_id(49)],
                            0.92,
                        ),
                        claim_node(
                            "company-1",
                            "CompanyFact",
                            "Acme Batteries",
                            vec![evidence_id(50)],
                            0.88,
                        ),
                        claim_node(
                            "demand-1",
                            "DemandFact",
                            "Battery demand surges",
                            vec![evidence_id(52)],
                            0.86,
                        ),
                    ],
                    Vec::new(),
                )
                .unwrap(),
                &event_delta().with_new_claim_ids(vec![Uuid::from_u128(7102)]),
                dt(2026, 7, 11, 23),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses must preserve prior graph payload when evolving"
        );
    }

    #[test]
    fn linked_non_company_stock_code_lists_fall_back_instead_of_emitting_placeholder_targets() {
        for list_label in [
            "600519.SH,000001.SZ",
            "600519.SH/000001.SZ",
            "600519.SH; 000001.SZ",
        ] {
            for node_type in [
                "DemandFact",
                "SupplyFact",
                "PriceFact",
                "PolicyFact",
                "RegulatoryFact",
            ] {
                let graph = build_impact_hypothesis_graph(
                    &ClaimGraph::new(
                        "claim_graph_v1",
                        vec![
                            claim_node(
                                "policy-1",
                                "PolicyFact",
                                "Battery subsidy widened",
                                vec![evidence_id(53)],
                                0.9,
                            ),
                            claim_node(
                                "linked-1",
                                node_type,
                                list_label,
                                vec![evidence_id(54)],
                                0.82,
                            ),
                        ],
                        vec![claim_edge(
                            "policy-1",
                            "linked-1",
                            "affects",
                            vec![evidence_id(55)],
                            0.8,
                        )],
                    )
                    .unwrap(),
                    vec![Uuid::from_u128(7201)],
                    dt(2026, 7, 12, 0),
                )
                .unwrap();

                assert!(
                    graph
                        .nodes
                        .iter()
                        .all(|node| node.label != "directly mentioned entity"),
                    "expected {node_type} stock-code list label {list_label} to be rejected before sanitization"
                );
                assert!(
                    graph
                        .nodes
                        .iter()
                        .any(|node| node.node_type == "IndustryImpact"
                            && node.label == "subsidy-linked industry demand"),
                    "expected {node_type} stock-code list label {list_label} to fall back to the template target"
                );
            }
        }
    }

    #[test]
    fn target_from_claim_node_rejects_stock_code_only_labels_across_non_company_paths() {
        let edge = claim_edge(
            "source-1",
            "target-1",
            "impacts",
            vec![evidence_id(58)],
            0.81,
        );

        for list_label in [
            "600519.SH,000001.SZ",
            "600519.SH/000001.SZ",
            "600519.SH; 000001.SZ",
        ] {
            for node_type in [
                "DemandFact",
                "SupplyFact",
                "PriceFact",
                "PolicyFact",
                "RegulatoryFact",
                "SourceLabel",
            ] {
                let target = target_from_claim_node(
                    &claim_node(
                        "target-1",
                        node_type,
                        list_label,
                        vec![evidence_id(59)],
                        0.86,
                    ),
                    &edge,
                );

                assert!(
                    target.is_none(),
                    "expected {node_type} stock-code list label {list_label} to be rejected"
                );
            }
        }
    }

    #[test]
    fn company_template_fallback_does_not_emit_stock_code_list_source_subjects() {
        let order_graph = build_impact_hypothesis_graph(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "order-1",
                    "CompanyFact",
                    "600519.SH,000001.SZ wins major automation order",
                    vec![evidence_id(56)],
                    0.91,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(7301)],
            dt(2026, 7, 12, 1),
        )
        .unwrap();

        assert!(order_graph
            .nodes
            .iter()
            .all(|node| !(node.node_type == "RevenueImpact"
                && (node.label == "directly mentioned entity"
                    || node.label == "wins major automation order"))));
        assert!(order_graph
            .nodes
            .iter()
            .any(|node| node.node_type == "RevenueImpact"
                && node.label == "direct order beneficiary"));

        let accident_graph = build_impact_hypothesis_graph(
            &ClaimGraph::new(
                "claim_graph_v1",
                vec![claim_node(
                    "accident-1",
                    "OperationalFact",
                    "600519.SH/000001.SZ accident halts production",
                    vec![evidence_id(60)],
                    0.88,
                )],
                Vec::new(),
            )
            .unwrap(),
            vec![Uuid::from_u128(7302)],
            dt(2026, 7, 12, 2),
        )
        .unwrap();

        assert!(accident_graph
            .nodes
            .iter()
            .all(|node| !(node.node_type == "MarginImpact"
                && (node.label == "directly mentioned entity"
                    || node.label == "accident halts production"))));
        assert!(accident_graph
            .nodes
            .iter()
            .any(|node| node.node_type == "MarginImpact"
                && node.label == "operationally exposed company margin"));
    }

    #[test]
    fn frozen_hypothesis_wrapper_fields_are_private() {
        let source =
            std::fs::read_to_string(format!("{}/{}", env!("CARGO_MANIFEST_DIR"), file!())).unwrap();

        let struct_body = source
            .split("pub struct FrozenImpactHypothesis {")
            .nth(1)
            .and_then(|tail| tail.split("}").next())
            .unwrap();

        assert!(!struct_body.contains("pub hypothesis_id:"));
        assert!(!struct_body.contains("pub hypothesis_version:"));
        assert!(!struct_body.contains("pub supersedes_hypothesis_id:"));
        assert!(!struct_body.contains("pub graph:"));

        let _hypothesis_id: fn(&FrozenImpactHypothesis) -> Uuid =
            FrozenImpactHypothesis::hypothesis_id;
        let _hypothesis_version: fn(&FrozenImpactHypothesis) -> i32 =
            FrozenImpactHypothesis::hypothesis_version;
        let _supersedes: fn(&FrozenImpactHypothesis) -> Option<Uuid> =
            FrozenImpactHypothesis::supersedes_hypothesis_id;
        let _graph: fn(&FrozenImpactHypothesis) -> &ImpactHypothesisGraph =
            FrozenImpactHypothesis::graph;
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

    struct EventDeltaBuilder {
        delta: EventDelta,
    }

    fn event_delta() -> EventDeltaBuilder {
        EventDeltaBuilder {
            delta: EventDelta {
                new_claim_ids: Vec::new(),
                repeated_claim_ids: Vec::new(),
                revised_values: Vec::new(),
                removed_claim_ids: Vec::new(),
                status_changes: Vec::new(),
                expectation_gap: None,
                new_uncertainties: Vec::new(),
                resolved_uncertainties: Vec::new(),
            },
        }
    }

    impl EventDeltaBuilder {
        fn with_new_claim_ids(mut self, new_claim_ids: Vec<Uuid>) -> Self {
            self.delta.new_claim_ids = new_claim_ids;
            self
        }

        fn with_revised_values(mut self, revised_values: Vec<RevisedValue>) -> Self {
            self.delta.revised_values = revised_values;
            self
        }

        fn with_removed_claim_ids(mut self, removed_claim_ids: Vec<Uuid>) -> Self {
            self.delta.removed_claim_ids = removed_claim_ids;
            self
        }

        fn with_status_changes(mut self, status_changes: Vec<StatusChange>) -> Self {
            self.delta.status_changes = status_changes;
            self
        }

        fn with_expectation_gap(mut self, expectation_gap: Option<ExpectationGap>) -> Self {
            self.delta.expectation_gap = expectation_gap;
            self
        }
    }

    impl std::ops::Deref for EventDeltaBuilder {
        type Target = EventDelta;

        fn deref(&self) -> &Self::Target {
            &self.delta
        }
    }

    fn normalized_value(
        raw_value: &str,
        raw_unit: &str,
        normalized_value: &str,
        normalized_unit: &str,
    ) -> NormalizedValue {
        NormalizedValue {
            raw_value: raw_value.to_string(),
            raw_unit: Some(raw_unit.to_string()),
            normalized_value: normalized_value.to_string(),
            normalized_unit: Some(normalized_unit.to_string()),
        }
    }

    fn evidence_id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        chrono::TimeZone::with_ymd_and_hms(&Utc, year, month, day, hour, 0, 0)
            .single()
            .unwrap()
    }

    fn contains_stock_code(value: &str) -> bool {
        value.split_whitespace().any(|token| {
            let mut pieces = token.split('.');
            let Some(left) = pieces.next() else {
                return false;
            };
            let Some(right) = pieces.next() else {
                return false;
            };
            if pieces.next().is_some() {
                return false;
            }

            (left.len() == 6 && left.chars().all(|ch| ch.is_ascii_digit()))
                && matches!(right, "SH" | "SZ" | "BJ")
        })
    }
}
