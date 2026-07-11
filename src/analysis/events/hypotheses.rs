use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::claims::{ClaimEdge, ClaimGraph, ClaimNode};
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
        based_on_claim_ids: Vec<Uuid>,
        frozen_at: DateTime<Utc>,
    ) -> Result<Self> {
        let canonical_ids = canonical_claim_ids(based_on_claim_ids)?;
        let prior_ids = &self.graph.based_on_claim_ids;
        let prior_preserved = prior_ids
            .iter()
            .all(|claim_id| canonical_ids.binary_search(claim_id).is_ok());
        let has_new_fact = canonical_ids.len() > prior_ids.len();

        if !prior_preserved || !has_new_fact {
            return Err(AppError::BadRequest(
                "frozen hypotheses require new facts to create a new version".to_string(),
            ));
        }

        Ok(Self {
            hypothesis_id: Uuid::new_v4(),
            hypothesis_version: self.hypothesis_version + 1,
            supersedes_hypothesis_id: Some(self.hypothesis_id),
            graph: build_impact_hypothesis_graph(claim_graph, canonical_ids, frozen_at)?,
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
    let sanitized_label = sanitize_hypothesis_label(&node.label);
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
        "company_order_v1" | "company_accident_v1" => (
            extract_company_subject(&sanitize_hypothesis_label(&source_node.label)),
            template.fallback_target_type.to_string(),
        ),
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

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
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
    let lower = value.to_ascii_lowercase();
    !contains_any(&lower, &["peer", "beneficiary", "indirect", "basket"])
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
                vec![Uuid::from_u128(3001), Uuid::from_u128(3002)],
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
                vec![Uuid::from_u128(4001)],
                dt(2026, 7, 11, 15),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses require new facts to create a new version"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_subset_claim_ids_when_evolving() {
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
                vec![Uuid::from_u128(5001)],
                dt(2026, 7, 11, 17),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses require new facts to create a new version"
        );
    }

    #[test]
    fn frozen_hypotheses_reject_replacement_claim_ids_when_evolving() {
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
                            "CompanyFact",
                            "Replacement order fact",
                            vec![evidence_id(44)],
                            0.85,
                        ),
                    ],
                    Vec::new(),
                )
                .unwrap(),
                vec![Uuid::from_u128(6001), Uuid::from_u128(6003)],
                dt(2026, 7, 11, 19),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(
            error.to_string(),
            "frozen hypotheses require new facts to create a new version"
        );
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
