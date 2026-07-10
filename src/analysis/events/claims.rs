use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{AppError, Result};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimGraph {
    pub schema_version: String,
    pub nodes: Vec<ClaimNode>,
    pub edges: Vec<ClaimEdge>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimNode {
    pub node_id: String,
    pub node_type: String,
    pub label: String,
    pub evidence_ids: Vec<Uuid>,
    pub confidence: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimEdge {
    pub from: String,
    pub to: String,
    pub relation: String,
    pub evidence_ids: Vec<Uuid>,
    pub confidence: f64,
}

impl ClaimGraph {
    pub fn new(
        schema_version: impl Into<String>,
        nodes: Vec<ClaimNode>,
        edges: Vec<ClaimEdge>,
    ) -> Result<Self> {
        for node in &nodes {
            if node.evidence_ids.is_empty() {
                return Err(AppError::DataProvider(format!(
                    "claim graph node `{}` must reference at least one evidence id",
                    node.node_id
                )));
            }
        }

        for edge in &edges {
            if edge.evidence_ids.is_empty() {
                return Err(AppError::DataProvider(format!(
                    "claim graph edge `{}->{}:{}` must reference at least one evidence id",
                    edge.from, edge.to, edge.relation
                )));
            }
        }

        Ok(Self {
            schema_version: schema_version.into(),
            nodes,
            edges,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_graph_accepts_evidence_backed_nodes_and_edges() {
        let graph = ClaimGraph::new(
            "claim_graph_v1",
            vec![
                node(
                    "policy-1",
                    "PolicyFact",
                    "Tax rebate",
                    vec![evidence_id(1)],
                    0.95,
                ),
                node(
                    "company-1",
                    "CompanyFact",
                    "Acme Electronics",
                    vec![evidence_id(1), evidence_id(2)],
                    0.9,
                ),
            ],
            vec![edge(
                "policy-1",
                "company-1",
                "applies_to",
                vec![evidence_id(2)],
                0.88,
            )],
        )
        .unwrap();

        assert_eq!(graph.schema_version, "claim_graph_v1");
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes[0].evidence_ids, vec![evidence_id(1)]);
        assert_eq!(graph.edges[0].evidence_ids, vec![evidence_id(2)]);
    }

    #[test]
    fn claim_graph_rejects_nodes_without_evidence_ids() {
        let error = ClaimGraph::new(
            "claim_graph_v1",
            vec![node(
                "policy-1",
                "PolicyFact",
                "Tax rebate",
                Vec::new(),
                0.95,
            )],
            Vec::new(),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Data provider error: claim graph node `policy-1` must reference at least one evidence id"
        );
    }

    #[test]
    fn claim_graph_rejects_edges_without_evidence_ids() {
        let error = ClaimGraph::new(
            "claim_graph_v1",
            vec![
                node(
                    "policy-1",
                    "PolicyFact",
                    "Tax rebate",
                    vec![evidence_id(1)],
                    0.95,
                ),
                node(
                    "company-1",
                    "CompanyFact",
                    "Acme",
                    vec![evidence_id(2)],
                    0.9,
                ),
            ],
            vec![edge(
                "policy-1",
                "company-1",
                "applies_to",
                Vec::new(),
                0.88,
            )],
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Data provider error: claim graph edge `policy-1->company-1:applies_to` must reference at least one evidence id"
        );
    }

    fn node(
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

    fn edge(
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
}
