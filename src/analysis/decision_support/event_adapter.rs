use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};

use crate::analysis::decision_support::contracts::{
    DecisionCandidate, SupportStatement, SupportStatementKind,
};
use crate::analysis::events::claims::ClaimGraph;
use crate::analysis::events::extraction::EventExtractionV1;
use crate::analysis::events::hypotheses::ImpactHypothesisGraph;
use crate::analysis::events::BriefSource;
use crate::analysis::events::DailyEventBrief;
use crate::error::Result;
use crate::storage::event_repository::EventRepository;
use crate::storage::event_repository::MarketObservationRow;
use crate::storage::market_repository::MarketRepository;

pub(crate) async fn apply_event_context(
    trade_date: NaiveDate,
    candidates: Vec<DecisionCandidate>,
    event_summary: Option<&DailyEventBrief>,
    event_repo: &EventRepository,
    market_repo: &MarketRepository,
) -> Result<Vec<DecisionCandidate>> {
    let Some(event_summary) = event_summary else {
        return Ok(normalize_scores(candidates));
    };
    if candidates.is_empty() || event_summary.sources.is_empty() {
        return Ok(normalize_scores(candidates));
    }

    let evidence_contexts = load_evidence_contexts(event_summary, event_repo).await?;
    let mut sector_cache = BTreeMap::<(String, DateTime<Utc>), BTreeSet<String>>::new();
    let mut enriched = Vec::with_capacity(candidates.len());

    for mut candidate in candidates {
        for context in &evidence_contexts {
            let direct_match = context.direct_company_codes.contains(&candidate.code);
            let matched_industries = if direct_match || context.direct_industries.is_empty() {
                Vec::new()
            } else {
                matched_candidate_industries(
                    &candidate.code,
                    trade_date,
                    context.source.available_at,
                    &context.direct_industries,
                    market_repo,
                    &mut sector_cache,
                )
                .await?
            };

            if !direct_match && matched_industries.is_empty() {
                continue;
            }

            candidate.facts.extend(context.facts.clone());
            candidate.inferences.extend(context.inferences.clone());

            if direct_match {
                candidate.calculations.extend(
                    context
                        .observations
                        .iter()
                        .filter(|observation| {
                            observation.entity_type == "company"
                                && observation.entity_id == candidate.code
                        })
                        .map(|observation| calculation_statement(&context.source, observation)),
                );
            } else {
                candidate.unknowns.push(SupportStatement::missing_status(format!(
                    "Industry context matched {} via point-in-time sector membership [{}], but no direct stock mapping was persisted for {}.",
                    candidate.code,
                    matched_industries.join(", "),
                    context.source.title,
                )));
            }
        }

        dedupe_statements(&mut candidate.facts);
        dedupe_statements(&mut candidate.calculations);
        dedupe_statements(&mut candidate.inferences);
        dedupe_statements(&mut candidate.unknowns);
        candidate.event_adjustment = 0.0;
        candidate.final_score = candidate.base_score + candidate.risk_adjustment;
        enriched.push(candidate);
    }

    enriched.sort_by(|left, right| {
        right
            .final_score
            .total_cmp(&left.final_score)
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.horizon.cmp(&right.horizon))
    });
    Ok(enriched)
}

#[derive(Clone)]
struct EvidenceContext {
    source: BriefSource,
    direct_company_codes: BTreeSet<String>,
    direct_industries: BTreeSet<String>,
    facts: Vec<SupportStatement>,
    inferences: Vec<SupportStatement>,
    observations: Vec<MarketObservationRow>,
}

async fn load_evidence_contexts(
    event_summary: &DailyEventBrief,
    event_repo: &EventRepository,
) -> Result<Vec<EvidenceContext>> {
    let evidence_ids = event_summary
        .sources
        .iter()
        .map(|source| source.evidence_id)
        .collect::<Vec<_>>();
    let extraction_rows = event_repo
        .list_latest_extractions_for_evidence_ids(&evidence_ids)
        .await?;
    let extraction_by_evidence = extraction_rows
        .into_iter()
        .map(|row| (row.evidence_id, row))
        .collect::<BTreeMap<_, _>>();

    let mut contexts = Vec::with_capacity(event_summary.sources.len());
    for source in &event_summary.sources {
        let parsed_extraction = extraction_by_evidence
            .get(&source.evidence_id)
            .map(|row| serde_json::from_value::<EventExtractionV1>(row.extracted_payload.clone()))
            .transpose()?;
        let direct_company_codes = parsed_extraction
            .as_ref()
            .map(extraction_direct_company_codes)
            .unwrap_or_default();
        let direct_industries = parsed_extraction
            .as_ref()
            .map(extraction_direct_industries)
            .unwrap_or_default();

        let claim_graph = event_repo
            .find_latest_claim_graph_for_evidence(source.evidence_id)
            .await?
            .map(|row| serde_json::from_value::<ClaimGraph>(row.graph_payload))
            .transpose()?;
        let facts = claim_graph
            .as_ref()
            .map(|graph| fact_statements(source, graph))
            .unwrap_or_default();

        let hypothesis = event_repo
            .find_latest_hypothesis_for_evidence(source.evidence_id)
            .await?
            .map(|row| serde_json::from_value::<ImpactHypothesisGraph>(row.graph_payload))
            .transpose()?;
        let mut all_direct_company_codes = direct_company_codes.clone();
        if let Some(hypothesis) = hypothesis.as_ref() {
            all_direct_company_codes.extend(
                hypothesis
                    .direct_observation_entities
                    .iter()
                    .filter(|entity| entity.entity_type == "company")
                    .map(|entity| entity.entity_id.clone()),
            );
        }
        let inferences = hypothesis
            .as_ref()
            .map(|graph| inference_statements(source, graph))
            .unwrap_or_default();
        let observations = event_repo
            .list_market_observations_for_evidence(source.evidence_id)
            .await?;

        contexts.push(EvidenceContext {
            source: source.clone(),
            direct_company_codes: all_direct_company_codes,
            direct_industries,
            facts,
            inferences,
            observations,
        });
    }

    Ok(contexts)
}

fn extraction_direct_company_codes(extraction: &EventExtractionV1) -> BTreeSet<String> {
    extraction
        .entities
        .iter()
        .filter(|entity| entity.role == "subject")
        .filter(|entity| {
            matches!(
                entity.entity_type.as_str(),
                "organization" | "company" | "issuer"
            )
        })
        .filter_map(|entity| entity.stock_code.clone())
        .collect()
}

fn extraction_direct_industries(extraction: &EventExtractionV1) -> BTreeSet<String> {
    extraction
        .entities
        .iter()
        .filter(|entity| entity.role == "subject")
        .filter(|entity| matches!(entity.entity_type.as_str(), "industry" | "sector"))
        .map(|entity| canonical_industry(&entity.text))
        .collect()
}

fn fact_statements(source: &BriefSource, graph: &ClaimGraph) -> Vec<SupportStatement> {
    let refs = source_refs(source);
    let node_labels = graph
        .nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node.label.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut statements = graph
        .nodes
        .iter()
        .filter(|node| node.evidence_ids.contains(&source.evidence_id))
        .map(|node| SupportStatement::event_fact(node.label.clone(), refs.clone()))
        .collect::<Vec<_>>();

    statements.extend(graph.edges.iter().filter_map(|edge| {
        if !edge.evidence_ids.contains(&source.evidence_id) {
            return None;
        }
        let from = node_labels
            .get(edge.from.as_str())
            .copied()
            .unwrap_or(edge.from.as_str());
        let to = node_labels
            .get(edge.to.as_str())
            .copied()
            .unwrap_or(edge.to.as_str());
        Some(SupportStatement::event_fact(
            format!("{from} {} {to}", edge.relation),
            refs.clone(),
        ))
    }));

    statements
}

fn inference_statements(
    source: &BriefSource,
    graph: &ImpactHypothesisGraph,
) -> Vec<SupportStatement> {
    let refs = source_refs(source);
    let node_labels = graph
        .nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node.label.as_str()))
        .collect::<BTreeMap<_, _>>();

    graph
        .edges
        .iter()
        .map(|edge| {
            let from = node_labels
                .get(edge.from.as_str())
                .copied()
                .unwrap_or(edge.from.as_str());
            let to = node_labels
                .get(edge.to.as_str())
                .copied()
                .unwrap_or(edge.to.as_str());
            SupportStatement::new(
                SupportStatementKind::ImpactHypothesis,
                format!(
                    "{from} {} {to} (confidence {:.2}, horizon {})",
                    edge.relation, edge.confidence, edge.expected_horizon
                ),
                refs.clone(),
            )
        })
        .collect()
}

fn calculation_statement(
    source: &BriefSource,
    observation: &MarketObservationRow,
) -> SupportStatement {
    SupportStatement::new(
        SupportStatementKind::OtherCalculation,
        format!(
            "Market observation for {} on {}: status={}, causal_confidence={:.2}, market_alignment_score={}, abnormal_market_return={}, abnormal_industry_return={}",
            observation.entity_id,
            observation.trade_date,
            observation.observation_status,
            observation.causal_confidence,
            optional_number(observation.market_alignment_score),
            optional_number(observation.abnormal_market_return),
            optional_number(observation.abnormal_industry_return),
        ),
        source_refs(source),
    )
}

async fn matched_candidate_industries(
    code: &str,
    trade_date: NaiveDate,
    as_of: DateTime<Utc>,
    direct_industries: &BTreeSet<String>,
    market_repo: &MarketRepository,
    cache: &mut BTreeMap<(String, DateTime<Utc>), BTreeSet<String>>,
) -> Result<Vec<String>> {
    let key = (code.to_string(), as_of);
    if !cache.contains_key(&key) {
        let memberships = market_repo
            .active_sector_memberships(code, trade_date, as_of)
            .await?;
        let mut normalized = BTreeSet::new();
        for membership in memberships {
            normalized.insert(canonical_industry(&membership.sector_name));
            normalized.insert(canonical_industry(&membership.sector_code));
        }
        cache.insert(key.clone(), normalized);
    }

    Ok(cache[&key]
        .intersection(direct_industries)
        .cloned()
        .collect())
}

fn canonical_industry(value: &str) -> String {
    value
        .trim()
        .strip_prefix("industry:")
        .unwrap_or(value.trim())
        .to_ascii_lowercase()
}

fn source_refs(source: &BriefSource) -> Vec<String> {
    vec![
        format!("evidence:{}", source.evidence_id),
        format!("source:{}:{}", source.source_id, source.source_item_id),
    ]
}

fn optional_number(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn dedupe_statements(statements: &mut Vec<SupportStatement>) {
    let mut seen = BTreeSet::new();
    statements.retain(|statement| {
        seen.insert((
            format!("{:?}", statement.kind),
            statement.statement.clone(),
            statement.source_refs.clone(),
        ))
    });
}

fn normalize_scores(candidates: Vec<DecisionCandidate>) -> Vec<DecisionCandidate> {
    candidates
        .into_iter()
        .map(|mut candidate| {
            candidate.event_adjustment = 0.0;
            candidate.final_score = candidate.base_score + candidate.risk_adjustment;
            candidate
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::apply_event_context;
    use crate::analysis::decision_support::{
        DecisionCandidate, SupportStatement, SupportStatementKind,
    };
    use crate::analysis::events::claims::{ClaimEdge, ClaimGraph, ClaimNode};
    use crate::analysis::events::extraction::{
        ClaimType, EventExtractionV1, ExtractedAmount, ExtractedClaim, ExtractedDate,
        ExtractedEntity, EVENT_EXTRACTION_SCHEMA_VERSION,
    };
    use crate::analysis::events::hypotheses::{
        HypothesisEdge, HypothesisNode, HypothesisObservationEntity, ImpactHypothesisGraph,
        IMPACT_HYPOTHESIS_SCHEMA_VERSION,
    };
    use crate::analysis::events::{BriefEntity, BriefFact, BriefSource, DailyEventBrief};
    use crate::analysis::market_snapshot::{AvailabilityQuality, SectorMembership};
    use crate::storage::event_repository::{
        ClaimGraphRow, EventClusterRow, EventEvidenceRow, EventHypothesisRow, EventMentionRow,
        EventRepository, ExtractionRow, MarketObservationRow,
    };
    use crate::storage::market_repository::MarketRepository;
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

    fn candidate(code: &str) -> DecisionCandidate {
        DecisionCandidate {
            code: code.to_string(),
            name: format!("Name {code}"),
            horizon: "short".to_string(),
            base_source: "scan_ranker".to_string(),
            base_score: 82.0,
            pattern_score: None,
            event_adjustment: 9.9,
            risk_adjustment: -1.5,
            final_score: -999.0,
            support_tier: "A".to_string(),
            facts: Vec::new(),
            calculations: Vec::new(),
            inferences: Vec::new(),
            unknowns: Vec::new(),
            risk_flags: Vec::new(),
            invalidations: Vec::new(),
        }
    }

    fn evidence_row(
        evidence_id: Uuid,
        source_item_id: &str,
        title: &str,
        trade_date: NaiveDate,
        available_at: chrono::DateTime<Utc>,
    ) -> EventEvidenceRow {
        EventEvidenceRow {
            evidence_id,
            source_id: "manual".to_string(),
            source_item_id: source_item_id.to_string(),
            source_url: None,
            source_tier: "tier1".to_string(),
            source_terms_version: "terms-v1".to_string(),
            occurred_at: None,
            published_at: Some(available_at),
            first_seen_at: available_at,
            available_at,
            effective_trade_date: trade_date,
            title: title.to_string(),
            content: Some(format!("{title} content")),
            language: "en".to_string(),
            content_hash: format!("hash-{source_item_id}"),
            raw_payload: json!({}),
            version: 1,
            supersedes_evidence_id: None,
            status: "publishable".to_string(),
            created_at: available_at,
        }
    }

    fn extraction_row(
        evidence_id: Uuid,
        entities: Vec<ExtractedEntity>,
        created_at: chrono::DateTime<Utc>,
    ) -> ExtractionRow {
        ExtractionRow {
            extraction_id: Uuid::new_v4(),
            evidence_id,
            schema_version: EVENT_EXTRACTION_SCHEMA_VERSION.to_string(),
            prompt_version: Some("prompt-v1".to_string()),
            model_name: Some("test-model".to_string()),
            model_parameters: json!({}),
            extracted_payload: serde_json::to_value(EventExtractionV1 {
                event_type: "company_event".to_string(),
                event_subtype: None,
                claims: vec![ExtractedClaim {
                    claim_type: ClaimType::Fact,
                    text: "Persisted fact".to_string(),
                    evidence_ids: vec![evidence_id],
                    confidence: 0.91,
                }],
                entities,
                amounts: Vec::<ExtractedAmount>::new(),
                dates: Vec::<ExtractedDate>::new(),
                uncertainties: Vec::new(),
                missing_information: Vec::new(),
            })
            .unwrap(),
            validation_status: "valid".to_string(),
            validation_errors: json!([]),
            input_fingerprint: format!("extract-{evidence_id}"),
            claims: Vec::new(),
            created_at,
        }
    }

    fn claim_graph_row(
        evidence_id: Uuid,
        label: &str,
        created_at: chrono::DateTime<Utc>,
    ) -> ClaimGraphRow {
        ClaimGraphRow {
            claim_graph_id: Uuid::new_v4(),
            evidence_id,
            graph_version: 1,
            schema_version: "claim_graph_v1".to_string(),
            graph_payload: serde_json::to_value(
                ClaimGraph::new(
                    "claim_graph_v1",
                    vec![ClaimNode {
                        node_id: "fact-1".to_string(),
                        node_type: "CompanyFact".to_string(),
                        label: label.to_string(),
                        evidence_ids: vec![evidence_id],
                        confidence: 0.94,
                    }],
                    vec![ClaimEdge {
                        from: "fact-1".to_string(),
                        to: "fact-1".to_string(),
                        relation: "echoes".to_string(),
                        evidence_ids: vec![evidence_id],
                        confidence: 0.75,
                    }],
                )
                .unwrap(),
            )
            .unwrap(),
            review_status: "published".to_string(),
            created_at,
        }
    }

    fn cluster_row(
        cluster_id: Uuid,
        evidence_id: Uuid,
        created_at: chrono::DateTime<Utc>,
    ) -> EventClusterRow {
        EventClusterRow {
            event_cluster_id: cluster_id,
            cluster_version: 1,
            canonical_title: "cluster".to_string(),
            event_time: None,
            first_seen_at: created_at,
            last_seen_at: created_at,
            lifecycle_status: "active".to_string(),
            primary_evidence_id: evidence_id,
            representative_ids: vec![evidence_id],
            source_entropy: 1.0,
            independent_sources: 1,
            mention_count: 1,
            cluster_payload: json!({}),
            supersedes_version: None,
            created_at,
        }
    }

    fn mention_row(
        evidence_id: Uuid,
        cluster_id: Uuid,
        created_at: chrono::DateTime<Utc>,
    ) -> EventMentionRow {
        EventMentionRow {
            mention_id: Uuid::new_v4(),
            evidence_id,
            event_cluster_id: Some(cluster_id),
            cluster_version: Some(1),
            mention_time: created_at,
            adds_new_fact: true,
            source_independence: 0.9,
            mention_payload: json!({}),
            created_at,
        }
    }

    fn hypothesis_row(
        cluster_id: Uuid,
        direct_entities: Vec<HypothesisObservationEntity>,
        source_label: &str,
        impact_label: &str,
        created_at: chrono::DateTime<Utc>,
    ) -> EventHypothesisRow {
        EventHypothesisRow {
            hypothesis_id: Uuid::new_v4(),
            event_cluster_id: cluster_id,
            cluster_version: 1,
            hypothesis_version: 1,
            schema_version: IMPACT_HYPOTHESIS_SCHEMA_VERSION.to_string(),
            graph_payload: serde_json::to_value(ImpactHypothesisGraph {
                schema_version: IMPACT_HYPOTHESIS_SCHEMA_VERSION.to_string(),
                nodes: vec![
                    HypothesisNode {
                        node_id: "source".to_string(),
                        node_type: "Source".to_string(),
                        label: source_label.to_string(),
                    },
                    HypothesisNode {
                        node_id: "impact".to_string(),
                        node_type: "IndustryImpact".to_string(),
                        label: impact_label.to_string(),
                    },
                ],
                edges: vec![HypothesisEdge {
                    from: "source".to_string(),
                    to: "impact".to_string(),
                    relation: "supports".to_string(),
                    generation_method: "domain_rule".to_string(),
                    logic_rule_id: Some("rule-v1".to_string()),
                    confidence: 0.83,
                    assumptions: vec!["assumption".to_string()],
                    expected_horizon: "swing".to_string(),
                    observable_indicators: vec!["indicator".to_string()],
                    counter_scenario: vec!["counter".to_string()],
                    invalidation_conditions: vec!["invalidation".to_string()],
                }],
                direct_observation_entities: direct_entities,
                based_on_claim_ids: vec![Uuid::new_v4()],
                frozen_at: created_at,
            })
            .unwrap(),
            frozen_at: created_at,
            based_on_claim_ids: vec![Uuid::new_v4()],
            review_status: "published".to_string(),
            supersedes_id: None,
            created_at,
        }
    }

    fn observation_row(
        hypothesis_id: Uuid,
        entity_id: &str,
        trade_date: NaiveDate,
        created_at: chrono::DateTime<Utc>,
    ) -> MarketObservationRow {
        MarketObservationRow {
            hypothesis_id,
            entity_type: "company".to_string(),
            entity_id: entity_id.to_string(),
            trade_date,
            observation_status: "market_aligned".to_string(),
            market_alignment_score: Some(0.78),
            causal_confidence: 0.46,
            abnormal_market_return: Some(0.032),
            abnormal_industry_return: Some(0.021),
            market_metrics: json!({"window": "t+1"}),
            confounding_events: json!([]),
            created_at,
        }
    }

    fn brief_from_sources(
        trade_date: NaiveDate,
        sources: &[EventEvidenceRow],
        direct_entities: Vec<BriefEntity>,
    ) -> DailyEventBrief {
        DailyEventBrief {
            trade_date,
            new_facts: vec![BriefFact {
                fact_id: Uuid::new_v4(),
                summary: "Brief fact".to_string(),
                evidence_ids: sources.iter().map(|row| row.evidence_id).collect(),
            }],
            revisions: Vec::new(),
            unconfirmed: Vec::new(),
            direct_entities,
            sources: sources
                .iter()
                .map(|row| BriefSource {
                    evidence_id: row.evidence_id,
                    source_id: row.source_id.clone(),
                    source_item_id: row.source_item_id.clone(),
                    published_at: row.published_at,
                    available_at: row.available_at,
                    title: row.title.clone(),
                })
                .collect(),
            input_fingerprint: "brief".to_string(),
        }
    }

    fn sector_membership(
        code: &str,
        sector_name: &str,
        valid_to: Option<NaiveDate>,
        available_at: chrono::DateTime<Utc>,
    ) -> SectorMembership {
        SectorMembership {
            code: code.to_string(),
            sector_code: sector_name.to_ascii_uppercase(),
            sector_name: sector_name.to_string(),
            sector_type: "industry".to_string(),
            valid_from: date(2026, 7, 1),
            valid_to,
            available_at,
            ingested_at: available_at,
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn direct_stock_match_adds_bucketed_event_context_and_keeps_zero_weight(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let trade_date = date(2026, 7, 11);
        let event_repo = EventRepository::new(pool.clone());
        let market_repo = MarketRepository::new(pool);
        let evidence_id = Uuid::new_v4();
        let available_at = dt(2026, 7, 11, 9);
        let evidence = evidence_row(
            evidence_id,
            "direct-stock",
            "Direct stock event",
            trade_date,
            available_at,
        );
        event_repo.insert_evidence(&evidence).await.unwrap();
        event_repo
            .save_extraction(&extraction_row(
                evidence_id,
                vec![ExtractedEntity {
                    text: "Kweichow Moutai".to_string(),
                    entity_type: "issuer".to_string(),
                    role: "subject".to_string(),
                    stock_code: Some("600519.SH".to_string()),
                }],
                available_at,
            ))
            .await
            .unwrap();
        event_repo
            .save_claim_graph(&claim_graph_row(
                evidence_id,
                "Kweichow Moutai 600519.SH expands capex",
                available_at,
            ))
            .await
            .unwrap();
        let cluster_id = Uuid::new_v4();
        event_repo
            .save_event_cluster_version_with_mentions(
                &cluster_row(cluster_id, evidence_id, available_at),
                &[mention_row(evidence_id, cluster_id, available_at)],
            )
            .await
            .unwrap();
        let hypothesis = hypothesis_row(
            cluster_id,
            vec![HypothesisObservationEntity {
                entity_type: "company".to_string(),
                entity_id: "600519.SH".to_string(),
                display_name: "Kweichow Moutai".to_string(),
            }],
            "Capex expansion",
            "Margin expansion",
            available_at,
        );
        event_repo
            .save_frozen_hypothesis(&hypothesis)
            .await
            .unwrap();
        event_repo
            .save_market_observation(&observation_row(
                hypothesis.hypothesis_id,
                "600519.SH",
                trade_date,
                available_at,
            ))
            .await
            .unwrap();

        let enriched = apply_event_context(
            trade_date,
            vec![candidate("600519.SH")],
            Some(&brief_from_sources(
                trade_date,
                std::slice::from_ref(&evidence),
                vec![BriefEntity {
                    entity_id: "600519.SH".to_string(),
                    display_name: "Kweichow Moutai".to_string(),
                }],
            )),
            &event_repo,
            &market_repo,
        )
        .await
        .unwrap();

        let candidate = &enriched[0];
        assert!(candidate
            .facts
            .iter()
            .any(|statement| statement.statement.contains("expands capex")));
        assert!(candidate
            .calculations
            .iter()
            .any(|statement| statement.statement.contains("market_aligned")));
        assert!(candidate
            .inferences
            .iter()
            .any(|statement| statement.statement.contains("Margin expansion")));
        assert!(candidate.unknowns.is_empty());
        assert_eq!(candidate.event_adjustment, 0.0);
        assert_eq!(
            candidate.final_score,
            candidate.base_score + candidate.risk_adjustment
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn industry_match_uses_point_in_time_membership_and_marks_missing_direct_mapping(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let trade_date = date(2026, 7, 11);
        let event_repo = EventRepository::new(pool.clone());
        let market_repo = MarketRepository::new(pool.clone());
        let early_evidence_id = Uuid::new_v4();
        let late_evidence_id = Uuid::new_v4();
        let early_at = dt(2026, 7, 11, 9);
        let late_at = dt(2026, 7, 11, 13);
        let early = evidence_row(
            early_evidence_id,
            "industry-early",
            "Early semiconductor event",
            trade_date,
            early_at,
        );
        let late = evidence_row(
            late_evidence_id,
            "industry-late",
            "Late semiconductor event",
            trade_date,
            late_at,
        );
        event_repo.insert_evidence(&early).await.unwrap();
        event_repo.insert_evidence(&late).await.unwrap();
        for (evidence_id, created_at, label) in [
            (
                early_evidence_id,
                early_at,
                "Semiconductor supply policy tightens early",
            ),
            (
                late_evidence_id,
                late_at,
                "Semiconductor supply policy tightens late",
            ),
        ] {
            event_repo
                .save_extraction(&extraction_row(
                    evidence_id,
                    vec![ExtractedEntity {
                        text: "Semiconductor".to_string(),
                        entity_type: "industry".to_string(),
                        role: "subject".to_string(),
                        stock_code: None,
                    }],
                    created_at,
                ))
                .await
                .unwrap();
            event_repo
                .save_claim_graph(&claim_graph_row(evidence_id, label, created_at))
                .await
                .unwrap();
            let cluster_id = Uuid::new_v4();
            event_repo
                .save_event_cluster_version_with_mentions(
                    &cluster_row(cluster_id, evidence_id, created_at),
                    &[mention_row(evidence_id, cluster_id, created_at)],
                )
                .await
                .unwrap();
            event_repo
                .save_frozen_hypothesis(&hypothesis_row(
                    cluster_id,
                    Vec::new(),
                    "Semiconductor policy",
                    "Chip pricing support",
                    created_at,
                ))
                .await
                .unwrap();
        }
        market_repo
            .append_sector_memberships(&[
                sector_membership("300001.SZ", "Semiconductor", None, dt(2026, 7, 11, 8)),
                sector_membership(
                    "300001.SZ",
                    "Semiconductor",
                    Some(date(2026, 7, 9)),
                    dt(2026, 7, 11, 12),
                ),
            ])
            .await
            .unwrap();

        let enriched = apply_event_context(
            trade_date,
            vec![candidate("300001.SZ")],
            Some(&brief_from_sources(
                trade_date,
                &[early.clone(), late.clone()],
                vec![BriefEntity {
                    entity_id: "industry:Semiconductor".to_string(),
                    display_name: "Semiconductor".to_string(),
                }],
            )),
            &event_repo,
            &market_repo,
        )
        .await
        .unwrap();

        let candidate = &enriched[0];
        assert!(candidate
            .facts
            .iter()
            .any(|statement| statement.statement.contains("tightens early")));
        assert!(!candidate
            .facts
            .iter()
            .any(|statement| statement.statement.contains("tightens late")));
        assert!(candidate
            .unknowns
            .iter()
            .any(|statement| statement.statement.contains("direct stock mapping")));
        assert_eq!(candidate.event_adjustment, 0.0);
        assert_eq!(
            candidate.final_score,
            candidate.base_score + candidate.risk_adjustment
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn beneficiary_lists_do_not_create_event_context_matches(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let trade_date = date(2026, 7, 11);
        let event_repo = EventRepository::new(pool.clone());
        let market_repo = MarketRepository::new(pool);
        let evidence_id = Uuid::new_v4();
        let available_at = dt(2026, 7, 11, 10);
        let evidence = evidence_row(
            evidence_id,
            "beneficiary-only",
            "Beneficiary basket event",
            trade_date,
            available_at,
        );
        event_repo.insert_evidence(&evidence).await.unwrap();
        event_repo
            .save_extraction(&extraction_row(
                evidence_id,
                vec![ExtractedEntity {
                    text: "Beneficiary Holdings".to_string(),
                    entity_type: "company".to_string(),
                    role: "beneficiary".to_string(),
                    stock_code: Some("000001.SZ".to_string()),
                }],
                available_at,
            ))
            .await
            .unwrap();
        event_repo
            .save_claim_graph(&claim_graph_row(
                evidence_id,
                "Peer beneficiary basket 600519.SH 000001.SZ",
                available_at,
            ))
            .await
            .unwrap();

        let enriched = apply_event_context(
            trade_date,
            vec![candidate("000001.SZ")],
            Some(&brief_from_sources(
                trade_date,
                std::slice::from_ref(&evidence),
                Vec::new(),
            )),
            &event_repo,
            &market_repo,
        )
        .await
        .unwrap();

        let candidate = &enriched[0];
        assert!(candidate.facts.is_empty());
        assert!(candidate.calculations.is_empty());
        assert!(candidate.inferences.is_empty());
        assert!(candidate.unknowns.is_empty());
        assert_eq!(candidate.event_adjustment, 0.0);
        assert_eq!(
            candidate.final_score,
            candidate.base_score + candidate.risk_adjustment
        );
        Ok(())
    }

    #[test]
    fn event_adapter_tests_reference_expected_statement_kinds() {
        let statements = vec![
            SupportStatement::new(
                SupportStatementKind::EventFact,
                "fact",
                vec!["evidence:1".to_string()],
            ),
            SupportStatement::new(SupportStatementKind::OtherCalculation, "calc", Vec::new()),
            SupportStatement::new(SupportStatementKind::ImpactHypothesis, "infer", Vec::new()),
            SupportStatement::missing_status("unknown"),
        ];

        assert_eq!(
            statements[0].bucket(),
            crate::analysis::decision_support::StatementBucket::Fact
        );
        assert_eq!(
            statements[1].bucket(),
            crate::analysis::decision_support::StatementBucket::Calculation
        );
        assert_eq!(
            statements[2].bucket(),
            crate::analysis::decision_support::StatementBucket::Inference
        );
        assert_eq!(
            statements[3].bucket(),
            crate::analysis::decision_support::StatementBucket::Unknown
        );
    }
}
