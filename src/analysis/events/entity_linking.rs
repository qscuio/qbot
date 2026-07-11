use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::extraction::ExtractedEntity;
use crate::data::types::StockInfo;

pub const ENTITY_MATCH_METHOD_SECURITY_CODE: &str = "security_code";
pub const ENTITY_MATCH_METHOD_EXACT_LEGAL_NAME: &str = "exact_legal_name";
pub const ENTITY_MATCH_METHOD_REVIEWED_ALIAS: &str = "reviewed_alias";
pub const ENTITY_MATCH_METHOD_EXACT_OFFICIAL_INDUSTRY_NAME: &str = "exact_official_industry_name";
pub const ENTITY_MATCH_METHOD_AMBIGUOUS_ALIAS: &str = "ambiguous_alias";
pub const ENTITY_MATCH_METHOD_UNRESOLVED: &str = "unresolved";

pub const ENTITY_REVIEW_STATUS_LINKED: &str = "linked";
pub const ENTITY_REVIEW_STATUS_REVIEW_REQUIRED: &str = "review_required";
pub const ENTITY_REVIEW_STATUS_UNMAPPED: &str = "unmapped";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityLink {
    pub entity_link_id: Uuid,
    pub evidence_id: Uuid,
    pub raw_name: String,
    pub canonical_type: String,
    pub canonical_id: Option<String>,
    pub role: String,
    pub match_method: String,
    pub confidence: f64,
    pub review_status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewedAlias {
    pub alias: String,
    pub security_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OfficialIndustry {
    pub canonical_id: String,
    pub official_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct EntityCatalog {
    securities_by_code: BTreeMap<String, StockInfo>,
    securities_by_legal_name: BTreeMap<String, StockInfo>,
    aliases_by_name: BTreeMap<String, Vec<String>>,
    industries_by_name: BTreeMap<String, OfficialIndustry>,
}

impl EntityCatalog {
    pub fn new(
        stocks: impl IntoIterator<Item = StockInfo>,
        aliases: impl IntoIterator<Item = ReviewedAlias>,
        industries: impl IntoIterator<Item = OfficialIndustry>,
    ) -> Self {
        let mut securities_by_code = BTreeMap::new();
        let mut securities_by_legal_name = BTreeMap::new();

        for stock in stocks {
            if stock.code.is_empty() || stock.name.is_empty() {
                continue;
            }

            securities_by_legal_name.insert(stock.name.clone(), stock.clone());
            securities_by_code.insert(stock.code.clone(), stock);
        }

        let mut aliases_by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for alias in aliases {
            if alias.alias.is_empty() || !securities_by_code.contains_key(&alias.security_code) {
                continue;
            }

            aliases_by_name
                .entry(alias.alias)
                .or_default()
                .push(alias.security_code);
        }

        for codes in aliases_by_name.values_mut() {
            codes.sort();
            codes.dedup();
        }

        let mut industries_by_name = BTreeMap::new();
        for industry in industries {
            if industry.official_name.is_empty() {
                continue;
            }

            industries_by_name.insert(industry.official_name.clone(), industry);
        }

        Self {
            securities_by_code,
            securities_by_legal_name,
            aliases_by_name,
            industries_by_name,
        }
    }
}

pub fn link_entities(
    evidence_id: Uuid,
    entities: &[ExtractedEntity],
    catalog: &EntityCatalog,
) -> Vec<EntityLink> {
    link_entities_at(evidence_id, entities, catalog, Utc::now())
}

pub fn link_entities_at(
    evidence_id: Uuid,
    entities: &[ExtractedEntity],
    catalog: &EntityCatalog,
    created_at: DateTime<Utc>,
) -> Vec<EntityLink> {
    entities
        .iter()
        .map(|entity| {
            if let Some(stock_code) = entity.stock_code.as_deref() {
                if let Some(stock) = catalog.securities_by_code.get(stock_code) {
                    return company_link(
                        evidence_id,
                        entity,
                        stock,
                        ENTITY_MATCH_METHOD_SECURITY_CODE,
                        1.0,
                        created_at,
                    );
                }
            }

            if let Some(stock) = catalog.securities_by_legal_name.get(&entity.text) {
                return company_link(
                    evidence_id,
                    entity,
                    stock,
                    ENTITY_MATCH_METHOD_EXACT_LEGAL_NAME,
                    0.98,
                    created_at,
                );
            }

            if let Some(codes) = catalog.aliases_by_name.get(&entity.text) {
                if codes.len() == 1 {
                    if let Some(stock) = catalog.securities_by_code.get(&codes[0]) {
                        return company_link(
                            evidence_id,
                            entity,
                            stock,
                            ENTITY_MATCH_METHOD_REVIEWED_ALIAS,
                            0.95,
                            created_at,
                        );
                    }
                }

                if codes.len() > 1 {
                    return EntityLink {
                        entity_link_id: Uuid::new_v4(),
                        evidence_id,
                        raw_name: entity.text.clone(),
                        canonical_type: "company".to_string(),
                        canonical_id: None,
                        role: entity.role.clone(),
                        match_method: ENTITY_MATCH_METHOD_AMBIGUOUS_ALIAS.to_string(),
                        confidence: 0.5,
                        review_status: ENTITY_REVIEW_STATUS_REVIEW_REQUIRED.to_string(),
                        created_at,
                    };
                }
            }

            if is_official_industry_entity_type(&entity.entity_type) {
                if let Some(industry) = catalog.industries_by_name.get(&entity.text) {
                    return EntityLink {
                        entity_link_id: Uuid::new_v4(),
                        evidence_id,
                        raw_name: entity.text.clone(),
                        canonical_type: "industry".to_string(),
                        canonical_id: Some(industry.canonical_id.clone()),
                        role: entity.role.clone(),
                        match_method: ENTITY_MATCH_METHOD_EXACT_OFFICIAL_INDUSTRY_NAME.to_string(),
                        confidence: 0.92,
                        review_status: ENTITY_REVIEW_STATUS_LINKED.to_string(),
                        created_at,
                    };
                }
            }

            EntityLink {
                entity_link_id: Uuid::new_v4(),
                evidence_id,
                raw_name: entity.text.clone(),
                canonical_type: entity.entity_type.clone(),
                canonical_id: None,
                role: entity.role.clone(),
                match_method: ENTITY_MATCH_METHOD_UNRESOLVED.to_string(),
                confidence: 0.0,
                review_status: ENTITY_REVIEW_STATUS_UNMAPPED.to_string(),
                created_at,
            }
        })
        .collect()
}

fn company_link(
    evidence_id: Uuid,
    entity: &ExtractedEntity,
    stock: &StockInfo,
    match_method: &str,
    confidence: f64,
    created_at: DateTime<Utc>,
) -> EntityLink {
    EntityLink {
        entity_link_id: Uuid::new_v4(),
        evidence_id,
        raw_name: entity.text.clone(),
        canonical_type: "company".to_string(),
        canonical_id: Some(stock.code.clone()),
        role: entity.role.clone(),
        match_method: match_method.to_string(),
        confidence,
        review_status: ENTITY_REVIEW_STATUS_LINKED.to_string(),
        created_at,
    }
}

fn is_official_industry_entity_type(entity_type: &str) -> bool {
    matches!(entity_type, "industry" | "sector")
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn exact_stock_code_links_to_company_with_highest_priority() {
        let evidence_id = evidence_id();
        let created_at = created_at();
        let links = link_entities_at(
            evidence_id,
            &[entity("平安", "organization", "issuer", Some("600519.SH"))],
            &catalog(),
            created_at,
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].evidence_id, evidence_id);
        assert_eq!(links[0].raw_name, "平安");
        assert_eq!(links[0].canonical_type, "company");
        assert_eq!(links[0].canonical_id.as_deref(), Some("600519.SH"));
        assert_eq!(links[0].role, "issuer");
        assert_eq!(links[0].match_method, ENTITY_MATCH_METHOD_SECURITY_CODE);
        assert_eq!(links[0].review_status, ENTITY_REVIEW_STATUS_LINKED);
        assert_eq!(links[0].created_at, created_at);
    }

    #[test]
    fn exact_company_name_links_when_no_security_code_is_present() {
        let links = link_entities_at(
            evidence_id(),
            &[entity("平安银行", "organization", "issuer", None)],
            &catalog(),
            created_at(),
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].canonical_type, "company");
        assert_eq!(links[0].canonical_id.as_deref(), Some("000001.SZ"));
        assert_eq!(links[0].match_method, ENTITY_MATCH_METHOD_EXACT_LEGAL_NAME);
        assert_eq!(links[0].review_status, ENTITY_REVIEW_STATUS_LINKED);
    }

    #[test]
    fn reviewed_alias_links_to_company() {
        let links = link_entities_at(
            evidence_id(),
            &[entity("茅台", "organization", "issuer", None)],
            &catalog(),
            created_at(),
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].canonical_type, "company");
        assert_eq!(links[0].canonical_id.as_deref(), Some("600519.SH"));
        assert_eq!(links[0].match_method, ENTITY_MATCH_METHOD_REVIEWED_ALIAS);
        assert_eq!(links[0].review_status, ENTITY_REVIEW_STATUS_LINKED);
    }

    #[test]
    fn ambiguous_short_name_requires_review_and_stays_unresolved() {
        let links = link_entities_at(
            evidence_id(),
            &[entity("平安", "organization", "issuer", None)],
            &catalog(),
            created_at(),
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].canonical_type, "company");
        assert_eq!(links[0].canonical_id, None);
        assert_eq!(links[0].match_method, ENTITY_MATCH_METHOD_AMBIGUOUS_ALIAS);
        assert_eq!(links[0].review_status, ENTITY_REVIEW_STATUS_REVIEW_REQUIRED);
    }

    #[test]
    fn unknown_organization_remains_unmapped() {
        let links = link_entities_at(
            evidence_id(),
            &[entity("火星控股", "organization", "issuer", None)],
            &catalog(),
            created_at(),
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].canonical_type, "organization");
        assert_eq!(links[0].canonical_id, None);
        assert_eq!(links[0].match_method, ENTITY_MATCH_METHOD_UNRESOLVED);
        assert_eq!(links[0].review_status, ENTITY_REVIEW_STATUS_UNMAPPED);
    }

    #[test]
    fn exact_official_industry_name_links_after_company_and_alias_misses() {
        let links = link_entities_at(
            evidence_id(),
            &[entity("半导体", "industry", "affected_sector", None)],
            &catalog(),
            created_at(),
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].canonical_type, "industry");
        assert_eq!(
            links[0].canonical_id.as_deref(),
            Some("industry:semiconductors")
        );
        assert_eq!(
            links[0].match_method,
            ENTITY_MATCH_METHOD_EXACT_OFFICIAL_INDUSTRY_NAME
        );
        assert_eq!(links[0].review_status, ENTITY_REVIEW_STATUS_LINKED);
    }

    #[test]
    fn official_industry_name_does_not_remap_organization_entities() {
        let links = link_entities_at(
            evidence_id(),
            &[entity("半导体", "organization", "issuer", None)],
            &catalog(),
            created_at(),
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].canonical_type, "organization");
        assert_eq!(links[0].canonical_id, None);
        assert_eq!(links[0].match_method, ENTITY_MATCH_METHOD_UNRESOLVED);
        assert_eq!(links[0].review_status, ENTITY_REVIEW_STATUS_UNMAPPED);
    }

    fn catalog() -> EntityCatalog {
        EntityCatalog::new(
            vec![
                StockInfo {
                    code: "000001.SZ".to_string(),
                    name: "平安银行".to_string(),
                    market: "SZ".to_string(),
                    industry: Some("银行".to_string()),
                },
                StockInfo {
                    code: "600519.SH".to_string(),
                    name: "贵州茅台".to_string(),
                    market: "SH".to_string(),
                    industry: Some("白酒".to_string()),
                },
                StockInfo {
                    code: "601318.SH".to_string(),
                    name: "中国平安".to_string(),
                    market: "SH".to_string(),
                    industry: Some("保险".to_string()),
                },
            ],
            vec![
                ReviewedAlias {
                    alias: "茅台".to_string(),
                    security_code: "600519.SH".to_string(),
                },
                ReviewedAlias {
                    alias: "平安".to_string(),
                    security_code: "000001.SZ".to_string(),
                },
                ReviewedAlias {
                    alias: "平安".to_string(),
                    security_code: "601318.SH".to_string(),
                },
            ],
            vec![OfficialIndustry {
                canonical_id: "industry:semiconductors".to_string(),
                official_name: "半导体".to_string(),
            }],
        )
    }

    fn entity(
        text: &str,
        entity_type: &str,
        role: &str,
        stock_code: Option<&str>,
    ) -> ExtractedEntity {
        ExtractedEntity {
            text: text.to_string(),
            entity_type: entity_type.to_string(),
            role: role.to_string(),
            stock_code: stock_code.map(str::to_string),
        }
    }

    fn evidence_id() -> Uuid {
        Uuid::parse_str("d88f66cb-5a10-4d3b-8126-7f918cf22145").unwrap()
    }

    fn created_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap()
    }
}
