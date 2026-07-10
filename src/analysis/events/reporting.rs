use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::contracts::{
    BriefEntity, BriefFact, BriefRevision, BriefSource, BriefUnconfirmed, DailyEventBrief,
};
use crate::error::{AppError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BriefEvidenceRecord {
    pub evidence_id: Uuid,
    pub source_id: String,
    pub source_item_id: String,
    pub published_at: Option<DateTime<Utc>>,
    pub available_at: DateTime<Utc>,
    pub title: String,
    pub supersedes_evidence_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BriefClaimRecord {
    pub claim_id: Uuid,
    pub claim_type: String,
    pub claim_text: String,
    pub review_status: String,
    pub evidence_ids: Vec<Uuid>,
    pub previous_fact_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BriefEntityRecord {
    pub entity_id: String,
    pub display_name: String,
}

pub(crate) fn build_daily_event_brief(
    trade_date: NaiveDate,
    evidence: Vec<BriefEvidenceRecord>,
    claims: Vec<BriefClaimRecord>,
    entities: Vec<BriefEntityRecord>,
) -> Result<DailyEventBrief> {
    let mut evidence_by_id = std::collections::BTreeMap::new();
    let mut evidence_order = Vec::new();
    for record in evidence {
        evidence_order.push(record.evidence_id);
        evidence_by_id.insert(record.evidence_id, record);
    }

    let mut referenced_evidence_ids = Vec::new();
    let mut new_facts = Vec::new();
    let mut revisions = Vec::new();
    let mut unconfirmed = Vec::new();

    for claim in claims {
        let mapped_evidence_ids = map_claim_evidence_ids(&claim, &evidence_by_id)?;
        referenced_evidence_ids.extend(mapped_evidence_ids.iter().copied());

        if claim.review_status == "published" && claim.claim_type == "fact" {
            if let Some(previous_fact_id) = claim.previous_fact_id {
                revisions.push(BriefRevision {
                    revision_id: claim.claim_id,
                    previous_fact_id,
                    summary: claim.claim_text,
                    evidence_ids: mapped_evidence_ids,
                });
            } else {
                new_facts.push(BriefFact {
                    fact_id: claim.claim_id,
                    summary: claim.claim_text,
                    evidence_ids: mapped_evidence_ids,
                });
            }
        } else {
            unconfirmed.push(BriefUnconfirmed {
                item_id: claim.claim_id,
                summary: claim.claim_text,
                evidence_ids: mapped_evidence_ids,
            });
        }
    }

    let mut referenced_unique = std::collections::BTreeSet::new();
    let sources = evidence_order
        .into_iter()
        .filter(|evidence_id| referenced_unique.insert(*evidence_id))
        .filter_map(|evidence_id| {
            referenced_evidence_ids
                .contains(&evidence_id)
                .then(|| evidence_by_id.get(&evidence_id))
                .flatten()
        })
        .map(|record| BriefSource {
            evidence_id: record.evidence_id,
            source_id: record.source_id.clone(),
            source_item_id: record.source_item_id.clone(),
            published_at: record.published_at,
            available_at: record.available_at,
            title: record.title.clone(),
        })
        .collect::<Vec<_>>();

    let mut seen_entities = std::collections::BTreeSet::new();
    let direct_entities = entities
        .into_iter()
        .filter(|entity| {
            seen_entities.insert((entity.entity_id.clone(), entity.display_name.clone()))
        })
        .map(|entity| BriefEntity {
            entity_id: entity.entity_id,
            display_name: entity.display_name,
        })
        .collect::<Vec<_>>();

    let input_fingerprint = fingerprint_json(&BriefFingerprintInput {
        trade_date,
        new_facts: &new_facts,
        revisions: &revisions,
        unconfirmed: &unconfirmed,
        direct_entities: &direct_entities,
        sources: &sources,
    })?;

    Ok(DailyEventBrief {
        trade_date,
        new_facts,
        revisions,
        unconfirmed,
        direct_entities,
        sources,
        input_fingerprint,
    })
}

pub(crate) fn render_daily_event_brief(brief: &DailyEventBrief) -> Result<String> {
    let source_labels = source_labels_by_evidence_id(&brief.sources);
    let mut sections = Vec::new();
    sections.push(render_fact_section(
        "今日新增事实",
        brief
            .new_facts
            .iter()
            .map(|fact| {
                let labels = labels_for_evidence_ids(
                    &source_labels,
                    &fact.evidence_ids,
                    format!("brief fact {}", fact.fact_id),
                )?;
                Ok(format!("{}. {}[来源: {}]", 0, fact.summary, labels))
            })
            .collect::<Result<Vec<_>>>()?,
    ));
    sections.push(render_fact_section(
        "今日修订",
        brief
            .revisions
            .iter()
            .map(|revision| {
                let labels = labels_for_evidence_ids(
                    &source_labels,
                    &revision.evidence_ids,
                    format!("brief revision {}", revision.revision_id),
                )?;
                Ok(format!("{}. {}[来源: {}]", 0, revision.summary, labels))
            })
            .collect::<Result<Vec<_>>>()?,
    ));
    sections.push(render_fact_section(
        "未确认内容",
        brief
            .unconfirmed
            .iter()
            .map(|item| {
                let labels = labels_for_evidence_ids(
                    &source_labels,
                    &item.evidence_ids,
                    format!("brief unconfirmed item {}", item.item_id),
                )?;
                Ok(format!("{}. {}[来源: {}]", 0, item.summary, labels))
            })
            .collect::<Result<Vec<_>>>()?,
    ));
    sections.push(render_fact_section(
        "直接涉及公司与行业",
        brief
            .direct_entities
            .iter()
            .map(|entity| Ok(format!("{}. {}", 0, entity.display_name)))
            .collect::<Result<Vec<_>>>()?,
    ));
    sections.push(render_sources_section(&brief.sources));

    Ok(sections.join("\n\n"))
}

pub(crate) fn daily_event_brief_input_fingerprint(brief: &DailyEventBrief) -> Result<String> {
    fingerprint_json(brief)
}

fn map_claim_evidence_ids(
    claim: &BriefClaimRecord,
    evidence_by_id: &std::collections::BTreeMap<Uuid, BriefEvidenceRecord>,
) -> Result<Vec<Uuid>> {
    if claim.evidence_ids.is_empty() {
        return Err(AppError::Internal(
            "published fact must reference at least one source evidence".to_string(),
        ));
    }

    claim.evidence_ids
        .iter()
        .map(|evidence_id| {
            if evidence_by_id.contains_key(evidence_id) {
                return Ok(*evidence_id);
            }

            evidence_by_id
                .values()
                .find(|record| record.supersedes_evidence_id == Some(*evidence_id))
                .map(|record| record.evidence_id)
                .ok_or_else(|| {
                    AppError::Internal(format!(
                        "published fact must reference at least one source evidence; missing {evidence_id}"
                    ))
                })
        })
        .collect()
}

fn source_labels_by_evidence_id(
    sources: &[BriefSource],
) -> std::collections::BTreeMap<Uuid, String> {
    let mut labels = std::collections::BTreeMap::new();
    for (index, source) in sources.iter().enumerate() {
        labels.insert(source.evidence_id, format!("S{}", index + 1));
    }
    labels
}

fn labels_for_evidence_ids(
    source_labels: &std::collections::BTreeMap<Uuid, String>,
    evidence_ids: &[Uuid],
    label: String,
) -> Result<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut labels = Vec::new();
    for evidence_id in evidence_ids {
        let source_label = source_labels.get(evidence_id).cloned().ok_or_else(|| {
            AppError::Internal(format!(
                "{label} references unknown source evidence {evidence_id}"
            ))
        })?;
        if seen.insert(source_label.clone()) {
            labels.push(source_label);
        }
    }

    if labels.is_empty() {
        return Err(AppError::Internal(format!(
            "{label} must reference at least one source label"
        )));
    }

    Ok(labels.join(","))
}

fn render_fact_section(title: &str, lines: Vec<String>) -> String {
    let mut rendered = String::from(title);
    if lines.is_empty() {
        return rendered;
    }

    for (index, line) in lines.into_iter().enumerate() {
        let numbered = line.replacen("0.", &format!("{}.", index + 1), 1);
        rendered.push('\n');
        rendered.push_str(&numbered);
    }
    rendered
}

fn render_sources_section(sources: &[BriefSource]) -> String {
    let mut rendered = String::from("来源");
    if sources.is_empty() {
        return rendered;
    }

    for (index, source) in sources.iter().enumerate() {
        rendered.push('\n');
        rendered.push_str(&format!(
            "S{}. {} | {} / {} | {}",
            index + 1,
            source.title,
            source.source_id,
            source.source_item_id,
            source
                .published_at
                .unwrap_or(source.available_at)
                .to_rfc3339()
        ));
    }
    rendered
}

fn fingerprint_json<T: Serialize>(value: &T) -> Result<String> {
    use sha2::{Digest, Sha256};

    let serialized = serde_json::to_vec(value).map_err(AppError::Json)?;
    let mut hasher = Sha256::new();
    hasher.update(serialized);
    Ok(format!("{:x}", hasher.finalize()))
}

#[derive(Serialize)]
struct BriefFingerprintInput<'a> {
    trade_date: NaiveDate,
    new_facts: &'a [BriefFact],
    revisions: &'a [BriefRevision],
    unconfirmed: &'a [BriefUnconfirmed],
    direct_entities: &'a [BriefEntity],
    sources: &'a [BriefSource],
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn renders_golden_chinese_sections_with_evidence_backed_facts_only() {
        let evidence_one = Uuid::from_u128(1);
        let evidence_two = Uuid::from_u128(2);
        let brief = DailyEventBrief {
            trade_date: date(2026, 7, 10),
            new_facts: vec![BriefFact {
                fact_id: Uuid::from_u128(11),
                summary: "交易所公告确认公司债券临时停牌。".to_string(),
                evidence_ids: vec![evidence_one],
            }],
            revisions: vec![BriefRevision {
                revision_id: Uuid::from_u128(12),
                previous_fact_id: Uuid::from_u128(10),
                summary: "公司将回购规模修订为不超过10亿元。".to_string(),
                evidence_ids: vec![evidence_two],
            }],
            unconfirmed: vec![BriefUnconfirmed {
                item_id: Uuid::from_u128(13),
                summary: "市场传闻称供应链订单仍在洽谈。".to_string(),
                evidence_ids: vec![evidence_two],
            }],
            direct_entities: vec![
                BriefEntity {
                    entity_id: "600000.SH".to_string(),
                    display_name: "浦发银行".to_string(),
                },
                BriefEntity {
                    entity_id: "industry:bank".to_string(),
                    display_name: "银行".to_string(),
                },
            ],
            sources: vec![
                BriefSource {
                    evidence_id: evidence_one,
                    source_id: "official:market_event".to_string(),
                    source_item_id: "notice-001".to_string(),
                    published_at: Some(dt(2026, 7, 10, 8, 15, 0)),
                    available_at: dt(2026, 7, 10, 8, 15, 0),
                    title: "交易所临时停牌公告".to_string(),
                },
                BriefSource {
                    evidence_id: evidence_two,
                    source_id: "official:market_event".to_string(),
                    source_item_id: "notice-002".to_string(),
                    published_at: Some(dt(2026, 7, 10, 9, 30, 0)),
                    available_at: dt(2026, 7, 10, 9, 30, 0),
                    title: "公司回购方案修订公告".to_string(),
                },
            ],
            input_fingerprint: "fingerprint-v1".to_string(),
        };

        let rendered = render_daily_event_brief(&brief).unwrap();

        assert_eq!(
            rendered,
            concat!(
                "今日新增事实\n",
                "1. 交易所公告确认公司债券临时停牌。[来源: S1]\n",
                "\n",
                "今日修订\n",
                "1. 公司将回购规模修订为不超过10亿元。[来源: S2]\n",
                "\n",
                "未确认内容\n",
                "1. 市场传闻称供应链订单仍在洽谈。[来源: S2]\n",
                "\n",
                "直接涉及公司与行业\n",
                "1. 浦发银行\n",
                "2. 银行\n",
                "\n",
                "来源\n",
                "S1. 交易所临时停牌公告 | official:market_event / notice-001 | 2026-07-10T08:15:00+00:00\n",
                "S2. 公司回购方案修订公告 | official:market_event / notice-002 | 2026-07-10T09:30:00+00:00"
            )
        );
    }

    #[test]
    fn rejects_published_fact_without_any_evidence_backed_source_reference() {
        let error = build_daily_event_brief(
            date(2026, 7, 10),
            Vec::new(),
            vec![BriefClaimRecord {
                claim_id: Uuid::from_u128(21),
                claim_type: "fact".to_string(),
                claim_text: "公司披露中期分红安排。".to_string(),
                review_status: "published".to_string(),
                evidence_ids: Vec::new(),
                previous_fact_id: None,
            }],
            Vec::new(),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("published fact must reference at least one source evidence"),
            "{error}"
        );
    }

    #[test]
    fn rejects_rendering_when_fact_references_a_missing_source_label() {
        let brief = DailyEventBrief {
            trade_date: date(2026, 7, 10),
            new_facts: vec![BriefFact {
                fact_id: Uuid::from_u128(31),
                summary: "监管公告确认交易安排。".to_string(),
                evidence_ids: vec![Uuid::from_u128(999)],
            }],
            revisions: Vec::new(),
            unconfirmed: Vec::new(),
            direct_entities: Vec::new(),
            sources: Vec::new(),
            input_fingerprint: "fingerprint-v1".to_string(),
        };

        let error = render_daily_event_brief(&brief).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("brief fact 00000000-0000-0000-0000-00000000001f references unknown source evidence"),
            "{error}"
        );
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .unwrap()
    }
}
