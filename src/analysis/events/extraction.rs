use std::collections::BTreeSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{AppError, Result};

pub(crate) const EVENT_EXTRACTION_SCHEMA_VERSION: &str = "event_extraction_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventExtractionV1 {
    pub event_type: String,
    pub event_subtype: Option<String>,
    pub claims: Vec<ExtractedClaim>,
    pub entities: Vec<ExtractedEntity>,
    pub amounts: Vec<ExtractedAmount>,
    pub dates: Vec<ExtractedDate>,
    pub uncertainties: Vec<String>,
    pub missing_information: Vec<String>,
}

impl EventExtractionV1 {
    pub fn validate(&self, context: &ExtractionValidationContext<'_>) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        let classified_as_rumor_or_opinion = self
            .claims
            .iter()
            .filter(|claim| claim.claim_type.is_rumor_or_opinion())
            .map(|claim| normalize_claim_text(&claim.text))
            .collect::<BTreeSet<_>>();

        for (claim_index, claim) in self.claims.iter().enumerate() {
            if claim.claim_type == ClaimType::Fact && claim.evidence_ids.is_empty() {
                issues.push(ValidationIssue::new(
                    format!("claims[{claim_index}].evidence_ids"),
                    "fact claims must reference at least one evidence id",
                ));
            }

            if !(0.0..=1.0).contains(&claim.confidence) {
                issues.push(ValidationIssue::new(
                    format!("claims[{claim_index}].confidence"),
                    "claim confidence must be within [0,1]",
                ));
            }

            for (evidence_index, evidence_id) in claim.evidence_ids.iter().enumerate() {
                if !context.contains_evidence(*evidence_id) {
                    issues.push(ValidationIssue::new(
                        format!("claims[{claim_index}].evidence_ids[{evidence_index}]"),
                        format!(
                            "claim references evidence id {evidence_id} that is not present in the extraction input"
                        ),
                    ));
                }
            }

            if claim.claim_type == ClaimType::Fact
                && classified_as_rumor_or_opinion.contains(&normalize_claim_text(&claim.text))
            {
                issues.push(ValidationIssue::new(
                    format!("claims[{claim_index}].claim_type"),
                    "rumor or journalist interpretation claims cannot be promoted to facts",
                ));
            }
        }

        for (entity_index, entity) in self.entities.iter().enumerate() {
            if let Some(stock_code) = entity.stock_code.as_deref() {
                if context.stock_code_lookup.resolve(stock_code).is_none() {
                    issues.push(ValidationIssue::new(
                        format!("entities[{entity_index}].stock_code"),
                        format!(
                            "stock code `{stock_code}` does not map to a known stock_info entry"
                        ),
                    ));
                }

                if !context.source_contains(stock_code) {
                    issues.push(ValidationIssue::new(
                        format!("entities[{entity_index}].stock_code"),
                        format!(
                            "stock code `{stock_code}` does not appear in the extraction input content"
                        ),
                    ));
                }
            }
        }

        for (amount_index, amount) in self.amounts.iter().enumerate() {
            if !context.source_contains(&amount.value) {
                issues.push(ValidationIssue::new(
                    format!("amounts[{amount_index}].value"),
                    format!(
                        "amount value `{}` does not appear in the extraction input content",
                        amount.value
                    ),
                ));
            }
        }

        for (date_index, date) in self.dates.iter().enumerate() {
            if !context.source_contains(&date.value) {
                issues.push(ValidationIssue::new(
                    format!("dates[{date_index}].value"),
                    format!(
                        "date value `{}` does not appear in the extraction input content",
                        date.value
                    ),
                ));
            }
        }

        issues.sort();
        issues
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExtractedClaim {
    pub claim_type: ClaimType,
    pub text: String,
    pub evidence_ids: Vec<Uuid>,
    pub confidence: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaimType {
    Fact,
    DirectQuote,
    ThirdPartyClaim,
    JournalistInterpretation,
    Rumor,
    Unknown,
}

impl ClaimType {
    fn is_rumor_or_opinion(self) -> bool {
        matches!(self, Self::Rumor | Self::JournalistInterpretation)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExtractedEntity {
    pub text: String,
    pub entity_type: String,
    pub role: String,
    pub stock_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExtractedAmount {
    pub raw_text: String,
    pub value: String,
    pub unit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExtractedDate {
    pub raw_text: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ExtractionEvidence {
    pub evidence_id: Uuid,
    pub source_text: String,
}

impl ExtractionEvidence {
    pub fn new(evidence_id: Uuid, source_text: String) -> Self {
        Self {
            evidence_id,
            source_text,
        }
    }
}

pub(crate) struct ExtractionValidationContext<'a> {
    evidence_ids: BTreeSet<Uuid>,
    source_texts: Vec<&'a str>,
    stock_code_lookup: &'a dyn StockCodeLookup,
}

impl<'a> ExtractionValidationContext<'a> {
    pub fn new(
        evidence: &'a [ExtractionEvidence],
        stock_code_lookup: &'a dyn StockCodeLookup,
    ) -> Self {
        Self {
            evidence_ids: evidence.iter().map(|item| item.evidence_id).collect(),
            source_texts: evidence
                .iter()
                .map(|item| item.source_text.as_str())
                .collect(),
            stock_code_lookup,
        }
    }

    fn contains_evidence(&self, evidence_id: Uuid) -> bool {
        self.evidence_ids.contains(&evidence_id)
    }

    fn source_contains(&self, needle: &str) -> bool {
        self.source_texts.iter().any(|text| text.contains(needle))
    }
}

pub(crate) trait StockCodeLookup: Send + Sync {
    fn resolve(&self, raw_code: &str) -> Option<String>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StockCodeDirectory {
    known_codes: BTreeSet<String>,
}

impl StockCodeDirectory {
    pub fn from_known_codes<I, S>(codes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut known_codes = BTreeSet::new();

        for code in codes {
            let exact_code = code.as_ref();
            if exact_code.is_empty() {
                continue;
            }

            known_codes.insert(exact_code.to_string());
        }

        Self { known_codes }
    }
}

impl StockCodeLookup for StockCodeDirectory {
    fn resolve(&self, raw_code: &str) -> Option<String> {
        self.known_codes
            .contains(raw_code)
            .then(|| raw_code.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventExtractionInput {
    pub evidence_id: Uuid,
    pub input_fingerprint: String,
    pub evidence: Vec<ExtractionEvidence>,
}

impl EventExtractionInput {
    pub fn validation_context<'a>(
        &'a self,
        stock_code_lookup: &'a dyn StockCodeLookup,
    ) -> ExtractionValidationContext<'a> {
        ExtractionValidationContext::new(&self.evidence, stock_code_lookup)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EventExtractionMetadata {
    pub schema_version: String,
    pub prompt_version: String,
    pub model_name: String,
    pub model_parameters: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EventExtractionOutput {
    pub extraction: EventExtractionV1,
    pub metadata: EventExtractionMetadata,
}

#[async_trait]
pub(crate) trait EventExtractor: Send + Sync {
    async fn extract(&self, input: EventExtractionInput) -> Result<EventExtractionOutput>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ValidationIssue {
    pub path: String,
    pub message: String,
}

impl ValidationIssue {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

pub(crate) fn validation_error_message(issues: &[ValidationIssue]) -> String {
    let details = issues
        .iter()
        .map(|issue| format!("{}: {}", issue.path, issue.message))
        .collect::<Vec<_>>()
        .join("; ");
    format!("event extraction validation failed: {details}")
}

pub(crate) fn validation_error(issues: &[ValidationIssue]) -> AppError {
    AppError::DataProvider(validation_error_message(issues))
}

fn normalize_claim_text(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use serde_json::json;

    use super::*;

    #[test]
    fn fact_claim_requires_evidence_ids() {
        let extraction = base_extraction(vec![ExtractedClaim {
            claim_type: ClaimType::Fact,
            text: "The issuer raised guidance.".to_string(),
            evidence_ids: Vec::new(),
            confidence: 0.9,
        }]);

        let errors = extraction.validate(&validation_context());

        assert_eq!(
            errors,
            vec![ValidationIssue::new(
                "claims[0].evidence_ids",
                "fact claims must reference at least one evidence id",
            )]
        );
    }

    #[test]
    fn confidence_outside_unit_interval_fails_validation() {
        let extraction = base_extraction(vec![ExtractedClaim {
            claim_type: ClaimType::ThirdPartyClaim,
            text: "A supplier expects stronger demand.".to_string(),
            evidence_ids: vec![primary_evidence_id()],
            confidence: 1.2,
        }]);

        let errors = extraction.validate(&validation_context());

        assert_eq!(
            errors,
            vec![ValidationIssue::new(
                "claims[0].confidence",
                "claim confidence must be within [0,1]",
            )]
        );
    }

    #[test]
    fn claim_evidence_ids_must_belong_to_the_extraction_input() {
        let extraction = base_extraction(vec![ExtractedClaim {
            claim_type: ClaimType::Fact,
            text: "The issuer raised guidance.".to_string(),
            evidence_ids: vec![Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()],
            confidence: 0.9,
        }]);

        let errors = extraction.validate(&validation_context());

        assert_eq!(
            errors,
            vec![ValidationIssue::new(
                "claims[0].evidence_ids[0]",
                "claim references evidence id 11111111-1111-1111-1111-111111111111 that is not present in the extraction input",
            )]
        );
    }

    #[test]
    fn date_and_amount_values_must_appear_in_source_content() {
        let extraction = EventExtractionV1 {
            amounts: vec![ExtractedAmount {
                raw_text: "CNY 2 billion".to_string(),
                value: "2000000000".to_string(),
                unit: Some("CNY".to_string()),
            }],
            dates: vec![ExtractedDate {
                raw_text: "July 10, 2026".to_string(),
                value: "2026-07-15".to_string(),
            }],
            ..base_extraction(vec![ExtractedClaim {
                claim_type: ClaimType::Fact,
                text: "The issuer raised guidance.".to_string(),
                evidence_ids: vec![primary_evidence_id()],
                confidence: 0.9,
            }])
        };

        let evidence = vec![ExtractionEvidence::new(
            primary_evidence_id(),
            "Kweichow Moutai (600519.SH) raised guidance on July 10, 2026 and expects CNY 2 billion in incremental revenue.".to_string(),
        )];
        let stock_codes = StockCodeDirectory::from_known_codes(["600519.SH", "000001.SZ"]);
        let errors =
            extraction.validate(&ExtractionValidationContext::new(&evidence, &stock_codes));

        assert_eq!(
            errors,
            vec![
                ValidationIssue::new(
                    "amounts[0].value",
                    "amount value `2000000000` does not appear in the extraction input content",
                ),
                ValidationIssue::new(
                    "dates[0].value",
                    "date value `2026-07-15` does not appear in the extraction input content",
                ),
            ]
        );
    }

    #[test]
    fn direct_stock_codes_must_map_to_known_stock_info() {
        let extraction = EventExtractionV1 {
            entities: vec![ExtractedEntity {
                text: "Unknown issuer".to_string(),
                entity_type: "issuer".to_string(),
                role: "subject".to_string(),
                stock_code: Some("123456".to_string()),
            }],
            ..base_extraction(vec![ExtractedClaim {
                claim_type: ClaimType::Fact,
                text: "The issuer raised guidance.".to_string(),
                evidence_ids: vec![primary_evidence_id()],
                confidence: 0.9,
            }])
        };

        let errors = extraction.validate(&validation_context());

        assert_eq!(
            errors,
            vec![
                ValidationIssue::new(
                    "entities[0].stock_code",
                    "stock code `123456` does not appear in the extraction input content",
                ),
                ValidationIssue::new(
                    "entities[0].stock_code",
                    "stock code `123456` does not map to a known stock_info entry",
                ),
            ]
        );
    }

    #[test]
    fn stock_codes_must_match_known_directory_exactly() {
        let extraction = EventExtractionV1 {
            entities: vec![ExtractedEntity {
                text: "Kweichow Moutai".to_string(),
                entity_type: "issuer".to_string(),
                role: "subject".to_string(),
                stock_code: Some("600519".to_string()),
            }],
            ..base_extraction(vec![ExtractedClaim {
                claim_type: ClaimType::Fact,
                text: "Kweichow Moutai raised guidance.".to_string(),
                evidence_ids: vec![primary_evidence_id()],
                confidence: 0.95,
            }])
        };

        let stock_codes = StockCodeDirectory::from_known_codes(["600519.SH"]);
        let errors = extraction.validate(&ExtractionValidationContext::new(
            &sample_evidence(),
            &stock_codes,
        ));

        assert_eq!(
            errors,
            vec![ValidationIssue::new(
                "entities[0].stock_code",
                "stock code `600519` does not map to a known stock_info entry",
            )]
        );
        assert_eq!(stock_codes.resolve("600519"), None);
        assert_eq!(
            stock_codes.resolve("600519.SH").as_deref(),
            Some("600519.SH")
        );
    }

    #[test]
    fn lowercase_stock_codes_do_not_pass_direct_validation() {
        let extraction = EventExtractionV1 {
            entities: vec![ExtractedEntity {
                text: "Kweichow Moutai".to_string(),
                entity_type: "issuer".to_string(),
                role: "subject".to_string(),
                stock_code: Some("600519.sh".to_string()),
            }],
            ..base_extraction(vec![ExtractedClaim {
                claim_type: ClaimType::Fact,
                text: "Kweichow Moutai raised guidance.".to_string(),
                evidence_ids: vec![primary_evidence_id()],
                confidence: 0.95,
            }])
        };

        let stock_codes = StockCodeDirectory::from_known_codes(["600519.SH"]);
        let errors = extraction.validate(&ExtractionValidationContext::new(
            &sample_evidence(),
            &stock_codes,
        ));

        assert_eq!(
            errors,
            vec![
                ValidationIssue::new(
                    "entities[0].stock_code",
                    "stock code `600519.sh` does not appear in the extraction input content",
                ),
                ValidationIssue::new(
                    "entities[0].stock_code",
                    "stock code `600519.sh` does not map to a known stock_info entry",
                ),
            ]
        );
    }

    #[test]
    fn whitespace_padded_stock_codes_do_not_pass_direct_validation() {
        let extraction = EventExtractionV1 {
            entities: vec![ExtractedEntity {
                text: "Kweichow Moutai".to_string(),
                entity_type: "issuer".to_string(),
                role: "subject".to_string(),
                stock_code: Some(" 600519.SH ".to_string()),
            }],
            ..base_extraction(vec![ExtractedClaim {
                claim_type: ClaimType::Fact,
                text: "Kweichow Moutai raised guidance.".to_string(),
                evidence_ids: vec![primary_evidence_id()],
                confidence: 0.95,
            }])
        };

        let stock_codes = StockCodeDirectory::from_known_codes(["600519.SH"]);
        let errors = extraction.validate(&ExtractionValidationContext::new(
            &sample_evidence(),
            &stock_codes,
        ));

        assert_eq!(
            errors,
            vec![
                ValidationIssue::new(
                    "entities[0].stock_code",
                    "stock code ` 600519.SH ` does not appear in the extraction input content",
                ),
                ValidationIssue::new(
                    "entities[0].stock_code",
                    "stock code ` 600519.SH ` does not map to a known stock_info entry",
                ),
            ]
        );
    }

    #[test]
    fn exact_known_stock_codes_must_appear_in_source_text() {
        let extraction = base_extraction(vec![ExtractedClaim {
            claim_type: ClaimType::Fact,
            text: "Kweichow Moutai raised guidance.".to_string(),
            evidence_ids: vec![primary_evidence_id()],
            confidence: 0.95,
        }]);

        let stock_codes = StockCodeDirectory::from_known_codes(["600519.SH"]);
        let evidence_without_code = vec![ExtractionEvidence::new(
            primary_evidence_id(),
            "Kweichow Moutai raised guidance on 2026-07-10 and expects CNY 2 billion in incremental revenue.".to_string(),
        )];
        let errors = extraction.validate(&ExtractionValidationContext::new(
            &evidence_without_code,
            &stock_codes,
        ));

        assert_eq!(
            errors,
            vec![ValidationIssue::new(
                "entities[0].stock_code",
                "stock code `600519.SH` does not appear in the extraction input content",
            )]
        );
    }

    #[test]
    fn exact_known_stock_codes_pass_when_the_exact_string_appears_in_source_text() {
        let extraction = base_extraction(vec![ExtractedClaim {
            claim_type: ClaimType::Fact,
            text: "Kweichow Moutai raised guidance.".to_string(),
            evidence_ids: vec![primary_evidence_id()],
            confidence: 0.95,
        }]);

        let stock_codes = StockCodeDirectory::from_known_codes(["600519.SH"]);
        let errors = extraction.validate(&ExtractionValidationContext::new(
            &sample_evidence(),
            &stock_codes,
        ));

        assert_eq!(errors, Vec::new());
    }

    #[test]
    fn rumor_and_journalist_interpretation_cannot_be_promoted_to_facts() {
        let extraction = base_extraction(vec![
            ExtractedClaim {
                claim_type: ClaimType::Rumor,
                text: "The issuer may sell a subsidiary.".to_string(),
                evidence_ids: vec![primary_evidence_id()],
                confidence: 0.4,
            },
            ExtractedClaim {
                claim_type: ClaimType::Fact,
                text: " The issuer may sell a subsidiary. ".to_string(),
                evidence_ids: vec![primary_evidence_id()],
                confidence: 0.9,
            },
        ]);

        let errors = extraction.validate(&validation_context());

        assert_eq!(
            errors,
            vec![ValidationIssue::new(
                "claims[1].claim_type",
                "rumor or journalist interpretation claims cannot be promoted to facts",
            )]
        );
    }

    #[test]
    fn unknown_json_fields_are_rejected() {
        let result = serde_json::from_value::<EventExtractionV1>(json!({
            "event_type": "earnings",
            "event_subtype": "guidance",
            "claims": [],
            "entities": [],
            "amounts": [],
            "dates": [],
            "uncertainties": [],
            "missing_information": [],
            "unexpected": true
        }));

        let error = result.unwrap_err().to_string();
        assert!(error.contains("unknown field `unexpected`"), "{error}");
    }

    #[test]
    fn fixture_round_trips() {
        let fixture = fs::read_to_string(fixture_path()).unwrap();
        let extraction = serde_json::from_str::<EventExtractionV1>(&fixture).unwrap();
        let reparsed = serde_json::to_value(&extraction).unwrap();
        let original = serde_json::from_str::<Value>(&fixture).unwrap();

        assert_eq!(reparsed, original);
        assert_eq!(extraction.validate(&validation_context()), Vec::new());
    }

    fn base_extraction(claims: Vec<ExtractedClaim>) -> EventExtractionV1 {
        EventExtractionV1 {
            event_type: "earnings".to_string(),
            event_subtype: Some("guidance".to_string()),
            claims,
            entities: vec![ExtractedEntity {
                text: "Kweichow Moutai".to_string(),
                entity_type: "issuer".to_string(),
                role: "subject".to_string(),
                stock_code: Some("600519.SH".to_string()),
            }],
            amounts: vec![ExtractedAmount {
                raw_text: "CNY 2 billion".to_string(),
                value: "CNY 2 billion".to_string(),
                unit: Some("CNY".to_string()),
            }],
            dates: vec![ExtractedDate {
                raw_text: "2026-07-10".to_string(),
                value: "2026-07-10".to_string(),
            }],
            uncertainties: vec!["Customer timing was not disclosed.".to_string()],
            missing_information: vec!["No margin guidance was provided.".to_string()],
        }
    }

    fn primary_evidence_id() -> Uuid {
        Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap()
    }

    fn sample_evidence() -> Vec<ExtractionEvidence> {
        vec![ExtractionEvidence::new(
            primary_evidence_id(),
            "Kweichow Moutai (600519.SH) raised guidance on 2026-07-10 and expects CNY 2 billion in incremental revenue.".to_string(),
        )]
    }

    fn validation_context() -> ExtractionValidationContext<'static> {
        let evidence = Box::leak(Box::new(sample_evidence()));
        let stock_codes = Box::leak(Box::new(StockCodeDirectory::from_known_codes([
            "600519.SH",
            "000001.SZ",
        ])));

        ExtractionValidationContext::new(evidence, stock_codes)
    }

    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/event_extraction_v1.json")
    }
}
