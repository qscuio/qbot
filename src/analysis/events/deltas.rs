use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventDelta {
    pub new_claim_ids: Vec<Uuid>,
    pub repeated_claim_ids: Vec<Uuid>,
    pub revised_values: Vec<RevisedValue>,
    pub removed_claim_ids: Vec<Uuid>,
    pub status_changes: Vec<StatusChange>,
    pub expectation_gap: Option<ExpectationGap>,
    pub new_uncertainties: Vec<String>,
    pub resolved_uncertainties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisedValue {
    pub canonical_claim_id: String,
    pub previous_claim_id: Uuid,
    pub current_claim_id: Uuid,
    pub previous: Option<NormalizedValue>,
    pub current: Option<NormalizedValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusChange {
    pub canonical_claim_id: String,
    pub previous_claim_id: Uuid,
    pub current_claim_id: Uuid,
    pub previous_status: Option<String>,
    pub current_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectationGap {
    pub canonical_claim_id: String,
    pub expected: Option<NormalizedValue>,
    pub observed: Option<NormalizedValue>,
    pub expected_date: Option<NaiveDate>,
    pub observed_date: Option<NaiveDate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventClusterVersionSnapshot {
    pub event_cluster_id: Uuid,
    pub cluster_version: i32,
    pub lifecycle_status: String,
    pub claims: Vec<EventClaimSnapshot>,
    pub expectation: Option<ExpectationSnapshot>,
    pub uncertainties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventClaimSnapshot {
    pub claim_id: Uuid,
    pub canonical_claim_id: String,
    pub status: Option<String>,
    pub value: Option<NormalizedValue>,
    pub entity_roles: Vec<ClaimEntityRole>,
    pub claim_date: Option<NaiveDate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimEntityRole {
    pub entity_id: String,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedValue {
    pub raw_value: String,
    pub raw_unit: Option<String>,
    pub normalized_value: String,
    pub normalized_unit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectationSnapshot {
    pub canonical_claim_id: String,
    pub expected: Option<NormalizedValue>,
    pub observed: Option<NormalizedValue>,
    pub expected_date: Option<NaiveDate>,
    pub observed_date: Option<NaiveDate>,
}

pub fn compute_event_delta(
    previous: &EventClusterVersionSnapshot,
    current: &EventClusterVersionSnapshot,
) -> EventDelta {
    let previous_claims = claims_by_key(&previous.claims);
    let current_claims = claims_by_key(&current.claims);
    let claim_keys = previous_claims
        .keys()
        .chain(current_claims.keys())
        .cloned()
        .collect::<BTreeSet<_>>();

    let mut new_claim_ids = Vec::new();
    let mut repeated_claim_ids = Vec::new();
    let mut revised_values = Vec::new();
    let mut removed_claim_ids = Vec::new();
    let mut status_changes = Vec::new();

    for key in claim_keys {
        let previous_entries = previous_claims
            .get(&key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let current_entries = current_claims
            .get(&key)
            .map(Vec::as_slice)
            .unwrap_or_default();

        for index in 0..previous_entries.len().max(current_entries.len()) {
            match (previous_entries.get(index), current_entries.get(index)) {
                (Some(previous_claim), Some(current_claim)) => {
                    let previous_status = canonical_status(previous_claim.status.as_deref());
                    let current_status = canonical_status(current_claim.status.as_deref());
                    let status_changed = previous_status != current_status;
                    let value_changed = canonical_value(previous_claim.value.as_ref())
                        != canonical_value(current_claim.value.as_ref());

                    if value_changed {
                        revised_values.push(RevisedValue {
                            canonical_claim_id: display_claim_id(current_claim),
                            previous_claim_id: previous_claim.claim_id,
                            current_claim_id: current_claim.claim_id,
                            previous: previous_claim.value.clone().map(canonicalized_value),
                            current: current_claim.value.clone().map(canonicalized_value),
                        });
                    }

                    if status_changed {
                        status_changes.push(StatusChange {
                            canonical_claim_id: display_claim_id(current_claim),
                            previous_claim_id: previous_claim.claim_id,
                            current_claim_id: current_claim.claim_id,
                            previous_status,
                            current_status,
                        });
                    }

                    if !value_changed && !status_changed {
                        repeated_claim_ids.push(current_claim.claim_id);
                    }
                }
                (Some(previous_claim), None) => removed_claim_ids.push(previous_claim.claim_id),
                (None, Some(current_claim)) => new_claim_ids.push(current_claim.claim_id),
                (None, None) => {}
            }
        }
    }

    new_claim_ids.sort_unstable();
    repeated_claim_ids.sort_unstable();
    removed_claim_ids.sort_unstable();
    revised_values.sort_by(revised_value_sort_key);
    status_changes.sort_by(status_change_sort_key);

    EventDelta {
        new_claim_ids,
        repeated_claim_ids,
        revised_values,
        removed_claim_ids,
        status_changes,
        expectation_gap: changed_expectation_gap(
            previous.expectation.as_ref(),
            current.expectation.as_ref(),
        ),
        new_uncertainties: uncertainty_difference(&previous.uncertainties, &current.uncertainties),
        resolved_uncertainties: uncertainty_difference(
            &current.uncertainties,
            &previous.uncertainties,
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ClaimComparisonKey {
    canonical_claim_id: String,
    entity_roles: Vec<(String, String)>,
    claim_date: Option<NaiveDate>,
}

fn claims_by_key(
    claims: &[EventClaimSnapshot],
) -> BTreeMap<ClaimComparisonKey, Vec<&EventClaimSnapshot>> {
    let mut claims_by_key = BTreeMap::<ClaimComparisonKey, Vec<&EventClaimSnapshot>>::new();

    for claim in claims {
        claims_by_key
            .entry(claim_comparison_key(claim))
            .or_default()
            .push(claim);
    }

    for claims in claims_by_key.values_mut() {
        claims.sort_by_key(|claim| claim.claim_id);
    }

    claims_by_key
}

fn claim_comparison_key(claim: &EventClaimSnapshot) -> ClaimComparisonKey {
    let mut entity_roles = claim
        .entity_roles
        .iter()
        .map(|entity_role| {
            (
                canonical_text(&entity_role.entity_id),
                canonical_text(&entity_role.role),
            )
        })
        .collect::<Vec<_>>();
    entity_roles.sort();

    ClaimComparisonKey {
        canonical_claim_id: canonical_text(&claim.canonical_claim_id),
        entity_roles,
        claim_date: claim.claim_date,
    }
}

fn changed_expectation_gap(
    previous: Option<&ExpectationSnapshot>,
    current: Option<&ExpectationSnapshot>,
) -> Option<ExpectationGap> {
    let previous_gap = previous.and_then(expectation_gap_from_snapshot);
    let current_gap = current.and_then(expectation_gap_from_snapshot);
    match (previous_gap, current_gap) {
        (_, None) => None,
        (Some(previous_gap), Some(current_gap)) if previous_gap == current_gap => None,
        (_, Some(current_gap)) => Some(current_gap),
    }
}

fn expectation_gap_from_snapshot(snapshot: &ExpectationSnapshot) -> Option<ExpectationGap> {
    let expected = snapshot.expected.clone().map(canonicalized_value);
    let observed = snapshot.observed.clone().map(canonicalized_value);
    if canonical_value(expected.as_ref()) == canonical_value(observed.as_ref())
        && snapshot.expected_date == snapshot.observed_date
    {
        return None;
    }

    Some(ExpectationGap {
        canonical_claim_id: canonical_text(&snapshot.canonical_claim_id),
        expected,
        observed,
        expected_date: snapshot.expected_date,
        observed_date: snapshot.observed_date,
    })
}

fn uncertainty_difference(previous: &[String], current: &[String]) -> Vec<String> {
    let previous_map = canonicalized_strings(previous);
    let current_map = canonicalized_strings(current);

    current_map
        .into_iter()
        .filter_map(|(normalized, display)| {
            (!previous_map.contains_key(&normalized)).then_some(display)
        })
        .collect()
}

fn canonicalized_strings(values: &[String]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for value in values {
        let normalized = canonical_text(value);
        if normalized.is_empty() {
            continue;
        }
        let display = collapse_whitespace(value);
        match map.get(&normalized) {
            Some(existing) if existing <= &display => {}
            _ => {
                map.insert(normalized, display);
            }
        }
    }
    map
}

fn display_claim_id(claim: &EventClaimSnapshot) -> String {
    canonical_text(&claim.canonical_claim_id)
}

fn canonical_status(status: Option<&str>) -> Option<String> {
    status
        .map(canonical_text)
        .and_then(|status| (!status.is_empty()).then_some(status))
}

fn canonical_value(value: Option<&NormalizedValue>) -> Option<(String, Option<String>)> {
    value.map(|value| {
        (
            canonical_text(&value.normalized_value),
            value
                .normalized_unit
                .as_deref()
                .map(canonical_text)
                .and_then(|unit| (!unit.is_empty()).then_some(unit)),
        )
    })
}

fn canonicalized_value(value: NormalizedValue) -> NormalizedValue {
    NormalizedValue {
        raw_value: collapse_whitespace(&value.raw_value),
        raw_unit: value.raw_unit.map(|unit| canonical_text(&unit)),
        normalized_value: canonical_text(&value.normalized_value),
        normalized_unit: value.normalized_unit.map(|unit| canonical_text(&unit)),
    }
}

fn revised_value_sort_key(left: &RevisedValue, right: &RevisedValue) -> std::cmp::Ordering {
    left.canonical_claim_id
        .cmp(&right.canonical_claim_id)
        .then(left.previous_claim_id.cmp(&right.previous_claim_id))
        .then(left.current_claim_id.cmp(&right.current_claim_id))
}

fn status_change_sort_key(left: &StatusChange, right: &StatusChange) -> std::cmp::Ordering {
    left.canonical_claim_id
        .cmp(&right.canonical_claim_id)
        .then(left.previous_claim_id.cmp(&right.previous_claim_id))
        .then(left.current_claim_id.cmp(&right.current_claim_id))
}

fn collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn canonical_text(value: &str) -> String {
    collapse_whitespace(value).trim().to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_revisions_use_revised_values_instead_of_new_and_removed_claims() {
        let canonical_claim_id = "order_amount".to_string();
        let issuer_id = "issuer:acme".to_string();
        let order_date = date(2026, 7, 10);
        let previous_claim_id = Uuid::from_u128(1);
        let current_claim_id = Uuid::from_u128(2);

        let previous = cluster_version(
            1,
            "active",
            vec![claim_snapshot(
                previous_claim_id,
                &canonical_claim_id,
                Some("expected"),
                Some(normalized_value("1 billion", "cny", "1000000000", "cny")),
                vec![entity_role(&issuer_id, "issuer")],
                Some(order_date),
            )],
            None,
            vec!["Customer schedule pending"],
        );
        let current = cluster_version(
            2,
            "active",
            vec![claim_snapshot(
                current_claim_id,
                &canonical_claim_id,
                Some("expected"),
                Some(normalized_value("800 million", "cny", "800000000", "cny")),
                vec![entity_role(&issuer_id, "issuer")],
                Some(order_date),
            )],
            None,
            vec!["Customer schedule pending"],
        );

        let delta = compute_event_delta(&previous, &current);

        assert!(delta.new_claim_ids.is_empty());
        assert!(delta.repeated_claim_ids.is_empty());
        assert!(delta.removed_claim_ids.is_empty());
        assert_eq!(
            delta.revised_values,
            vec![RevisedValue {
                canonical_claim_id,
                previous_claim_id,
                current_claim_id,
                previous: Some(normalized_value("1 billion", "cny", "1000000000", "cny")),
                current: Some(normalized_value("800 million", "cny", "800000000", "cny")),
            }]
        );
    }

    #[test]
    fn comparison_key_uses_entity_roles_and_dates() {
        let canonical_claim_id = "order_status".to_string();
        let issuer_id = "issuer:acme".to_string();
        let supplier_id = "supplier:beta".to_string();
        let previous_claim_id = Uuid::from_u128(11);
        let current_claim_id = Uuid::from_u128(12);

        let previous = cluster_version(
            1,
            "active",
            vec![claim_snapshot(
                previous_claim_id,
                &canonical_claim_id,
                Some("planned"),
                None,
                vec![entity_role(&issuer_id, "issuer")],
                Some(date(2026, 7, 10)),
            )],
            None,
            Vec::new(),
        );
        let current = cluster_version(
            2,
            "active",
            vec![claim_snapshot(
                current_claim_id,
                &canonical_claim_id,
                Some("planned"),
                None,
                vec![entity_role(&supplier_id, "supplier")],
                Some(date(2026, 7, 11)),
            )],
            None,
            Vec::new(),
        );

        let delta = compute_event_delta(&previous, &current);

        assert_eq!(delta.new_claim_ids, vec![current_claim_id]);
        assert_eq!(delta.removed_claim_ids, vec![previous_claim_id]);
        assert!(delta.repeated_claim_ids.is_empty());
        assert!(delta.revised_values.is_empty());
        assert!(delta.status_changes.is_empty());
    }

    #[test]
    fn delta_output_is_deterministic_across_input_order() {
        let expectation_claim_id = "production_guidance".to_string();
        let first_new_claim_id = Uuid::from_u128(21);
        let second_new_claim_id = Uuid::from_u128(22);
        let revised_previous_claim_id = Uuid::from_u128(23);
        let revised_current_claim_id = Uuid::from_u128(24);
        let repeated_claim_id = Uuid::from_u128(25);
        let previous_removed_claim_id = Uuid::from_u128(26);

        let previous = cluster_version(
            1,
            "active",
            vec![
                claim_snapshot(
                    previous_removed_claim_id,
                    "obsolete_claim",
                    Some("withdrawn"),
                    None,
                    vec![entity_role("issuer:acme", "issuer")],
                    None,
                ),
                claim_snapshot(
                    revised_previous_claim_id,
                    "order_amount",
                    Some("expected"),
                    Some(normalized_value("1 billion", "cny", "1000000000", "cny")),
                    vec![entity_role("issuer:acme", "issuer")],
                    Some(date(2026, 7, 10)),
                ),
                claim_snapshot(
                    repeated_claim_id,
                    "plant_opening",
                    Some("confirmed"),
                    None,
                    vec![entity_role("issuer:acme", "issuer")],
                    Some(date(2026, 7, 12)),
                ),
            ],
            Some(expectation_snapshot(
                &expectation_claim_id,
                Some(normalized_value("5000 units", "units", "5000", "units")),
                Some(normalized_value("4800 units", "units", "4800", "units")),
                Some(date(2026, 7, 15)),
                Some(date(2026, 7, 15)),
            )),
            vec!["Financing terms pending", " Permits under review "],
        );
        let current = cluster_version(
            2,
            "active",
            vec![
                claim_snapshot(
                    second_new_claim_id,
                    "follow_on_order",
                    Some("confirmed"),
                    None,
                    vec![entity_role("customer:omega", "customer")],
                    Some(date(2026, 7, 13)),
                ),
                claim_snapshot(
                    revised_current_claim_id,
                    "order_amount",
                    Some("confirmed"),
                    Some(normalized_value("800 million", "cny", "800000000", "cny")),
                    vec![entity_role("issuer:acme", "issuer")],
                    Some(date(2026, 7, 10)),
                ),
                claim_snapshot(
                    first_new_claim_id,
                    "government_approval",
                    Some("confirmed"),
                    None,
                    vec![entity_role("regulator:cn", "regulator")],
                    Some(date(2026, 7, 11)),
                ),
                claim_snapshot(
                    repeated_claim_id,
                    "plant_opening",
                    Some("confirmed"),
                    None,
                    vec![entity_role("issuer:acme", "issuer")],
                    Some(date(2026, 7, 12)),
                ),
            ],
            Some(expectation_snapshot(
                &expectation_claim_id,
                Some(normalized_value("5000 units", "units", "5000", "units")),
                Some(normalized_value("4200 units", "units", "4200", "units")),
                Some(date(2026, 7, 15)),
                Some(date(2026, 7, 15)),
            )),
            vec!["Permits under review", "Customer schedule pending"],
        );

        let delta = compute_event_delta(&previous, &current);

        assert_eq!(
            delta.new_claim_ids,
            vec![first_new_claim_id, second_new_claim_id]
        );
        assert_eq!(delta.repeated_claim_ids, vec![repeated_claim_id]);
        assert_eq!(delta.removed_claim_ids, vec![previous_removed_claim_id]);
        assert_eq!(
            delta.status_changes,
            vec![StatusChange {
                canonical_claim_id: "order_amount".to_string(),
                previous_claim_id: revised_previous_claim_id,
                current_claim_id: revised_current_claim_id,
                previous_status: Some("expected".to_string()),
                current_status: Some("confirmed".to_string()),
            }]
        );
        assert_eq!(
            delta.revised_values,
            vec![RevisedValue {
                canonical_claim_id: "order_amount".to_string(),
                previous_claim_id: revised_previous_claim_id,
                current_claim_id: revised_current_claim_id,
                previous: Some(normalized_value("1 billion", "cny", "1000000000", "cny")),
                current: Some(normalized_value("800 million", "cny", "800000000", "cny")),
            }]
        );
        assert_eq!(
            delta.expectation_gap,
            Some(ExpectationGap {
                canonical_claim_id: expectation_claim_id,
                expected: Some(normalized_value("5000 units", "units", "5000", "units")),
                observed: Some(normalized_value("4200 units", "units", "4200", "units")),
                expected_date: Some(date(2026, 7, 15)),
                observed_date: Some(date(2026, 7, 15)),
            })
        );
        assert_eq!(
            delta.new_uncertainties,
            vec!["Customer schedule pending".to_string()]
        );
        assert_eq!(
            delta.resolved_uncertainties,
            vec!["Financing terms pending".to_string()]
        );
    }

    fn cluster_version(
        cluster_version: i32,
        lifecycle_status: &str,
        claims: Vec<EventClaimSnapshot>,
        expectation: Option<ExpectationSnapshot>,
        uncertainties: Vec<&str>,
    ) -> EventClusterVersionSnapshot {
        EventClusterVersionSnapshot {
            event_cluster_id: Uuid::from_u128(700),
            cluster_version,
            lifecycle_status: lifecycle_status.to_string(),
            claims,
            expectation,
            uncertainties: uncertainties.into_iter().map(str::to_string).collect(),
        }
    }

    fn claim_snapshot(
        claim_id: Uuid,
        canonical_claim_id: &str,
        status: Option<&str>,
        value: Option<NormalizedValue>,
        entity_roles: Vec<ClaimEntityRole>,
        claim_date: Option<NaiveDate>,
    ) -> EventClaimSnapshot {
        EventClaimSnapshot {
            claim_id,
            canonical_claim_id: canonical_claim_id.to_string(),
            status: status.map(str::to_string),
            value,
            entity_roles,
            claim_date,
        }
    }

    fn entity_role(entity_id: &str, role: &str) -> ClaimEntityRole {
        ClaimEntityRole {
            entity_id: entity_id.to_string(),
            role: role.to_string(),
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

    fn expectation_snapshot(
        canonical_claim_id: &str,
        expected: Option<NormalizedValue>,
        observed: Option<NormalizedValue>,
        expected_date: Option<NaiveDate>,
        observed_date: Option<NaiveDate>,
    ) -> ExpectationSnapshot {
        ExpectationSnapshot {
            canonical_claim_id: canonical_claim_id.to_string(),
            expected,
            observed,
            expected_date,
            observed_date,
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }
}
