use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventStatisticsKey {
    pub event_type: String,
    pub event_subtype: Option<String>,
    pub entity_type: String,
    pub observation_window: String,
    pub data_cutoff: DateTime<Utc>,
    pub logic_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalEventObservation {
    pub event_type: String,
    pub event_subtype: Option<String>,
    pub entity_type: String,
    pub observation_window: String,
    pub available_at: DateTime<Utc>,
    pub first_seen_at: DateTime<Utc>,
    pub abnormal_return: Option<f64>,
    pub turnover_response: Option<f64>,
    pub breadth_response: Option<f64>,
    pub time_to_peak: Option<f64>,
    pub failed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalEventStatistics {
    pub key: EventStatisticsKey,
    pub sample_count: u64,
    pub median_abnormal_return: Option<f64>,
    pub positive_rate: Option<f64>,
    pub turnover_response: Option<f64>,
    pub breadth_response: Option<f64>,
    pub time_to_peak: Option<f64>,
    pub failure_rate: f64,
}

pub fn build_historical_event_statistics(
    observations: &[HistoricalEventObservation],
    data_cutoff: DateTime<Utc>,
    logic_version: impl Into<String>,
) -> Vec<HistoricalEventStatistics> {
    let logic_version = logic_version.into();
    let mut grouped = BTreeMap::<EventStatisticsKey, Vec<&HistoricalEventObservation>>::new();

    for observation in observations.iter().filter(|observation| {
        observation.available_at <= data_cutoff && observation.first_seen_at <= data_cutoff
    }) {
        grouped
            .entry(EventStatisticsKey {
                event_type: observation.event_type.clone(),
                event_subtype: observation.event_subtype.clone(),
                entity_type: observation.entity_type.clone(),
                observation_window: observation.observation_window.clone(),
                data_cutoff,
                logic_version: logic_version.clone(),
            })
            .or_default()
            .push(observation);
    }

    grouped
        .into_iter()
        .map(|(key, observations)| HistoricalEventStatistics {
            sample_count: observations.len() as u64,
            median_abnormal_return: median_metric(
                observations
                    .iter()
                    .filter_map(|observation| observation.abnormal_return),
            ),
            positive_rate: positive_rate(&observations),
            turnover_response: median_metric(
                observations
                    .iter()
                    .filter_map(|observation| observation.turnover_response),
            ),
            breadth_response: median_metric(
                observations
                    .iter()
                    .filter_map(|observation| observation.breadth_response),
            ),
            time_to_peak: median_metric(
                observations
                    .iter()
                    .filter_map(|observation| observation.time_to_peak),
            ),
            failure_rate: round_metric(
                observations
                    .iter()
                    .filter(|observation| observation.failed)
                    .count() as f64
                    / observations.len() as f64,
            ),
            key,
        })
        .collect()
}

fn positive_rate(observations: &[&HistoricalEventObservation]) -> Option<f64> {
    let abnormal_returns = observations
        .iter()
        .filter_map(|observation| observation.abnormal_return)
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if abnormal_returns.is_empty() {
        return None;
    }

    Some(round_metric(
        abnormal_returns
            .iter()
            .filter(|value| **value > 0.0)
            .count() as f64
            / abnormal_returns.len() as f64,
    ))
}

fn median_metric(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }

    values.sort_by(|left, right| left.total_cmp(right));
    let midpoint = values.len() / 2;
    let median = if values.len() % 2 == 0 {
        (values[midpoint - 1] + values[midpoint]) / 2.0
    } else {
        values[midpoint]
    };

    Some(round_metric(median))
}

fn round_metric(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn builds_versioned_statistics_with_the_exact_aggregation_key_fields() {
        let data_cutoff = dt(2026, 7, 14, 9, 30, 0);
        let statistics = build_historical_event_statistics(
            &[
                observation(
                    "contract_award",
                    Some("major_customer"),
                    "issuer",
                    "t+1",
                    dt(2026, 7, 14, 9, 0, 0),
                    dt(2026, 7, 14, 9, 0, 0),
                    0.04,
                    1.2,
                    0.3,
                    2.0,
                    false,
                ),
                observation(
                    "contract_award",
                    Some("major_customer"),
                    "issuer",
                    "t+1",
                    dt(2026, 7, 14, 9, 10, 0),
                    dt(2026, 7, 14, 9, 10, 0),
                    -0.01,
                    0.8,
                    0.1,
                    1.0,
                    true,
                ),
                observation(
                    "contract_award",
                    None,
                    "issuer",
                    "t+5",
                    dt(2026, 7, 14, 9, 20, 0),
                    dt(2026, 7, 14, 9, 20, 0),
                    0.03,
                    1.5,
                    0.4,
                    3.0,
                    false,
                ),
            ],
            data_cutoff,
            "event-statistics-v1",
        );

        assert_eq!(statistics.len(), 2);
        assert_eq!(
            statistics[0].key,
            EventStatisticsKey {
                event_type: "contract_award".to_string(),
                event_subtype: None,
                entity_type: "issuer".to_string(),
                observation_window: "t+5".to_string(),
                data_cutoff,
                logic_version: "event-statistics-v1".to_string(),
            }
        );
        assert_eq!(
            statistics[1].key,
            EventStatisticsKey {
                event_type: "contract_award".to_string(),
                event_subtype: Some("major_customer".to_string()),
                entity_type: "issuer".to_string(),
                observation_window: "t+1".to_string(),
                data_cutoff,
                logic_version: "event-statistics-v1".to_string(),
            }
        );
    }

    #[test]
    fn events_first_seen_after_cutoff_are_excluded_from_the_baseline() {
        let data_cutoff = dt(2026, 7, 14, 9, 30, 0);
        let statistics = build_historical_event_statistics(
            &[
                observation(
                    "earnings",
                    Some("guidance_raise"),
                    "issuer",
                    "t+1",
                    dt(2026, 7, 14, 9, 0, 0),
                    dt(2026, 7, 14, 9, 0, 0),
                    -0.02,
                    -0.4,
                    -0.1,
                    1.0,
                    false,
                ),
                observation(
                    "earnings",
                    Some("guidance_raise"),
                    "issuer",
                    "t+1",
                    dt(2026, 7, 14, 9, 20, 0),
                    dt(2026, 7, 14, 9, 45, 0),
                    0.08,
                    2.0,
                    0.8,
                    4.0,
                    true,
                ),
            ],
            data_cutoff,
            "event-statistics-v1",
        );

        assert_eq!(statistics.len(), 1);
        assert_eq!(statistics[0].sample_count, 1);
        assert_eq!(statistics[0].median_abnormal_return, Some(-0.02));
        assert_eq!(statistics[0].positive_rate, Some(0.0));
        assert_eq!(statistics[0].failure_rate, 0.0);
    }

    #[test]
    fn calculates_required_historical_baseline_metrics() {
        let statistics = build_historical_event_statistics(
            &[
                observation(
                    "factory_fire",
                    None,
                    "issuer",
                    "t+5",
                    dt(2026, 7, 14, 8, 0, 0),
                    dt(2026, 7, 14, 8, 0, 0),
                    -0.02,
                    1.0,
                    -0.4,
                    1.0,
                    false,
                ),
                observation(
                    "factory_fire",
                    None,
                    "issuer",
                    "t+5",
                    dt(2026, 7, 14, 8, 5, 0),
                    dt(2026, 7, 14, 8, 5, 0),
                    0.01,
                    2.0,
                    0.0,
                    2.0,
                    true,
                ),
                observation(
                    "factory_fire",
                    None,
                    "issuer",
                    "t+5",
                    dt(2026, 7, 14, 8, 10, 0),
                    dt(2026, 7, 14, 8, 10, 0),
                    0.03,
                    5.0,
                    0.2,
                    3.0,
                    false,
                ),
                observation(
                    "factory_fire",
                    None,
                    "issuer",
                    "t+5",
                    dt(2026, 7, 14, 8, 15, 0),
                    dt(2026, 7, 14, 8, 15, 0),
                    0.05,
                    7.0,
                    0.6,
                    6.0,
                    true,
                ),
            ],
            dt(2026, 7, 14, 9, 30, 0),
            "event-statistics-v1",
        );

        assert_eq!(statistics.len(), 1);
        assert_eq!(statistics[0].sample_count, 4);
        assert_eq!(statistics[0].median_abnormal_return, Some(0.02));
        assert_eq!(statistics[0].positive_rate, Some(0.75));
        assert_eq!(statistics[0].turnover_response, Some(3.5));
        assert_eq!(statistics[0].breadth_response, Some(0.1));
        assert_eq!(statistics[0].time_to_peak, Some(2.5));
        assert_eq!(statistics[0].failure_rate, 0.5);
    }

    fn observation(
        event_type: &str,
        event_subtype: Option<&str>,
        entity_type: &str,
        observation_window: &str,
        available_at: chrono::DateTime<Utc>,
        first_seen_at: chrono::DateTime<Utc>,
        abnormal_return: f64,
        turnover_response: f64,
        breadth_response: f64,
        time_to_peak: f64,
        failed: bool,
    ) -> HistoricalEventObservation {
        HistoricalEventObservation {
            event_type: event_type.to_string(),
            event_subtype: event_subtype.map(str::to_string),
            entity_type: entity_type.to_string(),
            observation_window: observation_window.to_string(),
            available_at,
            first_seen_at,
            abnormal_return: Some(abnormal_return),
            turnover_response: Some(turnover_response),
            breadth_response: Some(breadth_response),
            time_to_peak: Some(time_to_peak),
            failed,
        }
    }

    fn dt(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .unwrap()
    }
}
