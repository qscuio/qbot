use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::services::scan_ranker::{
    POOL_LONG_A_ID, POOL_LONG_B_ID, POOL_MID_A_ID, POOL_MID_B_ID, POOL_SHORT_A_ID, POOL_SHORT_B_ID,
};
use crate::state::AppState;
use crate::storage::postgres;

pub const FORWARD_HORIZONS: [usize; 4] = [1, 3, 5, 10];

#[derive(Debug, Clone)]
pub struct SignalOutcomeSample {
    pub signal_id: String,
    pub entry_close: f64,
    pub close_1d: Option<f64>,
    pub close_3d: Option<f64>,
    pub close_5d: Option<f64>,
    pub close_10d: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct HorizonPerformance {
    pub days: usize,
    pub samples: usize,
    pub avg_return_pct: f64,
    pub win_rate_pct: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SignalPerformanceSummary {
    pub signal_id: String,
    pub total_samples: usize,
    pub horizons: Vec<HorizonPerformance>,
}

pub struct ScannerStatsService {
    state: Arc<AppState>,
}

impl ScannerStatsService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn summarize(
        &self,
        lookback_days: i64,
        signal_id: Option<&str>,
    ) -> Result<Vec<SignalPerformanceSummary>> {
        let rows =
            postgres::list_signal_outcome_samples(&self.state.db, lookback_days, signal_id).await?;
        let samples: Vec<SignalOutcomeSample> = rows.into_iter().map(Into::into).collect();
        Ok(summarize_signal_performance(&samples))
    }
}

impl From<postgres::SignalOutcomeRow> for SignalOutcomeSample {
    fn from(value: postgres::SignalOutcomeRow) -> Self {
        Self {
            signal_id: value.signal_id,
            entry_close: value.entry_close,
            close_1d: value.close_1d,
            close_3d: value.close_3d,
            close_5d: value.close_5d,
            close_10d: value.close_10d,
        }
    }
}

pub fn summarize_signal_performance(
    samples: &[SignalOutcomeSample],
) -> Vec<SignalPerformanceSummary> {
    let mut grouped: HashMap<&str, Vec<&SignalOutcomeSample>> = HashMap::new();
    for sample in samples {
        grouped.entry(&sample.signal_id).or_default().push(sample);
    }

    let mut summaries: Vec<SignalPerformanceSummary> = grouped
        .into_iter()
        .map(|(signal_id, rows)| SignalPerformanceSummary {
            signal_id: signal_id.to_string(),
            total_samples: rows.len(),
            horizons: FORWARD_HORIZONS
                .iter()
                .map(|days| summarize_horizon(*days, &rows))
                .collect(),
        })
        .collect();

    summaries.sort_by(|a, b| {
        signal_summary_sort_rank(&a.signal_id)
            .cmp(&signal_summary_sort_rank(&b.signal_id))
            .then_with(|| b.total_samples.cmp(&a.total_samples))
            .then_with(|| a.signal_id.cmp(&b.signal_id))
    });
    summaries
}

fn signal_summary_sort_rank(signal_id: &str) -> i32 {
    match signal_id {
        POOL_SHORT_A_ID => 0,
        POOL_SHORT_B_ID => 1,
        POOL_MID_A_ID => 2,
        POOL_MID_B_ID => 3,
        POOL_LONG_A_ID => 4,
        POOL_LONG_B_ID => 5,
        "multi_signal" => 6,
        _ => 20,
    }
}

fn summarize_horizon(days: usize, rows: &[&SignalOutcomeSample]) -> HorizonPerformance {
    let returns: Vec<f64> = rows
        .iter()
        .filter_map(|sample| {
            let exit_close = match days {
                1 => sample.close_1d,
                3 => sample.close_3d,
                5 => sample.close_5d,
                10 => sample.close_10d,
                _ => None,
            }?;
            if sample.entry_close <= 0.0 {
                return None;
            }
            Some((exit_close / sample.entry_close - 1.0) * 100.0)
        })
        .collect();

    if returns.is_empty() {
        return HorizonPerformance {
            days,
            samples: 0,
            avg_return_pct: 0.0,
            win_rate_pct: 0.0,
        };
    }

    let samples = returns.len();
    let avg_return_pct = returns.iter().sum::<f64>() / samples as f64;
    let wins = returns.iter().filter(|ret| **ret > 0.0).count();

    HorizonPerformance {
        days,
        samples,
        avg_return_pct: round2(avg_return_pct),
        win_rate_pct: round2(wins as f64 / samples as f64 * 100.0),
    }
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarize_signal_performance_groups_by_signal_and_horizon() {
        let summaries = summarize_signal_performance(&[
            SignalOutcomeSample {
                signal_id: "startup".to_string(),
                entry_close: 10.0,
                close_1d: Some(11.0),
                close_3d: Some(9.0),
                close_5d: None,
                close_10d: Some(12.0),
            },
            SignalOutcomeSample {
                signal_id: "startup".to_string(),
                entry_close: 20.0,
                close_1d: Some(18.0),
                close_3d: Some(22.0),
                close_5d: Some(24.0),
                close_10d: None,
            },
            SignalOutcomeSample {
                signal_id: "breakout".to_string(),
                entry_close: 8.0,
                close_1d: Some(8.4),
                close_3d: Some(8.8),
                close_5d: Some(9.2),
                close_10d: Some(9.6),
            },
        ]);

        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].signal_id, "startup");
        assert_eq!(summaries[0].total_samples, 2);

        assert_eq!(
            summaries[0].horizons,
            vec![
                HorizonPerformance {
                    days: 1,
                    samples: 2,
                    avg_return_pct: 0.0,
                    win_rate_pct: 50.0,
                },
                HorizonPerformance {
                    days: 3,
                    samples: 2,
                    avg_return_pct: 0.0,
                    win_rate_pct: 50.0,
                },
                HorizonPerformance {
                    days: 5,
                    samples: 1,
                    avg_return_pct: 20.0,
                    win_rate_pct: 100.0,
                },
                HorizonPerformance {
                    days: 10,
                    samples: 1,
                    avg_return_pct: 20.0,
                    win_rate_pct: 100.0,
                },
            ]
        );

        assert_eq!(summaries[1].signal_id, "breakout");
        assert_eq!(summaries[1].total_samples, 1);
        assert_eq!(summaries[1].horizons[0].avg_return_pct, 5.0);
    }

    #[test]
    fn summarize_signal_performance_prioritizes_ranked_pools_before_raw_signals() {
        let summaries = summarize_signal_performance(&[
            SignalOutcomeSample {
                signal_id: "startup".to_string(),
                entry_close: 10.0,
                close_1d: Some(10.5),
                close_3d: Some(10.7),
                close_5d: Some(10.9),
                close_10d: Some(11.1),
            },
            SignalOutcomeSample {
                signal_id: "startup".to_string(),
                entry_close: 9.0,
                close_1d: Some(9.1),
                close_3d: Some(9.2),
                close_5d: Some(9.3),
                close_10d: Some(9.5),
            },
            SignalOutcomeSample {
                signal_id: crate::services::scan_ranker::POOL_SHORT_A_ID.to_string(),
                entry_close: 12.0,
                close_1d: Some(12.6),
                close_3d: Some(13.0),
                close_5d: Some(13.3),
                close_10d: Some(13.8),
            },
        ]);

        assert_eq!(summaries.len(), 2);
        assert_eq!(
            summaries[0].signal_id,
            crate::services::scan_ranker::POOL_SHORT_A_ID
        );
        assert_eq!(summaries[1].signal_id, "startup");
    }
}
