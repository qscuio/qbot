use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::analysis::market_snapshot::adjustment::adjust_candles;
use crate::analysis::market_snapshot::{
    AdjustmentFactor, IndexDailyBar, MarketSnapshot, SecurityDailyStatus,
};
use crate::data::types::Candle;
use crate::error::{AppError, Result};
use crate::storage::market_repository::{MarketRepository, PointInTimeDailyBarVersion};

const MARKET_SNAPSHOT_VERSION: &str = "market-v1";
const MARKET_BREADTH_LOOKBACK: usize = 20;
const MARKET_INDEX_CODES: &[&str] = &["000001.SH", "399001.SZ", "399006.SZ", "000688.SH"];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MarketBreadthMetrics {
    pub up_count: usize,
    pub down_count: usize,
    pub flat_count: usize,
    pub above_ma20_count: usize,
    pub new_high_20_count: usize,
    pub new_low_20_count: usize,
    pub limit_up_count: usize,
    pub limit_down_count: usize,
    pub total_amount: f64,
}

#[derive(Debug, Clone)]
pub struct SecurityBreadthInput {
    pub code: String,
    pub bars: Vec<Candle>,
    pub price_limit_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSnapshotBuildResult {
    pub snapshot: MarketSnapshot,
}

#[derive(Clone)]
pub struct MarketSnapshotModule {
    repo: MarketRepository,
}

impl MarketSnapshotModule {
    pub fn new(pool: PgPool) -> Self {
        Self {
            repo: MarketRepository::new(pool),
        }
    }

    pub async fn build_trade_date(
        &self,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<MarketSnapshotBuildResult> {
        let bar_rows = self
            .repo
            .daily_bar_history_as_of(trade_date, as_of, MARKET_BREADTH_LOOKBACK as i64)
            .await?;
        let mut bars_by_code: BTreeMap<String, Vec<PointInTimeDailyBarVersion>> = BTreeMap::new();
        for row in bar_rows {
            bars_by_code.entry(row.code.clone()).or_default().push(row);
        }
        let current_bar_codes: BTreeSet<String> = bars_by_code
            .iter()
            .filter(|(_, rows)| {
                rows.last()
                    .is_some_and(|row| row.bar.trade_date == trade_date)
            })
            .map(|(code, _)| code.clone())
            .collect();

        let status_rows = self
            .repo
            .security_status_universe_as_of(trade_date, as_of)
            .await?;
        let status_by_code: BTreeMap<String, SecurityDailyStatus> = status_rows
            .into_iter()
            .map(|row| (row.code.clone(), row))
            .collect();

        if current_bar_codes.is_empty() && status_by_code.is_empty() {
            return Err(AppError::NotFound(format!(
                "no point-in-time daily bar versions or statuses for {} as of {}",
                trade_date, as_of
            )));
        }

        let codes: Vec<String> = bars_by_code.keys().cloned().collect();
        let history_start = bars_by_code
            .values()
            .filter_map(|rows| rows.first().map(|row| row.bar.trade_date))
            .min()
            .unwrap_or(trade_date);

        let adjustment_rows = self
            .repo
            .adjustment_factors_as_of(&codes, history_start, trade_date, as_of)
            .await?;
        let mut adjustments_by_code: BTreeMap<String, BTreeMap<NaiveDate, AdjustmentFactor>> =
            BTreeMap::new();
        for row in adjustment_rows {
            adjustments_by_code
                .entry(row.code.clone())
                .or_default()
                .insert(row.trade_date, row);
        }

        let index_rows = self
            .repo
            .index_bars_as_of(
                &MARKET_INDEX_CODES
                    .iter()
                    .map(|code| (*code).to_string())
                    .collect::<Vec<_>>(),
                trade_date,
                as_of,
            )
            .await?;
        let index_by_code: BTreeMap<String, IndexDailyBar> = index_rows
            .into_iter()
            .map(|row| (row.code.clone(), row))
            .collect();

        let mut missing_inputs = BTreeSet::new();
        let mut fingerprint_inputs = BTreeSet::new();
        let mut breadth_inputs = Vec::new();

        for status in status_by_code.values() {
            fingerprint_inputs.insert(fingerprint_component(
                "security_daily_status",
                &status.code,
                status.trade_date,
                &status.source,
                status.available_at,
                status.ingested_at,
            ));

            if !current_bar_codes.contains(&status.code) {
                missing_inputs.insert(format!(
                    "stock_daily_bar_versions:{}:{}",
                    status.code, trade_date
                ));
            }
        }

        for (code, rows) in &bars_by_code {
            for row in rows {
                fingerprint_inputs.insert(fingerprint_component(
                    "stock_daily_bar_versions",
                    &row.code,
                    row.bar.trade_date,
                    &row.source,
                    row.available_at,
                    row.ingested_at,
                ));
            }

            if let Some(factors) = adjustments_by_code.get(code) {
                for factor in factors.values() {
                    fingerprint_inputs.insert(fingerprint_component(
                        "stock_adjustment_factors",
                        &factor.code,
                        factor.trade_date,
                        &factor.source,
                        factor.available_at,
                        factor.ingested_at,
                    ));
                }
            }

            let status = status_by_code.get(code);
            if status.is_none() {
                missing_inputs.insert(format!("security_daily_status:{code}:{trade_date}"));
            }

            if !current_bar_codes.contains(code) {
                continue;
            }

            let mut factors = Vec::with_capacity(rows.len());
            let factor_map = adjustments_by_code.get(code);
            let mut missing_adjustment = false;
            for row in rows {
                let Some(factor) = factor_map.and_then(|entries| entries.get(&row.bar.trade_date))
                else {
                    missing_inputs.insert(format!(
                        "stock_adjustment_factors:{code}:{}",
                        row.bar.trade_date
                    ));
                    missing_adjustment = true;
                    continue;
                };
                factors.push(factor.clone());
            }

            let Some(status) = status else {
                continue;
            };
            if missing_adjustment {
                continue;
            }

            if rows.len() < 2 {
                if status.listed_days.unwrap_or(0) > 1 {
                    missing_inputs
                        .insert(format!("stock_daily_bar_versions:{code}:previous_close"));
                }
                continue;
            }

            if rows.len() < MARKET_BREADTH_LOOKBACK
                && status.listed_days.unwrap_or_default() >= MARKET_BREADTH_LOOKBACK as i32
            {
                missing_inputs.insert(format!(
                    "stock_daily_bar_versions:{code}:lookback_{MARKET_BREADTH_LOOKBACK}"
                ));
            }

            let raw_bars: Vec<Candle> = rows.iter().map(|row| row.bar.clone()).collect();
            let adjusted_bars = adjust_candles(&raw_bars, &factors)?;

            breadth_inputs.push(SecurityBreadthInput {
                code: code.clone(),
                bars: adjusted_bars,
                price_limit_pct: status.price_limit_pct,
            });
        }

        let mut indices = Vec::with_capacity(MARKET_INDEX_CODES.len());
        for code in MARKET_INDEX_CODES {
            let Some(index) = index_by_code.get(*code) else {
                missing_inputs.insert(format!("index_daily_bars:{code}:{trade_date}"));
                continue;
            };

            fingerprint_inputs.insert(fingerprint_component(
                "index_daily_bars",
                &index.code,
                index.trade_date,
                &index.source,
                index.available_at,
                index.ingested_at,
            ));
            indices.push(index.clone());
        }

        let breadth = calculate_market_breadth(&breadth_inputs);
        let snapshot = MarketSnapshot {
            trade_date,
            snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
            available_at: as_of,
            data_complete: missing_inputs.is_empty(),
            metrics: json!({
                "breadth": breadth,
                "indices": indices,
            }),
            missing_inputs: missing_inputs.into_iter().collect(),
            input_fingerprint: calculate_input_fingerprint(
                &fingerprint_inputs.into_iter().collect::<Vec<_>>(),
            ),
        };

        self.repo.save_market_snapshot(&snapshot).await?;

        Ok(MarketSnapshotBuildResult { snapshot })
    }
}

pub fn calculate_market_breadth(inputs: &[SecurityBreadthInput]) -> MarketBreadthMetrics {
    let mut metrics = MarketBreadthMetrics::default();

    for input in inputs {
        if input.bars.len() < 2 {
            continue;
        }

        let mut bars = input.bars.clone();
        bars.sort_by_key(|bar| bar.trade_date);

        let today = &bars[bars.len() - 1];
        let previous = &bars[bars.len() - 2];
        metrics.total_amount += today.amount;

        if today.close > previous.close {
            metrics.up_count += 1;
        } else if today.close < previous.close {
            metrics.down_count += 1;
        } else {
            metrics.flat_count += 1;
        }

        if let Some(limit_pct) = input.price_limit_pct {
            let change_pct = if previous.close > 0.0 {
                (today.close - previous.close) / previous.close * 100.0
            } else {
                0.0
            };
            let tolerance = 0.05;
            if change_pct >= limit_pct - tolerance {
                metrics.limit_up_count += 1;
            }
            if change_pct <= -limit_pct + tolerance {
                metrics.limit_down_count += 1;
            }
        }

        if bars.len() < MARKET_BREADTH_LOOKBACK {
            continue;
        }

        let window = &bars[bars.len() - MARKET_BREADTH_LOOKBACK..];
        let ma20 = window.iter().map(|bar| bar.close).sum::<f64>() / MARKET_BREADTH_LOOKBACK as f64;
        if today.close > ma20 {
            metrics.above_ma20_count += 1;
        }

        let prior_high = window[..MARKET_BREADTH_LOOKBACK - 1]
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);
        if today.high > prior_high {
            metrics.new_high_20_count += 1;
        }

        let prior_low = window[..MARKET_BREADTH_LOOKBACK - 1]
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min);
        if today.low < prior_low {
            metrics.new_low_20_count += 1;
        }
    }

    metrics
}

fn fingerprint_component(
    category: &str,
    code: &str,
    trade_date: NaiveDate,
    source: &str,
    available_at: DateTime<Utc>,
    ingested_at: DateTime<Utc>,
) -> String {
    format!(
        "{category}|{code}|{trade_date}|{source}|{}|{}",
        available_at.to_rfc3339(),
        ingested_at.to_rfc3339()
    )
}

fn calculate_input_fingerprint(entries: &[String]) -> String {
    let mut sorted = entries.to_vec();
    sorted.sort();

    let mut hasher = Sha256::new();
    for entry in sorted {
        hasher.update(entry.as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use sqlx::PgPool;

    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 7, day).unwrap()
    }

    fn dt(day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, day, hour, minute, 0).unwrap()
    }

    fn candle(day: u32, open: f64, high: f64, low: f64, close: f64, amount: f64) -> Candle {
        Candle {
            trade_date: date(day),
            open,
            high,
            low,
            close,
            volume: 1_000,
            amount,
            turnover: None,
            pe: None,
            pb: None,
        }
    }

    fn input(code: &str, rows: &[(f64, f64, f64, f64)]) -> SecurityBreadthInput {
        let bars = rows
            .iter()
            .enumerate()
            .map(|(index, (open, high, low, close))| {
                candle(index as u32 + 1, *open, *high, *low, *close, 1_000.0)
            })
            .collect();

        SecurityBreadthInput {
            code: code.to_string(),
            bars,
            price_limit_pct: Some(10.0),
        }
    }

    #[test]
    fn calculates_market_breadth_from_four_securities() {
        let metrics = calculate_market_breadth(&[
            input(
                "AAA",
                &[
                    (10.0, 10.5, 9.8, 10.2),
                    (10.2, 10.7, 10.0, 10.4),
                    (10.4, 10.9, 10.2, 10.6),
                    (10.6, 11.1, 10.4, 10.8),
                    (10.8, 11.3, 10.6, 11.0),
                    (11.0, 11.5, 10.8, 11.2),
                    (11.2, 11.7, 11.0, 11.4),
                    (11.4, 11.9, 11.2, 11.6),
                    (11.6, 12.1, 11.4, 11.8),
                    (11.8, 12.3, 11.6, 12.0),
                    (12.0, 12.5, 11.8, 12.2),
                    (12.2, 12.7, 12.0, 12.4),
                    (12.4, 12.9, 12.2, 12.6),
                    (12.6, 13.1, 12.4, 12.8),
                    (12.8, 13.3, 12.6, 13.0),
                    (13.0, 13.5, 12.8, 13.2),
                    (13.2, 13.7, 13.0, 13.4),
                    (13.4, 13.9, 13.2, 13.6),
                    (13.6, 14.1, 13.4, 13.8),
                    (13.8, 15.0, 13.6, 14.6),
                ],
            ),
            input(
                "BBB",
                &[
                    (30.0, 30.4, 29.6, 30.0),
                    (29.8, 30.2, 29.2, 29.6),
                    (29.4, 29.8, 28.8, 29.2),
                    (29.0, 29.4, 28.4, 28.8),
                    (28.6, 29.0, 28.0, 28.4),
                    (28.2, 28.6, 27.6, 28.0),
                    (27.8, 28.2, 27.2, 27.6),
                    (27.4, 27.8, 26.8, 27.2),
                    (27.0, 27.4, 26.4, 26.8),
                    (26.6, 27.0, 26.0, 26.4),
                    (26.2, 26.6, 25.6, 26.0),
                    (25.8, 26.2, 25.2, 25.6),
                    (25.4, 25.8, 24.8, 25.2),
                    (25.0, 25.4, 24.4, 24.8),
                    (24.6, 25.0, 24.0, 24.4),
                    (24.2, 24.6, 23.6, 24.0),
                    (23.8, 24.2, 23.2, 23.6),
                    (23.4, 23.8, 22.8, 23.2),
                    (23.0, 23.4, 22.4, 22.8),
                    (22.6, 23.0, 21.0, 21.8),
                ],
            ),
            input(
                "CCC",
                &[
                    (10.0, 23.0, 9.8, 10.2),
                    (10.2, 23.0, 10.0, 10.4),
                    (10.4, 23.0, 10.2, 10.6),
                    (10.6, 23.0, 10.4, 10.8),
                    (10.8, 23.0, 10.6, 11.0),
                    (11.0, 23.0, 10.8, 11.2),
                    (11.2, 23.0, 11.0, 11.4),
                    (11.4, 23.0, 11.2, 11.6),
                    (11.6, 23.0, 11.4, 11.8),
                    (11.8, 23.0, 11.6, 12.0),
                    (12.0, 23.0, 11.8, 12.2),
                    (12.2, 23.0, 12.0, 12.4),
                    (12.4, 23.0, 12.2, 12.6),
                    (12.6, 23.0, 12.4, 12.8),
                    (12.8, 23.0, 12.6, 13.0),
                    (13.0, 23.0, 12.8, 13.2),
                    (13.2, 23.0, 13.0, 13.4),
                    (13.4, 23.0, 13.2, 13.6),
                    (13.8, 22.8, 13.5, 14.0),
                    (14.0, 22.5, 13.4, 14.0),
                ],
            ),
            input(
                "DDD",
                &[
                    (30.0, 30.5, 20.0, 30.0),
                    (29.8, 30.3, 19.8, 29.8),
                    (29.6, 30.1, 19.6, 29.6),
                    (29.4, 29.9, 19.4, 29.4),
                    (29.2, 29.7, 19.2, 29.2),
                    (29.0, 29.5, 19.0, 29.0),
                    (28.8, 29.3, 18.8, 28.8),
                    (28.6, 29.1, 18.6, 28.6),
                    (28.4, 28.9, 18.4, 28.4),
                    (28.2, 28.7, 18.2, 28.2),
                    (28.0, 28.5, 18.0, 28.0),
                    (27.8, 28.3, 17.8, 27.8),
                    (27.6, 28.1, 17.6, 27.6),
                    (27.4, 27.9, 17.4, 27.4),
                    (27.2, 27.7, 17.2, 27.2),
                    (27.0, 27.5, 17.0, 27.0),
                    (15.0, 15.4, 8.0, 14.0),
                    (13.5, 13.9, 7.5, 12.5),
                    (10.5, 10.8, 6.5, 10.0),
                    (10.0, 12.0, 7.0, 11.0),
                ],
            ),
        ]);

        assert_eq!(metrics.up_count, 2);
        assert_eq!(metrics.down_count, 1);
        assert_eq!(metrics.flat_count, 1);
        assert_eq!(metrics.above_ma20_count, 2);
        assert_eq!(metrics.new_high_20_count, 1);
        assert_eq!(metrics.new_low_20_count, 1);
    }

    #[test]
    fn input_fingerprint_is_stable_for_unsorted_entries() {
        let fingerprint =
            calculate_input_fingerprint(&["b|2".to_string(), "a|1".to_string(), "a|1".to_string()]);

        let expected =
            calculate_input_fingerprint(&["a|1".to_string(), "a|1".to_string(), "b|2".to_string()]);

        assert_eq!(fingerprint, expected);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn build_trade_date_persists_incomplete_snapshot_and_records_missing_inputs(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        seed_security(&pool, "600001.SH", true, true).await?;
        seed_security(&pool, "600002.SH", false, true).await?;
        seed_security(&pool, "600003.SH", true, false).await?;
        seed_index(&pool, "000001.SH", 3000.0, dt(20, 18, 0), dt(20, 18, 5)).await?;

        let module = MarketSnapshotModule::new(pool.clone());
        let result = module
            .build_trade_date(date(20), dt(20, 19, 0))
            .await
            .unwrap();

        assert!(!result.snapshot.data_complete);
        assert!(result
            .snapshot
            .missing_inputs
            .contains(&"stock_adjustment_factors:600002.SH:2026-07-19".to_string()));
        assert!(result
            .snapshot
            .missing_inputs
            .contains(&"security_daily_status:600003.SH:2026-07-20".to_string()));
        assert!(result
            .snapshot
            .missing_inputs
            .contains(&"index_daily_bars:399001.SZ:2026-07-20".to_string()));

        let saved = MarketRepository::new(pool)
            .market_snapshot(date(20), MARKET_SNAPSHOT_VERSION)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.trade_date, date(20));
        assert_eq!(saved.snapshot_version, MARKET_SNAPSHOT_VERSION);
        assert_eq!(saved.metrics["breadth"]["up_count"], json!(1));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn build_trade_date_uses_sorted_source_inputs_for_fingerprint(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        seed_security(&pool, "600010.SH", true, true).await?;
        seed_index(&pool, "000001.SH", 3000.0, dt(20, 18, 0), dt(20, 18, 5)).await?;
        seed_index(&pool, "399001.SZ", 12000.0, dt(20, 18, 1), dt(20, 18, 5)).await?;
        seed_index(&pool, "399006.SZ", 2500.0, dt(20, 18, 2), dt(20, 18, 5)).await?;
        seed_index(&pool, "000688.SH", 900.0, dt(20, 18, 3), dt(20, 18, 5)).await?;

        let snapshot = MarketSnapshotModule::new(pool)
            .build_trade_date(date(20), dt(20, 19, 0))
            .await
            .unwrap()
            .snapshot;

        let mut entries = Vec::new();
        for day in 1..=20 {
            entries.push(fingerprint_component(
                "stock_daily_bar_versions",
                "600010.SH",
                date(day),
                "bars",
                dt(20, 17, 0),
                dt(20, 17, 5),
            ));
            entries.push(fingerprint_component(
                "stock_adjustment_factors",
                "600010.SH",
                date(day),
                "adjustments",
                dt(20, 17, 10),
                dt(20, 17, 15),
            ));
        }
        entries.push(fingerprint_component(
            "security_daily_status",
            "600010.SH",
            date(20),
            "status",
            dt(20, 17, 20),
            dt(20, 17, 25),
        ));
        entries.push(fingerprint_component(
            "index_daily_bars",
            "000001.SH",
            date(20),
            "index",
            dt(20, 18, 0),
            dt(20, 18, 5),
        ));
        entries.push(fingerprint_component(
            "index_daily_bars",
            "399001.SZ",
            date(20),
            "index",
            dt(20, 18, 1),
            dt(20, 18, 5),
        ));
        entries.push(fingerprint_component(
            "index_daily_bars",
            "399006.SZ",
            date(20),
            "index",
            dt(20, 18, 2),
            dt(20, 18, 5),
        ));
        entries.push(fingerprint_component(
            "index_daily_bars",
            "000688.SH",
            date(20),
            "index",
            dt(20, 18, 3),
            dt(20, 18, 5),
        ));

        assert_eq!(
            snapshot.input_fingerprint,
            calculate_input_fingerprint(&entries)
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn build_trade_date_rebuilds_for_later_as_of_and_updates_saved_snapshot(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        seed_security(&pool, "600020.SH", true, true).await?;
        seed_all_indices(&pool).await?;

        let module = MarketSnapshotModule::new(pool.clone());
        let first = module
            .build_trade_date(date(20), dt(20, 18, 30))
            .await
            .unwrap()
            .snapshot;
        assert_eq!(first.available_at, dt(20, 18, 30));
        assert_eq!(first.metrics["indices"][0]["close"], json!(3000.0));

        seed_index_version(&pool, "000001.SH", 3100.0, dt(20, 19, 0), dt(20, 19, 5)).await?;

        let rebuilt = module
            .build_trade_date(date(20), dt(20, 19, 30))
            .await
            .unwrap()
            .snapshot;

        assert_eq!(rebuilt.available_at, dt(20, 19, 30));
        assert_eq!(rebuilt.metrics["indices"][0]["close"], json!(3100.0));
        assert_ne!(rebuilt.input_fingerprint, first.input_fingerprint);

        let saved = MarketRepository::new(pool)
            .market_snapshot(date(20), MARKET_SNAPSHOT_VERSION)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.available_at, dt(20, 19, 30));
        assert_eq!(saved.metrics["indices"][0]["close"], json!(3100.0));
        assert_eq!(saved.input_fingerprint, rebuilt.input_fingerprint);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn build_trade_date_fingerprint_includes_loaded_inputs_for_excluded_security(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        seed_security(&pool, "600030.SH", true, true).await?;
        seed_security(&pool, "600031.SH", false, true).await?;
        seed_all_indices(&pool).await?;

        let snapshot = MarketSnapshotModule::new(pool)
            .build_trade_date(date(20), dt(20, 19, 0))
            .await
            .unwrap()
            .snapshot;

        let mut entries = expected_security_fingerprint_entries("600030.SH", true);
        entries.extend(expected_security_fingerprint_entries("600031.SH", false));
        entries.extend(expected_index_fingerprint_entries());

        assert_eq!(
            snapshot.input_fingerprint,
            calculate_input_fingerprint(&entries)
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn build_trade_date_marks_status_universe_codes_missing_current_bar(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        seed_security(&pool, "600040.SH", true, true).await?;
        seed_security_history(&pool, "600041.SH", 1..=19, true).await?;
        seed_status(&pool, "600041.SH", dt(20, 17, 20), dt(20, 17, 25)).await?;
        seed_all_indices(&pool).await?;

        let snapshot = MarketSnapshotModule::new(pool)
            .build_trade_date(date(20), dt(20, 19, 0))
            .await
            .unwrap()
            .snapshot;

        assert!(!snapshot.data_complete);
        assert!(snapshot
            .missing_inputs
            .contains(&"stock_daily_bar_versions:600041.SH:2026-07-20".to_string()));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn build_trade_date_uses_deterministic_equal_available_at_versions(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        allow_duplicate_available_at_versions(&pool).await?;
        seed_security_history(&pool, "600050.SH", 1..=19, true).await?;

        seed_daily_bar_version(
            &pool,
            "600050.SH",
            20,
            20.0,
            dt(20, 17, 0),
            dt(20, 17, 5),
            "z-bars",
        )
        .await?;
        seed_daily_bar_version(
            &pool,
            "600050.SH",
            20,
            35.0,
            dt(20, 17, 0),
            dt(20, 17, 6),
            "z-bars",
        )
        .await?;
        seed_daily_bar_version(
            &pool,
            "600050.SH",
            20,
            30.0,
            dt(20, 17, 0),
            dt(20, 17, 6),
            "a-bars",
        )
        .await?;

        seed_adjustment_factor_version(
            &pool,
            "600050.SH",
            20,
            2.0,
            dt(20, 17, 10),
            dt(20, 17, 15),
            "z-adjustments",
        )
        .await?;
        seed_adjustment_factor_version(
            &pool,
            "600050.SH",
            20,
            3.0,
            dt(20, 17, 10),
            dt(20, 17, 16),
            "z-adjustments",
        )
        .await?;
        seed_adjustment_factor_version(
            &pool,
            "600050.SH",
            20,
            1.0,
            dt(20, 17, 10),
            dt(20, 17, 16),
            "a-adjustments",
        )
        .await?;

        seed_status_version(
            &pool,
            "600050.SH",
            1.0,
            dt(20, 17, 20),
            dt(20, 17, 25),
            "z-status",
        )
        .await?;
        seed_status_version(
            &pool,
            "600050.SH",
            5.0,
            dt(20, 17, 20),
            dt(20, 17, 26),
            "z-status",
        )
        .await?;
        seed_status_version(
            &pool,
            "600050.SH",
            10.0,
            dt(20, 17, 20),
            dt(20, 17, 26),
            "a-status",
        )
        .await?;

        seed_index(&pool, "000001.SH", 2900.0, dt(20, 18, 0), dt(20, 18, 5)).await?;
        seed_index(&pool, "000001.SH", 3100.0, dt(20, 18, 0), dt(20, 18, 6)).await?;
        seed_index_with_source(
            &pool,
            "000001.SH",
            3001.0,
            dt(20, 18, 0),
            dt(20, 18, 6),
            "a-index",
        )
        .await?;
        seed_index_version(&pool, "399001.SZ", 12000.0, dt(20, 18, 1), dt(20, 18, 5)).await?;
        seed_index_version(&pool, "399006.SZ", 2500.0, dt(20, 18, 2), dt(20, 18, 5)).await?;
        seed_index_version(&pool, "000688.SH", 900.0, dt(20, 18, 3), dt(20, 18, 5)).await?;

        let snapshot = MarketSnapshotModule::new(pool)
            .build_trade_date(date(20), dt(20, 19, 0))
            .await
            .unwrap()
            .snapshot;

        assert_eq!(snapshot.metrics["breadth"]["up_count"], json!(1));
        assert_eq!(snapshot.metrics["breadth"]["down_count"], json!(0));
        assert_eq!(snapshot.metrics["breadth"]["limit_up_count"], json!(0));
        assert_eq!(snapshot.metrics["indices"][0]["close"], json!(3001.0));

        let mut entries = expected_security_fingerprint_entries("600050.SH", true);
        entries.pop();
        entries.pop();
        entries.pop();
        entries.push(fingerprint_component(
            "stock_daily_bar_versions",
            "600050.SH",
            date(20),
            "a-bars",
            dt(20, 17, 0),
            dt(20, 17, 6),
        ));
        entries.push(fingerprint_component(
            "stock_adjustment_factors",
            "600050.SH",
            date(20),
            "a-adjustments",
            dt(20, 17, 10),
            dt(20, 17, 16),
        ));
        entries.push(fingerprint_component(
            "security_daily_status",
            "600050.SH",
            date(20),
            "a-status",
            dt(20, 17, 20),
            dt(20, 17, 26),
        ));
        entries.push(fingerprint_component(
            "index_daily_bars",
            "000001.SH",
            date(20),
            "a-index",
            dt(20, 18, 0),
            dt(20, 18, 6),
        ));
        entries.extend(expected_index_fingerprint_entries().into_iter().skip(1));

        assert_eq!(
            snapshot.input_fingerprint,
            calculate_input_fingerprint(&entries)
        );
        Ok(())
    }

    async fn seed_security(
        pool: &PgPool,
        code: &str,
        complete_adjustments: bool,
        include_status: bool,
    ) -> sqlx::Result<()> {
        seed_security_history(pool, code, 1..=20, complete_adjustments).await?;

        if include_status {
            seed_status(pool, code, dt(20, 17, 20), dt(20, 17, 25)).await?;
        }

        Ok(())
    }

    async fn seed_security_history<I>(
        pool: &PgPool,
        code: &str,
        days: I,
        complete_adjustments: bool,
    ) -> sqlx::Result<()>
    where
        I: IntoIterator<Item = u32>,
    {
        for day in days {
            let close = 10.0 + day as f64;
            sqlx::query(
                r#"INSERT INTO stock_daily_bar_versions
                   (code, trade_date, open, high, low, close, volume, amount, turnover, pe, pb,
                    available_at, availability_quality, source, ingested_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, NULL, $9, 'observed', 'bars', $10)"#,
            )
            .bind(code)
            .bind(date(day))
            .bind(close - 0.2)
            .bind(close + 0.4)
            .bind(close - 0.6)
            .bind(close)
            .bind(1_000_i64)
            .bind(1_000.0_f64)
            .bind(dt(20, 17, 0))
            .bind(dt(20, 17, 5))
            .execute(pool)
            .await?;

            if complete_adjustments || day != 19 {
                sqlx::query(
                    r#"INSERT INTO stock_adjustment_factors
                       (code, trade_date, adj_factor, available_at, availability_quality, source, ingested_at)
                       VALUES ($1, $2, $3, $4, 'observed', 'adjustments', $5)"#,
                )
                .bind(code)
                .bind(date(day))
                .bind(1.0_f64)
                .bind(dt(20, 17, 10))
                .bind(dt(20, 17, 15))
                .execute(pool)
                .await?;
            }
        }

        Ok(())
    }

    async fn allow_duplicate_available_at_versions(pool: &PgPool) -> sqlx::Result<()> {
        for (table, constraint) in [
            ("stock_daily_bar_versions", "stock_daily_bar_versions_pkey"),
            ("stock_adjustment_factors", "stock_adjustment_factors_pkey"),
            ("security_daily_status", "security_daily_status_pkey"),
            ("index_daily_bars", "index_daily_bars_pkey"),
        ] {
            sqlx::query(&format!("ALTER TABLE {table} DROP CONSTRAINT {constraint}"))
                .execute(pool)
                .await?;
        }
        Ok(())
    }

    async fn seed_daily_bar_version(
        pool: &PgPool,
        code: &str,
        day: u32,
        close: f64,
        available_at: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
        source: &str,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO stock_daily_bar_versions
               (code, trade_date, open, high, low, close, volume, amount, turnover, pe, pb,
                available_at, availability_quality, source, ingested_at)
               VALUES ($1, $2, $3, $4, $5, $6, 1000, 1000.0, NULL, NULL, NULL,
                       $7, 'observed', $8, $9)"#,
        )
        .bind(code)
        .bind(date(day))
        .bind(close - 0.2)
        .bind(close + 0.4)
        .bind(close - 0.6)
        .bind(close)
        .bind(available_at)
        .bind(source)
        .bind(ingested_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_adjustment_factor_version(
        pool: &PgPool,
        code: &str,
        day: u32,
        adj_factor: f64,
        available_at: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
        source: &str,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO stock_adjustment_factors
               (code, trade_date, adj_factor, available_at, availability_quality, source, ingested_at)
               VALUES ($1, $2, $3, $4, 'observed', $5, $6)"#,
        )
        .bind(code)
        .bind(date(day))
        .bind(adj_factor)
        .bind(available_at)
        .bind(source)
        .bind(ingested_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_status_version(
        pool: &PgPool,
        code: &str,
        price_limit_pct: f64,
        available_at: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
        source: &str,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO security_daily_status
               (code, trade_date, listed_days, is_st, is_suspended, price_limit_pct,
                available_at, availability_quality, source, ingested_at)
               VALUES ($1, $2, 120, FALSE, FALSE, $3, $4, 'observed', $5, $6)"#,
        )
        .bind(code)
        .bind(date(20))
        .bind(price_limit_pct)
        .bind(available_at)
        .bind(source)
        .bind(ingested_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_index(
        pool: &PgPool,
        code: &str,
        close: f64,
        available_at: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
    ) -> sqlx::Result<()> {
        seed_index_with_source(pool, code, close, available_at, ingested_at, "index").await
    }

    async fn seed_index_with_source(
        pool: &PgPool,
        code: &str,
        close: f64,
        available_at: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
        source: &str,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO index_daily_bars
               (code, trade_date, close, change_pct, volume, amount, available_at,
                availability_quality, source, ingested_at)
               VALUES ($1, $2, $3, 1.0, 1000, 10000.0, $4, 'observed', $5, $6)"#,
        )
        .bind(code)
        .bind(date(20))
        .bind(close)
        .bind(available_at)
        .bind(source)
        .bind(ingested_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_status(
        pool: &PgPool,
        code: &str,
        available_at: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO security_daily_status
               (code, trade_date, listed_days, is_st, is_suspended, price_limit_pct,
                available_at, availability_quality, source, ingested_at)
               VALUES ($1, $2, 120, FALSE, FALSE, 10.0, $3, 'observed', 'status', $4)"#,
        )
        .bind(code)
        .bind(date(20))
        .bind(available_at)
        .bind(ingested_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_all_indices(pool: &PgPool) -> sqlx::Result<()> {
        seed_index_version(pool, "000001.SH", 3000.0, dt(20, 18, 0), dt(20, 18, 5)).await?;
        seed_index_version(pool, "399001.SZ", 12000.0, dt(20, 18, 1), dt(20, 18, 5)).await?;
        seed_index_version(pool, "399006.SZ", 2500.0, dt(20, 18, 2), dt(20, 18, 5)).await?;
        seed_index_version(pool, "000688.SH", 900.0, dt(20, 18, 3), dt(20, 18, 5)).await?;
        Ok(())
    }

    async fn seed_index_version(
        pool: &PgPool,
        code: &str,
        close: f64,
        available_at: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
    ) -> sqlx::Result<()> {
        seed_index(pool, code, close, available_at, ingested_at).await
    }

    fn expected_security_fingerprint_entries(
        code: &str,
        complete_adjustments: bool,
    ) -> Vec<String> {
        let mut entries = Vec::new();
        for day in 1..=20 {
            entries.push(fingerprint_component(
                "stock_daily_bar_versions",
                code,
                date(day),
                "bars",
                dt(20, 17, 0),
                dt(20, 17, 5),
            ));

            if complete_adjustments || day != 19 {
                entries.push(fingerprint_component(
                    "stock_adjustment_factors",
                    code,
                    date(day),
                    "adjustments",
                    dt(20, 17, 10),
                    dt(20, 17, 15),
                ));
            }
        }
        entries.push(fingerprint_component(
            "security_daily_status",
            code,
            date(20),
            "status",
            dt(20, 17, 20),
            dt(20, 17, 25),
        ));
        entries
    }

    fn expected_index_fingerprint_entries() -> Vec<String> {
        vec![
            fingerprint_component(
                "index_daily_bars",
                "000001.SH",
                date(20),
                "index",
                dt(20, 18, 0),
                dt(20, 18, 5),
            ),
            fingerprint_component(
                "index_daily_bars",
                "399001.SZ",
                date(20),
                "index",
                dt(20, 18, 1),
                dt(20, 18, 5),
            ),
            fingerprint_component(
                "index_daily_bars",
                "399006.SZ",
                date(20),
                "index",
                dt(20, 18, 2),
                dt(20, 18, 5),
            ),
            fingerprint_component(
                "index_daily_bars",
                "000688.SH",
                date(20),
                "index",
                dt(20, 18, 3),
                dt(20, 18, 5),
            ),
        ]
    }
}
