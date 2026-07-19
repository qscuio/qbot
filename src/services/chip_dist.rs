use chrono::{Datelike, NaiveDate, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::error::Result;
use crate::market_time::{beijing_now, beijing_today};
use crate::state::AppState;
use crate::storage::postgres;

const DEFAULT_LOOKBACK_DAYS: i64 = 120;
const NUM_BUCKETS: usize = 30;

#[derive(Debug, Clone)]
struct BarPoint {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    turnover_rate: f64, // decimal, e.g. 0.03
}

#[cfg(test)]
mod coexistence_tests {
    use chrono::NaiveDate;
    use sqlx::PgPool;

    use super::{
        load_cached_from_pool, save_chip_distribution_to_pool, ChipBucket, ChipDistributionResult,
    };
    use crate::data::chip::{ChipBucket as CanonicalBucket, ChipSnapshot};
    use crate::storage::chip_repository::ChipRepository;

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 7, 17).unwrap()
    }

    fn legacy_result() -> ChipDistributionResult {
        ChipDistributionResult {
            code: "600519.SH".to_string(),
            date: date().to_string(),
            current_price: 1_550.0,
            distribution: vec![ChipBucket {
                price: 1_500.0,
                percentage: 100.0,
                is_profit: true,
            }],
            avg_cost: 1_500.0,
            profit_ratio: 100.0,
            concentration: 100.0,
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn legacy_dashboard_reads_normalized_canonical_rows(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool.clone());
        repo.upsert_snapshot(&ChipSnapshot {
            code: "600519.SH".to_string(),
            trade_date: date(),
            distribution: vec![
                CanonicalBucket {
                    price: 1_500.0,
                    weight: 0.25,
                },
                CanonicalBucket {
                    price: 1_600.0,
                    weight: 0.75,
                },
            ],
            average_cost: 1_575.0,
            winner_rate: 25.0,
            concentration: 75.0,
            dominant_peak_price: 1_600.0,
            source: "qbot_estimate".to_string(),
            model_version: Some("qbot-chip-v2".to_string()),
            validated: false,
            source_updated_at: chrono::Utc::now(),
        })
        .await?;

        let loaded = load_cached_from_pool(&pool, "600519.SH", date(), 1_550.0)
            .await?
            .expect("normalized row remains dashboard-readable");
        assert_eq!(loaded.distribution.len(), 2);
        assert!((loaded.distribution[0].percentage - 100.0 / 3.0).abs() < 1e-9);
        assert!(loaded.distribution[0].is_profit);
        assert_eq!(loaded.distribution[1].percentage, 100.0);
        assert!(!loaded.distribution[1].is_profit);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn legacy_updater_cannot_overwrite_normalized_rows_and_marks_legacy_inserts(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool.clone());
        let canonical = ChipSnapshot {
            code: "600519.SH".to_string(),
            trade_date: date(),
            distribution: vec![CanonicalBucket {
                price: 1_500.0,
                weight: 1.0,
            }],
            average_cost: 1_500.0,
            winner_rate: 50.0,
            concentration: 50.0,
            dominant_peak_price: 1_500.0,
            source: "qbot_estimate".to_string(),
            model_version: Some("qbot-chip-v2".to_string()),
            validated: true,
            source_updated_at: chrono::Utc::now(),
        };
        repo.upsert_snapshot(&canonical).await?;
        let before = repo.latest_snapshot("600519.SH").await?.unwrap();
        assert!(!save_chip_distribution_to_pool(&pool, &legacy_result(), date()).await?);
        assert_eq!(repo.latest_snapshot("600519.SH").await?.unwrap(), before);

        let mut other = legacy_result();
        other.code = "000001.SZ".to_string();
        assert!(save_chip_distribution_to_pool(&pool, &other, date()).await?);
        let provenance: (String, Option<String>, bool, String) = sqlx::query_as(
            "SELECT source, model_version, validated, distribution_format FROM chip_distribution WHERE code = '000001.SZ' AND trade_date = $1",
        )
        .bind(date())
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            provenance,
            (
                "legacy".to_string(),
                None,
                false,
                "legacy_peak_relative".to_string()
            )
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChipBucket {
    pub price: f64,
    pub percentage: f64,
    #[serde(rename = "isProfit")]
    pub is_profit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChipDistributionResult {
    pub code: String,
    pub date: String,
    #[serde(rename = "currentPrice")]
    pub current_price: f64,
    pub distribution: Vec<ChipBucket>,
    #[serde(rename = "avgCost")]
    pub avg_cost: f64,
    #[serde(rename = "profitRatio")]
    pub profit_ratio: f64,
    pub concentration: f64,
}

pub struct ChipDistService {
    state: Arc<AppState>,
    update_lock: Arc<Mutex<()>>,
}

impl ChipDistService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self {
            state,
            update_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn get_chip_distribution(
        &self,
        raw_code: &str,
        target_date: Option<NaiveDate>,
    ) -> Result<Option<ChipDistributionResult>> {
        let target_date = target_date.unwrap_or_else(beijing_today);
        let code = postgres::resolve_stock_code(&self.state.db, raw_code)
            .await?
            .unwrap_or_else(|| raw_code.trim().to_uppercase());

        if code.is_empty() {
            return Ok(None);
        }

        if let Some(cached) = self.load_cached(&code, target_date).await? {
            return Ok(Some(cached));
        }

        let calculated = match self.calculate_chip_distribution(&code, target_date).await? {
            Some(v) => v,
            None => return Ok(None),
        };

        self.save_chip_distribution(&calculated, target_date)
            .await?;
        Ok(Some(calculated))
    }

    async fn load_cached(
        &self,
        code: &str,
        target_date: NaiveDate,
    ) -> Result<Option<ChipDistributionResult>> {
        let current_price = self.latest_close(code, target_date).await?.unwrap_or(0.0);
        load_cached_from_pool(&self.state.db, code, target_date, current_price).await
    }

    async fn save_chip_distribution(
        &self,
        data: &ChipDistributionResult,
        target_date: NaiveDate,
    ) -> Result<()> {
        save_chip_distribution_to_pool(&self.state.db, data, target_date).await?;
        Ok(())
    }

    async fn latest_close(&self, code: &str, target_date: NaiveDate) -> Result<Option<f64>> {
        let row: Option<(Option<f64>,)> = sqlx::query_as(
            r#"SELECT close::float8
               FROM stock_daily_bars
               WHERE code = $1 AND trade_date <= $2
               ORDER BY trade_date DESC
               LIMIT 1"#,
        )
        .bind(code)
        .bind(target_date)
        .fetch_optional(&self.state.db)
        .await?;

        Ok(row.and_then(|r| r.0))
    }

    async fn calculate_chip_distribution(
        &self,
        code: &str,
        target_date: NaiveDate,
    ) -> Result<Option<ChipDistributionResult>> {
        let rows: Vec<(
            NaiveDate,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<i64>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT trade_date,
                      open::float8,
                      high::float8,
                      low::float8,
                      close::float8,
                      volume,
                      turnover::float8
               FROM stock_daily_bars
               WHERE code = $1 AND trade_date <= $2
               ORDER BY trade_date DESC
               LIMIT $3"#,
        )
        .bind(code)
        .bind(target_date)
        .bind(DEFAULT_LOOKBACK_DAYS)
        .fetch_all(&self.state.db)
        .await?;

        if rows.len() < 10 {
            return Ok(None);
        }

        let mut data: Vec<BarPoint> = rows
            .into_iter()
            .rev()
            .map(|(_, open, high, low, close, volume, turnover)| BarPoint {
                open: open.unwrap_or(0.0),
                high: high.unwrap_or(0.0),
                low: low.unwrap_or(0.0),
                close: close.unwrap_or(0.0),
                volume: volume.unwrap_or(0) as f64,
                turnover_rate: (turnover.unwrap_or(0.0) / 100.0).clamp(0.0, 0.95),
            })
            .collect();

        data.retain(|d| d.close > 0.0 && d.high >= d.low);
        if data.len() < 10 {
            return Ok(None);
        }

        let current_price = data.last().map(|d| d.close).unwrap_or(0.0);
        let min_price_raw = data
            .iter()
            .map(|d| d.low)
            .fold(f64::INFINITY, |a, b| a.min(b));
        let max_price_raw = data
            .iter()
            .map(|d| d.high)
            .fold(f64::NEG_INFINITY, |a, b| a.max(b));

        if !min_price_raw.is_finite() || !max_price_raw.is_finite() || max_price_raw <= 0.0 {
            return Ok(None);
        }

        let spread = (max_price_raw - min_price_raw).max(0.01);
        let min_price = (min_price_raw - spread * 0.1).max(0.01);
        let max_price = max_price_raw + spread * 0.1;
        let bucket_size = ((max_price - min_price) / NUM_BUCKETS as f64).max(0.01);

        let mut chips = vec![0.0f64; NUM_BUCKETS];

        for point in data {
            if point.volume <= 0.0 {
                continue;
            }

            let decay_factor = (1.0 - point.turnover_rate).clamp(0.0, 1.0);
            for chip in &mut chips {
                *chip *= decay_factor;
            }

            let body_low = point.open.min(point.close);
            let body_high = point.open.max(point.close);

            let body_volume = point.volume * 0.6;
            let lower_shadow_volume = point.volume * 0.2;
            let upper_shadow_volume = point.volume * 0.2;

            for (idx, slot) in chips.iter_mut().enumerate() {
                let bucket_low = min_price + idx as f64 * bucket_size;
                let bucket_high = bucket_low + bucket_size;
                let bucket_mid = (bucket_low + bucket_high) / 2.0;

                let mut added = 0.0;

                if bucket_mid >= body_low && bucket_mid <= body_high {
                    let body_range = (body_high - body_low).max(bucket_size);
                    added += body_volume / (body_range / bucket_size);
                }

                if bucket_mid >= point.low && bucket_mid < body_low {
                    let lower_range = (body_low - point.low).max(bucket_size);
                    added += lower_shadow_volume / (lower_range / bucket_size);
                }

                if bucket_mid > body_high && bucket_mid <= point.high {
                    let upper_range = (point.high - body_high).max(bucket_size);
                    added += upper_shadow_volume / (upper_range / bucket_size);
                }

                *slot += added;
            }
        }

        let max_chip = chips.iter().copied().fold(0.0f64, f64::max);
        if max_chip <= 0.0 {
            return Ok(None);
        }

        let mut distribution = Vec::with_capacity(NUM_BUCKETS);
        let mut profit_volume = 0.0f64;
        let mut loss_volume = 0.0f64;
        let mut weighted_sum = 0.0f64;
        let mut total_chips = 0.0f64;

        for (idx, chip_value) in chips.iter().copied().enumerate() {
            let price = min_price + (idx as f64 + 0.5) * bucket_size;
            let percentage = (chip_value / max_chip * 100.0).max(0.0);
            let is_profit = price <= current_price;

            if chip_value > 0.0 {
                if is_profit {
                    profit_volume += chip_value;
                } else {
                    loss_volume += chip_value;
                }
                weighted_sum += price * chip_value;
                total_chips += chip_value;
            }

            distribution.push(ChipBucket {
                price: (price * 100.0).round() / 100.0,
                percentage: (percentage * 10.0).round() / 10.0,
                is_profit,
            });
        }

        distribution.reverse(); // high -> low, consistent with existing mini-app behavior

        let avg_cost = if total_chips > 0.0 {
            weighted_sum / total_chips
        } else {
            current_price
        };

        let total_profit_loss = profit_volume + loss_volume;
        let profit_ratio = if total_profit_loss > 0.0 {
            (profit_volume / total_profit_loss) * 100.0
        } else {
            0.0
        };

        let total_chip_sum: f64 = chips.iter().sum();
        let mut sorted = chips;
        sorted.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        let top_sum: f64 = sorted.into_iter().take(5).sum();
        let concentration = if total_chip_sum > 0.0 {
            (top_sum / total_chip_sum * 100.0).clamp(0.0, 100.0)
        } else {
            0.0
        };

        Ok(Some(ChipDistributionResult {
            code: code.to_string(),
            date: target_date.to_string(),
            current_price,
            distribution,
            avg_cost: (avg_cost * 100.0).round() / 100.0,
            profit_ratio: (profit_ratio * 10.0).round() / 10.0,
            concentration: (concentration * 10.0).round() / 10.0,
        }))
    }

    pub async fn update_all_stocks(&self, target_date: Option<NaiveDate>) -> Result<usize> {
        let _guard = match self.update_lock.try_lock() {
            Ok(g) => g,
            Err(_) => {
                info!("chip distribution update is already running; skipping");
                return Ok(0);
            }
        };

        let date = target_date.unwrap_or_else(beijing_today);
        let codes: Vec<(String,)> = sqlx::query_as(
            r#"SELECT DISTINCT code
               FROM stock_daily_bars
               WHERE trade_date <= $1
               ORDER BY code"#,
        )
        .bind(date)
        .fetch_all(&self.state.db)
        .await?;

        let mut updated = 0usize;
        for (idx, (code,)) in codes.iter().enumerate() {
            match self.calculate_chip_distribution(code, date).await? {
                Some(dist) => {
                    self.save_chip_distribution(&dist, date).await?;
                    updated += 1;
                }
                None => {}
            }

            if idx > 0 && idx % 200 == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }

        info!(
            "chip distribution update finished for {}: {}/{}",
            date,
            updated,
            codes.len()
        );
        Ok(updated)
    }

    pub async fn run_daily_update_loop(&self) {
        let mut triggered_today: HashSet<String> = HashSet::new();

        loop {
            let now = beijing_now();
            let date_key = now.format("%Y-%m-%d").to_string();

            if now.hour() == 0 && now.minute() == 0 {
                triggered_today.clear();
            }

            if now.weekday().number_from_monday() <= 5
                && now.hour() == 15
                && now.minute() == 30
                && !triggered_today.contains(&date_key)
            {
                triggered_today.insert(date_key);
                if let Err(e) = self.update_all_stocks(Some(beijing_today())).await {
                    warn!("chip daily update failed: {}", e);
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }
}

async fn load_cached_from_pool(
    pool: &PgPool,
    code: &str,
    target_date: NaiveDate,
    current_price: f64,
) -> Result<Option<ChipDistributionResult>> {
    let row: Option<(Value, String, Option<f64>, Option<f64>, Option<f64>)> = sqlx::query_as(
        r#"SELECT distribution, distribution_format,
                  avg_cost::float8, profit_ratio::float8, concentration::float8
           FROM chip_distribution
           WHERE code = $1 AND trade_date = $2"#,
    )
    .bind(code)
    .bind(target_date)
    .fetch_optional(pool)
    .await?;
    let Some((distribution, format, avg_cost, profit_ratio, concentration)) = row else {
        return Ok(None);
    };

    let distribution = match format.as_str() {
        "legacy_peak_relative" => serde_json::from_value::<Vec<ChipBucket>>(distribution)?
            .into_iter()
            .map(|bucket| ChipBucket {
                is_profit: bucket.price <= current_price,
                ..bucket
            })
            .collect(),
        "normalized_probability" => {
            let buckets =
                serde_json::from_value::<Vec<crate::data::chip::ChipBucket>>(distribution)?;
            let max_weight = buckets
                .iter()
                .map(|bucket| bucket.weight)
                .fold(0.0_f64, f64::max);
            buckets
                .into_iter()
                .map(|bucket| ChipBucket {
                    price: bucket.price,
                    percentage: if max_weight > 0.0 {
                        bucket.weight / max_weight * 100.0
                    } else {
                        0.0
                    },
                    is_profit: bucket.price <= current_price,
                })
                .collect()
        }
        other => {
            return Err(crate::error::AppError::Internal(format!(
                "unknown chip distribution format: {other}"
            )))
        }
    };

    Ok(Some(ChipDistributionResult {
        code: code.to_string(),
        date: target_date.to_string(),
        current_price,
        distribution,
        avg_cost: avg_cost.unwrap_or(0.0),
        profit_ratio: profit_ratio.unwrap_or(0.0),
        concentration: concentration.unwrap_or(0.0),
    }))
}

async fn save_chip_distribution_to_pool(
    pool: &PgPool,
    data: &ChipDistributionResult,
    target_date: NaiveDate,
) -> Result<bool> {
    let result = sqlx::query(
        r#"INSERT INTO chip_distribution
               (code, trade_date, distribution, avg_cost, profit_ratio, concentration,
                source, model_version, validated, distribution_format,
                source_updated_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6,
                       'legacy', NULL, FALSE, 'legacy_peak_relative', NOW(), NOW())
               ON CONFLICT (code, trade_date) DO UPDATE SET
                 distribution = EXCLUDED.distribution,
                 avg_cost = EXCLUDED.avg_cost,
                 profit_ratio = EXCLUDED.profit_ratio,
                 concentration = EXCLUDED.concentration,
                 dominant_peak_price = NULL,
                 source = 'legacy',
                 model_version = NULL,
                 validated = FALSE,
                 distribution_format = 'legacy_peak_relative',
                 source_updated_at = NOW(),
                 updated_at = NOW()
               WHERE chip_distribution.distribution_format = 'legacy_peak_relative'"#,
    )
    .bind(&data.code)
    .bind(target_date)
    .bind(serde_json::to_value(&data.distribution)?)
    .bind(data.avg_cost)
    .bind(data.profit_ratio)
    .bind(data.concentration)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}
