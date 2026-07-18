use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::Result;
use crate::services::scan_ranker::{empty_ranked_pool_map, rank_scan_inputs, RankInput};
use crate::signals::base::StockContext;
use crate::signals::registry::SignalRegistry;
use crate::state::AppState;
use crate::storage::dashboard_repository::DashboardRepository;
use crate::storage::postgres;
use crate::storage::redis_cache::RedisCache;

const BATCH_SIZE: usize = 100;
const MIN_BARS: usize = 60;
const MULTI_SIGNAL_THRESHOLD: usize = 3;

fn short_code(code: &str) -> String {
    code.split('.').next().unwrap_or(code).to_ascii_uppercase()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SignalHit {
    pub code: String,
    pub name: String,
    pub signal_id: String,
    pub signal_name: String,
    pub icon: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DailySignalArchiveSummary {
    pub scan_date: NaiveDate,
    pub run_id: Uuid,
    pub rows: usize,
    pub codes: usize,
    pub signals: usize,
}

pub struct ScannerService {
    state: Arc<AppState>,
}

fn flatten_scan_results_for_storage(
    results: &HashMap<String, Vec<SignalHit>>,
) -> Vec<(String, String, String, serde_json::Value)> {
    let mut rows: Vec<(String, String, String, serde_json::Value)> = results
        .values()
        .flat_map(|hits| {
            hits.iter().map(|hit| {
                (
                    hit.code.clone(),
                    hit.name.clone(),
                    hit.signal_id.clone(),
                    hit.metadata.clone(),
                )
            })
        })
        .collect();
    rows.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)));
    rows
}

fn flatten_scan_results_for_daily_archive(
    results: &HashMap<String, Vec<SignalHit>>,
) -> Vec<postgres::DailySignalScanRow> {
    let mut rows: Vec<postgres::DailySignalScanRow> = results
        .values()
        .flat_map(|hits| {
            hits.iter().map(|hit| postgres::DailySignalScanRow {
                code: hit.code.clone(),
                name: hit.name.clone(),
                signal_id: hit.signal_id.clone(),
                signal_name: hit.signal_name.clone(),
                icon: hit.icon.clone(),
                metadata: hit.metadata.clone(),
            })
        })
        .collect();
    rows.sort_by(|a, b| {
        a.signal_id
            .cmp(&b.signal_id)
            .then_with(|| a.code.cmp(&b.code))
    });
    rows
}

impl ScannerService {
    pub fn new(state: Arc<AppState>) -> Self {
        ScannerService { state }
    }

    async fn collect_scan_results(&self) -> Result<(HashMap<String, Vec<SignalHit>>, usize)> {
        let signals = SignalRegistry::get_enabled();

        if signals.is_empty() {
            warn!("No signals enabled");
            return Ok((HashMap::new(), 0));
        }

        let codes = postgres::get_stock_codes_with_data(&self.state.db).await?;
        let total = codes.len();
        info!("Scanning {} stocks with {} signals", total, signals.len());

        let mut results: HashMap<String, Vec<SignalHit>> = HashMap::new();
        for sig in &signals {
            results.insert(sig.signal_id().to_string(), Vec::new());
        }
        results.insert("multi_signal".to_string(), Vec::new());
        results.extend(empty_ranked_pool_map());

        // Load stock names (support both "600519" and "600519.SH" style keys)
        let (names_exact, names_short): (HashMap<String, String>, HashMap<String, String>) = {
            let rows: Vec<(String, String)> = sqlx::query_as("SELECT code, name FROM stock_info")
                .fetch_all(&self.state.db)
                .await?;
            let mut exact = HashMap::new();
            let mut short = HashMap::new();
            for (code, name) in rows {
                exact.insert(code.clone(), name.clone());
                short.entry(short_code(&code)).or_insert(name);
            }
            (exact, short)
        };

        let mut checked = 0usize;
        let mut rank_inputs: Vec<RankInput> = Vec::new();

        for chunk in codes.chunks(BATCH_SIZE) {
            for code in chunk {
                let bars = match postgres::get_stock_history(&self.state.db, code, 120).await {
                    Ok(b) if b.len() >= MIN_BARS => b,
                    _ => {
                        checked += 1;
                        continue;
                    }
                };

                let name = names_exact
                    .get(code)
                    .cloned()
                    .or_else(|| names_short.get(&short_code(code)).cloned())
                    .unwrap_or_else(|| code.clone());
                let ctx = StockContext {
                    code: code.clone(),
                    name: name.clone(),
                };
                let mut triggered_count = 0usize;
                let mut stock_hits: Vec<SignalHit> = Vec::new();

                for signal in &signals {
                    if bars.len() < signal.min_bars() {
                        continue;
                    }
                    let result = signal.detect(&bars, &ctx);
                    if result.triggered {
                        let hit = SignalHit {
                            code: code.clone(),
                            name: name.clone(),
                            signal_id: signal.signal_id().to_string(),
                            signal_name: signal.display_name().to_string(),
                            icon: signal.icon().to_string(),
                            metadata: serde_json::to_value(&result.metadata).unwrap_or_default(),
                        };
                        results
                            .entry(signal.signal_id().to_string())
                            .or_default()
                            .push(hit.clone());
                        stock_hits.push(hit);
                        if signal.count_in_multi() {
                            triggered_count += 1;
                        }
                    }
                }

                if triggered_count >= MULTI_SIGNAL_THRESHOLD {
                    results
                        .entry("multi_signal".to_string())
                        .or_default()
                        .push(SignalHit {
                            code: code.clone(),
                            name: name.clone(),
                            signal_id: "multi_signal".to_string(),
                            signal_name: format!("多信号({triggered_count})"),
                            icon: "⭐".to_string(),
                            metadata: serde_json::json!({"count": triggered_count}),
                        });
                }

                rank_inputs.push(RankInput {
                    code: code.clone(),
                    name: name.clone(),
                    bars,
                    hits: stock_hits,
                });

                checked += 1;
            }

            tokio::task::yield_now().await;

            if checked.is_multiple_of(500) {
                info!("Scan progress: {}/{}", checked, total);
            }
        }

        for (pool_id, hits) in rank_scan_inputs(&rank_inputs) {
            results.insert(pool_id, hits);
        }

        let total_hits: usize = results.values().map(|v| v.len()).sum();
        info!(
            "Scan complete: {} stocks checked, {} signal hits",
            checked, total_hits
        );

        Ok((results, checked))
    }

    pub async fn run_full_scan(&self) -> Result<HashMap<String, Vec<SignalHit>>> {
        info!("Starting full stock scan...");
        let run_id = Uuid::new_v4();
        let repo = DashboardRepository::new(self.state.db.clone());
        repo.start_scan_run(run_id).await?;
        let (results, stocks_checked) = match self.collect_scan_results().await {
            Ok(value) => value,
            Err(error) => {
                let _ = repo.fail_scan_run(run_id, &error.to_string()).await;
                return Err(error);
            }
        };

        let db_inserts = flatten_scan_results_for_storage(&results);
        if !db_inserts.is_empty() {
            if let Err(error) =
                postgres::save_scan_results(&self.state.db, run_id, &db_inserts).await
            {
                let _ = repo.fail_scan_run(run_id, &error.to_string()).await;
                return Err(error);
            }
        }

        // Cache results
        let json = serde_json::to_value(&results).unwrap_or_default();
        let mut cache = RedisCache::new(self.state.redis.clone());
        let _ = cache.cache_scan_results(&json).await;

        repo.complete_scan_run(run_id, stocks_checked, db_inserts.len())
            .await?;

        Ok(results)
    }

    pub async fn run_daily_archive_scan(
        &self,
        scan_date: NaiveDate,
    ) -> Result<DailySignalArchiveSummary> {
        info!("Starting daily signal archive scan for {}", scan_date);
        let run_id = Uuid::new_v4();
        let (results, _) = self.collect_scan_results().await?;
        let rows = flatten_scan_results_for_daily_archive(&results);
        let saved =
            postgres::save_daily_signal_scan_results(&self.state.db, scan_date, run_id, &rows)
                .await?;

        let codes = rows
            .iter()
            .map(|row| row.code.as_str())
            .collect::<HashSet<_>>()
            .len();
        let signals = rows
            .iter()
            .map(|row| row.signal_id.as_str())
            .collect::<HashSet<_>>()
            .len();

        info!(
            "Daily signal archive complete: date={}, rows={}, codes={}, signals={}, run_id={}",
            scan_date, saved, codes, signals, run_id
        );

        Ok(DailySignalArchiveSummary {
            scan_date,
            run_id,
            rows: saved,
            codes,
            signals,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_scan_results_for_storage_includes_ranked_pools_and_multi_signal() {
        let mut results = HashMap::new();
        results.insert(
            "startup".to_string(),
            vec![SignalHit {
                code: "600000.SH".to_string(),
                name: "浦发银行".to_string(),
                signal_id: "startup".to_string(),
                signal_name: "底部快速启动".to_string(),
                icon: "🚀".to_string(),
                metadata: serde_json::json!({"source": "raw"}),
            }],
        );
        results.insert(
            "pool_short_a".to_string(),
            vec![SignalHit {
                code: "300001.SZ".to_string(),
                name: "特锐德".to_string(),
                signal_id: "pool_short_a".to_string(),
                signal_name: "短线A档".to_string(),
                icon: "🔥".to_string(),
                metadata: serde_json::json!({"score": 88.5}),
            }],
        );
        results.insert(
            "multi_signal".to_string(),
            vec![SignalHit {
                code: "002594.SZ".to_string(),
                name: "比亚迪".to_string(),
                signal_id: "multi_signal".to_string(),
                signal_name: "多信号(3)".to_string(),
                icon: "⭐".to_string(),
                metadata: serde_json::json!({"count": 3}),
            }],
        );
        results.insert("pool_mid_b".to_string(), Vec::new());

        let rows = flatten_scan_results_for_storage(&results);
        let signal_ids: Vec<&str> = rows.iter().map(|row| row.2.as_str()).collect();

        assert_eq!(rows.len(), 3);
        assert!(signal_ids.contains(&"startup"));
        assert!(signal_ids.contains(&"pool_short_a"));
        assert!(signal_ids.contains(&"multi_signal"));
    }

    #[test]
    fn flatten_scan_results_for_daily_archive_preserves_display_fields() {
        let mut results = HashMap::new();
        results.insert(
            "startup".to_string(),
            vec![SignalHit {
                code: "600000.SH".to_string(),
                name: "浦发银行".to_string(),
                signal_id: "startup".to_string(),
                signal_name: "底部快速启动".to_string(),
                icon: "🚀".to_string(),
                metadata: serde_json::json!({"source": "raw"}),
            }],
        );
        results.insert(
            "multi_signal".to_string(),
            vec![SignalHit {
                code: "002594.SZ".to_string(),
                name: "比亚迪".to_string(),
                signal_id: "multi_signal".to_string(),
                signal_name: "多信号(3)".to_string(),
                icon: "⭐".to_string(),
                metadata: serde_json::json!({"count": 3}),
            }],
        );

        let rows = flatten_scan_results_for_daily_archive(&results);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].signal_id, "multi_signal");
        assert_eq!(rows[0].signal_name, "多信号(3)");
        assert_eq!(rows[0].icon, "⭐");
        assert_eq!(rows[1].signal_id, "startup");
        assert_eq!(rows[1].signal_name, "底部快速启动");
        assert_eq!(rows[1].icon, "🚀");
    }
}
