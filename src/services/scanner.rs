use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::Result;
use crate::signals::base::StockContext;
use crate::signals::registry::SignalRegistry;
use crate::state::AppState;
use crate::storage::postgres;
use crate::storage::redis_cache::RedisCache;

const BATCH_SIZE: usize = 100;
const MIN_BARS: usize = 60;
const MULTI_SIGNAL_THRESHOLD: usize = 3;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SignalHit {
    pub code: String,
    pub name: String,
    pub signal_id: String,
    pub signal_name: String,
    pub icon: String,
    pub metadata: serde_json::Value,
}

pub struct ScannerService {
    state: Arc<AppState>,
}

impl ScannerService {
    pub fn new(state: Arc<AppState>) -> Self {
        ScannerService { state }
    }

    pub async fn run_full_scan(&self) -> Result<HashMap<String, Vec<SignalHit>>> {
        info!("Starting full stock scan...");
        let run_id = Uuid::new_v4();
        let signals = SignalRegistry::get_enabled();

        if signals.is_empty() {
            warn!("No signals enabled");
            return Ok(HashMap::new());
        }

        let codes = postgres::get_stock_codes_with_data(&self.state.db).await?;
        let total = codes.len();
        info!("Scanning {} stocks with {} signals", total, signals.len());

        let mut results: HashMap<String, Vec<SignalHit>> = HashMap::new();
        for sig in &signals {
            results.insert(sig.signal_id().to_string(), Vec::new());
        }
        results.insert("multi_signal".to_string(), Vec::new());

        // Load stock names
        let names: HashMap<String, String> = {
            let rows: Vec<(String, String)> =
                sqlx::query_as("SELECT code, name FROM stock_info")
                    .fetch_all(&self.state.db)
                    .await
                    .unwrap_or_default();
            rows.into_iter().collect()
        };

        let mut checked = 0usize;
        let mut db_inserts: Vec<(String, String, String, serde_json::Value)> = Vec::new();

        for chunk in codes.chunks(BATCH_SIZE) {
            for code in chunk {
                let bars = match postgres::get_stock_history(&self.state.db, code, 120).await {
                    Ok(b) if b.len() >= MIN_BARS => b,
                    _ => {
                        checked += 1;
                        continue;
                    }
                };

                let name = names.get(code).cloned().unwrap_or_else(|| code.clone());
                let ctx = StockContext { code: code.clone(), name: name.clone() };
                let mut triggered_count = 0usize;

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
                        db_inserts.push((
                            code.clone(),
                            name.clone(),
                            signal.signal_id().to_string(),
                            hit.metadata.clone(),
                        ));
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

                checked += 1;
            }

            tokio::task::yield_now().await;

            if checked % 500 == 0 {
                info!("Scan progress: {}/{}", checked, total);
            }
        }

        // Save to DB in background
        if !db_inserts.is_empty() {
            let db = self.state.db.clone();
            let inserts = db_inserts.clone();
            tokio::spawn(async move {
                if let Err(e) = postgres::save_scan_results(&db, run_id, &inserts).await {
                    warn!("Failed to save scan results: {}", e);
                }
            });
        }

        let total_hits: usize = results.values().map(|v| v.len()).sum();
        info!("Scan complete: {} stocks checked, {} signal hits", checked, total_hits);

        // Cache results
        let json = serde_json::to_value(&results).unwrap_or_default();
        let mut cache = RedisCache::new(self.state.redis.clone());
        let _ = cache.cache_scan_results(&json).await;

        Ok(results)
    }
}
