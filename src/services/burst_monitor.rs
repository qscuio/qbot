use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::data::sina::SinaClient;
use crate::error::Result;
use crate::market_time::is_a_share_trading_now;
use crate::state::AppState;
use crate::storage::redis_cache::RedisCache;
use crate::telegram::pusher::TelegramPusher;

const PRICE_SURGE_PCT: f64 = 3.0;
const POLL_INTERVAL_SECS: u64 = 30;
const OFF_HOURS_POLL_INTERVAL_SECS: u64 = 60;

pub struct BurstMonitorService {
    state: Arc<AppState>,
    sina: Arc<SinaClient>,
    pusher: Arc<TelegramPusher>,
    price_state: HashMap<String, f64>,
    was_market_open: bool,
}

impl BurstMonitorService {
    pub fn new(state: Arc<AppState>, sina: Arc<SinaClient>, pusher: Arc<TelegramPusher>) -> Self {
        BurstMonitorService {
            state,
            sina,
            pusher,
            price_state: HashMap::new(),
            was_market_open: false,
        }
    }

    pub async fn run_poll_loop(&mut self) {
        info!("Burst monitor started");
        loop {
            if !is_a_share_trading_now() {
                if self.was_market_open {
                    self.price_state.clear();
                    self.was_market_open = false;
                    info!("Burst monitor paused outside market hours; state cleared");
                }
                tokio::time::sleep(Duration::from_secs(OFF_HOURS_POLL_INTERVAL_SECS)).await;
                continue;
            }

            if !self.was_market_open {
                self.price_state.clear();
                self.was_market_open = true;
                info!("Burst monitor entered market session; warming baseline");
            }

            if let Err(e) = self.poll_once().await {
                warn!("Burst monitor poll error: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        }
    }

    async fn poll_once(&mut self) -> Result<()> {
        let codes = crate::storage::postgres::get_stock_codes_with_data(&self.state.db).await?;
        if codes.is_empty() {
            return Ok(());
        }

        let code_refs: Vec<&str> = codes.iter().map(|s| s.as_str()).collect();
        let mut cache = RedisCache::new(self.state.redis.clone());
        for batch in code_refs.chunks(100) {
            let quotes = self.sina.get_quotes(batch).await?;

            for (code, quote) in &quotes {
                if let Some(prev_price) = self.price_state.get(code).copied() {
                    if prev_price > 0.0 {
                        let surge_pct = (quote.price - prev_price) / prev_price * 100.0;
                        if surge_pct >= PRICE_SURGE_PCT && !cache.is_burst_alerted(code).await? {
                            let msg = format!(
                                "⚡ 异动提醒\n{} {}\n快速拉升 +{:.1}%\n当前涨幅 {:+.1}%\n现价: {:.2}",
                                code, quote.name, surge_pct, quote.change_pct, quote.price
                            );
                            if let Some(channel) = &self.state.config.stock_alert_channel {
                                let _ = self.pusher.push(channel, &msg).await;
                            }
                            cache.set_burst_alerted(code).await?;
                        }
                    }
                }

                self.price_state.insert(code.clone(), quote.price);
            }
        }
        Ok(())
    }
}
