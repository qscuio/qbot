use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::data::sina::SinaClient;
use crate::error::Result;
use crate::market_time::is_a_share_trading_now;
use crate::state::AppState;
use crate::telegram::pusher::TelegramPusher;

const PRICE_BURST_PCT: f64 = 3.0;
const POLL_INTERVAL_SECS: u64 = 30;
const OFF_HOURS_POLL_INTERVAL_SECS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq)]
enum BurstDirection {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct BurstEvent {
    direction: BurstDirection,
    move_pct: f64,
}

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
        for batch in code_refs.chunks(100) {
            let quotes = self.sina.get_quotes(batch).await?;

            for (code, quote) in &quotes {
                if let Some(prev_price) = self.price_state.get(code).copied() {
                    if let Some(event) = detect_burst_event(prev_price, quote.price) {
                        let msg = format_burst_message(
                            code,
                            &quote.name,
                            event,
                            quote.change_pct,
                            quote.price,
                        );
                        if let Some(channel) = &self.state.config.stock_alert_channel {
                            let _ = self.pusher.push(channel, &msg).await;
                        }
                    }
                }

                self.price_state.insert(code.clone(), quote.price);
            }
        }
        Ok(())
    }
}

fn detect_burst_event(prev_price: f64, current_price: f64) -> Option<BurstEvent> {
    if prev_price <= 0.0 || current_price <= 0.0 {
        return None;
    }

    let move_pct = (current_price - prev_price) / prev_price * 100.0;
    if move_pct >= PRICE_BURST_PCT {
        Some(BurstEvent {
            direction: BurstDirection::Up,
            move_pct,
        })
    } else if move_pct <= -PRICE_BURST_PCT {
        Some(BurstEvent {
            direction: BurstDirection::Down,
            move_pct,
        })
    } else {
        None
    }
}

fn format_burst_message(
    code: &str,
    name: &str,
    event: BurstEvent,
    change_pct: f64,
    price: f64,
) -> String {
    let action = match event.direction {
        BurstDirection::Up => "快速拉升",
        BurstDirection::Down => "快速下跌",
    };
    format!(
        "⚡ 异动提醒\n{} {}\n{} {:+.1}%\n当前涨幅 {:+.1}%\n现价: {:.2}",
        code, name, action, event.move_pct, change_pct, price
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_burst_event_reports_upward_move() {
        let event = detect_burst_event(10.0, 10.4).expect("expected upward burst");
        assert_eq!(event.direction, BurstDirection::Up);
        assert!((event.move_pct - 4.0).abs() < 1e-6);
    }

    #[test]
    fn detect_burst_event_reports_downward_move() {
        let event = detect_burst_event(10.0, 9.6).expect("expected downward burst");
        assert_eq!(event.direction, BurstDirection::Down);
        assert!((event.move_pct + 4.0).abs() < 1e-6);
    }

    #[test]
    fn detect_burst_event_ignores_flat_and_small_moves() {
        assert_eq!(detect_burst_event(10.0, 10.0), None);
        assert_eq!(detect_burst_event(10.0, 10.2), None);
        assert_eq!(detect_burst_event(10.0, 9.8), None);
    }

    #[test]
    fn format_burst_message_uses_downward_copy() {
        let msg = format_burst_message(
            "000001.SZ",
            "平安银行",
            BurstEvent {
                direction: BurstDirection::Down,
                move_pct: -3.5,
            },
            -1.2,
            9.65,
        );

        assert!(msg.contains("快速下跌 -3.5%"));
        assert!(msg.contains("当前涨幅 -1.2%"));
    }
}
