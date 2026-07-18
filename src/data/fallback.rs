use async_trait::async_trait;
use chrono::NaiveDate;
use std::sync::Arc;
use tracing::warn;

use crate::data::provider::DataProvider;
use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
use crate::error::{AppError, Result};

pub struct FallbackDataProvider {
    providers: Vec<Arc<dyn DataProvider>>,
}

impl FallbackDataProvider {
    pub fn new(providers: Vec<Arc<dyn DataProvider>>) -> Self {
        Self { providers }
    }
}

fn daily_bar_batch_is_usable(bars: &[(String, Candle)]) -> bool {
    !bars.is_empty()
        && bars.iter().all(|(_, bar)| {
            bar.open > 0.0
                && bar.high > 0.0
                && bar.low > 0.0
                && bar.close > 0.0
                && (bar.amount <= 0.0 || bar.volume > 0)
        })
}

#[async_trait]
impl DataProvider for FallbackDataProvider {
    fn name(&self) -> &'static str {
        "fallback"
    }

    async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
        let mut last_err: Option<AppError> = None;
        for p in &self.providers {
            match p.get_stock_list().await {
                Ok(data) if !data.is_empty() => return Ok(data),
                Ok(_) => warn!(
                    "provider {} returned empty stock list, trying fallback",
                    p.name()
                ),
                Err(e) => {
                    warn!("provider {} failed get_stock_list: {}", p.name(), e);
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(vec![])
        }
    }

    async fn get_daily_bars_by_date(&self, trade_date: NaiveDate) -> Result<Vec<(String, Candle)>> {
        let mut last_err: Option<AppError> = None;
        for p in &self.providers {
            match p.get_daily_bars_by_date(trade_date).await {
                Ok(data) if daily_bar_batch_is_usable(&data) => return Ok(data),
                Ok(data) => {
                    warn!(
                        "provider {} returned unusable bars for {} (rows={}), trying fallback",
                        p.name(),
                        trade_date,
                        data.len()
                    )
                }
                Err(e) => {
                    warn!("provider {} failed get_daily_bars_by_date: {}", p.name(), e);
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(vec![])
        }
    }

    async fn get_daily_bars_for_stock(
        &self,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Candle>> {
        let mut last_err: Option<AppError> = None;
        for p in &self.providers {
            match p.get_daily_bars_for_stock(code, start_date, end_date).await {
                Ok(data) if !data.is_empty() => return Ok(data),
                Ok(_) => warn!(
                    "provider {} returned empty bars for {} ({}..{}), trying fallback",
                    p.name(),
                    code,
                    start_date,
                    end_date
                ),
                Err(e) => {
                    warn!(
                        "provider {} failed get_daily_bars_for_stock: {}",
                        p.name(),
                        e
                    );
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(vec![])
        }
    }

    async fn get_trading_dates(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
        let mut last_err: Option<AppError> = None;
        for p in &self.providers {
            match p.get_trading_dates(start, end).await {
                Ok(data) if !data.is_empty() => return Ok(data),
                Ok(_) => warn!(
                    "provider {} returned empty trading dates ({}..{}), trying fallback",
                    p.name(),
                    start,
                    end
                ),
                Err(e) => {
                    warn!("provider {} failed get_trading_dates: {}", p.name(), e);
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(vec![])
        }
    }

    async fn get_limit_up_stocks(&self, trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        let mut last_err: Option<AppError> = None;
        for p in &self.providers {
            match p.get_limit_up_stocks(trade_date).await {
                Ok(data) if !data.is_empty() => return Ok(data),
                Ok(_) => warn!(
                    "provider {} returned empty limit-up data for {}, trying fallback",
                    p.name(),
                    trade_date
                ),
                Err(e) => {
                    warn!("provider {} failed get_limit_up_stocks: {}", p.name(), e);
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(vec![])
        }
    }

    async fn get_index_daily(
        &self,
        code: &str,
        trade_date: NaiveDate,
    ) -> Result<Option<IndexData>> {
        let mut last_err: Option<AppError> = None;
        for p in &self.providers {
            match p.get_index_daily(code, trade_date).await {
                Ok(Some(v)) => return Ok(Some(v)),
                Ok(None) => warn!(
                    "provider {} returned no index data for {} on {}, trying fallback",
                    p.name(),
                    code,
                    trade_date
                ),
                Err(e) => {
                    warn!("provider {} failed get_index_daily: {}", p.name(), e);
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(None)
        }
    }

    async fn get_sector_data(&self, trade_date: NaiveDate) -> Result<Vec<SectorData>> {
        let mut last_err: Option<AppError> = None;
        for p in &self.providers {
            match p.get_sector_data(trade_date).await {
                Ok(data) if !data.is_empty() => return Ok(data),
                Ok(_) => warn!(
                    "provider {} returned empty sector data for {}, trying fallback",
                    p.name(),
                    trade_date
                ),
                Err(e) => {
                    warn!("provider {} failed get_sector_data: {}", p.name(), e);
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(volume: i64, amount: f64) -> Candle {
        Candle {
            trade_date: NaiveDate::from_ymd_opt(2026, 7, 17).unwrap(),
            open: 10.0,
            high: 11.0,
            low: 9.0,
            close: 10.5,
            volume,
            amount,
            turnover: None,
            pe: None,
            pb: None,
        }
    }

    #[test]
    fn daily_bar_batch_rejects_traded_rows_with_missing_volume() {
        let bars = vec![("600000.SH".to_string(), candle(0, 10_000.0))];
        assert!(!daily_bar_batch_is_usable(&bars));
    }

    #[test]
    fn daily_bar_batch_accepts_suspended_rows_beside_valid_trades() {
        let bars = vec![
            ("600000.SH".to_string(), candle(1_000, 10_000.0)),
            ("600001.SH".to_string(), candle(0, 0.0)),
        ];
        assert!(daily_bar_batch_is_usable(&bars));
    }
}
