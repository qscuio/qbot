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
                Ok(data) if !data.is_empty() => return Ok(data),
                Ok(_) => {
                    warn!(
                        "provider {} returned empty bars for {}, trying fallback",
                        p.name(),
                        trade_date
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
