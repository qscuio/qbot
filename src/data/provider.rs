use async_trait::async_trait;
use chrono::NaiveDate;
use crate::error::Result;
use super::types::*;

#[async_trait]
pub trait DataProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Fetch full A-share stock universe
    async fn get_stock_list(&self) -> Result<Vec<StockInfo>>;

    /// Fetch OHLCV for all stocks on a specific trading date (backfill use)
    async fn get_daily_bars_by_date(&self, trade_date: NaiveDate) -> Result<Vec<(String, Candle)>>;

    /// Fetch OHLCV for a specific stock over a date range (daily update use)
    async fn get_daily_bars_for_stock(
        &self,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Candle>>;

    /// Fetch trading calendar
    async fn get_trading_dates(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>>;

    /// Fetch limit-up/down stocks for a date
    async fn get_limit_up_stocks(&self, trade_date: NaiveDate) -> Result<Vec<LimitUpStock>>;

    /// Fetch index daily bars
    async fn get_index_daily(&self, code: &str, trade_date: NaiveDate) -> Result<Option<IndexData>>;

    /// Fetch sector performance for a date
    async fn get_sector_data(&self, trade_date: NaiveDate) -> Result<Vec<SectorData>>;
}
