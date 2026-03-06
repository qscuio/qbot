use chrono::Duration;
use std::sync::Arc;
use tracing::{info, warn};

use crate::data::provider::DataProvider;
use crate::error::Result;
use crate::market_time::beijing_today;
use crate::state::AppState;
use crate::storage::{postgres, upsert_stock_info};

pub struct StockHistoryService {
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
}

impl StockHistoryService {
    pub fn new(state: Arc<AppState>, provider: Arc<dyn DataProvider>) -> Self {
        StockHistoryService { state, provider }
    }

    /// Full backfill: fetch all trading dates in last N years, date-by-date
    pub async fn backfill(&self, years: u32) -> Result<()> {
        let end = beijing_today();
        let start = end - Duration::days(years as i64 * 365);
        info!("Starting backfill {} to {}", start, end);

        let dates = self.provider.get_trading_dates(start, end).await?;
        info!("{} trading days to backfill", dates.len());

        for (i, date) in dates.iter().enumerate() {
            match self.provider.get_daily_bars_by_date(*date).await {
                Ok(bars) => {
                    let count = bars.len();
                    postgres::upsert_daily_bars(&self.state.db, &bars).await?;
                    if i % 50 == 0 {
                        info!(
                            "Backfill progress: {}/{} ({}, {} bars)",
                            i + 1,
                            dates.len(),
                            date,
                            count
                        );
                    }
                }
                Err(e) => {
                    warn!("Failed to fetch {}: {}", date, e);
                }
            }
            // Rate limit: ~200ms between calls
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        info!("Backfill complete");
        Ok(())
    }

    /// Daily incremental update: fetch today's bars for all known stocks
    pub async fn update_today(&self) -> Result<()> {
        let today = beijing_today();
        info!("Daily update for {}", today);

        let bars = self.provider.get_daily_bars_by_date(today).await?;
        let count = bars.len();
        postgres::upsert_daily_bars(&self.state.db, &bars).await?;
        info!("Daily update: {} bars saved for {}", count, today);

        // Also refresh stock info
        let stocks = self.provider.get_stock_list().await?;
        upsert_stock_info(&self.state.db, &stocks).await?;
        info!("Stock info refreshed: {} stocks", stocks.len());

        Ok(())
    }

    /// Check if the history table already has any data.
    pub async fn has_any_data(&self) -> bool {
        let result: Result<(bool,)> =
            sqlx::query_as("SELECT EXISTS (SELECT 1 FROM stock_daily_bars LIMIT 1)")
                .fetch_one(&self.state.db)
                .await
                .map_err(crate::error::AppError::Database);

        result.ok().map(|(exists,)| exists).unwrap_or(false)
    }
}
