use chrono::NaiveDate;
use std::sync::Arc;
use tracing::info;

use crate::data::provider::DataProvider;
use crate::data::types::LimitUpStock;
use crate::error::Result;
use crate::state::AppState;
use crate::storage::postgres::{self, StartupWatchStock, StrongLimitUpStock};

pub struct LimitUpService {
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
}

impl LimitUpService {
    pub fn new(state: Arc<AppState>, provider: Arc<dyn DataProvider>) -> Self {
        LimitUpService { state, provider }
    }

    pub async fn fetch_and_save(&self, date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        let stocks = self.provider.get_limit_up_stocks(date).await?;
        info!("涨停板: {} stocks on {}", stocks.len(), date);
        postgres::save_limit_up_stocks(&self.state.db, &stocks).await?;
        postgres::rebuild_startup_watchlist(&self.state.db, date).await?;
        Ok(stocks)
    }

    pub async fn latest_trade_date(&self) -> Result<Option<NaiveDate>> {
        postgres::latest_limit_up_trade_date(&self.state.db).await
    }

    pub async fn get_strong_stocks(
        &self,
        days: i64,
        min_limit_count: i64,
    ) -> Result<Vec<StrongLimitUpStock>> {
        postgres::list_strong_limit_up_stocks(&self.state.db, days, min_limit_count).await
    }

    pub async fn get_startup_watchlist(&self) -> Result<Vec<StartupWatchStock>> {
        postgres::list_startup_watchlist(&self.state.db).await
    }

    pub async fn get_stocks_by_date(&self, date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        postgres::list_limit_up_stocks_by_date(&self.state.db, date).await
    }

    pub async fn get_summary(&self, date: NaiveDate) -> Result<LimitUpSummary> {
        let rows: Vec<(
            String,
            Option<String>,
            Option<i32>,
            Option<f64>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT code, name, burst_count, seal_amount::float8, pct_chg::float8
                   FROM limit_up_stocks WHERE trade_date = $1 ORDER BY seal_amount DESC"#,
        )
        .bind(date)
        .fetch_all(&self.state.db)
        .await?;

        let total = rows.len();
        let burst = rows.iter().filter(|r| r.2.unwrap_or(0) > 0).count();
        let sealed = total - burst;

        Ok(LimitUpSummary {
            date,
            total,
            sealed,
            burst,
            burst_rate: if total > 0 {
                burst as f64 / total as f64 * 100.0
            } else {
                0.0
            },
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct LimitUpSummary {
    pub date: NaiveDate,
    pub total: usize,
    pub sealed: usize,
    pub burst: usize,
    pub burst_rate: f64,
}
