use chrono::NaiveDate;
use std::sync::Arc;

use crate::data::provider::DataProvider;
use crate::data::tushare::TushareClient;
use crate::data::types::IndexData;
use crate::error::Result;
use crate::state::AppState;

const INDICES: &[(&str, &str)] = &[
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
    ("000688.SH", "科创50"),
];

pub struct MarketService {
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
}

impl MarketService {
    pub fn new(state: Arc<AppState>, provider: Arc<TushareClient>) -> Self {
        MarketService { state, provider }
    }

    pub async fn get_market_overview(&self, date: NaiveDate) -> Result<MarketOverview> {
        let mut indices = Vec::new();
        for (code, _name) in INDICES {
            if let Ok(Some(data)) = self.provider.get_index_daily(code, date).await {
                indices.push(data);
            }
        }

        // Market breadth from DB
        let row: Option<(Option<i64>, Option<i64>, Option<i64>, Option<f64>)> =
            sqlx::query_as(
                r#"SELECT
                   COUNT(CASE WHEN close > open THEN 1 END),
                   COUNT(CASE WHEN close < open THEN 1 END),
                   COUNT(CASE WHEN ABS(close - open) / NULLIF(open,0) * 100 >= 9.8 THEN 1 END),
                   SUM(amount::float8)
                   FROM stock_daily_bars WHERE trade_date = $1"#,
            )
            .bind(date)
            .fetch_optional(&self.state.db)
            .await?;

        let (up_count, down_count, limit_up_count, total_amount) = row
            .flatten_opt()
            .unwrap_or((Some(0), Some(0), Some(0), Some(0.0)));

        Ok(MarketOverview {
            date,
            indices,
            up_count: up_count.unwrap_or(0) as usize,
            down_count: down_count.unwrap_or(0) as usize,
            limit_up_count: limit_up_count.unwrap_or(0) as usize,
            total_amount: total_amount.unwrap_or(0.0),
        })
    }
}

// Helper trait to flatten Option<(...)>
trait FlattenOpt {
    fn flatten_opt(self) -> Self;
}
impl<A, B, C, D> FlattenOpt for Option<(Option<A>, Option<B>, Option<C>, Option<D>)> {
    fn flatten_opt(self) -> Self {
        self
    }
}

#[derive(Debug, serde::Serialize)]
pub struct MarketOverview {
    pub date: NaiveDate,
    pub indices: Vec<IndexData>,
    pub up_count: usize,
    pub down_count: usize,
    pub limit_up_count: usize,
    pub total_amount: f64,
}
