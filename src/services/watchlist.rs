use serde::Serialize;
use std::sync::Arc;

use crate::error::Result;
use crate::state::AppState;
use crate::storage::postgres;

#[derive(Debug, Clone, Serialize)]
pub struct WatchlistItem {
    pub code: String,
    pub name: String,
}

pub struct WatchlistService {
    pub state: Arc<AppState>,
}

impl WatchlistService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn add_stock(&self, user_id: i64, raw_code: &str) -> Result<String> {
        let resolved = postgres::resolve_stock_code(&self.state.db, raw_code)
            .await?
            .unwrap_or_else(|| raw_code.trim().to_uppercase());
        postgres::add_watchlist_stock(&self.state.db, user_id, &resolved).await?;
        Ok(resolved)
    }

    pub async fn remove_stock(&self, user_id: i64, raw_code: &str) -> Result<bool> {
        let resolved = postgres::resolve_stock_code(&self.state.db, raw_code)
            .await?
            .unwrap_or_else(|| raw_code.trim().to_uppercase());
        postgres::remove_watchlist_stock(&self.state.db, user_id, &resolved).await
    }

    pub async fn contains_stock(&self, user_id: i64, raw_code: &str) -> Result<bool> {
        let resolved = postgres::resolve_stock_code(&self.state.db, raw_code)
            .await?
            .unwrap_or_else(|| raw_code.trim().to_uppercase());
        postgres::is_watchlist_stock(&self.state.db, user_id, &resolved).await
    }

    pub async fn list_stocks(&self, user_id: i64) -> Result<Vec<WatchlistItem>> {
        let rows = postgres::list_watchlist_stocks(&self.state.db, user_id).await?;
        Ok(rows
            .into_iter()
            .map(|(code, name)| WatchlistItem { code, name })
            .collect())
    }
}
