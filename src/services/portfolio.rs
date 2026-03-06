use std::sync::Arc;

use serde::Serialize;

use crate::error::Result;
use crate::state::AppState;
use crate::storage::postgres;

#[derive(Debug, Clone, Serialize)]
pub struct PortfolioPosition {
    pub code: String,
    pub name: String,
    pub cost_price: f64,
    pub shares: i32,
    pub last_price: Option<f64>,
    pub market_value: Option<f64>,
    pub pnl_pct: Option<f64>,
}

pub struct PortfolioService {
    pub state: Arc<AppState>,
}

impl PortfolioService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn add_position(
        &self,
        user_id: i64,
        raw_code: &str,
        cost_price: f64,
        shares: i32,
    ) -> Result<String> {
        let code = postgres::resolve_stock_code(&self.state.db, raw_code)
            .await?
            .unwrap_or_else(|| raw_code.trim().to_uppercase());

        sqlx::query(
            r#"INSERT INTO user_portfolio (user_id, code, cost_price, shares)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (user_id, code) DO UPDATE SET
                 cost_price = (
                   (user_portfolio.cost_price * user_portfolio.shares)
                   + (EXCLUDED.cost_price * EXCLUDED.shares)
                 ) / NULLIF((user_portfolio.shares + EXCLUDED.shares), 0),
                 shares = user_portfolio.shares + EXCLUDED.shares,
                 added_at = NOW()"#,
        )
        .bind(user_id)
        .bind(&code)
        .bind(cost_price)
        .bind(shares)
        .execute(&self.state.db)
        .await?;

        Ok(code)
    }

    pub async fn remove_position(&self, user_id: i64, raw_code: &str) -> Result<bool> {
        let code = postgres::resolve_stock_code(&self.state.db, raw_code)
            .await?
            .unwrap_or_else(|| raw_code.trim().to_uppercase());
        let res = sqlx::query(
            r#"DELETE FROM user_portfolio
               WHERE user_id = $1 AND code = $2"#,
        )
        .bind(user_id)
        .bind(code)
        .execute(&self.state.db)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn list_positions(&self, user_id: i64) -> Result<Vec<PortfolioPosition>> {
        let rows: Vec<(String, String, f64, i32, Option<f64>)> = sqlx::query_as(
            r#"SELECT p.code,
                      COALESCE(i.name, p.code) AS name,
                      p.cost_price::float8,
                      p.shares,
                      last_bar.close::float8 AS last_price
               FROM user_portfolio p
               LEFT JOIN stock_info i ON i.code = p.code
               LEFT JOIN LATERAL (
                 SELECT close
                 FROM stock_daily_bars b
                 WHERE b.code = p.code
                 ORDER BY b.trade_date DESC
                 LIMIT 1
               ) last_bar ON TRUE
               WHERE p.user_id = $1
               ORDER BY p.added_at DESC"#,
        )
        .bind(user_id)
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(code, name, cost_price, shares, last_price)| {
                let market_value = last_price.map(|p| p * shares as f64);
                let pnl_pct = last_price.map(|p| {
                    if cost_price <= 0.0 {
                        0.0
                    } else {
                        (p - cost_price) / cost_price * 100.0
                    }
                });
                PortfolioPosition {
                    code,
                    name,
                    cost_price,
                    shares,
                    last_price,
                    market_value,
                    pnl_pct,
                }
            })
            .collect())
    }
}
