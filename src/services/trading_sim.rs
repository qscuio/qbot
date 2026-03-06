use std::sync::Arc;

use serde::Serialize;

use crate::error::{AppError, Result};
use crate::market_time::beijing_today;
use crate::state::AppState;
use crate::storage::postgres;

#[derive(Debug, Clone, Serialize)]
pub struct SimPosition {
    pub id: i64,
    pub code: String,
    pub name: Option<String>,
    pub entry_price: f64,
    pub shares: i32,
    pub peak_price: Option<f64>,
    pub entry_date: Option<chrono::NaiveDate>,
    pub current_price: Option<f64>,
    pub unrealized_pnl_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SimStats {
    pub sim_type: String,
    pub balance: f64,
    pub open_positions: i64,
    pub closed_trades: i64,
    pub avg_closed_pnl_pct: f64,
    pub realized_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SimTradeResult {
    pub position_id: i64,
    pub code: String,
    pub shares: i32,
    pub entry_price: f64,
    pub exit_price: Option<f64>,
    pub pnl_pct: Option<f64>,
}

pub struct TradingSimService {
    pub state: Arc<AppState>,
}

impl TradingSimService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    fn ensure_supported_sim_type(sim_type: &str) -> Result<()> {
        if sim_type != "general" {
            return Err(AppError::Internal(format!(
                "unsupported sim_type: {} (currently only 'general')",
                sim_type
            )));
        }
        Ok(())
    }

    pub async fn get_balance(&self, sim_type: &str) -> Result<f64> {
        Self::ensure_supported_sim_type(sim_type)?;
        let row: Option<(f64,)> = sqlx::query_as(
            r#"SELECT balance::float8
               FROM sim_capital
               WHERE sim_type = $1
               LIMIT 1"#,
        )
        .bind(sim_type)
        .fetch_optional(&self.state.db)
        .await?;
        Ok(row.map(|r| r.0).unwrap_or(0.0))
    }

    pub async fn list_open_positions(&self, sim_type: &str) -> Result<Vec<SimPosition>> {
        Self::ensure_supported_sim_type(sim_type)?;
        let rows: Vec<(
            i64,
            String,
            Option<String>,
            Option<f64>,
            Option<i32>,
            Option<f64>,
            Option<chrono::NaiveDate>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT p.id,
                      p.code,
                      p.name,
                      p.entry_price::float8,
                      p.shares,
                      p.peak_price::float8,
                      p.entry_date,
                      last_bar.close::float8 AS current_price
               FROM trading_sim_positions p
               LEFT JOIN LATERAL (
                 SELECT close
                 FROM stock_daily_bars b
                 WHERE b.code = p.code
                 ORDER BY b.trade_date DESC
                 LIMIT 1
               ) last_bar ON TRUE
               WHERE p.is_open = TRUE
               ORDER BY p.entry_date DESC, p.id DESC"#,
        )
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(id, code, name, entry_price, shares, peak_price, entry_date, current_price)| {
                    let entry_price = entry_price.unwrap_or(0.0);
                    let unrealized_pnl_pct = current_price.map(|cp| {
                        if entry_price <= 0.0 {
                            0.0
                        } else {
                            (cp - entry_price) / entry_price * 100.0
                        }
                    });
                    SimPosition {
                        id,
                        code,
                        name,
                        entry_price,
                        shares: shares.unwrap_or(0),
                        peak_price,
                        entry_date,
                        current_price,
                        unrealized_pnl_pct,
                    }
                },
            )
            .collect())
    }

    pub async fn buy(
        &self,
        sim_type: &str,
        raw_code: &str,
        name: Option<String>,
        price: f64,
        shares: i32,
    ) -> Result<SimTradeResult> {
        Self::ensure_supported_sim_type(sim_type)?;
        if price <= 0.0 || shares <= 0 {
            return Err(AppError::Internal(
                "price and shares must be positive".to_string(),
            ));
        }

        let code = postgres::resolve_stock_code(&self.state.db, raw_code)
            .await?
            .unwrap_or_else(|| raw_code.trim().to_uppercase());

        let cost = price * shares as f64;
        let entry_date = beijing_today();
        let mut tx = self.state.db.begin().await?;

        let row: Option<(f64,)> = sqlx::query_as(
            r#"SELECT balance::float8
               FROM sim_capital
               WHERE sim_type = $1
               FOR UPDATE"#,
        )
        .bind(sim_type)
        .fetch_optional(&mut *tx)
        .await?;

        let balance = row.map(|r| r.0).ok_or_else(|| {
            AppError::Internal(format!("sim_capital row missing for {}", sim_type))
        })?;
        if balance < cost {
            return Err(AppError::Internal(format!(
                "insufficient balance: have {:.2}, need {:.2}",
                balance, cost
            )));
        }

        let name = name.or(postgres::get_stock_name(&self.state.db, &code).await?);
        let inserted: (i64,) = sqlx::query_as(
            r#"INSERT INTO trading_sim_positions
               (code, name, entry_price, shares, peak_price, entry_date, is_open)
               VALUES ($1, $2, $3, $4, $5, $6, TRUE)
               RETURNING id"#,
        )
        .bind(&code)
        .bind(name)
        .bind(price)
        .bind(shares)
        .bind(price)
        .bind(entry_date)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            r#"UPDATE sim_capital
               SET balance = balance - $1, updated_at = NOW()
               WHERE sim_type = $2"#,
        )
        .bind(cost)
        .bind(sim_type)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(SimTradeResult {
            position_id: inserted.0,
            code,
            shares,
            entry_price: price,
            exit_price: None,
            pnl_pct: None,
        })
    }

    pub async fn sell(
        &self,
        sim_type: &str,
        position_id: i64,
        exit_price: f64,
        reason: Option<String>,
    ) -> Result<SimTradeResult> {
        Self::ensure_supported_sim_type(sim_type)?;
        if exit_price <= 0.0 {
            return Err(AppError::Internal(
                "exit price must be positive".to_string(),
            ));
        }

        let mut tx = self.state.db.begin().await?;

        let row: Option<(String, Option<f64>, Option<i32>, bool)> = sqlx::query_as(
            r#"SELECT code, entry_price::float8, shares, is_open
               FROM trading_sim_positions
               WHERE id = $1
               FOR UPDATE"#,
        )
        .bind(position_id)
        .fetch_optional(&mut *tx)
        .await?;

        let (code, entry_price, shares, is_open) =
            row.ok_or_else(|| AppError::Internal(format!("position {} not found", position_id)))?;
        if !is_open {
            return Err(AppError::Internal(format!(
                "position {} is already closed",
                position_id
            )));
        }

        let entry_price = entry_price.unwrap_or(0.0);
        let shares = shares.unwrap_or(0);
        if shares <= 0 || entry_price <= 0.0 {
            return Err(AppError::Internal("invalid position state".to_string()));
        }

        let pnl_pct = (exit_price - entry_price) / entry_price * 100.0;
        let proceeds = exit_price * shares as f64;

        sqlx::query(
            r#"UPDATE trading_sim_positions
               SET exit_price = $1,
                   exit_date = $2,
                   exit_reason = $3,
                   pnl_pct = $4,
                   is_open = FALSE
               WHERE id = $5"#,
        )
        .bind(exit_price)
        .bind(beijing_today())
        .bind(reason.unwrap_or_else(|| "manual_sell".to_string()))
        .bind(pnl_pct)
        .bind(position_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"UPDATE sim_capital
               SET balance = balance + $1, updated_at = NOW()
               WHERE sim_type = $2"#,
        )
        .bind(proceeds)
        .bind(sim_type)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(SimTradeResult {
            position_id,
            code,
            shares,
            entry_price,
            exit_price: Some(exit_price),
            pnl_pct: Some(pnl_pct),
        })
    }

    pub async fn stats(&self, sim_type: &str) -> Result<SimStats> {
        Self::ensure_supported_sim_type(sim_type)?;
        let balance = self.get_balance(sim_type).await?;

        let row: Option<(Option<i64>, Option<i64>, Option<f64>, Option<f64>)> = sqlx::query_as(
            r#"SELECT
                 COUNT(*) FILTER (WHERE is_open = TRUE),
                 COUNT(*) FILTER (WHERE is_open = FALSE),
                 AVG(pnl_pct)::float8,
                 SUM((COALESCE(exit_price, 0)::float8 - COALESCE(entry_price, 0)::float8) * COALESCE(shares, 0)::float8)::float8
               FROM trading_sim_positions"#,
        )
        .fetch_optional(&self.state.db)
        .await?;

        let (open_positions, closed_trades, avg_closed_pnl_pct, realized_pnl) =
            row.unwrap_or((Some(0), Some(0), Some(0.0), Some(0.0)));

        Ok(SimStats {
            sim_type: sim_type.to_string(),
            balance,
            open_positions: open_positions.unwrap_or(0),
            closed_trades: closed_trades.unwrap_or(0),
            avg_closed_pnl_pct: avg_closed_pnl_pct.unwrap_or(0.0),
            realized_pnl: realized_pnl.unwrap_or(0.0),
        })
    }
}
