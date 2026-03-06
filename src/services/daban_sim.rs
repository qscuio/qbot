use std::sync::Arc;

use serde::Serialize;

use crate::error::{AppError, Result};
use crate::market_time::beijing_today;
use crate::state::AppState;
use crate::storage::postgres;

#[derive(Debug, Clone, Serialize)]
pub struct DabanSimPosition {
    pub id: i64,
    pub code: String,
    pub name: Option<String>,
    pub entry_price: f64,
    pub shares: i32,
    pub score: Option<f64>,
    pub entry_date: Option<chrono::NaiveDate>,
    pub current_price: Option<f64>,
    pub unrealized_pnl_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DabanSimStats {
    pub balance: f64,
    pub open_positions: i64,
    pub closed_trades: i64,
    pub avg_closed_pnl_pct: f64,
    pub realized_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DabanSimTradeResult {
    pub position_id: i64,
    pub code: String,
    pub shares: i32,
    pub entry_price: f64,
    pub exit_price: Option<f64>,
    pub pnl_pct: Option<f64>,
}

pub struct DabanSimService {
    pub state: Arc<AppState>,
}

impl DabanSimService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn get_balance(&self) -> Result<f64> {
        let row: Option<(f64,)> = sqlx::query_as(
            r#"SELECT balance::float8
               FROM sim_capital
               WHERE sim_type = 'daban'
               LIMIT 1"#,
        )
        .fetch_optional(&self.state.db)
        .await?;
        Ok(row.map(|r| r.0).unwrap_or(0.0))
    }

    pub async fn list_open_positions(&self) -> Result<Vec<DabanSimPosition>> {
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
                      p.score::float8,
                      p.entry_date,
                      last_bar.close::float8 AS current_price
               FROM daban_sim_positions p
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
                |(id, code, name, entry_price, shares, score, entry_date, current_price)| {
                    let entry_price = entry_price.unwrap_or(0.0);
                    let unrealized_pnl_pct = current_price.map(|cp| {
                        if entry_price <= 0.0 {
                            0.0
                        } else {
                            (cp - entry_price) / entry_price * 100.0
                        }
                    });
                    DabanSimPosition {
                        id,
                        code,
                        name,
                        entry_price,
                        shares: shares.unwrap_or(0),
                        score,
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
        raw_code: &str,
        name: Option<String>,
        price: f64,
        shares: i32,
        score: Option<f64>,
    ) -> Result<DabanSimTradeResult> {
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
               WHERE sim_type = 'daban'
               FOR UPDATE"#,
        )
        .fetch_optional(&mut *tx)
        .await?;

        let balance = row
            .map(|r| r.0)
            .ok_or_else(|| AppError::Internal("sim_capital row missing for daban".to_string()))?;
        if balance < cost {
            return Err(AppError::Internal(format!(
                "insufficient balance: have {:.2}, need {:.2}",
                balance, cost
            )));
        }

        let name = name.or(postgres::get_stock_name(&self.state.db, &code).await?);
        let inserted: (i64,) = sqlx::query_as(
            r#"INSERT INTO daban_sim_positions
               (code, name, entry_price, shares, score, entry_date, is_open)
               VALUES ($1, $2, $3, $4, $5, $6, TRUE)
               RETURNING id"#,
        )
        .bind(&code)
        .bind(name)
        .bind(price)
        .bind(shares)
        .bind(score)
        .bind(entry_date)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            r#"UPDATE sim_capital
               SET balance = balance - $1, updated_at = NOW()
               WHERE sim_type = 'daban'"#,
        )
        .bind(cost)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(DabanSimTradeResult {
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
        position_id: i64,
        exit_price: f64,
        reason: Option<String>,
    ) -> Result<DabanSimTradeResult> {
        if exit_price <= 0.0 {
            return Err(AppError::Internal(
                "exit price must be positive".to_string(),
            ));
        }

        let mut tx = self.state.db.begin().await?;

        let row: Option<(String, Option<f64>, Option<i32>, bool)> = sqlx::query_as(
            r#"SELECT code, entry_price::float8, shares, is_open
               FROM daban_sim_positions
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
            r#"UPDATE daban_sim_positions
               SET exit_price = $1,
                   exit_date = $2,
                   exit_reason = $3,
                   is_open = FALSE
               WHERE id = $4"#,
        )
        .bind(exit_price)
        .bind(beijing_today())
        .bind(reason.unwrap_or_else(|| "manual_sell".to_string()))
        .bind(position_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"UPDATE sim_capital
               SET balance = balance + $1, updated_at = NOW()
               WHERE sim_type = 'daban'"#,
        )
        .bind(proceeds)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(DabanSimTradeResult {
            position_id,
            code,
            shares,
            entry_price,
            exit_price: Some(exit_price),
            pnl_pct: Some(pnl_pct),
        })
    }

    pub async fn stats(&self) -> Result<DabanSimStats> {
        let balance = self.get_balance().await?;
        let rows: Vec<(Option<f64>, Option<f64>, Option<i32>, bool)> = sqlx::query_as(
            r#"SELECT entry_price::float8, exit_price::float8, shares, is_open
               FROM daban_sim_positions"#,
        )
        .fetch_all(&self.state.db)
        .await?;

        let mut open_positions: i64 = 0;
        let mut closed_trades: i64 = 0;
        let mut pnl_sum = 0.0;
        let mut pnl_count = 0i64;
        let mut realized = 0.0;

        for (entry, exit, shares, is_open) in rows {
            if is_open {
                open_positions += 1;
                continue;
            }
            closed_trades += 1;
            let entry = entry.unwrap_or(0.0);
            let exit = exit.unwrap_or(0.0);
            let shares = shares.unwrap_or(0) as f64;
            if entry > 0.0 {
                let pnl_pct = (exit - entry) / entry * 100.0;
                pnl_sum += pnl_pct;
                pnl_count += 1;
                realized += (exit - entry) * shares;
            }
        }

        Ok(DabanSimStats {
            balance,
            open_positions,
            closed_trades,
            avg_closed_pnl_pct: if pnl_count > 0 {
                pnl_sum / pnl_count as f64
            } else {
                0.0
            },
            realized_pnl: realized,
        })
    }
}
