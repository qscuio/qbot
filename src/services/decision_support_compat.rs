use std::sync::Arc;

use chrono::NaiveDate;
use serde_json::{Map, Value};
use sqlx::PgPool;

use crate::analysis::decision_support::{DecisionSupport, DecisionSupportConfig};
use crate::error::Result;
use crate::services::trend_analyzer::{TrendAnalysis, TrendAnalyzer};
use crate::state::AppState;
use crate::storage::decision_support_repository::{
    DecisionBriefRow, DecisionCandidateRow, DecisionSupportRepository,
};
use crate::storage::market_repository::MarketRepository;
use crate::storage::postgres;

#[derive(Debug, Clone)]
pub(crate) struct FactualSectorMove {
    pub name: String,
    pub change_pct: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct FactualTopStock {
    pub code: String,
    pub name: String,
    pub change_pct: f64,
    pub trend: Option<TrendAnalysis>,
}

#[derive(Debug, Clone)]
pub(crate) struct DecisionSupportCompatibilityContext {
    pub trade_date: NaiveDate,
    pub brief: Option<DecisionBriefRow>,
    pub candidates: Vec<DecisionCandidateRow>,
    pub market_metrics: Value,
    pub top_sectors: Vec<FactualSectorMove>,
    pub bottom_sectors: Vec<FactualSectorMove>,
    pub top_stock: Option<FactualTopStock>,
}

pub(crate) async fn load_or_build_for_date(
    state: &Arc<AppState>,
    trade_date: NaiveDate,
) -> Result<DecisionSupportCompatibilityContext> {
    let repo = DecisionSupportRepository::new(state.db.clone());
    let (top_sectors, bottom_sectors) = load_sector_leaderboards(&state.db, trade_date).await?;
    let top_stock = load_factual_top_stock(&state.db, trade_date).await?;

    if let Some(run) = repo.find_run_by_trade_date(trade_date).await? {
        let brief = repo.find_brief(run.run_id).await?;
        let candidates = repo.list_candidates(run.run_id).await?;
        let market_metrics = MarketRepository::new(state.db.clone())
            .market_snapshot(trade_date, &run.market_snapshot_version)
            .await?
            .map(|snapshot| snapshot.metrics)
            .unwrap_or_else(empty_metrics);

        return Ok(DecisionSupportCompatibilityContext {
            trade_date,
            brief,
            candidates,
            market_metrics,
            top_sectors,
            bottom_sectors,
            top_stock,
        });
    }

    let mut config = DecisionSupportConfig::from(&*state.config);
    config.persist_run = true;

    let support = DecisionSupport::new(state.db.clone())
        .build_daily(trade_date, config)
        .await?;
    let brief = repo.find_brief(support.run_id).await?;
    let candidates = repo.list_candidates(support.run_id).await?;

    Ok(DecisionSupportCompatibilityContext {
        trade_date: support.trade_date,
        brief,
        candidates,
        market_metrics: support.market_summary.metrics,
        top_sectors,
        bottom_sectors,
        top_stock,
    })
}

fn empty_metrics() -> Value {
    Value::Object(Map::new())
}

async fn load_sector_leaderboards(
    pool: &PgPool,
    trade_date: NaiveDate,
) -> Result<(Vec<FactualSectorMove>, Vec<FactualSectorMove>)> {
    let top_rows: Vec<(String, f64)> = sqlx::query_as(
        r#"SELECT name, change_pct::float8
           FROM sector_daily
           WHERE trade_date = $1
             AND name IS NOT NULL
             AND change_pct IS NOT NULL
           ORDER BY change_pct DESC NULLS LAST
           LIMIT 5"#,
    )
    .bind(trade_date)
    .fetch_all(pool)
    .await?;

    let bottom_rows: Vec<(String, f64)> = sqlx::query_as(
        r#"SELECT name, change_pct::float8
           FROM sector_daily
           WHERE trade_date = $1
             AND name IS NOT NULL
             AND change_pct IS NOT NULL
           ORDER BY change_pct ASC NULLS LAST
           LIMIT 5"#,
    )
    .bind(trade_date)
    .fetch_all(pool)
    .await?;

    Ok((
        top_rows
            .into_iter()
            .map(|(name, change_pct)| FactualSectorMove { name, change_pct })
            .collect(),
        bottom_rows
            .into_iter()
            .map(|(name, change_pct)| FactualSectorMove { name, change_pct })
            .collect(),
    ))
}

async fn load_factual_top_stock(
    pool: &PgPool,
    trade_date: NaiveDate,
) -> Result<Option<FactualTopStock>> {
    let top_row: Option<(String, String, f64)> = sqlx::query_as(
        r#"WITH ranked AS (
             SELECT b.code,
                    COALESCE(i.name, b.code) AS name,
                    b.trade_date,
                    b.close::float8 AS close,
                    LAG(b.close::float8) OVER (PARTITION BY b.code ORDER BY b.trade_date) AS prev_close
             FROM stock_daily_bars b
             LEFT JOIN stock_info i ON i.code = b.code
             WHERE b.trade_date <= $1
           )
           SELECT code,
                  name,
                  ((close - prev_close) / prev_close) * 100 AS change_pct
           FROM ranked
           WHERE trade_date = $1
             AND prev_close > 0
           ORDER BY change_pct DESC NULLS LAST
           LIMIT 1"#,
    )
    .bind(trade_date)
    .fetch_optional(pool)
    .await?;

    let Some((code, name, change_pct)) = top_row else {
        return Ok(None);
    };

    let bars = postgres::get_stock_history(pool, &code, 220).await?;
    let trend = TrendAnalyzer::analyze(&code, &bars);

    Ok(Some(FactualTopStock {
        code,
        name,
        change_pct,
        trend,
    }))
}
