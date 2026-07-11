use std::sync::Arc;

use chrono::NaiveDate;
use serde_json::{Map, Value};

use crate::analysis::decision_support::{DecisionSupport, DecisionSupportConfig};
use crate::error::Result;
use crate::state::AppState;
use crate::storage::decision_support_repository::{
    DecisionBriefRow, DecisionCandidateRow, DecisionSupportRepository,
};
use crate::storage::market_repository::MarketRepository;

#[derive(Debug, Clone)]
pub(crate) struct DecisionSupportCompatibilityContext {
    pub trade_date: NaiveDate,
    pub brief: Option<DecisionBriefRow>,
    pub candidates: Vec<DecisionCandidateRow>,
    pub market_metrics: Value,
}

pub(crate) async fn load_or_build_for_date(
    state: &Arc<AppState>,
    trade_date: NaiveDate,
) -> Result<DecisionSupportCompatibilityContext> {
    let repo = DecisionSupportRepository::new(state.db.clone());

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
    })
}

fn empty_metrics() -> Value {
    Value::Object(Map::new())
}
