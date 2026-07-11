use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::NaiveDate;
use serde_json::{json, Value};

use crate::analysis::decision_support::{DecisionSupport, DecisionSupportConfig};
use crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION;
use crate::error::{AppError, Result};
use crate::state::AppState;
use crate::storage::decision_support_repository::{
    DecisionBriefRow, DecisionCandidateRow, DecisionSupportRepository, DecisionSupportRunRow,
};
use crate::storage::market_repository::MarketRepository;
use crate::storage::postgres;

type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;

#[derive(Debug, Clone)]
struct PersistedDecisionSupportView {
    run: DecisionSupportRunRow,
    brief: Option<DecisionBriefRow>,
    candidates: Vec<DecisionCandidateRow>,
}

pub fn decision_support_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/api/analysis/decision-support/latest",
            get(get_latest_decision_support),
        )
        .route(
            "/api/analysis/decision-support/:date/:code",
            get(get_decision_support_detail),
        )
        .route(
            "/api/analysis/decision-support/:date",
            get(get_decision_support_by_date),
        )
        .route(
            "/api/jobs/analysis/decision-support",
            post(build_decision_support_job),
        )
        .with_state(state)
}

async fn get_latest_decision_support(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    require_auth(&headers, &state)?;

    let Some(view) = load_latest_view(&state)
        .await
        .map_err(|error| crate::api::routes::api_error(&error.to_string()))?
    else {
        return Err(not_found("no persisted decision support run"));
    };

    Ok(Json(view_json(&view)))
}

async fn get_decision_support_by_date(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(raw_date): Path<String>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let trade_date = parse_trade_date(&raw_date)?;

    let Some(view) = load_view_for_date(&state, trade_date)
        .await
        .map_err(|error| crate::api::routes::api_error(&error.to_string()))?
    else {
        return Err(not_found("decision support run not found for date"));
    };

    Ok(Json(view_json(&view)))
}

async fn get_decision_support_detail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((raw_date, code)): Path<(String, String)>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let trade_date = parse_trade_date(&raw_date)?;

    let Some(view) = load_view_for_date(&state, trade_date)
        .await
        .map_err(|error| crate::api::routes::api_error(&error.to_string()))?
    else {
        return Err(not_found("decision support run not found for date"));
    };

    let candidates: Vec<Value> = view
        .candidates
        .iter()
        .filter(|candidate| candidate_matches_code(&candidate.code, &code))
        .map(candidate_json)
        .collect();

    if candidates.is_empty() {
        return Err(not_found("decision support candidate not found for code"));
    }

    Ok(Json(json!({
        "tradeDate": view.run.trade_date,
        "requestedCode": code,
        "run": run_json(&view.run),
        "dailyBrief": view.brief.as_ref().map(brief_json),
        "count": candidates.len(),
        "candidates": candidates,
    })))
}

async fn build_decision_support_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    require_auth(&headers, &state)?;

    let _guard = state.analysis_job_lock.lock().await;
    let trade_date = resolve_build_trade_date(&state)
        .await
        .map_err(|error| map_app_error(&error))?;

    let mut config = DecisionSupportConfig::from(&*state.config);
    config.persist_run = true;

    let build_result = DecisionSupport::new(state.db.clone())
        .build_daily(trade_date, config.clone())
        .await;

    let view = match build_result {
        Ok(support) => load_view_for_run_id(&state, support.run_id)
            .await
            .map_err(|error| crate::api::routes::api_error(&error.to_string()))?
            .ok_or_else(|| {
                crate::api::routes::api_error("persisted decision support run missing")
            })?,
        Err(error) if is_duplicate_run_error(&error) => load_view_for_date(&state, trade_date)
            .await
            .map_err(|load_error| crate::api::routes::api_error(&load_error.to_string()))?
            .ok_or_else(|| crate::api::routes::api_error(&error.to_string()))?,
        Err(error) => return Err(map_app_error(&error)),
    };

    Ok(Json(json!({
        "status": "persisted",
        "tradeDate": view.run.trade_date,
        "supportVersion": config.support_version,
        "artifact": view_json(&view),
    })))
}

pub(crate) async fn handle_telegram_decision(
    state: Arc<AppState>,
    chat_id: i64,
) -> crate::error::Result<()> {
    let text = render_latest_decision_report(&state).await?;
    telegram_send(&state, chat_id, &text).await
}

pub(crate) async fn handle_telegram_decision_detail(
    state: Arc<AppState>,
    chat_id: i64,
    args: &str,
) -> crate::error::Result<()> {
    let code = args.trim();
    if code.is_empty() {
        return telegram_send(
            &state,
            chat_id,
            "用法: <code>/decision_detail &lt;code&gt;</code>",
        )
        .await;
    }

    let text = render_latest_decision_detail_report(&state, code).await?;
    telegram_send(&state, chat_id, &text).await
}

pub(crate) async fn render_latest_decision_report(state: &AppState) -> Result<String> {
    let Some(view) = load_latest_view(state).await? else {
        return Ok("📭 暂无已持久化的 DecisionSupport 日报".to_string());
    };

    Ok(format_decision_report(&view))
}

pub(crate) async fn render_latest_decision_detail_report(
    state: &AppState,
    code: &str,
) -> Result<String> {
    let Some(view) = load_latest_view(state).await? else {
        return Ok("📭 暂无已持久化的 DecisionSupport 日报".to_string());
    };

    let matched: Vec<&DecisionCandidateRow> = view
        .candidates
        .iter()
        .filter(|candidate| candidate_matches_code(&candidate.code, code))
        .collect();

    if matched.is_empty() {
        return Ok(format!(
            "📭 未找到代码 <code>{}</code> 的已持久化 DecisionSupport 记录",
            escape_html(code)
        ));
    }

    Ok(format_decision_detail_report(
        &view.run.trade_date,
        code,
        &matched,
    ))
}

fn require_auth(
    headers: &HeaderMap,
    state: &AppState,
) -> std::result::Result<(), (StatusCode, Json<Value>)> {
    if crate::api::routes::check_auth(headers, state.config.api_key.as_deref()) {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ))
    }
}

fn parse_trade_date(raw_date: &str) -> std::result::Result<NaiveDate, (StatusCode, Json<Value>)> {
    NaiveDate::parse_from_str(raw_date, "%Y-%m-%d").map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "date must use YYYY-MM-DD"})),
        )
    })
}

fn not_found(message: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": message })))
}

fn map_app_error(error: &AppError) -> (StatusCode, Json<Value>) {
    match error {
        AppError::BadRequest(message) => {
            (StatusCode::BAD_REQUEST, Json(json!({ "error": message })))
        }
        AppError::NotFound(message) => (StatusCode::NOT_FOUND, Json(json!({ "error": message }))),
        _ => crate::api::routes::api_error(&error.to_string()),
    }
}

async fn resolve_build_trade_date(state: &AppState) -> Result<NaiveDate> {
    let market_repo = MarketRepository::new(state.db.clone());
    if let Some(snapshot) = market_repo
        .latest_market_snapshot(MARKET_SNAPSHOT_VERSION)
        .await?
    {
        return Ok(snapshot.trade_date);
    }

    postgres::latest_stock_trade_date(&state.db)
        .await?
        .ok_or_else(|| AppError::NotFound("no market snapshot or trade date available".to_string()))
}

async fn load_latest_view(state: &AppState) -> Result<Option<PersistedDecisionSupportView>> {
    let repo = DecisionSupportRepository::new(state.db.clone());
    let Some(run) = repo.latest_run().await? else {
        return Ok(None);
    };

    Ok(Some(load_view(repo, run).await?))
}

async fn load_view_for_date(
    state: &AppState,
    trade_date: NaiveDate,
) -> Result<Option<PersistedDecisionSupportView>> {
    let repo = DecisionSupportRepository::new(state.db.clone());
    let Some(run) = repo.find_run_by_trade_date(trade_date).await? else {
        return Ok(None);
    };

    Ok(Some(load_view(repo, run).await?))
}

async fn load_view_for_run_id(
    state: &AppState,
    run_id: uuid::Uuid,
) -> Result<Option<PersistedDecisionSupportView>> {
    let repo = DecisionSupportRepository::new(state.db.clone());
    let Some(run) = repo.find_run(run_id).await? else {
        return Ok(None);
    };

    Ok(Some(load_view(repo, run).await?))
}

async fn load_view(
    repo: DecisionSupportRepository,
    run: DecisionSupportRunRow,
) -> Result<PersistedDecisionSupportView> {
    let brief = repo.find_brief(run.run_id).await?;
    let candidates = repo.list_candidates(run.run_id).await?;

    Ok(PersistedDecisionSupportView {
        run,
        brief,
        candidates,
    })
}

fn view_json(view: &PersistedDecisionSupportView) -> Value {
    json!({
        "tradeDate": view.run.trade_date,
        "run": run_json(&view.run),
        "dailyBrief": view.brief.as_ref().map(brief_json),
        "count": view.candidates.len(),
        "candidates": view.candidates.iter().map(candidate_json).collect::<Vec<_>>(),
    })
}

fn run_json(run: &DecisionSupportRunRow) -> Value {
    json!({
        "runId": run.run_id,
        "tradeDate": run.trade_date,
        "supportVersion": run.support_version,
        "marketSnapshotVersion": run.market_snapshot_version,
        "patternSetId": run.pattern_set_id,
        "eventBriefVersion": run.event_brief_version,
        "eventScoreEnabled": run.event_score_enabled,
        "eventScoreLimit": run.event_score_limit,
        "status": run.status,
        "inputFingerprint": run.input_fingerprint,
        "startedAt": run.started_at,
        "completedAt": run.completed_at,
        "errorMessage": run.error_message,
    })
}

fn brief_json(brief: &DecisionBriefRow) -> Value {
    json!({
        "tradeDate": brief.trade_date,
        "content": brief.content,
        "structuredPayload": brief.structured_payload,
        "createdAt": brief.created_at,
    })
}

fn candidate_json(candidate: &DecisionCandidateRow) -> Value {
    json!({
        "code": candidate.code,
        "name": candidate.name,
        "horizon": candidate.horizon,
        "baseSource": candidate.base_source,
        "baseScore": candidate.base_score,
        "patternScore": candidate.pattern_score,
        "eventAdjustment": candidate.event_adjustment.unwrap_or(0.0),
        "riskAdjustment": candidate.risk_adjustment.unwrap_or(0.0),
        "finalScore": candidate.final_score,
        "supportTier": candidate.support_tier,
        "facts": labeled_support_statements(&candidate.facts, "fact"),
        "calculations": labeled_support_statements(&candidate.calculations, "calculation"),
        "inferences": labeled_support_statements(&candidate.inferences, "inference"),
        "unknowns": labeled_support_statements(&candidate.unknowns, "unknown"),
        "riskFlags": candidate.risk_flags,
        "invalidations": candidate.invalidations,
        "sourceRefs": candidate.source_refs,
        "createdAt": candidate.created_at,
    })
}

fn labeled_support_statements(raw: &Value, label: &str) -> Vec<Value> {
    raw.as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| match item {
                    Value::Object(map) => {
                        let mut labeled = map.clone();
                        labeled.insert("label".to_string(), Value::String(label.to_string()));
                        Value::Object(labeled)
                    }
                    Value::String(statement) => json!({
                        "label": label,
                        "statement": statement,
                        "sourceRefs": [],
                    }),
                    _ => json!({
                        "label": label,
                        "statement": item.to_string(),
                        "sourceRefs": [],
                    }),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn candidate_matches_code(candidate_code: &str, requested_code: &str) -> bool {
    let requested = requested_code.trim().to_ascii_uppercase();
    let candidate = candidate_code.trim().to_ascii_uppercase();
    candidate == requested
        || candidate
            .split('.')
            .next()
            .is_some_and(|base| base == requested)
}

fn format_decision_report(view: &PersistedDecisionSupportView) -> String {
    let mut lines = vec![
        "🧭 <b>DecisionSupport 日报</b>".to_string(),
        format!("交易日: {}", view.run.trade_date),
        format!(
            "版本: {} | 候选数: {}",
            escape_html(&view.run.support_version),
            view.candidates.len()
        ),
        format!(
            "事件加分: {} | 上限: {:.2}",
            if view.run.event_score_enabled {
                "开启"
            } else {
                "关闭"
            },
            view.run.event_score_limit
        ),
    ];

    if let Some(brief) = &view.brief {
        lines.push(String::new());
        lines.push("<b>简报</b>".to_string());
        lines.push(escape_html(&brief.content));
    }

    if view.candidates.is_empty() {
        lines.push(String::new());
        lines.push("📭 无已持久化候选".to_string());
        return lines.join("\n");
    }

    lines.push(String::new());
    lines.push("<b>Top Candidates</b>".to_string());
    for candidate in view.candidates.iter().take(5) {
        lines.push(format!(
            "{} | {} | {} | {:.2}",
            escape_html(&candidate.code),
            escape_html(&candidate.horizon),
            escape_html(&candidate.support_tier),
            candidate.final_score
        ));
    }

    lines.join("\n")
}

fn format_decision_detail_report(
    trade_date: &NaiveDate,
    requested_code: &str,
    candidates: &[&DecisionCandidateRow],
) -> String {
    let mut lines = vec![
        "🔎 <b>DecisionSupport 详情</b>".to_string(),
        format!("交易日: {}", trade_date),
        format!("请求代码: {}", escape_html(requested_code)),
    ];

    for candidate in candidates {
        lines.push(String::new());
        lines.push(format!(
            "<b>{}</b> {} | {} | {:.2}",
            escape_html(&candidate.code),
            escape_html(&candidate.name),
            escape_html(&candidate.horizon),
            candidate.final_score
        ));
        lines.push(format!(
            "tier={} | base={:.2} | event={:+.2} | risk={:+.2}",
            escape_html(&candidate.support_tier),
            candidate.base_score,
            candidate.event_adjustment.unwrap_or(0.0),
            candidate.risk_adjustment.unwrap_or(0.0),
        ));

        append_statement_lines(&mut lines, "fact", &candidate.facts);
        append_statement_lines(&mut lines, "calculation", &candidate.calculations);
        append_statement_lines(&mut lines, "inference", &candidate.inferences);
        append_statement_lines(&mut lines, "unknown", &candidate.unknowns);
    }

    lines.join("\n")
}

fn append_statement_lines(lines: &mut Vec<String>, label: &str, raw: &Value) {
    for statement in labeled_support_statements(raw, label) {
        let text = statement
            .get("statement")
            .and_then(Value::as_str)
            .unwrap_or_default();
        lines.push(format!("{}: {}", label, escape_html(text)));
    }
}

fn escape_html(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn telegram_send(state: &Arc<AppState>, chat_id: i64, text: &str) -> Result<()> {
    state.pusher.push(&chat_id.to_string(), text).await
}

fn is_duplicate_run_error(error: &AppError) -> bool {
    error
        .to_string()
        .contains("analysis_decision_support_runs_trade_date_support_version_key")
        || error.to_string().contains("duplicate key")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{header, Method, Request, StatusCode};
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use serde_json::{json, Value};
    use sqlx::PgPool;
    use tokio::sync::Mutex;
    use tower::Service;
    use uuid::Uuid;

    use super::*;
    use crate::analysis::market_snapshot::{
        AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, MarketSnapshot,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
    };
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::provider::DataProvider;
    use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
    use crate::services::scan_ranker::POOL_SHORT_A_ID;
    use crate::storage::decision_support_repository::{
        DecisionBriefRow, DecisionCandidateRow, DecisionSupportRepository, DecisionSupportRunRow,
    };
    use crate::storage::market_repository::MarketRepository;
    use crate::storage::postgres::{save_daily_signal_scan_results, DailySignalScanRow};
    use crate::telegram::pusher::TelegramPusher;

    #[sqlx::test(migrations = "./migrations")]
    async fn decision_support_router_protects_routes(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool).await;

        for (method, path) in [
            (Method::GET, "/api/analysis/decision-support/latest"),
            (Method::GET, "/api/analysis/decision-support/2026-07-11"),
            (
                Method::GET,
                "/api/analysis/decision-support/2026-07-11/600000",
            ),
            (Method::POST, "/api/jobs/analysis/decision-support"),
        ] {
            let mut router = decision_support_router(state.clone());
            let response = router
                .call(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        }

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn latest_route_returns_persisted_artifact_with_statement_labels(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = DecisionSupportRepository::new(pool.clone());
        seed_persisted_decision_artifact(
            &repo,
            {
                let run_id = Uuid::new_v4();
                run_row(
                    run_id,
                    date(2026, 7, 10),
                    "decision-support-v1",
                    dt(2026, 7, 10, 18),
                )
            },
            &[],
            None,
        )
        .await?;

        let latest_run_id = Uuid::new_v4();
        let latest_trade_date = date(2026, 7, 11);
        let latest_run = run_row(
            latest_run_id,
            latest_trade_date,
            "decision-support-v1",
            dt(2026, 7, 11, 18),
        );
        let latest_candidate =
            candidate_row(latest_run_id, "600000.SH", "Alpha Bank", "short", 91.4);
        seed_persisted_decision_artifact(
            &repo,
            latest_run.clone(),
            &[latest_candidate.clone()],
            Some(brief_row(
                latest_run_id,
                latest_trade_date,
                "Latest persisted decision brief",
            )),
        )
        .await?;

        let before = safety_counts(&pool).await?;

        let payload = authed_json(
            decision_support_router(state),
            Method::GET,
            "/api/analysis/decision-support/latest",
        )
        .await;

        assert_eq!(payload["tradeDate"], json!(latest_trade_date));
        assert_eq!(payload["run"]["runId"], json!(latest_run.run_id));
        assert_eq!(payload["count"], json!(1));
        assert_eq!(
            payload["dailyBrief"]["content"],
            json!("Latest persisted decision brief")
        );
        assert_eq!(payload["candidates"][0]["code"], json!("600000.SH"));
        assert_statement_labels(&payload["candidates"][0]);

        let after = safety_counts(&pool).await?;
        assert_eq!(before, after);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn date_and_detail_routes_read_explicit_persisted_artifacts_without_trading_writes(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = DecisionSupportRepository::new(pool.clone());
        let run_id = Uuid::new_v4();
        let trade_date = date(2026, 7, 9);
        seed_persisted_decision_artifact(
            &repo,
            run_row(
                run_id,
                trade_date,
                "decision-support-v1",
                dt(2026, 7, 9, 18),
            ),
            &[
                candidate_row(run_id, "600123.SH", "Signal One", "short", 88.0),
                candidate_row(run_id, "000001.SZ", "Signal Two", "week", 84.0),
            ],
            Some(brief_row(run_id, trade_date, "detail brief")),
        )
        .await?;

        let before = safety_counts(&pool).await?;

        let daily_payload = authed_json(
            decision_support_router(state.clone()),
            Method::GET,
            "/api/analysis/decision-support/2026-07-09",
        )
        .await;
        assert_eq!(daily_payload["tradeDate"], json!(trade_date));
        assert_eq!(daily_payload["count"], json!(2));

        let detail_payload = authed_json(
            decision_support_router(state),
            Method::GET,
            "/api/analysis/decision-support/2026-07-09/600123",
        )
        .await;
        assert_eq!(detail_payload["requestedCode"], json!("600123"));
        assert_eq!(detail_payload["count"], json!(1));
        assert_eq!(detail_payload["candidates"][0]["code"], json!("600123.SH"));
        assert_statement_labels(&detail_payload["candidates"][0]);

        let after = safety_counts(&pool).await?;
        assert_eq!(before, after);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn post_build_persists_candidates_and_brief_without_touching_trading_tables(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let trade_date = date(2026, 7, 11);
        seed_market_snapshot(&pool, trade_date).await?;
        seed_ranked_pool_candidate(&pool, trade_date, "600000.SH", "Alpha Bank", 91.4).await?;

        let before = safety_counts(&pool).await?;

        let payload = authed_json(
            decision_support_router(state),
            Method::POST,
            "/api/jobs/analysis/decision-support",
        )
        .await;

        assert_eq!(payload["status"], json!("persisted"));
        assert_eq!(payload["tradeDate"], json!(trade_date));
        assert_eq!(payload["artifact"]["count"], json!(1));

        let decision_repo = DecisionSupportRepository::new(pool.clone());
        let latest = decision_repo.latest_run().await.unwrap().unwrap();
        assert_eq!(latest.trade_date, trade_date);
        assert_eq!(
            decision_repo
                .list_candidates(latest.run_id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(decision_repo
            .find_brief(latest.run_id)
            .await
            .unwrap()
            .is_some());

        let after = safety_counts(&pool).await?;
        assert_eq!(before, after);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn telegram_reports_render_persisted_decision_support_views(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = DecisionSupportRepository::new(pool.clone());
        let run_id = Uuid::new_v4();
        let trade_date = date(2026, 7, 11);
        seed_persisted_decision_artifact(
            &repo,
            run_row(
                run_id,
                trade_date,
                "decision-support-v1",
                dt(2026, 7, 11, 18),
            ),
            &[candidate_row(
                run_id,
                "600000.SH",
                "Alpha Bank",
                "short",
                91.4,
            )],
            Some(brief_row(run_id, trade_date, "Telegram brief")),
        )
        .await?;

        let report = render_latest_decision_report(&state).await.unwrap();
        assert!(report.contains("DecisionSupport"));
        assert!(report.contains("Telegram brief"));
        assert!(report.contains("600000.SH"));

        let detail = render_latest_decision_detail_report(&state, "600000")
            .await
            .unwrap();
        assert!(detail.contains("600000.SH"));
        assert!(detail.contains("fact:"));
        assert!(detail.contains("calculation:"));
        assert!(detail.contains("inference:"));
        assert!(detail.contains("unknown:"));

        Ok(())
    }

    fn assert_statement_labels(candidate: &Value) {
        for (field, label) in [
            ("facts", "fact"),
            ("calculations", "calculation"),
            ("inferences", "inference"),
            ("unknowns", "unknown"),
        ] {
            let statements = candidate[field].as_array().unwrap();
            assert!(!statements.is_empty(), "{field} should not be empty");
            for statement in statements {
                assert_eq!(statement["label"], json!(label));
            }
        }
    }

    async fn authed_json(mut router: Router, method: Method, path: &str) -> Value {
        let response = router
            .call(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{path}");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn seed_persisted_decision_artifact(
        repo: &DecisionSupportRepository,
        run: DecisionSupportRunRow,
        candidates: &[DecisionCandidateRow],
        brief: Option<DecisionBriefRow>,
    ) -> sqlx::Result<()> {
        repo.create_run(&run).await.unwrap();
        repo.save_candidates(candidates).await.unwrap();
        if let Some(brief) = brief {
            repo.save_brief(&brief).await.unwrap();
        }
        Ok(())
    }

    async fn seed_market_snapshot(pool: &PgPool, trade_date: NaiveDate) -> sqlx::Result<()> {
        MarketRepository::new(pool.clone())
            .save_market_snapshot(&MarketSnapshot {
                trade_date,
                snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                available_at: dt(2026, 7, 11, 18),
                data_complete: true,
                metrics: json!({"breadth": {"upCount": 321}}),
                missing_inputs: Vec::new(),
                input_fingerprint: "market-snapshot-fingerprint".to_string(),
            })
            .await
            .unwrap();
        Ok(())
    }

    async fn seed_ranked_pool_candidate(
        pool: &PgPool,
        trade_date: NaiveDate,
        code: &str,
        name: &str,
        score: f64,
    ) -> sqlx::Result<()> {
        save_daily_signal_scan_results(
            pool,
            trade_date,
            Uuid::new_v4(),
            &[DailySignalScanRow {
                code: code.to_string(),
                name: name.to_string(),
                signal_id: POOL_SHORT_A_ID.to_string(),
                signal_name: "短线A档".to_string(),
                icon: "🔥".to_string(),
                metadata: json!({
                    "line_type": "short",
                    "tier": "A",
                    "trigger_id": "breakout",
                    "trigger_name": "突破信号",
                    "score": score,
                    "reasons": ["突破确认"],
                    "risk_flags": ["量能不足"],
                    "factor_breakdown": [
                        {"name": "trend", "score": 18.5},
                        {"name": "volume", "score": 11.2}
                    ],
                    "supporting_signals": ["breakout"],
                    "matched_setups": [{"id": "breakout", "name": "突破信号"}]
                }),
            }],
        )
        .await
        .unwrap();
        Ok(())
    }

    async fn safety_counts(pool: &PgPool) -> sqlx::Result<BTreeMap<&'static str, i64>> {
        let mut counts = BTreeMap::new();
        for table in [
            "signal_strategy_candidates",
            "signal_strategy_positions",
            "trading_sim_positions",
            "daban_sim_positions",
        ] {
            let query = format!("SELECT COUNT(*) AS count FROM {table}");
            let count: i64 = sqlx::query_scalar(&query).fetch_one(pool).await?;
            counts.insert(table, count);
        }
        Ok(counts)
    }

    fn run_row(
        run_id: Uuid,
        trade_date: NaiveDate,
        support_version: &str,
        started_at: DateTime<Utc>,
    ) -> DecisionSupportRunRow {
        DecisionSupportRunRow {
            run_id,
            trade_date,
            support_version: support_version.to_string(),
            market_snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
            pattern_set_id: None,
            event_brief_version: Some("daily-event-brief-v1".to_string()),
            event_score_enabled: false,
            event_score_limit: 0.0,
            status: "completed".to_string(),
            input_fingerprint: format!("fp-{trade_date}-{support_version}"),
            started_at,
            completed_at: Some(started_at),
            error_message: None,
        }
    }

    fn candidate_row(
        run_id: Uuid,
        code: &str,
        name: &str,
        horizon: &str,
        final_score: f64,
    ) -> DecisionCandidateRow {
        DecisionCandidateRow {
            run_id,
            code: code.to_string(),
            name: name.to_string(),
            horizon: horizon.to_string(),
            base_source: "scan_ranker".to_string(),
            base_score: final_score - 1.4,
            pattern_score: Some(1.2),
            event_adjustment: Some(0.0),
            risk_adjustment: Some(-0.2),
            final_score,
            support_tier: "watch".to_string(),
            facts: json!([{
                "kind": "event_fact",
                "statement": format!("{code} has persisted factual context"),
                "sourceRefs": ["event:brief"]
            }]),
            calculations: json!([{
                "kind": "pattern_similarity",
                "statement": format!("{code} similarity score retained"),
                "sourceRefs": []
            }]),
            inferences: json!([{
                "kind": "impact_hypothesis",
                "statement": format!("{code} may continue if market remains stable"),
                "sourceRefs": []
            }]),
            unknowns: json!([{
                "kind": "missing_status",
                "statement": format!("{code} lacks fresh catalyst confirmation"),
                "sourceRefs": []
            }]),
            risk_flags: json!(["liquidity_watch"]),
            invalidations: json!(["close_below_ma20"]),
            source_refs: json!(["event:brief", "scan:ranked_pool"]),
            created_at: dt(2026, 7, 11, 19),
        }
    }

    fn brief_row(run_id: Uuid, trade_date: NaiveDate, content: &str) -> DecisionBriefRow {
        DecisionBriefRow {
            run_id,
            trade_date,
            content: content.to_string(),
            structured_payload: json!({
                "candidateCount": 1,
                "topCandidates": [{"code": "600000.SH"}]
            }),
            created_at: dt(2026, 7, 11, 20),
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    async fn test_state(pool: PgPool) -> Arc<AppState> {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let redis_client = redis::Client::open(redis_url).unwrap();
        let redis = redis::aio::ConnectionManager::new(redis_client)
            .await
            .unwrap();

        Arc::new(AppState {
            config: Arc::new(Config {
                tushare_token: "test".to_string(),
                database_url: "postgresql://qbot:qbot@127.0.0.1/qbot".to_string(),
                redis_url: "redis://127.0.0.1:6379".to_string(),
                telegram_bot_token: "test".to_string(),
                telegram_webhook_secret: None,
                webhook_url: None,
                stock_alert_channel: None,
                report_channel: None,
                daban_channel: None,
                api_port: 8080,
                api_key: Some("test-key".to_string()),
                ai_api_key: None,
                ai_base_url: "https://api.openai.com/v1".to_string(),
                ai_model: "gpt-4o-mini".to_string(),
                data_proxy: None,
                official_event_feed_url: None,
                official_event_feed_api_key: None,
                official_event_source_id: "official:market_event".to_string(),
                official_event_store_full_content: false,
                enable_gdelt_events: false,
                gdelt_event_query: String::new(),
                gdelt_max_records: 250,
                enable_burst_monitor: false,
                enable_daban_live: false,
                enable_ai_analysis: false,
                enable_chip_dist: false,
                enable_event_score_adjustment: false,
                max_event_score_adjustment: 0.0,
                enable_signal_auto_trading: false,
            }),
            db: pool,
            redis,
            provider: Arc::new(FakeProvider),
            point_in_time_provider: Arc::new(FakePointInTimeProvider),
            pusher: Arc::new(TelegramPusher::new("test".to_string())),
            fetch_job_lock: Arc::new(Mutex::new(())),
            analysis_job_lock: Arc::new(Mutex::new(())),
            scan_job_lock: Arc::new(Mutex::new(())),
            daily_report_job_lock: Arc::new(Mutex::new(())),
            weekly_report_job_lock: Arc::new(Mutex::new(())),
        })
    }

    struct FakeProvider;

    #[async_trait]
    impl DataProvider for FakeProvider {
        fn name(&self) -> &'static str {
            "fake"
        }

        async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
            Ok(Vec::new())
        }

        async fn get_daily_bars_by_date(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<(String, Candle)>> {
            Ok(Vec::new())
        }

        async fn get_daily_bars_for_stock(
            &self,
            _code: &str,
            _start_date: NaiveDate,
            _end_date: NaiveDate,
        ) -> Result<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn get_trading_dates(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<NaiveDate>> {
            Ok(Vec::new())
        }

        async fn get_limit_up_stocks(&self, _trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
            Ok(Vec::new())
        }

        async fn get_index_daily(
            &self,
            _code: &str,
            _trade_date: NaiveDate,
        ) -> Result<Option<IndexData>> {
            Ok(None)
        }

        async fn get_sector_data(&self, _trade_date: NaiveDate) -> Result<Vec<SectorData>> {
            Ok(Vec::new())
        }
    }

    struct FakePointInTimeProvider;

    #[async_trait]
    impl PointInTimeDataProvider for FakePointInTimeProvider {
        async fn probe_capabilities(&self) -> Result<PointInTimeCapabilities> {
            Ok(PointInTimeCapabilities {
                security_master_history: false,
                corporate_actions: false,
                adjustment_factors: false,
                daily_basic: false,
                daily_security_status: false,
                historical_index_bars: false,
                historical_sector_membership: false,
                details: BTreeMap::new(),
            })
        }

        async fn get_security_master_versions(&self) -> Result<Vec<SecurityMasterVersion>> {
            Ok(Vec::new())
        }

        async fn get_corporate_actions(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<CorporateAction>> {
            Ok(Vec::new())
        }

        async fn get_adjustment_factors(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<AdjustmentFactor>> {
            Ok(Vec::new())
        }

        async fn get_daily_basics(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<DailyBasicSnapshot>> {
            Ok(Vec::new())
        }

        async fn get_security_statuses(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<SecurityDailyStatus>> {
            Ok(Vec::new())
        }

        async fn get_index_daily_range(
            &self,
            _codes: &[String],
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<IndexDailyBar>> {
            Ok(Vec::new())
        }

        async fn get_sector_memberships(
            &self,
            _as_of_date: NaiveDate,
        ) -> Result<Vec<SectorMembership>> {
            Ok(Vec::new())
        }
    }
}
