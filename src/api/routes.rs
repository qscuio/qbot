use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::signals::registry::SignalRegistry;
use crate::state::AppState;
use crate::storage::postgres;

type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;

fn api_error(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": msg})))
}

fn check_auth(headers: &HeaderMap, api_key: Option<&str>) -> bool {
    match api_key {
        None => true,
        Some(key) => headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == format!("Bearer {}", key))
            .unwrap_or(false),
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/signals", get(list_signals))
        .route("/api/scan/latest", get(get_scan_latest))
        .route("/api/scan/trigger", post(trigger_scan))
        .route("/api/report/daily", get(get_daily_report))
        .route("/api/market/overview", get(market_overview_stub))
        .route("/api/jobs/fetch", post(trigger_fetch))
        .route("/api/jobs/scan", post(trigger_scan_job))
        .route("/api/jobs/report/daily", post(trigger_daily_report))
        .route("/api/jobs/report/weekly", post(trigger_weekly_report))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "service": "qbot"}))
}

async fn list_signals() -> Json<Value> {
    let signals: Vec<Value> = SignalRegistry::get_enabled()
        .iter()
        .map(|s| json!({
            "id": s.signal_id(),
            "name": s.display_name(),
            "icon": s.icon(),
            "group": s.group(),
        }))
        .collect();
    let count = signals.len();
    Json(json!({"signals": signals, "count": count}))
}

async fn get_scan_latest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }

    let mut cache = crate::storage::redis_cache::RedisCache::new(state.redis.clone());
    match cache.get_scan_results().await {
        Ok(Some(results)) => Ok(Json(results)),
        Ok(None) => Ok(Json(json!({"status": "no_scan_results"}))),
        Err(e) => Err(api_error(&e.to_string())),
    }
}

async fn trigger_scan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }

    let state_clone = state.clone();
    tokio::spawn(async move {
        let scanner = crate::services::scanner::ScannerService::new(state_clone);
        if let Err(e) = scanner.run_full_scan().await {
            tracing::warn!("Manual scan failed: {}", e);
        }
    });

    Ok(Json(json!({"status": "scan_started"})))
}

async fn get_daily_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }

    match postgres::get_latest_report(&state.db, "daily").await {
        Ok(Some(content)) => Ok(Json(json!({"content": content}))),
        Ok(None) => Ok(Json(json!({"status": "no_report_yet"}))),
        Err(e) => Err(api_error(&e.to_string())),
    }
}

async fn market_overview_stub() -> Json<Value> {
    Json(json!({"status": "coming_soon"}))
}

async fn trigger_fetch(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }
    let s = state.clone();
    let p = state.provider.clone();
    tokio::spawn(async move {
        crate::scheduler::run_fetch_job(s, p).await;
    });
    Ok(Json(json!({"status": "started", "job": "fetch"})))
}

async fn trigger_scan_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }
    let s = state.clone();
    tokio::spawn(async move {
        crate::scheduler::run_scan_job(s).await;
    });
    Ok(Json(json!({"status": "started", "job": "scan"})))
}

async fn trigger_daily_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }
    let s = state.clone();
    let p = state.provider.clone();
    let push = state.pusher.clone();
    tokio::spawn(async move {
        crate::scheduler::run_daily_report_job(s, p, push).await;
    });
    Ok(Json(json!({"status": "started", "job": "report/daily"})))
}

async fn trigger_weekly_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }
    let s = state.clone();
    let p = state.provider.clone();
    let push = state.pusher.clone();
    tokio::spawn(async move {
        crate::scheduler::run_weekly_report_job(s, p, push).await;
    });
    Ok(Json(json!({"status": "started", "job": "report/weekly"})))
}
