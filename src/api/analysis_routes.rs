use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use serde_json::{json, Value};

use crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION;
use crate::state::AppState;
use crate::storage::market_repository::{
    AnalysisRunSummary, MarketRepository, POINT_IN_TIME_BACKFILL_RUN_TYPE,
    POINT_IN_TIME_CAPABILITY_PROBE_RUN_TYPE, POINT_IN_TIME_REFERENCE_REFRESH_RUN_TYPE,
    POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
};

type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;

pub fn analysis_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/analysis/data-status", get(get_analysis_data_status))
        .route(
            "/api/jobs/analysis/point-in-time/refresh",
            post(trigger_point_in_time_refresh),
        )
        .route(
            "/api/jobs/analysis/point-in-time/reference-refresh",
            post(trigger_point_in_time_reference_refresh),
        )
        .route("/api/jobs/analysis/snapshot", post(trigger_market_snapshot))
        .with_state(state)
}

async fn get_analysis_data_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    require_auth(&headers, &state)?;

    let repo = MarketRepository::new(state.db.clone());

    let snapshot = repo
        .latest_market_snapshot(MARKET_SNAPSHOT_VERSION)
        .await
        .map_err(|e| crate::api::routes::api_error(&e.to_string()))?;
    let capability_status = repo
        .latest_analysis_run(POINT_IN_TIME_CAPABILITY_PROBE_RUN_TYPE)
        .await
        .map_err(|e| crate::api::routes::api_error(&e.to_string()))?;
    let refresh_runs = repo
        .latest_analysis_runs(&[
            POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
            POINT_IN_TIME_REFERENCE_REFRESH_RUN_TYPE,
            POINT_IN_TIME_BACKFILL_RUN_TYPE,
        ])
        .await
        .map_err(|e| crate::api::routes::api_error(&e.to_string()))?;

    let capability_failures = capability_failures(capability_status.as_ref());
    let capability_probe = capability_probe_state(capability_status.as_ref());
    let estimated_counts = estimated_row_counts(&refresh_runs);

    let (
        trade_date,
        snapshot_trade_date,
        snapshot_version,
        data_complete,
        missing_inputs,
        missing_input_count,
        available_at,
        input_fingerprint,
        snapshot_present,
    ) = match snapshot {
        Some(snapshot) => (
            json!(snapshot.trade_date),
            Some(snapshot.trade_date),
            json!(snapshot.snapshot_version),
            snapshot.data_complete,
            json!(snapshot.missing_inputs),
            json!(snapshot.missing_inputs.len()),
            json!(snapshot.available_at),
            json!(snapshot.input_fingerprint),
            true,
        ),
        None => (
            Value::Null,
            None,
            json!(MARKET_SNAPSHOT_VERSION),
            false,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            false,
        ),
    };

    let refresh_complete = snapshot_trade_date.is_some_and(|snapshot_trade_date| {
        refresh_runs.iter().any(|run| {
            run.run_type == POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE
                && run.status == "ok"
                && run.trade_date == Some(snapshot_trade_date)
        })
    });
    let capabilities_complete = capability_status
        .as_ref()
        .is_some_and(|status| status.status == "ok" && capability_failures.is_empty());

    Ok(Json(json!({
        "tradeDate": trade_date,
        "snapshotVersion": snapshot_version,
        "dataComplete": data_complete,
        "missingInputs": missing_inputs,
        "availableAt": available_at,
        "inputFingerprint": input_fingerprint,
        "capabilityFailures": capability_failures,
        "capabilityProbe": capability_probe,
        "capabilityStatus": capability_status.as_ref().map(analysis_run_summary_json),
        "completeness": {
            "snapshotPresent": snapshot_present,
            "snapshotComplete": data_complete,
            "capabilitiesComplete": capabilities_complete,
            "pointInTimeRefreshComplete": refresh_complete,
            "missingInputCount": missing_input_count,
        },
        "estimatedRowCounts": estimated_counts,
        "latestRuns": refresh_runs
            .iter()
            .map(analysis_run_summary_json)
            .collect::<Vec<_>>(),
    })))
}

async fn trigger_point_in_time_refresh(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    require_auth(&headers, &state)?;
    tokio::spawn(async move {
        crate::scheduler::run_point_in_time_trade_date_refresh_job(state).await;
    });
    Ok(Json(json!({"status": "point_in_time_refresh_started"})))
}

async fn trigger_point_in_time_reference_refresh(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    require_auth(&headers, &state)?;
    tokio::spawn(async move {
        crate::scheduler::run_point_in_time_reference_refresh_job(state).await;
    });
    Ok(Json(
        json!({"status": "point_in_time_reference_refresh_started"}),
    ))
}

async fn trigger_market_snapshot(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    require_auth(&headers, &state)?;
    tokio::spawn(async move {
        crate::scheduler::run_market_snapshot_job(state).await;
    });
    Ok(Json(json!({"status": "market_snapshot_started"})))
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

fn analysis_run_summary_json(run: &AnalysisRunSummary) -> Value {
    json!({
        "runType": run.run_type,
        "status": run.status,
        "details": run.details,
        "errorMessage": run.error_message,
        "startedAt": run.started_at,
        "completedAt": run.completed_at,
    })
}

fn capability_failures(status: Option<&AnalysisRunSummary>) -> Vec<String> {
    match status {
        Some(status) => status
            .details
            .get("missing_capabilities")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

fn capability_probe_state(status: Option<&AnalysisRunSummary>) -> Value {
    match status {
        Some(status) => json!({
            "persisted": true,
            "status": status.status,
            "completed": status.completed_at.is_some(),
        }),
        None => json!({
            "persisted": false,
            "status": "not_persisted",
            "completed": false,
        }),
    }
}

fn estimated_row_counts(runs: &[AnalysisRunSummary]) -> Value {
    let estimated_rows: u64 = runs
        .iter()
        .filter_map(|run| run.details.get("estimated_rows").and_then(Value::as_u64))
        .sum();
    let excluded_estimated_rows: u64 = runs
        .iter()
        .filter_map(|run| {
            run.details
                .get("excluded_estimated_rows")
                .and_then(Value::as_u64)
        })
        .sum();
    let sensitivity_excludes_estimated = runs.iter().any(|run| {
        run.details
            .get("sensitivity_excludes_estimated")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    });

    json!({
        "estimatedRows": estimated_rows,
        "excludedEstimatedRows": excluded_estimated_rows,
        "sensitivityExcludesEstimated": sensitivity_excludes_estimated,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{header, Method, Request, StatusCode};
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::{json, Value};
    use sqlx::PgPool;
    use tokio::sync::Mutex;
    use tower::Service;

    use super::*;
    use crate::analysis::market_snapshot::{
        AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, MarketSnapshot,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
    };
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::provider::DataProvider;
    use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
    use crate::error::Result;
    use crate::storage::market_repository::{
        MarketRepository, POINT_IN_TIME_CAPABILITY_PROBE_RUN_TYPE,
        POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
    };
    use crate::telegram::TelegramPusher;

    #[sqlx::test(migrations = "./migrations")]
    async fn analysis_router_protects_all_analysis_routes(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool).await;

        for (method, path) in [
            (Method::GET, "/api/analysis/data-status"),
            (Method::POST, "/api/jobs/analysis/point-in-time/refresh"),
            (
                Method::POST,
                "/api/jobs/analysis/point-in-time/reference-refresh",
            ),
            (Method::POST, "/api/jobs/analysis/snapshot"),
        ] {
            let mut router = analysis_router(state.clone());
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
    async fn build_router_exposes_analysis_routes(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool).await;

        for (method, path) in [
            (Method::GET, "/api/analysis/data-status"),
            (Method::POST, "/api/jobs/analysis/point-in-time/refresh"),
            (
                Method::POST,
                "/api/jobs/analysis/point-in-time/reference-refresh",
            ),
            (Method::POST, "/api/jobs/analysis/snapshot"),
        ] {
            let mut router = crate::api::routes::build_router(state.clone());
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
    async fn data_status_reports_snapshot_capabilities_and_estimated_rows(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = MarketRepository::new(pool.clone());
        let trade_date = date(2026, 7, 10);
        repo.save_market_snapshot(&MarketSnapshot {
            trade_date,
            snapshot_version: "market-v1".to_string(),
            available_at: Utc.with_ymd_and_hms(2026, 7, 10, 9, 30, 0).unwrap(),
            data_complete: false,
            metrics: json!({"breadth": {"up_count": 12}}),
            missing_inputs: vec!["stock_adjustment_factors:600000.SH:2026-07-10".to_string()],
            input_fingerprint: "abc123".to_string(),
        })
        .await
        .unwrap();
        repo.record_analysis_data_run(
            POINT_IN_TIME_CAPABILITY_PROBE_RUN_TYPE,
            None,
            "missing",
            json!({
                "missing_capabilities": ["corporate_actions"],
                "capabilities": {"daily_basic": true}
            }),
            None,
        )
        .await
        .unwrap();
        repo.record_analysis_data_run(
            POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
            Some(trade_date),
            "ok",
            json!({
                "estimated_rows": 7,
                "excluded_estimated_rows": 3,
                "sensitivity_excludes_estimated": true
            }),
            None,
        )
        .await
        .unwrap();

        let mut router = analysis_router(state);
        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/data-status")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(payload["tradeDate"], "2026-07-10");
        assert_eq!(payload["snapshotVersion"], "market-v1");
        assert_eq!(payload["dataComplete"], false);
        assert_eq!(payload["availableAt"], "2026-07-10T09:30:00Z");
        assert_eq!(payload["inputFingerprint"], "abc123");
        assert_eq!(
            payload["missingInputs"],
            json!(["stock_adjustment_factors:600000.SH:2026-07-10"])
        );
        assert_eq!(payload["capabilityFailures"], json!(["corporate_actions"]));
        assert_eq!(payload["completeness"]["snapshotComplete"], false);
        assert_eq!(payload["completeness"]["pointInTimeRefreshComplete"], true);
        assert_eq!(payload["estimatedRowCounts"]["estimatedRows"], 7);
        assert_eq!(payload["estimatedRowCounts"]["excludedEstimatedRows"], 3);
        assert_eq!(
            payload["estimatedRowCounts"]["sensitivityExcludesEstimated"],
            true
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn data_status_reports_missing_capability_probe_without_capability_failure(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool).await;

        let mut router = analysis_router(state);
        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/data-status")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(payload["capabilityFailures"], json!([]));
        assert_eq!(payload["capabilityStatus"], Value::Null);
        assert_eq!(payload["capabilityProbe"]["persisted"], false);
        assert_eq!(payload["capabilityProbe"]["status"], "not_persisted");
        assert_eq!(payload["completeness"]["capabilitiesComplete"], false);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn data_status_does_not_guess_missing_inputs_without_snapshot(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = MarketRepository::new(pool.clone());
        repo.record_analysis_data_run(
            POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
            Some(date(2026, 7, 10)),
            "ok",
            json!({"estimated_rows": 7}),
            None,
        )
        .await
        .unwrap();

        let mut router = analysis_router(state);
        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/data-status")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(payload["tradeDate"], Value::Null);
        assert_eq!(payload["missingInputs"], Value::Null);
        assert_eq!(payload["completeness"]["snapshotPresent"], false);
        assert_eq!(payload["completeness"]["pointInTimeRefreshComplete"], false);
        assert_eq!(payload["completeness"]["missingInputCount"], Value::Null);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn data_status_scopes_refresh_completeness_to_snapshot_trade_date(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = MarketRepository::new(pool.clone());
        let snapshot_trade_date = date(2026, 7, 10);
        repo.save_market_snapshot(&MarketSnapshot {
            trade_date: snapshot_trade_date,
            snapshot_version: "market-v1".to_string(),
            available_at: Utc.with_ymd_and_hms(2026, 7, 10, 9, 30, 0).unwrap(),
            data_complete: true,
            metrics: json!({"breadth": {"up_count": 12}}),
            missing_inputs: Vec::new(),
            input_fingerprint: "abc123".to_string(),
        })
        .await
        .unwrap();
        repo.record_analysis_data_run(
            POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
            Some(date(2026, 7, 11)),
            "ok",
            json!({"estimated_rows": 7}),
            None,
        )
        .await
        .unwrap();

        let mut router = analysis_router(state);
        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/data-status")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(payload["tradeDate"], "2026-07-10");
        assert_eq!(payload["completeness"]["pointInTimeRefreshComplete"], false);

        Ok(())
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
                enable_burst_monitor: false,
                enable_daban_live: false,
                enable_ai_analysis: false,
                enable_chip_dist: false,
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

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
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
                security_master_history: true,
                corporate_actions: true,
                adjustment_factors: true,
                daily_basic: true,
                daily_security_status: true,
                historical_index_bars: true,
                historical_sector_membership: true,
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
