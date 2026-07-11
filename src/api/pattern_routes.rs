use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION;
use crate::state::AppState;
use crate::storage::market_repository::MarketRepository;
use crate::storage::pattern_repository::{PatternRepository, ShadowCandidateRow};
use crate::storage::postgres;

type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;

#[derive(Debug, Deserialize)]
struct ShadowCandidateQuery {
    date: Option<String>,
    limit: Option<usize>,
}

pub fn pattern_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/analysis/patterns/shadow", get(list_shadow_candidates))
        .route(
            "/api/analysis/patterns/shadow/:code",
            get(list_shadow_candidates_by_code),
        )
        .route(
            "/api/jobs/analysis/pattern-match",
            post(trigger_pattern_match),
        )
        .with_state(state)
}

async fn list_shadow_candidates(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ShadowCandidateQuery>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let trade_date = resolve_shadow_trade_date(&state, query.date.as_deref()).await?;
    let candidates = match trade_date {
        Some(trade_date) => {
            let repo = PatternRepository::new(state.db.clone());
            let mut candidates = repo
                .list_shadow_candidates(trade_date)
                .await
                .map_err(|e| crate::api::routes::api_error(&e.to_string()))?;
            apply_limit(&mut candidates, query.limit);
            candidates
        }
        None => Vec::new(),
    };

    Ok(Json(json!({
        "tradeDate": trade_date,
        "source": "analysis_shadow_candidates",
        "count": candidates.len(),
        "candidates": candidates.iter().map(shadow_candidate_json).collect::<Vec<_>>(),
    })))
}

async fn list_shadow_candidates_by_code(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(code): Path<String>,
    Query(query): Query<ShadowCandidateQuery>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let trade_date = resolve_shadow_trade_date(&state, query.date.as_deref()).await?;
    let candidates = match trade_date {
        Some(trade_date) => {
            let repo = PatternRepository::new(state.db.clone());
            let mut candidates: Vec<ShadowCandidateRow> = repo
                .list_shadow_candidates(trade_date)
                .await
                .map_err(|e| crate::api::routes::api_error(&e.to_string()))?
                .into_iter()
                .filter(|candidate| candidate.code == code)
                .collect();
            apply_limit(&mut candidates, query.limit);
            candidates
        }
        None => Vec::new(),
    };

    Ok(Json(json!({
        "tradeDate": trade_date,
        "code": code,
        "source": "analysis_shadow_candidates",
        "count": candidates.len(),
        "candidates": candidates.iter().map(shadow_candidate_json).collect::<Vec<_>>(),
    })))
}

async fn trigger_pattern_match(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    require_auth(&headers, &state)?;
    tokio::spawn(async move {
        crate::scheduler::run_pattern_shadow_job(state).await;
    });
    Ok(Json(json!({"status": "pattern_match_started"})))
}

async fn resolve_shadow_trade_date(
    state: &AppState,
    raw_date: Option<&str>,
) -> std::result::Result<Option<NaiveDate>, (StatusCode, Json<Value>)> {
    if let Some(raw_date) = raw_date {
        let parsed = NaiveDate::parse_from_str(raw_date, "%Y-%m-%d").map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "date must use YYYY-MM-DD"})),
            )
        })?;
        return Ok(Some(parsed));
    }

    match postgres::latest_stock_trade_date(&state.db).await {
        Ok(Some(trade_date)) => Ok(Some(trade_date)),
        Ok(None) => {
            let repo = MarketRepository::new(state.db.clone());
            repo.latest_market_snapshot(MARKET_SNAPSHOT_VERSION)
                .await
                .map(|snapshot| snapshot.map(|snapshot| snapshot.trade_date))
                .map_err(|e| crate::api::routes::api_error(&e.to_string()))
        }
        Err(e) => Err(crate::api::routes::api_error(&e.to_string())),
    }
}

fn apply_limit(candidates: &mut Vec<ShadowCandidateRow>, limit: Option<usize>) {
    if let Some(limit) = limit {
        candidates.truncate(limit);
    }
}

fn shadow_candidate_json(candidate: &ShadowCandidateRow) -> Value {
    json!({
        "tradeDate": candidate.trade_date,
        "code": candidate.code,
        "name": candidate.name,
        "horizon": candidate.horizon,
        "patternVersionId": candidate.pattern_version_id.to_string(),
        "patternSetId": candidate.pattern_set_id.to_string(),
        "patternType": candidate.pattern_type,
        "similarityScore": candidate.similarity_score,
        "validatedLift": candidate.validated_lift,
        "finalScore": candidate.final_score,
        "shadowTier": candidate.shadow_tier,
        "matchedFeatures": candidate.matched_features,
        "riskFlags": candidate.risk_flags,
        "supportingSignals": candidate.supporting_signals,
        "invalidations": candidate.invalidations,
        "inputFingerprint": candidate.input_fingerprint,
        "createdAt": candidate.created_at,
    })
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
    use crate::analysis::market_snapshot::MARKET_SNAPSHOT_VERSION;
    use crate::analysis::market_snapshot::{
        AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, MarketSnapshot,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
    };
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::provider::DataProvider;
    use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
    use crate::error::Result;
    use crate::state::AppState;
    use crate::storage::market_repository::MarketRepository;
    use crate::storage::pattern_repository::{PatternRepository, ShadowCandidateRow};
    use crate::telegram::TelegramPusher;

    #[sqlx::test(migrations = "./migrations")]
    async fn pattern_router_protects_shadow_routes(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool).await;

        for (method, path) in [
            (Method::GET, "/api/analysis/patterns/shadow"),
            (Method::GET, "/api/analysis/patterns/shadow/600001.SH"),
            (Method::POST, "/api/jobs/analysis/pattern-match"),
        ] {
            let mut router = pattern_router(state.clone());
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
    async fn list_shadow_candidates_reads_persisted_rows_for_explicit_date_and_limit(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let pattern_repo = PatternRepository::new(pool.clone());
        let trade_date = date(2026, 7, 10);
        let pattern_version_id = seed_published_pattern(&pool, "dataset-shadow-list").await?;
        let pattern_set_id = seed_published_pattern_set(&pool, pattern_version_id).await?;
        pattern_repo
            .upsert_shadow_candidates(&[
                shadow_row(
                    trade_date,
                    "600001.SH",
                    "Alpha Bank",
                    pattern_version_id,
                    pattern_set_id,
                    2.0,
                    "shadow_a",
                ),
                shadow_row(
                    trade_date,
                    "600002.SH",
                    "Beta Steel",
                    pattern_version_id,
                    pattern_set_id,
                    1.0,
                    "watch",
                ),
            ])
            .await
            .unwrap();

        let mut router = pattern_router(state);
        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/patterns/shadow?date=2026-07-10&limit=1")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["tradeDate"], "2026-07-10");
        assert_eq!(payload["source"], "analysis_shadow_candidates");
        assert_eq!(payload["candidates"].as_array().unwrap().len(), 1);
        assert_eq!(payload["candidates"][0]["code"], "600001.SH");
        assert_eq!(payload["candidates"][0]["name"], "Alpha Bank");
        assert_eq!(payload["candidates"][0]["shadowTier"], "shadow_a");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn code_shadow_candidates_filter_by_code_and_use_latest_stock_trade_date(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let pattern_repo = PatternRepository::new(pool.clone());
        let market_repo = MarketRepository::new(pool.clone());
        let trade_date = date(2026, 7, 11);
        seed_stock_daily_bar(&pool, trade_date, "600001.SH").await?;
        market_repo
            .save_market_snapshot(&MarketSnapshot {
                trade_date: date(2026, 7, 10),
                snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                available_at: dt(2026, 7, 10, 10),
                data_complete: true,
                metrics: json!({"market_regime": "normal"}),
                missing_inputs: Vec::new(),
                input_fingerprint: "snapshot-fp".to_string(),
            })
            .await
            .unwrap();

        let pattern_version_id = seed_published_pattern(&pool, "dataset-shadow-code").await?;
        let pattern_set_id = seed_published_pattern_set(&pool, pattern_version_id).await?;
        pattern_repo
            .upsert_shadow_candidates(&[
                shadow_row(
                    trade_date,
                    "600001.SH",
                    "Alpha Bank",
                    pattern_version_id,
                    pattern_set_id,
                    2.0,
                    "shadow_b",
                ),
                shadow_row(
                    trade_date,
                    "600002.SH",
                    "Beta Steel",
                    pattern_version_id,
                    pattern_set_id,
                    3.0,
                    "shadow_a",
                ),
            ])
            .await
            .unwrap();

        let mut router = pattern_router(state);
        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/patterns/shadow/600001.SH")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["tradeDate"], "2026-07-11");
        assert_eq!(payload["code"], "600001.SH");
        assert_eq!(payload["candidates"].as_array().unwrap().len(), 1);
        assert_eq!(payload["candidates"][0]["code"], "600001.SH");

        Ok(())
    }

    #[test]
    fn pattern_routes_do_not_reference_auto_trading_candidate_table() {
        let source = include_str!("pattern_routes.rs");
        let forbidden_table = concat!("signal", "_strategy", "_candidates");
        assert!(!source.contains(forbidden_table));
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    async fn seed_stock_daily_bar(
        pool: &PgPool,
        trade_date: NaiveDate,
        code: &str,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO stock_daily_bars
               (code, trade_date, open, high, low, close, volume, amount)
               VALUES ($1, $2, 10, 11, 9, 10.5, 1000, 10000)"#,
        )
        .bind(code)
        .bind(trade_date)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_published_pattern(pool: &PgPool, dataset_version: &str) -> sqlx::Result<Uuid> {
        sqlx::query(
            r#"INSERT INTO analysis_dataset_manifests
               (dataset_version, schema_version, feature_version, horizon, data_cutoff,
                available_at_cutoff, row_count, date_from, date_to, manifest, input_fingerprint)
               VALUES ($1, '1', 'feature-v1', 'week', '2026-06-30', '2026-07-01T00:00:00Z',
                       10, '2026-01-01', '2026-06-30', '{"files":["x.parquet"]}', 'fp-1')"#,
        )
        .bind(dataset_version)
        .execute(pool)
        .await?;

        let pattern_version_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO analysis_pattern_versions
               (pattern_version_id, pattern_id, horizon, pattern_type, status,
                schema_version, feature_version, logic_version, dataset_version,
                model_payload, validation_payload, trained_from, trained_until,
                available_at_cutoff, approved_by, published_at)
               VALUES ($1, $2, 'week', 'trend', 'published',
                       '1', 'feature-v1', 'logic-v1', $3,
                       '{"centroid":{"close_strength":1.1}}',
                       '{"lift":0.42}',
                       '2026-01-01', '2026-06-30', '2026-07-01T00:00:00Z',
                       'reviewer', '2026-07-10T08:00:00Z')"#,
        )
        .bind(pattern_version_id)
        .bind(format!("pattern-{dataset_version}"))
        .bind(dataset_version)
        .execute(pool)
        .await?;

        Ok(pattern_version_id)
    }

    async fn seed_published_pattern_set(
        pool: &PgPool,
        pattern_version_id: Uuid,
    ) -> sqlx::Result<Uuid> {
        let pattern_set_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO analysis_pattern_sets (pattern_set_id, name, status, published_at)
               VALUES ($1, 'published-set', 'published', '2026-07-10T09:00:00Z')"#,
        )
        .bind(pattern_set_id)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO analysis_pattern_set_members
               (pattern_set_id, pattern_version_id, member_order)
               VALUES ($1, $2, 1)"#,
        )
        .bind(pattern_set_id)
        .bind(pattern_version_id)
        .execute(pool)
        .await?;
        Ok(pattern_set_id)
    }

    fn shadow_row(
        trade_date: NaiveDate,
        code: &str,
        name: &str,
        pattern_version_id: Uuid,
        pattern_set_id: Uuid,
        final_score: f64,
        shadow_tier: &str,
    ) -> ShadowCandidateRow {
        ShadowCandidateRow {
            trade_date,
            code: code.to_string(),
            name: Some(name.to_string()),
            horizon: "week".to_string(),
            pattern_version_id,
            pattern_set_id,
            pattern_type: "trend".to_string(),
            similarity_score: 0.71,
            validated_lift: 0.42,
            final_score,
            shadow_tier: shadow_tier.to_string(),
            matched_features: json!({"close_strength": 1.1}),
            risk_flags: json!([]),
            supporting_signals: json!({"shadow_tier": shadow_tier}),
            invalidations: json!([]),
            input_fingerprint: format!("shadow-fp-{code}-{final_score}"),
            created_at: dt(2026, 7, 10, 10),
        }
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
