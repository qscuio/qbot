use axum::extract::DefaultBodyLimit;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::json;
use std::{str::FromStr, sync::Arc};
use tower_http::services::ServeDir;

use crate::services::dashboard::{DashboardPeriod, DashboardService};
use crate::services::dashboard_auth::{
    session_token_from_cookie, DashboardAuth, SESSION_COOKIE_NAME, SESSION_TTL_SECONDS,
};
use crate::services::dashboard_company::DashboardCompanyService;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DashboardDetailQuery {
    period: Option<String>,
    days: Option<usize>,
}

impl DashboardDetailQuery {
    fn parsed_period(&self) -> crate::error::Result<DashboardPeriod> {
        DashboardPeriod::from_str(self.period.as_deref().unwrap_or("daily"))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct DashboardFinancialQuery {
    frequency: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
}

impl DashboardFinancialQuery {
    fn parsed_frequency(&self) -> crate::error::Result<crate::data::company::FinancialFrequency> {
        crate::data::company::FinancialFrequency::from_storage(
            self.frequency.as_deref().unwrap_or("annual"),
        )
        .ok_or_else(|| {
            crate::error::AppError::BadRequest("frequency must be annual or quarterly".to_string())
        })
    }

    fn parsed_limit(&self) -> crate::error::Result<usize> {
        parsed_company_page_limit(self.limit)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct DashboardDividendQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

impl DashboardDividendQuery {
    fn parsed_limit(&self) -> crate::error::Result<usize> {
        parsed_company_page_limit(self.limit)
    }
}

fn parsed_company_page_limit(value: Option<usize>) -> crate::error::Result<usize> {
    match value {
        None => Ok(50),
        Some(value) if (1..=100).contains(&value) => Ok(value),
        Some(_) => Err(crate::error::AppError::BadRequest(
            "limit must be between 1 and 100".to_string(),
        )),
    }
}

fn parse_dashboard_query<T: DeserializeOwned>(uri: &Uri) -> Result<T, Response> {
    Query::<T>::try_from_uri(uri)
        .map(|Query(query)| query)
        .map_err(|_| {
            private_no_store(
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid query parameters"})),
                )
                    .into_response(),
            )
        })
}

pub fn dashboard_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/dashboard/auth/login", post(login))
        .route("/api/dashboard/auth/logout", post(logout))
        .route("/api/dashboard/auth/session", get(session))
        .route("/api/dashboard/bootstrap", get(bootstrap))
        .route("/api/dashboard/stocks/:code", get(stock_detail))
        .route("/api/dashboard/stocks/:code/company", get(stock_company))
        .route(
            "/api/dashboard/stocks/:code/financials",
            get(stock_financials),
        )
        .route(
            "/api/dashboard/stocks/:code/dividends",
            get(stock_dividends),
        )
        .nest_service(
            "/dashboard",
            ServeDir::new("web/dashboard").append_index_html_on_directories(true),
        )
        .layer(DefaultBodyLimit::max(16 * 1024))
        .with_state(state)
}

fn auth_for(state: &AppState) -> Result<DashboardAuth, Response> {
    DashboardAuth::from_config(&state.config, state.redis.clone()).ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "dashboard is not configured"})),
        )
            .into_response()
    })
}

fn cookie_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(session_token_from_cookie)
}

async fn authorized(state: &AppState, headers: &HeaderMap) -> Result<DashboardAuth, Response> {
    let auth = auth_for(state)?;
    let Some(token) = cookie_token(headers) else {
        return Err(unauthorized());
    };
    match auth.authenticate(token).await {
        Ok(true) => Ok(auth),
        Ok(false) => Err(unauthorized()),
        Err(error) => Err(internal_error(&error.to_string())),
    }
}

async fn login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<LoginRequest>,
) -> Response {
    let auth = match auth_for(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    if !valid_origin(&state, &headers) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "invalid request origin"})),
        )
            .into_response();
    }
    let client_key = client_key(&headers);
    match auth.is_throttled(&client_key).await {
        Ok(true) => {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"error": "too many login attempts"})),
            )
                .into_response()
        }
        Err(error) => return internal_error(&error.to_string()),
        Ok(false) => {}
    }
    if !auth.verify_credentials(request.username.trim(), &request.password) {
        if let Err(error) = auth.record_failure(&client_key).await {
            return internal_error(&error.to_string());
        }
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Invalid credentials"})),
        )
            .into_response();
    }
    if let Err(error) = auth.clear_failures(&client_key).await {
        return internal_error(&error.to_string());
    }
    let token = match auth.create_session().await {
        Ok(token) => token,
        Err(error) => return internal_error(&error.to_string()),
    };
    let cookie = format!(
        "{SESSION_COOKIE_NAME}={token}; Path=/; Max-Age={SESSION_TTL_SECONDS}; Secure; HttpOnly; SameSite=Strict"
    );
    let mut response = Json(json!({"authenticated": true})).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie).expect("session cookie contains safe characters"),
    );
    response
}

async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !valid_origin(&state, &headers) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "invalid request origin"})),
        )
            .into_response();
    }
    let auth = match authorized(&state, &headers).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    if let Some(token) = cookie_token(&headers) {
        if let Err(error) = auth.logout(token).await {
            return internal_error(&error.to_string());
        }
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "{SESSION_COOKIE_NAME}=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Strict"
        ))
        .expect("session cookie name is a valid header value"),
    );
    response
}

async fn session(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match authorized(&state, &headers).await {
        Ok(_) => Json(json!({"authenticated": true})).into_response(),
        Err(response) => response,
    }
}

async fn bootstrap(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(response) = authorized(&state, &headers).await {
        return response;
    }
    match DashboardService::new(state).bootstrap().await {
        Ok(payload) => Json(payload).into_response(),
        Err(error) => internal_error(&error.to_string()),
    }
}

async fn stock_detail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(code): Path<String>,
    Query(query): Query<DashboardDetailQuery>,
) -> Response {
    if let Err(response) = authorized(&state, &headers).await {
        return response;
    }
    let period = match query.parsed_period() {
        Ok(period) => period,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": error.to_string()})),
            )
                .into_response()
        }
    };
    match DashboardService::new(state)
        .stock_detail(&code, period, query.days)
        .await
    {
        Ok(payload) => Json(payload).into_response(),
        Err(crate::error::AppError::NotFound(message)) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": message}))).into_response()
        }
        Err(error) => internal_error(&error.to_string()),
    }
}

fn company_service(state: &AppState) -> Result<DashboardCompanyService, Response> {
    let secret = state
        .config
        .dashboard_session_secret
        .clone()
        .ok_or_else(|| internal_error("dashboard cursor secret is not configured"))?;
    Ok(DashboardCompanyService::new(state.db.clone(), secret))
}

async fn stock_company(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(code): Path<String>,
) -> Response {
    if let Err(response) = authorized(&state, &headers).await {
        return private_no_store(response);
    }
    let service = match company_service(&state) {
        Ok(service) => service,
        Err(response) => return private_no_store(response),
    };
    private_no_store(match service.company(&code).await {
        Ok(payload) => Json(payload).into_response(),
        Err(error) => dashboard_company_error(error),
    })
}

async fn stock_financials(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(code): Path<String>,
    uri: Uri,
) -> Response {
    if let Err(response) = authorized(&state, &headers).await {
        return private_no_store(response);
    }
    let query = match parse_dashboard_query::<DashboardFinancialQuery>(&uri) {
        Ok(query) => query,
        Err(response) => return response,
    };
    let frequency = match query.parsed_frequency() {
        Ok(frequency) => frequency,
        Err(error) => return private_no_store(dashboard_company_error(error)),
    };
    let limit = match query.parsed_limit() {
        Ok(limit) => limit,
        Err(error) => return private_no_store(dashboard_company_error(error)),
    };
    let service = match company_service(&state) {
        Ok(service) => service,
        Err(response) => return private_no_store(response),
    };
    private_no_store(
        match service
            .financials(&code, frequency, limit, query.cursor.as_deref())
            .await
        {
            Ok(payload) => Json(payload).into_response(),
            Err(error) => dashboard_company_error(error),
        },
    )
}

async fn stock_dividends(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(code): Path<String>,
    uri: Uri,
) -> Response {
    if let Err(response) = authorized(&state, &headers).await {
        return private_no_store(response);
    }
    let query = match parse_dashboard_query::<DashboardDividendQuery>(&uri) {
        Ok(query) => query,
        Err(response) => return response,
    };
    let service = match company_service(&state) {
        Ok(service) => service,
        Err(response) => return private_no_store(response),
    };
    let limit = match query.parsed_limit() {
        Ok(limit) => limit,
        Err(error) => return private_no_store(dashboard_company_error(error)),
    };
    private_no_store(
        match service
            .dividends(&code, limit, query.cursor.as_deref())
            .await
        {
            Ok(payload) => Json(payload).into_response(),
            Err(error) => dashboard_company_error(error),
        },
    )
}

fn private_no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    response
}

fn dashboard_company_error(error: crate::error::AppError) -> Response {
    match error {
        crate::error::AppError::BadRequest(message) => {
            (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
        }
        crate::error::AppError::NotFound(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "stock not found"})),
        )
            .into_response(),
        error => internal_error(&error.to_string()),
    }
}

fn valid_origin(state: &AppState, headers: &HeaderMap) -> bool {
    let Some(expected) = state.config.dashboard_public_url.as_deref() else {
        return false;
    };
    headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|origin| origin.trim_end_matches('/') == expected.trim_end_matches('/'))
}

fn client_key(headers: &HeaderMap) -> String {
    headers
        .get("cf-connecting-ip")
        .or_else(|| headers.get("x-forwarded-for"))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "unauthorized"})),
    )
        .into_response()
}

fn internal_error(message: &str) -> Response {
    tracing::warn!("dashboard request failed: {message}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "dashboard request failed"})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use serde_json::Value;
    use sqlx::PgPool;
    use tokio::sync::Mutex;
    use tower::Service;
    use uuid::Uuid;

    use crate::config::Config;
    use crate::data::tushare::TushareClient;
    use crate::telegram::pusher::TelegramPusher;

    #[test]
    fn detail_query_rejects_unknown_period() {
        let query = DashboardDetailQuery {
            period: Some("hourly".to_string()),
            days: None,
        };

        assert!(query.parsed_period().is_err());
    }

    #[test]
    fn detail_query_defaults_to_daily() {
        let query = DashboardDetailQuery {
            period: None,
            days: None,
        };

        assert_eq!(query.parsed_period().unwrap(), DashboardPeriod::Daily);
    }

    async fn test_state(pool: PgPool) -> Arc<AppState> {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let redis_client = redis::Client::open(redis_url.clone()).unwrap();
        let redis = redis::aio::ConnectionManager::new(redis_client)
            .await
            .unwrap();
        let session_secret = format!("dashboard-route-test-secret-{}", Uuid::new_v4());
        let provider = Arc::new(TushareClient::new("test".to_string(), None));
        Arc::new(AppState {
            config: Arc::new(Config {
                tushare_token: "test".to_string(),
                database_url: "postgresql://qbot:qbot@127.0.0.1/qbot".to_string(),
                redis_url,
                telegram_bot_token: "test".to_string(),
                telegram_webhook_secret: None,
                webhook_url: None,
                stock_alert_channel: None,
                report_channel: None,
                daban_channel: None,
                api_port: 8080,
                api_key: Some("test-key".to_string()),
                dashboard_public_url: Some("https://dash.example.test".to_string()),
                dashboard_username: Some("analyst".to_string()),
                dashboard_password_hash: Some("$argon2-test".to_string()),
                dashboard_session_secret: Some(session_secret),
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
            provider: provider.clone(),
            point_in_time_provider: provider,
            pusher: Arc::new(TelegramPusher::new("test".to_string())),
            fetch_job_lock: Arc::new(Mutex::new(())),
            analysis_job_lock: Arc::new(Mutex::new(())),
            scan_job_lock: Arc::new(Mutex::new(())),
            daily_report_job_lock: Arc::new(Mutex::new(())),
            weekly_report_job_lock: Arc::new(Mutex::new(())),
        })
    }

    async fn authenticated_cookie(state: &AppState) -> String {
        let auth = DashboardAuth::from_config(&state.config, state.redis.clone()).unwrap();
        let token = auth.create_session().await.unwrap();
        format!("{SESSION_COOKIE_NAME}={token}")
    }

    fn headers(cookie: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(cookie) = cookie {
            headers.insert(header::COOKIE, HeaderValue::from_str(cookie).unwrap());
        }
        headers
    }

    fn assert_private_no_store(response: &Response) {
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("private, no-store"))
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn company_routes_authenticate_before_lookup_and_map_validation_and_existence(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        sqlx::query(
            r#"INSERT INTO stock_info (code, name, market, industry)
               VALUES ('600519.SH', '贵州茅台', 'SH', '白酒')"#,
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO security_master_versions
               (code, name, market, exchange, list_status, list_date, available_at,
                availability_quality, source)
               VALUES ('600519.SH', '贵州茅台', '主板', 'SSE', 'L', '2001-08-27',
                       '2026-01-01T00:00:00Z', 'observed', 'tushare')"#,
        )
        .execute(&pool)
        .await?;
        let state = test_state(pool).await;

        let unauthenticated = stock_financials(
            State(state.clone()),
            headers(None),
            Path("999999.SH".to_string()),
            "/?frequency=monthly".parse().unwrap(),
        )
        .await;
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        assert_private_no_store(&unauthenticated);
        let cookie = authenticated_cookie(&state).await;
        for uri in [
            "/?frequency=monthly",
            "/?limit=0",
            "/?limit=not-a-number",
            "/?cursor=invalid",
        ] {
            let response = stock_financials(
                State(state.clone()),
                headers(Some(&cookie)),
                Path("600519.SH".to_string()),
                uri.parse().unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_private_no_store(&response);
        }
        let missing = stock_company(
            State(state.clone()),
            headers(Some(&cookie)),
            Path("999999.SH".to_string()),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_private_no_store(&missing);
        let company = stock_company(
            State(state.clone()),
            headers(Some(&cookie)),
            Path("600519.SH".to_string()),
        )
        .await;
        assert_eq!(company.status(), StatusCode::OK);
        assert_private_no_store(&company);
        let financials = stock_financials(
            State(state.clone()),
            headers(Some(&cookie)),
            Path("600519.SH".to_string()),
            "/?frequency=annual&limit=10".parse().unwrap(),
        )
        .await;
        assert_eq!(financials.status(), StatusCode::OK);
        assert_private_no_store(&financials);
        let dividends = stock_dividends(
            State(state),
            headers(Some(&cookie)),
            Path("600519.SH".to_string()),
            "/?limit=10".parse().unwrap(),
        )
        .await;
        assert_eq!(dividends.status(), StatusCode::OK);
        assert_private_no_store(&dividends);
        Ok(())
    }

    async fn router_get(router: &mut Router, uri: &str, cookie: Option<&str>) -> Response {
        let mut request = Request::builder().uri(uri);
        if let Some(cookie) = cookie {
            request = request.header(header::COOKIE, cookie);
        }
        router
            .call(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn assert_safe_query_error(response: Response) {
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_private_no_store(&response);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload, json!({"error": "invalid query parameters"}));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn router_authenticates_before_rejecting_malformed_company_queries(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        sqlx::query(
            r#"INSERT INTO stock_info (code, name, market, industry)
               VALUES ('600519.SH', '贵州茅台', 'SH', '白酒')"#,
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO security_master_versions
               (code, name, market, exchange, list_status, list_date, available_at,
                availability_quality, source)
               VALUES ('600519.SH', '贵州茅台', '主板', 'SSE', 'L', '2001-08-27',
                       '2026-01-01T00:00:00Z', 'observed', 'tushare')"#,
        )
        .execute(&pool)
        .await?;
        let state = test_state(pool).await;
        let cookie = authenticated_cookie(&state).await;
        let mut router = dashboard_router(state);

        for uri in [
            "/api/dashboard/stocks/999999.SH/financials?limit=10&limit=11",
            "/api/dashboard/stocks/999999.SH/dividends?limit=%FF",
        ] {
            let response = router_get(&mut router, uri, None).await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
            assert_private_no_store(&response);
        }

        let duplicate = router_get(
            &mut router,
            "/api/dashboard/stocks/600519.SH/financials?limit=10&limit=11",
            Some(&cookie),
        )
        .await;
        assert_safe_query_error(duplicate).await;

        let malformed = router_get(
            &mut router,
            "/api/dashboard/stocks/600519.SH/dividends?limit=%FF",
            Some(&cookie),
        )
        .await;
        assert_safe_query_error(malformed).await;

        for uri in [
            "/api/dashboard/stocks/600519.SH/financials?frequency=annual&limit=10",
            "/api/dashboard/stocks/600519.SH/dividends?limit=10",
        ] {
            let response = router_get(&mut router, uri, Some(&cookie)).await;
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert_private_no_store(&response);
        }
        Ok(())
    }
}
