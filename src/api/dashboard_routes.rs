use axum::extract::DefaultBodyLimit;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::{str::FromStr, sync::Arc};
use tower_http::services::ServeDir;

use crate::services::dashboard::{DashboardPeriod, DashboardService};
use crate::services::dashboard_auth::{
    session_token_from_cookie, DashboardAuth, SESSION_COOKIE_NAME, SESSION_TTL_SECONDS,
};
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

pub fn dashboard_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/dashboard/auth/login", post(login))
        .route("/api/dashboard/auth/logout", post(logout))
        .route("/api/dashboard/auth/session", get(session))
        .route("/api/dashboard/bootstrap", get(bootstrap))
        .route("/api/dashboard/stocks/:code", get(stock_detail))
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
}
