use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::analysis::events::{EventIntelligence, EventReviewAction, ManualEventInput};
use crate::error::AppError;
use crate::state::AppState;

type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManualEventRequest {
    title: Option<String>,
    content: Option<String>,
    source_url: Option<String>,
    submitted_by: Option<String>,
    published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventReviewRequest {
    action: Option<String>,
    reviewed_by: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DailyBriefQuery {
    date: Option<String>,
}

pub fn event_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/analysis/events/manual", post(submit_manual_event))
        .route("/api/analysis/events", get(list_events))
        .route("/api/analysis/events/daily-brief", get(get_daily_brief))
        .route(
            "/api/analysis/events/market-logic-brief",
            get(get_market_logic_brief),
        )
        .route(
            "/api/analysis/events/:id/evolution",
            get(get_event_evolution),
        )
        .route(
            "/api/analysis/events/:id/hypothesis",
            get(get_event_hypothesis),
        )
        .route(
            "/api/analysis/events/:id/market-observations",
            get(get_event_market_observations),
        )
        .route("/api/analysis/events/:id", get(get_event_detail))
        .route("/api/analysis/events/:id/review", post(review_event))
        .with_state(state)
}

struct ManualSubmissionResponseFacts {
    source_readable: Option<bool>,
    manual_review_needed: Option<bool>,
}

pub(crate) async fn handle_telegram_submit_event(
    state: Arc<AppState>,
    chat_id: i64,
    user_id: i64,
    args: &str,
) -> crate::error::Result<()> {
    let title = args.trim();
    if title.is_empty() {
        return telegram_send(
            &state,
            chat_id,
            "用法: <code>/event &lt;文本或链接&gt;</code>",
        )
        .await;
    }

    let intelligence = EventIntelligence::new(state.db.clone());
    let source_url = looks_like_absolute_url(title).then(|| title.to_string());
    let content = (!looks_like_absolute_url(title)).then(|| title.to_string());
    let response_facts = manual_submission_response_facts(content.as_deref());
    let outcome = intelligence
        .submit_manual_event(ManualEventInput {
            title: title.to_string(),
            content,
            source_url,
            submitted_by: format!("telegram:{user_id}"),
            published_at: None,
        })
        .await?;
    let response = manual_submission_response(&outcome, &response_facts);

    telegram_send(
        &state,
        chat_id,
        &format!(
            "🗂️ <b>事件已提交</b>\n\n证据ID: <code>{}</code>\n重复状态: {}\n处理状态: {}\n生效交易日: {}\n来源可读: {}\n需要人工复核: {}",
            response["evidenceId"].as_str().unwrap_or_default(),
            response["duplicateStatus"].as_str().unwrap_or_default(),
            response["processingStatus"].as_str().unwrap_or_default(),
            response["effectiveTradeDate"].as_str().unwrap_or_default(),
            bool_label(response.get("sourceReadable").and_then(Value::as_bool)),
            bool_label(response.get("manualReviewNeeded").and_then(Value::as_bool)),
        ),
    )
    .await
}

pub(crate) async fn handle_telegram_list_events(
    state: Arc<AppState>,
    chat_id: i64,
) -> crate::error::Result<()> {
    let events = EventIntelligence::new(state.db.clone())
        .list_events(Some(10))
        .await?;
    if events.is_empty() {
        return telegram_send(&state, chat_id, "📭 暂无已提交事件").await;
    }

    let mut text = String::from("🗂️ <b>最新事件</b>\n");
    for event in events {
        text.push_str(&format!(
            "\n<code>{}</code>\n{} | {} | {}\n",
            event.evidence_id,
            escape_html(&event.title),
            event.processing_status,
            event.effective_trade_date
        ));
    }

    telegram_send(&state, chat_id, &text).await
}

pub(crate) async fn handle_telegram_event_detail(
    state: Arc<AppState>,
    chat_id: i64,
    args: &str,
) -> crate::error::Result<()> {
    let event_id = match parse_event_id(args, "event_detail") {
        Ok(id) => id,
        Err(message) => return telegram_send(&state, chat_id, message).await,
    };

    match EventIntelligence::new(state.db.clone())
        .get_event_detail(event_id)
        .await
    {
        Ok(detail) => {
            let mut text = format!(
                "🧾 <b>事件详情</b>\n\n证据ID: <code>{}</code>\n标题: {}\n处理状态: {}\n版本: {}\n生效交易日: {}\n首次看到: {}\n可用时间: {}",
                detail.evidence_id,
                escape_html(&detail.title),
                detail.processing_status,
                detail.version,
                detail.effective_trade_date,
                detail.first_seen_at,
                detail.available_at
            );
            if let Some(content) = detail.content.as_deref() {
                text.push_str(&format!("\n内容: {}", escape_html(content)));
            }
            if let Some(source_url) = detail.source_url.as_deref() {
                text.push_str(&format!("\n来源: {}", escape_html(source_url)));
            }
            telegram_send(&state, chat_id, &text).await
        }
        Err(AppError::NotFound(_)) => telegram_send(&state, chat_id, "❌ 未找到该事件").await,
        Err(error) => Err(error),
    }
}

pub(crate) async fn handle_telegram_review_event(
    state: Arc<AppState>,
    chat_id: i64,
    user_id: i64,
    args: &str,
) -> crate::error::Result<()> {
    let event_id = match parse_event_id(args, "event_review") {
        Ok(id) => id,
        Err(message) => return telegram_send(&state, chat_id, message).await,
    };

    match EventIntelligence::new(state.db.clone())
        .review_event(
            event_id,
            format!("telegram:{user_id}"),
            EventReviewAction::Publish,
        )
        .await
    {
        Ok(reviewed) => {
            telegram_send(
                &state,
                chat_id,
                &format!(
                    "✅ <b>事件已复核发布</b>\n\n新证据ID: <code>{}</code>\n上一版本: <code>{}</code>\n处理状态: {}\n版本: {}",
                    reviewed.evidence_id,
                    reviewed.supersedes_evidence_id,
                    reviewed.processing_status,
                    reviewed.version
                ),
            )
            .await
        }
        Err(AppError::NotFound(_)) => telegram_send(&state, chat_id, "❌ 未找到该事件").await,
        Err(AppError::BadRequest(message)) => telegram_send(
            &state,
            chat_id,
            &format!("❌ {}", escape_html(&message)),
        )
        .await,
        Err(error) => Err(error),
    }
}

pub(crate) async fn handle_telegram_market_facts(
    state: Arc<AppState>,
    chat_id: i64,
) -> crate::error::Result<()> {
    match EventIntelligence::new(state.db.clone())
        .get_daily_brief(None)
        .await
    {
        Ok(brief) => {
            let text = format!(
                "📰 <b>市场事实简报</b>\n\n交易日: {}\n版本: {}\n生成时间: {}\n\n{}",
                brief.trade_date,
                brief.brief_version,
                brief.generated_at,
                escape_html(&brief.content)
            );
            telegram_send(&state, chat_id, &text).await
        }
        Err(AppError::NotFound(_)) => {
            telegram_send(&state, chat_id, "📭 暂无已持久化的市场事实简报").await
        }
        Err(error) => Err(error),
    }
}

async fn submit_manual_event(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<ManualEventRequest>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let title = request.title.unwrap_or_default();
    let content = request.content.and_then(non_empty_trimmed);
    if title.trim().is_empty() && content.is_none() {
        return Err(bad_request("title or content is required"));
    }
    let response_facts = manual_submission_response_facts(content.as_deref());

    let outcome = EventIntelligence::new(state.db.clone())
        .submit_manual_event(ManualEventInput {
            title,
            content,
            source_url: request.source_url,
            submitted_by: request
                .submitted_by
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "rest".to_string()),
            published_at: request.published_at,
        })
        .await
        .map_err(map_event_error)?;

    Ok(Json(manual_submission_response(&outcome, &response_facts)))
}

async fn list_events(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    require_auth(&headers, &state)?;
    let events = EventIntelligence::new(state.db.clone())
        .list_events(None)
        .await
        .map_err(map_event_error)?;

    Ok(Json(json!({
        "count": events.len(),
        "events": events,
    })))
}

async fn get_event_detail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let event_id = parse_event_uuid(&id)?;
    let detail = EventIntelligence::new(state.db.clone())
        .get_event_detail(event_id)
        .await
        .map_err(map_event_error)?;
    Ok(Json(json!(detail)))
}

async fn get_event_evolution(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let event_id = parse_event_uuid(&id)?;
    let detail = EventIntelligence::new(state.db.clone())
        .get_event_evolution(event_id)
        .await
        .map_err(map_event_error)?;
    Ok(Json(json!(detail)))
}

async fn get_event_hypothesis(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let event_id = parse_event_uuid(&id)?;
    let detail = EventIntelligence::new(state.db.clone())
        .get_event_hypothesis(event_id)
        .await
        .map_err(map_event_error)?;
    Ok(Json(json!(detail)))
}

async fn get_event_market_observations(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let event_id = parse_event_uuid(&id)?;
    let detail = EventIntelligence::new(state.db.clone())
        .get_event_market_observations(event_id)
        .await
        .map_err(map_event_error)?;
    Ok(Json(json!(detail)))
}

async fn review_event(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<EventReviewRequest>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let action = parse_review_action(request.action.as_deref())?;

    let event_id = parse_event_uuid(&id)?;
    let reviewed_by = request
        .reviewed_by
        .and_then(non_empty_trimmed)
        .unwrap_or_else(|| "api-reviewer".to_string());
    let reviewed = EventIntelligence::new(state.db.clone())
        .review_event(event_id, reviewed_by, action)
        .await
        .map_err(map_event_error)?;

    Ok(Json(json!(reviewed)))
}

async fn get_daily_brief(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<DailyBriefQuery>,
) -> ApiResult {
    require_auth(&headers, &state)?;
    let trade_date = match query.date {
        Some(date) => Some(
            NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                .map_err(|_| bad_request("date must use YYYY-MM-DD"))?,
        ),
        None => None,
    };
    let brief = EventIntelligence::new(state.db.clone())
        .get_daily_brief(trade_date)
        .await
        .map_err(map_event_error)?;
    Ok(Json(json!(brief)))
}

async fn get_market_logic_brief(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    require_auth(&headers, &state)?;
    Ok(Json(json!(
        EventIntelligence::new(state.db.clone()).market_logic_brief()
    )))
}

fn manual_submission_response(
    outcome: &crate::analysis::events::ManualEventSubmissionOutcome,
    facts: &ManualSubmissionResponseFacts,
) -> Value {
    match outcome {
        crate::analysis::events::ManualEventSubmissionOutcome::Inserted(evidence) => json!({
            "evidenceId": evidence.evidence_id,
            "duplicateStatus": "independent",
            "processingStatus": external_manual_processing_status(),
            "effectiveTradeDate": evidence.effective_trade_date,
            "sourceReadable": facts.source_readable,
            "manualReviewNeeded": facts.manual_review_needed,
        }),
        crate::analysis::events::ManualEventSubmissionOutcome::Existing(existing) => json!({
            "evidenceId": existing.submitted.evidence_id,
            "duplicateStatus": "duplicate",
            "processingStatus": external_manual_processing_status(),
            "effectiveTradeDate": existing.submitted.effective_trade_date,
            "sourceReadable": facts.source_readable,
            "manualReviewNeeded": facts.manual_review_needed,
        }),
    }
}

fn parse_event_uuid(raw: &str) -> std::result::Result<Uuid, (StatusCode, Json<Value>)> {
    Uuid::parse_str(raw).map_err(|_| bad_request("invalid evidence ID"))
}

fn parse_event_id(args: &str, command: &'static str) -> std::result::Result<Uuid, &'static str> {
    let raw = args.trim();
    if raw.is_empty() {
        return Err(command_usage(command));
    }
    Uuid::parse_str(raw).map_err(|_| "❌ 无效的事件 ID")
}

fn command_usage(command: &'static str) -> &'static str {
    match command {
        "event_detail" => "用法: <code>/event_detail &lt;事件ID&gt;</code>",
        "event_review" => "用法: <code>/event_review &lt;事件ID&gt;</code>",
        _ => "❌ 无效命令",
    }
}

fn parse_review_action(
    action: Option<&str>,
) -> std::result::Result<EventReviewAction, (StatusCode, Json<Value>)> {
    match action.map(str::trim) {
        Some(action) if action.eq_ignore_ascii_case("publish") => Ok(EventReviewAction::Publish),
        Some(action) if action.eq_ignore_ascii_case("reject") => Ok(EventReviewAction::Reject),
        _ => Err(bad_request("unauthorized review action")),
    }
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

fn map_event_error(error: AppError) -> (StatusCode, Json<Value>) {
    match error {
        AppError::BadRequest(message) => bad_request(&message),
        AppError::NotFound(message) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Not found: {message}")})),
        ),
        other => crate::api::routes::api_error(&other.to_string()),
    }
}

fn bad_request(message: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({"error": message})))
}

fn non_empty_trimmed(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn looks_like_absolute_url(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok()
}

fn manual_submission_response_facts(content: Option<&str>) -> ManualSubmissionResponseFacts {
    ManualSubmissionResponseFacts {
        source_readable: content
            .map(str::trim)
            .and_then(|value| (!value.is_empty()).then_some(true)),
        manual_review_needed: None,
    }
}

fn external_manual_processing_status() -> &'static str {
    "collected"
}

fn bool_label(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "是",
        Some(false) => "否",
        None => "未知",
    }
}

fn escape_html(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn telegram_send(
    state: &Arc<AppState>,
    chat_id: i64,
    text: &str,
) -> crate::error::Result<()> {
    state.pusher.push(&chat_id.to_string(), text).await
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
        AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, SectorMembership,
        SecurityDailyStatus, SecurityMasterVersion,
    };
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::provider::DataProvider;
    use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
    use crate::error::Result;
    use crate::state::AppState;
    use crate::storage::event_repository::{
        DailyEventBriefRow, EventClusterRow, EventDeltaRow, EventEvidenceRow, EventHypothesisRow,
        EventRepository, MarketObservationRow,
    };
    use crate::telegram::TelegramPusher;

    #[sqlx::test(migrations = "./migrations")]
    async fn event_router_protects_all_event_routes(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool).await;

        for (method, path, body) in [
            (
                Method::POST,
                "/api/analysis/events/manual",
                Some(json!({"title": "Auth check", "submittedBy": "tester"}).to_string()),
            ),
            (Method::GET, "/api/analysis/events", None),
            (
                Method::GET,
                "/api/analysis/events/11111111-1111-1111-1111-111111111111",
                None,
            ),
            (
                Method::POST,
                "/api/analysis/events/11111111-1111-1111-1111-111111111111/review",
                Some(json!({"reviewedBy": "tester"}).to_string()),
            ),
            (Method::GET, "/api/analysis/events/daily-brief", None),
            (
                Method::GET,
                "/api/analysis/events/11111111-1111-1111-1111-111111111111/evolution",
                None,
            ),
            (
                Method::GET,
                "/api/analysis/events/11111111-1111-1111-1111-111111111111/hypothesis",
                None,
            ),
            (
                Method::GET,
                "/api/analysis/events/11111111-1111-1111-1111-111111111111/market-observations",
                None,
            ),
            (Method::GET, "/api/analysis/events/market-logic-brief", None),
        ] {
            let mut router = event_router(state.clone());
            let mut request = Request::builder().method(method).uri(path);
            let body = if let Some(body) = body {
                request = request.header(header::CONTENT_TYPE, "application/json");
                Body::from(body)
            } else {
                Body::empty()
            };
            let response = router.call(request.body(body).unwrap()).await.unwrap();

            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        }

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_event_submission_rejects_empty_title_and_content(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool).await;
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/analysis/events/manual")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "title": " \n\t ",
                            "content": "   ",
                            "submittedBy": "rest-user",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json(response).await;
        assert_eq!(payload["error"], "title or content is required");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_event_submission_rejects_malformed_url(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool).await;
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/analysis/events/manual")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "title": "Malformed URL example",
                            "content": "source should fail validation",
                            "sourceUrl": "not a valid url",
                            "submittedBy": "rest-user",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json(response).await;
        assert!(payload["error"]
            .as_str()
            .unwrap_or_default()
            .contains("source URL"));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_event_submission_returns_collected_status_for_content_submission(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool).await;
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/analysis/events/manual")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "title": "Manual event after close",
                            "content": "The issuer disclosed an after-close update.",
                            "submittedBy": "rest-user",
                            "publishedAt": "2026-07-10T07:30:00Z",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(Uuid::parse_str(payload["evidenceId"].as_str().unwrap()).is_ok());
        assert_eq!(payload["duplicateStatus"], "independent");
        assert_eq!(payload["processingStatus"], "collected");
        assert_eq!(payload["effectiveTradeDate"], "2026-07-13");
        assert_eq!(payload["sourceReadable"], true);
        assert_eq!(payload["manualReviewNeeded"], Value::Null);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_event_submission_accepts_content_only_and_persists_derived_title(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool).await;
        let mut router = event_router(state);
        let expected_title = "ACME signs definitive merger agreement after board approval";

        let create_response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/analysis/events/manual")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "title": " \n\t ",
                            "content": "  ACME   signs definitive merger agreement \n after board approval  ",
                            "submittedBy": "rest-user",
                            "publishedAt": "2026-07-10T07:30:00Z",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(create_response.status(), StatusCode::OK);
        let create_payload = response_json(create_response).await;
        assert_eq!(create_payload["duplicateStatus"], "independent");
        assert_eq!(create_payload["processingStatus"], "collected");
        assert_eq!(create_payload["sourceReadable"], true);
        let evidence_id = create_payload["evidenceId"].as_str().unwrap().to_string();

        let list_response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/events")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(list_response.status(), StatusCode::OK);
        let list_payload = response_json(list_response).await;
        let listed = list_payload["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["evidenceId"] == evidence_id)
            .cloned()
            .expect("submitted event should appear in list");
        assert_eq!(listed["title"], expected_title);
        assert_eq!(
            listed["content"],
            "ACME signs definitive merger agreement after board approval"
        );
        assert_eq!(listed["sourceReadable"], true);

        let detail_response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/api/analysis/events/{evidence_id}"))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(detail_response.status(), StatusCode::OK);
        let detail_payload = response_json(detail_response).await;
        assert_eq!(detail_payload["title"], expected_title);
        assert_eq!(
            detail_payload["content"],
            "ACME signs definitive merger agreement after board approval"
        );
        assert_eq!(detail_payload["sourceReadable"], true);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn manual_event_submission_with_url_only_source_leaves_readability_unknown(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool).await;
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/analysis/events/manual")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "title": "Issuer disclosure without pasted body",
                            "sourceUrl": "https://example.com/disclosure",
                            "submittedBy": "rest-user",
                            "publishedAt": "2026-07-10T07:30:00Z",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["processingStatus"], "collected");
        assert_eq!(payload["sourceReadable"], Value::Null);
        assert_eq!(payload["manualReviewNeeded"], Value::Null);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn list_events_returns_latest_versions_only(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let source_item_id = Uuid::new_v4().to_string();

        repo.insert_evidence(&evidence_row(
            &source_item_id,
            1,
            "pending",
            "Seeded event v1",
            dt(2026, 7, 10, 8, 0, 0),
        ))
        .await
        .unwrap();
        repo.insert_evidence(&evidence_row(
            &source_item_id,
            2,
            "publishable",
            "Seeded event v2",
            dt(2026, 7, 10, 9, 0, 0),
        ))
        .await
        .unwrap();
        repo.insert_evidence(&evidence_row(
            &Uuid::new_v4().to_string(),
            1,
            "pending",
            "Another event",
            dt(2026, 7, 10, 10, 0, 0),
        ))
        .await
        .unwrap();

        let mut router = event_router(state);
        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/events")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["events"].as_array().unwrap().len(), 2);
        assert_eq!(payload["events"][0]["title"], "Another event");
        assert_eq!(payload["events"][1]["title"], "Seeded event v2");
        assert_eq!(payload["events"][1]["processingStatus"], "publishable");
        assert_eq!(payload["events"][1]["version"], 2);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_detail_rejects_invalid_evidence_id(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool).await;
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/events/not-a-uuid")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json(response).await;
        assert_eq!(payload["error"], "invalid evidence ID");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn review_event_rejects_unauthorized_review_action(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let row = evidence_row(
            &Uuid::new_v4().to_string(),
            1,
            "pending",
            "Pending review",
            dt(2026, 7, 10, 8, 0, 0),
        );
        repo.insert_evidence(&row).await.unwrap();
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/analysis/events/{}/review", row.evidence_id))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "action": "archive",
                            "reviewedBy": "reviewer",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json(response).await;
        assert_eq!(payload["error"], "unauthorized review action");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn review_event_requires_explicit_publish_or_reject_action(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let row = evidence_row(
            &Uuid::new_v4().to_string(),
            1,
            "pending",
            "Pending explicit action",
            dt(2026, 7, 10, 8, 0, 0),
        );
        repo.insert_evidence(&row).await.unwrap();

        for request_body in [
            json!({
                "reviewedBy": "reviewer",
            }),
            json!({
                "action": "   ",
                "reviewedBy": "reviewer",
            }),
        ] {
            let mut router = event_router(state.clone());
            let response = router
                .call(
                    Request::builder()
                        .method(Method::POST)
                        .uri(format!("/api/analysis/events/{}/review", row.evidence_id))
                        .header(header::AUTHORIZATION, "Bearer test-key")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(request_body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let payload = response_json(response).await;
            assert_eq!(payload["error"], "unauthorized review action");
        }

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn review_event_rejects_invalid_evidence_id(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool).await;
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/analysis/events/not-a-uuid/review")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "action": "publish",
                            "reviewedBy": "reviewer",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json(response).await;
        assert_eq!(payload["error"], "invalid evidence ID");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn review_event_publishes_a_new_evidence_version(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let row = evidence_row(
            &Uuid::new_v4().to_string(),
            1,
            "pending",
            "Pending publish",
            dt(2026, 7, 10, 8, 0, 0),
        );
        repo.insert_evidence(&row).await.unwrap();
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/analysis/events/{}/review", row.evidence_id))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "action": "publish",
                            "reviewedBy": "reviewer",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["processingStatus"], "publishable");
        assert_eq!(payload["version"], 2);
        assert_eq!(payload["supersedesEvidenceId"], row.evidence_id.to_string());

        let latest = repo
            .latest_evidence_for_source_item(&row.source_id, &row.source_item_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.status, "publishable");
        assert_eq!(latest.version, 2);
        assert_eq!(latest.supersedes_evidence_id, Some(row.evidence_id));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn review_event_can_reject_a_pending_evidence_version(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let row = evidence_row(
            &Uuid::new_v4().to_string(),
            1,
            "pending",
            "Pending reject",
            dt(2026, 7, 10, 8, 0, 0),
        );
        repo.insert_evidence(&row).await.unwrap();
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/analysis/events/{}/review", row.evidence_id))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "action": "reject",
                            "reviewedBy": "reviewer",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["processingStatus"], "rejected");
        assert_eq!(payload["version"], 2);

        let latest = repo
            .latest_evidence_for_source_item(&row.source_id, &row.source_item_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.status, "rejected");
        assert_eq!(latest.version, 2);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn daily_brief_endpoint_reads_persisted_brief(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool);
        repo.save_daily_brief(&DailyEventBriefRow {
            trade_date: date(2026, 7, 10),
            brief_version: "brief-v1".to_string(),
            content: "Facts for the day".to_string(),
            structured_payload: json!({"facts": ["A"]}),
            input_fingerprint: "brief-fp".to_string(),
            generated_at: dt(2026, 7, 10, 18, 0, 0),
        })
        .await
        .unwrap();
        let mut router = event_router(state);

        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/events/daily-brief")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["tradeDate"], "2026-07-10");
        assert_eq!(payload["briefVersion"], "brief-v1");
        assert_eq!(payload["content"], "Facts for the day");
        assert_eq!(payload["structuredPayload"], json!({"facts": ["A"]}));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_logic_endpoints_return_explicit_non_causal_absence_contracts(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let row = evidence_row(
            &Uuid::new_v4().to_string(),
            1,
            "publishable",
            "Seeded event",
            dt(2026, 7, 10, 8, 0, 0),
        );
        repo.insert_evidence(&row).await.unwrap();
        let mut router = event_router(state);

        for (path, availability_field) in [
            (
                format!("/api/analysis/events/{}/evolution", row.evidence_id),
                "hasPersistedEvolution",
            ),
            (
                format!("/api/analysis/events/{}/hypothesis", row.evidence_id),
                "hasFrozenHypothesis",
            ),
            (
                format!(
                    "/api/analysis/events/{}/market-observations",
                    row.evidence_id
                ),
                "hasMarketObservations",
            ),
        ] {
            let response = router
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri(&path)
                        .header(header::AUTHORIZATION, "Bearer test-key")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let payload = response_json(response).await;
            assert_eq!(payload["eventId"], row.evidence_id.to_string(), "{path}");
            assert_eq!(payload["eventScore"], 0.0, "{path}");
            assert_eq!(payload[availability_field], false, "{path}");
        }

        let response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/analysis/events/market-logic-brief")
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["eventScore"], 0.0);
        assert_eq!(payload["hypothesisPolicy"], "inference_only");
        assert_eq!(payload["marketCausality"], "not_claimed");
        assert_eq!(payload["indirectStockCodes"], json!([]));

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_logic_endpoints_surface_persisted_gate3_rows(pool: PgPool) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let repo = EventRepository::new(pool.clone());
        let previous = evidence_row(
            &Uuid::new_v4().to_string(),
            1,
            "publishable",
            "Seeded previous event",
            dt(2026, 7, 10, 7, 0, 0),
        );
        let current = evidence_row(
            &Uuid::new_v4().to_string(),
            1,
            "publishable",
            "Seeded current event",
            dt(2026, 7, 10, 8, 0, 0),
        );
        repo.insert_evidence(&previous).await.unwrap();
        repo.insert_evidence(&current).await.unwrap();

        let cluster_id = Uuid::new_v4();
        repo.save_event_cluster_version(&event_cluster_row(cluster_id, 1, previous.evidence_id))
            .await
            .unwrap();
        repo.save_event_cluster_version(&event_cluster_row(cluster_id, 2, current.evidence_id))
            .await
            .unwrap();
        repo.save_event_delta(&event_delta_row(cluster_id, 1, 2))
            .await
            .unwrap();

        let hypothesis = frozen_hypothesis_row(cluster_id, 2, None);
        repo.save_frozen_hypothesis(&hypothesis).await.unwrap();
        repo.save_market_observation(&market_observation_row(hypothesis.hypothesis_id))
            .await
            .unwrap();

        let mut router = event_router(state);

        let evolution = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "/api/analysis/events/{}/evolution",
                        current.evidence_id
                    ))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evolution.status(), StatusCode::OK);
        let evolution_payload = response_json(evolution).await;
        assert_eq!(evolution_payload["eventScore"], 0.0);
        assert_eq!(evolution_payload["hasPersistedEvolution"], true);
        assert_eq!(
            evolution_payload["evolution"]["new_claim_ids"],
            json!([Uuid::from_u128(202)])
        );

        let hypothesis_response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "/api/analysis/events/{}/hypothesis",
                        current.evidence_id
                    ))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hypothesis_response.status(), StatusCode::OK);
        let hypothesis_payload = response_json(hypothesis_response).await;
        assert_eq!(hypothesis_payload["eventScore"], 0.0);
        assert_eq!(hypothesis_payload["hasFrozenHypothesis"], true);
        assert_eq!(hypothesis_payload["hypothesisPolicy"], "inference_only");
        assert_eq!(
            hypothesis_payload["hypothesis"]["graph"]["nodes"][0]["label"],
            "600519.SH wins major automation order"
        );

        let observations_response = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "/api/analysis/events/{}/market-observations",
                        current.evidence_id
                    ))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(observations_response.status(), StatusCode::OK);
        let observations_payload = response_json(observations_response).await;
        assert_eq!(observations_payload["eventScore"], 0.0);
        assert_eq!(observations_payload["hasMarketObservations"], true);
        assert_eq!(observations_payload["marketCausality"], "not_claimed");
        assert_eq!(
            observations_payload["observations"][0]["observation_status"],
            "market_aligned"
        );
        assert_eq!(
            observations_payload["observations"][0]["entity_id"],
            "600519.SH"
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_logic_endpoints_surface_persisted_rows_for_mention_linked_evidence(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let state = test_state(pool.clone()).await;
        let mut router = event_router(state);
        let repo = EventRepository::new(pool.clone());

        let linked = evidence_row(
            "linked-mention-only",
            1,
            "publishable",
            "Linked mention only",
            dt(2026, 7, 10, 8, 0, 0),
        );
        let primary_v1 = evidence_row(
            "primary-v1",
            1,
            "publishable",
            "Primary v1",
            dt(2026, 7, 10, 8, 15, 0),
        );
        let primary_v2 = evidence_row(
            "primary-v2",
            1,
            "publishable",
            "Primary v2",
            dt(2026, 7, 10, 8, 30, 0),
        );
        repo.insert_evidence(&linked).await.unwrap();
        repo.insert_evidence(&primary_v1).await.unwrap();
        repo.insert_evidence(&primary_v2).await.unwrap();

        let cluster_id = Uuid::new_v4();
        repo.save_event_cluster_version(&event_cluster_row(cluster_id, 1, primary_v1.evidence_id))
            .await
            .unwrap();
        repo.save_event_cluster_version(&EventClusterRow {
            representative_ids: vec![primary_v2.evidence_id],
            ..event_cluster_row(cluster_id, 2, primary_v2.evidence_id)
        })
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO market_event_mentions
               (mention_id, evidence_id, event_cluster_id, cluster_version, mention_time,
                adds_new_fact, source_independence, mention_payload)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
        )
        .bind(Uuid::new_v4())
        .bind(linked.evidence_id)
        .bind(cluster_id)
        .bind(1_i32)
        .bind(dt(2026, 7, 10, 8, 45, 0))
        .bind(true)
        .bind(0.93_f64)
        .bind(json!({"path": "mention-only"}))
        .execute(&pool)
        .await?;

        repo.save_event_delta(&event_delta_row(cluster_id, 1, 2))
            .await
            .unwrap();
        let previous_hypothesis = frozen_hypothesis_row(cluster_id, 1, None);
        repo.save_frozen_hypothesis(&previous_hypothesis)
            .await
            .unwrap();
        let latest_hypothesis =
            frozen_hypothesis_row(cluster_id, 2, Some(previous_hypothesis.hypothesis_id));
        repo.save_frozen_hypothesis(&latest_hypothesis)
            .await
            .unwrap();
        repo.save_market_observation(&market_observation_row(latest_hypothesis.hypothesis_id))
            .await
            .unwrap();

        let evolution = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "/api/analysis/events/{}/evolution",
                        linked.evidence_id
                    ))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evolution.status(), StatusCode::OK);
        let evolution_payload = response_json(evolution).await;
        assert_eq!(evolution_payload["hasPersistedEvolution"], true);
        assert_eq!(
            evolution_payload["evolution"]["new_claim_ids"],
            json!([Uuid::from_u128(202)])
        );

        let hypothesis = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "/api/analysis/events/{}/hypothesis",
                        linked.evidence_id
                    ))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hypothesis.status(), StatusCode::OK);
        let hypothesis_payload = response_json(hypothesis).await;
        assert_eq!(hypothesis_payload["hasFrozenHypothesis"], true);
        assert_eq!(
            hypothesis_payload["hypothesis"]["graph"]["nodes"][0]["label"],
            "600519.SH wins major automation order"
        );

        let observations = router
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "/api/analysis/events/{}/market-observations",
                        linked.evidence_id
                    ))
                    .header(header::AUTHORIZATION, "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(observations.status(), StatusCode::OK);
        let observations_payload = response_json(observations).await;
        assert_eq!(observations_payload["hasMarketObservations"], true);
        assert_eq!(
            observations_payload["observations"][0]["observation_status"],
            "market_aligned"
        );

        Ok(())
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[test]
    fn parse_event_id_reports_command_specific_usage_for_missing_arguments() {
        assert_eq!(
            parse_event_id("", "event_detail").unwrap_err(),
            "用法: <code>/event_detail &lt;事件ID&gt;</code>"
        );
        assert_eq!(
            parse_event_id("", "event_review").unwrap_err(),
            "用法: <code>/event_review &lt;事件ID&gt;</code>"
        );
    }

    fn evidence_row(
        source_item_id: &str,
        version: i32,
        status: &str,
        title: &str,
        available_at: DateTime<Utc>,
    ) -> EventEvidenceRow {
        EventEvidenceRow {
            evidence_id: Uuid::new_v4(),
            source_id: "manual:rest".to_string(),
            source_item_id: source_item_id.to_string(),
            source_url: None,
            source_tier: "manual".to_string(),
            source_terms_version: "terms-v1".to_string(),
            occurred_at: Some(available_at),
            published_at: Some(available_at),
            first_seen_at: available_at,
            available_at,
            effective_trade_date: available_at.date_naive(),
            title: title.to_string(),
            content: Some(format!("{title} content")),
            language: "und".to_string(),
            content_hash: format!("{source_item_id}-{version}"),
            raw_payload: json!({"submitted_by": "seed"}),
            version,
            supersedes_evidence_id: None,
            status: status.to_string(),
            created_at: available_at,
        }
    }

    fn event_cluster_row(
        event_cluster_id: Uuid,
        cluster_version: i32,
        primary_evidence_id: Uuid,
    ) -> EventClusterRow {
        EventClusterRow {
            event_cluster_id,
            cluster_version,
            canonical_title: format!("Cluster {event_cluster_id} v{cluster_version}"),
            event_time: Some(dt(2026, 7, 10, 8, 0, 0)),
            first_seen_at: dt(2026, 7, 10, 8, 0, 0),
            last_seen_at: dt(2026, 7, 10, 8, 30, 0),
            lifecycle_status: "active".to_string(),
            primary_evidence_id,
            representative_ids: vec![primary_evidence_id],
            source_entropy: 0.42,
            independent_sources: cluster_version,
            mention_count: cluster_version,
            cluster_payload: json!({"clusterVersion": cluster_version}),
            supersedes_version: (cluster_version > 1).then_some(cluster_version - 1),
            created_at: dt(2026, 7, 10, 8, 30 + cluster_version as u32, 0),
        }
    }

    fn event_delta_row(
        event_cluster_id: Uuid,
        from_version: i32,
        to_version: i32,
    ) -> EventDeltaRow {
        EventDeltaRow {
            event_cluster_id,
            from_version,
            to_version,
            delta_payload: json!({
                "new_claim_ids": [Uuid::from_u128(202)],
                "repeated_claim_ids": [Uuid::from_u128(201)],
                "revised_values": [],
                "removed_claim_ids": [],
                "status_changes": [],
                "expectation_gap": null,
                "new_uncertainties": ["pending customer acceptance"],
                "resolved_uncertainties": []
            }),
            created_at: dt(2026, 7, 10, 9, 0, 0),
        }
    }

    fn frozen_hypothesis_row(
        event_cluster_id: Uuid,
        cluster_version: i32,
        supersedes_id: Option<Uuid>,
    ) -> EventHypothesisRow {
        let hypothesis_id = Uuid::new_v4();
        let based_on_claim_ids = vec![Uuid::from_u128(201), Uuid::from_u128(202)];
        EventHypothesisRow {
            hypothesis_id,
            event_cluster_id,
            cluster_version,
            hypothesis_version: 1,
            schema_version: "impact_hypothesis_graph_v1".to_string(),
            graph_payload: json!({
                "hypothesis_id": hypothesis_id,
                "hypothesis_version": 1,
                "supersedes_hypothesis_id": supersedes_id,
                "graph": {
                    "schema_version": "impact_hypothesis_graph_v1",
                    "nodes": [
                        {
                            "node_id": "company_order_v1:source:order-1",
                            "node_type": "CompanyFact",
                            "label": "600519.SH wins major automation order"
                        },
                        {
                            "node_id": "company_order_v1:impact:600519-sh:0",
                            "node_type": "RevenueImpact",
                            "label": "600519.SH"
                        }
                    ],
                    "edges": [
                        {
                            "from": "company_order_v1:source:order-1",
                            "to": "company_order_v1:impact:600519-sh:0",
                            "relation": "increases",
                            "generation_method": "domain_rule",
                            "logic_rule_id": "company_order_v1",
                            "confidence": 0.91,
                            "assumptions": ["Customer executes the awarded order."],
                            "expected_horizon": "t+1",
                            "observable_indicators": ["Order-related revenue expectations strengthen."],
                            "counter_scenario": ["Order is canceled or materially delayed."],
                            "invalidation_conditions": ["The order is publicly withdrawn."]
                        }
                    ],
                    "based_on_claim_ids": based_on_claim_ids,
                    "frozen_at": dt(2026, 7, 10, 9, 0, 0)
                }
            }),
            frozen_at: dt(2026, 7, 10, 9, 0, 0),
            based_on_claim_ids,
            review_status: "frozen".to_string(),
            supersedes_id,
            created_at: dt(2026, 7, 10, 9, 0, 0),
        }
    }

    fn market_observation_row(hypothesis_id: Uuid) -> MarketObservationRow {
        MarketObservationRow {
            hypothesis_id,
            entity_type: "company".to_string(),
            entity_id: "600519.SH".to_string(),
            trade_date: date(2026, 7, 11),
            observation_status: "market_aligned".to_string(),
            market_alignment_score: Some(0.015),
            causal_confidence: 0.72,
            abnormal_market_return: Some(0.02),
            abnormal_industry_return: Some(0.01),
            market_metrics: json!({
                "snapshot_version": "pit-market-snapshot-v1",
                "window_label": "t+1",
                "benchmark_id": "CSI300",
                "industry_benchmark_id": "SW-FoodBeverage",
                "stock_return": 0.05,
                "market_return": 0.03,
                "industry_return": 0.04,
                "expected_direction": "positive"
            }),
            confounding_events: json!([]),
            created_at: dt(2026, 7, 11, 9, 30, 0),
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .unwrap()
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
