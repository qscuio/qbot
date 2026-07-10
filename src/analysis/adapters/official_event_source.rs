use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::analysis::adapters::{ContentRetentionPolicy, EventSource, FetchBatch, FetchedEvent};
use crate::config::Config;
use crate::error::{AppError, Result};

const OFFICIAL_MARKET_EVENT_SOURCE_ID: &str = "official:market_event";

pub struct OfficialEventSource {
    source_id: &'static str,
    feed_url: Url,
    api_key: Option<String>,
    retention_policy: ContentRetentionPolicy,
    client: Client,
}

impl OfficialEventSource {
    pub fn from_config(config: &Config) -> Result<Option<Self>> {
        let Some(feed_url) = config.official_event_feed_url.clone() else {
            return Ok(None);
        };

        Self::new(
            config.official_event_source_id.clone(),
            feed_url,
            config.official_event_feed_api_key.clone(),
            if config.official_event_store_full_content {
                ContentRetentionPolicy::StoreFullContent
            } else {
                ContentRetentionPolicy::StoreSummaryOnly
            },
        )
        .map(Some)
    }

    pub fn new(
        source_id: impl AsRef<str>,
        feed_url: String,
        api_key: Option<String>,
        retention_policy: ContentRetentionPolicy,
    ) -> Result<Self> {
        let source_id = supported_source_id(source_id.as_ref())?;
        let feed_url = Url::parse(&feed_url).map_err(|error| {
            AppError::Config(format!(
                "OFFICIAL_EVENT_FEED_URL must be a valid URL: {error}"
            ))
        })?;

        Ok(Self {
            source_id,
            feed_url,
            api_key,
            retention_policy,
            client: Client::new(),
        })
    }

    fn parse_response_body(&self, body: &str, until: DateTime<Utc>) -> Result<FetchBatch> {
        let feed: OfficialFeedResponse = serde_json::from_str(body)?;
        let mut items = Vec::with_capacity(feed.items.len());

        for raw_item in feed.items {
            if let Some(item) = self.parse_item(raw_item, until)? {
                items.push(item);
            }
        }

        Ok(FetchBatch {
            items,
            next_cursor: feed.next_cursor,
        })
    }

    fn parse_item(&self, raw_item: Value, until: DateTime<Utc>) -> Result<Option<FetchedEvent>> {
        let mut raw_payload = raw_item.as_object().cloned().ok_or_else(|| {
            AppError::DataProvider("official event feed item must be a JSON object".to_string())
        })?;
        let source_item_id = required_string(&raw_payload, "source_item_id")?;
        let published_at = parse_published_at(required_string(&raw_payload, "published_at")?)?;
        if published_at > until {
            return Ok(None);
        }

        let title = required_string(&raw_payload, "title")?;
        let source_url = required_string(&raw_payload, "source_url")?;
        let summary = optional_string(&raw_payload, "summary");
        let full_content = optional_string(&raw_payload, "content");
        let content = match self.retention_policy {
            ContentRetentionPolicy::StoreFullContent => full_content,
            ContentRetentionPolicy::StoreSummaryOnly => {
                raw_payload.remove("content");
                summary
            }
        };

        Ok(Some(FetchedEvent {
            source_item_id,
            published_at,
            title,
            content,
            source_url,
            raw_payload: Value::Object(raw_payload),
        }))
    }
}

#[async_trait]
impl EventSource for OfficialEventSource {
    fn source_id(&self) -> &'static str {
        self.source_id
    }

    fn retention_policy(&self) -> ContentRetentionPolicy {
        self.retention_policy
    }

    async fn fetch(&self, cursor: Option<String>, until: DateTime<Utc>) -> Result<FetchBatch> {
        let mut request = self
            .client
            .get(self.feed_url.clone())
            .query(&[("until", until.to_rfc3339())]);

        if let Some(cursor) = cursor.as_deref() {
            request = request.query(&[("cursor", cursor)]);
        }

        if let Some(api_key) = self.api_key.as_deref() {
            request = request.header("x-api-key", api_key);
        }

        let response_body = request.send().await?.error_for_status()?.text().await?;
        self.parse_response_body(&response_body, until)
    }
}

#[derive(Debug, Deserialize)]
struct OfficialFeedResponse {
    next_cursor: Option<String>,
    items: Vec<Value>,
}

fn supported_source_id(source_id: &str) -> Result<&'static str> {
    match source_id {
        OFFICIAL_MARKET_EVENT_SOURCE_ID => Ok(OFFICIAL_MARKET_EVENT_SOURCE_ID),
        unsupported => Err(AppError::Config(format!(
            "OFFICIAL_EVENT_SOURCE_ID `{unsupported}` is not supported; expected one of: {OFFICIAL_MARKET_EVENT_SOURCE_ID}"
        ))),
    }
}

fn required_string(payload: &Map<String, Value>, field: &str) -> Result<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            AppError::DataProvider(format!(
                "official event feed item missing string field `{field}`"
            ))
        })
}

fn optional_string(payload: &Map<String, Value>, field: &str) -> Option<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn parse_published_at(value: String) -> Result<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|error| {
            AppError::DataProvider(format!(
                "official event feed published_at must be RFC3339: {error}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use axum::{http::header, routing::get, Router};
    use chrono::{TimeZone, Utc};
    use serde_json::Value;
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;
    use crate::analysis::adapters::{EventSource, FetchedEvent};

    const OFFICIAL_FIXTURE: &str = r#"{
      "next_cursor": "cursor-2",
      "items": [
        {
          "source_item_id": "notice-001",
          "published_at": "2026-07-10T08:15:00Z",
          "title": "Exchange trading status update",
          "content": "Full bulletin body that must only persist when policy allows it.",
          "summary": "Exchange confirms normal trading conditions.",
          "source_url": "https://example.test/notices/notice-001",
          "category": "market-status"
        }
      ]
    }"#;

    #[test]
    fn parses_fixture_and_keeps_full_content_when_policy_allows_it() {
        let source = OfficialEventSource::new(
            "official:market_event",
            "https://example.test/feed".to_string(),
            Some("secret".to_string()),
            ContentRetentionPolicy::StoreFullContent,
        )
        .unwrap();

        let batch = source
            .parse_response_body(
                OFFICIAL_FIXTURE,
                Utc.with_ymd_and_hms(2026, 7, 10, 9, 0, 0).unwrap(),
            )
            .unwrap();

        assert_eq!(batch.next_cursor.as_deref(), Some("cursor-2"));
        assert_eq!(
            batch.items,
            vec![FetchedEvent {
                source_item_id: "notice-001".to_string(),
                published_at: Utc.with_ymd_and_hms(2026, 7, 10, 8, 15, 0).unwrap(),
                title: "Exchange trading status update".to_string(),
                content: Some(
                    "Full bulletin body that must only persist when policy allows it.".to_string()
                ),
                source_url: "https://example.test/notices/notice-001".to_string(),
                raw_payload: serde_json::from_str::<Value>(
                    r#"{
                      "source_item_id": "notice-001",
                      "published_at": "2026-07-10T08:15:00Z",
                      "title": "Exchange trading status update",
                      "content": "Full bulletin body that must only persist when policy allows it.",
                      "summary": "Exchange confirms normal trading conditions.",
                      "source_url": "https://example.test/notices/notice-001",
                      "category": "market-status"
                    }"#,
                )
                .unwrap(),
            }]
        );
    }

    #[test]
    fn parses_fixture_and_discards_full_content_when_policy_is_summary_only() {
        let source = OfficialEventSource::new(
            "official:market_event",
            "https://example.test/feed".to_string(),
            None,
            ContentRetentionPolicy::StoreSummaryOnly,
        )
        .unwrap();

        let batch = source
            .parse_response_body(
                OFFICIAL_FIXTURE,
                Utc.with_ymd_and_hms(2026, 7, 10, 9, 0, 0).unwrap(),
            )
            .unwrap();

        assert_eq!(batch.items.len(), 1);
        assert_eq!(
            batch.items[0].content.as_deref(),
            Some("Exchange confirms normal trading conditions.")
        );
        assert!(batch.items[0].raw_payload.get("content").is_none());
        assert_eq!(
            batch.items[0]
                .raw_payload
                .get("summary")
                .and_then(Value::as_str),
            Some("Exchange confirms normal trading conditions.")
        );
    }

    #[test]
    fn from_config_requires_a_feed_url_and_uses_env_selected_retention() {
        let mut config = base_config();
        config.official_event_feed_url = None;

        assert!(OfficialEventSource::from_config(&config).unwrap().is_none());

        config.official_event_feed_url = Some("https://example.test/feed".to_string());
        config.official_event_store_full_content = false;

        let source = OfficialEventSource::from_config(&config)
            .unwrap()
            .expect("configured official source");

        assert_eq!(source.source_id(), "official:market_event");
        assert_eq!(
            source.retention_policy(),
            ContentRetentionPolicy::StoreSummaryOnly
        );
    }

    #[test]
    fn from_config_rejects_unsupported_source_ids() {
        let mut config = base_config();
        config.official_event_source_id = "official:unsupported".to_string();

        match OfficialEventSource::from_config(&config) {
            Err(AppError::Config(message)) => {
                assert_eq!(
                    message,
                    "OFFICIAL_EVENT_SOURCE_ID `official:unsupported` is not supported; expected one of: official:market_event"
                );
            }
            Ok(_) => panic!("expected config error, got Ok result"),
            Err(other) => panic!("expected config error, got {other:?}"),
        }
    }

    #[test]
    fn parse_response_body_filters_items_after_until_cutoff() {
        let source = OfficialEventSource::new(
            "official:market_event",
            "https://example.test/feed".to_string(),
            None,
            ContentRetentionPolicy::StoreFullContent,
        )
        .unwrap();

        let batch = source
            .parse_response_body(
                OFFICIAL_FIXTURE,
                Utc.with_ymd_and_hms(2026, 7, 10, 8, 0, 0).unwrap(),
            )
            .unwrap();

        assert!(batch.items.is_empty());
        assert_eq!(batch.next_cursor.as_deref(), Some("cursor-2"));
    }

    #[tokio::test]
    async fn fetch_reads_fixture_from_loopback_http_without_live_network_calls() {
        let (feed_url, server) = spawn_fixture_server().await;
        let source = OfficialEventSource::new(
            "official:market_event",
            feed_url,
            Some("secret".to_string()),
            ContentRetentionPolicy::StoreSummaryOnly,
        )
        .unwrap();

        let source: &dyn EventSource = &source;
        let batch = source
            .fetch(None, Utc.with_ymd_and_hms(2026, 7, 10, 9, 0, 0).unwrap())
            .await
            .unwrap();

        assert_eq!(batch.items.len(), 1);
        assert_eq!(
            batch.items[0].content.as_deref(),
            Some("Exchange confirms normal trading conditions.")
        );

        server.abort();
        let _ = server.await;
    }

    fn base_config() -> Config {
        Config {
            tushare_token: "token".to_string(),
            database_url: "postgresql://qbot:qbot@127.0.0.1/qbot".to_string(),
            redis_url: "redis://127.0.0.1:6379".to_string(),
            telegram_bot_token: "123:abc".to_string(),
            telegram_webhook_secret: None,
            webhook_url: None,
            stock_alert_channel: None,
            report_channel: None,
            daban_channel: None,
            api_port: 8080,
            api_key: None,
            ai_api_key: None,
            ai_base_url: "https://api.openai.com/v1".to_string(),
            ai_model: "gpt-4o-mini".to_string(),
            data_proxy: None,
            enable_burst_monitor: true,
            enable_daban_live: false,
            enable_ai_analysis: false,
            enable_chip_dist: true,
            enable_signal_auto_trading: false,
            official_event_feed_url: Some("https://example.test/feed".to_string()),
            official_event_feed_api_key: Some("secret".to_string()),
            official_event_source_id: "official:market_event".to_string(),
            official_event_store_full_content: true,
        }
    }

    async fn spawn_fixture_server() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/feed",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    OFFICIAL_FIXTURE,
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{addr}/feed"), server)
    }
}
