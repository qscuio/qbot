use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::analysis::adapters::{ContentRetentionPolicy, EventSource, FetchBatch, FetchedEvent};
use crate::config::Config;
use crate::error::{AppError, Result};

const GDELT_MACRO_EVENT_SOURCE_ID: &str = "gdelt:macro_event";
const GDELT_DOC_API_URL: &str = "https://api.gdeltproject.org/api/v2/doc/doc";
const GDELT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

pub struct GdeltEventSource {
    query: String,
    max_records: usize,
    endpoint: Url,
    client: Client,
}

impl GdeltEventSource {
    pub fn from_config(config: &Config) -> Result<Option<Self>> {
        if !config.enable_gdelt_events {
            return Ok(None);
        }

        Self::with_endpoint(
            config.gdelt_event_query.clone(),
            config.gdelt_max_records,
            GDELT_DOC_API_URL.to_string(),
            config.data_proxy.as_deref(),
        )
        .map(Some)
    }

    pub fn new(query: impl AsRef<str>, max_records: usize) -> Result<Self> {
        Self::with_endpoint(
            query.as_ref().to_string(),
            max_records,
            GDELT_DOC_API_URL.to_string(),
            None,
        )
    }

    pub fn with_endpoint(
        query: String,
        max_records: usize,
        endpoint: String,
        data_proxy: Option<&str>,
    ) -> Result<Self> {
        let query = normalize_query(&query)?;
        if max_records == 0 {
            return Err(AppError::Config(
                "GDELT_MAX_RECORDS must be greater than zero".to_string(),
            ));
        }

        let endpoint = Url::parse(&endpoint).map_err(|error| {
            AppError::Config(format!("GDELT DOC endpoint must be a valid URL: {error}"))
        })?;

        Ok(Self {
            query,
            max_records,
            endpoint,
            client: build_client(data_proxy)?,
        })
    }

    fn parse_response_body(
        &self,
        body: &str,
        until: DateTime<Utc>,
        cursor: Option<String>,
    ) -> Result<FetchBatch> {
        let response: GdeltDocResponse = serde_json::from_str(body)?;
        let cursor = cursor.as_deref().map(parse_cursor).transpose()?;
        let mut items = response
            .articles
            .into_iter()
            .map(parse_article)
            .collect::<Result<Vec<_>>>()?;

        items.retain(|item| item.published_at <= until);
        if let Some(cursor) = cursor.as_ref() {
            items.retain(|item| item_is_after_cursor(item, cursor));
        }

        items.sort_by(|left, right| {
            left.published_at
                .cmp(&right.published_at)
                .then(left.source_item_id.cmp(&right.source_item_id))
        });
        if items.len() > self.max_records {
            items.truncate(self.max_records);
        }

        let next_cursor = items.last().map(cursor_for_item);
        Ok(FetchBatch { items, next_cursor })
    }
}

#[async_trait]
impl EventSource for GdeltEventSource {
    fn source_id(&self) -> &'static str {
        GDELT_MACRO_EVENT_SOURCE_ID
    }

    fn retention_policy(&self) -> ContentRetentionPolicy {
        ContentRetentionPolicy::StoreSummaryOnly
    }

    async fn fetch(&self, cursor: Option<String>, until: DateTime<Utc>) -> Result<FetchBatch> {
        let mut request = self.client.get(self.endpoint.clone()).query(&[
            ("query", self.query.as_str()),
            ("mode", "artlist"),
            ("format", "json"),
            ("sort", "DateAsc"),
        ]);
        request = request.query(&[("maxrecords", self.max_records.to_string())]);
        request = request.query(&[("ENDDATETIME", gdelt_datetime(until))]);

        if let Some(parsed_cursor) = cursor.as_deref().map(parse_cursor).transpose()? {
            request =
                request.query(&[("STARTDATETIME", gdelt_datetime(parsed_cursor.published_at))]);
        }

        let response_body = request.send().await?.error_for_status()?.text().await?;
        self.parse_response_body(&response_body, until, cursor)
    }
}

#[derive(Debug, Deserialize)]
struct GdeltDocResponse {
    articles: Vec<Value>,
}

#[derive(Debug, Clone)]
struct CursorBoundary {
    published_at: DateTime<Utc>,
    source_item_id: String,
}

fn parse_article(raw_article: Value) -> Result<FetchedEvent> {
    let mut raw_payload = raw_article
        .as_object()
        .cloned()
        .ok_or_else(|| AppError::DataProvider("GDELT article must be a JSON object".to_string()))?;
    let source_url = required_string_with_aliases(&raw_payload, &["source_url", "url"])?;
    let source_item_id = optional_string_with_aliases(&raw_payload, &["source_item_id"])
        .unwrap_or_else(|| source_item_id_from_url(&source_url));
    let published_at = parse_published_at(required_string_with_aliases(
        &raw_payload,
        &["published_at", "seendate", "date"],
    )?)?;
    let title = required_string_with_aliases(&raw_payload, &["title"])?;
    let language = optional_string_with_aliases(&raw_payload, &["language", "lang"]);
    let themes = optional_array_or_empty(&raw_payload, "themes")?;
    let locations = optional_array_or_empty(&raw_payload, "locations")?;
    let organizations = optional_array_or_empty(&raw_payload, "organizations")?;
    let description = optional_string_with_aliases(&raw_payload, &["description", "desc"]);

    raw_payload.insert(
        "source_item_id".to_string(),
        Value::String(source_item_id.clone()),
    );
    raw_payload.insert(
        "published_at".to_string(),
        Value::String(published_at.to_rfc3339()),
    );
    raw_payload.insert("title".to_string(), Value::String(title.clone()));
    raw_payload.insert("source_url".to_string(), Value::String(source_url.clone()));
    if let Some(language) = language {
        raw_payload.insert("language".to_string(), Value::String(language));
    }
    raw_payload.insert("themes".to_string(), themes);
    raw_payload.insert("locations".to_string(), locations);
    raw_payload.insert("organizations".to_string(), organizations);
    if let Some(description) = description.clone() {
        raw_payload.insert("description".to_string(), Value::String(description));
    }
    raw_payload.insert(
        "sourceRole".to_string(),
        Value::String("macro_supplement".to_string()),
    );
    raw_payload.insert("companyFactEligible".to_string(), Value::Bool(false));

    Ok(FetchedEvent {
        source_item_id,
        published_at,
        title,
        content: description,
        source_url,
        raw_payload: Value::Object(raw_payload),
    })
}

fn normalize_query(query: &str) -> Result<String> {
    let normalized = query.trim();
    if normalized.is_empty() {
        return Err(AppError::Config(
            "GDELT_EVENT_QUERY is required when ENABLE_GDELT_EVENTS=true".to_string(),
        ));
    }

    Ok(normalized.to_string())
}

fn build_client(data_proxy: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder().timeout(GDELT_TIMEOUT);

    if let Some(proxy_url) = data_proxy.filter(|proxy_url| !proxy_url.trim().is_empty()) {
        let proxy = reqwest::Proxy::all(proxy_url).map_err(|error| {
            AppError::Config(format!(
                "DATA_PROXY must be a valid proxy URL for GDELT: {error}"
            ))
        })?;
        builder = builder.proxy(proxy);
    }

    builder
        .build()
        .map_err(|error| AppError::Config(format!("failed to build GDELT HTTP client: {error}")))
}

fn required_string_with_aliases(payload: &Map<String, Value>, fields: &[&str]) -> Result<String> {
    fields
        .iter()
        .find_map(|field| {
            payload
                .get(*field)
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| {
            AppError::DataProvider(format!(
                "GDELT article missing string field `{}`",
                fields.join("` or `")
            ))
        })
}

fn optional_string_with_aliases(payload: &Map<String, Value>, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        payload
            .get(*field)
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn optional_array_or_empty(payload: &Map<String, Value>, field: &str) -> Result<Value> {
    match payload.get(field) {
        Some(value) if value.is_array() => Ok(value.clone()),
        Some(_) => Err(AppError::DataProvider(format!(
            "GDELT article field `{field}` must be an array when present"
        ))),
        None => Ok(Value::Array(Vec::new())),
    }
}

fn source_item_id_from_url(source_url: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source_url.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn parse_published_at(value: String) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(&value, "%Y%m%d%H%M%S")
                .map(|timestamp| DateTime::<Utc>::from_naive_utc_and_offset(timestamp, Utc))
        })
        .map_err(|error| {
            AppError::DataProvider(format!(
                "GDELT article published_at must be RFC3339 or YYYYMMDDHHMMSS: {error}"
            ))
        })
}

fn gdelt_datetime(timestamp: DateTime<Utc>) -> String {
    timestamp.format("%Y%m%d%H%M%S").to_string()
}

fn parse_cursor(cursor: &str) -> Result<CursorBoundary> {
    let (published_at, source_item_id) = cursor.split_once('|').ok_or_else(|| {
        AppError::DataProvider("GDELT cursor must be `<published_at>|<source_item_id>`".to_string())
    })?;

    Ok(CursorBoundary {
        published_at: parse_published_at(published_at.to_string())?,
        source_item_id: source_item_id.to_string(),
    })
}

fn cursor_for_item(item: &FetchedEvent) -> String {
    format!("{}|{}", item.published_at.to_rfc3339(), item.source_item_id)
}

fn item_is_after_cursor(item: &FetchedEvent, cursor: &CursorBoundary) -> bool {
    item.published_at > cursor.published_at
        || (item.published_at == cursor.published_at && item.source_item_id > cursor.source_item_id)
}

#[cfg(test)]
mod tests {
    use axum::{http::header, routing::get, Router};
    use chrono::{TimeZone, Utc};
    use serde_json::{json, Value};
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;
    use crate::analysis::adapters::{ContentRetentionPolicy, EventSource, FetchedEvent};

    #[test]
    fn parses_real_artlist_fixture_derives_ids_and_defaults_missing_enrichments() {
        let source = GdeltEventSource::new("red sea shipping", 250).unwrap();
        let fixture = include_str!("../../../tests/fixtures/gdelt_doc_artlist_real_shape.json");

        let batch = source
            .parse_response_body(
                fixture,
                Utc.with_ymd_and_hms(2026, 7, 10, 9, 0, 0).unwrap(),
                None,
            )
            .unwrap();

        assert_eq!(
            batch.next_cursor.as_deref(),
            Some(
                "2026-07-10T08:45:00+00:00|35d4ee9122a063c4dadc1d3470dae354be627436fae4c8b518d9a07dfd973aea"
            )
        );
        assert_eq!(batch.items.len(), 2);
        assert_eq!(
            batch.items[0],
            FetchedEvent {
                source_item_id: "6a007dabace71ba342877915f290b557f7822697e6926947dc721cff14b6d388"
                    .to_string(),
                published_at: Utc.with_ymd_and_hms(2026, 7, 10, 8, 15, 0).unwrap(),
                title: "Shipping reroutes after Red Sea attacks raise freight concerns".to_string(),
                content: None,
                source_url: "https://example.test/articles/red-sea-shipping".to_string(),
                raw_payload: json!({
                    "url": "https://example.test/articles/red-sea-shipping",
                    "title": "Shipping reroutes after Red Sea attacks raise freight concerns",
                    "seendate": "20260710081500",
                    "language": "English",
                    "domain": "example.test",
                    "sourcecountry": "US",
                    "source_item_id": "6a007dabace71ba342877915f290b557f7822697e6926947dc721cff14b6d388",
                    "published_at": "2026-07-10T08:15:00+00:00",
                    "source_url": "https://example.test/articles/red-sea-shipping",
                    "themes": [],
                    "locations": [],
                    "organizations": [],
                    "sourceRole": "macro_supplement",
                    "companyFactEligible": false
                }),
            }
        );
        assert_eq!(
            batch.items[1]
                .raw_payload
                .get("themes")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(
            batch.items[1]
                .raw_payload
                .get("locations")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            batch.items[1]
                .raw_payload
                .get("organizations")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn parses_fixture_and_maps_supplementary_metadata() {
        let source = GdeltEventSource::new("red sea shipping", 250).unwrap();
        let fixture = include_str!("../../../tests/fixtures/gdelt_doc_articles.json");

        let batch = source
            .parse_response_body(
                fixture,
                Utc.with_ymd_and_hms(2026, 7, 10, 9, 0, 0).unwrap(),
                None,
            )
            .unwrap();

        assert_eq!(
            batch.next_cursor.as_deref(),
            Some("2026-07-10T08:45:00+00:00|gdelt-article-002")
        );
        assert_eq!(batch.items.len(), 2);
        assert_eq!(
            batch.items[0],
            FetchedEvent {
                source_item_id: "gdelt-article-001".to_string(),
                published_at: Utc.with_ymd_and_hms(2026, 7, 10, 8, 15, 0).unwrap(),
                title: "Shipping reroutes after Red Sea attacks raise freight concerns".to_string(),
                content: Some(
                    "Major carriers diverted vessels after renewed attacks in the Red Sea."
                        .to_string()
                ),
                source_url: "https://example.test/articles/red-sea-shipping".to_string(),
                raw_payload: json!({
                    "source_item_id": "gdelt-article-001",
                    "published_at": "2026-07-10T08:15:00+00:00",
                    "title": "Shipping reroutes after Red Sea attacks raise freight concerns",
                    "source_url": "https://example.test/articles/red-sea-shipping",
                    "language": "en",
                    "themes": ["MARITIME_SECURITY", "SUPPLY_CHAIN"],
                    "locations": ["Red Sea", "Suez Canal"],
                    "organizations": ["Maersk", "Hapag-Lloyd"],
                    "description": "Major carriers diverted vessels after renewed attacks in the Red Sea.",
                    "sourceRole": "macro_supplement",
                    "companyFactEligible": false
                }),
            }
        );
        assert_eq!(
            batch.items[1]
                .raw_payload
                .get("sourceRole")
                .and_then(Value::as_str),
            Some("macro_supplement")
        );
        assert_eq!(
            batch.items[1]
                .raw_payload
                .get("companyFactEligible")
                .and_then(Value::as_bool),
            Some(false)
        );
    }

    #[tokio::test]
    async fn repeated_fetches_with_cursor_do_not_repeat_items() {
        let (feed_url, server) = spawn_fixture_server().await;
        let source =
            GdeltEventSource::with_endpoint("red sea shipping".to_string(), 250, feed_url, None)
                .unwrap();
        let until = Utc.with_ymd_and_hms(2026, 7, 10, 9, 0, 0).unwrap();

        let first = source.fetch(None, until).await.unwrap();
        let second = source
            .fetch(first.next_cursor.clone(), until)
            .await
            .unwrap();

        assert_eq!(first.items.len(), 2);
        assert!(second.items.is_empty());
        assert!(second.next_cursor.is_none());

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn from_config_uses_defaults_and_treats_disabled_source_as_none() {
        let mut config = base_config();
        config.enable_gdelt_events = false;
        assert!(GdeltEventSource::from_config(&config).unwrap().is_none());

        config.enable_gdelt_events = true;
        let source = GdeltEventSource::from_config(&config)
            .unwrap()
            .expect("configured gdelt source");

        assert_eq!(source.source_id(), "gdelt:macro_event");
        assert_eq!(
            source.retention_policy(),
            ContentRetentionPolicy::StoreSummaryOnly
        );
    }

    #[test]
    fn from_config_rejects_blank_query_when_enabled() {
        let mut config = base_config();
        config.gdelt_event_query = "   ".to_string();

        match GdeltEventSource::from_config(&config) {
            Err(AppError::Config(message)) => {
                assert_eq!(
                    message,
                    "GDELT_EVENT_QUERY is required when ENABLE_GDELT_EVENTS=true"
                );
            }
            Ok(_) => panic!("expected config error, got Ok result"),
            Err(other) => panic!("expected config error, got {other:?}"),
        }
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
            official_event_feed_url: Some("https://example.test/feed".to_string()),
            official_event_feed_api_key: Some("secret".to_string()),
            official_event_source_id: "official:market_event".to_string(),
            official_event_store_full_content: true,
            enable_gdelt_events: true,
            gdelt_event_query: "red sea shipping".to_string(),
            gdelt_max_records: 250,
            enable_burst_monitor: true,
            enable_daban_live: false,
            enable_ai_analysis: false,
            enable_chip_dist: true,
            enable_event_score_adjustment: false,
            max_event_score_adjustment: 0.0,
            enable_signal_auto_trading: false,
        }
    }

    async fn spawn_fixture_server() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/doc",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    include_str!("../../../tests/fixtures/gdelt_doc_articles.json"),
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{addr}/doc"), server)
    }
}
