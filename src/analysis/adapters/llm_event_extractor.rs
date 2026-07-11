use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::{Client, Url};
use serde_json::{json, Value};

use crate::analysis::events::extraction::{
    validation_error, EventExtractionInput, EventExtractionMetadata, EventExtractionOutput,
    EventExtractionV1, EventExtractor, StockCodeLookup, ValidationIssue,
    EVENT_EXTRACTION_SCHEMA_VERSION,
};
use crate::config::Config;
use crate::error::{AppError, Result};

const PROMPT_VERSION: &str = "market_event_extraction_prompt_v1";
const EXTRACTOR_TIMEOUT: Duration = Duration::from_secs(45);
const JSON_RESPONSE_FORMAT: &str = "json_object";
const SYSTEM_PROMPT: &str = concat!(
    "You extract candidate market-event claims from evidence. ",
    "Return JSON only with no markdown, explanation, or surrounding prose. ",
    "The output must match the requested schema exactly, include only candidate claims, ",
    "never publish or rank claims, and never invent evidence ids."
);

pub struct LlmEventExtractor {
    api_key: String,
    base_url: String,
    model: String,
    stock_code_lookup: Arc<dyn StockCodeLookup>,
    client: Client,
}

impl LlmEventExtractor {
    pub fn from_config(
        config: &Config,
        stock_code_lookup: Arc<dyn StockCodeLookup>,
    ) -> Result<Self> {
        let api_key = config.ai_api_key.clone().ok_or_else(|| {
            AppError::Config("AI_API_KEY is required for the llm event extractor".to_string())
        })?;

        Self::new(
            api_key,
            config.ai_base_url.clone(),
            config.ai_model.clone(),
            stock_code_lookup,
        )
    }

    pub fn new(
        api_key: String,
        base_url: String,
        model: String,
        stock_code_lookup: Arc<dyn StockCodeLookup>,
    ) -> Result<Self> {
        Url::parse(&base_url).map_err(|error| {
            AppError::Config(format!(
                "AI_BASE_URL must be a valid URL for the llm event extractor: {error}"
            ))
        })?;

        let client = Client::builder()
            .timeout(EXTRACTOR_TIMEOUT)
            .build()
            .map_err(|error| {
                AppError::Config(format!(
                    "failed to build llm event extractor HTTP client: {error}"
                ))
            })?;

        Ok(Self {
            api_key,
            base_url,
            model,
            stock_code_lookup,
            client,
        })
    }

    fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url.trim_end_matches('/'))
    }

    fn model_parameters(&self) -> Value {
        json!({
            "temperature": 0,
            "response_format": {
                "type": JSON_RESPONSE_FORMAT,
            },
        })
    }

    fn initial_messages(&self, input: &EventExtractionInput) -> Result<Vec<Value>> {
        Ok(vec![
            json!({
                "role": "system",
                "content": SYSTEM_PROMPT,
            }),
            json!({
                "role": "user",
                "content": self.build_user_prompt(input)?,
            }),
        ])
    }

    fn repair_messages(
        &self,
        input: &EventExtractionInput,
        original_response: &str,
        issues: &[ValidationIssue],
    ) -> Result<Vec<Value>> {
        let mut messages = self.initial_messages(input)?;
        messages.push(json!({
            "role": "assistant",
            "content": original_response,
        }));
        messages.push(json!({
            "role": "user",
            "content": self.build_repair_prompt(issues),
        }));
        Ok(messages)
    }

    fn build_user_prompt(&self, input: &EventExtractionInput) -> Result<String> {
        let evidence = serde_json::to_string_pretty(&input.evidence).map_err(AppError::Json)?;
        Ok(format!(
            concat!(
                "Extract market-event candidate claims into schema `{schema}`.\n",
                "Return exactly one JSON object with these top-level fields: ",
                "event_type, event_subtype, claims, entities, amounts, dates, uncertainties, missing_information.\n",
                "Rules:\n",
                "- claim_type must be one of: fact, direct_quote, third_party_claim, journalist_interpretation, rumor, unknown.\n",
                "- Candidate claims only. Nothing is published, ranked, or promoted.\n",
                "- Use only evidence ids present in the provided evidence set.\n",
                "- For amounts and dates, value must match text that appears verbatim in the evidence.\n",
                "- Provide stock_code only when the source text contains it directly.\n",
                "- Keep rumors and journalist interpretations distinct from facts.\n",
                "Evidence set:\n{evidence}\n"
            ),
            schema = EVENT_EXTRACTION_SCHEMA_VERSION,
            evidence = evidence,
        ))
    }

    fn build_repair_prompt(&self, issues: &[ValidationIssue]) -> String {
        let details = issues
            .iter()
            .map(|issue| format!("- {}: {}", issue.path, issue.message))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "Repair the JSON only. Return exactly one JSON object that satisfies the schema. Fix these issues:\n{details}"
        )
    }

    async fn send_chat_completion(&self, messages: Vec<Value>) -> Result<String> {
        let body = json!({
            "model": self.model,
            "messages": messages,
            "temperature": 0,
            "response_format": {
                "type": JSON_RESPONSE_FORMAT,
            },
        });

        let payload = self
            .client
            .post(self.endpoint())
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;

        payload
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|content| !content.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                AppError::DataProvider(
                    "llm event extractor response did not include a message content string"
                        .to_string(),
                )
            })
    }

    fn parse_and_validate(
        &self,
        input: &EventExtractionInput,
        content: &str,
    ) -> std::result::Result<EventExtractionV1, Vec<ValidationIssue>> {
        let extraction = serde_json::from_str::<EventExtractionV1>(content).map_err(|error| {
            vec![ValidationIssue {
                path: "response".to_string(),
                message: format!(
                    "response body is not valid {EVENT_EXTRACTION_SCHEMA_VERSION} JSON: {error}"
                ),
            }]
        })?;

        let issues =
            extraction.validate(&input.validation_context(self.stock_code_lookup.as_ref()));
        if issues.is_empty() {
            Ok(extraction)
        } else {
            Err(issues)
        }
    }

    fn build_output(&self, extraction: EventExtractionV1) -> EventExtractionOutput {
        EventExtractionOutput {
            extraction,
            metadata: EventExtractionMetadata {
                schema_version: EVENT_EXTRACTION_SCHEMA_VERSION.to_string(),
                prompt_version: PROMPT_VERSION.to_string(),
                model_name: self.model.clone(),
                model_parameters: self.model_parameters(),
            },
        }
    }
}

#[async_trait]
impl EventExtractor for LlmEventExtractor {
    async fn extract(&self, input: EventExtractionInput) -> Result<EventExtractionOutput> {
        let first_messages = self.initial_messages(&input)?;
        let first_content = self.send_chat_completion(first_messages).await?;

        match self.parse_and_validate(&input, &first_content) {
            Ok(extraction) => Ok(self.build_output(extraction)),
            Err(first_issues) => {
                let repair_messages =
                    self.repair_messages(&input, &first_content, &first_issues)?;
                let repaired_content = self.send_chat_completion(repair_messages).await?;

                match self.parse_and_validate(&input, &repaired_content) {
                    Ok(extraction) => Ok(self.build_output(extraction)),
                    Err(second_issues) => Err(validation_error(&second_issues)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, fs, path::PathBuf, sync::Arc};

    use axum::{extract::State, http::header, routing::post, Json, Router};
    use serde_json::{json, Value};
    use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
    use uuid::Uuid;

    use super::{LlmEventExtractor, PROMPT_VERSION};
    use crate::analysis::events::extraction::{
        EventExtractionInput, EventExtractor, ExtractionEvidence, StockCodeDirectory,
        EVENT_EXTRACTION_SCHEMA_VERSION,
    };
    use crate::config::Config;

    #[tokio::test]
    async fn extracts_valid_json_with_zero_temperature_and_prompt_metadata() {
        let (base_url, requests, server) = spawn_test_server(vec![valid_chat_response()]).await;
        let extractor = LlmEventExtractor::new(
            "test-key".to_string(),
            base_url,
            "gpt-4o-mini".to_string(),
            Arc::new(StockCodeDirectory::from_known_codes(["600519.SH"])),
        )
        .unwrap();

        let output = extractor.extract(sample_input()).await.unwrap();

        let requests = requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].get("temperature").and_then(Value::as_i64),
            Some(0)
        );
        assert_eq!(
            requests[0]
                .get("response_format")
                .and_then(|value| value.get("type"))
                .and_then(Value::as_str),
            Some("json_object")
        );
        assert!(requests[0]
            .get("messages")
            .and_then(Value::as_array)
            .and_then(|messages| messages.get(1))
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .unwrap()
            .contains("For amounts and dates, value must match text that appears verbatim in the evidence."));
        assert_eq!(
            output.metadata.schema_version,
            EVENT_EXTRACTION_SCHEMA_VERSION
        );
        assert_eq!(output.metadata.prompt_version, PROMPT_VERSION);
        assert_eq!(output.metadata.model_name, "gpt-4o-mini");
        assert_eq!(
            output
                .metadata
                .model_parameters
                .get("temperature")
                .and_then(Value::as_i64),
            Some(0)
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn retries_once_with_a_repair_prompt_after_invalid_first_response() {
        let (base_url, requests, server) =
            spawn_test_server(vec![invalid_chat_response(), valid_chat_response()]).await;
        let extractor = LlmEventExtractor::new(
            "test-key".to_string(),
            base_url,
            "gpt-4o-mini".to_string(),
            Arc::new(StockCodeDirectory::from_known_codes(["600519.SH"])),
        )
        .unwrap();

        let _output = extractor.extract(sample_input()).await.unwrap();

        let requests = requests.lock().await;
        assert_eq!(requests.len(), 2);
        let repair_message = requests[1]
            .get("messages")
            .and_then(Value::as_array)
            .and_then(|messages| messages.last())
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .unwrap();
        assert!(repair_message.contains("Repair the JSON only"));
        assert!(repair_message.contains("unknown field `unexpected`"));

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn returns_validation_error_after_second_failure() {
        let (base_url, _requests, server) =
            spawn_test_server(vec![invalid_chat_response(), invalid_chat_response()]).await;
        let extractor = LlmEventExtractor::new(
            "test-key".to_string(),
            base_url,
            "gpt-4o-mini".to_string(),
            Arc::new(StockCodeDirectory::from_known_codes(["600519.SH"])),
        )
        .unwrap();

        let error = extractor
            .extract(sample_input())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("event extraction validation failed"),
            "{error}"
        );
        assert!(error.contains("unknown field `unexpected`"), "{error}");

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn from_config_uses_openai_compatible_settings() {
        let mut config = base_config();
        config.ai_api_key = Some("config-key".to_string());
        config.ai_base_url = "https://example.test/v1".to_string();
        config.ai_model = "gpt-4.1-mini".to_string();

        let extractor = LlmEventExtractor::from_config(
            &config,
            Arc::new(StockCodeDirectory::from_known_codes(["600519.SH"])),
        )
        .unwrap();

        assert_eq!(
            extractor.endpoint(),
            "https://example.test/v1/chat/completions"
        );
        assert_eq!(extractor.model, "gpt-4.1-mini");
    }

    fn sample_input() -> EventExtractionInput {
        EventExtractionInput {
            evidence_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
            input_fingerprint: "fingerprint-v1".to_string(),
            evidence: vec![ExtractionEvidence::new(
                Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                "Kweichow Moutai (600519.SH) raised guidance on 2026-07-10 and expects CNY 2 billion in incremental revenue.".to_string(),
            )],
        }
    }

    async fn spawn_test_server(
        responses: Vec<Value>,
    ) -> (String, Arc<Mutex<Vec<Value>>>, JoinHandle<()>) {
        #[derive(Clone)]
        struct TestState {
            requests: Arc<Mutex<Vec<Value>>>,
            responses: Arc<Mutex<VecDeque<Value>>>,
        }

        async fn completions_handler(
            State(state): State<TestState>,
            Json(body): Json<Value>,
        ) -> ([(header::HeaderName, &'static str); 1], Json<Value>) {
            state.requests.lock().await.push(body);
            let payload = state
                .responses
                .lock()
                .await
                .pop_front()
                .expect("queued test response");

            ([(header::CONTENT_TYPE, "application/json")], Json(payload))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = TestState {
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        };
        let requests = Arc::clone(&state.requests);
        let app = Router::new()
            .route("/chat/completions", post(completions_handler))
            .with_state(state);
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{addr}"), requests, server)
    }

    fn valid_chat_response() -> Value {
        json!({
            "choices": [
                {
                    "message": {
                        "content": fixture_json()
                    }
                }
            ]
        })
    }

    fn invalid_chat_response() -> Value {
        json!({
            "choices": [
                {
                    "message": {
                        "content": r#"{
                          "event_type": "earnings",
                          "event_subtype": "guidance",
                          "claims": [],
                          "entities": [],
                          "amounts": [],
                          "dates": [],
                          "uncertainties": [],
                          "missing_information": [],
                          "unexpected": true
                        }"#
                    }
                }
            ]
        })
    }

    fn fixture_json() -> String {
        fs::read_to_string(fixture_path()).unwrap()
    }

    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/event_extraction_v1.json")
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
            official_event_feed_url: None,
            official_event_feed_api_key: None,
            official_event_source_id: "official:market_event".to_string(),
            official_event_store_full_content: false,
            enable_gdelt_events: false,
            gdelt_event_query: String::new(),
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
}
