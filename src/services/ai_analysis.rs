use chrono::NaiveDate;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

use crate::data::types::IndexData;
use crate::error::Result;
use crate::market_time::beijing_today;
use crate::services::decision_support_compat::{
    load_or_build_for_date, DecisionSupportCompatibilityContext,
};
use crate::services::trend_analyzer::TrendAnalysis;
use crate::state::AppState;

const MAIN_INDICES: &[(&str, &str)] = &[
    ("000001.SH", "SSE Composite"),
    ("399001.SZ", "SZSE Component"),
    ("399006.SZ", "ChiNext"),
    ("000688.SH", "STAR 50"),
];

#[derive(Debug, Clone, Serialize)]
pub struct SectorMove {
    pub name: String,
    #[serde(rename = "changePct")]
    pub change_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopStockInsight {
    pub code: String,
    pub name: String,
    #[serde(rename = "changePct")]
    pub change_pct: f64,
    pub trend: Option<TrendAnalysis>,
    #[serde(rename = "horizon", skip_serializing_if = "Option::is_none")]
    pub horizon: Option<String>,
    #[serde(rename = "supportTier", skip_serializing_if = "Option::is_none")]
    pub support_tier: Option<String>,
    #[serde(rename = "finalScore", skip_serializing_if = "Option::is_none")]
    pub final_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketOverviewResponse {
    pub date: String,
    pub indices: Vec<IndexData>,
    #[serde(rename = "upCount")]
    pub up_count: usize,
    #[serde(rename = "downCount")]
    pub down_count: usize,
    #[serde(rename = "flatCount")]
    pub flat_count: usize,
    #[serde(rename = "limitUpCount")]
    pub limit_up_count: usize,
    #[serde(rename = "totalAmount")]
    pub total_amount: f64,
    #[serde(rename = "topSectors")]
    pub top_sectors: Vec<SectorMove>,
    #[serde(rename = "bottomSectors")]
    pub bottom_sectors: Vec<SectorMove>,
    #[serde(rename = "topStock")]
    pub top_stock: Option<TopStockInsight>,
    /// Deprecated compatibility field. Rendered from the structured
    /// DecisionSupport brief, not from any LLM-generated narrative.
    #[serde(rename = "aiNarrative")]
    pub ai_narrative: Option<String>,
    pub report: String,
}

pub struct AiAnalysisService {
    pub state: Arc<AppState>,
}

impl AiAnalysisService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn market_overview(&self, date: Option<NaiveDate>) -> Result<MarketOverviewResponse> {
        let trade_date = date.unwrap_or_else(beijing_today);
        let context = load_or_build_for_date(&self.state, trade_date).await?;

        let mut response = MarketOverviewResponse::from_decision_support(&context);
        response.report = self.build_report(&response, &context);
        Ok(response)
    }

    pub async fn generate_daily_report(&self, date: Option<NaiveDate>) -> Result<String> {
        let overview = self.market_overview(date).await?;
        Ok(overview.report)
    }

    fn build_report(
        &self,
        overview: &MarketOverviewResponse,
        context: &DecisionSupportCompatibilityContext,
    ) -> String {
        let mut lines = Vec::new();
        lines.push(format!("<b>{} Market Dashboard</b>", overview.date));
        lines.push(String::new());

        if overview.indices.is_empty() {
            lines.push("No index snapshot available".to_string());
        } else {
            lines.push("<b>Major Indices</b>".to_string());
            for idx in &overview.indices {
                lines.push(format!(
                    "{}: {:.2} ({:+.2}%)",
                    idx.name, idx.close, idx.change_pct
                ));
            }
        }

        lines.push(String::new());
        lines.push("<b>Market Breadth</b>".to_string());
        lines.push(format!(
            "Up {} | Down {} | Flat {} | Limit-up {}",
            overview.up_count, overview.down_count, overview.flat_count, overview.limit_up_count
        ));
        lines.push(format!("Turnover {:.0} bn", overview.total_amount / 1e8));

        if !context.candidates.is_empty() {
            lines.push(String::new());
            lines.push("<b>Top Decision Candidates</b>".to_string());
            let text = context
                .candidates
                .iter()
                .take(3)
                .map(|candidate| {
                    format!(
                        "{} {} {} {:.2}",
                        candidate.code,
                        candidate.horizon,
                        candidate.support_tier,
                        candidate.final_score
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(text);
        }

        if let Some(top) = &overview.top_stock {
            lines.push(String::new());
            lines.push("<b>Top Candidate Focus</b>".to_string());
            lines.push(format!("{} {}", top.code, top.name));
            if let Some(final_score) = top.final_score {
                lines.push(format!(
                    "horizon={} | tier={} | score={:.2}",
                    top.horizon.as_deref().unwrap_or("n/a"),
                    top.support_tier.as_deref().unwrap_or("n/a"),
                    final_score
                ));
            }
        }

        if let Some(text) = overview.ai_narrative.as_deref() {
            lines.push(String::new());
            lines.push("<b>DecisionSupport Brief</b>".to_string());
            lines.push(text.to_string());
        }

        lines.join("\n")
    }

    fn parse_indices(metrics: &Value) -> Vec<IndexData> {
        metrics
            .get("indices")
            .and_then(Value::as_array)
            .map(|indices| {
                indices
                    .iter()
                    .filter_map(|item| {
                        let code = item.get("code")?.as_str()?.to_string();
                        let trade_date = parse_date_field(item, &["trade_date", "tradeDate"])?;
                        Some(IndexData {
                            name: index_name(&code),
                            code,
                            trade_date,
                            close: number_field(item, &["close"]),
                            change_pct: number_field(item, &["change_pct", "changePct"]),
                            volume: integer_field(item, &["volume"]),
                            amount: number_field(item, &["amount"]),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn build_deprecated_ai_narrative(
        context: &DecisionSupportCompatibilityContext,
    ) -> Option<String> {
        context
            .brief
            .as_ref()
            .and_then(Self::render_structured_brief)
            .or_else(|| {
                if context.candidates.is_empty() {
                    None
                } else {
                    let top_candidates = context
                        .candidates
                        .iter()
                        .take(3)
                        .map(|candidate| {
                            format!(
                                "{} {} {} {:.2}",
                                candidate.code,
                                candidate.horizon,
                                candidate.support_tier,
                                candidate.final_score
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    Some(Self::escape_html(&format!(
                        "DecisionSupport {} candidates: {}",
                        context.trade_date, top_candidates
                    )))
                }
            })
    }

    fn render_structured_brief(
        brief: &crate::storage::decision_support_repository::DecisionBriefRow,
    ) -> Option<String> {
        if !brief.content.trim().is_empty() {
            return Some(Self::escape_html(&brief.content));
        }

        let candidate_count = brief
            .structured_payload
            .get("candidateCount")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let top_candidates = brief
            .structured_payload
            .get("topCandidates")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let code = item.get("code")?.as_str()?;
                        let horizon = item.get("horizon").and_then(Value::as_str).unwrap_or("n/a");
                        let score = item
                            .get("finalScore")
                            .and_then(Value::as_f64)
                            .unwrap_or_default();
                        Some(format!("{code} {horizon} {score:.2}"))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let data_complete = brief
            .structured_payload
            .get("dataStatus")
            .and_then(|status| status.get("dataComplete"))
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let missing_inputs = brief
            .structured_payload
            .get("dataStatus")
            .and_then(|status| status.get("missingInputs"))
            .and_then(Value::as_array)
            .map(|items| items.len())
            .unwrap_or(0);

        Some(Self::escape_html(&format!(
            "DecisionSupport persisted with {} candidates; top: {}; data_status={} missing_inputs={}",
            candidate_count,
            if top_candidates.is_empty() {
                "none"
            } else {
                &top_candidates
            },
            if data_complete { "complete" } else { "incomplete" },
            missing_inputs
        )))
    }

    fn top_stock(context: &DecisionSupportCompatibilityContext) -> Option<TopStockInsight> {
        let candidate = context.candidates.first()?;
        Some(TopStockInsight {
            code: candidate.code.clone(),
            name: candidate.name.clone(),
            change_pct: 0.0,
            trend: None,
            horizon: Some(candidate.horizon.clone()),
            support_tier: Some(candidate.support_tier.clone()),
            final_score: Some(candidate.final_score),
        })
    }

    fn escape_html(input: &str) -> String {
        input
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }
}

impl MarketOverviewResponse {
    fn from_decision_support(context: &DecisionSupportCompatibilityContext) -> Self {
        let metrics = &context.market_metrics;
        let breadth = metrics.get("breadth").unwrap_or(&Value::Null);

        Self {
            date: context.trade_date.to_string(),
            indices: AiAnalysisService::parse_indices(metrics),
            up_count: usize_field(breadth, &["up_count", "upCount"]),
            down_count: usize_field(breadth, &["down_count", "downCount"]),
            flat_count: usize_field(breadth, &["flat_count", "flatCount"]),
            limit_up_count: usize_field(breadth, &["limit_up_count", "limitUpCount"]),
            total_amount: number_field(breadth, &["total_amount", "totalAmount"]),
            top_sectors: Vec::new(),
            bottom_sectors: Vec::new(),
            top_stock: AiAnalysisService::top_stock(context),
            ai_narrative: AiAnalysisService::build_deprecated_ai_narrative(context),
            report: String::new(),
        }
    }
}

fn index_name(code: &str) -> String {
    MAIN_INDICES
        .iter()
        .find_map(|(index_code, name)| (*index_code == code).then(|| (*name).to_string()))
        .unwrap_or_else(|| code.to_string())
}

fn parse_date_field(value: &Value, keys: &[&str]) -> Option<NaiveDate> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(Value::as_str)
        .and_then(|raw| NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok())
}

fn number_field(value: &Value, keys: &[&str]) -> f64 {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

fn integer_field(value: &Value, keys: &[&str]) -> i64 {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

fn usize_field(value: &Value, keys: &[&str]) -> usize {
    integer_field(value, keys).max(0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use axum::{http::StatusCode, routing::post, Router};
    use chrono::{DateTime, TimeZone, Utc};
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::analysis::market_snapshot::{MarketSnapshot, MARKET_SNAPSHOT_VERSION};
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::provider::DataProvider;
    use crate::data::types::{Candle, LimitUpStock, SectorData, StockInfo};
    use crate::state::AppState;
    use crate::storage::decision_support_repository::{
        DecisionBriefRow, DecisionCandidateRow, DecisionSupportRepository, DecisionSupportRunRow,
    };
    use crate::storage::market_repository::MarketRepository;
    use crate::telegram::pusher::TelegramPusher;

    #[sqlx::test(migrations = "./migrations")]
    async fn market_overview_uses_persisted_decision_support_without_llm_call(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let llm_calls = Arc::new(AtomicUsize::new(0));
        let (llm_base_url, llm_handle) = spawn_failing_llm_server(llm_calls.clone()).await;

        let trade_date = date(2026, 7, 11);
        seed_market_snapshot(&pool, trade_date).await?;
        seed_persisted_decision_artifact(&pool, trade_date).await?;

        let state = test_state(pool, Some("test-ai-key"), &llm_base_url).await;

        let overview = AiAnalysisService::new(state)
            .market_overview(Some(trade_date))
            .await
            .expect("market overview should load persisted decision support");

        llm_handle.abort();

        assert_eq!(overview.date, "2026-07-11");
        assert_eq!(overview.up_count, 321);
        assert_eq!(overview.down_count, 123);
        assert_eq!(overview.limit_up_count, 12);
        assert_eq!(overview.total_amount, 987_654_321.0);
        assert_eq!(overview.indices.len(), 2);
        assert_eq!(
            overview.ai_narrative.as_deref(),
            Some("Persisted DecisionSupport brief")
        );
        assert!(overview.report.contains("Persisted DecisionSupport brief"));
        assert_eq!(llm_calls.load(Ordering::SeqCst), 0);

        Ok(())
    }

    async fn spawn_failing_llm_server(
        llm_calls: Arc<AtomicUsize>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake llm server");
        let addr = listener.local_addr().expect("llm server addr");

        let app = Router::new().route(
            "/chat/completions",
            post({
                let llm_calls = llm_calls.clone();
                move || {
                    let llm_calls = llm_calls.clone();
                    async move {
                        llm_calls.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "llm endpoint should remain unused",
                        )
                    }
                }
            }),
        );

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve fake llm");
        });

        (format!("http://{addr}"), handle)
    }

    async fn seed_market_snapshot(pool: &PgPool, trade_date: NaiveDate) -> sqlx::Result<()> {
        MarketRepository::new(pool.clone())
            .save_market_snapshot(&MarketSnapshot {
                trade_date,
                snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
                available_at: dt(2026, 7, 11, 18),
                data_complete: true,
                metrics: json!({
                    "breadth": {
                        "up_count": 321,
                        "down_count": 123,
                        "flat_count": 45,
                        "limit_up_count": 12,
                        "total_amount": 987654321.0
                    },
                    "indices": [
                        {
                            "code": "000001.SH",
                            "trade_date": trade_date,
                            "close": 3100.0,
                            "change_pct": 1.2,
                            "volume": 1000,
                            "amount": 2000.0
                        },
                        {
                            "code": "399001.SZ",
                            "trade_date": trade_date,
                            "close": 9900.0,
                            "change_pct": -0.4,
                            "volume": 3000,
                            "amount": 4000.0
                        }
                    ]
                }),
                missing_inputs: Vec::new(),
                input_fingerprint: "snapshot-fingerprint".to_string(),
            })
            .await
            .unwrap();

        Ok(())
    }

    async fn seed_persisted_decision_artifact(
        pool: &PgPool,
        trade_date: NaiveDate,
    ) -> sqlx::Result<()> {
        let repo = DecisionSupportRepository::new(pool.clone());
        let run_id = Uuid::new_v4();
        let run = DecisionSupportRunRow {
            run_id,
            trade_date,
            support_version: "decision-support-v1".to_string(),
            market_snapshot_version: MARKET_SNAPSHOT_VERSION.to_string(),
            pattern_set_id: None,
            event_brief_version: Some("daily-event-brief-v1".to_string()),
            event_score_enabled: false,
            event_score_limit: 0.0,
            status: "completed".to_string(),
            input_fingerprint: format!("fp-{trade_date}"),
            started_at: dt(2026, 7, 11, 19),
            completed_at: Some(dt(2026, 7, 11, 19)),
            error_message: None,
        };
        let candidate = DecisionCandidateRow {
            run_id,
            code: "600000.SH".to_string(),
            name: "Alpha Bank".to_string(),
            horizon: "short".to_string(),
            base_source: "scan_ranker".to_string(),
            base_score: 90.0,
            pattern_score: Some(1.2),
            event_adjustment: Some(0.0),
            risk_adjustment: Some(-0.2),
            final_score: 91.0,
            support_tier: "watch".to_string(),
            facts: json!([{
                "kind": "event_fact",
                "statement": "Alpha Bank has persisted factual context",
                "sourceRefs": ["event:brief"]
            }]),
            calculations: json!([{
                "kind": "pattern_similarity",
                "statement": "Similarity score retained",
                "sourceRefs": []
            }]),
            inferences: json!([{
                "kind": "impact_hypothesis",
                "statement": "Momentum may continue",
                "sourceRefs": []
            }]),
            unknowns: json!([{
                "kind": "missing_status",
                "statement": "Catalyst confirmation pending",
                "sourceRefs": []
            }]),
            risk_flags: json!(["liquidity_watch"]),
            invalidations: json!(["close_below_ma20"]),
            source_refs: json!(["event:brief", "scan:ranked_pool"]),
            created_at: dt(2026, 7, 11, 19),
        };
        let brief = DecisionBriefRow {
            run_id,
            trade_date,
            content: "Persisted DecisionSupport brief".to_string(),
            structured_payload: json!({
                "candidateCount": 1,
                "topCandidates": [{
                    "code": "600000.SH",
                    "name": "Alpha Bank",
                    "horizon": "short",
                    "supportTier": "watch",
                    "finalScore": 91.0
                }],
                "dataStatus": {
                    "dataComplete": true,
                    "missingInputs": []
                }
            }),
            created_at: dt(2026, 7, 11, 20),
        };

        repo.create_run(&run).await.unwrap();
        repo.save_candidates(&[candidate]).await.unwrap();
        repo.save_brief(&brief).await.unwrap();
        Ok(())
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    async fn test_state(
        pool: PgPool,
        ai_api_key: Option<&str>,
        ai_base_url: &str,
    ) -> Arc<AppState> {
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
                ai_api_key: ai_api_key.map(str::to_string),
                ai_base_url: ai_base_url.to_string(),
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
            provider: Arc::new(PanicDataProvider),
            point_in_time_provider: Arc::new(PanicPointInTimeProvider),
            pusher: Arc::new(TelegramPusher::new("test".to_string())),
            fetch_job_lock: Arc::new(tokio::sync::Mutex::new(())),
            analysis_job_lock: Arc::new(tokio::sync::Mutex::new(())),
            scan_job_lock: Arc::new(tokio::sync::Mutex::new(())),
            daily_report_job_lock: Arc::new(tokio::sync::Mutex::new(())),
            weekly_report_job_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    struct PanicDataProvider;

    #[async_trait]
    impl DataProvider for PanicDataProvider {
        fn name(&self) -> &'static str {
            "panic-provider"
        }

        async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
            panic!("data provider should not be called");
        }

        async fn get_daily_bars_by_date(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<(String, Candle)>> {
            panic!("data provider should not be called");
        }

        async fn get_daily_bars_for_stock(
            &self,
            _code: &str,
            _start_date: NaiveDate,
            _end_date: NaiveDate,
        ) -> Result<Vec<Candle>> {
            panic!("data provider should not be called");
        }

        async fn get_trading_dates(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<NaiveDate>> {
            panic!("data provider should not be called");
        }

        async fn get_limit_up_stocks(&self, _trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
            panic!("data provider should not be called");
        }

        async fn get_index_daily(
            &self,
            _code: &str,
            _trade_date: NaiveDate,
        ) -> Result<Option<IndexData>> {
            panic!("data provider should not be called");
        }

        async fn get_sector_data(&self, _trade_date: NaiveDate) -> Result<Vec<SectorData>> {
            panic!("data provider should not be called");
        }
    }

    struct PanicPointInTimeProvider;

    #[async_trait]
    impl PointInTimeDataProvider for PanicPointInTimeProvider {
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

        async fn get_security_master_versions(
            &self,
        ) -> Result<Vec<crate::analysis::market_snapshot::SecurityMasterVersion>> {
            panic!("point-in-time provider should not be called");
        }

        async fn get_corporate_actions(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<crate::analysis::market_snapshot::CorporateAction>> {
            panic!("point-in-time provider should not be called");
        }

        async fn get_adjustment_factors(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<crate::analysis::market_snapshot::AdjustmentFactor>> {
            panic!("point-in-time provider should not be called");
        }

        async fn get_daily_basics(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<crate::analysis::market_snapshot::DailyBasicSnapshot>> {
            panic!("point-in-time provider should not be called");
        }

        async fn get_security_statuses(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<crate::analysis::market_snapshot::SecurityDailyStatus>> {
            panic!("point-in-time provider should not be called");
        }

        async fn get_index_daily_range(
            &self,
            _codes: &[String],
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<crate::analysis::market_snapshot::IndexDailyBar>> {
            panic!("point-in-time provider should not be called");
        }

        async fn get_sector_memberships(
            &self,
            _as_of_date: NaiveDate,
        ) -> Result<Vec<crate::analysis::market_snapshot::SectorMembership>> {
            panic!("point-in-time provider should not be called");
        }
    }
}
