use chrono::{Datelike, NaiveDate, Timelike};
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::warn;

use crate::data::types::IndexData;
use crate::error::Result;
use crate::market_time::{beijing_now, beijing_today};
use crate::services::trend_analyzer::{TrendAnalysis, TrendAnalyzer};
use crate::state::AppState;
use crate::storage::postgres;
use crate::telegram::pusher::TelegramPusher;

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
        let date = date.unwrap_or_else(beijing_today);

        let mut indices = Vec::new();
        for (code, name) in MAIN_INDICES {
            if let Some(mut row) = self.state.provider.get_index_daily(code, date).await? {
                row.name = (*name).to_string();
                indices.push(row);
            }
        }

        let breadth: Option<(
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT
                  COUNT(CASE WHEN close > open THEN 1 END),
                  COUNT(CASE WHEN close < open THEN 1 END),
                  COUNT(CASE WHEN close = open THEN 1 END),
                  COUNT(CASE WHEN prev_close > 0 AND (close - prev_close) / prev_close * 100 >= 9.8 THEN 1 END),
                  SUM(amount)
               FROM (
                 SELECT trade_date,
                        open::float8 AS open,
                        close::float8 AS close,
                        amount::float8 AS amount,
                        LAG(close::float8) OVER (PARTITION BY code ORDER BY trade_date) AS prev_close
                 FROM stock_daily_bars
                 WHERE trade_date <= $1
               ) bars
               WHERE trade_date = $1"#,
        )
        .bind(date)
        .fetch_optional(&self.state.db)
        .await?;

        let (up_count, down_count, flat_count, limit_up_count, total_amount) =
            breadth.unwrap_or((Some(0), Some(0), Some(0), Some(0), Some(0.0)));

        let top_rows: Vec<(String, Option<f64>)> = sqlx::query_as(
            r#"SELECT name, change_pct::float8
               FROM sector_daily
               WHERE trade_date = $1
               ORDER BY change_pct DESC NULLS LAST
               LIMIT 5"#,
        )
        .bind(date)
        .fetch_all(&self.state.db)
        .await?;

        let bottom_rows: Vec<(String, Option<f64>)> = sqlx::query_as(
            r#"SELECT name, change_pct::float8
               FROM sector_daily
               WHERE trade_date = $1
               ORDER BY change_pct ASC NULLS LAST
               LIMIT 5"#,
        )
        .bind(date)
        .fetch_all(&self.state.db)
        .await?;

        let top_sectors = top_rows
            .into_iter()
            .map(|(name, change_pct)| SectorMove {
                name,
                change_pct: change_pct.unwrap_or(0.0),
            })
            .collect::<Vec<_>>();

        let bottom_sectors = bottom_rows
            .into_iter()
            .map(|(name, change_pct)| SectorMove {
                name,
                change_pct: change_pct.unwrap_or(0.0),
            })
            .collect::<Vec<_>>();

        let top_stock = self.top_stock_insight(date).await?;

        let mut response = MarketOverviewResponse {
            date: date.to_string(),
            indices,
            up_count: up_count.unwrap_or(0).max(0) as usize,
            down_count: down_count.unwrap_or(0).max(0) as usize,
            flat_count: flat_count.unwrap_or(0).max(0) as usize,
            limit_up_count: limit_up_count.unwrap_or(0).max(0) as usize,
            total_amount: total_amount.unwrap_or(0.0),
            top_sectors,
            bottom_sectors,
            top_stock,
            ai_narrative: None,
            report: String::new(),
        };

        response.ai_narrative = self.generate_ai_narrative(&response).await;
        response.report = self.build_report(&response, response.ai_narrative.as_deref());

        Ok(response)
    }

    pub async fn generate_daily_report(&self, date: Option<NaiveDate>) -> Result<String> {
        let overview = self.market_overview(date).await?;
        Ok(overview.report)
    }

    pub async fn run_daily_loop(&self, pusher: Arc<TelegramPusher>, channel: String) {
        let mut triggered_today: HashSet<String> = HashSet::new();

        loop {
            let now = beijing_now();
            let date_key = now.format("%Y-%m-%d").to_string();

            if now.hour() == 0 && now.minute() == 0 {
                triggered_today.clear();
            }

            if now.weekday().number_from_monday() <= 5
                && now.hour() == 15
                && now.minute() == 30
                && !triggered_today.contains(&date_key)
            {
                triggered_today.insert(date_key);
                match self.generate_daily_report(Some(beijing_today())).await {
                    Ok(report) => {
                        if let Err(e) = pusher.push(&channel, &report).await {
                            warn!("AI daily report push failed: {}", e);
                        }
                    }
                    Err(e) => warn!("AI daily report generation failed: {}", e),
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }

    async fn top_stock_insight(&self, date: NaiveDate) -> Result<Option<TopStockInsight>> {
        let top_row: Option<(String, String, Option<f64>)> = sqlx::query_as(
            r#"WITH ranked AS (
                 SELECT b.code,
                        COALESCE(i.name, b.code) AS name,
                        b.trade_date,
                        b.close::float8 AS close,
                        LAG(b.close::float8) OVER (PARTITION BY b.code ORDER BY b.trade_date) AS prev_close
                 FROM stock_daily_bars b
                 LEFT JOIN stock_info i ON i.code = b.code
                 WHERE b.trade_date <= $1
               )
               SELECT code,
                      name,
                      ((close - prev_close) / NULLIF(prev_close, 0)) * 100 AS change_pct
               FROM ranked
               WHERE trade_date = $1 AND prev_close IS NOT NULL
               ORDER BY change_pct DESC NULLS LAST
               LIMIT 1"#,
        )
        .bind(date)
        .fetch_optional(&self.state.db)
        .await?;

        let Some((code, name, change_pct)) = top_row else {
            return Ok(None);
        };

        let bars = postgres::get_stock_history(&self.state.db, &code, 220).await?;
        let trend = TrendAnalyzer::analyze(&code, &bars);

        Ok(Some(TopStockInsight {
            code,
            name,
            change_pct: change_pct.unwrap_or(0.0),
            trend,
        }))
    }

    async fn generate_ai_narrative(&self, overview: &MarketOverviewResponse) -> Option<String> {
        let api_key = self.state.config.ai_api_key.as_ref()?;
        let base = self.state.config.ai_base_url.trim_end_matches('/');
        let url = format!("{}/chat/completions", base);
        let prompt = self.build_ai_prompt(overview);

        let body = json!({
            "model": self.state.config.ai_model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an A-share market analyst. Produce concise, actionable analysis with risk-first discipline. Output plain text only."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.3,
            "max_tokens": 800
        });

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(45))
            .build()
            .unwrap_or_default();

        let resp = match client
            .post(url)
            .bearer_auth(api_key)
            .json(&body)
            .send()
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!("AI narrative request failed: {}", e);
                return None;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!("AI narrative API error {}: {}", status, text);
            return None;
        }

        let payload: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!("AI narrative decode failed: {}", e);
                return None;
            }
        };

        let content = payload
            .get("choices")
            .and_then(|c| c.as_array())
            .and_then(|arr| arr.first())
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        content.map(|s| Self::escape_html(&s))
    }

    fn build_ai_prompt(&self, overview: &MarketOverviewResponse) -> String {
        let index_text = overview
            .indices
            .iter()
            .map(|i| format!("{} {:+.2}%", i.name, i.change_pct))
            .collect::<Vec<_>>()
            .join(", ");

        let top_sector_text = overview
            .top_sectors
            .iter()
            .take(5)
            .map(|s| format!("{}({:+.2}%)", s.name, s.change_pct))
            .collect::<Vec<_>>()
            .join(", ");

        let weak_sector_text = overview
            .bottom_sectors
            .iter()
            .take(5)
            .map(|s| format!("{}({:+.2}%)", s.name, s.change_pct))
            .collect::<Vec<_>>()
            .join(", ");

        let top_stock_text = match &overview.top_stock {
            Some(ts) => {
                let trend_text = ts
                    .trend
                    .as_ref()
                    .map(|t| {
                        format!(
                            "trend={:?}, signal={:?}, score={:.0}, ma20_bias={:+.2}%",
                            t.trend_status, t.buy_signal, t.score, t.bias_ma20
                        )
                    })
                    .unwrap_or_else(|| "trend unavailable".to_string());
                format!(
                    "{} {} ({:+.2}%), {}",
                    ts.code, ts.name, ts.change_pct, trend_text
                )
            }
            None => "none".to_string(),
        };

        format!(
            "Date: {}\nIndices: {}\nBreadth: up={} down={} flat={} limit_up={}\nTurnover(yuan): {:.0}\nTop sectors: {}\nWeak sectors: {}\nTop stock: {}\n\nWrite a 4-part report:\n1) Market regime summary\n2) Sector rotation interpretation\n3) Actionable plan (position sizing + setups to prioritize/avoid)\n4) Risk checklist for next session\n\nConstraints:\n- Keep under 350 words\n- Be concrete, no generic disclaimers\n- Risk-first tone",
            overview.date,
            index_text,
            overview.up_count,
            overview.down_count,
            overview.flat_count,
            overview.limit_up_count,
            overview.total_amount,
            top_sector_text,
            weak_sector_text,
            top_stock_text
        )
    }

    fn build_report(
        &self,
        overview: &MarketOverviewResponse,
        ai_narrative: Option<&str>,
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

        if !overview.top_sectors.is_empty() {
            lines.push(String::new());
            lines.push("<b>Top Sectors</b>".to_string());
            let text = overview
                .top_sectors
                .iter()
                .take(3)
                .map(|s| format!("{}({:+.2}%)", s.name, s.change_pct))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(text);
        }

        if !overview.bottom_sectors.is_empty() {
            lines.push(String::new());
            lines.push("<b>Weak Sectors</b>".to_string());
            let text = overview
                .bottom_sectors
                .iter()
                .take(3)
                .map(|s| format!("{}({:+.2}%)", s.name, s.change_pct))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(text);
        }

        if let Some(top) = &overview.top_stock {
            lines.push(String::new());
            lines.push("<b>Top Gainer Focus</b>".to_string());
            lines.push(format!(
                "{} {} ({:+.2}%)",
                top.code, top.name, top.change_pct
            ));
            if let Some(trend) = &top.trend {
                lines.push(format!(
                    "Trend score {:.0}, MA20 bias {:+.2}%",
                    trend.score, trend.bias_ma20
                ));
                lines.push(format!("Signal: {:?}", trend.buy_signal));
            }
        }

        if let Some(text) = ai_narrative {
            lines.push(String::new());
            lines.push("<b>AI Narrative</b>".to_string());
            lines.push(text.to_string());
        }

        lines.join("\n")
    }

    fn escape_html(input: &str) -> String {
        input
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }
}
