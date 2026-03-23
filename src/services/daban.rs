use std::cmp::Ordering;
use std::sync::Arc;

use chrono::{Datelike, NaiveDate, Timelike, Weekday};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};
use tracing::warn;

use crate::data::provider::DataProvider;
use crate::data::types::LimitUpStock;
use crate::error::Result;
use crate::state::AppState;
use crate::storage::postgres;
use crate::telegram::pusher::TelegramPusher;

#[derive(Debug, Clone, Serialize)]
pub struct DabanScore {
    pub code: String,
    pub name: String,
    pub score: f64,
    pub seal_score: f64,
    pub time_score: f64,
    pub burst_penalty: f64,
    pub executability: String,
    pub verdict: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DabanSummary {
    pub date: NaiveDate,
    pub total: usize,
    pub sealed: usize,
    pub burst: usize,
    pub avg_score: f64,
    pub sentiment: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DabanReport {
    pub summary: DabanSummary,
    pub top: Vec<DabanScore>,
}

pub struct DabanService {
    pub state: Arc<AppState>,
}

impl DabanService {
    pub fn new(state: Arc<AppState>) -> Self {
        DabanService { state }
    }

    pub fn score_stock(&self, stock: &LimitUpStock) -> DabanScore {
        // Seal strength score (0-30): fd_amount / 1M normalized
        let seal_score = (stock.fd_amount / 1_000_000.0).min(30.0);

        // Timing score (0-25): earlier limit-up is better
        let time_score = if let Some(ref t) = stock.first_time {
            let hour: u32 = t
                .split(':')
                .next()
                .and_then(|h| h.parse().ok())
                .unwrap_or(15);
            let min: u32 = t
                .split(':')
                .nth(1)
                .and_then(|m| m.parse().ok())
                .unwrap_or(0);
            let minutes_since_open = (hour * 60 + min).saturating_sub(9 * 60 + 30);
            (25.0 - (minutes_since_open as f64 / 6.0 * 25.0 / 60.0)).max(0.0)
        } else {
            5.0
        };

        // Burst penalty
        let burst_penalty = (stock.open_times as f64) * 5.0;

        let raw_score = seal_score + time_score - burst_penalty;
        let score = raw_score.clamp(0.0, 100.0);

        let executability = if stock.open_times == 0 && stock.pct_chg >= 9.8 {
            "一字板".to_string()
        } else if stock.open_times > 2 {
            "多次炸板".to_string()
        } else if score >= 60.0 {
            "可打".to_string()
        } else {
            "观望".to_string()
        };

        let verdict = if score >= 80.0 {
            "强烈推荐"
        } else if score >= 60.0 {
            "推荐"
        } else if score >= 40.0 {
            "观望"
        } else {
            "回避"
        }
        .to_string();

        DabanScore {
            code: stock.code.clone(),
            name: stock.name.clone(),
            score,
            seal_score,
            time_score,
            burst_penalty,
            executability,
            verdict,
        }
    }

    pub fn score_all(&self, stocks: &[LimitUpStock]) -> Vec<DabanScore> {
        let mut scores: Vec<DabanScore> = stocks.iter().map(|s| self.score_stock(s)).collect();
        scores.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        scores
    }

    pub async fn latest_trade_date(&self) -> Result<Option<NaiveDate>> {
        let row: Option<(NaiveDate,)> = sqlx::query_as(
            r#"SELECT trade_date FROM limit_up_stocks ORDER BY trade_date DESC LIMIT 1"#,
        )
        .fetch_optional(&self.state.db)
        .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn load_limit_up_by_date(&self, date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        let rows: Vec<(
            String,
            Option<String>,
            NaiveDate,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<String>,
            Option<i32>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT code, name, trade_date, close::float8, pct_chg::float8,
                      seal_amount::float8, limit_time, burst_count, strth::float8
               FROM limit_up_stocks
               WHERE trade_date = $1
               ORDER BY seal_amount DESC, pct_chg DESC"#,
        )
        .bind(date)
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    name,
                    trade_date,
                    close,
                    pct_chg,
                    seal_amount,
                    limit_time,
                    burst_count,
                    strth,
                )| {
                    LimitUpStock {
                        code,
                        name: name.unwrap_or_default(),
                        trade_date,
                        close: close.unwrap_or(0.0),
                        pct_chg: pct_chg.unwrap_or(0.0),
                        fd_amount: seal_amount.unwrap_or(0.0),
                        first_time: limit_time,
                        last_time: None,
                        open_times: burst_count.unwrap_or(0),
                        strth: strth.unwrap_or(0.0),
                        limit: "U".to_string(),
                    }
                },
            )
            .collect())
    }

    fn sentiment_from_metrics(total: usize, sealed: usize, avg_score: f64) -> String {
        if total == 0 {
            return "冷清".to_string();
        }
        let seal_rate = sealed as f64 / total as f64;
        if avg_score >= 70.0 && seal_rate >= 0.70 {
            "强势".to_string()
        } else if avg_score >= 55.0 && seal_rate >= 0.55 {
            "偏强".to_string()
        } else if avg_score >= 45.0 && seal_rate >= 0.45 {
            "中性".to_string()
        } else {
            "偏弱".to_string()
        }
    }

    pub async fn build_report(&self, date: Option<NaiveDate>, top_n: usize) -> Result<DabanReport> {
        let target_date = match date {
            Some(d) => d,
            None => self
                .latest_trade_date()
                .await?
                .unwrap_or_else(crate::market_time::beijing_today),
        };

        let stocks = self.load_limit_up_by_date(target_date).await?;
        let scores = self.score_all(&stocks);

        let total = scores.len();
        let burst = stocks.iter().filter(|s| s.open_times > 0).count();
        let sealed = total.saturating_sub(burst);
        let avg_score = if total == 0 {
            0.0
        } else {
            scores.iter().map(|s| s.score).sum::<f64>() / total as f64
        };
        let sentiment = Self::sentiment_from_metrics(total, sealed, avg_score);

        Ok(DabanReport {
            summary: DabanSummary {
                date: target_date,
                total,
                sealed,
                burst,
                avg_score,
                sentiment,
            },
            top: scores.into_iter().take(top_n.max(1)).collect(),
        })
    }

    pub fn format_report_text(report: &DabanReport) -> String {
        let base = std::env::var("WEBHOOK_URL").ok();
        Self::format_report_text_with_base(report, base.as_deref())
    }

    pub fn format_report_markup(report: &DabanReport) -> Option<Value> {
        let base = std::env::var("WEBHOOK_URL").ok();
        Self::format_report_markup_with_base(report, base.as_deref())
    }

    fn format_report_text_with_base(report: &DabanReport, _webhook_url: Option<&str>) -> String {
        let mut msg = format!(
            "🎯 <b>打板评分</b> {}\n总数: {} 封板: {} 炸板: {}\n情绪: <b>{}</b>  平均分: <b>{:.1}</b>\n\n",
            report.summary.date,
            report.summary.total,
            report.summary.sealed,
            report.summary.burst,
            report.summary.sentiment,
            report.summary.avg_score
        );

        for (idx, s) in report.top.iter().take(12).enumerate() {
            msg.push_str(&format!(
                "{}. {}  分数:{:.1}  {}\n",
                idx + 1,
                format!("{} {}", s.code, s.name).trim(),
                s.score,
                s.verdict
            ));
        }
        msg
    }

    fn format_report_markup_with_base(report: &DabanReport, webhook_url: Option<&str>) -> Option<Value> {
        let buttons: Vec<Vec<Value>> = report
            .top
            .iter()
            .take(12)
            .enumerate()
            .filter_map(|(idx, stock)| {
                let label = format!("{}. {} ({})", idx + 1, stock.name.trim(), stock.code.split('.').next().unwrap_or(&stock.code));
                crate::telegram::formatter::stock_button_with_base(&stock.code, &label, webhook_url)
                    .map(|button| vec![button])
            })
            .collect();

        if buttons.is_empty() {
            return None;
        }

        Some(json!({ "inline_keyboard": buttons }))
    }

    fn in_trading_hours() -> bool {
        let now = crate::market_time::beijing_now();
        let wd = now.weekday();
        if wd == Weekday::Sat || wd == Weekday::Sun {
            return false;
        }
        let t = now.time();
        // 09:30-11:30, 13:00-15:00
        let am = (t.hour() > 9 || (t.hour() == 9 && t.minute() >= 30))
            && (t.hour() < 11 || (t.hour() == 11 && t.minute() <= 30));
        let pm = (t.hour() >= 13) && (t.hour() < 15 || (t.hour() == 15 && t.minute() == 0));
        am || pm
    }

    fn live_signature(report: &DabanReport) -> String {
        let top = report
            .top
            .first()
            .map(|s| format!("{}:{:.1}", s.code, s.score))
            .unwrap_or_else(|| "none".to_string());
        format!(
            "{}:{}:{}:{}:{}",
            report.summary.date,
            report.summary.total,
            report.summary.sealed,
            report.summary.sentiment,
            top
        )
    }

    pub async fn run_live_loop(
        &self,
        provider: Arc<dyn DataProvider>,
        pusher: Arc<TelegramPusher>,
        channel: String,
    ) {
        let mut last_sig = String::new();
        loop {
            if !Self::in_trading_hours() {
                sleep(Duration::from_secs(60)).await;
                continue;
            }

            let today = crate::market_time::beijing_today();
            let stocks = match provider.get_limit_up_stocks(today).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("daban live fetch failed: {}", e);
                    sleep(Duration::from_secs(120)).await;
                    continue;
                }
            };

            if let Err(e) = postgres::save_limit_up_stocks(&self.state.db, &stocks).await {
                warn!("daban live save failed: {}", e);
            }

            let scores = self.score_all(&stocks);
            let total = scores.len();
            let burst = stocks.iter().filter(|s| s.open_times > 0).count();
            let sealed = total.saturating_sub(burst);
            let avg_score = if total == 0 {
                0.0
            } else {
                scores.iter().map(|s| s.score).sum::<f64>() / total as f64
            };
            let sentiment = Self::sentiment_from_metrics(total, sealed, avg_score);
            let report = DabanReport {
                summary: DabanSummary {
                    date: today,
                    total,
                    sealed,
                    burst,
                    avg_score,
                    sentiment,
                },
                top: scores.into_iter().take(20).collect(),
            };

            let sig = Self::live_signature(&report);
            if sig != last_sig {
                let msg = Self::format_report_text(&report);
                let push_result = match Self::format_report_markup(&report) {
                    Some(markup) => pusher.push_with_markup(&channel, &msg, markup).await,
                    None => pusher.push(&channel, &msg).await,
                };
                if let Err(e) = push_result {
                    warn!("daban live push failed: {}", e);
                } else {
                    last_sig = sig;
                }
            }

            sleep(Duration::from_secs(120)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_report_text_lists_ranked_stocks_without_html_links() {
        let report = DabanReport {
            summary: DabanSummary {
                date: NaiveDate::from_ymd_opt(2026, 3, 10).unwrap(),
                total: 1,
                sealed: 1,
                burst: 0,
                avg_score: 88.6,
                sentiment: "强势".to_string(),
            },
            top: vec![DabanScore {
                code: "600519.SH".to_string(),
                name: "贵州茅台".to_string(),
                score: 88.6,
                seal_score: 30.0,
                time_score: 20.0,
                burst_penalty: 0.0,
                executability: "可打".to_string(),
                verdict: "强烈推荐".to_string(),
            }],
        };

        let text =
            DabanService::format_report_text_with_base(&report, Some("https://bot.example/"));

        assert!(text.contains("打板评分"));
        assert!(text.contains("1. 600519.SH 贵州茅台  分数:88.6  强烈推荐"));
        assert!(!text.contains("<a href="));
    }

    #[test]
    fn format_report_markup_uses_web_app_buttons() {
        let report = DabanReport {
            summary: DabanSummary {
                date: NaiveDate::from_ymd_opt(2026, 3, 10).unwrap(),
                total: 1,
                sealed: 1,
                burst: 0,
                avg_score: 88.6,
                sentiment: "强势".to_string(),
            },
            top: vec![DabanScore {
                code: "600519.SH".to_string(),
                name: "贵州茅台".to_string(),
                score: 88.6,
                seal_score: 30.0,
                time_score: 20.0,
                burst_penalty: 0.0,
                executability: "可打".to_string(),
                verdict: "强烈推荐".to_string(),
            }],
        };

        let markup =
            DabanService::format_report_markup_with_base(&report, Some("https://bot.example/"))
                .expect("markup");

        assert_eq!(
            markup["inline_keyboard"][0][0]["web_app"]["url"].as_str(),
            Some("https://bot.example/miniapp/chart/?code=600519")
        );
        assert_eq!(
            markup["inline_keyboard"][0][0]["text"].as_str(),
            Some("1. 贵州茅台 (600519)")
        );
    }
}
