use chrono::{Datelike, Duration, NaiveDate};
use std::sync::Arc;
use tracing::info;

use crate::error::Result;
use crate::market_time::beijing_today;
use crate::services::limit_up::LimitUpService;
use crate::services::market::MarketService;
use crate::services::sector::SectorService;
use crate::state::AppState;
use crate::storage::{postgres, redis_cache::RedisCache};
use crate::telegram::formatter;

pub struct MarketReportService {
    state: Arc<AppState>,
    market: Arc<MarketService>,
    limit_up: Arc<LimitUpService>,
    sector: Arc<SectorService>,
}

impl MarketReportService {
    pub fn new(
        state: Arc<AppState>,
        market: Arc<MarketService>,
        limit_up: Arc<LimitUpService>,
        sector: Arc<SectorService>,
    ) -> Self {
        MarketReportService {
            state,
            market,
            limit_up,
            sector,
        }
    }

    pub async fn generate_daily(&self, date: NaiveDate) -> Result<String> {
        info!("Generating daily report for {}", date);

        let overview = self.market.get_market_overview(date).await?;
        let limit_summary = self.limit_up.get_summary(date).await?;
        let top_sectors = self.sector.get_top_sectors(date, 5).await?;
        let bottom_sectors = self.sector.get_bottom_sectors(date, 3).await?;

        let mut cache = RedisCache::new(self.state.redis.clone());
        let scan_hits: Option<serde_json::Value> = cache.get_scan_results().await.ok().flatten();

        let report = formatter::format_daily_report(
            &overview,
            &limit_summary,
            &top_sectors,
            &bottom_sectors,
            scan_hits.as_ref(),
        );

        postgres::save_report(&self.state.db, "daily", &report).await?;
        info!("Daily report generated ({} chars)", report.len());
        Ok(report)
    }

    pub async fn generate_limitup_report(&self, date: NaiveDate) -> Result<String> {
        info!("Generating standalone limit-up report for {}", date);
        let stocks = self.limit_up.get_stocks_by_date(date).await?;
        let report = formatter::format_limit_up_report(date, &stocks);
        postgres::save_report(&self.state.db, "limitup", &report).await?;
        Ok(report)
    }

    pub async fn generate_strong_report(&self, date: NaiveDate, days: i64) -> Result<String> {
        info!("Generating standalone strong-stock report for {}", date);
        let stocks = self.limit_up.get_strong_stocks(days, 3).await?;
        let report = formatter::format_strong_stock_report(date, days, &stocks);
        postgres::save_report(&self.state.db, "strong", &report).await?;
        Ok(report)
    }

    pub async fn generate_weekly(&self) -> Result<String> {
        let date = beijing_today();
        let start = date - Duration::days(date.weekday().num_days_from_monday() as i64);

        let rows: Vec<(String, Option<String>, Option<f64>)> = sqlx::query_as(
            r#"WITH ranked AS (
                 SELECT b.code,
                        i.name,
                        b.close::float8 AS close,
                        b.trade_date,
                        ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY b.trade_date ASC) AS rn_first,
                        ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY b.trade_date DESC) AS rn_last
                 FROM stock_daily_bars b
                 LEFT JOIN stock_info i USING (code)
                 WHERE b.trade_date >= $1 AND b.trade_date <= $2
               )
               SELECT code,
                      MAX(name) AS name,
                      (
                          MAX(close) FILTER (WHERE rn_last = 1)
                          - MAX(close) FILTER (WHERE rn_first = 1)
                      ) / NULLIF(MAX(close) FILTER (WHERE rn_first = 1), 0) * 100 AS gain_pct
               FROM ranked
               GROUP BY code
               ORDER BY gain_pct DESC NULLS LAST LIMIT 20"#,
        )
        .bind(start)
        .bind(date)
        .fetch_all(&self.state.db)
        .await?;

        let report = format_weekly_report(date, &rows);

        postgres::save_report(&self.state.db, "weekly", &report).await?;
        Ok(report)
    }
}

fn format_weekly_report(date: NaiveDate, rows: &[(String, Option<String>, Option<f64>)]) -> String {
    let base = std::env::var("WEBHOOK_URL").ok();
    format_weekly_report_with_base(date, rows, base.as_deref())
}

fn format_weekly_report_with_base(
    date: NaiveDate,
    rows: &[(String, Option<String>, Option<f64>)],
    webhook_url: Option<&str>,
) -> String {
    let mut report = format!("📅 <b>周报 - {}</b>\n\n", date.format("%Y-%m-%d"));
    report.push_str("🏆 <b>本周涨幅榜 Top 20</b>\n");
    for (i, (code, name, gain_pct)) in rows.iter().enumerate() {
        let gain = gain_pct.unwrap_or(0.0);
        let label = match name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            Some(name) => format!("{code} {name}"),
            None => code.clone(),
        };
        report.push_str(&format!(
            "{}. {} {}{:.1}%\n",
            i + 1,
            formatter::stock_anchor_with_base(code, &label, webhook_url),
            if gain >= 0.0 { "+" } else { "" },
            gain,
        ));
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn weekly_report_links_top_gainers_to_internal_chart() {
        let report = format_weekly_report_with_base(
            NaiveDate::from_ymd_opt(2026, 3, 10).unwrap(),
            &[(
                "600519.SH".to_string(),
                Some("贵州茅台".to_string()),
                Some(12.3),
            )],
            Some("https://bot.example"),
        );

        assert!(report.contains("周报"));
        assert!(report.contains(
            "<a href=\"https://bot.example/miniapp/chart/?code=600519\">600519.SH 贵州茅台</a> +12.3%"
        ));
    }
}
