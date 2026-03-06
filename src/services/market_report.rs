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

        let mut report = format!("📅 <b>周报 - {}</b>\n\n", date.format("%Y-%m-%d"));
        report.push_str("🏆 <b>本周涨幅榜 Top 20</b>\n");
        for (i, (code, name, gain_pct)) in rows.iter().enumerate() {
            let gain = gain_pct.unwrap_or(0.0);
            report.push_str(&format!(
                "{}. {} {} {}{:.1}%\n",
                i + 1,
                code,
                name.as_deref().unwrap_or(""),
                if gain >= 0.0 { "+" } else { "" },
                gain,
            ));
        }

        postgres::save_report(&self.state.db, "weekly", &report).await?;
        Ok(report)
    }
}
