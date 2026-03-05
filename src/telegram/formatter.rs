use crate::services::limit_up::LimitUpSummary;
use crate::services::market::MarketOverview;
use crate::services::sector::SectorRank;

pub fn format_daily_report(
    overview: &MarketOverview,
    limit_up: &LimitUpSummary,
    top_sectors: &[SectorRank],
    bottom_sectors: &[SectorRank],
    scan_results: Option<&serde_json::Value>,
) -> String {
    let mut msg = String::new();

    msg.push_str(&format!(
        "📊 <b>每日市场报告 {}</b>\n\n",
        overview.date.format("%Y-%m-%d")
    ));

    // Indices
    msg.push_str("📈 <b>指数表现</b>\n");
    for idx in &overview.indices {
        let arrow = if idx.change_pct >= 0.0 { "🔺" } else { "🔻" };
        msg.push_str(&format!(
            "{} {} {}{:.2}%\n",
            arrow, idx.name, if idx.change_pct >= 0.0 { "+" } else { "" }, idx.change_pct
        ));
    }

    // Breadth
    msg.push_str(&format!(
        "\n🔢 上涨 {} | 下跌 {} | 涨停 {}\n",
        overview.up_count, overview.down_count, overview.limit_up_count
    ));

    // Limit-up summary
    msg.push_str(&format!(
        "\n🎯 <b>涨停板</b>\n总计: {} | 封: {} | 炸: {} | 炸板率: {:.1}%\n",
        limit_up.total, limit_up.sealed, limit_up.burst, limit_up.burst_rate
    ));

    // Top sectors
    if !top_sectors.is_empty() {
        msg.push_str("\n🏆 <b>板块涨幅榜</b>\n");
        for (i, s) in top_sectors.iter().enumerate() {
            msg.push_str(&format!("{}. {} +{:.2}%\n", i + 1, s.name, s.change_pct));
        }
    }

    // Bottom sectors
    if !bottom_sectors.is_empty() {
        msg.push_str("\n📉 <b>板块跌幅榜</b>\n");
        for s in bottom_sectors {
            msg.push_str(&format!("• {} {:.2}%\n", s.name, s.change_pct));
        }
    }

    // Signal scan summary
    if let Some(results) = scan_results {
        if let Some(obj) = results.as_object() {
            let mut non_empty: Vec<(&String, usize)> = obj
                .iter()
                .filter_map(|(k, v)| v.as_array().map(|a| (k, a.len())).filter(|(_, n)| *n > 0))
                .collect();
            non_empty.sort_by(|a, b| b.1.cmp(&a.1));

            if !non_empty.is_empty() {
                msg.push_str("\n📡 <b>信号扫描</b>\n");
                for (signal_id, count) in &non_empty {
                    msg.push_str(&format!("• {}: {} 只\n", signal_id, count));
                }
            }
        }
    }

    msg.push_str(&format!("\n🕐 {}", chrono::Local::now().format("%H:%M")));
    msg
}

pub fn format_scan_alert(signal_name: &str, icon: &str, hits: &[serde_json::Value]) -> String {
    let mut msg = format!("{} <b>{}</b> — {} 只\n\n", icon, signal_name, hits.len());
    for hit in hits.iter().take(20) {
        let code = hit["code"].as_str().unwrap_or("");
        let name = hit["name"].as_str().unwrap_or("");
        msg.push_str(&format!("• {} {}\n", code, name));
    }
    if hits.len() > 20 {
        msg.push_str(&format!("... 共 {} 只\n", hits.len()));
    }
    msg
}

pub fn format_limit_up_report(date: chrono::NaiveDate, stocks: &[crate::data::types::LimitUpStock]) -> String {
    let mut msg = format!("🎯 <b>涨停板 {}</b> — {} 只\n\n", date.format("%m-%d"), stocks.len());

    // Group by streak
    let mut by_streak: std::collections::HashMap<i32, Vec<&crate::data::types::LimitUpStock>> = std::collections::HashMap::new();
    for s in stocks {
        by_streak.entry(s.open_times).or_default().push(s);
    }

    if let Some(daban) = by_streak.get(&0) {
        msg.push_str(&format!("🏆 一字板 ({}):\n", daban.len()));
        for s in daban.iter().take(10) {
            msg.push_str(&format!("  {} {}\n", s.code, s.name));
        }
        msg.push('\n');
    }

    msg
}
