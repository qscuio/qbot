use crate::market_time::beijing_now;
use crate::services::limit_up::LimitUpSummary;
use crate::services::market::MarketOverview;
use crate::services::sector::SectorRank;
use crate::storage::postgres::StrongLimitUpStock;

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
        let arrow = if idx.change_pct >= 0.0 {
            "🔺"
        } else {
            "🔻"
        };
        msg.push_str(&format!(
            "{} {} {}{:.2}%\n",
            arrow,
            idx.name,
            if idx.change_pct >= 0.0 { "+" } else { "" },
            idx.change_pct
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

    msg.push_str(&format!("\n🕐 {}", beijing_now().format("%H:%M")));
    msg
}

fn escape_html(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn normalize_stock_code(raw: &str) -> String {
    raw.split('.')
        .next()
        .unwrap_or(raw)
        .trim()
        .to_ascii_uppercase()
}

fn chart_url_for_stock(code: &str) -> Option<String> {
    let base = std::env::var("WEBHOOK_URL").ok();
    chart_url_for_stock_with_base(code, base.as_deref())
}

fn chart_url_for_stock_with_base(code: &str, miniapp_base: Option<&str>) -> Option<String> {
    let code = normalize_stock_code(code);
    if code.is_empty() {
        return None;
    }

    let base = miniapp_base?.trim().trim_end_matches('/');
    if base.is_empty() {
        return None;
    }

    Some(format!("{base}/miniapp/chart/?code={code}"))
}

pub fn stock_anchor(code: &str, label: &str) -> String {
    let label = escape_html(label);
    match chart_url_for_stock(code) {
        Some(url) => format!("<a href=\"{}\">{}</a>", escape_html(&url), label),
        None => label,
    }
}

pub(crate) fn stock_anchor_with_base(
    code: &str,
    label: &str,
    miniapp_base: Option<&str>,
) -> String {
    let label = escape_html(label);
    match chart_url_for_stock_with_base(code, miniapp_base) {
        Some(url) => format!("<a href=\"{}\">{}</a>", escape_html(&url), label),
        None => label,
    }
}

pub fn format_limit_up_report(
    date: chrono::NaiveDate,
    stocks: &[crate::data::types::LimitUpStock],
) -> String {
    let base = std::env::var("WEBHOOK_URL").ok();
    format_limit_up_report_with_base(date, stocks, base.as_deref())
}

pub(crate) fn format_limit_up_report_with_base(
    date: chrono::NaiveDate,
    stocks: &[crate::data::types::LimitUpStock],
    miniapp_base: Option<&str>,
) -> String {
    let mut rows: Vec<&crate::data::types::LimitUpStock> = stocks.iter().collect();
    rows.sort_by(|a, b| {
        b.fd_amount
            .partial_cmp(&a.fd_amount)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.code.cmp(&b.code))
    });

    let burst = rows.iter().filter(|s| s.open_times > 0).count();
    let sealed = rows.len().saturating_sub(burst);

    let mut msg = format!(
        "🎯 <b>涨停股报告 {}</b>\n━━━━━━━━━━━━━━━━━━━━━\n总数: {} 只 | 封板: {} | 炸板: {}\n\n",
        date.format("%Y-%m-%d"),
        rows.len(),
        sealed,
        burst
    );

    if rows.is_empty() {
        msg.push_str("📭 暂无涨停数据");
        return msg;
    }

    msg.push_str("🏆 <b>封单额靠前</b>\n");
    for (idx, stock) in rows.iter().take(20).enumerate() {
        let amount = stock.fd_amount / 10_000.0;
        let anchor = match miniapp_base {
            Some(base) => stock_anchor_with_base(&stock.code, &stock.name, Some(base)),
            None => stock_anchor(&stock.code, &stock.name),
        };
        msg.push_str(&format!(
            "{}. {} ({}) {:+.1}% 封单{:.0}万 打开{}次\n",
            idx + 1,
            anchor,
            escape_html(&stock.code),
            stock.pct_chg,
            amount,
            stock.open_times
        ));
    }

    msg.push_str(&format!("\n🕐 {}", beijing_now().format("%H:%M")));
    msg
}

pub fn format_strong_stock_report(
    date: chrono::NaiveDate,
    days: i64,
    stocks: &[StrongLimitUpStock],
) -> String {
    let base = std::env::var("WEBHOOK_URL").ok();
    format_strong_stock_report_with_base(date, days, stocks, base.as_deref())
}

pub(crate) fn format_strong_stock_report_with_base(
    date: chrono::NaiveDate,
    days: i64,
    stocks: &[StrongLimitUpStock],
    miniapp_base: Option<&str>,
) -> String {
    let mut msg = format!(
        "💪 <b>近期强势股报告 {}</b>\n━━━━━━━━━━━━━━━━━━━━━\n窗口: 近{}日 | 命中: {}只\n\n",
        date.format("%Y-%m-%d"),
        days,
        stocks.len()
    );

    if stocks.is_empty() {
        msg.push_str("📭 暂无符合条件的强势股");
        return msg;
    }

    for (idx, stock) in stocks.iter().take(20).enumerate() {
        let label = format!("{} ({})", stock.name, stock.code);
        let anchor = match miniapp_base {
            Some(base) => stock_anchor_with_base(&stock.code, &label, Some(base)),
            None => stock_anchor(&stock.code, &label),
        };
        msg.push_str(&format!(
            "{}. {} - {}次涨停\n",
            idx + 1,
            anchor,
            stock.limit_count
        ));
    }

    msg.push_str(&format!("\n🕐 {}", beijing_now().format("%H:%M")));
    msg
}

pub fn format_scan_alert(signal_name: &str, icon: &str, hits: &[serde_json::Value]) -> String {
    let base = std::env::var("WEBHOOK_URL").ok();
    format_scan_alert_with_base(signal_name, icon, hits, base.as_deref())
}

pub(crate) fn format_scan_alert_with_base(
    signal_name: &str,
    icon: &str,
    hits: &[serde_json::Value],
    miniapp_base: Option<&str>,
) -> String {
    let mut msg = format!("{} <b>{}</b> — {} 只\n\n", icon, signal_name, hits.len());
    for hit in hits.iter().take(20) {
        let code = hit["code"].as_str().unwrap_or("");
        let name = hit["name"].as_str().unwrap_or("");
        let label = format!("{code} {name}").trim().to_string();
        let anchor = match miniapp_base {
            Some(base) => stock_anchor_with_base(code, &label, Some(base)),
            None => stock_anchor(code, &label),
        };
        msg.push_str(&format!("• {}\n", anchor));
    }
    if hits.len() > 20 {
        msg.push_str(&format!("... 共 {} 只\n", hits.len()));
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::types::LimitUpStock;
    use crate::storage::postgres::StrongLimitUpStock;
    use chrono::NaiveDate;

    fn d(raw: &str) -> NaiveDate {
        NaiveDate::parse_from_str(raw, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn format_limit_up_report_includes_summary_and_names() {
        let report = format_limit_up_report_with_base(
            d("2026-03-09"),
            &[
                LimitUpStock {
                    code: "600001.SH".to_string(),
                    name: "Alpha".to_string(),
                    trade_date: d("2026-03-09"),
                    close: 10.2,
                    pct_chg: 10.0,
                    fd_amount: 1_500_000.0,
                    first_time: Some("09:35".to_string()),
                    last_time: Some("14:51".to_string()),
                    open_times: 0,
                    strth: 82.0,
                    limit: "U".to_string(),
                },
                LimitUpStock {
                    code: "600002.SH".to_string(),
                    name: "Beta".to_string(),
                    trade_date: d("2026-03-09"),
                    close: 8.7,
                    pct_chg: 9.9,
                    fd_amount: 900_000.0,
                    first_time: Some("10:11".to_string()),
                    last_time: Some("14:40".to_string()),
                    open_times: 2,
                    strth: 61.0,
                    limit: "U".to_string(),
                },
            ],
            Some("https://bot.example/"),
        );

        assert!(report.contains("2026-03-09"));
        assert!(report.contains("2 只"));
        assert!(
            report.contains("<a href=\"https://bot.example/miniapp/chart/?code=600001\">Alpha</a>")
        );
        assert!(
            report.contains("<a href=\"https://bot.example/miniapp/chart/?code=600002\">Beta</a>")
        );
    }

    #[test]
    fn format_strong_stock_report_includes_window_and_counts() {
        let report = format_strong_stock_report_with_base(
            d("2026-03-09"),
            7,
            &[
                StrongLimitUpStock {
                    code: "600010.SH".to_string(),
                    name: "Solo".to_string(),
                    limit_count: 4,
                    latest_trade_date: d("2026-03-09"),
                },
                StrongLimitUpStock {
                    code: "600011.SH".to_string(),
                    name: "Repeat".to_string(),
                    limit_count: 3,
                    latest_trade_date: d("2026-03-08"),
                },
            ],
            Some("https://bot.example"),
        );

        assert!(report.contains("强势股"));
        assert!(report.contains("7日"));
        assert!(report.contains("4次涨停"));
        assert!(report.contains(
            "<a href=\"https://bot.example/miniapp/chart/?code=600010\">Solo (600010.SH)</a>"
        ));
        assert!(report.contains(
            "<a href=\"https://bot.example/miniapp/chart/?code=600011\">Repeat (600011.SH)</a>"
        ));
    }

    #[test]
    fn format_scan_alert_links_hits_to_internal_chart() {
        let report = format_scan_alert_with_base(
            "强势突破",
            "⚡",
            &[serde_json::json!({
                "code": "300001.SZ",
                "name": "Gamma",
            })],
            Some("https://bot.example/root/"),
        );

        assert!(report.contains("强势突破"));
        assert!(report.contains(
            "<a href=\"https://bot.example/root/miniapp/chart/?code=300001\">300001.SZ Gamma</a>"
        ));
    }
}
