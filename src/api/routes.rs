use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::Datelike;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::data::types::Candle;
use crate::services::ai_analysis::AiAnalysisService;
use crate::services::chip_dist::ChipDistService;
use crate::services::daban::DabanService;
use crate::services::daban_sim::DabanSimService;
use crate::services::portfolio::PortfolioService;
use crate::services::trading_sim::TradingSimService;
use crate::services::watchlist::WatchlistService;
use crate::signals::registry::SignalRegistry;
use crate::state::AppState;
use crate::storage::postgres;

type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;

fn api_error(msg: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": msg})),
    )
}

fn check_auth(headers: &HeaderMap, api_key: Option<&str>) -> bool {
    match api_key {
        None => true,
        Some(key) => headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == format!("Bearer {}", key))
            .unwrap_or(false),
    }
}

#[derive(Debug, Deserialize)]
struct ChartQuery {
    days: Option<usize>,
    period: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChartSearchQuery {
    q: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct WatchlistMutation {
    user_id: i64,
    code: String,
}

#[derive(Debug, Deserialize)]
struct WatchlistStatusQuery {
    user_id: i64,
    code: String,
}

#[derive(Debug, Deserialize)]
struct WatchlistListQuery {
    user_id: i64,
}

#[derive(Debug, Deserialize)]
struct PortfolioListQuery {
    user_id: i64,
}

#[derive(Debug, Deserialize)]
struct PortfolioAddRequest {
    user_id: i64,
    code: String,
    cost_price: f64,
    shares: i32,
}

#[derive(Debug, Deserialize)]
struct PortfolioRemoveRequest {
    user_id: i64,
    code: String,
}

#[derive(Debug, Deserialize)]
struct SimTypeQuery {
    sim_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SimBuyRequest {
    sim_type: Option<String>,
    code: String,
    name: Option<String>,
    price: f64,
    shares: i32,
}

#[derive(Debug, Deserialize)]
struct SimSellRequest {
    sim_type: Option<String>,
    position_id: i64,
    price: f64,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DabanReportQuery {
    date: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct DabanSimBuyRequest {
    code: String,
    name: Option<String>,
    price: f64,
    shares: i32,
    score: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DabanSimSellRequest {
    position_id: i64,
    price: f64,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DateQuery {
    date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramUpdate {
    message: Option<TelegramMessage>,
    edited_message: Option<TelegramMessage>,
}

#[derive(Debug, Deserialize)]
struct TelegramMessage {
    from: Option<TelegramUser>,
    chat: TelegramChat,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramUser {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct TelegramChat {
    id: i64,
}

fn normalize_period(period: Option<&str>) -> &'static str {
    match period.map(|v| v.to_ascii_lowercase()) {
        Some(p) if p == "weekly" => "weekly",
        Some(p) if p == "monthly" => "monthly",
        _ => "daily",
    }
}

fn normalize_sim_type(sim_type: Option<&str>) -> &'static str {
    match sim_type.map(|s| s.to_ascii_lowercase()) {
        Some(s) if s == "general" => "general",
        _ => "general",
    }
}

fn resample_bars(bars: &[Candle], period: &str) -> Vec<Candle> {
    if period == "daily" || bars.is_empty() {
        return bars.to_vec();
    }

    let mut out = Vec::new();

    let mut active = false;
    let mut key: (i32, u32) = (0, 0);
    let mut open = 0.0;
    let mut high = 0.0;
    let mut low = 0.0;
    let mut close = 0.0;
    let mut volume = 0i64;
    let mut amount = 0.0;
    let mut trade_date = bars[0].trade_date;

    let bucket_key = |d: chrono::NaiveDate| -> (i32, u32) {
        if period == "weekly" {
            let iso = d.iso_week();
            (iso.year(), iso.week())
        } else {
            (d.year(), d.month())
        }
    };

    for b in bars {
        let b_key = bucket_key(b.trade_date);
        if !active {
            active = true;
            key = b_key;
            open = b.open;
            high = b.high;
            low = b.low;
            close = b.close;
            volume = b.volume;
            amount = b.amount;
            trade_date = b.trade_date;
            continue;
        }

        if b_key != key {
            out.push(Candle {
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                amount,
                turnover: None,
                pe: None,
                pb: None,
            });

            key = b_key;
            open = b.open;
            high = b.high;
            low = b.low;
            close = b.close;
            volume = b.volume;
            amount = b.amount;
            trade_date = b.trade_date;
            continue;
        }

        if b.high > high {
            high = b.high;
        }
        if b.low < low {
            low = b.low;
        }
        close = b.close;
        volume += b.volume;
        amount += b.amount;
        trade_date = b.trade_date;
    }

    if active {
        out.push(Candle {
            trade_date,
            open,
            high,
            low,
            close,
            volume,
            amount,
            turnover: None,
            pe: None,
            pb: None,
        });
    }

    out
}

fn bars_to_json(bars: &[Candle]) -> Vec<Value> {
    bars.iter()
        .map(|b| {
            json!({
                "time": b.trade_date.to_string(),
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
                "amount": b.amount,
            })
        })
        .collect()
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/telegram/webhook", post(telegram_webhook))
        .route("/api/signals", get(list_signals))
        .route("/api/scan/latest", get(get_scan_latest))
        .route("/api/scan/trigger", post(trigger_scan))
        .route("/api/report/daily", get(get_daily_report))
        .route("/api/market/overview", get(market_overview))
        .route("/api/chart/data/:code", get(chart_data))
        .route("/api/chart/chips/:code", get(chart_chips))
        .route("/api/chart/search", get(chart_search))
        .route("/api/chart/watchlist/add", post(chart_watchlist_add))
        .route("/api/chart/watchlist/remove", post(chart_watchlist_remove))
        .route("/api/chart/watchlist/status", get(chart_watchlist_status))
        .route("/api/chart/watchlist/list", get(chart_watchlist_list))
        .route("/api/portfolio/list", get(portfolio_list))
        .route("/api/portfolio/add", post(portfolio_add))
        .route("/api/portfolio/remove", post(portfolio_remove))
        .route("/api/sim/balance", get(sim_balance))
        .route("/api/sim/positions", get(sim_positions))
        .route("/api/sim/buy", post(sim_buy))
        .route("/api/sim/sell", post(sim_sell))
        .route("/api/sim/stats", get(sim_stats))
        .route("/api/daban/report", get(daban_report))
        .route("/api/daban/top", get(daban_top))
        .route("/api/daban/sim/balance", get(daban_sim_balance))
        .route("/api/daban/sim/positions", get(daban_sim_positions))
        .route("/api/daban/sim/buy", post(daban_sim_buy))
        .route("/api/daban/sim/sell", post(daban_sim_sell))
        .route("/api/daban/sim/stats", get(daban_sim_stats))
        .route("/api/jobs/fetch", post(trigger_fetch))
        .route("/api/jobs/scan", post(trigger_scan_job))
        .route("/api/jobs/report/daily", post(trigger_daily_report))
        .route("/api/jobs/report/weekly", post(trigger_weekly_report))
        .with_state(state)
}

fn parse_optional_date(
    raw: Option<&str>,
) -> std::result::Result<Option<chrono::NaiveDate>, &'static str> {
    match raw {
        Some(s) if !s.trim().is_empty() => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map(Some)
            .map_err(|_| "date must be YYYY-MM-DD"),
        _ => Ok(None),
    }
}

fn check_telegram_webhook_secret(headers: &HeaderMap, secret: Option<&str>) -> bool {
    match secret.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        None => true,
        Some(expected) => headers
            .get("X-Telegram-Bot-Api-Secret-Token")
            .and_then(|v| v.to_str().ok())
            .map(|actual| actual == expected)
            .unwrap_or(false),
    }
}

fn parse_telegram_command(text: &str) -> Option<(String, String)> {
    let trimmed = text.trim();
    if !trimmed.starts_with('/') {
        return None;
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let head = parts.next()?.trim_start_matches('/');
    if head.is_empty() {
        return None;
    }

    let cmd = head
        .split('@')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if cmd.is_empty() {
        return None;
    }

    let args = parts.next().unwrap_or_default().trim().to_string();
    Some((cmd, args))
}

fn escape_html(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn tg_send(state: &Arc<AppState>, chat_id: i64, text: &str) -> crate::error::Result<()> {
    state.pusher.push(&chat_id.to_string(), text).await
}

fn telegram_help_text() -> String {
    [
        "🤖 <b>Qbot Commands</b>",
        "",
        "<b>Watchlist</b>",
        "/watch <code>  添加自选",
        "/watch          查看自选",
        "/unwatch <code> 删除自选",
        "/mywatch        查看自选",
        "/export         导出自选",
        "",
        "<b>Portfolio</b>",
        "/port                   查看持仓",
        "/port add <code> <cost> <shares>",
        "/port del <code>",
        "",
        "<b>Signals / Daban</b>",
        "/scan           扫描信号",
        "/daban          打板评分",
        "/daban portfolio 打板持仓",
        "/daban stats    打板统计",
        "",
        "<b>Sectors / AI</b>",
        "/industry /concept",
        "/hot7 /hot14 /hot30",
        "/sector_sync",
        "/ai_analysis",
        "",
        "<b>Charts</b>",
        "/history <code>",
        "/chart <code>",
        "/dbcheck /dbsync",
    ]
    .join("\n")
}

async fn format_watchlist(state: Arc<AppState>, user_id: i64) -> crate::error::Result<String> {
    let svc = WatchlistService::new(state);
    let items = svc.list_stocks(user_id).await?;

    if items.is_empty() {
        return Ok(
            "⭐ <b>自选列表</b>\n\n📭 暂无自选股票\n使用 <code>/watch 600519</code> 添加"
                .to_string(),
        );
    }

    let mut lines = vec![
        format!("⭐ <b>自选列表</b> ({})", items.len()),
        "━━━━━━━━━━━━━━━━━━━━━".to_string(),
    ];
    for (idx, item) in items.iter().take(80).enumerate() {
        lines.push(format!(
            "{}. <b>{}</b> ({})",
            idx + 1,
            escape_html(&item.name),
            escape_html(&item.code)
        ));
    }
    Ok(lines.join("\n"))
}

async fn format_portfolio(state: Arc<AppState>, user_id: i64) -> crate::error::Result<String> {
    let svc = PortfolioService::new(state);
    let items = svc.list_positions(user_id).await?;
    if items.is_empty() {
        return Ok(
            "💼 <b>实盘持仓</b>\n\n📭 当前无持仓\n使用 <code>/port add 600519 1500 100</code> 添加"
                .to_string(),
        );
    }

    let mut total_market_value = 0.0f64;
    let mut total_cost = 0.0f64;
    let mut total_unrealized = 0.0f64;
    let mut lines = vec![
        "💼 <b>实盘持仓</b>".to_string(),
        "━━━━━━━━━━━━━━━━━━━━━".to_string(),
    ];

    for p in items.iter().take(30) {
        let last_price = p.last_price.unwrap_or(0.0);
        let market_value = p.market_value.unwrap_or(0.0);
        let cost_value = p.cost_price * p.shares as f64;
        let unrealized = market_value - cost_value;
        let pnl_pct = p.pnl_pct.unwrap_or(0.0);
        let emoji = if pnl_pct > 0.0 {
            "🟢"
        } else if pnl_pct < 0.0 {
            "🔴"
        } else {
            "⚪"
        };

        total_market_value += market_value;
        total_cost += cost_value;
        total_unrealized += unrealized;

        lines.push(format!(
            "{} <b>{}</b> ({})\n   现价: {:.2}  持仓: {}股 @ {:.2}\n   盈亏: {:+.2} ({:+.2}%)",
            emoji,
            escape_html(&p.name),
            escape_html(&p.code),
            last_price,
            p.shares,
            p.cost_price,
            unrealized,
            pnl_pct
        ));
    }

    let total_return = if total_cost > 0.0 {
        total_unrealized / total_cost * 100.0
    } else {
        0.0
    };
    lines.push("━━━━━━━━━━━━━━━━━━━━━".to_string());
    lines.push(format!(
        "💰 总市值: {:.2}\n📈 总盈亏: {:+.2} ({:+.2}%)",
        total_market_value, total_unrealized, total_return
    ));
    Ok(lines.join("\n"))
}

async fn format_daban_portfolio(state: Arc<AppState>) -> crate::error::Result<String> {
    let svc = DabanSimService::new(state.clone());
    let balance = svc.get_balance().await?;
    let positions = svc.list_open_positions().await?;

    if positions.is_empty() {
        return Ok(format!(
            "📦 <b>打板持仓</b>\n\n📭 当前无持仓\n💵 可用资金: {:.2}",
            balance
        ));
    }

    let mut lines = vec![
        "📦 <b>打板持仓</b>".to_string(),
        "━━━━━━━━━━━━━━━━━━━━━".to_string(),
    ];
    for p in positions.iter().take(20) {
        lines.push(format!(
            "#{} <b>{}</b> ({})\n   成本: {:.2}  股数: {}\n   浮盈亏: {:+.2}%",
            p.id,
            escape_html(p.name.as_deref().unwrap_or(&p.code)),
            escape_html(&p.code),
            p.entry_price,
            p.shares,
            p.unrealized_pnl_pct.unwrap_or(0.0)
        ));
    }
    lines.push(format!("💵 可用资金: {:.2}", balance));
    Ok(lines.join("\n"))
}

async fn format_daban_stats(state: Arc<AppState>) -> crate::error::Result<String> {
    let stats = DabanSimService::new(state).stats().await?;
    Ok(format!(
        "📊 <b>打板统计</b>\n━━━━━━━━━━━━━━━━━━━━━\n💵 余额: {:.2}\n📦 持仓数: {}\n🧾 已平仓: {}\n📈 平均收益: {:+.2}%\n💰 已实现盈亏: {:+.2}",
        stats.balance,
        stats.open_positions,
        stats.closed_trades,
        stats.avg_closed_pnl_pct,
        stats.realized_pnl
    ))
}

async fn send_sector_snapshot(
    state: Arc<AppState>,
    chat_id: i64,
    sector_type: &str,
) -> crate::error::Result<()> {
    let latest: Option<(chrono::NaiveDate,)> =
        sqlx::query_as("SELECT trade_date FROM sector_daily ORDER BY trade_date DESC LIMIT 1")
            .fetch_optional(&state.db)
            .await?;
    let Some((trade_date,)) = latest else {
        tg_send(
            &state,
            chat_id,
            "📊 板块数据为空，请先执行 <code>/sector_sync</code>",
        )
        .await?;
        return Ok(());
    };

    let top_rows: Vec<(String, Option<f64>)> = sqlx::query_as(
        r#"SELECT name, change_pct::float8
           FROM sector_daily
           WHERE trade_date = $1 AND sector_type = $2
           ORDER BY change_pct DESC NULLS LAST
           LIMIT 10"#,
    )
    .bind(trade_date)
    .bind(sector_type)
    .fetch_all(&state.db)
    .await?;

    let bottom_rows: Vec<(String, Option<f64>)> = sqlx::query_as(
        r#"SELECT name, change_pct::float8
           FROM sector_daily
           WHERE trade_date = $1 AND sector_type = $2
           ORDER BY change_pct ASC NULLS LAST
           LIMIT 3"#,
    )
    .bind(trade_date)
    .bind(sector_type)
    .fetch_all(&state.db)
    .await?;

    let (title, icon) = if sector_type == "industry" {
        ("行业板块", "🏭")
    } else {
        ("概念板块", "💡")
    };

    let mut text = format!(
        "{} <b>{}</b> {}\n━━━━━━━━━━━━━━━━━━━━━\n",
        icon, title, trade_date
    );
    if top_rows.is_empty() {
        text.push_str("暂无数据");
    } else {
        text.push_str("\n📈 <b>领涨</b>\n");
        for (idx, (name, pct)) in top_rows.iter().enumerate() {
            text.push_str(&format!(
                "{}. {} {:+.2}%\n",
                idx + 1,
                escape_html(name),
                pct.unwrap_or(0.0)
            ));
        }
        text.push_str("\n📉 <b>领跌</b>\n");
        for (name, pct) in &bottom_rows {
            text.push_str(&format!(
                "• {} {:+.2}%\n",
                escape_html(name),
                pct.unwrap_or(0.0)
            ));
        }
    }
    tg_send(&state, chat_id, &text).await
}

async fn send_hot_sectors(
    state: Arc<AppState>,
    chat_id: i64,
    days: i64,
) -> crate::error::Result<()> {
    let rows: Vec<(String, String, Option<f64>, Option<i64>, Option<i64>)> = sqlx::query_as(
        r#"WITH latest_dates AS (
             SELECT DISTINCT trade_date
             FROM sector_daily
             ORDER BY trade_date DESC
             LIMIT $1
           )
           SELECT name,
                  COALESCE(sector_type, 'unknown') AS sector_type,
                  SUM(change_pct)::float8 AS total_change,
                  SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END)::bigint AS up_days,
                  COUNT(*)::bigint AS total_days
           FROM sector_daily
           WHERE trade_date IN (SELECT trade_date FROM latest_dates)
           GROUP BY code, name, sector_type
           ORDER BY total_change DESC NULLS LAST
           LIMIT 15"#,
    )
    .bind(days)
    .fetch_all(&state.db)
    .await?;

    if rows.is_empty() {
        tg_send(
            &state,
            chat_id,
            &format!(
                "🔥 <b>{}日强势板块</b>\n\n暂无数据，请先执行 <code>/sector_sync</code>",
                days
            ),
        )
        .await?;
        return Ok(());
    }

    let mut text = format!("🔥 <b>{}日强势板块</b>\n━━━━━━━━━━━━━━━━━━━━━\n", days);
    for (idx, (name, sector_type, total_change, up_days, total_days)) in rows.iter().enumerate() {
        let icon = if sector_type == "industry" {
            "🏭"
        } else if sector_type == "concept" {
            "💡"
        } else {
            "📊"
        };
        text.push_str(&format!(
            "{}. {} {} {:+.2}% ({}/{})\n",
            idx + 1,
            icon,
            escape_html(name),
            total_change.unwrap_or(0.0),
            up_days.unwrap_or(0),
            total_days.unwrap_or(0)
        ));
    }
    tg_send(&state, chat_id, &text).await
}

async fn format_scan_summary(state: Arc<AppState>) -> crate::error::Result<String> {
    let results = crate::services::scanner::ScannerService::new(state.clone())
        .run_full_scan()
        .await?;

    let total_hits: usize = results.values().map(|v| v.len()).sum();
    if total_hits == 0 {
        return Ok("🔍 扫描完成\n\n📭 暂无信号".to_string());
    }

    let mut meta: HashMap<String, (String, String)> = HashMap::new();
    for s in SignalRegistry::get_enabled() {
        meta.insert(
            s.signal_id().to_string(),
            (s.display_name().to_string(), s.icon().to_string()),
        );
    }
    meta.insert(
        "multi_signal".to_string(),
        ("多信号共振".to_string(), "⭐".to_string()),
    );

    let mut rows: Vec<(String, usize)> = results
        .iter()
        .filter_map(|(k, v)| {
            if v.is_empty() {
                None
            } else {
                Some((k.clone(), v.len()))
            }
        })
        .collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut text = format!(
        "🔍 <b>扫描完成</b>\n━━━━━━━━━━━━━━━━━━━━━\n共 <b>{}</b> 个信号\n\n",
        total_hits
    );
    for (signal_id, count) in rows.iter().take(20) {
        let (name, icon) = meta
            .get(signal_id)
            .cloned()
            .unwrap_or_else(|| (signal_id.clone(), "•".to_string()));
        text.push_str(&format!(
            "{} {}: <b>{}</b>\n",
            icon,
            escape_html(&name),
            count
        ));
    }
    Ok(text)
}

async fn handle_telegram_command(
    state: Arc<AppState>,
    chat_id: i64,
    user_id: i64,
    command: String,
    args: String,
) -> crate::error::Result<()> {
    match command.as_str() {
        "start" | "help" => {
            tg_send(&state, chat_id, &telegram_help_text()).await?;
        }
        "watch" => {
            let code = args.split_whitespace().next().unwrap_or_default();
            if code.is_empty() {
                let text = format_watchlist(state.clone(), user_id).await?;
                tg_send(&state, chat_id, &text).await?;
            } else {
                let svc = WatchlistService::new(state.clone());
                let resolved = svc.add_stock(user_id, code).await?;
                let stock_name = postgres::get_stock_name(&state.db, &resolved)
                    .await?
                    .unwrap_or_else(|| resolved.clone());
                tg_send(
                    &state,
                    chat_id,
                    &format!(
                        "✅ 已添加 <b>{}</b> ({})",
                        escape_html(&stock_name),
                        escape_html(&resolved)
                    ),
                )
                .await?;
            }
        }
        "unwatch" => {
            let code = args.split_whitespace().next().unwrap_or_default();
            if code.is_empty() {
                tg_send(&state, chat_id, "用法: <code>/unwatch 600519</code>").await?;
            } else {
                let svc = WatchlistService::new(state.clone());
                let removed = svc.remove_stock(user_id, code).await?;
                if removed {
                    tg_send(
                        &state,
                        chat_id,
                        &format!("✅ 已从自选删除 {}", escape_html(code)),
                    )
                    .await?;
                } else {
                    tg_send(
                        &state,
                        chat_id,
                        &format!("❌ 删除失败，{} 可能不在自选列表中", escape_html(code)),
                    )
                    .await?;
                }
            }
        }
        "mywatch" => {
            let text = format_watchlist(state.clone(), user_id).await?;
            tg_send(&state, chat_id, &text).await?;
        }
        "export" => {
            let svc = WatchlistService::new(state.clone());
            let items = svc.list_stocks(user_id).await?;
            if items.is_empty() {
                tg_send(&state, chat_id, "📭 自选为空，暂无可导出内容").await?;
            } else {
                let mut csv = String::from("code,name\n");
                for item in items {
                    csv.push_str(&format!("{},{}\n", item.code, item.name.replace(',', " ")));
                }
                tg_send(
                    &state,
                    chat_id,
                    &format!(
                        "📤 <b>自选导出</b>\n<pre>{}</pre>",
                        escape_html(csv.trim_end())
                    ),
                )
                .await?;
            }
        }
        "port" => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            if parts.is_empty() {
                tg_send(
                    &state,
                    chat_id,
                    &format_portfolio(state.clone(), user_id).await?,
                )
                .await?;
            } else {
                match parts[0].to_ascii_lowercase().as_str() {
                    "add" => {
                        if parts.len() < 4 {
                            tg_send(
                                &state,
                                chat_id,
                                "用法: <code>/port add &lt;代码&gt; &lt;成本价&gt; &lt;股数&gt;</code>",
                            )
                            .await?;
                        } else {
                            let cost_price = parts[2].parse::<f64>().unwrap_or(0.0);
                            let shares = parts[3].parse::<i32>().unwrap_or(0);
                            if cost_price <= 0.0 || shares <= 0 {
                                tg_send(&state, chat_id, "❌ 价格或股数格式错误").await?;
                            } else {
                                let svc = PortfolioService::new(state.clone());
                                let code = svc
                                    .add_position(user_id, parts[1], cost_price, shares)
                                    .await?;
                                tg_send(
                                    &state,
                                    chat_id,
                                    &format!(
                                        "✅ 已添加 {}: {}股 @ {:.2}",
                                        escape_html(&code),
                                        shares,
                                        cost_price
                                    ),
                                )
                                .await?;
                                tg_send(
                                    &state,
                                    chat_id,
                                    &format_portfolio(state.clone(), user_id).await?,
                                )
                                .await?;
                            }
                        }
                    }
                    "del" | "remove" => {
                        if parts.len() < 2 {
                            tg_send(&state, chat_id, "用法: <code>/port del &lt;代码&gt;</code>")
                                .await?;
                        } else {
                            let svc = PortfolioService::new(state.clone());
                            let removed = svc.remove_position(user_id, parts[1]).await?;
                            if removed {
                                tg_send(
                                    &state,
                                    chat_id,
                                    &format!("✅ 已删除 {}", escape_html(parts[1])),
                                )
                                .await?;
                            } else {
                                tg_send(&state, chat_id, "❌ 删除失败").await?;
                            }
                        }
                    }
                    _ => {
                        tg_send(
                            &state,
                            chat_id,
                            "💼 <b>持仓管理</b>\n\n• 查看: <code>/port</code>\n• 添加: <code>/port add &lt;代码&gt; &lt;成本&gt; &lt;股数&gt;</code>\n• 删除: <code>/port del &lt;代码&gt;</code>",
                        )
                        .await?;
                    }
                }
            }
        }
        "daban" => {
            let sub = args.trim().to_ascii_lowercase();
            match sub.as_str() {
                "portfolio" => {
                    tg_send(
                        &state,
                        chat_id,
                        &format_daban_portfolio(state.clone()).await?,
                    )
                    .await?
                }
                "stats" => {
                    tg_send(&state, chat_id, &format_daban_stats(state.clone()).await?).await?
                }
                "scan" => {
                    tg_send(
                        &state,
                        chat_id,
                        "ℹ️ 当前 Rust 版暂未实现自动打板买入，先返回最新打板评分：",
                    )
                    .await?;
                    let svc = DabanService::new(state.clone());
                    let report = svc.build_report(None, 20).await?;
                    tg_send(&state, chat_id, &DabanService::format_report_text(&report)).await?;
                }
                _ => {
                    let svc = DabanService::new(state.clone());
                    let report = svc.build_report(None, 20).await?;
                    tg_send(&state, chat_id, &DabanService::format_report_text(&report)).await?;
                }
            }
        }
        "scan" => {
            tg_send(&state, chat_id, "⏳ 正在扫描信号，请稍候...").await?;
            let summary = format_scan_summary(state.clone()).await?;
            tg_send(&state, chat_id, &summary).await?;
        }
        "industry" => send_sector_snapshot(state.clone(), chat_id, "industry").await?,
        "concept" => send_sector_snapshot(state.clone(), chat_id, "concept").await?,
        "hot7" => send_hot_sectors(state.clone(), chat_id, 7).await?,
        "hot14" => send_hot_sectors(state.clone(), chat_id, 14).await?,
        "hot30" => send_hot_sectors(state.clone(), chat_id, 30).await?,
        "sector_sync" => {
            tg_send(&state, chat_id, "⏳ 正在同步板块数据...").await?;
            let target_date = crate::market_time::beijing_today();
            let svc =
                crate::services::sector::SectorService::new(state.clone(), state.provider.clone());
            svc.fetch_and_save(target_date).await?;
            let count: Option<(i64,)> =
                sqlx::query_as("SELECT COUNT(*) FROM sector_daily WHERE trade_date = $1")
                    .bind(target_date)
                    .fetch_optional(&state.db)
                    .await?;
            tg_send(
                &state,
                chat_id,
                &format!(
                    "✅ 板块同步完成\n日期: {}\n记录数: {}",
                    target_date,
                    count.map(|r| r.0).unwrap_or(0)
                ),
            )
            .await?;
        }
        "ai_analysis" => {
            tg_send(
                &state,
                chat_id,
                "🤖 正在生成今日 A 股复盘报告，请稍候（约 10-30 秒）...",
            )
            .await?;
            let report = AiAnalysisService::new(state.clone())
                .generate_daily_report(None)
                .await?;
            tg_send(&state, chat_id, &report).await?;
        }
        "history" => {
            let raw = args.trim();
            if raw.is_empty() {
                tg_send(&state, chat_id, "用法: <code>/history 600519</code>").await?;
            } else {
                let code = postgres::resolve_stock_code(&state.db, raw)
                    .await?
                    .unwrap_or_else(|| raw.to_uppercase());
                let bars = postgres::get_stock_history(&state.db, &code, 10).await?;
                if bars.is_empty() {
                    tg_send(
                        &state,
                        chat_id,
                        &format!("❌ {} 暂无历史数据", escape_html(&code)),
                    )
                    .await?;
                } else {
                    let mut text = format!(
                        "📜 <b>HISTORY: {}</b>\n━━━━━━━━━━━━━━━━━━━━━\n<pre>Date       Close   Chg%   Vol</pre>\n",
                        escape_html(&code)
                    );
                    let mut prev: Option<f64> = None;
                    for b in bars {
                        let pct = if let Some(p) = prev {
                            if p > 0.0 {
                                (b.close - p) / p * 100.0
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        };
                        prev = Some(b.close);
                        text.push_str(&format!(
                            "<pre>{:<10} {:>6.2} {:>6.2}% {:>6}</pre>\n",
                            b.trade_date.format("%m-%d"),
                            b.close,
                            pct,
                            b.volume / 10000
                        ));
                    }
                    tg_send(&state, chat_id, &text).await?;
                }
            }
        }
        "chart" => {
            let raw = args.trim();
            if raw.is_empty() {
                tg_send(&state, chat_id, "用法: <code>/chart 600519</code>").await?;
            } else {
                let code = postgres::resolve_stock_code(&state.db, raw)
                    .await?
                    .unwrap_or_else(|| raw.to_uppercase());
                if let Some(base) = state.config.webhook_url.as_deref() {
                    let api_base = base.trim_end_matches('/');
                    let msg = format!(
                        "📈 <b>Chart: {}</b>\n\nK线: {}/api/chart/data/{}\n筹码: {}/api/chart/chips/{}",
                        escape_html(&code),
                        api_base,
                        escape_html(&code),
                        api_base,
                        escape_html(&code)
                    );
                    tg_send(&state, chat_id, &msg).await?;
                } else {
                    tg_send(
                        &state,
                        chat_id,
                        &format!(
                            "📈 {} 图表接口:\n<code>/api/chart/data/{}</code>\n<code>/api/chart/chips/{}</code>",
                            escape_html(&code),
                            escape_html(&code),
                            escape_html(&code)
                        ),
                    )
                    .await?;
                }
            }
        }
        "dbcheck" => {
            let row: Option<(Option<i64>, Option<i64>, Option<chrono::NaiveDate>, Option<chrono::NaiveDate>)> =
                sqlx::query_as(
                    r#"SELECT
                         COUNT(*)::bigint,
                         COUNT(DISTINCT code)::bigint,
                         MIN(trade_date),
                         MAX(trade_date)
                       FROM stock_daily_bars"#,
                )
                .fetch_optional(&state.db)
                .await?;
            let (total_rows, stock_count, min_date, max_date) =
                row.unwrap_or((Some(0), Some(0), None, None));

            let today = crate::market_time::beijing_today();
            let days_old = max_date.map(|d| (today - d).num_days()).unwrap_or(999);
            let freshness = if days_old <= 1 {
                "✅ 最新".to_string()
            } else {
                format!("⚠️ {}天前", days_old)
            };

            let msg = format!(
                "📊 <b>stock_history 数据库状态</b>\n━━━━━━━━━━━━━━━━━━━━━\n📁 总记录数: <b>{}</b>\n📈 股票数量: <b>{}</b>\n📅 数据范围: {} ~ {}\n🕐 数据新鲜度: {}",
                total_rows.unwrap_or(0),
                stock_count.unwrap_or(0),
                min_date.map(|d| d.to_string()).unwrap_or_else(|| "N/A".to_string()),
                max_date.map(|d| d.to_string()).unwrap_or_else(|| "N/A".to_string()),
                freshness
            );
            tg_send(&state, chat_id, &msg).await?;
        }
        "dbsync" => {
            tg_send(&state, chat_id, "⏳ 正在同步今日行情数据...").await?;
            let svc =
                crate::services::stock_history::StockHistoryService::new(state.clone(), state.provider.clone());
            svc.update_today().await?;
            tg_send(&state, chat_id, "✅ 数据同步完成").await?;
        }
        _ => {}
    }
    Ok(())
}

async fn telegram_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(update): Json<TelegramUpdate>,
) -> ApiResult {
    if !check_telegram_webhook_secret(&headers, state.config.telegram_webhook_secret.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid telegram webhook secret"})),
        ));
    }

    let msg = update.message.or(update.edited_message);
    let Some(message) = msg else {
        return Ok(Json(json!({"ok": true, "ignored": "no_message"})));
    };

    let Some(text) = message.text else {
        return Ok(Json(json!({"ok": true, "ignored": "no_text"})));
    };

    let Some((command, args)) = parse_telegram_command(&text) else {
        return Ok(Json(json!({"ok": true, "ignored": "non_command"})));
    };

    let chat_id = message.chat.id;
    let user_id = message.from.map(|u| u.id).unwrap_or(chat_id);
    let state_clone = state.clone();

    tokio::spawn(async move {
        if let Err(e) =
            handle_telegram_command(state_clone.clone(), chat_id, user_id, command, args).await
        {
            tracing::warn!("telegram command handling failed: {}", e);
            let _ = tg_send(&state_clone, chat_id, "❌ 命令执行失败，请稍后重试。").await;
        }
    });

    Ok(Json(json!({"ok": true})))
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "service": "qbot"}))
}

async fn list_signals() -> Json<Value> {
    let signals: Vec<Value> = SignalRegistry::get_enabled()
        .iter()
        .map(|s| {
            json!({
                "id": s.signal_id(),
                "name": s.display_name(),
                "icon": s.icon(),
                "group": s.group(),
            })
        })
        .collect();
    let count = signals.len();
    Json(json!({"signals": signals, "count": count}))
}

async fn get_scan_latest(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let mut cache = crate::storage::redis_cache::RedisCache::new(state.redis.clone());
    match cache.get_scan_results().await {
        Ok(Some(results)) => Ok(Json(results)),
        Ok(None) => Ok(Json(json!({"status": "no_scan_results"}))),
        Err(e) => Err(api_error(&e.to_string())),
    }
}

async fn trigger_scan(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let state_clone = state.clone();
    tokio::spawn(async move {
        crate::scheduler::run_scan_job(state_clone).await;
    });

    Ok(Json(json!({"status": "scan_started"})))
}

async fn get_daily_report(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    match postgres::get_latest_report(&state.db, "daily").await {
        Ok(Some(content)) => Ok(Json(json!({"content": content}))),
        Ok(None) => Ok(Json(json!({"status": "no_report_yet"}))),
        Err(e) => Err(api_error(&e.to_string())),
    }
}

async fn market_overview(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<DateQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let date = parse_optional_date(query.date.as_deref())
        .map_err(|msg| (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))))?;

    let svc = AiAnalysisService::new(state);
    let overview = svc
        .market_overview(date)
        .await
        .map_err(|e| api_error(&e.to_string()))?;

    Ok(Json(json!(overview)))
}

async fn chart_data(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(raw_code): Path<String>,
    Query(query): Query<ChartQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let days = query.days.unwrap_or(60).clamp(1, 1000);
    let period = normalize_period(query.period.as_deref());
    let code = postgres::resolve_stock_code(&state.db, &raw_code)
        .await
        .map_err(|e| api_error(&e.to_string()))?
        .unwrap_or_else(|| raw_code.trim().to_uppercase());

    let mut bars = postgres::get_stock_history(&state.db, &code, days)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    if bars.is_empty() {
        let end_date = crate::market_time::beijing_today();
        let start_date = end_date - chrono::Duration::days((days as i64 * 3).max(60));
        match state
            .provider
            .get_daily_bars_for_stock(&code, start_date, end_date)
            .await
        {
            Ok(mut fetched) if !fetched.is_empty() => {
                fetched.sort_by_key(|b| b.trade_date);
                let tuples: Vec<(String, Candle)> =
                    fetched.iter().cloned().map(|b| (code.clone(), b)).collect();
                let _ = postgres::upsert_daily_bars(&state.db, &tuples).await;
                bars = fetched;
            }
            _ => {
                return Err((
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": format!("No data for {}", code)})),
                ));
            }
        }
    }

    if period != "daily" {
        bars = resample_bars(&bars, period);
    }

    let name = postgres::get_stock_name(&state.db, &code)
        .await
        .map_err(|e| api_error(&e.to_string()))?
        .unwrap_or_else(|| code.clone());

    Ok(Json(json!({
        "code": code,
        "name": name,
        "period": period,
        "data": bars_to_json(&bars),
    })))
}

async fn chart_chips(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(code): Path<String>,
    Query(query): Query<DateQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let date = parse_optional_date(query.date.as_deref())
        .map_err(|msg| (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))))?;

    let svc = ChipDistService::new(state);
    let result = svc
        .get_chip_distribution(&code, date)
        .await
        .map_err(|e| api_error(&e.to_string()))?;

    match result {
        Some(v) => Ok(Json(json!(v))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("No chip data for {}", code)})),
        )),
    }
}

async fn chart_search(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ChartSearchQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let q = query.q.unwrap_or_default();
    if q.trim().is_empty() {
        return Ok(Json(json!({"results": []})));
    }

    let limit = query.limit.unwrap_or(10).clamp(1, 50) as i64;
    let stocks = postgres::search_stocks(&state.db, &q, limit)
        .await
        .map_err(|e| api_error(&e.to_string()))?;

    let results: Vec<Value> = stocks
        .iter()
        .map(|s| json!({"code": s.code, "name": s.name}))
        .collect();
    Ok(Json(json!({"results": results})))
}

async fn chart_watchlist_add(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<WatchlistMutation>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let svc = WatchlistService::new(state);
    let code = svc
        .add_stock(req.user_id, &req.code)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"success": true, "code": code})))
}

async fn chart_watchlist_remove(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<WatchlistMutation>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let svc = WatchlistService::new(state);
    let removed = svc
        .remove_stock(req.user_id, &req.code)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"success": removed})))
}

async fn chart_watchlist_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<WatchlistStatusQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let svc = WatchlistService::new(state);
    let in_watchlist = svc
        .contains_stock(query.user_id, &query.code)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"in_watchlist": in_watchlist})))
}

async fn chart_watchlist_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<WatchlistListQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let svc = WatchlistService::new(state);
    let items = svc
        .list_stocks(query.user_id)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"watchlist": items})))
}

async fn portfolio_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<PortfolioListQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let svc = PortfolioService::new(state);
    let items = svc
        .list_positions(query.user_id)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"portfolio": items})))
}

async fn portfolio_add(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<PortfolioAddRequest>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    if req.cost_price <= 0.0 || req.shares <= 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "cost_price and shares must be positive"})),
        ));
    }

    let svc = PortfolioService::new(state);
    let code = svc
        .add_position(req.user_id, &req.code, req.cost_price, req.shares)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"success": true, "code": code})))
}

async fn portfolio_remove(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<PortfolioRemoveRequest>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }

    let svc = PortfolioService::new(state);
    let removed = svc
        .remove_position(req.user_id, &req.code)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"success": removed})))
}

async fn sim_balance(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SimTypeQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let sim_type = normalize_sim_type(query.sim_type.as_deref());
    let svc = TradingSimService::new(state);
    let balance = svc
        .get_balance(sim_type)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": sim_type, "balance": balance})))
}

async fn sim_positions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SimTypeQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let sim_type = normalize_sim_type(query.sim_type.as_deref());
    let svc = TradingSimService::new(state);
    let positions = svc
        .list_open_positions(sim_type)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": sim_type, "positions": positions})))
}

async fn sim_buy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<SimBuyRequest>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    if req.price <= 0.0 || req.shares <= 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "price and shares must be positive"})),
        ));
    }

    let sim_type = normalize_sim_type(req.sim_type.as_deref());
    let svc = TradingSimService::new(state);
    let result = svc
        .buy(sim_type, &req.code, req.name, req.price, req.shares)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": sim_type, "trade": result})))
}

async fn sim_sell(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<SimSellRequest>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    if req.price <= 0.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "price must be positive"})),
        ));
    }

    let sim_type = normalize_sim_type(req.sim_type.as_deref());
    let svc = TradingSimService::new(state);
    let result = svc
        .sell(sim_type, req.position_id, req.price, req.reason)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": sim_type, "trade": result})))
}

async fn sim_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SimTypeQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let sim_type = normalize_sim_type(query.sim_type.as_deref());
    let svc = TradingSimService::new(state);
    let stats = svc
        .stats(sim_type)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"stats": stats})))
}

async fn daban_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<DabanReportQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let date = match query.date.as_deref() {
        Some(s) => match chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            Ok(d) => Some(d),
            Err(_) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "date must be YYYY-MM-DD"})),
                ));
            }
        },
        None => None,
    };
    let top_n = query.limit.unwrap_or(20).clamp(1, 100);
    let svc = DabanService::new(state);
    let report = svc
        .build_report(date, top_n)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"report": report})))
}

async fn daban_top(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<DabanReportQuery>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let date = match query.date.as_deref() {
        Some(s) => match chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            Ok(d) => Some(d),
            Err(_) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "date must be YYYY-MM-DD"})),
                ));
            }
        },
        None => None,
    };
    let top_n = query.limit.unwrap_or(20).clamp(1, 100);
    let svc = DabanService::new(state);
    let report = svc
        .build_report(date, top_n)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(
        json!({"date": report.summary.date, "top": report.top}),
    ))
}

async fn daban_sim_balance(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let svc = DabanSimService::new(state);
    let balance = svc
        .get_balance()
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": "daban", "balance": balance})))
}

async fn daban_sim_positions(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let svc = DabanSimService::new(state);
    let positions = svc
        .list_open_positions()
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": "daban", "positions": positions})))
}

async fn daban_sim_buy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<DabanSimBuyRequest>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    if req.price <= 0.0 || req.shares <= 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "price and shares must be positive"})),
        ));
    }
    let svc = DabanSimService::new(state);
    let trade = svc
        .buy(&req.code, req.name, req.price, req.shares, req.score)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": "daban", "trade": trade})))
}

async fn daban_sim_sell(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<DabanSimSellRequest>,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    if req.price <= 0.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "price must be positive"})),
        ));
    }
    let svc = DabanSimService::new(state);
    let trade = svc
        .sell(req.position_id, req.price, req.reason)
        .await
        .map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": "daban", "trade": trade})))
}

async fn daban_sim_stats(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let svc = DabanSimService::new(state);
    let stats = svc.stats().await.map_err(|e| api_error(&e.to_string()))?;
    Ok(Json(json!({"sim_type": "daban", "stats": stats})))
}

async fn trigger_fetch(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let s = state.clone();
    let p = state.provider.clone();
    tokio::spawn(async move {
        crate::scheduler::run_fetch_job(s, p).await;
    });
    Ok(Json(json!({"status": "started", "job": "fetch"})))
}

async fn trigger_scan_job(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let s = state.clone();
    tokio::spawn(async move {
        crate::scheduler::run_scan_job(s).await;
    });
    Ok(Json(json!({"status": "started", "job": "scan"})))
}

async fn trigger_daily_report(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let s = state.clone();
    let p = state.provider.clone();
    let push = state.pusher.clone();
    tokio::spawn(async move {
        crate::scheduler::run_daily_report_job(s, p, push).await;
    });
    Ok(Json(json!({"status": "started", "job": "report/daily"})))
}

async fn trigger_weekly_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ));
    }
    let s = state.clone();
    let p = state.provider.clone();
    let push = state.pusher.clone();
    tokio::spawn(async move {
        crate::scheduler::run_weekly_report_job(s, p, push).await;
    });
    Ok(Json(json!({"status": "started", "job": "report/weekly"})))
}
