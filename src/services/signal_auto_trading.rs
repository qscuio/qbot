use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Timelike};
use serde::Serialize;
use tracing::{info, warn};

use crate::data::sina::SinaClient;
use crate::data::types::{Candle, Quote};
use crate::error::{AppError, Result};
use crate::market_time::{beijing_now, beijing_today, is_a_share_trading_now};
use crate::services::daban::{DabanScore, DabanService};
use crate::services::limit_up::LimitUpService;
use crate::services::prestart::{
    is_prestart_signal, PrestartCandidate, PrestartService, PrestartTier,
};
use crate::services::scan_ranker::{
    POOL_LONG_A_ID, POOL_LONG_B_ID, POOL_MID_A_ID, POOL_MID_B_ID, POOL_SHORT_A_ID, POOL_SHORT_B_ID,
};
use crate::services::scanner::SignalHit;
use crate::signals::base::{avg_volume, sma};
use crate::signals::registry::SignalRegistry;
use crate::state::AppState;
use crate::storage::postgres::{self, StrongLimitUpStock};

const INITIAL_CAPITAL: f64 = 100_000.0;
const DEFAULT_STOP_LOSS_PCT: f64 = 5.0;
const DEFAULT_TRAILING_STOP_PCT: f64 = 3.5;
const BUY_START_MINUTE: u32 = 9 * 60 + 35;
const BUY_END_MINUTE: u32 = 14 * 60 + 30;
const EXPIRE_MINUTE: u32 = 14 * 60 + 45;
const POLL_INTERVAL_SECS: u64 = 30;
const OFF_HOURS_POLL_INTERVAL_SECS: u64 = 60;
const AUTO_DABAN_ID: &str = "auto_daban";
const AUTO_DABAN_NAME: &str = "自动打板";
const AUTO_STRONG_ID: &str = "auto_strong";
const AUTO_STRONG_NAME: &str = "自动强势股";
const DABAN_MIN_SCORE: f64 = 60.0;
const STRONG_WINDOW_DAYS: i64 = 7;
const STRONG_MIN_LIMIT_COUNT: i64 = 3;
const AUTO_STRONG_POOL_PRIORITY: [&str; 3] = [POOL_SHORT_A_ID, POOL_MID_A_ID, POOL_LONG_A_ID];

fn builtin_ranked_pool_accounts() -> [(&'static str, &'static str); 3] {
    [
        (POOL_SHORT_A_ID, "短线A档"),
        (POOL_MID_A_ID, "中线A档"),
        (POOL_LONG_A_ID, "长线A档"),
    ]
}

fn is_ranked_pool_signal(signal_id: &str) -> bool {
    matches!(
        signal_id,
        POOL_SHORT_A_ID
            | POOL_SHORT_B_ID
            | POOL_MID_A_ID
            | POOL_MID_B_ID
            | POOL_LONG_A_ID
            | POOL_LONG_B_ID
    )
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyAccountSnapshot {
    pub signal_id: String,
    pub signal_name: String,
    pub cash_balance: f64,
    pub initial_capital: f64,
    pub open_positions: i64,
    pub pending_candidates: i64,
    pub equity: f64,
    pub pnl_pct: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub closed_trades: i64,
    pub winning_trades: i64,
    pub weekly_pnl: f64,
    pub max_drawdown_pct: f64,
    pub win_streak: i64,
    pub loss_streak: i64,
}

#[derive(Debug, Clone)]
struct SignalAutoEventLine {
    signal_id: String,
    signal_name: String,
    title: String,
    detail: String,
}

#[derive(Debug, Clone)]
struct StrategyAccountRow {
    id: i64,
    signal_id: String,
    signal_name: String,
    cash_balance: f64,
    initial_capital: f64,
    stop_loss_pct: f64,
    trailing_stop_pct: f64,
}

#[derive(Debug, Clone)]
struct PendingCandidateRow {
    id: i64,
    account_id: i64,
    signal_id: String,
    signal_name: String,
    signal_date: NaiveDate,
    code: String,
    name: String,
    score: f64,
    selection_reason: String,
}

#[derive(Debug, Clone)]
struct OpenPositionRow {
    id: i64,
    account_id: i64,
    signal_id: String,
    signal_name: String,
    code: String,
    name: String,
    entry_price: f64,
    shares: i32,
    peak_price: f64,
    stop_loss_price: f64,
    trailing_stop_pct: f64,
    entry_date: NaiveDate,
}

#[derive(Debug, Clone)]
struct PositionMetricRow {
    code: String,
    shares: i32,
    entry_price: f64,
    entry_date: NaiveDate,
    exit_price: Option<f64>,
    exit_date: Option<NaiveDate>,
    is_open: bool,
    pnl_pct: Option<f64>,
}

#[derive(Debug, Clone)]
struct CandidateScore {
    code: String,
    name: String,
    score: f64,
    selection_reason: String,
    metadata: serde_json::Value,
}

#[derive(Debug, Clone)]
struct EntryDecision {
    buy_now: bool,
    reason: String,
}

#[derive(Debug, Clone)]
struct ExitDecision {
    sell_now: bool,
    reason: String,
}

pub struct SignalAutoTradingService {
    state: Arc<AppState>,
    sina: Arc<SinaClient>,
}

impl SignalAutoTradingService {
    pub fn new(state: Arc<AppState>, sina: Arc<SinaClient>) -> Self {
        Self { state, sina }
    }

    pub async fn ensure_accounts(&self) -> Result<()> {
        for signal in SignalRegistry::get_enabled() {
            self.ensure_account(signal.signal_id(), signal.display_name())
                .await?;
        }
        for (signal_id, signal_name) in builtin_ranked_pool_accounts() {
            self.ensure_account(signal_id, signal_name).await?;
        }
        self.ensure_account(AUTO_DABAN_ID, AUTO_DABAN_NAME).await?;
        self.ensure_account(AUTO_STRONG_ID, AUTO_STRONG_NAME)
            .await?;
        Ok(())
    }

    pub async fn prepare_candidates_from_scan(
        &self,
        results: &HashMap<String, Vec<SignalHit>>,
    ) -> Result<usize> {
        self.ensure_accounts().await?;
        let signal_date = beijing_today();
        let accounts = self.list_accounts_internal().await?;
        let prestart_candidates = PrestartService::new(self.state.clone())
            .list_candidates_from_scan(results, 50)
            .await?;
        let daban_candidate = self.pick_best_daban_candidate(signal_date).await?;
        let strong_candidate = match self.pick_best_ranked_pool_candidate(results).await? {
            Some(candidate) => Some(candidate),
            None => self.pick_best_strong_candidate(signal_date).await?,
        };
        let mut created = 0usize;

        for account in accounts {
            if self.account_has_open_position(account.id).await? {
                continue;
            }
            if self.account_has_pending_candidate(account.id).await? {
                continue;
            }

            if account.signal_id == AUTO_DABAN_ID {
                if let Some(best) = daban_candidate.clone() {
                    self.save_candidate(&account, signal_date, &best).await?;
                    created += 1;
                }
                continue;
            }

            if account.signal_id == AUTO_STRONG_ID {
                if let Some(best) = strong_candidate.clone() {
                    self.save_candidate(&account, signal_date, &best).await?;
                    created += 1;
                }
                continue;
            }

            if is_prestart_signal(&account.signal_id) {
                if let Some(best) = self
                    .pick_best_prestart_candidate(
                        &account.signal_id,
                        &prestart_candidates,
                        PrestartTier::A,
                    )
                    .await?
                {
                    self.save_candidate(&account, signal_date, &best).await?;
                    created += 1;
                    continue;
                }

                if let Some(observe) = self
                    .pick_best_prestart_candidate(
                        &account.signal_id,
                        &prestart_candidates,
                        PrestartTier::B,
                    )
                    .await?
                {
                    self.record_watch_only(&account, &observe).await?;
                }
                continue;
            }

            if let Some(best) = self
                .pick_best_signal_candidate(&account.signal_id, results)
                .await?
            {
                self.save_candidate(&account, signal_date, &best).await?;
                created += 1;
            }
        }

        Ok(created)
    }

    async fn ensure_account(&self, signal_id: &str, signal_name: &str) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO signal_strategy_accounts
               (signal_id, signal_name, enabled, initial_capital, cash_balance, stop_loss_pct, trailing_stop_pct, max_positions)
               VALUES ($1, $2, TRUE, $3, $4, $5, $6, 1)
               ON CONFLICT (signal_id) DO UPDATE SET
                 signal_name = EXCLUDED.signal_name,
                 enabled = TRUE,
                 updated_at = NOW()"#,
        )
        .bind(signal_id)
        .bind(signal_name)
        .bind(INITIAL_CAPITAL)
        .bind(INITIAL_CAPITAL)
        .bind(DEFAULT_STOP_LOSS_PCT)
        .bind(DEFAULT_TRAILING_STOP_PCT)
        .execute(&self.state.db)
        .await?;
        Ok(())
    }

    async fn save_candidate(
        &self,
        account: &StrategyAccountRow,
        signal_date: NaiveDate,
        candidate: &CandidateScore,
    ) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO signal_strategy_candidates
               (account_id, signal_id, signal_date, code, name, score, selection_reason, signal_metadata, candidate_status, planned_entry_date)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', NULL)
               ON CONFLICT (account_id, signal_date) DO UPDATE SET
                 code = EXCLUDED.code,
                 name = EXCLUDED.name,
                 score = EXCLUDED.score,
                 selection_reason = EXCLUDED.selection_reason,
                 signal_metadata = EXCLUDED.signal_metadata,
                 candidate_status = 'pending',
                 planned_entry_date = NULL,
                 updated_at = NOW()"#,
        )
        .bind(account.id)
        .bind(&account.signal_id)
        .bind(signal_date)
        .bind(&candidate.code)
        .bind(&candidate.name)
        .bind(candidate.score)
        .bind(&candidate.selection_reason)
        .bind(&candidate.metadata)
        .execute(&self.state.db)
        .await?;

        sqlx::query(
            r#"UPDATE signal_strategy_accounts
               SET last_candidate_date = $1, updated_at = NOW()
               WHERE id = $2"#,
        )
        .bind(signal_date)
        .bind(account.id)
        .execute(&self.state.db)
        .await?;

        let detail = format!(
            "已选出次日候选: {} {}\n评分: {:.1}\n逻辑: {}",
            candidate.code, candidate.name, candidate.score, candidate.selection_reason
        );
        self.record_event(
            account.id,
            None,
            None,
            &account.signal_id,
            "candidate_selected",
            Some(&candidate.code),
            "候选入池",
            &detail,
        )
        .await?;
        Ok(())
    }

    async fn record_watch_only(
        &self,
        account: &StrategyAccountRow,
        observe: &CandidateScore,
    ) -> Result<()> {
        let detail = format!(
            "仅进入观察池: {} {}\n评分: {:.1}\n逻辑: {}",
            observe.code, observe.name, observe.score, observe.selection_reason
        );
        self.record_event(
            account.id,
            None,
            None,
            &account.signal_id,
            "watch_only",
            Some(&observe.code),
            "B档观察",
            &detail,
        )
        .await?;
        self.push_telegram_message(&format!(
            "🔎 <b>B档观察</b>\n信号: {}\n{} {}\n{}",
            escape_html(&account.signal_id),
            escape_html(&observe.code),
            escape_html(&observe.name),
            escape_html(&detail)
        ))
        .await;
        Ok(())
    }

    pub async fn run_poll_loop(&self) {
        info!("Signal auto trading loop started");
        loop {
            if !is_a_share_trading_now() {
                tokio::time::sleep(Duration::from_secs(OFF_HOURS_POLL_INTERVAL_SECS)).await;
                continue;
            }

            if let Err(e) = self.poll_once().await {
                warn!("Signal auto trading poll error: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        }
    }

    pub async fn poll_once(&self) -> Result<()> {
        self.ensure_accounts().await?;
        self.process_pending_entries().await?;
        self.process_open_positions().await?;
        Ok(())
    }

    pub async fn generate_daily_report(&self, report_date: NaiveDate) -> Result<String> {
        self.ensure_accounts().await?;
        let accounts = self.list_account_snapshots().await?;
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            r#"SELECT a.signal_id,
                      a.signal_name,
                      e.title,
                      e.detail
               FROM signal_strategy_events e
               JOIN signal_strategy_accounts a ON a.id = e.account_id
               WHERE (e.event_time AT TIME ZONE 'Asia/Shanghai')::date = $1
               ORDER BY e.event_time DESC
               LIMIT 30"#,
        )
        .bind(report_date)
        .fetch_all(&self.state.db)
        .await?;

        let events: Vec<SignalAutoEventLine> = rows
            .into_iter()
            .map(
                |(signal_id, signal_name, title, detail)| SignalAutoEventLine {
                    signal_id,
                    signal_name,
                    title,
                    detail,
                },
            )
            .collect();

        let report = format_signal_auto_daily_report(report_date, &accounts, &events);
        postgres::save_report(&self.state.db, "signal_auto", &report).await?;
        Ok(report)
    }

    pub async fn latest_report(&self) -> Result<Option<String>> {
        postgres::get_latest_report(&self.state.db, "signal_auto").await
    }

    pub async fn list_account_snapshots(&self) -> Result<Vec<StrategyAccountSnapshot>> {
        self.ensure_accounts().await?;
        let accounts = self.list_accounts_internal().await?;
        let mut snapshots = Vec::with_capacity(accounts.len());
        let today = beijing_today();
        let week_start =
            today - chrono::Duration::days(today.weekday().num_days_from_monday() as i64);

        for account in accounts {
            let open_positions: i64 = sqlx::query_scalar(
                r#"SELECT COUNT(*)::bigint
                   FROM signal_strategy_positions
                   WHERE account_id = $1 AND is_open = TRUE"#,
            )
            .bind(account.id)
            .fetch_one(&self.state.db)
            .await
            .unwrap_or(0);

            let pending_candidates: i64 = sqlx::query_scalar(
                r#"SELECT COUNT(*)::bigint
                   FROM signal_strategy_candidates
                   WHERE account_id = $1 AND candidate_status = 'pending'"#,
            )
            .bind(account.id)
            .fetch_one(&self.state.db)
            .await
            .unwrap_or(0);

            let open_value: Option<f64> = sqlx::query_scalar(
                r#"SELECT SUM(p.shares::float8 * last_bar.close::float8)
                   FROM signal_strategy_positions p
                   LEFT JOIN LATERAL (
                     SELECT close
                     FROM stock_daily_bars b
                     WHERE b.code = p.code
                     ORDER BY b.trade_date DESC
                     LIMIT 1
                   ) last_bar ON TRUE
                   WHERE p.account_id = $1 AND p.is_open = TRUE"#,
            )
            .bind(account.id)
            .fetch_one(&self.state.db)
            .await
            .unwrap_or(None);

            let open_cost: Option<f64> = sqlx::query_scalar(
                r#"SELECT SUM(p.shares::float8 * p.entry_price::float8)
                   FROM signal_strategy_positions p
                   WHERE p.account_id = $1 AND p.is_open = TRUE"#,
            )
            .bind(account.id)
            .fetch_one(&self.state.db)
            .await
            .unwrap_or(None);

            let closed_stats: Option<(i64, i64, f64)> = sqlx::query_as(
                r#"SELECT COUNT(*)::bigint AS closed_trades,
                          COUNT(*) FILTER (WHERE p.pnl_pct > 0)::bigint AS winning_trades,
                          COALESCE(SUM((p.exit_price::float8 - p.entry_price::float8) * p.shares::float8), 0)::float8 AS realized_pnl
                   FROM signal_strategy_positions p
                   WHERE p.account_id = $1 AND p.is_open = FALSE"#,
            )
            .bind(account.id)
            .fetch_optional(&self.state.db)
            .await
            .unwrap_or(None);

            let equity = account.cash_balance + open_value.unwrap_or(0.0);
            let pnl_pct = if account.initial_capital > 0.0 {
                (equity - account.initial_capital) / account.initial_capital * 100.0
            } else {
                0.0
            };
            let unrealized_pnl = open_value.unwrap_or(0.0) - open_cost.unwrap_or(0.0);
            let (closed_trades, winning_trades, realized_pnl) = closed_stats.unwrap_or((0, 0, 0.0));
            let metrics = self.list_position_metrics(account.id).await?;
            let weekly_pnl = self.compute_weekly_pnl(&metrics, week_start).await?;
            let max_drawdown_pct =
                compute_max_drawdown_pct(account.initial_capital, realized_pnl, equity, &metrics);
            let (win_streak, loss_streak) = compute_trade_streaks(&metrics);

            snapshots.push(StrategyAccountSnapshot {
                signal_id: account.signal_id,
                signal_name: account.signal_name,
                cash_balance: round2(account.cash_balance),
                initial_capital: round2(account.initial_capital),
                open_positions,
                pending_candidates,
                equity: round2(equity),
                pnl_pct: round2(pnl_pct),
                realized_pnl: round2(realized_pnl),
                unrealized_pnl: round2(unrealized_pnl),
                closed_trades,
                winning_trades,
                weekly_pnl: round2(weekly_pnl),
                max_drawdown_pct: round2(max_drawdown_pct),
                win_streak,
                loss_streak,
            });
        }

        snapshots.sort_by(|a, b| {
            b.pnl_pct
                .partial_cmp(&a.pnl_pct)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.signal_id.cmp(&b.signal_id))
        });
        Ok(snapshots)
    }

    async fn list_position_metrics(&self, account_id: i64) -> Result<Vec<PositionMetricRow>> {
        let rows: Vec<(
            String,
            i32,
            f64,
            NaiveDate,
            Option<f64>,
            Option<NaiveDate>,
            bool,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT code,
                      shares,
                      entry_price::float8,
                      entry_date,
                      exit_price::float8,
                      exit_date,
                      is_open,
                      pnl_pct::float8
               FROM signal_strategy_positions
               WHERE account_id = $1
               ORDER BY COALESCE(exit_date, entry_date) ASC, id ASC"#,
        )
        .bind(account_id)
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    code,
                    shares,
                    entry_price,
                    entry_date,
                    exit_price,
                    exit_date,
                    is_open,
                    pnl_pct,
                )| {
                    PositionMetricRow {
                        code,
                        shares,
                        entry_price,
                        entry_date,
                        exit_price,
                        exit_date,
                        is_open,
                        pnl_pct,
                    }
                },
            )
            .collect())
    }

    async fn compute_weekly_pnl(
        &self,
        metrics: &[PositionMetricRow],
        week_start: NaiveDate,
    ) -> Result<f64> {
        let mut weekly_pnl = 0.0;

        for row in metrics {
            if !row.is_open && row.exit_date.map(|d| d < week_start).unwrap_or(true) {
                continue;
            }

            let week_start_price = if row.entry_date > week_start {
                row.entry_price
            } else {
                self.get_close_on_or_before(&row.code, week_start)
                    .await?
                    .unwrap_or(row.entry_price)
            };

            let end_price = if row.is_open {
                self.latest_close(&row.code)
                    .await?
                    .unwrap_or(row.entry_price)
            } else {
                row.exit_price.unwrap_or(row.entry_price)
            };

            weekly_pnl += (end_price - week_start_price) * row.shares as f64;
        }

        Ok(weekly_pnl)
    }

    async fn get_close_on_or_before(&self, code: &str, date: NaiveDate) -> Result<Option<f64>> {
        let close: Option<f64> = sqlx::query_scalar(
            r#"SELECT close::float8
               FROM stock_daily_bars
               WHERE code = $1 AND trade_date <= $2
               ORDER BY trade_date DESC
               LIMIT 1"#,
        )
        .bind(code)
        .bind(date)
        .fetch_optional(&self.state.db)
        .await?
        .flatten();
        Ok(close)
    }

    async fn latest_close(&self, code: &str) -> Result<Option<f64>> {
        let close: Option<f64> = sqlx::query_scalar(
            r#"SELECT close::float8
               FROM stock_daily_bars
               WHERE code = $1
               ORDER BY trade_date DESC
               LIMIT 1"#,
        )
        .bind(code)
        .fetch_optional(&self.state.db)
        .await?
        .flatten();
        Ok(close)
    }

    async fn process_pending_entries(&self) -> Result<()> {
        let now = beijing_now();
        let minutes = now.hour() * 60 + now.minute();
        let pending = self.list_pending_candidates().await?;
        if pending.is_empty() {
            return Ok(());
        }

        let refs: Vec<&str> = pending.iter().map(|row| row.code.as_str()).collect();
        let quotes = self.sina.get_quotes(&refs).await?;

        for candidate in pending {
            let Some(quote) = quotes.get(&candidate.code) else {
                continue;
            };

            if candidate.signal_date >= beijing_today() {
                continue;
            }

            if minutes > EXPIRE_MINUTE {
                self.skip_candidate(
                    candidate.id,
                    candidate.account_id,
                    &candidate.signal_id,
                    &candidate.code,
                    "候选过期",
                    "次日买入窗口结束，今日未出现承接确认条件",
                )
                .await?;
                continue;
            }

            let decision = evaluate_entry_signal(now, quote);
            if !decision.buy_now {
                continue;
            }

            let account = self.load_account(candidate.account_id).await?;
            if self.account_has_open_position(account.id).await? {
                continue;
            }

            let shares = board_lot_shares(account.cash_balance, quote.price);
            if shares < 100 {
                self.skip_candidate(
                    candidate.id,
                    candidate.account_id,
                    &candidate.signal_id,
                    &candidate.code,
                    "资金不足",
                    "账户资金不足以买入一手",
                )
                .await?;
                continue;
            }

            let cost = shares as f64 * quote.price;
            let mut tx = self.state.db.begin().await?;

            sqlx::query(
                r#"UPDATE signal_strategy_accounts
                   SET cash_balance = cash_balance - $1,
                       last_trade_date = $2,
                       updated_at = NOW()
                   WHERE id = $3"#,
            )
            .bind(cost)
            .bind(beijing_today())
            .bind(account.id)
            .execute(&mut *tx)
            .await?;

            let position_id: i64 = sqlx::query_scalar(
                r#"INSERT INTO signal_strategy_positions
                   (account_id, candidate_id, signal_id, code, name, score, entry_price, shares, peak_price, stop_loss_price, trailing_stop_pct, entry_date, entry_time, entry_reason, is_open)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $7, $9, $10, $11, NOW(), $12, TRUE)
                   RETURNING id"#,
            )
            .bind(account.id)
            .bind(candidate.id)
            .bind(&candidate.signal_id)
            .bind(&candidate.code)
            .bind(&candidate.name)
            .bind(candidate.score)
            .bind(quote.price)
            .bind(shares)
            .bind(quote.price * (1.0 - account.stop_loss_pct / 100.0))
            .bind(account.trailing_stop_pct)
            .bind(beijing_today())
            .bind(format!(
                "评分最高候选，盘中承接确认。{}；候选逻辑：{}",
                decision.reason, candidate.selection_reason
            ))
            .fetch_one(&mut *tx)
            .await?;

            sqlx::query(
                r#"UPDATE signal_strategy_candidates
                   SET candidate_status = 'bought',
                       planned_entry_date = $1,
                       entry_reason = $2,
                       updated_at = NOW()
                   WHERE id = $3"#,
            )
            .bind(beijing_today())
            .bind(&decision.reason)
            .bind(candidate.id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            let detail = format!(
                "{} {}\n买入价 {:.2}，{}股，金额 {:.0}\n评分 {:.1}\n买入逻辑: {}\n选股逻辑: {}",
                candidate.code,
                candidate.name,
                quote.price,
                shares,
                cost,
                candidate.score,
                decision.reason,
                candidate.selection_reason
            );
            self.record_event(
                account.id,
                Some(position_id),
                Some(candidate.id),
                &candidate.signal_id,
                "buy",
                Some(&candidate.code),
                "自动买入",
                &detail,
            )
            .await?;
            self.push_telegram_message(&format!(
                "🟢 <b>自动买入</b>\n信号: {} {}\n{}",
                escape_html(&candidate.signal_name),
                escape_html(&candidate.signal_id),
                escape_html(&detail)
            ))
            .await;
        }

        Ok(())
    }

    async fn process_open_positions(&self) -> Result<()> {
        let open_positions = self.list_open_positions().await?;
        if open_positions.is_empty() {
            return Ok(());
        }

        let refs: Vec<&str> = open_positions.iter().map(|row| row.code.as_str()).collect();
        let quotes = self.sina.get_quotes(&refs).await?;

        for position in open_positions {
            let Some(quote) = quotes.get(&position.code) else {
                continue;
            };

            let new_peak = position.peak_price.max(quote.price);
            if new_peak > position.peak_price {
                sqlx::query(
                    r#"UPDATE signal_strategy_positions
                       SET peak_price = $1
                       WHERE id = $2"#,
                )
                .bind(new_peak)
                .bind(position.id)
                .execute(&self.state.db)
                .await?;
            }

            let decision = evaluate_exit_signal(
                beijing_today(),
                position.entry_date,
                quote,
                position.entry_price,
                new_peak,
                position.stop_loss_price,
                position.trailing_stop_pct,
            );
            if !decision.sell_now {
                continue;
            }

            let proceeds = quote.price * position.shares as f64;
            let pnl_pct = (quote.price - position.entry_price) / position.entry_price * 100.0;

            let mut tx = self.state.db.begin().await?;
            sqlx::query(
                r#"UPDATE signal_strategy_positions
                   SET peak_price = $1,
                       exit_price = $2,
                       exit_date = $3,
                       exit_time = NOW(),
                       exit_reason = $4,
                       pnl_pct = $5,
                       is_open = FALSE
                   WHERE id = $6"#,
            )
            .bind(new_peak)
            .bind(quote.price)
            .bind(beijing_today())
            .bind(&decision.reason)
            .bind(pnl_pct)
            .bind(position.id)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"UPDATE signal_strategy_accounts
                   SET cash_balance = cash_balance + $1,
                       last_trade_date = $2,
                       updated_at = NOW()
                   WHERE id = $3"#,
            )
            .bind(proceeds)
            .bind(beijing_today())
            .bind(position.account_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;

            let detail = format!(
                "{} {}\n卖出价 {:.2}，{}股\n收益 {:+.2}%\n卖出逻辑: {}",
                position.code,
                position.name,
                quote.price,
                position.shares,
                pnl_pct,
                decision.reason
            );
            self.record_event(
                position.account_id,
                Some(position.id),
                None,
                &position.signal_id,
                "sell",
                Some(&position.code),
                "自动卖出",
                &detail,
            )
            .await?;
            self.push_telegram_message(&format!(
                "🔴 <b>自动卖出</b>\n信号: {} {}\n{}",
                escape_html(&position.signal_name),
                escape_html(&position.signal_id),
                escape_html(&detail)
            ))
            .await;
        }

        Ok(())
    }

    async fn pick_best_prestart_candidate(
        &self,
        signal_id: &str,
        candidates: &[PrestartCandidate],
        desired_tier: PrestartTier,
    ) -> Result<Option<CandidateScore>> {
        let mut best: Option<CandidateScore> = None;

        for item in candidates {
            if item.tier != desired_tier
                || !item.matched_signal_ids.iter().any(|id| id == signal_id)
            {
                continue;
            }

            let bars = match postgres::get_stock_history(&self.state.db, &item.code, 90).await {
                Ok(v) if v.len() >= 25 => v,
                _ => continue,
            };
            let (signal_score, signal_reasons) = score_candidate(signal_id, &bars);
            let score = round2(item.score + signal_score * 0.35);
            let mut combined_reasons = item.reasons.clone();
            combined_reasons.extend(signal_reasons.clone());
            let selection_reason = combined_reasons.join("；");
            let metadata = serde_json::json!({
                "prestart_tier": item.tier,
                "prestart_reasons": item.reasons,
                "matched_signal_ids": item.matched_signal_ids,
                "matched_signal_names": item.matched_signal_names,
                "signal_score_reasons": signal_reasons,
            });
            let candidate = CandidateScore {
                code: item.code.clone(),
                name: item.name.clone(),
                score,
                selection_reason,
                metadata,
            };
            match &best {
                Some(current) if current.score >= candidate.score => {}
                _ => best = Some(candidate),
            }
        }

        Ok(best)
    }

    async fn pick_best_signal_candidate(
        &self,
        signal_id: &str,
        results: &HashMap<String, Vec<SignalHit>>,
    ) -> Result<Option<CandidateScore>> {
        let Some(hits) = results.get(signal_id) else {
            return Ok(None);
        };

        let mut best: Option<CandidateScore> = None;
        for hit in hits {
            let bars = match postgres::get_stock_history(&self.state.db, &hit.code, 90).await {
                Ok(v) if v.len() >= 25 => v,
                _ => continue,
            };
            let Some(candidate) = build_signal_candidate(signal_id, hit, &bars) else {
                continue;
            };
            match &best {
                Some(current) if current.score >= candidate.score => {}
                _ => best = Some(candidate),
            }
        }

        Ok(best)
    }

    async fn pick_best_ranked_pool_candidate(
        &self,
        results: &HashMap<String, Vec<SignalHit>>,
    ) -> Result<Option<CandidateScore>> {
        let mut best: Option<CandidateScore> = None;

        for signal_id in AUTO_STRONG_POOL_PRIORITY {
            let Some(hits) = results.get(signal_id) else {
                continue;
            };

            for hit in hits {
                let bars = match postgres::get_stock_history(&self.state.db, &hit.code, 90).await {
                    Ok(v) if v.len() >= 25 => v,
                    _ => continue,
                };
                let Some(candidate) = build_signal_candidate(signal_id, hit, &bars) else {
                    continue;
                };
                match &best {
                    Some(current) if current.score >= candidate.score => {}
                    _ => best = Some(candidate),
                }
            }
        }

        Ok(best)
    }

    async fn pick_best_daban_candidate(
        &self,
        target_date: NaiveDate,
    ) -> Result<Option<CandidateScore>> {
        let svc = DabanService::new(self.state.clone());
        let scores = svc.score_all(&svc.load_limit_up_by_date(target_date).await?);
        let mut best: Option<CandidateScore> = None;

        for item in scores {
            if item.score < DABAN_MIN_SCORE
                || item.executability == "一字板"
                || item.executability == "多次炸板"
            {
                continue;
            }

            let bars = match postgres::get_stock_history(&self.state.db, &item.code, 90).await {
                Ok(v) if v.len() >= 25 => v,
                _ => continue,
            };

            let (score, reasons) = score_daban_candidate(&item, &bars);
            let selection_reason = reasons.join("；");
            let metadata = serde_json::json!({
                "daban_score": item,
                "score_reasons": reasons,
            });
            let candidate = CandidateScore {
                code: item.code.clone(),
                name: item.name.clone(),
                score,
                selection_reason,
                metadata,
            };
            match &best {
                Some(current) if current.score >= candidate.score => {}
                _ => best = Some(candidate),
            }
        }

        Ok(best)
    }

    async fn pick_best_strong_candidate(
        &self,
        target_date: NaiveDate,
    ) -> Result<Option<CandidateScore>> {
        let svc = LimitUpService::new(self.state.clone(), self.state.provider.clone());
        let mut best: Option<CandidateScore> = None;

        for item in svc
            .get_strong_stocks(STRONG_WINDOW_DAYS, STRONG_MIN_LIMIT_COUNT)
            .await?
        {
            let bars = match postgres::get_stock_history(&self.state.db, &item.code, 90).await {
                Ok(v) if v.len() >= 25 => v,
                _ => continue,
            };

            let (score, reasons) = score_strong_candidate(&item, target_date, &bars);
            let selection_reason = reasons.join("；");
            let metadata = serde_json::json!({
                "strong_limit_up": {
                    "limit_count": item.limit_count,
                    "latest_trade_date": item.latest_trade_date,
                },
                "score_reasons": reasons,
            });
            let candidate = CandidateScore {
                code: item.code.clone(),
                name: item.name.clone(),
                score,
                selection_reason,
                metadata,
            };
            match &best {
                Some(current) if current.score >= candidate.score => {}
                _ => best = Some(candidate),
            }
        }

        Ok(best)
    }

    async fn list_accounts_internal(&self) -> Result<Vec<StrategyAccountRow>> {
        let rows: Vec<(i64, String, String, f64, f64, f64, f64)> = sqlx::query_as(
            r#"SELECT id,
                      signal_id,
                      signal_name,
                      cash_balance::float8,
                      initial_capital::float8,
                      stop_loss_pct::float8,
                      trailing_stop_pct::float8
               FROM signal_strategy_accounts
               WHERE enabled = TRUE
               ORDER BY signal_id"#,
        )
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    signal_id,
                    signal_name,
                    cash_balance,
                    initial_capital,
                    stop_loss_pct,
                    trailing_stop_pct,
                )| StrategyAccountRow {
                    id,
                    signal_id,
                    signal_name,
                    cash_balance,
                    initial_capital,
                    stop_loss_pct,
                    trailing_stop_pct,
                },
            )
            .collect())
    }

    async fn load_account(&self, account_id: i64) -> Result<StrategyAccountRow> {
        let row: Option<(i64, String, String, f64, f64, f64, f64)> = sqlx::query_as(
            r#"SELECT id,
                      signal_id,
                      signal_name,
                      cash_balance::float8,
                      initial_capital::float8,
                      stop_loss_pct::float8,
                      trailing_stop_pct::float8
               FROM signal_strategy_accounts
               WHERE id = $1
               LIMIT 1"#,
        )
        .bind(account_id)
        .fetch_optional(&self.state.db)
        .await?;

        let (
            id,
            signal_id,
            signal_name,
            cash_balance,
            initial_capital,
            stop_loss_pct,
            trailing_stop_pct,
        ) = row.ok_or_else(|| AppError::Internal(format!("account {} not found", account_id)))?;

        Ok(StrategyAccountRow {
            id,
            signal_id,
            signal_name,
            cash_balance,
            initial_capital,
            stop_loss_pct,
            trailing_stop_pct,
        })
    }

    async fn account_has_open_position(&self, account_id: i64) -> Result<bool> {
        let row: Option<(bool,)> = sqlx::query_as(
            r#"SELECT EXISTS(
                 SELECT 1
                 FROM signal_strategy_positions
                 WHERE account_id = $1 AND is_open = TRUE
               )"#,
        )
        .bind(account_id)
        .fetch_optional(&self.state.db)
        .await?;
        Ok(row.map(|r| r.0).unwrap_or(false))
    }

    async fn account_has_pending_candidate(&self, account_id: i64) -> Result<bool> {
        let row: Option<(bool,)> = sqlx::query_as(
            r#"SELECT EXISTS(
                 SELECT 1
                 FROM signal_strategy_candidates
                 WHERE account_id = $1 AND candidate_status = 'pending'
               )"#,
        )
        .bind(account_id)
        .fetch_optional(&self.state.db)
        .await?;
        Ok(row.map(|r| r.0).unwrap_or(false))
    }

    async fn list_pending_candidates(&self) -> Result<Vec<PendingCandidateRow>> {
        let rows: Vec<(
            i64,
            i64,
            String,
            String,
            NaiveDate,
            String,
            Option<String>,
            f64,
            String,
        )> = sqlx::query_as(
            r#"SELECT c.id,
                          c.account_id,
                          c.signal_id,
                          a.signal_name,
                          c.signal_date,
                          c.code,
                          c.name,
                          c.score::float8,
                          c.selection_reason
                   FROM signal_strategy_candidates c
                   JOIN signal_strategy_accounts a ON a.id = c.account_id
                   WHERE c.candidate_status = 'pending'
                   ORDER BY c.signal_date, c.score DESC"#,
        )
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    account_id,
                    signal_id,
                    signal_name,
                    signal_date,
                    code,
                    name,
                    score,
                    selection_reason,
                )| PendingCandidateRow {
                    id,
                    account_id,
                    signal_id,
                    signal_name,
                    signal_date,
                    name: name.unwrap_or_else(|| code.clone()),
                    code,
                    score,
                    selection_reason,
                },
            )
            .collect())
    }

    async fn list_open_positions(&self) -> Result<Vec<OpenPositionRow>> {
        let rows: Vec<(
            i64,
            i64,
            String,
            String,
            String,
            Option<String>,
            f64,
            i32,
            f64,
            f64,
            f64,
            NaiveDate,
        )> = sqlx::query_as(
            r#"SELECT p.id,
                      p.account_id,
                      p.signal_id,
                      a.signal_name,
                      p.code,
                      p.name,
                      p.entry_price::float8,
                      p.shares,
                      p.peak_price::float8,
                      p.stop_loss_price::float8,
                      p.trailing_stop_pct::float8,
                      p.entry_date
               FROM signal_strategy_positions p
               JOIN signal_strategy_accounts a ON a.id = p.account_id
               WHERE p.is_open = TRUE
               ORDER BY p.entry_date, p.id"#,
        )
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    account_id,
                    signal_id,
                    signal_name,
                    code,
                    name,
                    entry_price,
                    shares,
                    peak_price,
                    stop_loss_price,
                    trailing_stop_pct,
                    entry_date,
                )| OpenPositionRow {
                    name: name.unwrap_or_else(|| code.clone()),
                    id,
                    account_id,
                    signal_id,
                    signal_name,
                    code,
                    entry_price,
                    shares,
                    peak_price,
                    stop_loss_price,
                    trailing_stop_pct,
                    entry_date,
                },
            )
            .collect())
    }

    async fn skip_candidate(
        &self,
        candidate_id: i64,
        account_id: i64,
        signal_id: &str,
        code: &str,
        title: &str,
        detail: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE signal_strategy_candidates
               SET candidate_status = 'skipped',
                   entry_reason = $1,
                   updated_at = NOW()
               WHERE id = $2"#,
        )
        .bind(detail)
        .bind(candidate_id)
        .execute(&self.state.db)
        .await?;

        self.record_event(
            account_id,
            None,
            Some(candidate_id),
            signal_id,
            "skip",
            Some(code),
            title,
            detail,
        )
        .await?;
        self.push_telegram_message(&format!(
            "🟡 <b>{}</b>\n信号: {}\n{} {}",
            escape_html(title),
            escape_html(signal_id),
            escape_html(code),
            escape_html(detail)
        ))
        .await;
        Ok(())
    }

    async fn record_event(
        &self,
        account_id: i64,
        position_id: Option<i64>,
        candidate_id: Option<i64>,
        signal_id: &str,
        event_type: &str,
        code: Option<&str>,
        title: &str,
        detail: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO signal_strategy_events
               (account_id, position_id, candidate_id, signal_id, event_type, code, title, detail)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
        )
        .bind(account_id)
        .bind(position_id)
        .bind(candidate_id)
        .bind(signal_id)
        .bind(event_type)
        .bind(code)
        .bind(title)
        .bind(detail)
        .execute(&self.state.db)
        .await?;
        Ok(())
    }

    async fn push_telegram_message(&self, text: &str) {
        let Some(channel) = self.state.config.stock_alert_channel.as_ref().or(self
            .state
            .config
            .report_channel
            .as_ref())
        else {
            return;
        };

        if let Err(e) = self.state.pusher.push(channel, text).await {
            warn!("Signal auto trading telegram push failed: {}", e);
        }
    }
}

fn score_candidate(signal_id: &str, bars: &[Candle]) -> (f64, Vec<String>) {
    let today = match bars.last() {
        Some(v) => v,
        None => return (0.0, vec!["无K线数据".to_string()]),
    };
    let prev = match bars.get(bars.len().saturating_sub(2)) {
        Some(v) => v,
        None => return (0.0, vec!["K线样本不足".to_string()]),
    };

    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let ma5 = sma(&closes, 5).unwrap_or(today.close);
    let ma20 = sma(&closes, 20).unwrap_or(today.close);
    let ma60 = sma(&closes, 60).unwrap_or(ma20);
    let gain_pct = pct_change(prev.close, today.close);
    let body_pct = pct_change(today.open.max(0.01), today.close);
    let avg20 = avg_volume(&bars[..bars.len().saturating_sub(1)], 20).max(1.0);
    let vol_ratio = today.volume as f64 / avg20;
    let close_pos = if today.high > today.low {
        (today.close - today.low) / (today.high - today.low)
    } else {
        0.5
    };
    let recent_high = bars[bars.len().saturating_sub(21)..bars.len().saturating_sub(1)]
        .iter()
        .map(|b| b.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let breakout_pct = if recent_high.is_finite() && recent_high > 0.0 {
        (today.close - recent_high) / recent_high * 100.0
    } else {
        0.0
    };

    let mut score = 50.0;
    let mut reasons = Vec::new();

    score += close_pos * 14.0;
    score += vol_ratio.min(3.0) * 8.0;
    if today.close > ma5 {
        score += 5.0;
    }
    if today.close > ma20 {
        score += 6.0;
        reasons.push("站上MA20".to_string());
    }
    if today.close > ma60 {
        score += 4.0;
    }
    if vol_ratio >= 1.5 {
        reasons.push(format!("放量{:.1}x", vol_ratio));
    }
    if close_pos >= 0.75 {
        reasons.push("收盘靠近高点".to_string());
    }
    if breakout_pct > 0.0 {
        reasons.push(format!("突破前高{:.1}%", breakout_pct));
    }

    let heat_bonus = match signal_id {
        "startup" | "volume_surge" | "kuangbiao" | "breakout" | "uptrend_breakout"
        | "bottom_quick_start" | "low_accumulation" => triangular_bonus(gain_pct, 4.0, 2.0, 8.0),
        "ma_pullback" | "strong_pullback" | "strong_first_neg" | "fanbao" | "broken_board" => {
            triangular_bonus(gain_pct, 2.5, 0.5, 5.0)
        }
        "ma_bullish" | "slow_bull" | "weekly_monthly_bullish" | "linreg" | "volume_price" => {
            triangular_bonus(gain_pct, 2.0, 0.0, 4.5)
        }
        _ => triangular_bonus(gain_pct, 3.0, 0.5, 6.5),
    };
    score += heat_bonus;

    if body_pct > 0.0 {
        score += body_pct.min(4.0) * 1.5;
    }

    if today.close > ma20 * 1.12 {
        score -= 8.0;
        reasons.push("偏离均线过大".to_string());
    }
    if gain_pct > 8.0 {
        score -= ((gain_pct - 8.0) * 3.0).min(18.0);
        reasons.push("当日涨幅过热".to_string());
    }
    if reasons.is_empty() {
        reasons.push("形态与趋势综合评分领先".to_string());
    }

    (round2(score.max(0.0)), reasons)
}

fn build_signal_candidate(
    signal_id: &str,
    hit: &SignalHit,
    bars: &[Candle],
) -> Option<CandidateScore> {
    if bars.len() < 25 {
        return None;
    }

    let (overlay_score, score_reasons) = score_candidate(signal_id, bars);
    if signal_id.starts_with("pool_") {
        let pool_score = hit
            .metadata
            .get("score")
            .and_then(|value| value.as_f64())
            .unwrap_or(overlay_score);
        let pool_reasons = metadata_string_vec(&hit.metadata, "reasons");
        let risk_flags = metadata_string_vec(&hit.metadata, "risk_flags");
        let trigger_name = hit
            .metadata
            .get("trigger_name")
            .and_then(|value| value.as_str())
            .unwrap_or(hit.signal_name.as_str());

        let mut reasons = Vec::new();
        push_unique_reason(
            &mut reasons,
            format!("{} {}", hit.signal_name, trigger_name),
        );
        for reason in &pool_reasons {
            push_unique_reason(&mut reasons, reason);
        }
        for reason in score_reasons.iter().take(2) {
            push_unique_reason(&mut reasons, reason);
        }
        if reasons.is_empty() {
            reasons.push("池内综合排序领先".to_string());
        }

        return Some(CandidateScore {
            code: hit.code.clone(),
            name: hit.name.clone(),
            score: round2(pool_score * 0.8 + overlay_score * 0.2),
            selection_reason: reasons.join("；"),
            metadata: serde_json::json!({
                "signal_hit": hit,
                "pool_score": pool_score,
                "pool_reasons": pool_reasons,
                "risk_flags": risk_flags,
                "score_reasons": score_reasons,
            }),
        });
    }

    let selection_reason = score_reasons.join("；");
    Some(CandidateScore {
        code: hit.code.clone(),
        name: hit.name.clone(),
        score: overlay_score,
        selection_reason,
        metadata: serde_json::json!({
            "signal_hit": hit,
            "score_reasons": score_reasons,
        }),
    })
}

fn metadata_string_vec(metadata: &serde_json::Value, key: &str) -> Vec<String> {
    metadata
        .get(key)
        .and_then(|value| value.as_array())
        .into_iter()
        .flat_map(|items| items.iter())
        .filter_map(|item| item.as_str().map(ToString::to_string))
        .collect()
}

fn push_unique_reason(reasons: &mut Vec<String>, reason: impl AsRef<str>) {
    let value = reason.as_ref().trim();
    if value.is_empty() || reasons.iter().any(|item| item == value) {
        return;
    }
    reasons.push(value.to_string());
}

fn compute_max_drawdown_pct(
    initial_capital: f64,
    realized_pnl: f64,
    current_equity: f64,
    metrics: &[PositionMetricRow],
) -> f64 {
    if initial_capital <= 0.0 {
        return 0.0;
    }

    let mut peak = initial_capital;
    let mut equity = initial_capital;
    let mut max_drawdown: f64 = 0.0;

    for row in metrics.iter().filter(|row| !row.is_open) {
        let exit_price = row.exit_price.unwrap_or(row.entry_price);
        equity += (exit_price - row.entry_price) * row.shares as f64;
        if equity > peak {
            peak = equity;
        }
        if peak > 0.0 {
            max_drawdown = max_drawdown.max((peak - equity) / peak * 100.0);
        }
    }

    let realized_equity = initial_capital + realized_pnl;
    if realized_equity > peak {
        peak = realized_equity;
    }
    if peak > 0.0 {
        max_drawdown = max_drawdown.max((peak - current_equity) / peak * 100.0);
    }

    max_drawdown
}

fn compute_trade_streaks(metrics: &[PositionMetricRow]) -> (i64, i64) {
    let mut closed: Vec<&PositionMetricRow> = metrics
        .iter()
        .filter(|row| !row.is_open && row.exit_date.is_some())
        .collect();
    closed.sort_by(|a, b| {
        b.exit_date
            .cmp(&a.exit_date)
            .then_with(|| b.code.cmp(&a.code))
    });

    let mut win_streak = 0;
    let mut loss_streak = 0;

    for row in &closed {
        let pnl = row.pnl_pct.unwrap_or(0.0);
        if pnl > 0.0 {
            win_streak += 1;
        } else {
            break;
        }
    }

    for row in &closed {
        let pnl = row.pnl_pct.unwrap_or(0.0);
        if pnl < 0.0 {
            loss_streak += 1;
        } else {
            break;
        }
    }

    (win_streak, loss_streak)
}

fn format_signal_auto_daily_report(
    report_date: NaiveDate,
    accounts: &[StrategyAccountSnapshot],
    events: &[SignalAutoEventLine],
) -> String {
    let active_accounts = accounts
        .iter()
        .filter(|a| a.open_positions > 0 || a.pending_candidates > 0)
        .count();
    let mut lines = vec![
        format!("🤖 <b>信号自动交易日报 {}</b>", report_date),
        "━━━━━━━━━━━━━━━━━━━━━".to_string(),
        format!("账户数: {} | 活跃账户: {}", accounts.len(), active_accounts),
    ];

    append_daily_account_group(
        &mut lines,
        "🎯 分层票池",
        accounts,
        signal_auto_group_key_ranked_pool,
    );
    append_daily_account_group(
        &mut lines,
        "📈 普通信号",
        accounts,
        signal_auto_group_key_standard,
    );
    append_daily_account_group(
        &mut lines,
        "🌱 预启动",
        accounts,
        signal_auto_group_key_prestart,
    );
    append_daily_account_group(
        &mut lines,
        "🧱 自动打板",
        accounts,
        signal_auto_group_key_daban,
    );
    append_daily_account_group(
        &mut lines,
        "💪 自动强势股",
        accounts,
        signal_auto_group_key_strong,
    );

    if !events.is_empty() {
        lines.push(String::new());
        lines.push("<b>今日动作</b>".to_string());
        append_daily_event_group(
            &mut lines,
            "🎯 分层票池",
            events,
            signal_auto_group_key_ranked_pool,
        );
        append_daily_event_group(
            &mut lines,
            "📈 普通信号",
            events,
            signal_auto_group_key_standard,
        );
        append_daily_event_group(
            &mut lines,
            "🌱 预启动",
            events,
            signal_auto_group_key_prestart,
        );
        append_daily_event_group(
            &mut lines,
            "🧱 自动打板",
            events,
            signal_auto_group_key_daban,
        );
        append_daily_event_group(
            &mut lines,
            "💪 自动强势股",
            events,
            signal_auto_group_key_strong,
        );
    }

    lines.join("\n")
}

fn append_daily_account_group(
    lines: &mut Vec<String>,
    title: &str,
    accounts: &[StrategyAccountSnapshot],
    predicate: fn(&str) -> bool,
) {
    let group: Vec<&StrategyAccountSnapshot> = accounts
        .iter()
        .filter(|account| predicate(&account.signal_id))
        .collect();
    if group.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push(format!("<b>{}</b>", title));
    let active_accounts = group
        .iter()
        .filter(|account| account.open_positions > 0 || account.pending_candidates > 0)
        .count();
    let total_equity: f64 = group.iter().map(|account| account.equity).sum();
    let total_realized: f64 = group.iter().map(|account| account.realized_pnl).sum();
    let total_unrealized: f64 = group.iter().map(|account| account.unrealized_pnl).sum();
    let total_weekly_pnl: f64 = group.iter().map(|account| account.weekly_pnl).sum();
    let avg_pnl_pct = if group.is_empty() {
        0.0
    } else {
        group.iter().map(|account| account.pnl_pct).sum::<f64>() / group.len() as f64
    };
    let total_closed_trades: i64 = group.iter().map(|account| account.closed_trades).sum();
    let total_winning_trades: i64 = group.iter().map(|account| account.winning_trades).sum();
    let win_rate = if total_closed_trades > 0 {
        total_winning_trades as f64 / total_closed_trades as f64 * 100.0
    } else {
        0.0
    };
    lines.push(format!(
        "账户 {} | 活跃 {} | 总权益 {:.0} | 平均收益 {:+.2}%",
        group.len(),
        active_accounts,
        total_equity,
        avg_pnl_pct
    ));
    lines.push(format!(
        "已实现 {:+.0} | 未实现 {:+.0} | 胜率 {:.1}% ({}/{})",
        total_realized, total_unrealized, win_rate, total_winning_trades, total_closed_trades
    ));
    lines.push(format!("本周收益 {:+.0}", total_weekly_pnl));
    for account in group.iter().take(12) {
        lines.push(format!(
            "• <b>{}</b> 资金 {:.0} | 持仓 {} | 待买 {} | 收益 {:+.2}% | 周收益 {:+.0} | 回撤 {:.2}% | 连胜 {} | 连亏 {}",
            account.signal_name,
            account.cash_balance,
            account.open_positions,
            account.pending_candidates,
            account.pnl_pct,
            account.weekly_pnl,
            account.max_drawdown_pct,
            account.win_streak,
            account.loss_streak
        ));
    }
}

fn append_daily_event_group(
    lines: &mut Vec<String>,
    title: &str,
    events: &[SignalAutoEventLine],
    predicate: fn(&str) -> bool,
) {
    let group: Vec<&SignalAutoEventLine> = events
        .iter()
        .filter(|event| predicate(&event.signal_id))
        .collect();
    if group.is_empty() {
        return;
    }

    lines.push(format!("<b>{}</b>", title));
    for event in group.iter().take(8) {
        lines.push(format!(
            "• {} {}: {}",
            event.signal_name, event.title, event.detail
        ));
    }
}

fn signal_auto_group_key_standard(signal_id: &str) -> bool {
    signal_id != AUTO_DABAN_ID
        && signal_id != AUTO_STRONG_ID
        && !is_ranked_pool_signal(signal_id)
        && !is_prestart_signal(signal_id)
}

fn signal_auto_group_key_ranked_pool(signal_id: &str) -> bool {
    is_ranked_pool_signal(signal_id)
}

fn signal_auto_group_key_prestart(signal_id: &str) -> bool {
    is_prestart_signal(signal_id)
}

fn signal_auto_group_key_daban(signal_id: &str) -> bool {
    signal_id == AUTO_DABAN_ID
}

fn signal_auto_group_key_strong(signal_id: &str) -> bool {
    signal_id == AUTO_STRONG_ID
}

fn score_daban_candidate(item: &DabanScore, bars: &[Candle]) -> (f64, Vec<String>) {
    let today = match bars.last() {
        Some(v) => v,
        None => return (0.0, vec!["无K线数据".to_string()]),
    };
    let prev = match bars.get(bars.len().saturating_sub(2)) {
        Some(v) => v,
        None => return (0.0, vec!["K线样本不足".to_string()]),
    };
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let ma5 = sma(&closes, 5).unwrap_or(today.close);
    let ma20 = sma(&closes, 20).unwrap_or(today.close);
    let avg10 = avg_volume(&bars[..bars.len().saturating_sub(1)], 10).max(1.0);
    let vol_ratio = today.volume as f64 / avg10;
    let close_pos = if today.high > today.low {
        (today.close - today.low) / (today.high - today.low)
    } else {
        0.5
    };
    let gain_pct = pct_change(prev.close, today.close);

    let mut score = item.score + close_pos * 10.0 + vol_ratio.min(2.5) * 6.0;
    let mut reasons = vec![
        format!("打板分 {:.1} {}", item.score, item.verdict),
        format!("执行性 {}", item.executability),
    ];

    if today.close > ma5 {
        score += 4.0;
    }
    if today.close > ma20 {
        score += 6.0;
        reasons.push("站上MA20".to_string());
    }
    if close_pos >= 0.7 {
        reasons.push("收盘承接较强".to_string());
    }
    if vol_ratio >= 1.2 {
        reasons.push(format!("量能 {:.1}x", vol_ratio));
    }
    score += triangular_bonus(gain_pct, 6.0, 3.0, 11.0);

    if today.close > ma20 * 1.18 {
        score -= 10.0;
        reasons.push("次日接力位置偏高".to_string());
    }
    if item.burst_penalty >= 10.0 {
        score -= 6.0;
        reasons.push("炸板偏多".to_string());
    }

    (round2(score.max(0.0)), reasons)
}

fn score_strong_candidate(
    item: &StrongLimitUpStock,
    target_date: NaiveDate,
    bars: &[Candle],
) -> (f64, Vec<String>) {
    let today = match bars.last() {
        Some(v) => v,
        None => return (0.0, vec!["无K线数据".to_string()]),
    };
    let prev = match bars.get(bars.len().saturating_sub(2)) {
        Some(v) => v,
        None => return (0.0, vec!["K线样本不足".to_string()]),
    };
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let ma5 = sma(&closes, 5).unwrap_or(today.close);
    let ma20 = sma(&closes, 20).unwrap_or(today.close);
    let ma60 = sma(&closes, 60).unwrap_or(ma20);
    let avg10 = avg_volume(&bars[..bars.len().saturating_sub(1)], 10).max(1.0);
    let vol_ratio = today.volume as f64 / avg10;
    let close_pos = if today.high > today.low {
        (today.close - today.low) / (today.high - today.low)
    } else {
        0.5
    };
    let gain_pct = pct_change(prev.close, today.close);
    let recent_high = bars[bars.len().saturating_sub(21)..bars.len().saturating_sub(1)]
        .iter()
        .map(|b| b.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let gap_to_high_pct = if recent_high.is_finite() && recent_high > 0.0 {
        (recent_high - today.close) / recent_high * 100.0
    } else {
        99.0
    };
    let recency_days = (target_date - item.latest_trade_date).num_days().max(0) as f64;

    let mut score = item.limit_count as f64 * 18.0;
    let mut reasons = vec![format!(
        "{}日{}板 最近涨停 {}",
        STRONG_WINDOW_DAYS, item.limit_count, item.latest_trade_date
    )];

    score += (10.0 - recency_days * 3.0).max(0.0);
    score += close_pos * 10.0;
    score += vol_ratio.min(2.0) * 6.0;
    score += triangular_bonus(gain_pct, 2.5, -1.0, 6.0);

    if today.close > ma5 {
        score += 4.0;
    }
    if today.close > ma20 {
        score += 6.0;
        reasons.push("站上MA20".to_string());
    }
    if today.close > ma60 {
        score += 4.0;
    }
    if close_pos >= 0.7 {
        reasons.push("收盘强于日内中枢".to_string());
    }
    if gap_to_high_pct >= 0.0 && gap_to_high_pct <= 4.0 {
        score += 8.0;
        reasons.push(format!("距前高 {:.1}%", gap_to_high_pct));
    }

    if today.close > ma20 * 1.15 {
        score -= 10.0;
        reasons.push("偏离MA20过大".to_string());
    }
    if gain_pct > 7.5 {
        score -= 8.0;
        reasons.push("当日涨幅偏热".to_string());
    }
    if recency_days > 3.0 {
        reasons.push("最近涨停时间偏远".to_string());
    }

    (round2(score.max(0.0)), reasons)
}

fn evaluate_entry_signal(now: DateTime<FixedOffset>, quote: &Quote) -> EntryDecision {
    let minutes = now.hour() * 60 + now.minute();
    if minutes < BUY_START_MINUTE {
        return EntryDecision {
            buy_now: false,
            reason: "仍在开盘噪音阶段".to_string(),
        };
    }
    if minutes > BUY_END_MINUTE {
        return EntryDecision {
            buy_now: false,
            reason: "已过日内买入窗口".to_string(),
        };
    }
    if quote.prev_close <= 0.0 || quote.price <= 0.0 {
        return EntryDecision {
            buy_now: false,
            reason: "行情价格无效".to_string(),
        };
    }

    let intraday_gain = pct_change(quote.prev_close, quote.price);
    if quote.price < quote.open.max(quote.prev_close) {
        return EntryDecision {
            buy_now: false,
            reason: "价格未站稳昨收与今开".to_string(),
        };
    }
    if intraday_gain < 0.3 {
        return EntryDecision {
            buy_now: false,
            reason: "承接力度不足".to_string(),
        };
    }
    if intraday_gain > 3.5 {
        return EntryDecision {
            buy_now: false,
            reason: "盘中过度追高".to_string(),
        };
    }

    let range_strength = if quote.high > quote.low {
        (quote.price - quote.low) / (quote.high - quote.low)
    } else {
        1.0
    };
    if range_strength < 0.55 {
        return EntryDecision {
            buy_now: false,
            reason: "价格未处于日内强势区".to_string(),
        };
    }

    EntryDecision {
        buy_now: true,
        reason: format!(
            "09:35后承接确认，现价站上昨收/今开，涨幅{:+.2}%，未超过追高阈值",
            intraday_gain
        ),
    }
}

fn evaluate_exit_signal(
    today: NaiveDate,
    entry_date: NaiveDate,
    quote: &Quote,
    entry_price: f64,
    peak_price: f64,
    stop_loss_price: f64,
    trailing_stop_pct: f64,
) -> ExitDecision {
    if entry_date >= today {
        return ExitDecision {
            sell_now: false,
            reason: "A股T+1，不允许当日卖出".to_string(),
        };
    }
    if quote.price <= 0.0 {
        return ExitDecision {
            sell_now: false,
            reason: "行情价格无效".to_string(),
        };
    }
    if quote.price <= stop_loss_price {
        return ExitDecision {
            sell_now: true,
            reason: format!(
                "触发-5%硬止损，现价 {:.2} 跌破 {:.2}",
                quote.price, stop_loss_price
            ),
        };
    }

    if peak_price > entry_price * 1.01 {
        let trailing_line = peak_price * (1.0 - trailing_stop_pct / 100.0);
        if quote.price <= trailing_line {
            return ExitDecision {
                sell_now: true,
                reason: format!(
                    "触发{:.1}%移动止盈，现价 {:.2} 跌破 {:.2}",
                    trailing_stop_pct, quote.price, trailing_line
                ),
            };
        }
    }

    ExitDecision {
        sell_now: false,
        reason: "未触发卖出条件".to_string(),
    }
}

fn triangular_bonus(value: f64, center: f64, min: f64, max: f64) -> f64 {
    if value < min || value > max {
        return 0.0;
    }
    let width = (center - min).max(max - center).max(0.01);
    let distance = (value - center).abs();
    (1.0 - distance / width).max(0.0) * 12.0
}

fn pct_change(base: f64, value: f64) -> f64 {
    if base.abs() < f64::EPSILON {
        0.0
    } else {
        (value - base) / base * 100.0
    }
}

fn board_lot_shares(cash_balance: f64, price: f64) -> i32 {
    if price <= 0.0 {
        return 0;
    }
    let lots = (cash_balance / (price * 100.0)).floor() as i32;
    lots.max(0) * 100
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn candle(open: f64, high: f64, low: f64, close: f64, volume: i64) -> Candle {
        Candle {
            trade_date: NaiveDate::from_ymd_opt(2026, 3, 9).unwrap(),
            open,
            high,
            low,
            close,
            volume,
            amount: volume as f64 * close,
            turnover: None,
            pe: None,
            pb: None,
        }
    }

    #[test]
    fn entry_signal_requires_strength_without_chasing() {
        let tz = crate::market_time::beijing_tz();
        let now = tz.with_ymd_and_hms(2026, 3, 9, 10, 5, 0).unwrap();
        let quote = Quote {
            code: "600000.SH".to_string(),
            name: "Test".to_string(),
            price: 10.15,
            open: 10.02,
            high: 10.20,
            low: 9.98,
            prev_close: 10.0,
            change_pct: 1.5,
            volume: 1_000_000,
            amount: 10_150_000.0,
            timestamp: Utc::now().naive_utc(),
        };
        assert!(evaluate_entry_signal(now, &quote).buy_now);

        let chased = Quote {
            price: 10.60,
            ..quote.clone()
        };
        assert!(!evaluate_entry_signal(now, &chased).buy_now);
    }

    #[test]
    fn exit_signal_enforces_t_plus_one_and_trailing_stop() {
        let quote = Quote {
            code: "600000.SH".to_string(),
            name: "Test".to_string(),
            price: 10.55,
            open: 10.8,
            high: 11.0,
            low: 10.5,
            prev_close: 10.9,
            change_pct: -3.2,
            volume: 1_000_000,
            amount: 10_550_000.0,
            timestamp: Utc::now().naive_utc(),
        };

        let same_day = evaluate_exit_signal(
            NaiveDate::from_ymd_opt(2026, 3, 10).unwrap(),
            NaiveDate::from_ymd_opt(2026, 3, 10).unwrap(),
            &quote,
            10.0,
            11.0,
            9.5,
            3.5,
        );
        assert!(!same_day.sell_now);

        let next_day = evaluate_exit_signal(
            NaiveDate::from_ymd_opt(2026, 3, 11).unwrap(),
            NaiveDate::from_ymd_opt(2026, 3, 10).unwrap(),
            &quote,
            10.0,
            11.0,
            9.5,
            3.5,
        );
        assert!(next_day.sell_now);
    }

    #[test]
    fn candidate_score_rewards_volume_and_close_strength() {
        let mut bars = Vec::new();
        for _ in 0..30 {
            bars.push(candle(10.0, 10.3, 9.9, 10.1, 1_000_000));
        }
        bars.pop();
        bars.push(candle(10.0, 10.9, 9.95, 10.85, 3_200_000));

        let (score, reasons) = score_candidate("startup", &bars);
        assert!(score > 60.0);
        assert!(!reasons.is_empty());
    }

    #[test]
    fn build_signal_candidate_wraps_scan_hit_for_standard_strategy() {
        let mut bars = Vec::new();
        for _ in 0..30 {
            bars.push(candle(10.0, 10.3, 9.9, 10.1, 1_000_000));
        }
        bars.pop();
        bars.push(candle(10.0, 10.9, 9.95, 10.85, 3_200_000));

        let hit = SignalHit {
            code: "600010.SH".to_string(),
            name: "包钢股份".to_string(),
            signal_id: "startup".to_string(),
            signal_name: "底部快速启动".to_string(),
            icon: "🚀".to_string(),
            metadata: serde_json::json!({"source": "unit-test"}),
        };

        let candidate =
            build_signal_candidate("startup", &hit, &bars).expect("expected signal candidate");

        assert_eq!(candidate.code, "600010.SH");
        assert_eq!(candidate.name, "包钢股份");
        assert!(candidate.score > 60.0);
        assert!(!candidate.selection_reason.is_empty());
        assert!(candidate.metadata.to_string().contains("unit-test"));
    }

    #[test]
    fn build_signal_candidate_prefers_ranked_pool_score_and_reasons() {
        let mut bars = Vec::new();
        for _ in 0..30 {
            bars.push(candle(10.0, 10.4, 9.9, 10.2, 1_200_000));
        }
        bars.pop();
        bars.push(candle(10.2, 10.95, 10.1, 10.88, 3_000_000));

        let hit = SignalHit {
            code: "300001.SZ".to_string(),
            name: "特锐德".to_string(),
            signal_id: "pool_short_a".to_string(),
            signal_name: "短线A档".to_string(),
            icon: "🔥".to_string(),
            metadata: serde_json::json!({
                "score": 91.0,
                "trigger_name": "强势分歧转强",
                "reasons": ["强势票分歧后重新转强", "收盘已突破短期压力"],
                "risk_flags": ["量能不足"]
            }),
        };

        let candidate = build_signal_candidate("pool_short_a", &hit, &bars)
            .expect("expected ranked pool candidate");

        assert!(candidate.score >= 85.0);
        assert!(candidate.selection_reason.contains("强势分歧转强"));
        assert!(candidate.selection_reason.contains("强势票分歧后重新转强"));
        assert!(candidate.metadata.to_string().contains("risk_flags"));
        assert!(candidate.metadata.to_string().contains("pool_short_a"));
    }

    #[test]
    fn builtin_ranked_pool_accounts_include_short_mid_long_a_lines() {
        let accounts = builtin_ranked_pool_accounts();

        assert_eq!(accounts.len(), 3);
        assert!(accounts.contains(&(POOL_SHORT_A_ID, "短线A档")));
        assert!(accounts.contains(&(POOL_MID_A_ID, "中线A档")));
        assert!(accounts.contains(&(POOL_LONG_A_ID, "长线A档")));
    }

    #[test]
    fn daban_candidate_score_rewards_tradeable_board_strength() {
        let mut bars = Vec::new();
        for _ in 0..30 {
            bars.push(candle(10.0, 10.5, 9.9, 10.2, 1_000_000));
        }
        bars.pop();
        bars.push(candle(10.3, 11.1, 10.2, 11.0, 2_200_000));

        let item = DabanScore {
            code: "600000.SH".to_string(),
            name: "Test".to_string(),
            score: 72.0,
            seal_score: 20.0,
            time_score: 18.0,
            burst_penalty: 0.0,
            executability: "可打".to_string(),
            verdict: "推荐".to_string(),
        };

        let (score, reasons) = score_daban_candidate(&item, &bars);
        assert!(score > 80.0);
        assert!(reasons.iter().any(|r| r.contains("打板分")));
    }

    #[test]
    fn strong_candidate_score_rewards_recent_multi_limit_leaders() {
        let mut bars = Vec::new();
        for _ in 0..30 {
            bars.push(candle(10.0, 10.4, 9.9, 10.1, 1_000_000));
        }
        bars.pop();
        bars.push(candle(10.15, 10.85, 10.1, 10.78, 1_800_000));

        let item = StrongLimitUpStock {
            code: "600001.SH".to_string(),
            name: "Leader".to_string(),
            limit_count: 4,
            latest_trade_date: NaiveDate::from_ymd_opt(2026, 3, 9).unwrap(),
        };

        let (score, reasons) =
            score_strong_candidate(&item, NaiveDate::from_ymd_opt(2026, 3, 9).unwrap(), &bars);
        assert!(score > 80.0);
        assert!(reasons.iter().any(|r| r.contains("4板")));
    }

    #[test]
    fn daily_report_groups_prestart_daban_and_strong_sections() {
        let report = format_signal_auto_daily_report(
            NaiveDate::from_ymd_opt(2026, 3, 9).unwrap(),
            &[
                StrategyAccountSnapshot {
                    signal_id: "startup".to_string(),
                    signal_name: "底部快速启动".to_string(),
                    cash_balance: 100_000.0,
                    initial_capital: 100_000.0,
                    open_positions: 0,
                    pending_candidates: 1,
                    equity: 100_000.0,
                    pnl_pct: 0.0,
                    realized_pnl: 0.0,
                    unrealized_pnl: 0.0,
                    closed_trades: 0,
                    winning_trades: 0,
                    weekly_pnl: 0.0,
                    max_drawdown_pct: 1.0,
                    win_streak: 0,
                    loss_streak: 0,
                },
                StrategyAccountSnapshot {
                    signal_id: "ma_bullish".to_string(),
                    signal_name: "均线多头".to_string(),
                    cash_balance: 100_000.0,
                    initial_capital: 100_000.0,
                    open_positions: 0,
                    pending_candidates: 1,
                    equity: 100_000.0,
                    pnl_pct: 0.0,
                    realized_pnl: 0.0,
                    unrealized_pnl: 0.0,
                    closed_trades: 0,
                    winning_trades: 0,
                    weekly_pnl: 0.0,
                    max_drawdown_pct: 1.5,
                    win_streak: 0,
                    loss_streak: 0,
                },
                StrategyAccountSnapshot {
                    signal_id: "auto_daban".to_string(),
                    signal_name: "自动打板".to_string(),
                    cash_balance: 96_000.0,
                    initial_capital: 100_000.0,
                    open_positions: 1,
                    pending_candidates: 0,
                    equity: 101_000.0,
                    pnl_pct: 1.0,
                    realized_pnl: 800.0,
                    unrealized_pnl: 200.0,
                    closed_trades: 2,
                    winning_trades: 1,
                    weekly_pnl: 350.0,
                    max_drawdown_pct: 3.8,
                    win_streak: 1,
                    loss_streak: 0,
                },
                StrategyAccountSnapshot {
                    signal_id: "auto_strong".to_string(),
                    signal_name: "自动强势股".to_string(),
                    cash_balance: 100_000.0,
                    initial_capital: 100_000.0,
                    open_positions: 0,
                    pending_candidates: 1,
                    equity: 99_000.0,
                    pnl_pct: -1.0,
                    realized_pnl: -1000.0,
                    unrealized_pnl: 0.0,
                    closed_trades: 1,
                    winning_trades: 0,
                    weekly_pnl: -600.0,
                    max_drawdown_pct: 5.2,
                    win_streak: 0,
                    loss_streak: 1,
                },
            ],
            &[
                SignalAutoEventLine {
                    signal_id: "startup".to_string(),
                    signal_name: "底部快速启动".to_string(),
                    title: "候选入池".to_string(),
                    detail: "候选 Delta".to_string(),
                },
                SignalAutoEventLine {
                    signal_id: "ma_bullish".to_string(),
                    signal_name: "均线多头".to_string(),
                    title: "B档观察".to_string(),
                    detail: "观察 Alpha".to_string(),
                },
                SignalAutoEventLine {
                    signal_id: "auto_daban".to_string(),
                    signal_name: "自动打板".to_string(),
                    title: "候选入池".to_string(),
                    detail: "候选 Beta".to_string(),
                },
                SignalAutoEventLine {
                    signal_id: "auto_strong".to_string(),
                    signal_name: "自动强势股".to_string(),
                    title: "候选入池".to_string(),
                    detail: "候选 Gamma".to_string(),
                },
            ],
        );

        assert!(report.contains("📈 普通信号"));
        assert!(report.contains("🌱 预启动"));
        assert!(report.contains("🧱 自动打板"));
        assert!(report.contains("💪 自动强势股"));
        assert!(report.contains("账户 1 | 活跃 1"));
        assert!(report.contains("总权益 101000"));
        assert!(report.contains("已实现 +800 | 未实现 +200 | 胜率 50.0% (1/2)"));
        assert!(report.contains("本周收益 +350"));
        assert!(report.contains("回撤 3.80% | 连胜 1 | 连亏 0"));
        assert!(report.contains("底部快速启动 候选入池"));
        assert!(report.contains("均线多头 B档观察"));
        assert!(report.contains("自动打板 候选入池"));
        assert!(report.contains("自动强势股 候选入池"));
    }

    #[test]
    fn daily_report_groups_ranked_pool_accounts_separately() {
        let report = format_signal_auto_daily_report(
            NaiveDate::from_ymd_opt(2026, 3, 9).unwrap(),
            &[
                StrategyAccountSnapshot {
                    signal_id: POOL_SHORT_A_ID.to_string(),
                    signal_name: "短线A档".to_string(),
                    cash_balance: 101_000.0,
                    initial_capital: 100_000.0,
                    open_positions: 1,
                    pending_candidates: 0,
                    equity: 103_000.0,
                    pnl_pct: 3.0,
                    realized_pnl: 1500.0,
                    unrealized_pnl: 1500.0,
                    closed_trades: 2,
                    winning_trades: 2,
                    weekly_pnl: 1000.0,
                    max_drawdown_pct: 2.5,
                    win_streak: 2,
                    loss_streak: 0,
                },
                StrategyAccountSnapshot {
                    signal_id: "startup".to_string(),
                    signal_name: "底部快速启动".to_string(),
                    cash_balance: 100_000.0,
                    initial_capital: 100_000.0,
                    open_positions: 0,
                    pending_candidates: 1,
                    equity: 100_200.0,
                    pnl_pct: 0.2,
                    realized_pnl: 200.0,
                    unrealized_pnl: 0.0,
                    closed_trades: 1,
                    winning_trades: 1,
                    weekly_pnl: 100.0,
                    max_drawdown_pct: 1.0,
                    win_streak: 1,
                    loss_streak: 0,
                },
            ],
            &[SignalAutoEventLine {
                signal_id: POOL_SHORT_A_ID.to_string(),
                signal_name: "短线A档".to_string(),
                title: "候选入池".to_string(),
                detail: "候选 PoolAlpha".to_string(),
            }],
        );

        assert!(report.contains("🎯 分层票池"));
        assert!(report.contains("短线A档 候选入池: 候选 PoolAlpha"));
        assert!(report.contains("📈 普通信号"));
    }
}
