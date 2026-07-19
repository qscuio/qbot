mod analysis;
mod api;
mod config;
mod data;
mod error;
mod market_time;
mod scheduler;
mod services;
mod signals;
mod state;
mod storage;
mod telegram;

use crate::data::provider::DataProvider;
use anyhow::Result;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

fn api_bind_addr(api_port: u16) -> std::net::SocketAddr {
    std::net::SocketAddr::from(([127, 0, 0, 1], api_port))
}

fn repair_daily_bars_requested(args: impl IntoIterator<Item = String>) -> bool {
    args.into_iter().any(|arg| arg == "--repair-daily-bars")
}

fn repair_company_intelligence_requested(args: impl IntoIterator<Item = String>) -> bool {
    args.into_iter()
        .any(|arg| arg == "--repair-company-intelligence")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairMode {
    None,
    DailyBars,
    CompanyIntelligence,
}

fn repair_mode(args: impl IntoIterator<Item = String>) -> Result<RepairMode> {
    let args = args.into_iter().collect::<Vec<_>>();
    let daily_bars = repair_daily_bars_requested(args.iter().cloned());
    let company_intelligence = repair_company_intelligence_requested(args.iter().cloned());

    match (daily_bars, company_intelligence) {
        (true, true) => anyhow::bail!(
            "--repair-daily-bars and --repair-company-intelligence are mutually exclusive"
        ),
        (true, false) => Ok(RepairMode::DailyBars),
        (false, true) => Ok(RepairMode::CompanyIntelligence),
        (false, false) => Ok(RepairMode::None),
    }
}

async fn run_company_intelligence_repair<Financials, FinancialFuture, Dividends, DividendFuture>(
    financials: Financials,
    dividends: Dividends,
) -> Result<()>
where
    Financials: FnOnce() -> FinancialFuture,
    FinancialFuture:
        Future<Output = crate::error::Result<services::company_intelligence::CompanySyncReport>>,
    Dividends: FnOnce() -> DividendFuture,
    DividendFuture:
        Future<Output = crate::error::Result<services::company_intelligence::CompanySyncReport>>,
{
    let financials = financials().await;
    let financial_error = match financials {
        Ok(report) => {
            info!(
                "Company financial repair finished: completed={}, failed={}, pending={}",
                report.completed, report.failed, report.pending
            );
            (report.failed > 0 || report.pending > 0).then(|| {
                format!(
                    "financial repair incomplete: completed={}, failed={}, pending={}",
                    report.completed, report.failed, report.pending
                )
            })
        }
        Err(error) => {
            warn!("Company financial repair failed: {}", error);
            Some(error.to_string())
        }
    };

    let dividends = dividends().await;
    let dividend_error = match dividends {
        Ok(report) => {
            info!(
                "Company dividend repair finished: completed={}, failed={}, pending={}",
                report.completed, report.failed, report.pending
            );
            (report.failed > 0 || report.pending > 0).then(|| {
                format!(
                    "dividend repair incomplete: completed={}, failed={}, pending={}",
                    report.completed, report.failed, report.pending
                )
            })
        }
        Err(error) => {
            warn!("Company dividend repair failed: {}", error);
            Some(error.to_string())
        }
    };

    match (financial_error, dividend_error) {
        (None, None) => Ok(()),
        (Some(financial_error), None) => {
            anyhow::bail!("company intelligence repair failed: {financial_error}")
        }
        (None, Some(dividend_error)) => {
            anyhow::bail!("company intelligence repair failed: {dividend_error}")
        }
        (Some(financial_error), Some(dividend_error)) => anyhow::bail!(
            "company intelligence repair failed: financials: {financial_error}; dividends: {dividend_error}"
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let repair_mode = repair_mode(std::env::args())?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "qbot=info,sqlx=warn,tokio_cron_scheduler=warn".into()),
        )
        .init();

    let repair_daily_bars = repair_mode == RepairMode::DailyBars;
    let repair_company_intelligence = repair_mode == RepairMode::CompanyIntelligence;
    info!("qbot starting...");

    let config = config::Config::from_env()?;
    let api_port = config.api_port;
    info!("Config loaded (port: {})", api_port);

    // PostgreSQL
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;
    storage::postgres::run_migrations(&db).await?;
    info!("PostgreSQL connected + migrations applied");

    // Redis
    let redis_client = redis::Client::open(config.redis_url.as_str())?;
    let redis = redis::aio::ConnectionManager::new(redis_client).await?;
    info!("Redis connected");

    // Initialize signal registry
    signals::registry::SignalRegistry::init();

    // Data providers (tushare primary, eastmoney + tencent secondary, db fallback) and Telegram pusher
    let tushare_client = Arc::new(data::tushare::TushareClient::new(
        config.tushare_token.clone(),
        config.data_proxy.as_deref(),
    ));
    let tushare_provider: Arc<dyn DataProvider> = tushare_client.clone();
    let repair_provider = tushare_provider.clone();
    let company_repair_provider = tushare_client.clone();
    let point_in_time_provider: Arc<dyn data::point_in_time_provider::PointInTimeDataProvider> =
        tushare_client.clone();
    let eastmoney_provider: Arc<dyn DataProvider> = Arc::new(
        data::eastmoney::EastmoneyProvider::new(config.data_proxy.as_deref()),
    );
    let tencent_provider: Arc<dyn DataProvider> = Arc::new(data::tencent::TencentProvider::new(
        config.data_proxy.as_deref(),
    ));
    let db_provider: Arc<dyn DataProvider> = Arc::new(data::db::DbDataProvider::new(db.clone()));
    let provider: Arc<dyn DataProvider> =
        Arc::new(data::fallback::FallbackDataProvider::new(vec![
            tushare_provider,
            eastmoney_provider,
            tencent_provider,
            db_provider,
        ]));
    info!("Data providers configured: tushare -> eastmoney -> tencent -> db fallback");
    let pusher = Arc::new(telegram::TelegramPusher::new(
        config.telegram_bot_token.clone(),
    ));
    let sina_client = Arc::new(data::sina::SinaClient::new());
    let bot_commands = [
        ("start", "显示帮助"),
        ("help", "显示帮助"),
        ("menu", "打开按钮菜单"),
        ("scan", "扫描信号"),
        ("prestart", "预启动候选"),
        ("scan_stats", "信号统计"),
        ("daily_scan_stats", "每日归档统计"),
        ("autosim", "自动交易状态"),
        ("autosim_report", "自动交易日报"),
        ("sim", "普通模拟交易"),
        ("daban", "打板评分"),
        ("event", "提交市场事件"),
        ("events", "查看最新事件"),
        ("event_detail", "查看事件详情"),
        ("event_review", "复核发布事件"),
        ("market_facts", "查看市场事实简报"),
        ("decision", "查看决策支持日报"),
        ("decision_detail", "查看决策支持详情"),
        ("limitup", "涨停追踪概览"),
        ("strong", "近期强势股"),
        ("startup", "启动追踪"),
        ("limitup_sync", "同步涨停数据"),
        ("limitup_report", "最新涨停股报告"),
        ("strong_report", "最新强势股报告"),
        ("watch", "添加/查看自选"),
        ("unwatch", "删除自选"),
        ("mywatch", "查看自选"),
        ("port", "持仓管理"),
        ("industry", "行业板块"),
        ("concept", "概念板块"),
        ("hot7", "7日热门板块"),
        ("hot14", "14日热门板块"),
        ("hot30", "30日热门板块"),
        ("history", "查看历史K线"),
        ("chart", "查看图表接口"),
        ("dbcheck", "检查数据库"),
        ("dbsync", "同步今日数据"),
    ];
    if !repair_daily_bars && !repair_company_intelligence {
        match pusher.set_my_commands(&bot_commands).await {
            Ok(_) => info!("Telegram bot commands registered"),
            Err(e) => warn!("Telegram setMyCommands failed: {}", e),
        }
    }

    let state = Arc::new(state::AppState {
        config: Arc::new(config.clone()),
        db,
        redis,
        provider: provider.clone(),
        point_in_time_provider,
        pusher: pusher.clone(),
        fetch_job_lock: Arc::new(Mutex::new(())),
        analysis_job_lock: Arc::new(Mutex::new(())),
        scan_job_lock: Arc::new(Mutex::new(())),
        daily_report_job_lock: Arc::new(Mutex::new(())),
        weekly_report_job_lock: Arc::new(Mutex::new(())),
    });

    if repair_daily_bars {
        let service = services::stock_history::StockHistoryService::new(state, repair_provider);
        let report = service.repair_invalid_daily_bars().await?;
        info!(
            "Daily bar repair finished: attempted_dates={}, repaired_dates={}, failed_dates={}, remaining_rows={}",
            report.attempted_dates,
            report.repaired_dates,
            report.failed_dates.len(),
            report.remaining_rows
        );
        if !report.failed_dates.is_empty() || report.remaining_rows > 0 {
            anyhow::bail!(
                "daily bar repair incomplete: {} dates failed and {} invalid rows remain; rerun the workflow to resume",
                report.failed_dates.len(),
                report.remaining_rows
            );
        }
        return Ok(());
    }

    if repair_company_intelligence {
        let service = services::company_intelligence::CompanyIntelligenceService::new(
            state.db.clone(),
            company_repair_provider,
        );
        run_company_intelligence_repair(
            || service.backfill_financials(),
            || service.backfill_dividends(),
        )
        .await?;
        return Ok(());
    }

    {
        let probe_provider = state.point_in_time_provider.clone();
        let repo = storage::market_repository::MarketRepository::new(state.db.clone());
        tokio::spawn(async move {
            let capability_probe = probe_provider.probe_capabilities().await;
            if let Err(e) = repo
                .persist_point_in_time_capability_probe(&capability_probe)
                .await
            {
                warn!("Failed to persist point-in-time capability probe: {}", e);
            }
            match &capability_probe {
                Ok(capabilities) => {
                    let missing: Vec<&str> = [
                        (
                            "security_master_history",
                            capabilities.security_master_history,
                        ),
                        ("corporate_actions", capabilities.corporate_actions),
                        ("adjustment_factors", capabilities.adjustment_factors),
                        ("daily_basic", capabilities.daily_basic),
                        ("daily_security_status", capabilities.daily_security_status),
                        ("historical_index_bars", capabilities.historical_index_bars),
                        (
                            "historical_sector_membership",
                            capabilities.historical_sector_membership,
                        ),
                    ]
                    .into_iter()
                    .filter_map(|(name, supported)| (!supported).then_some(name))
                    .collect();
                    if missing.is_empty() {
                        info!("Point-in-time data capability probe completed");
                    } else {
                        warn!(
                            "Point-in-time data prerequisites missing: {}",
                            missing.join(", ")
                        );
                    }
                }
                Err(e) => warn!("Point-in-time data capability probe failed: {}", e),
            }
        });
    }

    if let Some(base_url) = state.config.webhook_url.as_deref() {
        let endpoint = format!("{}/telegram/webhook", base_url.trim_end_matches('/'));
        match pusher
            .set_webhook(&endpoint, state.config.telegram_webhook_secret.as_deref())
            .await
        {
            Ok(_) => info!("Telegram webhook registered: {}", endpoint),
            Err(e) => warn!("Telegram webhook registration failed: {}", e),
        }
    } else {
        warn!("WEBHOOK_URL is not set; Telegram inbound commands are disabled");
    }

    if state.config.enable_burst_monitor && state.config.stock_alert_channel.is_some() {
        let mut burst_monitor = services::burst_monitor::BurstMonitorService::new(
            state.clone(),
            sina_client.clone(),
            pusher.clone(),
        );
        tokio::spawn(async move {
            burst_monitor.run_poll_loop().await;
        });
        info!("Burst monitor started");
    }

    if state.config.enable_signal_auto_trading {
        let auto_trading = services::signal_auto_trading::SignalAutoTradingService::new(
            state.clone(),
            sina_client,
        );
        tokio::spawn(async move {
            auto_trading.run_poll_loop().await;
        });
        info!("Signal auto trading loop started");
    }

    if state.config.enable_daban_live && state.config.daban_channel.is_some() {
        let daban = services::daban::DabanService::new(state.clone());
        let provider_clone = provider.clone();
        let pusher_clone = pusher.clone();
        let channel = state.config.daban_channel.clone().unwrap_or_default();
        tokio::spawn(async move {
            daban
                .run_live_loop(provider_clone, pusher_clone, channel)
                .await;
        });
        info!("Daban live loop started");
    }

    if state.config.enable_chip_dist {
        let chip_dist = services::chip_dist::ChipDistService::new(state.clone());
        tokio::spawn(async move {
            chip_dist.run_daily_update_loop().await;
        });
        info!("Chip distribution loop started");
    }

    if state.config.enable_ai_analysis {
        info!("ENABLE_AI_ANALYSIS no longer starts a free-form loop; DecisionSupport scheduler owns daily analysis generation");
    }

    // Check if first-run backfill needed
    {
        let history_svc =
            services::stock_history::StockHistoryService::new(state.clone(), provider.clone());
        if !history_svc.has_any_data().await {
            info!("First run detected - starting full-history backfill in background");
            let state_clone = state.clone();
            let provider_clone = provider.clone();
            tokio::spawn(async move {
                let svc =
                    services::stock_history::StockHistoryService::new(state_clone, provider_clone);
                if let Err(e) = svc.backfill_full().await {
                    tracing::warn!("Backfill failed: {}", e);
                }
            });
        }
    }

    // --run-now: fire all jobs sequentially for local testing
    if std::env::args().any(|a| a == "--run-now") {
        info!("--run-now: firing all jobs sequentially");
        scheduler::run_fetch_job(state.clone(), provider.clone()).await;
        scheduler::run_point_in_time_reference_refresh_job(state.clone()).await;
        scheduler::run_point_in_time_trade_date_refresh_job(state.clone()).await;
        scheduler::run_market_snapshot_job(state.clone()).await;
        scheduler::run_scan_job(state.clone()).await;
        scheduler::run_daily_report_job(state.clone(), provider.clone(), pusher.clone()).await;
        scheduler::run_weekly_report_job(state.clone(), provider.clone(), pusher.clone()).await;
        scheduler::run_daily_signal_archive_job(state.clone()).await;
        info!("--run-now: all jobs complete, API server starting");
    }

    // Start scheduler
    let _sched =
        scheduler::start_scheduler(state.clone(), provider.clone(), pusher.clone()).await?;
    info!("Scheduler started");

    // Start Axum REST API
    let router = api::build_router(state.clone());
    let addr = api_bind_addr(api_port);
    info!("API server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        api_bind_addr, repair_company_intelligence_requested, repair_daily_bars_requested,
        repair_mode, run_company_intelligence_repair,
    };
    use crate::error::AppError;
    use crate::services::company_intelligence::CompanySyncReport;
    use std::sync::{Arc, Mutex};

    #[test]
    fn api_listener_is_loopback_only() {
        let addr = api_bind_addr(8080);

        assert!(addr.ip().is_loopback());
        assert_eq!(addr.port(), 8080);
    }

    #[test]
    fn repair_mode_only_activates_for_explicit_flag() {
        assert!(repair_daily_bars_requested([
            "qbot".to_string(),
            "--repair-daily-bars".to_string(),
        ]));
        assert!(!repair_daily_bars_requested([
            "qbot".to_string(),
            "--run-now".to_string(),
        ]));
    }

    #[test]
    fn company_intelligence_repair_only_activates_for_explicit_flag() {
        assert!(repair_company_intelligence_requested(
            ["qbot", "--repair-company-intelligence"].map(str::to_string)
        ));
        assert!(!repair_company_intelligence_requested(
            ["qbot", "--repair-daily-bars"].map(str::to_string)
        ));
    }

    #[test]
    fn repair_modes_are_mutually_exclusive_before_startup() {
        let error = repair_mode(
            [
                "qbot",
                "--repair-daily-bars",
                "--repair-company-intelligence",
            ]
            .map(str::to_string),
        )
        .unwrap_err();

        assert!(error.to_string().contains("mutually exclusive"));
    }

    #[derive(Clone, Copy)]
    enum RepairOutcome {
        Report {
            completed: usize,
            failed: usize,
            pending: usize,
        },
        Error(&'static str),
    }

    impl RepairOutcome {
        fn result(self) -> crate::error::Result<CompanySyncReport> {
            match self {
                Self::Report {
                    completed,
                    failed,
                    pending,
                } => Ok(CompanySyncReport {
                    completed,
                    failed,
                    pending,
                }),
                Self::Error(message) => Err(AppError::DataProvider(message.to_string())),
            }
        }
    }

    #[tokio::test]
    async fn company_repair_attempts_both_phases_and_reports_every_outcome() {
        struct Case {
            name: &'static str,
            financials: RepairOutcome,
            dividends: RepairOutcome,
            succeeds: bool,
            expected_errors: &'static [&'static str],
        }

        let success = RepairOutcome::Report {
            completed: 2,
            failed: 0,
            pending: 0,
        };
        let cases = [
            Case {
                name: "both phases succeed",
                financials: success,
                dividends: success,
                succeeds: true,
                expected_errors: &[],
            },
            Case {
                name: "pending report is incomplete",
                financials: RepairOutcome::Report {
                    completed: 1,
                    failed: 0,
                    pending: 1,
                },
                dividends: success,
                succeeds: false,
                expected_errors: &["financial repair incomplete", "pending=1"],
            },
            Case {
                name: "failed report is incomplete",
                financials: RepairOutcome::Report {
                    completed: 1,
                    failed: 1,
                    pending: 0,
                },
                dividends: success,
                succeeds: false,
                expected_errors: &["financial repair incomplete", "failed=1"],
            },
            Case {
                name: "dividend failure follows financial success",
                financials: success,
                dividends: RepairOutcome::Error("dividend failure"),
                succeeds: false,
                expected_errors: &["dividend failure"],
            },
            Case {
                name: "both failures are retained",
                financials: RepairOutcome::Error("financial failure"),
                dividends: RepairOutcome::Error("dividend failure"),
                succeeds: false,
                expected_errors: &["financial failure", "dividend failure"],
            },
        ];

        for case in cases {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let financial_calls = calls.clone();
            let dividend_calls = calls.clone();
            let financials = case.financials;
            let dividends = case.dividends;

            let result = run_company_intelligence_repair(
                move || async move {
                    financial_calls.lock().unwrap().push("financials");
                    financials.result()
                },
                move || async move {
                    dividend_calls.lock().unwrap().push("dividends");
                    dividends.result()
                },
            )
            .await;
            let error = result
                .as_ref()
                .err()
                .map(ToString::to_string)
                .unwrap_or_default();

            assert_eq!(result.is_ok(), case.succeeds, "{}: {error}", case.name);
            assert_eq!(
                *calls.lock().unwrap(),
                vec!["financials", "dividends"],
                "{}",
                case.name
            );
            let mut previous_position = 0;
            for expected in case.expected_errors {
                let position = error[previous_position..]
                    .find(expected)
                    .map(|offset| previous_position + offset)
                    .unwrap_or_else(|| panic!("{}: missing {expected:?} in {error:?}", case.name));
                previous_position = position + expected.len();
            }
        }
    }
}
