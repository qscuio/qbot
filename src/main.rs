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
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "qbot=info,sqlx=warn,tokio_cron_scheduler=warn".into()),
        )
        .init();

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
    let tushare_provider: Arc<dyn DataProvider> = Arc::new(data::tushare::TushareClient::new(
        config.tushare_token.clone(),
        config.data_proxy.as_deref(),
    ));
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
    let bot_commands = [
        ("start", "显示帮助"),
        ("help", "显示帮助"),
        ("menu", "打开按钮菜单"),
        ("scan", "扫描信号"),
        ("daban", "打板评分"),
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
    match pusher.set_my_commands(&bot_commands).await {
        Ok(_) => info!("Telegram bot commands registered"),
        Err(e) => warn!("Telegram setMyCommands failed: {}", e),
    }

    let state = Arc::new(state::AppState {
        config: Arc::new(config.clone()),
        db,
        redis,
        provider: provider.clone(),
        pusher: pusher.clone(),
        fetch_job_lock: Arc::new(Mutex::new(())),
        scan_job_lock: Arc::new(Mutex::new(())),
        daily_report_job_lock: Arc::new(Mutex::new(())),
        weekly_report_job_lock: Arc::new(Mutex::new(())),
    });

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
            Arc::new(data::sina::SinaClient::new()),
            pusher.clone(),
        );
        tokio::spawn(async move {
            burst_monitor.run_poll_loop().await;
        });
        info!("Burst monitor started");
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

    if state.config.enable_ai_analysis && state.config.report_channel.is_some() {
        let ai = services::ai_analysis::AiAnalysisService::new(state.clone());
        let channel = state.config.report_channel.clone().unwrap_or_default();
        let pusher_clone = pusher.clone();
        tokio::spawn(async move {
            ai.run_daily_loop(pusher_clone, channel).await;
        });
        info!("AI analysis loop started");
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

    // --run-now: fire all 4 jobs sequentially for local testing
    if std::env::args().any(|a| a == "--run-now") {
        info!("--run-now: firing all jobs sequentially");
        scheduler::run_fetch_job(state.clone(), provider.clone()).await;
        scheduler::run_scan_job(state.clone()).await;
        scheduler::run_daily_report_job(state.clone(), provider.clone(), pusher.clone()).await;
        scheduler::run_weekly_report_job(state.clone(), provider.clone(), pusher.clone()).await;
        info!("--run-now: all jobs complete, API server starting");
    }

    // Start scheduler
    let _sched =
        scheduler::start_scheduler(state.clone(), provider.clone(), pusher.clone()).await?;
    info!("Scheduler started");

    // Start Axum REST API
    let router = api::build_router(state.clone());
    let addr = format!("0.0.0.0:{}", api_port).parse::<std::net::SocketAddr>()?;
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
