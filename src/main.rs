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
use tracing::info;

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

    // Data provider and Telegram pusher
    let provider: Arc<dyn DataProvider> = Arc::new(data::tushare::TushareClient::new(
        config.tushare_token.clone(),
        config.data_proxy.as_deref(),
    ));
    let pusher = Arc::new(telegram::TelegramPusher::new(
        config.telegram_bot_token.clone(),
    ));

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

    // Check if first-run backfill needed
    {
        let history_svc =
            services::stock_history::StockHistoryService::new(state.clone(), provider.clone());
        if !history_svc.has_any_data().await {
            info!("First run detected - starting 3-year backfill in background");
            let state_clone = state.clone();
            let provider_clone = provider.clone();
            tokio::spawn(async move {
                let svc =
                    services::stock_history::StockHistoryService::new(state_clone, provider_clone);
                if let Err(e) = svc.backfill(3).await {
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
