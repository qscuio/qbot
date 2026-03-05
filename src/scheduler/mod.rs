use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, warn};

use crate::data::tushare::TushareClient;
use crate::services::{
    limit_up::LimitUpService,
    market::MarketService,
    market_report::MarketReportService,
    scanner::ScannerService,
    sector::SectorService,
    stock_history::StockHistoryService,
};
use crate::state::AppState;
use crate::telegram::pusher::TelegramPusher;

/// Fetch today's OHLCV, limit-up stocks, and sector data (15:05 job).
pub async fn run_fetch_job(state: Arc<AppState>, provider: Arc<TushareClient>) {
    let today = chrono::Local::now().naive_local().date();
    info!("Fetch job: OHLCV + limit-up + sector for {}", today);

    let history_svc = StockHistoryService::new(state.clone(), provider.clone());
    if let Err(e) = history_svc.update_today().await {
        warn!("Daily data fetch failed: {}", e);
    }

    let limit_svc = LimitUpService::new(state.clone(), provider.clone());
    match limit_svc.fetch_and_save(today).await {
        Ok(stocks) => info!("Limit-up: {} stocks", stocks.len()),
        Err(e) => warn!("Limit-up fetch failed: {}", e),
    }

    let sector_svc = SectorService::new(state.clone(), provider.clone());
    if let Err(e) = sector_svc.fetch_and_save(today).await {
        warn!("Sector data failed: {}", e);
    }
}

/// Run all 21 signal detectors and cache results to Redis (15:35 job).
pub async fn run_scan_job(state: Arc<AppState>) {
    info!("Scan job: running full signal scan");
    let scanner = ScannerService::new(state);
    if let Err(e) = scanner.run_full_scan().await {
        warn!("Scan failed: {}", e);
    }
}

/// Generate daily market report and push to Telegram (16:00 job).
pub async fn run_daily_report_job(
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
    pusher: Arc<TelegramPusher>,
) {
    let today = chrono::Local::now().naive_local().date();
    info!("Daily report job for {}", today);

    let market_svc = Arc::new(MarketService::new(state.clone(), provider.clone()));
    let limit_svc = Arc::new(LimitUpService::new(state.clone(), provider.clone()));
    let sector_svc = Arc::new(SectorService::new(state.clone(), provider.clone()));
    let scanner_svc = Arc::new(ScannerService::new(state.clone()));
    let report_svc = MarketReportService::new(
        state.clone(), market_svc, limit_svc, sector_svc, scanner_svc,
    );

    match report_svc.generate_daily(today).await {
        Ok(report) => {
            if let Some(channel) = &state.config.report_channel {
                if let Err(e) = pusher.push(channel, &report).await {
                    warn!("Telegram push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Daily report failed: {}", e),
    }
}

/// Generate weekly market report and push to Telegram (Friday 20:00 job).
pub async fn run_weekly_report_job(
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
    pusher: Arc<TelegramPusher>,
) {
    info!("Weekly report job");

    let market_svc = Arc::new(MarketService::new(state.clone(), provider.clone()));
    let limit_svc = Arc::new(LimitUpService::new(state.clone(), provider.clone()));
    let sector_svc = Arc::new(SectorService::new(state.clone(), provider.clone()));
    let scanner_svc = Arc::new(ScannerService::new(state.clone()));
    let report_svc = MarketReportService::new(
        state.clone(), market_svc, limit_svc, sector_svc, scanner_svc,
    );

    match report_svc.generate_weekly().await {
        Ok(report) => {
            if let Some(channel) = &state.config.report_channel {
                if let Err(e) = pusher.push(channel, &report).await {
                    warn!("Telegram push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Weekly report failed: {}", e),
    }
}

pub async fn start_scheduler(
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
    pusher: Arc<TelegramPusher>,
) -> anyhow::Result<JobScheduler> {
    let sched = JobScheduler::new().await?;

    // 15:05 weekdays
    {
        let s = state.clone();
        let p = provider.clone();
        sched.add(Job::new_async("0 5 15 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone(); let p = p.clone();
            Box::pin(async move { run_fetch_job(s, p).await })
        })?).await?;
    }

    // 15:35 weekdays
    {
        let s = state.clone();
        sched.add(Job::new_async("0 35 15 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone();
            Box::pin(async move { run_scan_job(s).await })
        })?).await?;
    }

    // 16:00 weekdays
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 0 16 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move { run_daily_report_job(s, p, push).await })
        })?).await?;
    }

    // 20:00 Friday
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 0 20 * * Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move { run_weekly_report_job(s, p, push).await })
        })?).await?;
    }

    sched.start().await?;
    info!("Scheduler started with 4 jobs");
    Ok(sched)
}
