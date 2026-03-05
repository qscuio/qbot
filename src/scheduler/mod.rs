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

pub async fn start_scheduler(
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
    pusher: Arc<TelegramPusher>,
) -> anyhow::Result<JobScheduler> {
    let sched = JobScheduler::new().await?;

    // 15:05 weekdays — fetch daily OHLCV + limit-up data
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 5 15 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move {
                info!("15:05 - Fetching daily data");
                let today = chrono::Local::now().naive_local().date();
                let history_svc = StockHistoryService::new(s.clone(), p.clone());
                if let Err(e) = history_svc.update_today().await {
                    warn!("Daily data fetch failed: {}", e);
                }
                let limit_svc = LimitUpService::new(s.clone(), p.clone());
                if let Ok(stocks) = limit_svc.fetch_and_save(today).await {
                    info!("Limit-up: {} stocks", stocks.len());
                }
                let sector_svc = SectorService::new(s.clone(), p.clone());
                if let Err(e) = sector_svc.fetch_and_save(today).await {
                    warn!("Sector data failed: {}", e);
                }
                let _ = push; // pusher reserved for future alerts
            })
        })?).await?;
    }

    // 15:35 weekdays — run full signal scan
    {
        let s = state.clone();
        sched.add(Job::new_async("0 35 15 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone();
            Box::pin(async move {
                info!("15:35 - Running full scan");
                let scanner = ScannerService::new(s);
                if let Err(e) = scanner.run_full_scan().await {
                    warn!("Scan failed: {}", e);
                }
            })
        })?).await?;
    }

    // 16:00 weekdays — generate and push daily report
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 0 16 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move {
                info!("16:00 - Generating daily report");
                let today = chrono::Local::now().naive_local().date();
                let market_svc = Arc::new(MarketService::new(s.clone(), p.clone()));
                let limit_svc = Arc::new(LimitUpService::new(s.clone(), p.clone()));
                let sector_svc = Arc::new(SectorService::new(s.clone(), p.clone()));
                let scanner_svc = Arc::new(ScannerService::new(s.clone()));
                let report_svc = MarketReportService::new(
                    s.clone(), market_svc, limit_svc, sector_svc, scanner_svc
                );
                match report_svc.generate_daily(today).await {
                    Ok(report) => {
                        if let Some(channel) = &s.config.report_channel {
                            let _ = push.push(channel, &report).await;
                        }
                    }
                    Err(e) => warn!("Daily report failed: {}", e),
                }
            })
        })?).await?;
    }

    // 20:00 Friday — weekly report
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 0 20 * * Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move {
                info!("Friday 20:00 - Weekly report");
                let market_svc = Arc::new(MarketService::new(s.clone(), p.clone()));
                let limit_svc = Arc::new(LimitUpService::new(s.clone(), p.clone()));
                let sector_svc = Arc::new(SectorService::new(s.clone(), p.clone()));
                let scanner_svc = Arc::new(ScannerService::new(s.clone()));
                let report_svc = MarketReportService::new(
                    s.clone(), market_svc, limit_svc, sector_svc, scanner_svc
                );
                match report_svc.generate_weekly().await {
                    Ok(report) => {
                        if let Some(channel) = &s.config.report_channel {
                            let _ = push.push(channel, &report).await;
                        }
                    }
                    Err(e) => warn!("Weekly report failed: {}", e),
                }
            })
        })?).await?;
    }

    sched.start().await?;
    info!("Scheduler started with 4 jobs");
    Ok(sched)
}
