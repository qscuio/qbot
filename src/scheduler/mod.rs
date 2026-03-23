use std::sync::Arc;
use chrono::{Datelike, Duration};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, warn};

use crate::data::provider::DataProvider;
use crate::market_time::{beijing_today, beijing_tz};
use crate::services::{
    limit_up::LimitUpService, market::MarketService, market_report::MarketReportService,
    scanner::ScannerService, sector::SectorService, signal_auto_trading::SignalAutoTradingService,
    stock_history::StockHistoryService,
};
use crate::state::AppState;
use crate::telegram::pusher::TelegramPusher;

const FETCH_JOB_CRON: &str = "0 0 17 * * Mon,Tue,Wed,Thu,Fri";
const SCAN_JOB_CRON: &str = "0 30 17 * * Mon,Tue,Wed,Thu,Fri";
const DAILY_REPORT_JOB_CRON: &str = "0 0 18 * * Mon,Tue,Wed,Thu,Fri";
const WEEKLY_REPORT_JOB_CRON: &str = "0 0 20 * * Fri";

/// Fetch today's OHLCV, limit-up stocks, and sector data (17:00 job).
pub async fn run_fetch_job(state: Arc<AppState>, provider: Arc<dyn DataProvider>) {
    let _guard = state.fetch_job_lock.lock().await;
    let today = beijing_today();
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

/// Run all 21 signal detectors and cache results to Redis (17:30 job).
pub async fn run_scan_job(state: Arc<AppState>) {
    let _guard = state.scan_job_lock.lock().await;
    info!("Scan job: running full signal scan");
    let scanner = ScannerService::new(state.clone());
    match scanner.run_full_scan().await {
        Ok(results) => {
            if state.config.enable_signal_auto_trading {
                let auto_svc = SignalAutoTradingService::new(
                    state.clone(),
                    Arc::new(crate::data::sina::SinaClient::new()),
                );
                if let Err(e) = auto_svc.prepare_candidates_from_scan(&results).await {
                    warn!("Signal auto candidate prep failed: {}", e);
                }
            }
        }
        Err(e) => {
            warn!("Scan failed: {}", e);
        }
    }
}

/// Generate daily market report and push to Telegram (18:00 job).
pub async fn run_daily_report_job(
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
    pusher: Arc<TelegramPusher>,
) {
    let _guard = state.daily_report_job_lock.lock().await;
    let today = beijing_today();
    info!("Daily report job for {}", today);

    let market_svc = Arc::new(MarketService::new(state.clone(), provider.clone()));
    let limit_svc = Arc::new(LimitUpService::new(state.clone(), provider.clone()));
    let sector_svc = Arc::new(SectorService::new(state.clone(), provider.clone()));
    let report_svc = MarketReportService::new(state.clone(), market_svc, limit_svc, sector_svc);

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

    let alert_channel = state
        .config
        .stock_alert_channel
        .as_ref()
        .or(state.config.report_channel.as_ref());

    match report_svc.generate_limitup_report(today).await {
        Ok(report) => {
            if let Some(channel) = alert_channel {
                let push_result = match report_svc.load_limitup_report_data(today).await {
                    Ok(stocks) => match crate::telegram::formatter::limit_up_report_markup(&stocks) {
                        Some(markup) => pusher.push_with_markup(channel, &report, markup).await,
                        None => pusher.push(channel, &report).await,
                    },
                    Err(_) => pusher.push(channel, &report).await,
                };
                if let Err(e) = push_result {
                    warn!("Limit-up report push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Limit-up standalone report failed: {}", e),
    }

    match report_svc.generate_strong_report(today, 7).await {
        Ok(report) => {
            if let Some(channel) = alert_channel {
                let push_result = match report_svc.load_strong_report_data(7).await {
                    Ok(stocks) => match crate::telegram::formatter::strong_stock_report_markup(&stocks) {
                        Some(markup) => pusher.push_with_markup(channel, &report, markup).await,
                        None => pusher.push(channel, &report).await,
                    },
                    Err(_) => pusher.push(channel, &report).await,
                };
                if let Err(e) = push_result {
                    warn!("Strong-stock report push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Strong-stock standalone report failed: {}", e),
    }

    if state.config.enable_signal_auto_trading {
        let auto_svc = SignalAutoTradingService::new(
            state.clone(),
            Arc::new(crate::data::sina::SinaClient::new()),
        );
        match auto_svc.generate_daily_report(today).await {
            Ok(report) => {
                if let Some(channel) = alert_channel {
                    if let Err(e) = pusher.push(channel, &report).await {
                        warn!("Signal-auto report push failed: {}", e);
                    }
                }
            }
            Err(e) => warn!("Signal-auto daily report failed: {}", e),
        }
    }
}

/// Generate weekly market report and push to Telegram (Friday 20:00 job).
pub async fn run_weekly_report_job(
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
    pusher: Arc<TelegramPusher>,
) {
    let _guard = state.weekly_report_job_lock.lock().await;
    info!("Weekly report job");

    let market_svc = Arc::new(MarketService::new(state.clone(), provider.clone()));
    let limit_svc = Arc::new(LimitUpService::new(state.clone(), provider.clone()));
    let sector_svc = Arc::new(SectorService::new(state.clone(), provider.clone()));
    let report_svc = MarketReportService::new(state.clone(), market_svc, limit_svc, sector_svc);
    let today = beijing_today();
    let start = today - Duration::days(today.weekday().num_days_from_monday() as i64);

    match report_svc.generate_weekly().await {
        Ok(report) => {
            if let Some(channel) = &state.config.report_channel {
                let push_result = match report_svc.load_weekly_report_rows(start, today).await {
                    Ok(rows) => match crate::services::market_report::weekly_report_markup(&rows) {
                        Some(markup) => pusher.push_with_markup(channel, &report, markup).await,
                        None => pusher.push(channel, &report).await,
                    },
                    Err(_) => pusher.push(channel, &report).await,
                };
                if let Err(e) = push_result {
                    warn!("Telegram push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Weekly report failed: {}", e),
    }
}

pub async fn start_scheduler(
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
    pusher: Arc<TelegramPusher>,
) -> anyhow::Result<JobScheduler> {
    let sched = JobScheduler::new().await?;

    // 17:00 weekdays
    {
        let s = state.clone();
        let p = provider.clone();
        sched
            .add(Job::new_async_tz(
                FETCH_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    let p = p.clone();
                    Box::pin(async move { run_fetch_job(s, p).await })
                },
            )?)
            .await?;
    }

    // 17:30 weekdays
    {
        let s = state.clone();
        sched
            .add(Job::new_async_tz(
                SCAN_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    Box::pin(async move { run_scan_job(s).await })
                },
            )?)
            .await?;
    }

    // 18:00 weekdays
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched
            .add(Job::new_async_tz(
                DAILY_REPORT_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    let p = p.clone();
                    let push = push.clone();
                    Box::pin(async move { run_daily_report_job(s, p, push).await })
                },
            )?)
            .await?;
    }

    // 20:00 Friday
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched
            .add(Job::new_async_tz(
                WEEKLY_REPORT_JOB_CRON,
                beijing_tz(),
                move |_, _| {
                    let s = s.clone();
                    let p = p.clone();
                    let push = push.clone();
                    Box::pin(async move { run_weekly_report_job(s, p, push).await })
                },
            )?)
            .await?;
    }

    sched.start().await?;
    info!("Scheduler started with 4 jobs");
    Ok(sched)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weekday_pipeline_runs_after_tushare_eod_window() {
        assert_eq!(FETCH_JOB_CRON, "0 0 17 * * Mon,Tue,Wed,Thu,Fri");
        assert_eq!(SCAN_JOB_CRON, "0 30 17 * * Mon,Tue,Wed,Thu,Fri");
        assert_eq!(DAILY_REPORT_JOB_CRON, "0 0 18 * * Mon,Tue,Wed,Thu,Fri");
    }

    #[test]
    fn weekly_report_schedule_stays_on_friday_evening() {
        assert_eq!(WEEKLY_REPORT_JOB_CRON, "0 0 20 * * Fri");
    }
}
