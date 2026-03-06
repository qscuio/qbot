use crate::config::Config;
use crate::data::provider::DataProvider;
use crate::telegram::pusher::TelegramPusher;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: sqlx::PgPool,
    pub redis: redis::aio::ConnectionManager,
    pub provider: Arc<dyn DataProvider>,
    pub pusher: Arc<TelegramPusher>,
    pub fetch_job_lock: Arc<Mutex<()>>,
    pub scan_job_lock: Arc<Mutex<()>>,
    pub daily_report_job_lock: Arc<Mutex<()>>,
    pub weekly_report_job_lock: Arc<Mutex<()>>,
}
