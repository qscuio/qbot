use std::sync::Arc;
use crate::config::Config;
use crate::data::tushare::TushareClient;
use crate::telegram::pusher::TelegramPusher;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: sqlx::PgPool,
    pub redis: redis::aio::ConnectionManager,
    pub provider: Arc<TushareClient>,
    pub pusher: Arc<TelegramPusher>,
}
