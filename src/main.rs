mod config;
mod data;
mod error;
mod state;

use anyhow::Result;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "qbot=info,sqlx=warn".into()),
        )
        .init();

    info!("qbot starting...");

    let config = config::Config::from_env()?;
    info!("Config loaded");

    // Connect to PostgreSQL
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;
    info!("PostgreSQL connected");

    // Connect to Redis
    let redis_client = redis::Client::open(config.redis_url.as_str())?;
    let redis = redis::aio::ConnectionManager::new(redis_client).await?;
    info!("Redis connected");

    let _state = state::AppState {
        config: Arc::new(config),
        db,
        redis,
    };

    info!("qbot ready");

    // Keep alive until signal
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    Ok(())
}
