use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    // Tushare
    pub tushare_token: String,

    // Database
    pub database_url: String,
    pub redis_url: String,

    // Telegram
    pub telegram_bot_token: String,
    pub telegram_webhook_secret: Option<String>,
    pub webhook_url: Option<String>,
    pub stock_alert_channel: Option<String>,
    pub report_channel: Option<String>,
    pub daban_channel: Option<String>,

    // API
    pub api_port: u16,
    pub api_key: Option<String>,
    pub ai_api_key: Option<String>,
    pub ai_base_url: String,
    pub ai_model: String,

    // Data proxy (optional)
    pub data_proxy: Option<String>,

    // Feature flags
    pub enable_burst_monitor: bool,
    pub enable_daban_live: bool,
    pub enable_ai_analysis: bool,
    pub enable_chip_dist: bool,
    pub enable_signal_auto_trading: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        Ok(Config {
            tushare_token: std::env::var("TUSHARE_TOKEN").context("TUSHARE_TOKEN is required")?,
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://qbot:qbot@127.0.0.1/qbot".to_string()),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            telegram_bot_token: std::env::var("TELEGRAM_BOT_TOKEN")
                .context("TELEGRAM_BOT_TOKEN is required")?,
            telegram_webhook_secret: std::env::var("TELEGRAM_WEBHOOK_SECRET").ok(),
            webhook_url: std::env::var("WEBHOOK_URL").ok(),
            stock_alert_channel: std::env::var("STOCK_ALERT_CHANNEL").ok(),
            report_channel: std::env::var("REPORT_CHANNEL").ok(),
            daban_channel: std::env::var("DABAN_CHANNEL").ok(),
            api_port: std::env::var("API_PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .unwrap_or(8080),
            api_key: std::env::var("API_KEY").ok(),
            ai_api_key: std::env::var("AI_API_KEY").ok(),
            ai_base_url: std::env::var("AI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            ai_model: std::env::var("AI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string()),
            data_proxy: std::env::var("DATA_PROXY").ok(),
            enable_burst_monitor: std::env::var("ENABLE_BURST_MONITOR")
                .unwrap_or_else(|_| "true".to_string())
                == "true",
            enable_daban_live: std::env::var("ENABLE_DABAN_LIVE")
                .unwrap_or_else(|_| "false".to_string())
                == "true",
            enable_ai_analysis: std::env::var("ENABLE_AI_ANALYSIS")
                .unwrap_or_else(|_| "false".to_string())
                == "true",
            enable_chip_dist: std::env::var("ENABLE_CHIP_DIST")
                .unwrap_or_else(|_| "true".to_string())
                == "true",
            enable_signal_auto_trading: std::env::var("ENABLE_SIGNAL_AUTO_TRADING")
                .unwrap_or_else(|_| "false".to_string())
                == "true",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        // Only TUSHARE_TOKEN and TELEGRAM_BOT_TOKEN are required
        // DATABASE_URL and REDIS_URL have internal defaults
        std::env::set_var("TUSHARE_TOKEN", "test_token");
        std::env::set_var("TELEGRAM_BOT_TOKEN", "123:abc");
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("REDIS_URL");

        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.tushare_token, "test_token");
        assert_eq!(cfg.database_url, "postgresql://qbot:qbot@127.0.0.1/qbot");
        assert_eq!(cfg.redis_url, "redis://127.0.0.1:6379");
        assert_eq!(cfg.api_port, 8080); // default
    }
}
