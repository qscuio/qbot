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

    // Official event source (optional)
    pub official_event_feed_url: Option<String>,
    pub official_event_feed_api_key: Option<String>,
    pub official_event_source_id: String,
    pub official_event_store_full_content: bool,

    // GDELT supplementary event source
    pub enable_gdelt_events: bool,
    pub gdelt_event_query: String,
    pub gdelt_max_records: usize,

    // Feature flags
    pub enable_burst_monitor: bool,
    pub enable_daban_live: bool,
    pub enable_ai_analysis: bool,
    pub enable_chip_dist: bool,
    pub enable_event_score_adjustment: bool,
    pub max_event_score_adjustment: f64,
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
            data_proxy: optional_nonblank_env_var("DATA_PROXY"),
            official_event_feed_url: optional_nonblank_env_var("OFFICIAL_EVENT_FEED_URL"),
            official_event_feed_api_key: optional_nonblank_env_var("OFFICIAL_EVENT_FEED_API_KEY"),
            official_event_source_id: std::env::var("OFFICIAL_EVENT_SOURCE_ID")
                .unwrap_or_else(|_| "official:market_event".to_string()),
            official_event_store_full_content: std::env::var("OFFICIAL_EVENT_STORE_FULL_CONTENT")
                .unwrap_or_else(|_| "false".to_string())
                == "true",
            enable_gdelt_events: std::env::var("ENABLE_GDELT_EVENTS")
                .unwrap_or_else(|_| "false".to_string())
                == "true",
            gdelt_event_query: std::env::var("GDELT_EVENT_QUERY").unwrap_or_default(),
            gdelt_max_records: std::env::var("GDELT_MAX_RECORDS")
                .unwrap_or_else(|_| "250".to_string())
                .parse()
                .context("GDELT_MAX_RECORDS must be a positive integer")?,
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
            enable_event_score_adjustment: std::env::var("ENABLE_EVENT_SCORE_ADJUSTMENT")
                .unwrap_or_else(|_| "false".to_string())
                == "true",
            max_event_score_adjustment: clamped_env_f64(
                "MAX_EVENT_SCORE_ADJUSTMENT",
                0.0,
                0.0,
                5.0,
            )?,
            enable_signal_auto_trading: std::env::var("ENABLE_SIGNAL_AUTO_TRADING")
                .unwrap_or_else(|_| "false".to_string())
                == "true",
        })
    }
}

fn optional_nonblank_env_var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn clamped_env_f64(name: &str, default: f64, min: f64, max: f64) -> Result<f64> {
    Ok(std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<f64>()
        .with_context(|| format!("{name} must be a number"))?
        .clamp(min, max))
}

#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::{LazyLock, Mutex, MutexGuard};

    static ENV_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    pub(crate) struct ScopedEnvGuard {
        _lock: MutexGuard<'static, ()>,
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl ScopedEnvGuard {
        pub(crate) fn lock(names: &[&'static str]) -> Self {
            let lock = ENV_MUTEX.lock().unwrap();
            let saved = names
                .iter()
                .map(|name| (*name, std::env::var(name).ok()))
                .collect();

            Self { _lock: lock, saved }
        }

        pub(crate) fn set_var(&self, name: &str, value: &str) {
            std::env::set_var(name, value);
        }

        pub(crate) fn remove_var(&self, name: &str) {
            std::env::remove_var(name);
        }
    }

    impl Drop for ScopedEnvGuard {
        fn drop(&mut self) {
            for (name, value) in &self.saved {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_env::ScopedEnvGuard;
    use super::*;

    #[test]
    fn test_config_defaults() {
        let env = ScopedEnvGuard::lock(&[
            "TUSHARE_TOKEN",
            "TELEGRAM_BOT_TOKEN",
            "DATABASE_URL",
            "REDIS_URL",
            "OFFICIAL_EVENT_FEED_URL",
            "OFFICIAL_EVENT_FEED_API_KEY",
            "OFFICIAL_EVENT_SOURCE_ID",
            "OFFICIAL_EVENT_STORE_FULL_CONTENT",
            "ENABLE_GDELT_EVENTS",
            "GDELT_EVENT_QUERY",
            "GDELT_MAX_RECORDS",
            "ENABLE_EVENT_SCORE_ADJUSTMENT",
            "MAX_EVENT_SCORE_ADJUSTMENT",
        ]);

        // Only TUSHARE_TOKEN and TELEGRAM_BOT_TOKEN are required
        // DATABASE_URL and REDIS_URL have internal defaults
        env.set_var("TUSHARE_TOKEN", "test_token");
        env.set_var("TELEGRAM_BOT_TOKEN", "123:abc");
        env.remove_var("DATABASE_URL");
        env.remove_var("REDIS_URL");
        env.remove_var("OFFICIAL_EVENT_FEED_URL");
        env.remove_var("OFFICIAL_EVENT_FEED_API_KEY");
        env.remove_var("OFFICIAL_EVENT_SOURCE_ID");
        env.remove_var("OFFICIAL_EVENT_STORE_FULL_CONTENT");
        env.remove_var("ENABLE_GDELT_EVENTS");
        env.remove_var("GDELT_EVENT_QUERY");
        env.remove_var("GDELT_MAX_RECORDS");
        env.remove_var("ENABLE_EVENT_SCORE_ADJUSTMENT");
        env.remove_var("MAX_EVENT_SCORE_ADJUSTMENT");

        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.tushare_token, "test_token");
        assert_eq!(cfg.database_url, "postgresql://qbot:qbot@127.0.0.1/qbot");
        assert_eq!(cfg.redis_url, "redis://127.0.0.1:6379");
        assert_eq!(cfg.api_port, 8080); // default
        assert_eq!(cfg.official_event_feed_url, None);
        assert_eq!(cfg.official_event_feed_api_key, None);
        assert_eq!(cfg.official_event_source_id, "official:market_event");
        assert!(!cfg.official_event_store_full_content);
        assert!(!cfg.enable_gdelt_events);
        assert!(cfg.gdelt_event_query.is_empty());
        assert_eq!(cfg.gdelt_max_records, 250);
        assert!(!cfg.enable_event_score_adjustment);
        assert_eq!(cfg.max_event_score_adjustment, 0.0);
    }

    #[test]
    fn test_config_normalizes_blank_official_event_api_key() {
        let env = ScopedEnvGuard::lock(&[
            "TUSHARE_TOKEN",
            "TELEGRAM_BOT_TOKEN",
            "OFFICIAL_EVENT_FEED_URL",
            "OFFICIAL_EVENT_FEED_API_KEY",
        ]);
        env.set_var("TUSHARE_TOKEN", "test_token");
        env.set_var("TELEGRAM_BOT_TOKEN", "123:abc");
        env.set_var("OFFICIAL_EVENT_FEED_URL", "https://example.test/feed");
        env.set_var("OFFICIAL_EVENT_FEED_API_KEY", "   ");

        let cfg = Config::from_env().unwrap();

        assert_eq!(
            cfg.official_event_feed_url.as_deref(),
            Some("https://example.test/feed")
        );
        assert_eq!(cfg.official_event_feed_api_key, None);
    }

    #[test]
    fn test_config_reads_gdelt_settings() {
        let env = ScopedEnvGuard::lock(&[
            "TUSHARE_TOKEN",
            "TELEGRAM_BOT_TOKEN",
            "ENABLE_GDELT_EVENTS",
            "GDELT_EVENT_QUERY",
            "GDELT_MAX_RECORDS",
        ]);
        env.set_var("TUSHARE_TOKEN", "test_token");
        env.set_var("TELEGRAM_BOT_TOKEN", "123:abc");
        env.set_var("ENABLE_GDELT_EVENTS", "true");
        env.set_var("GDELT_EVENT_QUERY", "red sea shipping");
        env.set_var("GDELT_MAX_RECORDS", "25");

        let cfg = Config::from_env().unwrap();

        assert!(cfg.enable_gdelt_events);
        assert_eq!(cfg.gdelt_event_query, "red sea shipping");
        assert_eq!(cfg.gdelt_max_records, 25);
    }

    #[test]
    fn test_config_clamps_event_score_adjustment_limit() {
        let env = ScopedEnvGuard::lock(&[
            "TUSHARE_TOKEN",
            "TELEGRAM_BOT_TOKEN",
            "ENABLE_EVENT_SCORE_ADJUSTMENT",
            "MAX_EVENT_SCORE_ADJUSTMENT",
        ]);
        env.set_var("TUSHARE_TOKEN", "test_token");
        env.set_var("TELEGRAM_BOT_TOKEN", "123:abc");
        env.set_var("ENABLE_EVENT_SCORE_ADJUSTMENT", "true");
        env.set_var("MAX_EVENT_SCORE_ADJUSTMENT", "10");

        let cfg = Config::from_env().unwrap();

        assert!(cfg.enable_event_score_adjustment);
        assert_eq!(cfg.max_event_score_adjustment, 5.0);
    }
}
