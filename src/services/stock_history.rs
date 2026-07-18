use chrono::{Duration, NaiveDate};
use std::sync::Arc;
use tracing::{info, warn};

use crate::data::provider::DataProvider;
use crate::error::Result;
use crate::market_time::beijing_today;
use crate::state::AppState;
use crate::storage::market_repository::MarketRepository;
use crate::storage::{postgres, upsert_stock_info};

pub struct StockHistoryService {
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
}

impl StockHistoryService {
    pub fn new(state: Arc<AppState>, provider: Arc<dyn DataProvider>) -> Self {
        StockHistoryService { state, provider }
    }

    /// Full backfill: fetch all trading dates in last N years, date-by-date
    pub async fn backfill(&self, years: u32) -> Result<()> {
        let end = beijing_today();
        let start = end - Duration::days(years as i64 * 365);
        self.backfill_range(start, end).await
    }

    /// Full history backfill from A-share market inception.
    pub async fn backfill_full(&self) -> Result<()> {
        let end = beijing_today();
        let start = NaiveDate::from_ymd_opt(1990, 1, 1).expect("valid full history start date");
        self.backfill_range(start, end).await
    }

    async fn backfill_range(&self, start: NaiveDate, end: NaiveDate) -> Result<()> {
        info!("Starting backfill {} to {}", start, end);

        // Refresh stock universe/name mapping first, so scan/watch/chart can show names correctly.
        let stocks = self.provider.get_stock_list().await?;
        upsert_stock_info(&self.state.db, &stocks).await?;
        info!(
            "Stock info refreshed before backfill: {} stocks",
            stocks.len()
        );

        let dates = self.provider.get_trading_dates(start, end).await?;
        info!("{} trading days to backfill", dates.len());

        for (i, date) in dates.iter().enumerate() {
            match self.provider.get_daily_bars_by_date(*date).await {
                Ok(bars) => {
                    let count = bars.len();
                    let mut tx = self.state.db.begin().await?;
                    postgres::upsert_daily_bars_in_tx(&mut tx, &bars).await?;
                    tx.commit().await?;
                    if i % 50 == 0 {
                        info!(
                            "Backfill progress: {}/{} ({}, {} bars)",
                            i + 1,
                            dates.len(),
                            date,
                            count
                        );
                    }
                }
                Err(e) => {
                    warn!("Failed to fetch {}: {}", date, e);
                }
            }
            // Rate limit: ~200ms between calls
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        info!("Backfill complete");
        Ok(())
    }

    /// Daily incremental update: fetch today's bars for all known stocks
    pub async fn update_today(&self) -> Result<()> {
        let today = beijing_today();
        info!("Daily update for {}", today);

        let bars = self.provider.get_daily_bars_by_date(today).await?;
        let count = bars.len();
        let mut tx = self.state.db.begin().await?;
        postgres::upsert_daily_bars_in_tx(&mut tx, &bars).await?;
        MarketRepository::append_daily_bar_versions_in_tx(
            &mut tx,
            &bars,
            chrono::Utc::now(),
            "observed",
            self.provider.name(),
        )
        .await?;
        tx.commit().await?;
        info!("Daily update: {} bars saved for {}", count, today);

        // Also refresh stock info
        let stocks = self.provider.get_stock_list().await?;
        upsert_stock_info(&self.state.db, &stocks).await?;
        info!("Stock info refreshed: {} stocks", stocks.len());

        Ok(())
    }

    /// Check if the history table already has any data.
    pub async fn has_any_data(&self) -> bool {
        let result: Result<(bool,)> =
            sqlx::query_as("SELECT EXISTS (SELECT 1 FROM stock_daily_bars LIMIT 1)")
                .fetch_one(&self.state.db)
                .await
                .map_err(crate::error::AppError::Database);

        result.ok().map(|(exists,)| exists).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use chrono::NaiveDate;
    use sqlx::PgPool;
    use tokio::sync::Mutex;

    use super::*;
    use crate::analysis::market_snapshot::{
        AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, SectorMembership,
        SecurityDailyStatus, SecurityMasterVersion,
    };
    use crate::config::Config;
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
    use crate::telegram::TelegramPusher;

    #[derive(Clone)]
    struct FakeProvider {
        trading_dates: Vec<NaiveDate>,
        stocks: Vec<StockInfo>,
        bars: Vec<(String, Candle)>,
    }

    #[async_trait]
    impl DataProvider for FakeProvider {
        fn name(&self) -> &'static str {
            "fake"
        }

        async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
            Ok(self.stocks.clone())
        }

        async fn get_daily_bars_by_date(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<(String, Candle)>> {
            Ok(self.bars.clone())
        }

        async fn get_daily_bars_for_stock(
            &self,
            _code: &str,
            _start_date: NaiveDate,
            _end_date: NaiveDate,
        ) -> Result<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn get_trading_dates(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<NaiveDate>> {
            Ok(self.trading_dates.clone())
        }

        async fn get_limit_up_stocks(&self, _trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
            Ok(Vec::new())
        }

        async fn get_index_daily(
            &self,
            _code: &str,
            _trade_date: NaiveDate,
        ) -> Result<Option<IndexData>> {
            Ok(None)
        }

        async fn get_sector_data(&self, _trade_date: NaiveDate) -> Result<Vec<SectorData>> {
            Ok(Vec::new())
        }
    }

    struct FakePointInTimeProvider;

    #[async_trait]
    impl PointInTimeDataProvider for FakePointInTimeProvider {
        async fn probe_capabilities(&self) -> Result<PointInTimeCapabilities> {
            Ok(PointInTimeCapabilities {
                security_master_history: false,
                corporate_actions: false,
                adjustment_factors: false,
                daily_basic: false,
                daily_security_status: false,
                historical_index_bars: false,
                historical_sector_membership: false,
                details: BTreeMap::new(),
            })
        }

        async fn get_security_master_versions(&self) -> Result<Vec<SecurityMasterVersion>> {
            Ok(Vec::new())
        }

        async fn get_corporate_actions(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<CorporateAction>> {
            Ok(Vec::new())
        }

        async fn get_adjustment_factors(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<AdjustmentFactor>> {
            Ok(Vec::new())
        }

        async fn get_daily_basics(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<DailyBasicSnapshot>> {
            Ok(Vec::new())
        }

        async fn get_security_statuses(
            &self,
            _trade_date: NaiveDate,
        ) -> Result<Vec<SecurityDailyStatus>> {
            Ok(Vec::new())
        }

        async fn get_index_daily_range(
            &self,
            _codes: &[String],
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<IndexDailyBar>> {
            Ok(Vec::new())
        }

        async fn get_sector_memberships(
            &self,
            _as_of_date: NaiveDate,
        ) -> Result<Vec<SectorMembership>> {
            Ok(Vec::new())
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn full_backfill_upserts_current_state_without_writing_pit_versions(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let trade_date = date(2026, 7, 10);
        let provider = Arc::new(FakeProvider {
            trading_dates: vec![trade_date],
            stocks: vec![StockInfo {
                code: "600000.SH".to_string(),
                name: "Alpha".to_string(),
                market: "SH".to_string(),
                industry: Some("Bank".to_string()),
            }],
            bars: vec![(
                "600000.SH".to_string(),
                Candle {
                    trade_date,
                    open: 10.0,
                    high: 11.0,
                    low: 9.5,
                    close: 10.5,
                    volume: 123_456,
                    amount: 1_000_000.0,
                    turnover: Some(1.2),
                    pe: Some(10.0),
                    pb: Some(1.1),
                },
            )],
        });
        let state = test_state(pool.clone(), provider.clone()).await;
        let service = StockHistoryService::new(state, provider);

        service.backfill_full().await.unwrap();

        let current_row: (f64, f64, i64) = sqlx::query_as(
            r#"SELECT open::float8, close::float8, volume
               FROM stock_daily_bars
               WHERE code = '600000.SH' AND trade_date = '2026-07-10'"#,
        )
        .fetch_one(&pool)
        .await?;
        let version_count: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*)
               FROM stock_daily_bar_versions
               WHERE code = '600000.SH' AND trade_date = '2026-07-10'"#,
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(current_row, (10.0, 10.5, 123_456));
        assert_eq!(version_count.0, 0);
        Ok(())
    }

    async fn test_state(pool: PgPool, provider: Arc<dyn DataProvider>) -> Arc<AppState> {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let redis_client = redis::Client::open(redis_url).unwrap();
        let redis = redis::aio::ConnectionManager::new(redis_client)
            .await
            .unwrap();
        Arc::new(AppState {
            config: Arc::new(Config {
                tushare_token: "test".to_string(),
                database_url: "postgresql://qbot:qbot@127.0.0.1/qbot".to_string(),
                redis_url: "redis://127.0.0.1:6379".to_string(),
                telegram_bot_token: "test".to_string(),
                telegram_webhook_secret: None,
                webhook_url: None,
                stock_alert_channel: None,
                report_channel: None,
                daban_channel: None,
                api_port: 8080,
                api_key: Some("test-key".to_string()),
                dashboard_public_url: None,
                dashboard_username: None,
                dashboard_password_hash: None,
                dashboard_session_secret: None,
                ai_api_key: None,
                ai_base_url: "https://api.openai.com/v1".to_string(),
                ai_model: "gpt-4o-mini".to_string(),
                data_proxy: None,
                official_event_feed_url: None,
                official_event_feed_api_key: None,
                official_event_source_id: "official:market_event".to_string(),
                official_event_store_full_content: false,
                enable_gdelt_events: false,
                gdelt_event_query: String::new(),
                gdelt_max_records: 250,
                enable_burst_monitor: false,
                enable_daban_live: false,
                enable_ai_analysis: false,
                enable_chip_dist: false,
                enable_event_score_adjustment: false,
                max_event_score_adjustment: 0.0,
                enable_signal_auto_trading: false,
            }),
            db: pool,
            redis,
            provider,
            point_in_time_provider: Arc::new(FakePointInTimeProvider),
            pusher: Arc::new(TelegramPusher::new("test".to_string())),
            fetch_job_lock: Arc::new(Mutex::new(())),
            analysis_job_lock: Arc::new(Mutex::new(())),
            scan_job_lock: Arc::new(Mutex::new(())),
            daily_report_job_lock: Arc::new(Mutex::new(())),
            weekly_report_job_lock: Arc::new(Mutex::new(())),
        })
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }
}
