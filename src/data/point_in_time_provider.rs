use async_trait::async_trait;
use chrono::NaiveDate;
use std::collections::BTreeMap;

use crate::analysis::market_snapshot::{
    AdjustmentFactor, CorporateAction, DailyBasicSnapshot, IndexDailyBar, SectorMembership,
    SecurityDailyStatus, SecurityMasterVersion,
};
use crate::error::Result;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PointInTimeCapabilities {
    pub security_master_history: bool,
    pub corporate_actions: bool,
    pub adjustment_factors: bool,
    pub daily_basic: bool,
    pub daily_security_status: bool,
    pub historical_index_bars: bool,
    pub historical_sector_membership: bool,
    pub details: BTreeMap<String, String>,
}

#[async_trait]
pub trait PointInTimeDataProvider: Send + Sync {
    async fn probe_capabilities(&self) -> Result<PointInTimeCapabilities>;

    async fn get_security_master_versions(&self) -> Result<Vec<SecurityMasterVersion>>;

    async fn get_corporate_actions(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<CorporateAction>>;

    async fn get_adjustment_factors(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<AdjustmentFactor>>;

    async fn get_daily_basics(&self, trade_date: NaiveDate) -> Result<Vec<DailyBasicSnapshot>>;

    async fn get_security_statuses(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<SecurityDailyStatus>>;

    async fn get_index_daily_range(
        &self,
        codes: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<IndexDailyBar>>;

    async fn get_sector_memberships(&self, as_of_date: NaiveDate) -> Result<Vec<SectorMembership>>;
}
