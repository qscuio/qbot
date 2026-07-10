use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::analysis::market_snapshot::{
    AdjustmentFactor, AvailabilityQuality, CorporateAction, DailyBasicSnapshot, IndexDailyBar,
    SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
};
use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
use crate::error::{AppError, Result};
use crate::storage::market_repository::{
    MarketRepository, POINT_IN_TIME_BACKFILL_RUN_TYPE, POINT_IN_TIME_REFERENCE_REFRESH_RUN_TYPE,
    POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
};

const DEFAULT_INDEX_CODES: [&str; 3] = ["000001.SH", "399001.SZ", "399006.SZ"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PointInTimeRefreshStatus {
    Ok,
    Partial,
    Failed,
}

impl PointInTimeRefreshStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Partial => "partial",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PointInTimeCategoryResult {
    pub fetched_rows: usize,
    pub inserted_rows: usize,
    pub skipped_unchanged_rows: usize,
    pub excluded_rows: usize,
    pub error: Option<String>,
}

impl PointInTimeCategoryResult {
    fn ok(fetched_rows: usize, inserted_rows: usize, skipped_unchanged_rows: usize) -> Self {
        Self {
            fetched_rows,
            inserted_rows,
            skipped_unchanged_rows,
            excluded_rows: 0,
            error: None,
        }
    }

    fn excluded(fetched_rows: usize, inserted_rows: usize, excluded_rows: usize) -> Self {
        Self {
            fetched_rows,
            inserted_rows,
            skipped_unchanged_rows: 0,
            excluded_rows,
            error: None,
        }
    }

    fn failed(error: String) -> Self {
        Self {
            fetched_rows: 0,
            inserted_rows: 0,
            skipped_unchanged_rows: 0,
            excluded_rows: 0,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PointInTimeRefreshResult {
    pub status: PointInTimeRefreshStatus,
    pub inserted_rows: usize,
    pub estimated_rows: usize,
    pub excluded_estimated_rows: usize,
    pub sensitivity_excludes_estimated: bool,
    pub categories: BTreeMap<String, PointInTimeCategoryResult>,
}

impl PointInTimeRefreshResult {
    fn new() -> Self {
        Self {
            status: PointInTimeRefreshStatus::Ok,
            inserted_rows: 0,
            estimated_rows: 0,
            excluded_estimated_rows: 0,
            sensitivity_excludes_estimated: false,
            categories: BTreeMap::new(),
        }
    }

    fn mark_partial(&mut self) {
        if self.status != PointInTimeRefreshStatus::Failed {
            self.status = PointInTimeRefreshStatus::Partial;
        }
    }

    fn mark_failed(&mut self) {
        self.status = PointInTimeRefreshStatus::Failed;
    }

    fn add_category(&mut self, name: &str, category: PointInTimeCategoryResult) {
        self.inserted_rows += category.inserted_rows;
        self.excluded_estimated_rows += category.excluded_rows;
        self.categories.insert(name.to_string(), category);
    }
}

#[derive(Clone)]
pub struct PointInTimeIngestion {
    provider: Arc<dyn PointInTimeDataProvider>,
    repo: MarketRepository,
}

impl PointInTimeIngestion {
    pub fn new(provider: Arc<dyn PointInTimeDataProvider>, pool: PgPool) -> Self {
        Self {
            provider,
            repo: MarketRepository::new(pool),
        }
    }

    pub async fn refresh_reference_data(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<PointInTimeRefreshResult> {
        let mut result = PointInTimeRefreshResult::new();
        let capabilities = match self.provider.probe_capabilities().await {
            Ok(capabilities) => capabilities,
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "capability_probe",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
                self.record_run(POINT_IN_TIME_REFERENCE_REFRESH_RUN_TYPE, None, &result)
                    .await?;
                return Ok(result);
            }
        };

        if !capabilities.security_master_history || !capabilities.historical_sector_membership {
            result.mark_failed();
            result.add_category(
                "readiness",
                PointInTimeCategoryResult::failed(missing_capabilities(
                    &capabilities,
                    &["security_master_history", "historical_sector_membership"],
                )),
            );
            self.record_run(POINT_IN_TIME_REFERENCE_REFRESH_RUN_TYPE, None, &result)
                .await?;
            return Ok(result);
        }

        match self.provider.get_security_master_versions().await {
            Ok(rows) => {
                let mut normalized = Vec::new();
                let mut skipped = 0;
                for mut row in rows.iter().cloned() {
                    row.ingested_at = as_of;
                    row.availability_quality = AvailabilityQuality::Observed;
                    if self
                        .repo
                        .latest_security_master_payload_unchanged(&row)
                        .await?
                    {
                        skipped += 1;
                    } else {
                        normalized.push(row);
                    }
                }
                let inserted = self
                    .repo
                    .append_security_master_versions(&normalized)
                    .await?;
                result.add_category(
                    "security_master_versions",
                    PointInTimeCategoryResult::ok(rows.len(), inserted, skipped),
                );
            }
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "security_master_versions",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
            }
        }

        let as_of_date = as_of.date_naive();
        let corporate_start = as_of_date - Duration::days(365);
        let corporate_end = as_of_date + Duration::days(365);
        if capabilities.corporate_actions {
            match self
                .provider
                .get_corporate_actions(corporate_start, corporate_end)
                .await
            {
                Ok(rows) => {
                    let mut normalized = Vec::new();
                    let mut skipped = 0;
                    for mut row in rows.iter().cloned() {
                        row.ingested_at = as_of;
                        row.availability_quality = AvailabilityQuality::Observed;
                        if self
                            .repo
                            .latest_corporate_action_payload_unchanged(&row)
                            .await?
                        {
                            skipped += 1;
                        } else {
                            normalized.push(row);
                        }
                    }
                    let inserted = self.repo.append_corporate_actions(&normalized).await?;
                    result.add_category(
                        "corporate_action_versions",
                        PointInTimeCategoryResult::ok(rows.len(), inserted, skipped),
                    );
                }
                Err(error) => {
                    result.mark_partial();
                    result.add_category(
                        "corporate_action_versions",
                        PointInTimeCategoryResult::failed(error.to_string()),
                    );
                }
            }
        } else {
            result.mark_partial();
            result.add_category(
                "corporate_action_versions",
                PointInTimeCategoryResult::failed(missing_capabilities(
                    &capabilities,
                    &["corporate_actions"],
                )),
            );
        }

        match self.provider.get_sector_memberships(as_of_date).await {
            Ok(rows) => {
                let mut normalized = Vec::new();
                let mut skipped = 0;
                for mut row in rows.iter().cloned() {
                    row.ingested_at = as_of;
                    row.availability_quality = AvailabilityQuality::Observed;
                    if self
                        .repo
                        .latest_sector_membership_payload_unchanged(&row)
                        .await?
                    {
                        skipped += 1;
                    } else {
                        normalized.push(row);
                    }
                }
                let inserted = self.repo.append_sector_memberships(&normalized).await?;
                result.add_category(
                    "stock_sector_membership",
                    PointInTimeCategoryResult::ok(rows.len(), inserted, skipped),
                );
            }
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "stock_sector_membership",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
            }
        }

        self.record_run(POINT_IN_TIME_REFERENCE_REFRESH_RUN_TYPE, None, &result)
            .await?;
        Ok(result)
    }

    pub async fn refresh_trade_date(
        &self,
        trade_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Result<PointInTimeRefreshResult> {
        let mut result = PointInTimeRefreshResult::new();
        let capabilities = match self.provider.probe_capabilities().await {
            Ok(capabilities) => capabilities,
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "capability_probe",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
                self.record_run(
                    POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
                    Some(trade_date),
                    &result,
                )
                .await?;
                return Ok(result);
            }
        };

        let required = [
            "daily_basic",
            "daily_security_status",
            "historical_index_bars",
            "adjustment_factors",
        ];
        if missing_required(&capabilities, &required) {
            result.mark_failed();
            result.add_category(
                "readiness",
                PointInTimeCategoryResult::failed(missing_capabilities(&capabilities, &required)),
            );
            self.record_run(
                POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
                Some(trade_date),
                &result,
            )
            .await?;
            return Ok(result);
        }

        match self.provider.get_daily_basics(trade_date).await {
            Ok(rows) => {
                let rows = normalize_daily_basics(rows, as_of, AvailabilityQuality::Observed);
                let inserted = self.repo.append_daily_basics(&rows).await?;
                result.add_category(
                    "stock_daily_basic_versions",
                    PointInTimeCategoryResult::ok(rows.len(), inserted, 0),
                );
            }
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "stock_daily_basic_versions",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
            }
        }

        match self.provider.get_security_statuses(trade_date).await {
            Ok(rows) => {
                let rows = normalize_security_statuses(rows, as_of, AvailabilityQuality::Observed);
                let inserted = self.repo.append_security_statuses(&rows).await?;
                result.add_category(
                    "security_daily_status",
                    PointInTimeCategoryResult::ok(rows.len(), inserted, 0),
                );
            }
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "security_daily_status",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
            }
        }

        let index_codes = default_index_codes();
        match self
            .provider
            .get_index_daily_range(&index_codes, trade_date, trade_date)
            .await
        {
            Ok(rows) => {
                let rows = normalize_index_bars(rows, as_of, AvailabilityQuality::Observed);
                let inserted = self.repo.append_index_bars(&rows).await?;
                result.add_category(
                    "index_daily_bars",
                    PointInTimeCategoryResult::ok(rows.len(), inserted, 0),
                );
            }
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "index_daily_bars",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
            }
        }

        match self
            .provider
            .get_adjustment_factors(trade_date, trade_date)
            .await
        {
            Ok(rows) => {
                let rows = normalize_adjustment_factors(rows, as_of, AvailabilityQuality::Observed);
                let inserted = self.repo.append_adjustment_factors(&rows).await?;
                result.add_category(
                    "stock_adjustment_factors",
                    PointInTimeCategoryResult::ok(rows.len(), inserted, 0),
                );
            }
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "stock_adjustment_factors",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
            }
        }

        let (daily_bars, sector_versions, limit_up_versions) =
            self.repo.task5_version_writes_exist(trade_date).await?;
        let missing_task5: Vec<&str> = [
            ("stock_daily_bar_versions", daily_bars),
            ("sector_daily_versions", sector_versions),
            ("limit_up_stock_versions", limit_up_versions),
        ]
        .into_iter()
        .filter_map(|(name, exists)| (!exists).then_some(name))
        .collect();
        if missing_task5.is_empty() {
            result.add_category(
                "task5_version_writes",
                PointInTimeCategoryResult::ok(3, 0, 0),
            );
        } else {
            result.mark_partial();
            result.add_category(
                "task5_version_writes",
                PointInTimeCategoryResult::failed(format!(
                    "missing required Task 5 version writes: {}",
                    missing_task5.join(", ")
                )),
            );
        }

        self.record_run(
            POINT_IN_TIME_TRADE_DATE_REFRESH_RUN_TYPE,
            Some(trade_date),
            &result,
        )
        .await?;
        Ok(result)
    }

    pub async fn backfill_range(
        &self,
        start: NaiveDate,
        end: NaiveDate,
        observed_at: DateTime<Utc>,
    ) -> Result<PointInTimeRefreshResult> {
        if start > end {
            return Err(AppError::Internal(format!(
                "invalid backfill range: {} is after {}",
                start, end
            )));
        }

        let mut result = PointInTimeRefreshResult::new();
        let capabilities = match self.provider.probe_capabilities().await {
            Ok(capabilities) => capabilities,
            Err(error) => {
                result.mark_failed();
                result.add_category(
                    "capability_probe",
                    PointInTimeCategoryResult::failed(error.to_string()),
                );
                self.record_run(POINT_IN_TIME_BACKFILL_RUN_TYPE, None, &result)
                    .await?;
                return Ok(result);
            }
        };

        let required = [
            "daily_basic",
            "daily_security_status",
            "historical_index_bars",
            "adjustment_factors",
            "security_master_history",
            "historical_sector_membership",
        ];
        if missing_required(&capabilities, &required) {
            result.mark_failed();
            result.add_category(
                "readiness",
                PointInTimeCategoryResult::failed(missing_capabilities(&capabilities, &required)),
            );
            self.record_run(POINT_IN_TIME_BACKFILL_RUN_TYPE, None, &result)
                .await?;
            return Ok(result);
        }

        for trade_date in date_range(start, end) {
            if capabilities.daily_basic {
                match self.provider.get_daily_basics(trade_date).await {
                    Ok(rows) => {
                        let rows = estimate_daily_basics(rows, observed_at);
                        result.estimated_rows += rows.len();
                        let inserted = self.repo.append_daily_basics(&rows).await?;
                        result.add_category(
                            &format!("stock_daily_basic_versions:{}", trade_date),
                            PointInTimeCategoryResult::ok(rows.len(), inserted, 0),
                        );
                    }
                    Err(error) => {
                        result.mark_failed();
                        result.add_category(
                            &format!("stock_daily_basic_versions:{}", trade_date),
                            PointInTimeCategoryResult::failed(error.to_string()),
                        );
                    }
                }
            }

            if capabilities.daily_security_status {
                match self.provider.get_security_statuses(trade_date).await {
                    Ok(rows) => {
                        let rows = estimate_security_statuses(rows, observed_at);
                        result.estimated_rows += rows.len();
                        let inserted = self.repo.append_security_statuses(&rows).await?;
                        result.add_category(
                            &format!("security_daily_status:{}", trade_date),
                            PointInTimeCategoryResult::ok(rows.len(), inserted, 0),
                        );
                    }
                    Err(error) => {
                        result.mark_failed();
                        result.add_category(
                            &format!("security_daily_status:{}", trade_date),
                            PointInTimeCategoryResult::failed(error.to_string()),
                        );
                    }
                }
            }

            if capabilities.historical_index_bars {
                let index_codes = default_index_codes();
                match self
                    .provider
                    .get_index_daily_range(&index_codes, trade_date, trade_date)
                    .await
                {
                    Ok(rows) => {
                        let rows = estimate_index_bars(rows, observed_at);
                        result.estimated_rows += rows.len();
                        let inserted = self.repo.append_index_bars(&rows).await?;
                        result.add_category(
                            &format!("index_daily_bars:{}", trade_date),
                            PointInTimeCategoryResult::ok(rows.len(), inserted, 0),
                        );
                    }
                    Err(error) => {
                        result.mark_failed();
                        result.add_category(
                            &format!("index_daily_bars:{}", trade_date),
                            PointInTimeCategoryResult::failed(error.to_string()),
                        );
                    }
                }
            }

            if capabilities.historical_sector_membership {
                match self.provider.get_sector_memberships(trade_date).await {
                    Ok(rows) => {
                        let fetched = rows.len();
                        let rows = estimate_sector_memberships(rows, observed_at);
                        let excluded = fetched.saturating_sub(rows.len());
                        result.estimated_rows += rows.len();
                        let inserted = self.repo.append_sector_memberships(&rows).await?;
                        result.add_category(
                            &format!("stock_sector_membership:{}", trade_date),
                            PointInTimeCategoryResult::excluded(fetched, inserted, excluded),
                        );
                    }
                    Err(error) => {
                        result.mark_partial();
                        result.add_category(
                            &format!("stock_sector_membership:{}", trade_date),
                            PointInTimeCategoryResult::failed(error.to_string()),
                        );
                    }
                }
            }
        }

        if capabilities.adjustment_factors {
            match self.provider.get_adjustment_factors(start, end).await {
                Ok(rows) => {
                    let rows = estimate_adjustment_factors(rows, observed_at);
                    result.estimated_rows += rows.len();
                    let inserted = self.repo.append_adjustment_factors(&rows).await?;
                    result.add_category(
                        "stock_adjustment_factors",
                        PointInTimeCategoryResult::ok(rows.len(), inserted, 0),
                    );
                }
                Err(error) => {
                    result.mark_failed();
                    result.add_category(
                        "stock_adjustment_factors",
                        PointInTimeCategoryResult::failed(error.to_string()),
                    );
                }
            }
        }

        if capabilities.security_master_history {
            match self.provider.get_security_master_versions().await {
                Ok(rows) => {
                    let fetched = rows.len();
                    let rows = estimate_security_master_versions(rows, observed_at);
                    let excluded = fetched.saturating_sub(rows.len());
                    result.estimated_rows += rows.len();
                    let inserted = self.repo.append_security_master_versions(&rows).await?;
                    result.add_category(
                        "security_master_versions",
                        PointInTimeCategoryResult::excluded(fetched, inserted, excluded),
                    );
                }
                Err(error) => {
                    result.mark_failed();
                    result.add_category(
                        "security_master_versions",
                        PointInTimeCategoryResult::failed(error.to_string()),
                    );
                }
            }
        }

        if capabilities.corporate_actions {
            match self.provider.get_corporate_actions(start, end).await {
                Ok(rows) => {
                    let fetched = rows.len();
                    let rows = estimate_corporate_actions(rows, observed_at);
                    let excluded = fetched.saturating_sub(rows.len());
                    result.estimated_rows += rows.len();
                    let inserted = self.repo.append_corporate_actions(&rows).await?;
                    result.add_category(
                        "corporate_action_versions",
                        PointInTimeCategoryResult::excluded(fetched, inserted, excluded),
                    );
                }
                Err(error) => {
                    result.mark_partial();
                    result.add_category(
                        "corporate_action_versions",
                        PointInTimeCategoryResult::failed(error.to_string()),
                    );
                }
            }
        }

        result.sensitivity_excludes_estimated = result.estimated_rows > 0;
        self.record_run(POINT_IN_TIME_BACKFILL_RUN_TYPE, None, &result)
            .await?;
        Ok(result)
    }

    async fn record_run(
        &self,
        run_type: &str,
        trade_date: Option<NaiveDate>,
        result: &PointInTimeRefreshResult,
    ) -> Result<()> {
        let details = serde_json::to_value(result)?;
        let error_message = result.categories.iter().find_map(|(name, category)| {
            category
                .error
                .as_ref()
                .map(|error| format!("{name}: {error}"))
        });
        self.repo
            .record_analysis_data_run(
                run_type,
                trade_date,
                result.status.as_str(),
                details,
                error_message,
            )
            .await
    }
}

fn default_index_codes() -> Vec<String> {
    DEFAULT_INDEX_CODES
        .iter()
        .map(|code| (*code).to_string())
        .collect()
}

fn missing_required(capabilities: &PointInTimeCapabilities, required: &[&str]) -> bool {
    required
        .iter()
        .any(|capability| !capability_supported(capabilities, capability))
}

fn missing_capabilities(capabilities: &PointInTimeCapabilities, required: &[&str]) -> String {
    let missing: Vec<String> = required
        .iter()
        .filter(|capability| !capability_supported(capabilities, capability))
        .map(|capability| {
            capabilities
                .details
                .get(*capability)
                .map(|detail| format!("{capability}: {detail}"))
                .unwrap_or_else(|| (*capability).to_string())
        })
        .collect();
    format!("missing point-in-time capabilities: {}", missing.join(", "))
}

fn capability_supported(capabilities: &PointInTimeCapabilities, capability: &str) -> bool {
    match capability {
        "security_master_history" => capabilities.security_master_history,
        "corporate_actions" => capabilities.corporate_actions,
        "adjustment_factors" => capabilities.adjustment_factors,
        "daily_basic" => capabilities.daily_basic,
        "daily_security_status" => capabilities.daily_security_status,
        "historical_index_bars" => capabilities.historical_index_bars,
        "historical_sector_membership" => capabilities.historical_sector_membership,
        _ => false,
    }
}

fn normalize_daily_basics(
    rows: Vec<DailyBasicSnapshot>,
    ingested_at: DateTime<Utc>,
    quality: AvailabilityQuality,
) -> Vec<DailyBasicSnapshot> {
    rows.into_iter()
        .map(|mut row| {
            row.ingested_at = ingested_at;
            row.availability_quality = quality;
            row
        })
        .collect()
}

fn normalize_security_statuses(
    rows: Vec<SecurityDailyStatus>,
    ingested_at: DateTime<Utc>,
    quality: AvailabilityQuality,
) -> Vec<SecurityDailyStatus> {
    rows.into_iter()
        .map(|mut row| {
            row.ingested_at = ingested_at;
            row.availability_quality = quality;
            row
        })
        .collect()
}

fn normalize_index_bars(
    rows: Vec<IndexDailyBar>,
    ingested_at: DateTime<Utc>,
    quality: AvailabilityQuality,
) -> Vec<IndexDailyBar> {
    rows.into_iter()
        .map(|mut row| {
            row.ingested_at = ingested_at;
            row.availability_quality = quality;
            row
        })
        .collect()
}

fn normalize_adjustment_factors(
    rows: Vec<AdjustmentFactor>,
    ingested_at: DateTime<Utc>,
    quality: AvailabilityQuality,
) -> Vec<AdjustmentFactor> {
    rows.into_iter()
        .map(|mut row| {
            row.ingested_at = ingested_at;
            row.availability_quality = quality;
            row
        })
        .collect()
}

fn estimate_daily_basics(
    rows: Vec<DailyBasicSnapshot>,
    observed_at: DateTime<Utc>,
) -> Vec<DailyBasicSnapshot> {
    rows.into_iter()
        .map(|mut row| {
            row.available_at = conservative_estimated_availability(row.trade_date);
            row.ingested_at = observed_at;
            row.availability_quality = AvailabilityQuality::Estimated;
            row
        })
        .collect()
}

fn estimate_security_statuses(
    rows: Vec<SecurityDailyStatus>,
    observed_at: DateTime<Utc>,
) -> Vec<SecurityDailyStatus> {
    rows.into_iter()
        .map(|mut row| {
            row.available_at = conservative_estimated_availability(row.trade_date);
            row.ingested_at = observed_at;
            row.availability_quality = AvailabilityQuality::Estimated;
            row
        })
        .collect()
}

fn estimate_index_bars(rows: Vec<IndexDailyBar>, observed_at: DateTime<Utc>) -> Vec<IndexDailyBar> {
    rows.into_iter()
        .map(|mut row| {
            row.available_at = conservative_estimated_availability(row.trade_date);
            row.ingested_at = observed_at;
            row.availability_quality = AvailabilityQuality::Estimated;
            row
        })
        .collect()
}

fn estimate_adjustment_factors(
    rows: Vec<AdjustmentFactor>,
    observed_at: DateTime<Utc>,
) -> Vec<AdjustmentFactor> {
    rows.into_iter()
        .map(|mut row| {
            row.available_at = conservative_estimated_availability(row.trade_date);
            row.ingested_at = observed_at;
            row.availability_quality = AvailabilityQuality::Estimated;
            row
        })
        .collect()
}

fn estimate_security_master_versions(
    rows: Vec<SecurityMasterVersion>,
    observed_at: DateTime<Utc>,
) -> Vec<SecurityMasterVersion> {
    rows.into_iter()
        .filter_map(|mut row| {
            let source_date = row.delist_date.or(row.list_date)?;
            row.available_at = conservative_estimated_availability(source_date);
            row.ingested_at = observed_at;
            row.availability_quality = AvailabilityQuality::Estimated;
            Some(row)
        })
        .collect()
}

fn estimate_corporate_actions(
    rows: Vec<CorporateAction>,
    observed_at: DateTime<Utc>,
) -> Vec<CorporateAction> {
    rows.into_iter()
        .filter_map(|mut row| {
            let announcement_date = row.announcement_date?;
            row.available_at = conservative_estimated_availability(announcement_date);
            row.ingested_at = observed_at;
            row.availability_quality = AvailabilityQuality::Estimated;
            Some(row)
        })
        .collect()
}

fn estimate_sector_memberships(
    rows: Vec<SectorMembership>,
    observed_at: DateTime<Utc>,
) -> Vec<SectorMembership> {
    rows.into_iter()
        .map(|mut row| {
            row.available_at = conservative_estimated_availability(row.valid_from);
            row.ingested_at = observed_at;
            row.availability_quality = AvailabilityQuality::Estimated;
            row
        })
        .collect()
}

fn conservative_estimated_availability(source_date: NaiveDate) -> DateTime<Utc> {
    let next = next_trading_day(source_date);
    Utc.with_ymd_and_hms(next.year(), next.month(), next.day(), 1, 0, 0)
        .single()
        .expect("09:00 Asia/Shanghai maps to a valid UTC timestamp")
}

fn next_trading_day(date: NaiveDate) -> NaiveDate {
    let mut candidate = date + Duration::days(1);
    while candidate.weekday().number_from_monday() > 5 {
        candidate += Duration::days(1);
    }
    candidate
}

fn date_range(start: NaiveDate, end: NaiveDate) -> impl Iterator<Item = NaiveDate> {
    let days = end.signed_duration_since(start).num_days();
    (0..=days).map(move |offset| start + Duration::days(offset))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use sqlx::PgPool;

    use super::super::{
        AdjustmentFactor, AvailabilityQuality, CorporateAction, DailyBasicSnapshot, IndexDailyBar,
        SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
    };
    use crate::analysis::market_snapshot::ingestion::{
        PointInTimeIngestion, PointInTimeRefreshStatus,
    };
    use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
    use crate::error::{AppError, Result};

    #[derive(Clone)]
    struct FakeProvider {
        state: Arc<Mutex<FakeProviderState>>,
    }

    #[derive(Clone)]
    struct FakeProviderState {
        capabilities: PointInTimeCapabilities,
        security_master: Vec<SecurityMasterVersion>,
        corporate_actions: Vec<CorporateAction>,
        adjustment_factors: Vec<AdjustmentFactor>,
        daily_basics: Vec<DailyBasicSnapshot>,
        security_statuses: Vec<SecurityDailyStatus>,
        index_bars: Vec<IndexDailyBar>,
        sector_memberships: Vec<SectorMembership>,
        corporate_actions_fail: bool,
    }

    impl FakeProvider {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeProviderState {
                    capabilities: all_capabilities(),
                    security_master: vec![security_master(
                        "600000.SH",
                        "Alpha",
                        dt(2026, 7, 10, 8),
                    )],
                    corporate_actions: vec![corporate_action(
                        dt(2026, 7, 10, 8),
                        Some(date(2026, 7, 1)),
                    )],
                    adjustment_factors: vec![adjustment_factor(
                        date(2026, 7, 10),
                        dt(2026, 7, 10, 8),
                    )],
                    daily_basics: vec![daily_basic(date(2026, 7, 10), dt(2026, 7, 10, 8))],
                    security_statuses: vec![security_status(date(2026, 7, 10), dt(2026, 7, 10, 8))],
                    index_bars: vec![index_bar(date(2026, 7, 10), dt(2026, 7, 10, 8))],
                    sector_memberships: vec![sector_membership(
                        date(2026, 7, 1),
                        dt(2026, 7, 10, 8),
                    )],
                    corporate_actions_fail: false,
                })),
            }
        }

        fn update(&self, update: impl FnOnce(&mut FakeProviderState)) {
            update(&mut self.state.lock().unwrap());
        }
    }

    #[async_trait]
    impl PointInTimeDataProvider for FakeProvider {
        async fn probe_capabilities(&self) -> Result<PointInTimeCapabilities> {
            Ok(self.state.lock().unwrap().capabilities.clone())
        }

        async fn get_security_master_versions(&self) -> Result<Vec<SecurityMasterVersion>> {
            Ok(self.state.lock().unwrap().security_master.clone())
        }

        async fn get_corporate_actions(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<CorporateAction>> {
            let state = self.state.lock().unwrap();
            if state.corporate_actions_fail {
                return Err(AppError::DataProvider(
                    "corporate actions unavailable".to_string(),
                ));
            }
            Ok(state.corporate_actions.clone())
        }

        async fn get_adjustment_factors(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Result<Vec<AdjustmentFactor>> {
            Ok(self.state.lock().unwrap().adjustment_factors.clone())
        }

        async fn get_daily_basics(&self, trade_date: NaiveDate) -> Result<Vec<DailyBasicSnapshot>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .daily_basics
                .iter()
                .filter(|row| row.trade_date == trade_date)
                .cloned()
                .collect())
        }

        async fn get_security_statuses(
            &self,
            trade_date: NaiveDate,
        ) -> Result<Vec<SecurityDailyStatus>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .security_statuses
                .iter()
                .filter(|row| row.trade_date == trade_date)
                .cloned()
                .collect())
        }

        async fn get_index_daily_range(
            &self,
            _codes: &[String],
            start: NaiveDate,
            end: NaiveDate,
        ) -> Result<Vec<IndexDailyBar>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .index_bars
                .iter()
                .filter(|row| row.trade_date >= start && row.trade_date <= end)
                .cloned()
                .collect())
        }

        async fn get_sector_memberships(
            &self,
            as_of_date: NaiveDate,
        ) -> Result<Vec<SectorMembership>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .sector_memberships
                .iter()
                .filter(|row| {
                    row.valid_from <= as_of_date
                        && row.valid_to.unwrap_or(NaiveDate::MAX) >= as_of_date
                })
                .cloned()
                .collect())
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn unsupported_critical_capability_returns_failed_readiness_result(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let provider = FakeProvider::new();
        provider.update(|state| state.capabilities.daily_basic = false);
        let ingestion = PointInTimeIngestion::new(Arc::new(provider), pool.clone());

        let result = ingestion
            .refresh_trade_date(date(2026, 7, 10), dt(2026, 7, 10, 18))
            .await
            .unwrap();

        assert_eq!(result.status, PointInTimeRefreshStatus::Failed);
        assert_eq!(result.inserted_rows, 0);
        assert_eq!(
            latest_run_status(&pool, "point_in_time_trade_date_refresh").await?,
            "failed"
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn repeated_reference_refresh_with_identical_available_at_is_idempotent(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let ingestion = PointInTimeIngestion::new(Arc::new(FakeProvider::new()), pool.clone());

        let first = ingestion
            .refresh_reference_data(dt(2026, 7, 10, 18))
            .await
            .unwrap();
        let second = ingestion
            .refresh_reference_data(dt(2026, 7, 10, 19))
            .await
            .unwrap();

        assert_eq!(first.status, PointInTimeRefreshStatus::Ok);
        assert_eq!(second.status, PointInTimeRefreshStatus::Ok);
        assert_eq!(second.inserted_rows, 0);
        assert_eq!(table_count(&pool, "security_master_versions").await?, 1);
        assert_eq!(table_count(&pool, "corporate_action_versions").await?, 1);
        assert_eq!(table_count(&pool, "stock_sector_membership").await?, 1);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn later_provider_observation_appends_new_version_when_payload_changes(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let provider = FakeProvider::new();
        let ingestion = PointInTimeIngestion::new(Arc::new(provider.clone()), pool.clone());

        ingestion
            .refresh_reference_data(dt(2026, 7, 10, 18))
            .await
            .unwrap();
        provider.update(|state| {
            state.security_master = vec![security_master("600000.SH", "Beta", dt(2026, 7, 11, 8))];
        });
        let result = ingestion
            .refresh_reference_data(dt(2026, 7, 11, 18))
            .await
            .unwrap();

        assert_eq!(result.status, PointInTimeRefreshStatus::Ok);
        assert_eq!(table_count(&pool, "security_master_versions").await?, 2);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn corporate_action_failure_does_not_erase_successfully_fetched_daily_basics(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let provider = FakeProvider::new();
        provider.update(|state| state.corporate_actions_fail = true);
        let ingestion = PointInTimeIngestion::new(Arc::new(provider), pool.clone());

        let result = ingestion
            .backfill_range(date(2026, 7, 10), date(2026, 7, 10), dt(2026, 7, 12, 12))
            .await
            .unwrap();

        assert_eq!(result.status, PointInTimeRefreshStatus::Partial);
        assert_eq!(table_count(&pool, "stock_daily_basic_versions").await?, 1);
        assert_eq!(
            latest_run_status(&pool, "point_in_time_backfill").await?,
            "partial"
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn non_critical_category_failure_records_partial_run_status(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let provider = FakeProvider::new();
        provider.update(|state| state.corporate_actions_fail = true);
        let ingestion = PointInTimeIngestion::new(Arc::new(provider), pool.clone());

        let result = ingestion
            .refresh_reference_data(dt(2026, 7, 10, 18))
            .await
            .unwrap();

        assert_eq!(result.status, PointInTimeRefreshStatus::Partial);
        assert_eq!(
            latest_run_status(&pool, "point_in_time_reference_refresh").await?,
            "partial"
        );
        assert_eq!(table_count(&pool, "security_master_versions").await?, 1);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn backfill_uses_conservative_estimated_availability_and_reports_exclusions(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let provider = FakeProvider::new();
        provider.update(|state| {
            state.corporate_actions = vec![
                corporate_action(dt(2026, 7, 10, 8), Some(date(2026, 7, 10))),
                corporate_action(dt(2026, 7, 10, 8), None),
            ];
        });
        let ingestion = PointInTimeIngestion::new(Arc::new(provider), pool.clone());

        let result = ingestion
            .backfill_range(date(2026, 7, 10), date(2026, 7, 10), dt(2026, 7, 12, 12))
            .await
            .unwrap();

        assert_eq!(result.status, PointInTimeRefreshStatus::Ok);
        assert!(result.estimated_rows > 0);
        assert!(result.excluded_estimated_rows > 0);
        assert!(result.sensitivity_excludes_estimated);

        let row: (DateTime<Utc>, DateTime<Utc>, String) = sqlx::query_as(
            r#"SELECT available_at, ingested_at, availability_quality
               FROM stock_daily_basic_versions
               WHERE code = '600000.SH' AND trade_date = '2026-07-10'"#,
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(row.0, dt(2026, 7, 13, 1));
        assert_eq!(row.1, dt(2026, 7, 12, 12));
        assert_eq!(row.2, "estimated");
        Ok(())
    }

    fn all_capabilities() -> PointInTimeCapabilities {
        PointInTimeCapabilities {
            security_master_history: true,
            corporate_actions: true,
            adjustment_factors: true,
            daily_basic: true,
            daily_security_status: true,
            historical_index_bars: true,
            historical_sector_membership: true,
            details: BTreeMap::new(),
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    fn security_master(
        code: &str,
        name: &str,
        available_at: DateTime<Utc>,
    ) -> SecurityMasterVersion {
        SecurityMasterVersion {
            code: code.to_string(),
            name: name.to_string(),
            market: Some("Main".to_string()),
            exchange: Some("SSE".to_string()),
            list_status: "L".to_string(),
            list_date: Some(date(2000, 1, 1)),
            delist_date: None,
            available_at,
            ingested_at: available_at,
            availability_quality: AvailabilityQuality::Observed,
            source: "fake".to_string(),
        }
    }

    fn corporate_action(
        available_at: DateTime<Utc>,
        announcement_date: Option<NaiveDate>,
    ) -> CorporateAction {
        CorporateAction {
            source: "fake".to_string(),
            action_key: format!(
                "600000.SH-div-{}",
                announcement_date
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "missing".to_string())
            ),
            code: "600000.SH".to_string(),
            action_type: "cash_dividend".to_string(),
            announcement_date,
            record_date: Some(date(2026, 7, 10)),
            ex_date: Some(date(2026, 7, 10)),
            pay_date: Some(date(2026, 7, 20)),
            cash_dividend: Some(0.1),
            stock_ratio: None,
            rights_ratio: None,
            rights_price: None,
            available_at,
            ingested_at: available_at,
            availability_quality: AvailabilityQuality::Observed,
        }
    }

    fn adjustment_factor(trade_date: NaiveDate, available_at: DateTime<Utc>) -> AdjustmentFactor {
        AdjustmentFactor {
            code: "600000.SH".to_string(),
            trade_date,
            adj_factor: 1.2,
            available_at,
            ingested_at: available_at,
            availability_quality: AvailabilityQuality::Observed,
            source: "fake".to_string(),
        }
    }

    fn daily_basic(trade_date: NaiveDate, available_at: DateTime<Utc>) -> DailyBasicSnapshot {
        DailyBasicSnapshot {
            code: "600000.SH".to_string(),
            trade_date,
            turnover_rate: Some(1.2),
            volume_ratio: Some(0.8),
            pe: Some(10.0),
            pb: Some(1.1),
            ps: Some(2.2),
            total_share: Some(100.0),
            float_share: Some(80.0),
            total_mv: Some(1000.0),
            circ_mv: Some(800.0),
            available_at,
            ingested_at: available_at,
            availability_quality: AvailabilityQuality::Observed,
            source: "fake".to_string(),
        }
    }

    fn security_status(trade_date: NaiveDate, available_at: DateTime<Utc>) -> SecurityDailyStatus {
        SecurityDailyStatus {
            code: "600000.SH".to_string(),
            trade_date,
            listed_days: Some(100),
            is_st: false,
            is_suspended: false,
            price_limit_pct: Some(10.0),
            available_at,
            ingested_at: available_at,
            availability_quality: AvailabilityQuality::Observed,
            source: "fake".to_string(),
        }
    }

    fn index_bar(trade_date: NaiveDate, available_at: DateTime<Utc>) -> IndexDailyBar {
        IndexDailyBar {
            code: "000001.SH".to_string(),
            trade_date,
            close: 3000.0,
            change_pct: Some(1.0),
            volume: Some(1000),
            amount: Some(10_000.0),
            available_at,
            ingested_at: available_at,
            availability_quality: AvailabilityQuality::Observed,
            source: "fake".to_string(),
        }
    }

    fn sector_membership(valid_from: NaiveDate, available_at: DateTime<Utc>) -> SectorMembership {
        SectorMembership {
            code: "600000.SH".to_string(),
            sector_code: "BANK".to_string(),
            sector_name: "Banking".to_string(),
            sector_type: "industry".to_string(),
            valid_from,
            valid_to: None,
            available_at,
            ingested_at: available_at,
            availability_quality: AvailabilityQuality::Observed,
            source: "fake".to_string(),
        }
    }

    async fn table_count(pool: &PgPool, table: &str) -> sqlx::Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM {}", table);
        let (count,): (i64,) = sqlx::query_as(&sql).fetch_one(pool).await?;
        Ok(count)
    }

    async fn latest_run_status(pool: &PgPool, run_type: &str) -> sqlx::Result<String> {
        let (status,): (String,) = sqlx::query_as(
            r#"SELECT status
               FROM analysis_data_runs
               WHERE run_type = $1
               ORDER BY started_at DESC
               LIMIT 1"#,
        )
        .bind(run_type)
        .fetch_one(pool)
        .await?;
        Ok(status)
    }
}
