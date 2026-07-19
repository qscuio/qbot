use chrono::{Datelike, NaiveDate};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::data::types::Candle;
use crate::error::{AppError, Result};
use crate::market_time::{beijing_now, is_a_share_trading_now};
use crate::services::scan_ranker::{ranked_pool_meta, RANKED_POOL_IDS};
use crate::signals::registry::SignalRegistry;
use crate::state::AppState;
use crate::storage::dashboard_repository::{DashboardRepository, ScanHitRow};
use crate::storage::postgres;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardSignalDefinition {
    pub id: String,
    pub name: String,
    pub icon: String,
    pub group: String,
    pub is_ranked_pool: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardSignalHit {
    pub signal_id: String,
    pub name: String,
    pub icon: String,
    pub group: String,
    pub is_ranked_pool: bool,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardStockRow {
    pub code: String,
    pub name: String,
    pub close: Option<f64>,
    pub change_pct: Option<f64>,
    pub trade_date: Option<NaiveDate>,
    pub partial: bool,
    pub hits: Vec<DashboardSignalHit>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DashboardSummary {
    pub unique_stocks: usize,
    pub total_hits: usize,
    pub active_signals: usize,
    pub ranked_candidates: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScanFreshness {
    NeverScanned,
    Fresh,
    Stale,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardBootstrap {
    pub server_time: String,
    pub market_open: bool,
    pub run_id: Option<Uuid>,
    pub scanned_at: Option<String>,
    pub freshness: ScanFreshness,
    pub summary: DashboardSummary,
    pub catalog: Vec<DashboardSignalDefinition>,
    pub results: Vec<DashboardStockRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardPeriod {
    Daily,
    Weekly,
    Monthly,
}

impl DashboardPeriod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        }
    }
}

impl FromStr for DashboardPeriod {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "daily" => Ok(Self::Daily),
            "weekly" => Ok(Self::Weekly),
            "monthly" => Ok(Self::Monthly),
            _ => Err(AppError::BadRequest(
                "period must be daily, weekly, or monthly".to_string(),
            )),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum HistorySource {
    RecentDaily(usize),
    CompleteWeekly,
    CompleteMonthly,
}

fn history_source(period: DashboardPeriod, requested_days: Option<usize>) -> HistorySource {
    match period {
        DashboardPeriod::Daily => {
            HistorySource::RecentDaily(requested_days.unwrap_or(500).clamp(30, 5_000))
        }
        DashboardPeriod::Weekly => HistorySource::CompleteWeekly,
        DashboardPeriod::Monthly => HistorySource::CompleteMonthly,
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardBar {
    pub time: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
    pub amount: f64,
}

impl From<&Candle> for DashboardBar {
    fn from(bar: &Candle) -> Self {
        Self {
            time: bar.trade_date.to_string(),
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
            amount: bar.amount,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardStockDetail {
    pub run_id: Option<Uuid>,
    pub code: String,
    pub name: String,
    pub period: String,
    pub partial: bool,
    pub latest: Option<DashboardBar>,
    pub bars: Vec<DashboardBar>,
    pub hits: Vec<DashboardSignalHit>,
}

#[derive(Clone)]
pub struct DashboardService {
    state: Arc<AppState>,
    repo: DashboardRepository,
}

impl DashboardService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self {
            repo: DashboardRepository::new(state.db.clone()),
            state,
        }
    }

    pub async fn bootstrap(&self) -> Result<DashboardBootstrap> {
        let catalog = signal_catalog();
        let Some(run) = self.repo.latest_completed_scan().await? else {
            return Ok(DashboardBootstrap {
                server_time: beijing_now().to_rfc3339(),
                market_open: is_a_share_trading_now(),
                run_id: None,
                scanned_at: None,
                freshness: ScanFreshness::NeverScanned,
                summary: DashboardSummary::default(),
                catalog,
                results: Vec::new(),
            });
        };

        let hits = self.repo.scan_hits(run.run_id).await?;
        let mut results = group_scan_hits(hits, &catalog);
        for row in &mut results {
            match postgres::get_stock_history(&self.state.db, &row.code, 2).await {
                Ok(bars) if !bars.is_empty() => {
                    let latest = bars.last().expect("non-empty history");
                    row.close = Some(latest.close);
                    row.trade_date = Some(latest.trade_date);
                    row.change_pct = bars
                        .iter()
                        .rev()
                        .nth(1)
                        .filter(|previous| previous.close != 0.0)
                        .map(|previous| (latest.close - previous.close) / previous.close * 100.0);
                }
                _ => row.partial = true,
            }
        }

        let latest_trade_date = postgres::latest_stock_trade_date(&self.state.db).await?;
        let completed_at = run.completed_at.unwrap_or(run.started_at);
        let freshness = match latest_trade_date {
            Some(date)
                if completed_at
                    .with_timezone(&crate::market_time::beijing_tz())
                    .date_naive()
                    < date =>
            {
                ScanFreshness::Stale
            }
            _ => ScanFreshness::Fresh,
        };

        let active_signals = results
            .iter()
            .flat_map(|row| row.hits.iter().map(|hit| hit.signal_id.as_str()))
            .collect::<HashSet<_>>()
            .len();
        let summary = DashboardSummary {
            unique_stocks: results.len(),
            total_hits: results.iter().map(|row| row.hits.len()).sum(),
            active_signals,
            ranked_candidates: results
                .iter()
                .filter(|row| row.hits.iter().any(|hit| hit.is_ranked_pool))
                .count(),
        };

        Ok(DashboardBootstrap {
            server_time: beijing_now().to_rfc3339(),
            market_open: is_a_share_trading_now(),
            run_id: Some(run.run_id),
            scanned_at: Some(completed_at.to_rfc3339()),
            freshness,
            summary,
            catalog,
            results,
        })
    }

    pub async fn stock_detail(
        &self,
        raw_code: &str,
        period: DashboardPeriod,
        requested_days: Option<usize>,
    ) -> Result<DashboardStockDetail> {
        let code = postgres::resolve_stock_code(&self.state.db, raw_code)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("stock {raw_code}")))?;
        let name = postgres::get_stock_name(&self.state.db, &code)
            .await?
            .unwrap_or_else(|| code.clone());
        let period_bars = match history_source(period, requested_days) {
            HistorySource::RecentDaily(days) => {
                postgres::get_stock_history(&self.state.db, &code, days).await?
            }
            HistorySource::CompleteWeekly => self.repo.weekly_history(&code).await?,
            HistorySource::CompleteMonthly => {
                postgres::get_stock_monthly_history(&self.state.db, &code).await?
            }
        };
        let partial = period_bars.is_empty();
        let latest = period_bars.last().map(DashboardBar::from);
        let bars = period_bars.iter().map(DashboardBar::from).collect();

        let run = self.repo.latest_completed_scan().await?;
        let hits = if let Some(run) = &run {
            group_scan_hits(self.repo.scan_hits(run.run_id).await?, &signal_catalog())
                .into_iter()
                .find(|row| row.code == code)
                .map(|row| row.hits)
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        Ok(DashboardStockDetail {
            run_id: run.map(|value| value.run_id),
            code,
            name,
            period: period.as_str().to_string(),
            partial,
            latest,
            bars,
            hits,
        })
    }
}

pub fn signal_catalog() -> Vec<DashboardSignalDefinition> {
    let mut catalog: Vec<_> = SignalRegistry::get_enabled()
        .into_iter()
        .map(|signal| DashboardSignalDefinition {
            id: signal.signal_id().to_string(),
            name: signal.display_name().to_string(),
            icon: signal.icon().to_string(),
            group: signal.group().to_string(),
            is_ranked_pool: false,
        })
        .collect();
    catalog.push(DashboardSignalDefinition {
        id: "multi_signal".to_string(),
        name: "多信号共振".to_string(),
        icon: "⭐".to_string(),
        group: "composite".to_string(),
        is_ranked_pool: false,
    });
    catalog.extend(ranked_pool_meta().into_iter().map(|(id, name, icon)| {
        DashboardSignalDefinition {
            id: id.to_string(),
            name: name.to_string(),
            icon: icon.to_string(),
            group: "ranked_pool".to_string(),
            is_ranked_pool: true,
        }
    }));
    catalog
}

pub fn group_scan_hits(
    rows: Vec<ScanHitRow>,
    catalog: &[DashboardSignalDefinition],
) -> Vec<DashboardStockRow> {
    let definitions: HashMap<_, _> = catalog
        .iter()
        .map(|item| (item.id.as_str(), item))
        .collect();
    let mut grouped: BTreeMap<String, DashboardStockRow> = BTreeMap::new();
    for row in rows {
        let definition = definitions.get(row.signal_id.as_str()).copied();
        let hit = DashboardSignalHit {
            signal_id: row.signal_id.clone(),
            name: definition
                .map(|item| item.name.clone())
                .unwrap_or_else(|| row.signal_id.clone()),
            icon: definition
                .map(|item| item.icon.clone())
                .unwrap_or_else(|| "•".to_string()),
            group: definition
                .map(|item| item.group.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            is_ranked_pool: definition
                .map(|item| item.is_ranked_pool)
                .unwrap_or_else(|| RANKED_POOL_IDS.contains(&row.signal_id.as_str())),
            metadata: row.metadata,
        };
        let entry = grouped
            .entry(row.code.clone())
            .or_insert_with(|| DashboardStockRow {
                code: row.code,
                name: row.name,
                close: None,
                change_pct: None,
                trade_date: None,
                partial: false,
                hits: Vec::new(),
            });
        entry.hits.push(hit);
    }
    grouped.into_values().collect()
}

pub fn resample_dashboard_bars(bars: &[Candle], period: DashboardPeriod) -> Vec<Candle> {
    if period == DashboardPeriod::Daily || bars.is_empty() {
        return bars.to_vec();
    }
    let mut output = Vec::new();
    let mut bucket: Vec<&Candle> = Vec::new();
    let mut active_key: Option<(i32, u32)> = None;
    for bar in bars {
        let key = match period {
            DashboardPeriod::Weekly => {
                let week = bar.trade_date.iso_week();
                (week.year(), week.week())
            }
            DashboardPeriod::Monthly => (bar.trade_date.year(), bar.trade_date.month()),
            DashboardPeriod::Daily => unreachable!(),
        };
        if active_key.is_some_and(|current| current != key) {
            output.push(aggregate_bucket(&bucket));
            bucket.clear();
        }
        active_key = Some(key);
        bucket.push(bar);
    }
    if !bucket.is_empty() {
        output.push(aggregate_bucket(&bucket));
    }
    output
}

fn aggregate_bucket(bucket: &[&Candle]) -> Candle {
    let first = bucket.first().expect("non-empty bucket");
    let last = bucket.last().expect("non-empty bucket");
    Candle {
        trade_date: last.trade_date,
        open: first.open,
        high: bucket
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max),
        low: bucket
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min),
        close: last.close,
        volume: bucket.iter().map(|bar| bar.volume).sum(),
        amount: bucket.iter().map(|bar| bar.amount).sum(),
        turnover: last.turnover,
        pe: last.pe,
        pb: last.pb,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use serde_json::json;

    fn hit(code: &str, signal_id: &str) -> ScanHitRow {
        ScanHitRow {
            code: code.to_string(),
            name: "贵州茅台".to_string(),
            signal_id: signal_id.to_string(),
            metadata: json!({"score": 88}),
        }
    }

    fn bar(day: u32, open: f64, high: f64, low: f64, close: f64) -> Candle {
        Candle {
            trade_date: NaiveDate::from_ymd_opt(2026, 7, day).unwrap(),
            open,
            high,
            low,
            close,
            volume: 100,
            amount: 1_000.0,
            turnover: None,
            pe: None,
            pb: None,
        }
    }

    #[test]
    fn groups_hits_by_stock_and_marks_ranked_pools() {
        let rows = vec![
            hit("600519.SH", "startup"),
            hit("600519.SH", "pool_short_a"),
        ];

        let grouped = group_scan_hits(rows, &signal_catalog());

        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].hits.len(), 2);
        assert!(grouped[0].hits.iter().any(|item| item.is_ranked_pool));
    }

    #[test]
    fn weekly_resampling_uses_first_open_last_close_and_extremes() {
        let bars = vec![
            bar(6, 10.0, 12.0, 9.0, 11.0),
            bar(7, 11.0, 13.0, 10.0, 12.0),
            bar(8, 12.0, 14.0, 8.0, 13.0),
        ];

        let weekly = resample_dashboard_bars(&bars, DashboardPeriod::Weekly);

        assert_eq!(weekly.len(), 1);
        assert_eq!(weekly[0].open, 10.0);
        assert_eq!(weekly[0].close, 13.0);
        assert_eq!(weekly[0].high, 14.0);
        assert_eq!(weekly[0].low, 8.0);
    }

    #[test]
    fn dashboard_history_source_keeps_weekly_unbounded() {
        assert_eq!(
            history_source(DashboardPeriod::Weekly, Some(30)),
            HistorySource::CompleteWeekly
        );
        assert_eq!(
            history_source(DashboardPeriod::Daily, None),
            HistorySource::RecentDaily(500)
        );
        assert_eq!(
            history_source(DashboardPeriod::Daily, Some(1)),
            HistorySource::RecentDaily(30)
        );
        assert_eq!(
            history_source(DashboardPeriod::Daily, Some(10_000)),
            HistorySource::RecentDaily(5_000)
        );
        assert_eq!(
            history_source(DashboardPeriod::Monthly, None),
            HistorySource::CompleteMonthly
        );
    }
}
