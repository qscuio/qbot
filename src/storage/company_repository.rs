use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::data::company::{DividendRecord, FinancialFrequency, FinancialReport};
use crate::error::{AppError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinancialHistoryCursor {
    pub end_date: NaiveDate,
    pub report_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DividendHistoryCursor {
    pub dividend_date: NaiveDate,
    pub source: String,
    pub action_key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FinancialHistoryItem {
    pub report: FinancialReport,
    pub revision_count: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DividendHistoryItem {
    pub record: DividendRecord,
    pub revision_count: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FinancialHistoryPage {
    pub items: Vec<FinancialHistoryItem>,
    pub next_cursor: Option<FinancialHistoryCursor>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DividendHistoryPage {
    pub items: Vec<DividendHistoryItem>,
    pub next_cursor: Option<DividendHistoryCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct CompanyRepairCheckpoint {
    pub phase: String,
    pub code: String,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub status: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub lease_token: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointLease {
    pub phase: String,
    pub code: String,
    pub attempt: i32,
    pub token: Uuid,
}

#[derive(Debug, FromRow)]
struct FinancialHistoryRow {
    source: String,
    code: String,
    end_date: NaiveDate,
    announcement_date: Option<NaiveDate>,
    report_type: String,
    frequency: String,
    source_revision: String,
    total_revenue: Option<Decimal>,
    revenue: Option<Decimal>,
    operating_profit: Option<Decimal>,
    total_profit: Option<Decimal>,
    net_profit_parent: Option<Decimal>,
    deducted_net_profit: Option<Decimal>,
    basic_eps: Option<Decimal>,
    diluted_eps: Option<Decimal>,
    roe: Option<Decimal>,
    gross_margin: Option<Decimal>,
    net_margin: Option<Decimal>,
    revenue_yoy: Option<Decimal>,
    net_profit_yoy: Option<Decimal>,
    raw_payload: Value,
    available_at: DateTime<Utc>,
    ingested_at: DateTime<Utc>,
    revision_count: i64,
}

#[derive(Debug, FromRow)]
struct DividendHistoryRow {
    source: String,
    action_key: String,
    code: String,
    announcement_date: Option<NaiveDate>,
    record_date: Option<NaiveDate>,
    ex_date: Option<NaiveDate>,
    pay_date: Option<NaiveDate>,
    implementation_status: String,
    cash_dividend: Option<Decimal>,
    cash_dividend_tax: Option<Decimal>,
    stock_ratio: Option<Decimal>,
    source_revision: String,
    raw_payload: Value,
    available_at: DateTime<Utc>,
    ingested_at: DateTime<Utc>,
    revision_count: i64,
    dividend_date: NaiveDate,
}

#[derive(Debug, FromRow)]
struct StoredFinancialRevision {
    announcement_date: Option<NaiveDate>,
    frequency: String,
    total_revenue: Option<Decimal>,
    revenue: Option<Decimal>,
    operating_profit: Option<Decimal>,
    total_profit: Option<Decimal>,
    net_profit_parent: Option<Decimal>,
    deducted_net_profit: Option<Decimal>,
    basic_eps: Option<Decimal>,
    diluted_eps: Option<Decimal>,
    roe: Option<Decimal>,
    gross_margin: Option<Decimal>,
    net_margin: Option<Decimal>,
    revenue_yoy: Option<Decimal>,
    net_profit_yoy: Option<Decimal>,
    raw_payload: Value,
    available_at: DateTime<Utc>,
}

impl StoredFinancialRevision {
    fn matches(&self, report: &FinancialReport, available_at: DateTime<Utc>) -> bool {
        self.announcement_date == report.announcement_date
            && self.frequency == report.frequency.as_str()
            && self.total_revenue == report.total_revenue
            && self.revenue == report.revenue
            && self.operating_profit == report.operating_profit
            && self.total_profit == report.total_profit
            && self.net_profit_parent == report.net_profit_parent
            && self.deducted_net_profit == report.deducted_net_profit
            && self.basic_eps == report.basic_eps
            && self.diluted_eps == report.diluted_eps
            && self.roe == report.roe
            && self.gross_margin == report.gross_margin
            && self.net_margin == report.net_margin
            && self.revenue_yoy == report.revenue_yoy
            && self.net_profit_yoy == report.net_profit_yoy
            && self.raw_payload == report.raw_payload
            && self.available_at == available_at
    }
}

#[derive(Debug, FromRow)]
struct StoredDividendRevision {
    code: String,
    announcement_date: Option<NaiveDate>,
    record_date: Option<NaiveDate>,
    ex_date: Option<NaiveDate>,
    pay_date: Option<NaiveDate>,
    implementation_status: String,
    cash_dividend: Option<Decimal>,
    cash_dividend_tax: Option<Decimal>,
    stock_ratio: Option<Decimal>,
    raw_payload: Value,
    available_at: DateTime<Utc>,
}

impl StoredDividendRevision {
    fn matches(&self, record: &DividendRecord, available_at: DateTime<Utc>) -> bool {
        self.code == record.code
            && self.announcement_date == record.announcement_date
            && self.record_date == record.record_date
            && self.ex_date == record.ex_date
            && self.pay_date == record.pay_date
            && self.implementation_status == record.implementation_status
            && self.cash_dividend == record.cash_dividend
            && self.cash_dividend_tax == record.cash_dividend_tax
            && self.stock_ratio == record.stock_ratio
            && self.raw_payload == record.raw_payload
            && self.available_at == available_at
    }
}

fn postgres_timestamp(value: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(value.timestamp_micros())
        .expect("a valid UTC timestamp remains valid at PostgreSQL precision")
}

#[derive(Clone)]
pub struct CompanyRepository {
    pool: PgPool,
}

impl CompanyRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_financial_reports(&self, reports: &[FinancialReport]) -> Result<usize> {
        let mut transaction = self.pool.begin().await?;
        let mut inserted = 0;

        for report in reports {
            let available_at = postgres_timestamp(report.available_at);
            let ingested_at = postgres_timestamp(report.ingested_at);
            let rows_affected = sqlx::query(
                r#"INSERT INTO stock_financial_report_versions
                   (source, code, end_date, announcement_date, report_type, frequency,
                    source_revision, total_revenue, revenue, operating_profit, total_profit,
                    net_profit_parent, deducted_net_profit, basic_eps, diluted_eps, roe,
                    gross_margin, net_margin, revenue_yoy, net_profit_yoy, raw_payload,
                    available_at, ingested_at)
                   VALUES
                   ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                    $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
                   ON CONFLICT (source, code, end_date, report_type, source_revision)
                   DO NOTHING"#,
            )
            .bind(&report.source)
            .bind(&report.code)
            .bind(report.end_date)
            .bind(report.announcement_date)
            .bind(&report.report_type)
            .bind(report.frequency.as_str())
            .bind(&report.source_revision)
            .bind(report.total_revenue)
            .bind(report.revenue)
            .bind(report.operating_profit)
            .bind(report.total_profit)
            .bind(report.net_profit_parent)
            .bind(report.deducted_net_profit)
            .bind(report.basic_eps)
            .bind(report.diluted_eps)
            .bind(report.roe)
            .bind(report.gross_margin)
            .bind(report.net_margin)
            .bind(report.revenue_yoy)
            .bind(report.net_profit_yoy)
            .bind(&report.raw_payload)
            .bind(available_at)
            .bind(ingested_at)
            .execute(&mut *transaction)
            .await?
            .rows_affected();

            if rows_affected == 1 {
                inserted += 1;
                continue;
            }

            let stored = sqlx::query_as::<_, StoredFinancialRevision>(
                r#"SELECT announcement_date, frequency, total_revenue, revenue,
                          operating_profit, total_profit, net_profit_parent,
                          deducted_net_profit, basic_eps, diluted_eps, roe,
                          gross_margin, net_margin, revenue_yoy, net_profit_yoy,
                          raw_payload, available_at
                   FROM stock_financial_report_versions
                   WHERE source = $1 AND code = $2 AND end_date = $3
                     AND report_type = $4 AND source_revision = $5"#,
            )
            .bind(&report.source)
            .bind(&report.code)
            .bind(report.end_date)
            .bind(&report.report_type)
            .bind(&report.source_revision)
            .fetch_optional(&mut *transaction)
            .await?;

            if !stored.is_some_and(|row| row.matches(report, available_at)) {
                return Err(AppError::BadRequest(format!(
                    "immutable financial revision conflicts with stored history: {}/{}/{}/{}/{}",
                    report.source,
                    report.code,
                    report.end_date,
                    report.report_type,
                    report.source_revision
                )));
            }
        }

        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn upsert_dividends(&self, records: &[DividendRecord]) -> Result<usize> {
        let mut transaction = self.pool.begin().await?;
        let mut inserted = 0;

        for record in records {
            let available_at = postgres_timestamp(record.available_at);
            let ingested_at = postgres_timestamp(record.ingested_at);
            let rows_affected = sqlx::query(
                r#"INSERT INTO stock_dividend_versions
                   (source, action_key, code, announcement_date, record_date, ex_date,
                    pay_date, implementation_status, cash_dividend, cash_dividend_tax,
                    stock_ratio, source_revision, raw_payload, available_at, ingested_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                           $13, $14, $15)
                   ON CONFLICT (source, action_key, source_revision) DO NOTHING"#,
            )
            .bind(&record.source)
            .bind(&record.action_key)
            .bind(&record.code)
            .bind(record.announcement_date)
            .bind(record.record_date)
            .bind(record.ex_date)
            .bind(record.pay_date)
            .bind(&record.implementation_status)
            .bind(record.cash_dividend)
            .bind(record.cash_dividend_tax)
            .bind(record.stock_ratio)
            .bind(&record.source_revision)
            .bind(&record.raw_payload)
            .bind(available_at)
            .bind(ingested_at)
            .execute(&mut *transaction)
            .await?
            .rows_affected();

            if rows_affected == 1 {
                inserted += 1;
                continue;
            }

            let stored = sqlx::query_as::<_, StoredDividendRevision>(
                r#"SELECT code, announcement_date, record_date, ex_date, pay_date,
                          implementation_status, cash_dividend, cash_dividend_tax,
                          stock_ratio, raw_payload, available_at
                   FROM stock_dividend_versions
                   WHERE source = $1 AND action_key = $2 AND source_revision = $3"#,
            )
            .bind(&record.source)
            .bind(&record.action_key)
            .bind(&record.source_revision)
            .fetch_optional(&mut *transaction)
            .await?;

            if !stored.is_some_and(|row| row.matches(record, available_at)) {
                return Err(AppError::BadRequest(format!(
                    "immutable dividend revision conflicts with stored history: {}/{}/{}",
                    record.source, record.action_key, record.source_revision
                )));
            }
        }

        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn financial_history(
        &self,
        code: &str,
        frequency: FinancialFrequency,
        limit: usize,
        cursor: Option<FinancialHistoryCursor>,
    ) -> Result<FinancialHistoryPage> {
        let page_size = limit.clamp(1, 100);
        let cursor_end_date = cursor.as_ref().map(|value| value.end_date);
        let cursor_report_type = cursor.as_ref().map(|value| value.report_type.as_str());
        let rows = sqlx::query_as::<_, FinancialHistoryRow>(
            r#"WITH ranked AS (
                   SELECT source, code, end_date, announcement_date, report_type, frequency,
                          source_revision, total_revenue, revenue, operating_profit,
                          total_profit, net_profit_parent, deducted_net_profit, basic_eps,
                          diluted_eps, roe, gross_margin, net_margin, revenue_yoy,
                          net_profit_yoy, raw_payload, available_at, ingested_at,
                          COUNT(*) OVER (PARTITION BY end_date, report_type) AS revision_count,
                          ROW_NUMBER() OVER (
                            PARTITION BY end_date, report_type
                            ORDER BY available_at DESC, source, source_revision DESC
                          ) AS revision_rank
                   FROM stock_financial_report_versions
                   WHERE code = $1 AND frequency = $2
               )
               SELECT source, code, end_date, announcement_date, report_type, frequency,
                      source_revision, total_revenue, revenue, operating_profit,
                      total_profit, net_profit_parent, deducted_net_profit, basic_eps,
                      diluted_eps, roe, gross_margin, net_margin, revenue_yoy,
                      net_profit_yoy, raw_payload, available_at, ingested_at,
                      revision_count
               FROM ranked
               WHERE revision_rank = 1
                 AND ($3::date IS NULL OR (end_date, report_type) < ($3, $4))
               ORDER BY end_date DESC, report_type DESC
               LIMIT $5"#,
        )
        .bind(code)
        .bind(frequency.as_str())
        .bind(cursor_end_date)
        .bind(cursor_report_type)
        .bind((page_size + 1) as i64)
        .fetch_all(&self.pool)
        .await?;

        let has_more = rows.len() > page_size;
        let items = rows
            .into_iter()
            .take(page_size)
            .map(financial_history_item)
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = has_more.then(|| {
            let last = items.last().expect("a page with more rows is non-empty");
            FinancialHistoryCursor {
                end_date: last.report.end_date,
                report_type: last.report.report_type.clone(),
            }
        });

        Ok(FinancialHistoryPage { items, next_cursor })
    }

    pub async fn dividend_history(
        &self,
        code: &str,
        limit: usize,
        cursor: Option<DividendHistoryCursor>,
    ) -> Result<DividendHistoryPage> {
        let page_size = limit.clamp(1, 100);
        let cursor_date = cursor.as_ref().map(|value| value.dividend_date);
        let cursor_source = cursor.as_ref().map(|value| value.source.as_str());
        let cursor_action_key = cursor.as_ref().map(|value| value.action_key.as_str());
        let rows = sqlx::query_as::<_, DividendHistoryRow>(
            r#"WITH ranked AS (
                   SELECT source, action_key, code, announcement_date, record_date, ex_date,
                          pay_date, implementation_status, cash_dividend, cash_dividend_tax,
                          stock_ratio, source_revision, raw_payload, available_at, ingested_at,
                          COUNT(*) OVER (PARTITION BY source, action_key) AS revision_count,
                          ROW_NUMBER() OVER (
                            PARTITION BY source, action_key
                            ORDER BY available_at DESC, source, source_revision DESC
                          ) AS revision_rank
                   FROM stock_dividend_versions
                   WHERE code = $1
               ), latest AS (
                   SELECT *, COALESCE(ex_date, record_date, announcement_date,
                                      DATE '0001-01-01') AS dividend_date
                   FROM ranked
                   WHERE revision_rank = 1
               )
               SELECT source, action_key, code, announcement_date, record_date, ex_date,
                      pay_date, implementation_status, cash_dividend,
                      cash_dividend_tax, stock_ratio, source_revision,
                      raw_payload, available_at, ingested_at, revision_count, dividend_date
               FROM latest
               WHERE ($2::date IS NULL OR (dividend_date, source, action_key) < ($2, $3, $4))
               ORDER BY dividend_date DESC, source DESC, action_key DESC
               LIMIT $5"#,
        )
        .bind(code)
        .bind(cursor_date)
        .bind(cursor_source)
        .bind(cursor_action_key)
        .bind((page_size + 1) as i64)
        .fetch_all(&self.pool)
        .await?;

        let has_more = rows.len() > page_size;
        let items: Vec<_> = rows
            .into_iter()
            .take(page_size)
            .map(dividend_history_item)
            .collect();
        let next_cursor = has_more.then(|| {
            let last = items.last().expect("a page with more rows is non-empty");
            DividendHistoryCursor {
                dividend_date: last
                    .record
                    .ex_date
                    .or(last.record.record_date)
                    .or(last.record.announcement_date)
                    .unwrap_or_else(|| NaiveDate::from_ymd_opt(1, 1, 1).unwrap()),
                source: last.record.source.clone(),
                action_key: last.record.action_key.clone(),
            }
        });

        Ok(DividendHistoryPage { items, next_cursor })
    }

    pub async fn claim_checkpoint(
        &self,
        phase: &str,
        code: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<CheckpointLease> {
        let token = Uuid::new_v4();
        let claimed: Option<(i32, Uuid)> = sqlx::query_as(
            r#"INSERT INTO company_data_repair_checkpoints
               (phase, code, start_date, end_date, status, attempts, lease_token)
               VALUES ($1, $2, $3, $4, 'running', 1, $5)
               ON CONFLICT (phase, code) DO UPDATE SET
                 start_date = EXCLUDED.start_date,
                 end_date = EXCLUDED.end_date,
                 status = 'running',
                 attempts = company_data_repair_checkpoints.attempts + 1,
                 last_error = NULL,
                 lease_token = EXCLUDED.lease_token,
                 updated_at = NOW(),
                 completed_at = NULL
               WHERE company_data_repair_checkpoints.status <> 'running'
               RETURNING attempts, lease_token"#,
        )
        .bind(phase)
        .bind(code)
        .bind(start_date)
        .bind(end_date)
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;

        let (attempt, token) = claimed.ok_or_else(|| {
            AppError::BadRequest(format!("checkpoint already running: {phase}/{code}"))
        })?;
        Ok(CheckpointLease {
            phase: phase.to_string(),
            code: code.to_string(),
            attempt,
            token,
        })
    }

    pub async fn complete_checkpoint(&self, lease: &CheckpointLease) -> Result<()> {
        let rows_affected = sqlx::query(
            r#"UPDATE company_data_repair_checkpoints
               SET status = 'completed', last_error = NULL,
                   lease_token = NULL, updated_at = NOW(), completed_at = NOW()
               WHERE phase = $1 AND code = $2 AND status = 'running'
                 AND attempts = $3 AND lease_token = $4"#,
        )
        .bind(&lease.phase)
        .bind(&lease.code)
        .bind(lease.attempt)
        .bind(lease.token)
        .execute(&self.pool)
        .await?
        .rows_affected();
        checkpoint_transition_result(rows_affected, lease)
    }

    pub async fn fail_checkpoint(&self, lease: &CheckpointLease, error: &str) -> Result<()> {
        let bounded_error: String = error.chars().take(500).collect();
        let rows_affected = sqlx::query(
            r#"UPDATE company_data_repair_checkpoints
               SET status = 'failed', last_error = $3,
                   lease_token = NULL, updated_at = NOW(), completed_at = NULL
               WHERE phase = $1 AND code = $2 AND status = 'running'
                 AND attempts = $4 AND lease_token = $5"#,
        )
        .bind(&lease.phase)
        .bind(&lease.code)
        .bind(bounded_error)
        .bind(lease.attempt)
        .bind(lease.token)
        .execute(&self.pool)
        .await?
        .rows_affected();
        checkpoint_transition_result(rows_affected, lease)
    }

    pub async fn checkpoint(
        &self,
        phase: &str,
        code: &str,
    ) -> Result<Option<CompanyRepairCheckpoint>> {
        Ok(sqlx::query_as::<_, CompanyRepairCheckpoint>(
            r#"SELECT phase, code, start_date, end_date, status, attempts, last_error,
                      lease_token, created_at, updated_at, completed_at
               FROM company_data_repair_checkpoints
               WHERE phase = $1 AND code = $2"#,
        )
        .bind(phase)
        .bind(code)
        .fetch_optional(&self.pool)
        .await?)
    }
}

fn checkpoint_transition_result(rows_affected: u64, lease: &CheckpointLease) -> Result<()> {
    if rows_affected == 1 {
        Ok(())
    } else {
        Err(AppError::BadRequest(format!(
            "stale or missing checkpoint lease: {}/{} attempt {}",
            lease.phase, lease.code, lease.attempt
        )))
    }
}

fn financial_history_item(row: FinancialHistoryRow) -> Result<FinancialHistoryItem> {
    let frequency = FinancialFrequency::from_storage(&row.frequency).ok_or_else(|| {
        AppError::Internal(format!(
            "unsupported stored financial frequency: {}",
            row.frequency
        ))
    })?;
    Ok(FinancialHistoryItem {
        revision_count: row.revision_count,
        report: FinancialReport {
            source: row.source,
            code: row.code,
            end_date: row.end_date,
            announcement_date: row.announcement_date,
            report_type: row.report_type,
            frequency,
            source_revision: row.source_revision,
            total_revenue: row.total_revenue,
            revenue: row.revenue,
            operating_profit: row.operating_profit,
            total_profit: row.total_profit,
            net_profit_parent: row.net_profit_parent,
            deducted_net_profit: row.deducted_net_profit,
            basic_eps: row.basic_eps,
            diluted_eps: row.diluted_eps,
            roe: row.roe,
            gross_margin: row.gross_margin,
            net_margin: row.net_margin,
            revenue_yoy: row.revenue_yoy,
            net_profit_yoy: row.net_profit_yoy,
            raw_payload: row.raw_payload,
            available_at: row.available_at,
            ingested_at: row.ingested_at,
        },
    })
}

fn dividend_history_item(row: DividendHistoryRow) -> DividendHistoryItem {
    let _dividend_date = row.dividend_date;
    DividendHistoryItem {
        revision_count: row.revision_count,
        record: DividendRecord {
            source: row.source,
            action_key: row.action_key,
            code: row.code,
            announcement_date: row.announcement_date,
            record_date: row.record_date,
            ex_date: row.ex_date,
            pay_date: row.pay_date,
            implementation_status: row.implementation_status,
            cash_dividend: row.cash_dividend,
            cash_dividend_tax: row.cash_dividend_tax,
            stock_ratio: row.stock_ratio,
            source_revision: row.source_revision,
            raw_payload: row.raw_payload,
            available_at: row.available_at,
            ingested_at: row.ingested_at,
        },
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone, Utc};
    use rust_decimal::Decimal;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::{CheckpointLease, CompanyRepository};
    use crate::data::company::{DividendRecord, FinancialFrequency, FinancialReport};

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    fn dt_submicro(year: i32, month: u32, day: u32, hour: u32) -> chrono::DateTime<Utc> {
        dt(year, month, day, hour) + chrono::Duration::nanoseconds(789)
    }

    fn migration_022_sql() -> String {
        std::fs::read_to_string(format!(
            "{}/migrations/022_preserve_company_dividend_versions.sql",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap_or_default()
    }

    fn decimal(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    fn financial_report(source_revision: &str, announcement_day: u32) -> FinancialReport {
        FinancialReport {
            source: "tushare".to_string(),
            code: "600519.SH".to_string(),
            end_date: date(2025, 12, 31),
            announcement_date: Some(date(2026, 3, announcement_day)),
            report_type: "1".to_string(),
            frequency: FinancialFrequency::Annual,
            source_revision: source_revision.to_string(),
            total_revenue: Some(decimal("180000000000.0000")),
            revenue: Some(decimal("178000000000.0000")),
            operating_profit: Some(decimal("120000000000.0000")),
            total_profit: Some(decimal("121000000000.0000")),
            net_profit_parent: Some(decimal("86240000000.0000")),
            deducted_net_profit: Some(decimal("85000000000.0000")),
            basic_eps: Some(decimal("68.660000")),
            diluted_eps: Some(decimal("68.660000")),
            roe: Some(decimal("31.200000")),
            gross_margin: Some(decimal("91.400000")),
            net_margin: Some(decimal("48.000000")),
            revenue_yoy: Some(decimal("12.000000")),
            net_profit_yoy: Some(decimal("14.000000")),
            raw_payload: json!({"revision": source_revision}),
            available_at: dt(2026, 3, announcement_day, 8),
            ingested_at: dt(2026, 3, announcement_day, 9),
        }
    }

    fn dividend(source_revision: &str, cash_dividend: &str) -> DividendRecord {
        let cash_dividend = decimal(cash_dividend);
        DividendRecord {
            source: "tushare".to_string(),
            action_key: "600519.SH-2025-final".to_string(),
            code: "600519.SH".to_string(),
            announcement_date: Some(date(2026, 3, 30)),
            record_date: Some(date(2026, 6, 25)),
            ex_date: Some(date(2026, 6, 26)),
            pay_date: Some(date(2026, 6, 26)),
            implementation_status: "implemented".to_string(),
            cash_dividend: Some(cash_dividend),
            cash_dividend_tax: Some(cash_dividend),
            stock_ratio: None,
            source_revision: source_revision.to_string(),
            raw_payload: json!({"revision": source_revision}),
            available_at: dt(2026, 3, 30, 8),
            ingested_at: dt(2026, 3, 30, 8),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn financial_history_returns_only_latest_revision_and_counts_all_versions(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);
        let original = financial_report("v1", 20);
        let revision = financial_report("v2", 25);

        assert_eq!(
            repo.upsert_financial_reports(&[original.clone(), revision.clone()])
                .await?,
            2
        );
        assert_eq!(repo.upsert_financial_reports(&[revision]).await?, 0);

        let page = repo
            .financial_history("600519.SH", FinancialFrequency::Annual, 100, None)
            .await?;
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].report.source_revision, "v2");
        assert_eq!(page.items[0].revision_count, 2);
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn financial_history_clamps_pages_and_uses_period_and_report_type_cursor(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);
        let mut reports = Vec::new();
        for year in 1900..=2025 {
            let mut report = financial_report(&format!("v{year}"), 20);
            report.end_date = date(year, 12, 31);
            report.report_type = if year == 2025 { "2" } else { "1" }.to_string();
            reports.push(report);
        }
        let mut same_period = financial_report("same-period-type-1", 20);
        same_period.report_type = "1".to_string();
        reports.push(same_period);
        repo.upsert_financial_reports(&reports).await?;

        let first = repo
            .financial_history("600519.SH", FinancialFrequency::Annual, 500, None)
            .await?;
        assert_eq!(first.items.len(), 100);
        assert_eq!(first.items[0].report.end_date, date(2025, 12, 31));
        assert_eq!(first.items[0].report.report_type, "2");
        let cursor = first.next_cursor.expect("more financial history");

        let second = repo
            .financial_history("600519.SH", FinancialFrequency::Annual, 500, Some(cursor))
            .await?;
        assert_eq!(second.items.len(), 27);
        assert!(second.next_cursor.is_none());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dividend_upserts_are_revision_idempotent_and_history_is_latest_first(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool.clone());
        sqlx::query(
            r#"INSERT INTO corporate_action_versions
               (source, action_key, code, action_type, available_at, ingested_at,
                availability_quality)
               VALUES ('tushare', '600519.SH-2025-final', '600519.SH', 'cash_dividend',
                       $1, $1, 'observed')"#,
        )
        .bind(dt(2026, 3, 30, 8))
        .execute(&pool)
        .await?;

        let original = dividend("v1", "2.50000001");
        let revision = dividend("v2", "2.76000002");

        assert_eq!(
            repo.upsert_dividends(&[original.clone(), revision.clone()])
                .await?,
            2
        );
        assert_eq!(repo.upsert_dividends(&[revision]).await?, 0);

        let legacy_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM corporate_action_versions WHERE source = 'tushare'",
        )
        .fetch_one(&pool)
        .await?;
        let version_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM stock_dividend_versions WHERE source = 'tushare'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(legacy_count, 1);
        assert_eq!(version_count, 2);

        let page = repo.dividend_history("600519.SH", 100, None).await?;
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].record.source_revision, "v2");
        assert_eq!(
            page.items[0].record.cash_dividend,
            Some(decimal("2.76000002"))
        );
        assert_eq!(page.items[0].revision_count, 2);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn exact_financial_and_dividend_numerics_round_trip_without_float_loss(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);
        let mut report = financial_report("exact", 20);
        report.total_revenue = Some(decimal("12345678901234567890.1234"));
        report.roe = Some(decimal("0.123456"));
        let record = dividend("exact", "1234567890.12345678");

        repo.upsert_financial_reports(&[report]).await?;
        repo.upsert_dividends(&[record]).await?;

        let financials = repo
            .financial_history("600519.SH", FinancialFrequency::Annual, 10, None)
            .await?;
        assert_eq!(
            financials.items[0].report.total_revenue,
            Some(decimal("12345678901234567890.1234"))
        );
        assert_eq!(financials.items[0].report.roe, Some(decimal("0.123456")));

        let dividends = repo.dividend_history("600519.SH", 10, None).await?;
        assert_eq!(
            dividends.items[0].record.cash_dividend,
            Some(decimal("1234567890.12345678"))
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn financial_replay_preserves_ingestion_audit_and_conflicts_are_immutable(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool.clone());
        let original = financial_report("immutable", 20);
        repo.upsert_financial_reports(&[original.clone()]).await?;

        let mut replay = original.clone();
        replay.ingested_at = dt(2026, 4, 1, 12);
        assert_eq!(repo.upsert_financial_reports(&[replay]).await?, 0);

        let stored_ingested_at: chrono::DateTime<Utc> = sqlx::query_scalar(
            r#"SELECT ingested_at FROM stock_financial_report_versions
               WHERE source = $1 AND code = $2 AND end_date = $3
                 AND report_type = $4 AND source_revision = $5"#,
        )
        .bind(&original.source)
        .bind(&original.code)
        .bind(original.end_date)
        .bind(&original.report_type)
        .bind(&original.source_revision)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored_ingested_at, original.ingested_at);

        let mut conflict = original.clone();
        conflict.revenue = Some(decimal("177999999999.9999"));
        let error = repo
            .upsert_financial_reports(&[conflict])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("immutable financial revision"));

        let stored_revenue: Decimal = sqlx::query_scalar(
            r#"SELECT revenue FROM stock_financial_report_versions
               WHERE source = $1 AND code = $2 AND end_date = $3
                 AND report_type = $4 AND source_revision = $5"#,
        )
        .bind(&original.source)
        .bind(&original.code)
        .bind(original.end_date)
        .bind(&original.report_type)
        .bind(&original.source_revision)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored_revenue, original.revenue.unwrap());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dividend_replay_preserves_ingestion_audit_and_conflicts_are_immutable(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool.clone());
        let original = dividend("immutable", "2.76000002");
        repo.upsert_dividends(&[original.clone()]).await?;

        let mut replay = original.clone();
        replay.ingested_at = dt(2026, 4, 1, 12);
        assert_eq!(repo.upsert_dividends(&[replay]).await?, 0);

        let stored_ingested_at: chrono::DateTime<Utc> = sqlx::query_scalar(
            r#"SELECT ingested_at FROM stock_dividend_versions
               WHERE source = $1 AND action_key = $2 AND source_revision = $3"#,
        )
        .bind(&original.source)
        .bind(&original.action_key)
        .bind(&original.source_revision)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored_ingested_at, original.ingested_at);

        let mut conflict = original.clone();
        conflict.available_at = dt(2026, 3, 30, 9);
        let error = repo.upsert_dividends(&[conflict]).await.unwrap_err();
        assert!(error.to_string().contains("immutable dividend revision"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn financial_submicrosecond_timestamp_replay_is_a_noop(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool.clone());
        let mut original = financial_report("submicro", 20);
        original.available_at = dt_submicro(2026, 4, 1, 8);
        original.ingested_at = dt_submicro(2026, 4, 1, 9);

        assert_eq!(repo.upsert_financial_reports(&[original.clone()]).await?, 1);
        assert_eq!(repo.upsert_financial_reports(&[original.clone()]).await?, 0);

        let stored_ingested_at: chrono::DateTime<Utc> = sqlx::query_scalar(
            r#"SELECT ingested_at FROM stock_financial_report_versions
               WHERE source = $1 AND code = $2 AND end_date = $3
                 AND report_type = $4 AND source_revision = $5"#,
        )
        .bind(&original.source)
        .bind(&original.code)
        .bind(original.end_date)
        .bind(&original.report_type)
        .bind(&original.source_revision)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            stored_ingested_at,
            chrono::DateTime::from_timestamp_micros(original.ingested_at.timestamp_micros())
                .unwrap()
        );

        let mut changed_availability = original;
        changed_availability.available_at += chrono::Duration::microseconds(1);
        let error = repo
            .upsert_financial_reports(&[changed_availability])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("immutable financial revision"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dividend_submicrosecond_timestamp_replay_is_a_noop(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool.clone());
        let mut original = dividend("submicro", "2.76000002");
        original.available_at = dt_submicro(2026, 4, 1, 8);
        original.ingested_at = dt_submicro(2026, 4, 1, 9);

        assert_eq!(repo.upsert_dividends(&[original.clone()]).await?, 1);
        assert_eq!(repo.upsert_dividends(&[original.clone()]).await?, 0);

        let stored_ingested_at: chrono::DateTime<Utc> = sqlx::query_scalar(
            r#"SELECT ingested_at FROM stock_dividend_versions
               WHERE source = $1 AND action_key = $2 AND source_revision = $3"#,
        )
        .bind(&original.source)
        .bind(&original.action_key)
        .bind(&original.source_revision)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            stored_ingested_at,
            chrono::DateTime::from_timestamp_micros(original.ingested_at.timestamp_micros())
                .unwrap()
        );

        let mut changed_availability = original;
        changed_availability.available_at += chrono::Duration::microseconds(1);
        let error = repo
            .upsert_dividends(&[changed_availability])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("immutable dividend revision"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn immutable_conflict_rolls_back_earlier_inserts_in_the_batch(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool.clone());
        let original = financial_report("existing", 20);
        repo.upsert_financial_reports(&[original.clone()]).await?;

        let mut new_revision = financial_report("new-before-conflict", 21);
        new_revision.end_date = date(2024, 12, 31);
        let mut conflict = original;
        conflict.net_profit_parent = Some(decimal("1.0000"));

        let error = repo
            .upsert_financial_reports(&[new_revision, conflict])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("immutable financial revision"));

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM stock_financial_report_versions WHERE source_revision = 'new-before-conflict'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dividend_immutable_conflict_rolls_back_earlier_batch_insert(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool.clone());
        let original = dividend("existing", "2.76000002");
        repo.upsert_dividends(&[original.clone()]).await?;

        let mut new_action = dividend("new-before-conflict", "1.00000001");
        new_action.action_key = "600519.SH-new-action".to_string();
        let mut conflict = original;
        conflict.cash_dividend = Some(decimal("9.99999999"));

        let error = repo
            .upsert_dividends(&[new_action, conflict])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("immutable dividend revision"));

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM stock_dividend_versions WHERE action_key = '600519.SH-new-action'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn migration_022_preserves_nonlegacy_dividends_from_migration_020(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let migration_applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = 22 AND success)",
        )
        .fetch_one(&pool)
        .await?;
        assert!(
            migration_applied,
            "fresh databases must apply migration 022"
        );

        for (revision, cash, available_hour, ingested_hour) in [
            ("upgrade-v1", "2.50000001", 8, 9),
            ("upgrade-v2", "2.76000002", 10, 11),
        ] {
            sqlx::query(
                r#"INSERT INTO corporate_action_versions
                   (source, action_key, code, action_type, announcement_date, record_date,
                    ex_date, pay_date, implementation_status, cash_dividend,
                    cash_dividend_tax, stock_ratio, source_revision, raw_payload,
                    available_at, ingested_at, availability_quality)
                   VALUES ('migration020', '600519.SH-upgrade', '600519.SH', 'dividend',
                           $1, $2, $3, $4, 'implemented', $5, $6, $7, $8, $9, $10,
                           $11, 'observed')"#,
            )
            .bind(date(2026, 3, 30))
            .bind(date(2026, 6, 25))
            .bind(date(2026, 6, 26))
            .bind(date(2026, 6, 27))
            .bind(decimal(cash))
            .bind(decimal(cash))
            .bind(decimal("0.10000000"))
            .bind(revision)
            .bind(json!({"sourceRevision": revision, "preserved": true}))
            .bind(dt(2026, 4, 1, available_hour))
            .bind(dt(2026, 4, 1, ingested_hour))
            .execute(&pool)
            .await?;
        }

        let migration_sql = migration_022_sql();
        assert!(!migration_sql.is_empty(), "migration 022 must exist");
        sqlx::raw_sql(&migration_sql).execute(&pool).await?;
        sqlx::raw_sql(&migration_sql).execute(&pool).await?;

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM stock_dividend_versions WHERE source = 'migration020'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(count, 2);

        let preserved: (
            String,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            String,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            serde_json::Value,
            chrono::DateTime<Utc>,
            chrono::DateTime<Utc>,
        ) = sqlx::query_as(
            r#"SELECT code, announcement_date, record_date, ex_date, pay_date,
                      implementation_status, cash_dividend, cash_dividend_tax,
                      stock_ratio, raw_payload, available_at, ingested_at
               FROM stock_dividend_versions
               WHERE source = 'migration020' AND action_key = '600519.SH-upgrade'
                 AND source_revision = 'upgrade-v1'"#,
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(preserved.0, "600519.SH");
        assert_eq!(preserved.1, Some(date(2026, 3, 30)));
        assert_eq!(preserved.2, Some(date(2026, 6, 25)));
        assert_eq!(preserved.3, Some(date(2026, 6, 26)));
        assert_eq!(preserved.4, Some(date(2026, 6, 27)));
        assert_eq!(preserved.5, "implemented");
        assert_eq!(preserved.6, Some(decimal("2.50000001")));
        assert_eq!(preserved.7, Some(decimal("2.50000001")));
        assert_eq!(preserved.8, Some(decimal("0.10000000")));
        assert_eq!(
            preserved.9,
            json!({"sourceRevision": "upgrade-v1", "preserved": true})
        );
        assert_eq!(preserved.10, dt(2026, 4, 1, 8));
        assert_eq!(preserved.11, dt(2026, 4, 1, 9));

        let history = CompanyRepository::new(pool)
            .dividend_history("600519.SH", 10, None)
            .await?;
        assert_eq!(history.items.len(), 1);
        assert_eq!(history.items[0].record.source_revision, "upgrade-v2");
        assert_eq!(history.items[0].revision_count, 2);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn migration_022_rejects_changed_destination_conflicts(pool: PgPool) -> sqlx::Result<()> {
        sqlx::query(
            r#"INSERT INTO corporate_action_versions
               (source, action_key, code, action_type, implementation_status,
                cash_dividend, source_revision, raw_payload, available_at, ingested_at,
                availability_quality)
               VALUES ('migration020', 'conflict', '600519.SH', 'dividend', 'implemented',
                       2.76, 'v1', '{"side":"source"}', $1, $1, 'observed')"#,
        )
        .bind(dt(2026, 4, 1, 8))
        .execute(&pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO stock_dividend_versions
               (source, action_key, code, implementation_status, cash_dividend,
                source_revision, raw_payload, available_at, ingested_at)
               VALUES ('migration020', 'conflict', '600519.SH', 'implemented',
                       9.99, 'v1', '{"side":"destination"}', $1, $1)"#,
        )
        .bind(dt(2026, 4, 1, 8))
        .execute(&pool)
        .await?;

        let migration_sql = migration_022_sql();
        assert!(!migration_sql.is_empty(), "migration 022 must exist");
        let error = sqlx::raw_sql(&migration_sql)
            .execute(&pool)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("immutable dividend migration conflict"));

        let cash: Decimal = sqlx::query_scalar(
            r#"SELECT cash_dividend FROM stock_dividend_versions
               WHERE source = 'migration020' AND action_key = 'conflict'
                 AND source_revision = 'v1'"#,
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(cash, decimal("9.99"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn financial_latest_is_observation_first_and_ties_are_stable(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);
        let mut announced_later = financial_report("v1", 30);
        announced_later.available_at = dt(2026, 4, 1, 8);
        let mut correction = financial_report("v2", 10);
        correction.available_at = dt(2026, 4, 2, 8);

        let mut alpha_v1 = financial_report("v1", 20);
        alpha_v1.source = "alpha".to_string();
        alpha_v1.end_date = date(2024, 12, 31);
        alpha_v1.available_at = dt(2026, 4, 3, 8);
        let mut alpha_v2 = alpha_v1.clone();
        alpha_v2.source_revision = "v2".to_string();
        alpha_v2.raw_payload = json!({"revision": "v2"});
        let mut zeta = alpha_v1.clone();
        zeta.source = "zeta".to_string();
        zeta.source_revision = "v9".to_string();

        repo.upsert_financial_reports(&[announced_later, correction, alpha_v1, alpha_v2, zeta])
            .await?;
        let page = repo
            .financial_history("600519.SH", FinancialFrequency::Annual, 10, None)
            .await?;

        assert_eq!(page.items[0].report.source_revision, "v2");
        assert_eq!(
            page.items[0].report.announcement_date,
            Some(date(2026, 3, 10))
        );
        assert_eq!(page.items[1].report.source, "alpha");
        assert_eq!(page.items[1].report.source_revision, "v2");
        assert_eq!(page.items[1].revision_count, 3);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dividend_latest_is_observation_first_and_revision_ties_are_stable(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);
        let mut announced_later = dividend("v1", "2.50000000");
        announced_later.announcement_date = Some(date(2026, 3, 30));
        announced_later.available_at = dt(2026, 4, 1, 8);
        let mut correction = dividend("v2", "2.76000000");
        correction.announcement_date = Some(date(2026, 3, 10));
        correction.available_at = dt(2026, 4, 2, 8);
        let mut tied = dividend("v3", "2.88000000");
        tied.announcement_date = Some(date(2026, 3, 1));
        tied.available_at = correction.available_at;

        repo.upsert_dividends(&[announced_later, correction, tied])
            .await?;
        let page = repo.dividend_history("600519.SH", 10, None).await?;
        assert_eq!(page.items[0].record.source_revision, "v3");
        assert_eq!(
            page.items[0].record.announcement_date,
            Some(date(2026, 3, 1))
        );
        assert_eq!(page.items[0].revision_count, 3);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dividend_cursor_keeps_same_date_and_null_date_actions(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);
        let mut records = Vec::new();
        for suffix in ["a", "b", "c"] {
            let mut record = dividend("v1", "1.00000001");
            record.action_key = format!("same-date-{suffix}");
            records.push(record);
        }
        for suffix in ["a", "b"] {
            let mut record = dividend("v1", "1.00000001");
            record.action_key = format!("null-date-{suffix}");
            record.announcement_date = None;
            record.record_date = None;
            record.ex_date = None;
            record.pay_date = None;
            records.push(record);
        }
        repo.upsert_dividends(&records).await?;

        let mut cursor = None;
        let mut action_keys = Vec::new();
        loop {
            let page = repo.dividend_history("600519.SH", 1, cursor).await?;
            action_keys.extend(page.items.into_iter().map(|item| item.record.action_key));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(action_keys.len(), 5);
        action_keys.sort();
        action_keys.dedup();
        assert_eq!(action_keys.len(), 5);
        assert!(action_keys.contains(&"null-date-a".to_string()));
        assert!(action_keys.contains(&"null-date-b".to_string()));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn checkpoint_lifecycle_counts_claims_and_bounds_failures(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);

        let first_lease = repo
            .claim_checkpoint(
                "financials",
                "600519.SH",
                Some(date(1998, 1, 1)),
                Some(date(2026, 12, 31)),
            )
            .await?;
        repo.fail_checkpoint(&first_lease, &"超".repeat(600))
            .await?;

        let failed = repo.checkpoint("financials", "600519.SH").await?.unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.attempts, 1);
        assert_eq!(failed.last_error.unwrap().chars().count(), 500);

        let second_lease = repo
            .claim_checkpoint(
                "financials",
                "600519.SH",
                Some(date(1998, 1, 1)),
                Some(date(2026, 12, 31)),
            )
            .await?;
        repo.complete_checkpoint(&second_lease).await?;
        let completed = repo.checkpoint("financials", "600519.SH").await?.unwrap();
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.attempts, 2);
        assert!(completed.last_error.is_none());
        assert!(completed.completed_at.is_some());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn checkpoint_rejects_active_stealing_stale_workers_and_missing_leases(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);
        let first = repo
            .claim_checkpoint(
                "dividends",
                "600519.SH",
                Some(date(1998, 1, 1)),
                Some(date(2026, 12, 31)),
            )
            .await?;

        let active_error = repo
            .claim_checkpoint(
                "dividends",
                "600519.SH",
                Some(date(1998, 1, 1)),
                Some(date(2026, 12, 31)),
            )
            .await
            .unwrap_err();
        assert!(active_error.to_string().contains("already running"));

        repo.fail_checkpoint(&first, "retry").await?;
        let second = repo
            .claim_checkpoint(
                "dividends",
                "600519.SH",
                Some(date(1998, 1, 1)),
                Some(date(2026, 12, 31)),
            )
            .await?;
        assert_eq!(second.attempt, 2);

        let stale_error = repo.complete_checkpoint(&first).await.unwrap_err();
        assert!(stale_error
            .to_string()
            .contains("stale or missing checkpoint lease"));
        repo.complete_checkpoint(&second).await?;

        let missing = CheckpointLease {
            phase: "financials".to_string(),
            code: "missing.SH".to_string(),
            attempt: 1,
            token: Uuid::new_v4(),
        };
        let missing_error = repo.fail_checkpoint(&missing, "missing").await.unwrap_err();
        assert!(missing_error
            .to_string()
            .contains("stale or missing checkpoint lease"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn company_schema_separates_dividend_revisions_and_guards_checkpoint_leases(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let (dividend_table, latest_index, effective_date_index, lease_column): (
            bool,
            bool,
            bool,
            bool,
        ) = sqlx::query_as(
            r#"SELECT to_regclass('stock_dividend_versions') IS NOT NULL,
                      to_regclass('idx_dividend_versions_latest') IS NOT NULL,
                      to_regclass('idx_dividend_versions_effective_date') IS NOT NULL,
                      EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'company_data_repair_checkpoints'
                          AND column_name = 'lease_token'
                      )"#,
        )
        .fetch_one(&pool)
        .await?;

        assert!(dividend_table);
        assert!(latest_index);
        assert!(effective_date_index);
        assert!(lease_column);

        let (constraint_definition,): (String,) = sqlx::query_as(
            r#"SELECT pg_get_constraintdef(oid)
               FROM pg_constraint
               WHERE conname = 'company_repair_checkpoint_state_consistent'"#,
        )
        .fetch_one(&pool)
        .await?;
        assert!(constraint_definition.contains("lease_token"));
        assert!(constraint_definition.contains("completed_at"));
        Ok(())
    }
}
