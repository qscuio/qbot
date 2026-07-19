use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgPool};

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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
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
    total_revenue: Option<f64>,
    revenue: Option<f64>,
    operating_profit: Option<f64>,
    total_profit: Option<f64>,
    net_profit_parent: Option<f64>,
    deducted_net_profit: Option<f64>,
    basic_eps: Option<f64>,
    diluted_eps: Option<f64>,
    roe: Option<f64>,
    gross_margin: Option<f64>,
    net_margin: Option<f64>,
    revenue_yoy: Option<f64>,
    net_profit_yoy: Option<f64>,
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
    implementation_status: Option<String>,
    cash_dividend: Option<f64>,
    cash_dividend_tax: Option<f64>,
    stock_ratio: Option<f64>,
    source_revision: String,
    raw_payload: Value,
    available_at: DateTime<Utc>,
    ingested_at: DateTime<Utc>,
    revision_count: i64,
    dividend_date: NaiveDate,
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
        let mut changed = 0;

        for report in reports {
            changed += sqlx::query(
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
                   DO UPDATE SET
                     announcement_date = EXCLUDED.announcement_date,
                     frequency = EXCLUDED.frequency,
                     total_revenue = EXCLUDED.total_revenue,
                     revenue = EXCLUDED.revenue,
                     operating_profit = EXCLUDED.operating_profit,
                     total_profit = EXCLUDED.total_profit,
                     net_profit_parent = EXCLUDED.net_profit_parent,
                     deducted_net_profit = EXCLUDED.deducted_net_profit,
                     basic_eps = EXCLUDED.basic_eps,
                     diluted_eps = EXCLUDED.diluted_eps,
                     roe = EXCLUDED.roe,
                     gross_margin = EXCLUDED.gross_margin,
                     net_margin = EXCLUDED.net_margin,
                     revenue_yoy = EXCLUDED.revenue_yoy,
                     net_profit_yoy = EXCLUDED.net_profit_yoy,
                     raw_payload = EXCLUDED.raw_payload,
                     available_at = EXCLUDED.available_at,
                     ingested_at = EXCLUDED.ingested_at
                   WHERE (stock_financial_report_versions.announcement_date,
                          stock_financial_report_versions.frequency,
                          stock_financial_report_versions.total_revenue,
                          stock_financial_report_versions.revenue,
                          stock_financial_report_versions.operating_profit,
                          stock_financial_report_versions.total_profit,
                          stock_financial_report_versions.net_profit_parent,
                          stock_financial_report_versions.deducted_net_profit,
                          stock_financial_report_versions.basic_eps,
                          stock_financial_report_versions.diluted_eps,
                          stock_financial_report_versions.roe,
                          stock_financial_report_versions.gross_margin,
                          stock_financial_report_versions.net_margin,
                          stock_financial_report_versions.revenue_yoy,
                          stock_financial_report_versions.net_profit_yoy,
                          stock_financial_report_versions.raw_payload,
                          stock_financial_report_versions.available_at,
                          stock_financial_report_versions.ingested_at)
                         IS DISTINCT FROM
                         (EXCLUDED.announcement_date, EXCLUDED.frequency,
                          EXCLUDED.total_revenue, EXCLUDED.revenue,
                          EXCLUDED.operating_profit, EXCLUDED.total_profit,
                          EXCLUDED.net_profit_parent, EXCLUDED.deducted_net_profit,
                          EXCLUDED.basic_eps, EXCLUDED.diluted_eps, EXCLUDED.roe,
                          EXCLUDED.gross_margin, EXCLUDED.net_margin,
                          EXCLUDED.revenue_yoy, EXCLUDED.net_profit_yoy,
                          EXCLUDED.raw_payload, EXCLUDED.available_at, EXCLUDED.ingested_at)"#,
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
            .bind(report.available_at)
            .bind(report.ingested_at)
            .execute(&mut *transaction)
            .await?
            .rows_affected() as usize;
        }

        transaction.commit().await?;
        Ok(changed)
    }

    pub async fn upsert_dividends(&self, records: &[DividendRecord]) -> Result<usize> {
        let mut transaction = self.pool.begin().await?;
        let mut changed = 0;

        for record in records {
            let query = if record.source_revision == "legacy" {
                r#"INSERT INTO corporate_action_versions
                   (source, action_key, code, action_type, announcement_date, record_date,
                    ex_date, pay_date, implementation_status, cash_dividend,
                    cash_dividend_tax, stock_ratio, source_revision, raw_payload,
                    available_at, ingested_at, availability_quality)
                   VALUES ($1, $2, $3, 'dividend', $4, $5, $6, $7, $8, $9, $10,
                           $11, $12, $13, $14, $15, 'observed')
                   ON CONFLICT (source, action_key, available_at)
                   DO UPDATE SET
                     code = EXCLUDED.code, announcement_date = EXCLUDED.announcement_date,
                     record_date = EXCLUDED.record_date, ex_date = EXCLUDED.ex_date,
                     pay_date = EXCLUDED.pay_date,
                     implementation_status = EXCLUDED.implementation_status,
                     cash_dividend = EXCLUDED.cash_dividend,
                     cash_dividend_tax = EXCLUDED.cash_dividend_tax,
                     stock_ratio = EXCLUDED.stock_ratio, raw_payload = EXCLUDED.raw_payload,
                     ingested_at = EXCLUDED.ingested_at
                   WHERE (corporate_action_versions.code,
                          corporate_action_versions.announcement_date,
                          corporate_action_versions.record_date,
                          corporate_action_versions.ex_date,
                          corporate_action_versions.pay_date,
                          corporate_action_versions.implementation_status,
                          corporate_action_versions.cash_dividend,
                          corporate_action_versions.cash_dividend_tax,
                          corporate_action_versions.stock_ratio,
                          corporate_action_versions.raw_payload,
                          corporate_action_versions.ingested_at)
                         IS DISTINCT FROM
                         (EXCLUDED.code, EXCLUDED.announcement_date, EXCLUDED.record_date,
                          EXCLUDED.ex_date, EXCLUDED.pay_date,
                          EXCLUDED.implementation_status, EXCLUDED.cash_dividend,
                          EXCLUDED.cash_dividend_tax, EXCLUDED.stock_ratio,
                          EXCLUDED.raw_payload, EXCLUDED.ingested_at)"#
            } else {
                r#"INSERT INTO corporate_action_versions
                   (source, action_key, code, action_type, announcement_date, record_date,
                    ex_date, pay_date, implementation_status, cash_dividend,
                    cash_dividend_tax, stock_ratio, source_revision, raw_payload,
                    available_at, ingested_at, availability_quality)
                   VALUES ($1, $2, $3, 'dividend', $4, $5, $6, $7, $8, $9, $10,
                           $11, $12, $13, $14, $15, 'observed')
                   ON CONFLICT (source, action_key, source_revision)
                     WHERE source_revision <> 'legacy'
                   DO UPDATE SET
                     code = EXCLUDED.code, announcement_date = EXCLUDED.announcement_date,
                     record_date = EXCLUDED.record_date, ex_date = EXCLUDED.ex_date,
                     pay_date = EXCLUDED.pay_date,
                     implementation_status = EXCLUDED.implementation_status,
                     cash_dividend = EXCLUDED.cash_dividend,
                     cash_dividend_tax = EXCLUDED.cash_dividend_tax,
                     stock_ratio = EXCLUDED.stock_ratio, raw_payload = EXCLUDED.raw_payload,
                     available_at = EXCLUDED.available_at, ingested_at = EXCLUDED.ingested_at
                   WHERE (corporate_action_versions.code,
                          corporate_action_versions.announcement_date,
                          corporate_action_versions.record_date,
                          corporate_action_versions.ex_date,
                          corporate_action_versions.pay_date,
                          corporate_action_versions.implementation_status,
                          corporate_action_versions.cash_dividend,
                          corporate_action_versions.cash_dividend_tax,
                          corporate_action_versions.stock_ratio,
                          corporate_action_versions.raw_payload,
                          corporate_action_versions.available_at,
                          corporate_action_versions.ingested_at)
                         IS DISTINCT FROM
                         (EXCLUDED.code, EXCLUDED.announcement_date, EXCLUDED.record_date,
                          EXCLUDED.ex_date, EXCLUDED.pay_date,
                          EXCLUDED.implementation_status, EXCLUDED.cash_dividend,
                          EXCLUDED.cash_dividend_tax, EXCLUDED.stock_ratio,
                          EXCLUDED.raw_payload, EXCLUDED.available_at, EXCLUDED.ingested_at)"#
            };

            changed += sqlx::query(query)
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
                .bind(record.available_at)
                .bind(record.ingested_at)
                .execute(&mut *transaction)
                .await?
                .rows_affected() as usize;
        }

        transaction.commit().await?;
        Ok(changed)
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
                            ORDER BY announcement_date DESC NULLS LAST, available_at DESC
                          ) AS revision_rank
                   FROM stock_financial_report_versions
                   WHERE code = $1 AND frequency = $2
               )
               SELECT source, code, end_date, announcement_date, report_type, frequency,
                      source_revision, total_revenue::float8, revenue::float8,
                      operating_profit::float8, total_profit::float8,
                      net_profit_parent::float8, deducted_net_profit::float8,
                      basic_eps::float8, diluted_eps::float8, roe::float8,
                      gross_margin::float8, net_margin::float8, revenue_yoy::float8,
                      net_profit_yoy::float8, raw_payload, available_at, ingested_at,
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
                            ORDER BY announcement_date DESC NULLS LAST, available_at DESC,
                                     source_revision DESC
                          ) AS revision_rank
                   FROM corporate_action_versions
                   WHERE code = $1 AND action_type IN ('dividend', 'cash_dividend')
               ), latest AS (
                   SELECT *, COALESCE(ex_date, record_date, announcement_date,
                                      DATE '0001-01-01') AS dividend_date
                   FROM ranked
                   WHERE revision_rank = 1
               )
               SELECT source, action_key, code, announcement_date, record_date, ex_date,
                      pay_date, implementation_status, cash_dividend::float8,
                      cash_dividend_tax::float8, stock_ratio::float8, source_revision,
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
    ) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO company_data_repair_checkpoints
               (phase, code, start_date, end_date, status, attempts)
               VALUES ($1, $2, $3, $4, 'running', 1)
               ON CONFLICT (phase, code) DO UPDATE SET
                 start_date = EXCLUDED.start_date,
                 end_date = EXCLUDED.end_date,
                 status = 'running',
                 attempts = company_data_repair_checkpoints.attempts + 1,
                 last_error = NULL,
                 updated_at = NOW(),
                 completed_at = NULL"#,
        )
        .bind(phase)
        .bind(code)
        .bind(start_date)
        .bind(end_date)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn complete_checkpoint(&self, phase: &str, code: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE company_data_repair_checkpoints
               SET status = 'completed', last_error = NULL,
                   updated_at = NOW(), completed_at = NOW()
               WHERE phase = $1 AND code = $2"#,
        )
        .bind(phase)
        .bind(code)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_checkpoint(&self, phase: &str, code: &str, error: &str) -> Result<()> {
        let bounded_error: String = error.chars().take(500).collect();
        sqlx::query(
            r#"UPDATE company_data_repair_checkpoints
               SET status = 'failed', last_error = $3,
                   updated_at = NOW(), completed_at = NULL
               WHERE phase = $1 AND code = $2"#,
        )
        .bind(phase)
        .bind(code)
        .bind(bounded_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn checkpoint(
        &self,
        phase: &str,
        code: &str,
    ) -> Result<Option<CompanyRepairCheckpoint>> {
        Ok(sqlx::query_as::<_, CompanyRepairCheckpoint>(
            r#"SELECT phase, code, start_date, end_date, status, attempts, last_error,
                      created_at, updated_at, completed_at
               FROM company_data_repair_checkpoints
               WHERE phase = $1 AND code = $2"#,
        )
        .bind(phase)
        .bind(code)
        .fetch_optional(&self.pool)
        .await?)
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
            implementation_status: row
                .implementation_status
                .unwrap_or_else(|| "unknown".to_string()),
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
    use serde_json::json;
    use sqlx::PgPool;

    use super::CompanyRepository;
    use crate::data::company::{DividendRecord, FinancialFrequency, FinancialReport};

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
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
            total_revenue: Some(180_000_000_000.0),
            revenue: Some(178_000_000_000.0),
            operating_profit: Some(120_000_000_000.0),
            total_profit: Some(121_000_000_000.0),
            net_profit_parent: Some(86_240_000_000.0),
            deducted_net_profit: Some(85_000_000_000.0),
            basic_eps: Some(68.66),
            diluted_eps: Some(68.66),
            roe: Some(31.2),
            gross_margin: Some(91.4),
            net_margin: Some(48.0),
            revenue_yoy: Some(12.0),
            net_profit_yoy: Some(14.0),
            raw_payload: json!({"revision": source_revision}),
            available_at: dt(2026, 3, announcement_day, 8),
            ingested_at: dt(2026, 3, announcement_day, 9),
        }
    }

    fn dividend(source_revision: &str, cash_dividend: f64) -> DividendRecord {
        let available_hour = if source_revision == "v1" { 8 } else { 9 };
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
            available_at: dt(2026, 3, 30, available_hour),
            ingested_at: dt(2026, 3, 30, available_hour),
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
        let repo = CompanyRepository::new(pool);
        let original = dividend("v1", 2.50);
        let revision = dividend("v2", 2.76);

        assert_eq!(
            repo.upsert_dividends(&[original, revision.clone()]).await?,
            2
        );
        assert_eq!(repo.upsert_dividends(&[revision]).await?, 0);

        let page = repo.dividend_history("600519.SH", 100, None).await?;
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].record.source_revision, "v2");
        assert_eq!(page.items[0].record.cash_dividend, Some(2.76));
        assert_eq!(page.items[0].revision_count, 2);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn checkpoint_lifecycle_counts_claims_and_bounds_failures(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = CompanyRepository::new(pool);

        repo.claim_checkpoint(
            "financials",
            "600519.SH",
            Some(date(1998, 1, 1)),
            Some(date(2026, 12, 31)),
        )
        .await?;
        repo.fail_checkpoint("financials", "600519.SH", &"超".repeat(600))
            .await?;

        let failed = repo.checkpoint("financials", "600519.SH").await?.unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.attempts, 1);
        assert_eq!(failed.last_error.unwrap().chars().count(), 500);

        repo.claim_checkpoint(
            "financials",
            "600519.SH",
            Some(date(1998, 1, 1)),
            Some(date(2026, 12, 31)),
        )
        .await?;
        repo.complete_checkpoint("financials", "600519.SH").await?;
        let completed = repo.checkpoint("financials", "600519.SH").await?.unwrap();
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.attempts, 2);
        assert!(completed.last_error.is_none());
        assert!(completed.completed_at.is_some());
        Ok(())
    }
}
