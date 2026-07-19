use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::NaiveDate;
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::{FromRow, PgPool};

use crate::data::company::FinancialFrequency;
use crate::error::{AppError, Result};
use crate::storage::company_repository::{
    CompanyRepository, DividendHistoryCursor, FinancialHistoryCursor,
};

const MAX_PAGE_SIZE: usize = 100;
const CURSOR_HMAC_DOMAIN: &[u8] = b"qbot-dashboard-cursor-v1\0";
type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardCompany {
    pub code: String,
    pub name: String,
    pub industry: Option<String>,
    pub market: Option<String>,
    pub exchange: Option<String>,
    pub list_date: Option<NaiveDate>,
    pub quote: Option<DashboardCompanyQuote>,
    pub valuation: Option<DashboardCompanyValuation>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardCompanyQuote {
    pub trade_date: NaiveDate,
    pub open: Option<Decimal>,
    pub high: Option<Decimal>,
    pub low: Option<Decimal>,
    pub close: Decimal,
    pub volume: Option<i64>,
    pub amount: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardCompanyValuation {
    pub trade_date: NaiveDate,
    pub turnover_rate: Option<Decimal>,
    pub volume_ratio: Option<Decimal>,
    pub pe: Option<Decimal>,
    pub pb: Option<Decimal>,
    pub ps: Option<Decimal>,
    pub total_share: Option<Decimal>,
    pub float_share: Option<Decimal>,
    pub total_market_value: Option<Decimal>,
    pub circulating_market_value: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardFinancialPage {
    pub items: Vec<DashboardFinancialItem>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardFinancialItem {
    pub source: String,
    pub end_date: NaiveDate,
    pub announcement_date: Option<NaiveDate>,
    pub report_type: String,
    pub frequency: FinancialFrequency,
    pub total_revenue: Option<Decimal>,
    pub revenue: Option<Decimal>,
    pub operating_profit: Option<Decimal>,
    pub total_profit: Option<Decimal>,
    pub net_profit_parent: Option<Decimal>,
    pub deducted_net_profit: Option<Decimal>,
    pub basic_eps: Option<Decimal>,
    pub diluted_eps: Option<Decimal>,
    pub roe: Option<Decimal>,
    pub gross_margin: Option<Decimal>,
    pub net_margin: Option<Decimal>,
    pub revenue_yoy: Option<Decimal>,
    pub net_profit_yoy: Option<Decimal>,
    pub revision_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardDividendPage {
    pub items: Vec<DashboardDividendItem>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardDividendItem {
    pub source: String,
    pub announcement_date: Option<NaiveDate>,
    pub record_date: Option<NaiveDate>,
    pub ex_date: Option<NaiveDate>,
    pub pay_date: Option<NaiveDate>,
    pub implementation_status: String,
    pub cash_dividend: Option<Decimal>,
    pub cash_dividend_tax: Option<Decimal>,
    pub stock_ratio: Option<Decimal>,
    pub revision_count: i64,
}

#[derive(Debug, FromRow)]
struct CompanyRow {
    code: String,
    name: String,
    industry: Option<String>,
    market: Option<String>,
    exchange: Option<String>,
    list_date: Option<NaiveDate>,
    quote_trade_date: Option<NaiveDate>,
    quote_open: Option<Decimal>,
    quote_high: Option<Decimal>,
    quote_low: Option<Decimal>,
    quote_close: Option<Decimal>,
    quote_volume: Option<i64>,
    quote_amount: Option<Decimal>,
    valuation_trade_date: Option<NaiveDate>,
    turnover_rate: Option<Decimal>,
    volume_ratio: Option<Decimal>,
    pe: Option<Decimal>,
    pb: Option<Decimal>,
    ps: Option<Decimal>,
    total_share: Option<Decimal>,
    float_share: Option<Decimal>,
    total_mv: Option<Decimal>,
    circ_mv: Option<Decimal>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum CursorPayload {
    Financials {
        version: u8,
        code: String,
        frequency: FinancialFrequency,
        end_date: NaiveDate,
        report_type: String,
    },
    Dividends {
        version: u8,
        code: String,
        dividend_date: NaiveDate,
        source: String,
        action_key: String,
    },
}

#[derive(Clone)]
pub struct DashboardCompanyService {
    pool: PgPool,
    repository: CompanyRepository,
    cursor_secret: String,
}

impl DashboardCompanyService {
    pub fn new(pool: PgPool, cursor_secret: impl Into<String>) -> Self {
        Self::from_parts(pool, cursor_secret)
    }

    fn from_parts(pool: PgPool, cursor_secret: impl Into<String>) -> Self {
        Self {
            repository: CompanyRepository::new(pool.clone()),
            pool,
            cursor_secret: cursor_secret.into(),
        }
    }

    pub async fn company(&self, raw_code: &str) -> Result<DashboardCompany> {
        let code = canonical_code(raw_code)?;
        let today = crate::market_time::beijing_today();
        let row = sqlx::query_as::<_, CompanyRow>(
            r#"SELECT si.code,
                      sm.name,
                      si.industry,
                      COALESCE(sm.market, si.market) AS market,
                      sm.exchange,
                      sm.list_date,
                      quote.trade_date AS quote_trade_date,
                      quote.open AS quote_open,
                      quote.high AS quote_high,
                      quote.low AS quote_low,
                      quote.close AS quote_close,
                      quote.volume AS quote_volume,
                      quote.amount AS quote_amount,
                      valuation.trade_date AS valuation_trade_date,
                      valuation.turnover_rate,
                      valuation.volume_ratio,
                      valuation.pe,
                      valuation.pb,
                      valuation.ps,
                      valuation.total_share,
                      valuation.float_share,
                      valuation.total_mv,
                      valuation.circ_mv
               FROM stock_info si
               JOIN LATERAL (
                   SELECT name, market, exchange, list_status, list_date, delist_date
                   FROM security_master_versions
                   WHERE code = si.code AND available_at <= NOW()
                   ORDER BY available_at DESC, ingested_at DESC, source DESC
                   LIMIT 1
               ) sm ON sm.list_status = 'L'
                   AND sm.list_date IS NOT NULL
                   AND sm.list_date <= $2
                   AND (sm.delist_date IS NULL OR sm.delist_date > $2)
               LEFT JOIN LATERAL (
                   SELECT trade_date, open, high, low, close, volume, amount
                   FROM stock_daily_bar_versions
                   WHERE code = si.code AND available_at <= NOW()
                   ORDER BY trade_date DESC, available_at DESC, ingested_at DESC, source DESC
                   LIMIT 1
               ) quote ON TRUE
               LEFT JOIN LATERAL (
                   SELECT trade_date, turnover_rate, volume_ratio, pe, pb, ps,
                          total_share, float_share, total_mv, circ_mv
                   FROM stock_daily_basic_versions
                   WHERE code = si.code AND available_at <= NOW()
                   ORDER BY trade_date DESC, available_at DESC, ingested_at DESC, source DESC
                   LIMIT 1
               ) valuation ON TRUE
               WHERE si.code = $1"#,
        )
        .bind(code)
        .bind(today)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| stock_not_found(raw_code))?;

        Ok(company_view(row))
    }

    pub async fn financials(
        &self,
        raw_code: &str,
        frequency: FinancialFrequency,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<DashboardFinancialPage> {
        validate_page_size(limit)?;
        let code = self.ensure_current_code(raw_code).await?;
        let cursor = cursor
            .map(|value| self.decode_financial_cursor(value, code, frequency))
            .transpose()?;
        let page = self
            .repository
            .financial_history(code, frequency, limit, cursor)
            .await?;
        let next_cursor = page
            .next_cursor
            .map(|value| self.encode_financial_cursor(code, frequency, value))
            .transpose()?;
        let items = page
            .items
            .into_iter()
            .map(|item| {
                let report = item.report;
                DashboardFinancialItem {
                    source: report.source,
                    end_date: report.end_date,
                    announcement_date: report.announcement_date,
                    report_type: report.report_type,
                    frequency: report.frequency,
                    total_revenue: report.total_revenue,
                    revenue: report.revenue,
                    operating_profit: report.operating_profit,
                    total_profit: report.total_profit,
                    net_profit_parent: report.net_profit_parent,
                    deducted_net_profit: report.deducted_net_profit,
                    basic_eps: report.basic_eps,
                    diluted_eps: report.diluted_eps,
                    roe: report.roe,
                    gross_margin: report.gross_margin,
                    net_margin: report.net_margin,
                    revenue_yoy: report.revenue_yoy,
                    net_profit_yoy: report.net_profit_yoy,
                    revision_count: item.revision_count,
                }
            })
            .collect();
        Ok(DashboardFinancialPage { items, next_cursor })
    }

    pub async fn dividends(
        &self,
        raw_code: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<DashboardDividendPage> {
        validate_page_size(limit)?;
        let code = self.ensure_current_code(raw_code).await?;
        let cursor = cursor
            .map(|value| self.decode_dividend_cursor(value, code))
            .transpose()?;
        let page = self
            .repository
            .dividend_history(code, limit, cursor)
            .await?;
        let next_cursor = page
            .next_cursor
            .map(|value| self.encode_dividend_cursor(code, value))
            .transpose()?;
        let items = page
            .items
            .into_iter()
            .map(|item| {
                let record = item.record;
                DashboardDividendItem {
                    source: record.source,
                    announcement_date: record.announcement_date,
                    record_date: record.record_date,
                    ex_date: record.ex_date,
                    pay_date: record.pay_date,
                    implementation_status: record.implementation_status,
                    cash_dividend: record.cash_dividend,
                    cash_dividend_tax: record.cash_dividend_tax,
                    stock_ratio: record.stock_ratio,
                    revision_count: item.revision_count,
                }
            })
            .collect();
        Ok(DashboardDividendPage { items, next_cursor })
    }

    async fn ensure_current_code<'a>(&self, raw_code: &'a str) -> Result<&'a str> {
        let code = canonical_code(raw_code)?;
        let today = crate::market_time::beijing_today();
        let current: bool = sqlx::query_scalar(
            r#"SELECT EXISTS (
                   SELECT 1
                   FROM stock_info si
                   JOIN LATERAL (
                       SELECT list_status, list_date, delist_date
                       FROM security_master_versions
                       WHERE code = si.code AND available_at <= NOW()
                       ORDER BY available_at DESC, ingested_at DESC, source DESC
                       LIMIT 1
                   ) sm ON TRUE
                   WHERE si.code = $1
                     AND sm.list_status = 'L'
                     AND sm.list_date IS NOT NULL
                     AND sm.list_date <= $2
                     AND (sm.delist_date IS NULL OR sm.delist_date > $2)
               )"#,
        )
        .bind(code)
        .bind(today)
        .fetch_one(&self.pool)
        .await?;
        if current {
            Ok(code)
        } else {
            Err(stock_not_found(raw_code))
        }
    }

    fn encode_financial_cursor(
        &self,
        code: &str,
        frequency: FinancialFrequency,
        cursor: FinancialHistoryCursor,
    ) -> Result<String> {
        self.encode_cursor(&CursorPayload::Financials {
            version: 1,
            code: code.to_string(),
            frequency,
            end_date: cursor.end_date,
            report_type: cursor.report_type,
        })
    }

    fn decode_financial_cursor(
        &self,
        token: &str,
        expected_code: &str,
        expected_frequency: FinancialFrequency,
    ) -> Result<FinancialHistoryCursor> {
        match self.decode_cursor(token)? {
            CursorPayload::Financials {
                version: 1,
                code,
                frequency,
                end_date,
                report_type,
            } if code == expected_code
                && frequency == expected_frequency
                && !report_type.is_empty() =>
            {
                Ok(FinancialHistoryCursor {
                    end_date,
                    report_type,
                })
            }
            _ => Err(invalid_cursor()),
        }
    }

    fn encode_dividend_cursor(&self, code: &str, cursor: DividendHistoryCursor) -> Result<String> {
        self.encode_cursor(&CursorPayload::Dividends {
            version: 1,
            code: code.to_string(),
            dividend_date: cursor.dividend_date,
            source: cursor.source,
            action_key: cursor.action_key,
        })
    }

    fn decode_dividend_cursor(
        &self,
        token: &str,
        expected_code: &str,
    ) -> Result<DividendHistoryCursor> {
        match self.decode_cursor(token)? {
            CursorPayload::Dividends {
                version: 1,
                code,
                dividend_date,
                source,
                action_key,
            } if code == expected_code && !source.is_empty() && !action_key.is_empty() => {
                Ok(DividendHistoryCursor {
                    dividend_date,
                    source,
                    action_key,
                })
            }
            _ => Err(invalid_cursor()),
        }
    }

    fn encode_cursor(&self, payload: &CursorPayload) -> Result<String> {
        let body = serde_json::to_vec(payload)
            .map_err(|_| AppError::Internal("failed to encode dashboard cursor".to_string()))?;
        let body = URL_SAFE_NO_PAD.encode(body);
        let mut mac = HmacSha256::new_from_slice(self.cursor_secret.as_bytes())
            .map_err(|_| AppError::Internal("invalid dashboard cursor secret".to_string()))?;
        mac.update(CURSOR_HMAC_DOMAIN);
        mac.update(body.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("{body}.{signature}"))
    }

    fn decode_cursor(&self, token: &str) -> Result<CursorPayload> {
        let (body, signature) = token.split_once('.').ok_or_else(invalid_cursor)?;
        if body.is_empty() || signature.is_empty() || signature.contains('.') {
            return Err(invalid_cursor());
        }
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| invalid_cursor())?;
        let mut mac = HmacSha256::new_from_slice(self.cursor_secret.as_bytes())
            .map_err(|_| AppError::Internal("invalid dashboard cursor secret".to_string()))?;
        mac.update(CURSOR_HMAC_DOMAIN);
        mac.update(body.as_bytes());
        mac.verify_slice(&signature).map_err(|_| invalid_cursor())?;
        let payload = URL_SAFE_NO_PAD.decode(body).map_err(|_| invalid_cursor())?;
        serde_json::from_slice(&payload).map_err(|_| invalid_cursor())
    }
}

fn company_view(row: CompanyRow) -> DashboardCompany {
    let quote = row
        .quote_trade_date
        .zip(row.quote_close)
        .map(|(trade_date, close)| DashboardCompanyQuote {
            trade_date,
            open: row.quote_open,
            high: row.quote_high,
            low: row.quote_low,
            close,
            volume: row.quote_volume,
            amount: row.quote_amount,
        });
    let valuation = row
        .valuation_trade_date
        .map(|trade_date| DashboardCompanyValuation {
            trade_date,
            turnover_rate: row.turnover_rate,
            volume_ratio: row.volume_ratio,
            pe: row.pe,
            pb: row.pb,
            ps: row.ps,
            total_share: row.total_share,
            float_share: row.float_share,
            total_market_value: row.total_mv,
            circulating_market_value: row.circ_mv,
        });
    DashboardCompany {
        code: row.code,
        name: row.name,
        industry: row.industry,
        market: row.market,
        exchange: row.exchange,
        list_date: row.list_date,
        quote,
        valuation,
    }
}

fn canonical_code(raw_code: &str) -> Result<&str> {
    let bytes = raw_code.as_bytes();
    let valid = bytes.len() == 9
        && bytes[..6].iter().all(u8::is_ascii_digit)
        && bytes[6] == b'.'
        && matches!(&bytes[7..], b"SH" | b"SZ" | b"BJ");
    if valid {
        Ok(raw_code)
    } else {
        Err(stock_not_found(raw_code))
    }
}

fn validate_page_size(limit: usize) -> Result<()> {
    if (1..=MAX_PAGE_SIZE).contains(&limit) {
        Ok(())
    } else {
        Err(AppError::BadRequest(
            "limit must be between 1 and 100".to_string(),
        ))
    }
}

fn stock_not_found(raw_code: &str) -> AppError {
    AppError::NotFound(format!("stock {raw_code}"))
}

fn invalid_cursor() -> AppError {
    AppError::BadRequest("invalid pagination cursor".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Datelike, NaiveDate, Utc};
    use rust_decimal::Decimal;
    use serde_json::{json, to_value};
    use sqlx::PgPool;

    use crate::data::company::{DividendRecord, FinancialFrequency, FinancialReport};
    use crate::error::AppError;
    use crate::storage::company_repository::CompanyRepository;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        DateTime::from_naive_utc_and_offset(
            date(year, month, day).and_hms_opt(hour, 0, 0).unwrap(),
            Utc,
        )
    }

    async fn seed_current_stock(pool: &PgPool) -> crate::error::Result<()> {
        sqlx::query(
            r#"INSERT INTO stock_info (code, name, market, industry)
               VALUES ('600519.SH', '贵州茅台', 'SH', '白酒')"#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO security_master_versions
               (code, name, market, exchange, list_status, list_date, delist_date,
                available_at, availability_quality, source)
               VALUES
               ('600519.SH', '老名称', '主板', 'SSE', 'L', '2001-08-27', NULL,
                '2025-01-01T00:00:00Z', 'observed', 'tushare'),
               ('600519.SH', '贵州茅台', '主板', 'SSE', 'L', '2001-08-27', NULL,
                '2026-01-01T00:00:00Z', 'observed', 'tushare')"#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO stock_daily_bar_versions
               (code, trade_date, open, high, low, close, volume, amount, turnover, pe, pb,
                available_at, availability_quality, source)
               VALUES
               ('600519.SH', '2026-07-16', 1400, 1420, 1390, 1400, 10, 14000, 1, 20, 8,
                '2026-07-16T10:00:00Z', 'observed', 'tushare'),
               ('600519.SH', '2026-07-17', 1400, 1450, 1395, 1435, 20, 28700, 1.2, 21, 8.2,
                '2026-07-17T10:00:00Z', 'observed', 'tushare')"#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO stock_daily_basic_versions
               (code, trade_date, turnover_rate, volume_ratio, pe, pb, ps, total_share,
                float_share, total_mv, circ_mv, available_at, availability_quality, source)
               VALUES
               ('600519.SH', '2026-07-17', 1.2, 0.9, 21, 8.2, 9.1, 125600, 125600,
                180236000, 180236000, '2026-07-17T10:00:00Z', 'observed', 'tushare'),
               ('600519.SH', '2026-07-17', 1.3, 1.1, 22, 8.3, 9.2, 125600, 125600,
                180236000, 180236000, '2026-07-17T11:00:00Z', 'observed', 'tushare')"#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    fn financial(
        end_date: NaiveDate,
        report_type: &str,
        revision: &str,
        available_at: DateTime<Utc>,
    ) -> FinancialReport {
        FinancialReport {
            source: "tushare".to_string(),
            code: "600519.SH".to_string(),
            end_date,
            announcement_date: Some(date(end_date.year() + 1, 3, 30)),
            report_type: report_type.to_string(),
            frequency: FinancialFrequency::Annual,
            source_revision: revision.to_string(),
            total_revenue: Some(Decimal::new(100_000, 0)),
            revenue: Some(Decimal::new(99_000, 0)),
            operating_profit: Some(Decimal::new(50_000, 0)),
            total_profit: Some(Decimal::new(49_000, 0)),
            net_profit_parent: Some(Decimal::new(40_000, 0)),
            deducted_net_profit: Some(Decimal::new(39_000, 0)),
            basic_eps: Some(Decimal::new(3200, 2)),
            diluted_eps: Some(Decimal::new(3200, 2)),
            roe: Some(Decimal::new(312, 1)),
            gross_margin: Some(Decimal::new(920, 1)),
            net_margin: Some(Decimal::new(400, 1)),
            revenue_yoy: Some(Decimal::new(150, 1)),
            net_profit_yoy: Some(Decimal::new(180, 1)),
            raw_payload: json!({"mustNotLeak": true}),
            available_at,
            ingested_at: available_at,
        }
    }

    fn dividend(
        action_key: &str,
        ex_date: NaiveDate,
        revision: &str,
        available_at: DateTime<Utc>,
    ) -> DividendRecord {
        DividendRecord {
            source: "tushare".to_string(),
            action_key: action_key.to_string(),
            code: "600519.SH".to_string(),
            announcement_date: Some(ex_date - chrono::Days::new(30)),
            record_date: Some(ex_date - chrono::Days::new(1)),
            ex_date: Some(ex_date),
            pay_date: Some(ex_date),
            implementation_status: "implemented".to_string(),
            cash_dividend: Some(Decimal::new(276, 2)),
            cash_dividend_tax: Some(Decimal::new(250, 2)),
            stock_ratio: None,
            source_revision: revision.to_string(),
            raw_payload: json!({"mustNotLeak": true}),
            available_at,
            ingested_at: available_at,
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dashboard_company_joins_latest_current_master_quote_and_valuation(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        seed_current_stock(&pool).await?;
        let service = DashboardCompanyService::from_parts(pool, "cursor-test-secret");

        let payload = service.company("600519.SH").await?;

        assert_eq!(payload.code, "600519.SH");
        assert_eq!(payload.name, "贵州茅台");
        assert_eq!(payload.industry.as_deref(), Some("白酒"));
        assert_eq!(payload.exchange.as_deref(), Some("SSE"));
        assert_eq!(payload.list_date, Some(date(2001, 8, 27)));
        assert_eq!(
            payload.quote.as_ref().unwrap().trade_date,
            date(2026, 7, 17)
        );
        assert_eq!(payload.quote.as_ref().unwrap().close, Decimal::new(1435, 0));
        assert_eq!(
            payload.valuation.as_ref().unwrap().pe,
            Some(Decimal::new(22, 0))
        );
        assert_eq!(
            payload.valuation.as_ref().unwrap().turnover_rate,
            Some(Decimal::new(13, 1))
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dashboard_financials_serialize_latest_revisions_and_page_without_leaking_raw_data(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        seed_current_stock(&pool).await?;
        let repo = CompanyRepository::new(pool.clone());
        repo.upsert_financial_reports(&[
            financial(date(2025, 12, 31), "1", "v1", dt(2026, 3, 30, 8)),
            financial(date(2025, 12, 31), "1", "v2", dt(2026, 4, 1, 8)),
            financial(date(2024, 12, 31), "1", "v1", dt(2025, 3, 30, 8)),
        ])
        .await?;
        let service = DashboardCompanyService::from_parts(pool, "cursor-test-secret");

        let first = service
            .financials("600519.SH", FinancialFrequency::Annual, 1, None)
            .await?;
        assert_eq!(first.items[0].end_date, date(2025, 12, 31));
        assert_eq!(first.items[0].revision_count, 2);
        let serialized = to_value(&first)?;
        assert!(serialized.get("items").is_some());
        let text = serialized.to_string();
        assert!(!text.contains("rawPayload"));
        assert!(!text.contains("sourceRevision"));
        assert!(!text.contains("ingestedAt"));
        assert!(!text.contains("availableAt"));

        let cursor = first.next_cursor.clone().expect("second page cursor");
        let second = service
            .financials("600519.SH", FinancialFrequency::Annual, 1, Some(&cursor))
            .await?;
        assert_eq!(second.items[0].end_date, date(2024, 12, 31));
        assert!(second.next_cursor.is_none());

        let mismatch = service
            .financials("600519.SH", FinancialFrequency::Quarterly, 1, Some(&cursor))
            .await;
        assert!(matches!(mismatch, Err(AppError::BadRequest(_))));

        let (body, _) = cursor.split_once('.').expect("signed cursor");
        let mut legacy_mac = HmacSha256::new_from_slice(b"cursor-test-secret").unwrap();
        legacy_mac.update(body.as_bytes());
        let legacy_signature = URL_SAFE_NO_PAD.encode(legacy_mac.finalize().into_bytes());
        let legacy_cursor = format!("{body}.{legacy_signature}");
        assert!(matches!(
            service
                .financials(
                    "600519.SH",
                    FinancialFrequency::Annual,
                    1,
                    Some(&legacy_cursor)
                )
                .await,
            Err(AppError::BadRequest(_))
        ));

        let mut tampered = cursor;
        tampered.replace_range(0..1, if tampered.starts_with('a') { "b" } else { "a" });
        assert!(matches!(
            service
                .financials("600519.SH", FinancialFrequency::Annual, 1, Some(&tampered))
                .await,
            Err(AppError::BadRequest(_))
        ));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dashboard_dividends_are_latest_first_revision_aware_and_cursor_bounded(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        seed_current_stock(&pool).await?;
        let repo = CompanyRepository::new(pool.clone());
        repo.upsert_dividends(&[
            dividend("newer", date(2026, 6, 26), "v1", dt(2026, 3, 30, 8)),
            dividend("newer", date(2026, 6, 26), "v2", dt(2026, 4, 1, 8)),
            dividend("older", date(2025, 6, 26), "v1", dt(2025, 3, 30, 8)),
        ])
        .await?;
        let service = DashboardCompanyService::from_parts(pool, "cursor-test-secret");

        let first = service.dividends("600519.SH", 1, None).await?;
        assert_eq!(first.items[0].ex_date, Some(date(2026, 6, 26)));
        assert_eq!(first.items[0].revision_count, 2);
        let cursor = first.next_cursor.clone().expect("second page cursor");
        let second = service.dividends("600519.SH", 1, Some(&cursor)).await?;
        assert_eq!(second.items[0].ex_date, Some(date(2025, 6, 26)));
        assert!(second.next_cursor.is_none());
        let text = to_value(&first)?.to_string();
        assert!(!text.contains("actionKey"));
        assert!(!text.contains("rawPayload"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dashboard_company_rejects_noncanonical_unknown_and_noncurrent_codes(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        seed_current_stock(&pool).await?;
        sqlx::query("INSERT INTO stock_info (code, name) VALUES ('000001.SZ', '平安银行')")
            .execute(&pool)
            .await?;
        sqlx::query(
            r#"INSERT INTO security_master_versions
               (code, name, list_status, list_date, delist_date, available_at,
                availability_quality, source)
               VALUES ('000001.SZ', '平安银行', 'D', '1991-04-03', '2026-01-01',
                       '2026-01-02T00:00:00Z', 'observed', 'tushare')"#,
        )
        .execute(&pool)
        .await?;
        let service = DashboardCompanyService::from_parts(pool, "cursor-test-secret");

        for code in ["600519", "600519.sh", "999999.SH", "000001.SZ"] {
            assert!(matches!(
                service.company(code).await,
                Err(AppError::NotFound(_))
            ));
        }
        assert!(matches!(
            service
                .financials("600519.SH", FinancialFrequency::Annual, 0, None)
                .await,
            Err(AppError::BadRequest(_))
        ));
        assert!(matches!(
            service.dividends("600519.SH", 101, None).await,
            Err(AppError::BadRequest(_))
        ));
        Ok(())
    }
}
