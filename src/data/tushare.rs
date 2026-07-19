use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use tokio::sync::RwLock;
use tracing::warn;

use crate::analysis::market_snapshot::{
    AdjustmentFactor, AvailabilityQuality, CorporateAction, DailyBasicSnapshot, IndexDailyBar,
    SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
};
use crate::data::company::{
    CompanyDataProvider, DividendRecord, FinancialFrequency, FinancialReport,
};
use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
use crate::data::provider::DataProvider;
use crate::data::types::*;
use crate::error::{AppError, Result};

const TUSHARE_URL: &str = "https://api.tushare.pro";
const INCOME_FIELDS: &str = "ts_code,ann_date,f_ann_date,end_date,report_type,basic_eps,diluted_eps,total_revenue,revenue,operate_profit,total_profit,n_income_attr_p,update_flag";
const INDICATOR_FIELDS: &str = "ts_code,ann_date,end_date,eps,dt_eps,profit_dedt,roe,grossprofit_margin,netprofit_margin,tr_yoy,netprofit_yoy,update_flag";
const DIVIDEND_FIELDS: &str = "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share";

pub struct TushareClient {
    token: String,
    client: Client,
    point_in_time_capabilities: RwLock<Option<PointInTimeCapabilities>>,
}

impl TushareClient {
    pub fn new(token: String, proxy: Option<&str>) -> Self {
        let mut builder = Client::builder().timeout(std::time::Duration::from_secs(30));

        if let Some(proxy_url) = proxy {
            if let Ok(proxy) = reqwest::Proxy::all(proxy_url) {
                builder = builder.proxy(proxy);
            }
        }

        TushareClient {
            token,
            client: builder.build().unwrap_or_default(),
            point_in_time_capabilities: RwLock::new(None),
        }
    }

    async fn call(&self, api_name: &str, params: Value, fields: &str) -> Result<Value> {
        let body = json!({
            "api_name": api_name,
            "token": self.token,
            "params": params,
            "fields": fields
        });

        let resp = self
            .client
            .post(TUSHARE_URL)
            .json(&body)
            .send()
            .await
            .context("Tushare HTTP request failed")
            .map_err(|e| AppError::Internal(e.to_string()))?;

        let json: Value = resp.json().await.map_err(AppError::Http)?;
        Self::response_data(api_name, &json)
    }

    fn response_data(api_name: &str, response: &Value) -> Result<Value> {
        let code = response
            .get("code")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                AppError::DataProvider(format!(
                    "Tushare {} returned a malformed response: missing integer code",
                    api_name
                ))
            })?;

        if code != 0 {
            let message = response
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            return Err(AppError::DataProvider(format!(
                "Tushare {} [{}]: {}",
                api_name, code, message
            )));
        }

        response.get("data").cloned().ok_or_else(|| {
            AppError::DataProvider(format!(
                "Tushare {} returned a malformed response: missing data",
                api_name
            ))
        })
    }

    fn income_fields() -> &'static str {
        INCOME_FIELDS
    }

    fn indicator_fields() -> &'static str {
        INDICATOR_FIELDS
    }

    fn dividend_fields() -> &'static str {
        DIVIDEND_FIELDS
    }

    fn company_window_params(code: &str, start: NaiveDate, end: NaiveDate) -> Value {
        json!({
            "ts_code": code,
            "start_date": start.format("%Y%m%d").to_string(),
            "end_date": end.format("%Y%m%d").to_string(),
        })
    }

    fn company_rows(data: &Value, endpoint: &str) -> Result<Vec<serde_json::Map<String, Value>>> {
        let fields = data
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| Self::malformed_company_data(endpoint, "missing fields array"))?;
        let items = data
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| Self::malformed_company_data(endpoint, "missing items array"))?;

        let mut names = Vec::with_capacity(fields.len());
        let mut seen = HashSet::with_capacity(fields.len());
        for field in fields {
            let name = field
                .as_str()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    Self::malformed_company_data(endpoint, "field names must be non-empty strings")
                })?;
            if !seen.insert(name) {
                return Err(Self::malformed_company_data(
                    endpoint,
                    &format!("duplicate field {name}"),
                ));
            }
            names.push(name);
        }

        items
            .iter()
            .enumerate()
            .map(|(row_index, item)| {
                let values = item.as_array().ok_or_else(|| {
                    Self::malformed_company_data(
                        endpoint,
                        &format!("row {row_index} is not an array"),
                    )
                })?;
                if values.len() != names.len() {
                    return Err(Self::malformed_company_data(
                        endpoint,
                        &format!(
                            "row {row_index} has {} values for {} fields",
                            values.len(),
                            names.len()
                        ),
                    ));
                }

                Ok(names
                    .iter()
                    .zip(values)
                    .map(|(name, value)| ((*name).to_string(), value.clone()))
                    .collect())
            })
            .collect()
    }

    fn malformed_company_data(endpoint: &str, detail: &str) -> AppError {
        AppError::DataProvider(format!("Tushare {endpoint} data is malformed: {detail}"))
    }

    fn optional_company_text(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
        field: &str,
    ) -> Result<Option<String>> {
        match row.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) if value.trim().is_empty() => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.trim().to_string())),
            Some(Value::Number(value)) => Ok(Some(value.to_string())),
            Some(value) => Err(Self::malformed_company_data(
                endpoint,
                &format!("{endpoint}.{field} is not text: {value}"),
            )),
        }
    }

    fn required_company_text(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
        field: &str,
    ) -> Result<String> {
        Self::optional_company_text(row, endpoint, field)?.ok_or_else(|| {
            Self::malformed_company_data(endpoint, &format!("missing {endpoint}.{field}"))
        })
    }

    fn optional_company_decimal(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
        field: &str,
    ) -> Result<Option<Decimal>> {
        let Some(value) = row.get(field) else {
            return Ok(None);
        };
        let text = match value {
            Value::Null => return Ok(None),
            Value::String(value) if value.trim().is_empty() => return Ok(None),
            Value::String(value) => value.trim().to_string(),
            Value::Number(value) => value.to_string(),
            _ => {
                return Err(Self::malformed_company_data(
                    endpoint,
                    &format!("{endpoint}.{field} is not a Decimal: {value}"),
                ))
            }
        };

        Self::parse_exact_decimal(&text).map(Some).ok_or_else(|| {
            Self::malformed_company_data(
                endpoint,
                &format!("{endpoint}.{field} is not a Decimal: {text}"),
            )
        })
    }

    fn parse_exact_decimal(text: &str) -> Option<Decimal> {
        let Some(exponent_index) = text.find(['e', 'E']) else {
            return Decimal::from_str_exact(text).ok();
        };
        if text[exponent_index + 1..].contains(['e', 'E']) {
            return None;
        }

        let (mantissa, exponent) = text.split_at(exponent_index);
        let exponent: i32 = exponent[1..].parse().ok()?;
        if exponent.unsigned_abs() > 128 {
            return None;
        }
        let (sign, mantissa) = match mantissa.as_bytes().first() {
            Some(b'-') => ("-", &mantissa[1..]),
            Some(b'+') => ("", &mantissa[1..]),
            _ => ("", mantissa),
        };
        let mut parts = mantissa.split('.');
        let integer = parts.next()?;
        let fraction = parts.next().unwrap_or_default();
        if parts.next().is_some()
            || (integer.is_empty() && fraction.is_empty())
            || !integer
                .bytes()
                .chain(fraction.bytes())
                .all(|byte| byte.is_ascii_digit())
        {
            return None;
        }

        let digits = format!("{integer}{fraction}");
        let decimal_index = i32::try_from(integer.len()).ok()?.checked_add(exponent)?;
        let expanded = if decimal_index <= 0 {
            format!(
                "{sign}0.{}{}",
                "0".repeat(decimal_index.unsigned_abs() as usize),
                digits
            )
        } else {
            let decimal_index = usize::try_from(decimal_index).ok()?;
            if decimal_index >= digits.len() {
                format!("{sign}{digits}{}", "0".repeat(decimal_index - digits.len()))
            } else {
                format!(
                    "{sign}{}.{}",
                    &digits[..decimal_index],
                    &digits[decimal_index..]
                )
            }
        };
        if expanded.len() > 130 {
            return None;
        }
        Decimal::from_str_exact(&expanded).ok()
    }

    fn optional_company_date(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
        field: &str,
    ) -> Result<Option<NaiveDate>> {
        let Some(value) = Self::optional_company_text(row, endpoint, field)? else {
            return Ok(None);
        };
        NaiveDate::parse_from_str(&value, "%Y%m%d")
            .map(Some)
            .map_err(|_| {
                Self::malformed_company_data(
                    endpoint,
                    &format!("{endpoint}.{field} is not a YYYYMMDD date: {value}"),
                )
            })
    }

    fn required_company_date(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
        field: &str,
    ) -> Result<NaiveDate> {
        Self::optional_company_date(row, endpoint, field)?.ok_or_else(|| {
            Self::malformed_company_data(endpoint, &format!("missing {endpoint}.{field}"))
        })
    }

    fn financial_frequency(end_date: NaiveDate) -> Result<FinancialFrequency> {
        match end_date.format("%m%d").to_string().as_str() {
            "1231" => Ok(FinancialFrequency::Annual),
            "0331" | "0630" | "0930" => Ok(FinancialFrequency::Quarterly),
            suffix => Err(Self::malformed_company_data(
                "financial",
                &format!("unsupported report end-date suffix {suffix}"),
            )),
        }
    }

    fn deterministic_available_at(
        announcement_date: Option<NaiveDate>,
        fetched_at: DateTime<Utc>,
    ) -> DateTime<Utc> {
        announcement_date
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
            .unwrap_or(fetched_at)
    }

    fn content_revision(payload: &Value) -> String {
        let bytes = serde_json::to_vec(payload).expect("JSON values always serialize");
        format!("{:x}", Sha256::digest(bytes))
    }

    fn financial_join_identity(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
    ) -> Result<(String, NaiveDate, Option<String>, Option<String>)> {
        Ok((
            Self::required_company_text(row, endpoint, "ts_code")?,
            Self::required_company_date(row, endpoint, "end_date")?,
            Self::optional_company_text(row, endpoint, "report_type")?,
            Self::optional_company_text(row, endpoint, "update_flag")?,
        ))
    }

    fn parse_financial_report_row(
        income: Option<&serde_json::Map<String, Value>>,
        indicator: Option<&serde_json::Map<String, Value>>,
        fetched_at: DateTime<Utc>,
    ) -> Result<FinancialReport> {
        let (identity_row, identity_endpoint) = match (income, indicator) {
            (Some(row), _) => (row, "income"),
            (None, Some(row)) => (row, "fina_indicator"),
            (None, None) => unreachable!("a report needs at least one source row"),
        };
        let code = Self::required_company_text(identity_row, identity_endpoint, "ts_code")?;
        let end_date = Self::required_company_date(identity_row, identity_endpoint, "end_date")?;
        let report_type =
            Self::required_company_text(identity_row, identity_endpoint, "report_type")?;
        let frequency = Self::financial_frequency(end_date)?;

        let income_announcement = match income {
            Some(row) => Self::optional_company_date(row, "income", "f_ann_date")?
                .or(Self::optional_company_date(row, "income", "ann_date")?),
            None => None,
        };
        let indicator_announcement = match indicator {
            Some(row) => Self::optional_company_date(row, "fina_indicator", "ann_date")?,
            None => None,
        };
        let announcement_date = income_announcement.or(indicator_announcement);
        let available_date = match (income_announcement, indicator_announcement) {
            (Some(income_date), Some(indicator_date)) => Some(income_date.max(indicator_date)),
            (income_date, indicator_date) => income_date.or(indicator_date),
        };

        let raw_payload = json!({
            "income": income.cloned().map(Value::Object).unwrap_or(Value::Null),
            "indicator": indicator.cloned().map(Value::Object).unwrap_or(Value::Null),
        });
        let source_revision = Self::content_revision(&raw_payload);

        let income_decimal = |field| match income {
            Some(row) => Self::optional_company_decimal(row, "income", field),
            None => Ok(None),
        };
        let indicator_decimal = |field| match indicator {
            Some(row) => Self::optional_company_decimal(row, "fina_indicator", field),
            None => Ok(None),
        };

        Ok(FinancialReport {
            source: Self::source(),
            code,
            end_date,
            announcement_date,
            report_type,
            frequency,
            source_revision,
            total_revenue: income_decimal("total_revenue")?,
            revenue: income_decimal("revenue")?,
            operating_profit: income_decimal("operate_profit")?,
            total_profit: income_decimal("total_profit")?,
            net_profit_parent: income_decimal("n_income_attr_p")?,
            deducted_net_profit: indicator_decimal("profit_dedt")?,
            basic_eps: income_decimal("basic_eps")?.or(indicator_decimal("eps")?),
            diluted_eps: income_decimal("diluted_eps")?.or(indicator_decimal("dt_eps")?),
            roe: indicator_decimal("roe")?,
            gross_margin: indicator_decimal("grossprofit_margin")?,
            net_margin: indicator_decimal("netprofit_margin")?,
            revenue_yoy: indicator_decimal("tr_yoy")?,
            net_profit_yoy: indicator_decimal("netprofit_yoy")?,
            raw_payload,
            available_at: Self::deterministic_available_at(available_date, fetched_at),
            ingested_at: fetched_at,
        })
    }

    fn parse_financial_reports(
        income_data: &Value,
        indicator_data: &Value,
        fetched_at: DateTime<Utc>,
    ) -> Result<Vec<FinancialReport>> {
        let income_rows = Self::company_rows(income_data, "income")?;
        let indicator_rows = Self::company_rows(indicator_data, "fina_indicator")?;
        let income_identities = income_rows
            .iter()
            .map(|row| Self::financial_join_identity(row, "income"))
            .collect::<Result<Vec<_>>>()?;
        let indicator_identities = indicator_rows
            .iter()
            .map(|row| Self::financial_join_identity(row, "fina_indicator"))
            .collect::<Result<Vec<_>>>()?;
        let mut used_indicators = vec![false; indicator_rows.len()];
        let mut reports = Vec::with_capacity(income_rows.len() + indicator_rows.len());

        for (income_index, income) in income_rows.iter().enumerate() {
            let (code, end_date, report_type, update_flag) = &income_identities[income_index];
            let mut candidates: Vec<usize> = indicator_identities
                .iter()
                .enumerate()
                .filter(
                    |(index, (indicator_code, indicator_end, indicator_type, _))| {
                        !used_indicators[*index]
                            && indicator_code == code
                            && indicator_end == end_date
                            && indicator_type == report_type
                    },
                )
                .map(|(index, _)| index)
                .collect();

            if candidates.is_empty() {
                candidates = indicator_identities
                    .iter()
                    .enumerate()
                    .filter(
                        |(index, (indicator_code, indicator_end, indicator_type, _))| {
                            !used_indicators[*index]
                                && indicator_code == code
                                && indicator_end == end_date
                                && indicator_type.is_none()
                        },
                    )
                    .map(|(index, _)| index)
                    .collect();
            }

            if candidates.len() > 1 {
                if let Some(update_flag) = update_flag {
                    let matching: Vec<usize> = candidates
                        .iter()
                        .copied()
                        .filter(|index| {
                            indicator_identities[*index].3.as_ref() == Some(update_flag)
                        })
                        .collect();
                    if !matching.is_empty() {
                        candidates = matching;
                    }
                }
            }
            if candidates.len() > 1 {
                let income_announcement =
                    Self::optional_company_date(income, "income", "f_ann_date")?
                        .or(Self::optional_company_date(income, "income", "ann_date")?);
                if let Some(income_announcement) = income_announcement {
                    let mut matching = Vec::new();
                    for index in candidates.iter().copied() {
                        if Self::optional_company_date(
                            &indicator_rows[index],
                            "fina_indicator",
                            "ann_date",
                        )? == Some(income_announcement)
                        {
                            matching.push(index);
                        }
                    }
                    if !matching.is_empty() {
                        candidates = matching;
                    }
                }
            }

            let indicator_index = candidates.first().copied();
            if let Some(index) = indicator_index {
                used_indicators[index] = true;
            }
            reports.push(Self::parse_financial_report_row(
                Some(income),
                indicator_index.map(|index| &indicator_rows[index]),
                fetched_at,
            )?);
        }

        for (index, indicator) in indicator_rows.iter().enumerate() {
            if !used_indicators[index] {
                reports.push(Self::parse_financial_report_row(
                    None,
                    Some(indicator),
                    fetched_at,
                )?);
            }
        }

        Ok(reports)
    }

    fn normalize_dividend_status(value: Option<&str>) -> &'static str {
        let Some(value) = value else {
            return "unknown";
        };
        let lower = value.trim().to_ascii_lowercase();
        if value.contains("实施") || lower.contains("implemented") || lower.contains("completed")
        {
            "implemented"
        } else if value.contains("股东大会")
            || value.contains("董事会通过")
            || lower.contains("approved")
        {
            "approved"
        } else if value.contains("预案") || lower.contains("proposal") || lower.contains("proposed")
        {
            "proposed"
        } else {
            "unknown"
        }
    }

    fn parse_dividend_records(
        data: &Value,
        fetched_at: DateTime<Utc>,
    ) -> Result<Vec<DividendRecord>> {
        Self::company_rows(data, "dividend")?
            .into_iter()
            .map(|row| {
                let code = Self::required_company_text(&row, "dividend", "ts_code")?;
                let end_date = Self::required_company_date(&row, "dividend", "end_date")?;
                let announcement_date = Self::optional_company_date(&row, "dividend", "ann_date")?;
                let implementation_announcement =
                    Self::optional_company_date(&row, "dividend", "imp_ann_date")?;
                let record_date = Self::optional_company_date(&row, "dividend", "record_date")?;
                let ex_date = Self::optional_company_date(&row, "dividend", "ex_date")?;
                let pay_date = Self::optional_company_date(&row, "dividend", "pay_date")?;
                let progress = Self::optional_company_text(&row, "dividend", "div_proc")?;
                let implementation_status = Self::normalize_dividend_status(progress.as_deref());
                let cash_dividend = Self::optional_company_decimal(&row, "dividend", "cash_div")?;
                let cash_dividend_tax =
                    Self::optional_company_decimal(&row, "dividend", "cash_div_tax")?;
                let stock_dividend = Self::optional_company_decimal(&row, "dividend", "stk_div")?;
                let stock_bonus = Self::optional_company_decimal(&row, "dividend", "stk_bo_rate")?;
                let stock_conversion =
                    Self::optional_company_decimal(&row, "dividend", "stk_co_rate")?;
                let stock_ratio =
                    stock_dividend.or_else(|| match (stock_bonus, stock_conversion) {
                        (None, None) => None,
                        (bonus, conversion) => Some(
                            bonus.unwrap_or(Decimal::ZERO) + conversion.unwrap_or(Decimal::ZERO),
                        ),
                    });
                let base_date = Self::optional_company_date(&row, "dividend", "base_date")?;
                let action_date = announcement_date.or(base_date);
                let action_key = match action_date {
                    Some(date) => format!(
                        "{}:{}:{}",
                        code,
                        end_date.format("%Y%m%d"),
                        date.format("%Y%m%d")
                    ),
                    None => format!("{}:{}", code, end_date.format("%Y%m%d")),
                };
                let available_date = if implementation_status == "implemented" {
                    implementation_announcement.or(announcement_date)
                } else {
                    announcement_date
                };
                let raw_payload = Value::Object(row);
                let source_revision = Self::content_revision(&raw_payload);

                Ok(DividendRecord {
                    source: Self::source(),
                    action_key,
                    code,
                    announcement_date,
                    record_date,
                    ex_date,
                    pay_date,
                    implementation_status: implementation_status.to_string(),
                    cash_dividend,
                    cash_dividend_tax,
                    stock_ratio,
                    source_revision,
                    raw_payload,
                    available_at: Self::deterministic_available_at(available_date, fetched_at),
                    ingested_at: fetched_at,
                })
            })
            .collect()
    }

    async fn get_sector_name_map(&self) -> Result<HashMap<String, String>> {
        let data = self.call("ths_index", json!({}), "ts_code,name").await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();
        let idx = |name: &str| {
            fields
                .iter()
                .position(|f| f.as_str() == Some(name))
                .unwrap_or(999)
        };
        let (i_code, i_name) = (idx("ts_code"), idx("name"));

        let mut names = HashMap::new();
        for row in items {
            if let Some(arr) = row.as_array() {
                if let (Some(code), Some(name)) = (
                    arr.get(i_code).and_then(|v| v.as_str()),
                    arr.get(i_name).and_then(|v| v.as_str()),
                ) {
                    names.insert(code.to_string(), name.to_string());
                }
            }
        }
        Ok(names)
    }

    fn parse_date(s: &str) -> Option<NaiveDate> {
        NaiveDate::parse_from_str(s, "%Y%m%d").ok()
    }

    fn safe_f64(v: &Value) -> f64 {
        match v {
            Value::Number(n) => n.as_f64().unwrap_or(0.0),
            Value::String(s) => s.parse().unwrap_or(0.0),
            _ => 0.0,
        }
    }

    fn safe_i64(v: &Value) -> i64 {
        match v {
            Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_f64().map(|value| value as i64))
                .unwrap_or(0),
            Value::String(s) => s.parse().unwrap_or(0),
            _ => 0,
        }
    }

    fn volume_lots_to_shares(v: &Value) -> i64 {
        (Self::safe_f64(v) * 100.0).round() as i64
    }

    fn optional_f64(v: Option<&Value>) -> Option<f64> {
        match v {
            Some(Value::Number(n)) => n.as_f64(),
            Some(Value::String(s)) if !s.trim().is_empty() => s.parse().ok(),
            _ => None,
        }
    }

    fn optional_i32(v: Option<&Value>) -> Option<i32> {
        match v {
            Some(Value::Number(n)) => n.as_i64().and_then(|v| i32::try_from(v).ok()),
            Some(Value::String(s)) if !s.trim().is_empty() => s.parse().ok(),
            _ => None,
        }
    }

    fn optional_date(v: Option<&Value>) -> Option<NaiveDate> {
        v.and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .and_then(Self::parse_date)
    }

    fn field_index(fields: &[Value], name: &str) -> usize {
        fields
            .iter()
            .position(|f| f.as_str() == Some(name))
            .unwrap_or(usize::MAX)
    }

    fn row_str<'a>(row: &'a [Value], index: usize) -> Option<&'a str> {
        row.get(index).and_then(|v| v.as_str())
    }

    fn row_value(row: &[Value], index: usize) -> Option<&Value> {
        row.get(index)
    }

    fn source() -> String {
        "tushare".to_string()
    }

    fn empty_capabilities() -> PointInTimeCapabilities {
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

    fn set_capability(capabilities: &mut PointInTimeCapabilities, capability: &str, value: bool) {
        match capability {
            "security_master_history" => capabilities.security_master_history = value,
            "corporate_actions" => capabilities.corporate_actions = value,
            "adjustment_factors" => capabilities.adjustment_factors = value,
            "daily_basic" => capabilities.daily_basic = value,
            "daily_security_status" => capabilities.daily_security_status = value,
            "historical_index_bars" => capabilities.historical_index_bars = value,
            "historical_sector_membership" => capabilities.historical_sector_membership = value,
            _ => {}
        }
    }

    fn unsupported_detail(error: &AppError) -> String {
        let message = error.to_string();
        let lower = message.to_ascii_lowercase();
        if lower.contains("permission")
            || lower.contains("unauthor")
            || lower.contains("权限")
            || lower.contains("积分")
        {
            format!("unsupported: unauthorized: {}", message)
        } else {
            format!("unsupported: {}", message)
        }
    }

    fn record_probe_result(
        capabilities: &mut PointInTimeCapabilities,
        capability: &str,
        result: Result<Value>,
    ) {
        match result {
            Ok(_) => {
                Self::set_capability(capabilities, capability, true);
                capabilities
                    .details
                    .insert(capability.to_string(), "supported".to_string());
            }
            Err(error) => {
                Self::set_capability(capabilities, capability, false);
                capabilities
                    .details
                    .insert(capability.to_string(), Self::unsupported_detail(&error));
            }
        }
    }

    fn record_endpoint_probe_result(
        capabilities: &mut PointInTimeCapabilities,
        capability: &str,
        endpoint: &str,
        result: Result<Value>,
    ) {
        match result {
            Ok(_) => {
                Self::set_capability(capabilities, capability, true);
                capabilities
                    .details
                    .insert(capability.to_string(), format!("supported: {}", endpoint));
            }
            Err(error) => {
                Self::set_capability(capabilities, capability, false);
                capabilities.details.insert(
                    capability.to_string(),
                    format!("{}: {}", endpoint, Self::unsupported_detail(&error)),
                );
            }
        }
    }

    fn record_compound_probe_result(
        capabilities: &mut PointInTimeCapabilities,
        capability: &str,
        dependencies: &[(&str, Result<Value>)],
    ) {
        let missing: Vec<String> = dependencies
            .iter()
            .filter_map(|(endpoint, result)| match result {
                Ok(_) => None,
                Err(error) => Some(format!("{}: {}", endpoint, Self::unsupported_detail(error))),
            })
            .collect();

        if missing.is_empty() {
            Self::set_capability(capabilities, capability, true);
            capabilities.details.insert(
                capability.to_string(),
                format!(
                    "supported: {}",
                    dependencies
                        .iter()
                        .map(|(endpoint, _)| *endpoint)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            );
        } else {
            Self::set_capability(capabilities, capability, false);
            capabilities.details.insert(
                capability.to_string(),
                format!("unsupported: missing dependencies: {}", missing.join("; ")),
            );
        }
    }

    async fn probe_capabilities_with<C, Fut>(mut call: C) -> PointInTimeCapabilities
    where
        C: FnMut(&'static str, Value, &'static str) -> Fut,
        Fut: Future<Output = Result<Value>>,
    {
        let mut capabilities = Self::empty_capabilities();
        let today = Utc::now().date_naive();
        let start = today - Duration::days(14);
        let start_date = start.format("%Y%m%d").to_string();
        let end_date = today.format("%Y%m%d").to_string();
        let trade_date = end_date.clone();

        Self::record_probe_result(
            &mut capabilities,
            "security_master_history",
            call(
                "stock_basic",
                json!({ "exchange": "", "list_status": "L" }),
                "ts_code,name,market,exchange,list_status,list_date,delist_date",
            )
            .await,
        );
        Self::record_probe_result(
            &mut capabilities,
            "corporate_actions",
            call(
                "dividend",
                json!({ "start_date": start_date, "end_date": end_date }),
                "ts_code,ann_date,record_date,ex_date,pay_date,cash_div,stk_div,stk_bo_rate,stk_co_rate",
            )
            .await,
        );
        Self::record_probe_result(
            &mut capabilities,
            "adjustment_factors",
            call(
                "adj_factor",
                json!({
                    "ts_code": "600000.SH",
                    "start_date": start.format("%Y%m%d").to_string(),
                    "end_date": today.format("%Y%m%d").to_string(),
                }),
                "ts_code,trade_date,adj_factor",
            )
            .await,
        );
        Self::record_probe_result(
            &mut capabilities,
            "daily_basic",
            call(
                "daily_basic",
                json!({ "trade_date": trade_date }),
                "ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,ps,total_share,float_share,total_mv,circ_mv",
            )
            .await,
        );

        let daily_status_dependencies = [
            (
                "daily",
                call(
                    "daily",
                    json!({ "trade_date": today.format("%Y%m%d").to_string() }),
                    "ts_code,trade_date,close",
                )
                .await,
            ),
            (
                "stk_limit",
                call(
                    "stk_limit",
                    json!({ "trade_date": today.format("%Y%m%d").to_string() }),
                    "ts_code,trade_date,up_limit,down_limit",
                )
                .await,
            ),
            (
                "suspend_d",
                call(
                    "suspend_d",
                    json!({ "suspend_date": today.format("%Y%m%d").to_string() }),
                    "ts_code,suspend_date,suspend_type",
                )
                .await,
            ),
            (
                "stock_basic",
                call(
                    "stock_basic",
                    json!({ "exchange": "", "list_status": "L" }),
                    "ts_code,list_date",
                )
                .await,
            ),
            (
                "namechange",
                call(
                    "namechange",
                    json!({ "ts_code": "600000.SH" }),
                    "ts_code,name,start_date,end_date,change_reason",
                )
                .await,
            ),
        ];
        Self::record_compound_probe_result(
            &mut capabilities,
            "daily_security_status",
            &daily_status_dependencies,
        );

        Self::record_probe_result(
            &mut capabilities,
            "historical_index_bars",
            call(
                "index_daily",
                json!({
                    "ts_code": "000001.SH",
                    "start_date": start.format("%Y%m%d").to_string(),
                    "end_date": today.format("%Y%m%d").to_string(),
                }),
                "ts_code,trade_date,close,pct_chg,vol,amount",
            )
            .await,
        );

        let ths_member_probe = call(
            "ths_member",
            json!({ "ts_code": "885001.TI" }),
            "ts_code,con_code,con_name",
        )
        .await;
        if ths_member_probe.is_err() {
            Self::record_endpoint_probe_result(
                &mut capabilities,
                "historical_sector_membership",
                "ths_member",
                ths_member_probe,
            );
        } else {
            Self::mark_unsupported(
                &mut capabilities,
                "historical_sector_membership",
                "Tushare ths_member is current membership and is not verified historical as_of membership",
            );
        }

        capabilities
    }

    fn mark_unsupported(
        capabilities: &mut PointInTimeCapabilities,
        capability: &str,
        detail: impl Into<String>,
    ) {
        Self::set_capability(capabilities, capability, false);
        capabilities.details.insert(
            capability.to_string(),
            format!("unsupported: {}", detail.into()),
        );
    }

    async fn require_capability(&self, capability: &str) -> Result<()> {
        let capabilities = match self.point_in_time_capabilities.read().await.clone() {
            Some(capabilities) => capabilities,
            None => self.probe_capabilities().await?,
        };

        if Self::capability_supported(&capabilities, capability) {
            return Ok(());
        }

        let detail = capabilities
            .details
            .get(capability)
            .cloned()
            .unwrap_or_else(|| "unsupported".to_string());
        Err(AppError::DataProvider(format!(
            "Tushare point-in-time capability '{}' is unavailable: {}",
            capability, detail
        )))
    }

    fn parse_security_master_versions(
        data: &Value,
        fetched_at: DateTime<Utc>,
    ) -> Vec<SecurityMasterVersion> {
        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();
        let i_code = Self::field_index(&fields, "ts_code");
        let i_name = Self::field_index(&fields, "name");
        let i_market = Self::field_index(&fields, "market");
        let i_exchange = Self::field_index(&fields, "exchange");
        let i_status = Self::field_index(&fields, "list_status");
        let i_list_date = Self::field_index(&fields, "list_date");
        let i_delist_date = Self::field_index(&fields, "delist_date");

        items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(SecurityMasterVersion {
                    code: Self::row_str(arr, i_code)?.to_string(),
                    name: Self::row_str(arr, i_name)?.to_string(),
                    market: Self::row_str(arr, i_market).map(|v| v.to_string()),
                    exchange: Self::row_str(arr, i_exchange).map(|v| v.to_string()),
                    list_status: Self::row_str(arr, i_status).unwrap_or("").to_string(),
                    list_date: Self::optional_date(Self::row_value(arr, i_list_date)),
                    delist_date: Self::optional_date(Self::row_value(arr, i_delist_date)),
                    available_at: fetched_at,
                    ingested_at: fetched_at,
                    availability_quality: AvailabilityQuality::Observed,
                    source: Self::source(),
                })
            })
            .collect()
    }

    fn parse_corporate_actions(data: &Value, fetched_at: DateTime<Utc>) -> Vec<CorporateAction> {
        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();
        let i_code = Self::field_index(&fields, "ts_code");
        let i_ann = Self::field_index(&fields, "ann_date");
        let i_record = Self::field_index(&fields, "record_date");
        let i_ex = Self::field_index(&fields, "ex_date");
        let i_pay = Self::field_index(&fields, "pay_date");
        let i_cash = Self::field_index(&fields, "cash_div");
        let i_stk_div = Self::field_index(&fields, "stk_div");
        let i_bo = Self::field_index(&fields, "stk_bo_rate");
        let i_co = Self::field_index(&fields, "stk_co_rate");

        items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                let code = Self::row_str(arr, i_code)?.to_string();
                let announcement_date = Self::optional_date(Self::row_value(arr, i_ann));
                let ex_date = Self::optional_date(Self::row_value(arr, i_ex));
                let cash_dividend = Self::optional_f64(Self::row_value(arr, i_cash));
                let stock_ratio = [
                    Self::optional_f64(Self::row_value(arr, i_stk_div)),
                    Self::optional_f64(Self::row_value(arr, i_bo)),
                    Self::optional_f64(Self::row_value(arr, i_co)),
                ]
                .into_iter()
                .flatten()
                .sum::<f64>();
                let stock_ratio = (stock_ratio * 1_000_000.0).round() / 1_000_000.0;
                let stock_ratio = if stock_ratio > 0.0 {
                    Some(stock_ratio)
                } else {
                    None
                };
                let action_type = match (cash_dividend.unwrap_or(0.0) > 0.0, stock_ratio.is_some())
                {
                    (true, true) => "dividend",
                    (true, false) => "cash_dividend",
                    (false, true) => "stock_dividend",
                    (false, false) => "corporate_action",
                };
                Some(CorporateAction {
                    source: Self::source(),
                    action_key: format!(
                        "{}:{}:{}",
                        code,
                        announcement_date
                            .map(|d| d.format("%Y%m%d").to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        ex_date
                            .map(|d| d.format("%Y%m%d").to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    ),
                    code,
                    action_type: action_type.to_string(),
                    announcement_date,
                    record_date: Self::optional_date(Self::row_value(arr, i_record)),
                    ex_date,
                    pay_date: Self::optional_date(Self::row_value(arr, i_pay)),
                    cash_dividend,
                    stock_ratio,
                    rights_ratio: None,
                    rights_price: None,
                    available_at: fetched_at,
                    ingested_at: fetched_at,
                    availability_quality: AvailabilityQuality::Observed,
                })
            })
            .collect()
    }

    fn parse_adjustment_factors(data: &Value, fetched_at: DateTime<Utc>) -> Vec<AdjustmentFactor> {
        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();
        let i_code = Self::field_index(&fields, "ts_code");
        let i_date = Self::field_index(&fields, "trade_date");
        let i_factor = Self::field_index(&fields, "adj_factor");

        items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(AdjustmentFactor {
                    code: Self::row_str(arr, i_code)?.to_string(),
                    trade_date: Self::optional_date(Self::row_value(arr, i_date))?,
                    adj_factor: Self::optional_f64(Self::row_value(arr, i_factor))?,
                    available_at: fetched_at,
                    ingested_at: fetched_at,
                    availability_quality: AvailabilityQuality::Observed,
                    source: Self::source(),
                })
            })
            .collect()
    }

    fn parse_daily_basics(data: &Value, fetched_at: DateTime<Utc>) -> Vec<DailyBasicSnapshot> {
        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();
        let i_code = Self::field_index(&fields, "ts_code");
        let i_date = Self::field_index(&fields, "trade_date");
        let i_turnover = Self::field_index(&fields, "turnover_rate");
        let i_volume_ratio = Self::field_index(&fields, "volume_ratio");
        let i_pe = Self::field_index(&fields, "pe");
        let i_pb = Self::field_index(&fields, "pb");
        let i_ps = Self::field_index(&fields, "ps");
        let i_total_share = Self::field_index(&fields, "total_share");
        let i_float_share = Self::field_index(&fields, "float_share");
        let i_total_mv = Self::field_index(&fields, "total_mv");
        let i_circ_mv = Self::field_index(&fields, "circ_mv");

        items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(DailyBasicSnapshot {
                    code: Self::row_str(arr, i_code)?.to_string(),
                    trade_date: Self::optional_date(Self::row_value(arr, i_date))?,
                    turnover_rate: Self::optional_f64(Self::row_value(arr, i_turnover)),
                    volume_ratio: Self::optional_f64(Self::row_value(arr, i_volume_ratio)),
                    pe: Self::optional_f64(Self::row_value(arr, i_pe)),
                    pb: Self::optional_f64(Self::row_value(arr, i_pb)),
                    ps: Self::optional_f64(Self::row_value(arr, i_ps)),
                    total_share: Self::optional_f64(Self::row_value(arr, i_total_share)),
                    float_share: Self::optional_f64(Self::row_value(arr, i_float_share)),
                    total_mv: Self::optional_f64(Self::row_value(arr, i_total_mv)),
                    circ_mv: Self::optional_f64(Self::row_value(arr, i_circ_mv)),
                    available_at: fetched_at,
                    ingested_at: fetched_at,
                    availability_quality: AvailabilityQuality::Observed,
                    source: Self::source(),
                })
            })
            .collect()
    }

    fn parse_security_statuses(
        data: &Value,
        fetched_at: DateTime<Utc>,
    ) -> Vec<SecurityDailyStatus> {
        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();
        let i_code = Self::field_index(&fields, "ts_code");
        let i_date = Self::field_index(&fields, "trade_date");
        let i_listed_days = Self::field_index(&fields, "list_days");
        let i_is_st = Self::field_index(&fields, "is_st");
        let i_suspended = Self::field_index(&fields, "is_suspended");
        let i_suspend_type = Self::field_index(&fields, "suspend_type");
        let i_up_limit = Self::field_index(&fields, "up_limit");
        let i_close = Self::field_index(&fields, "close");
        let i_limit_pct = Self::field_index(&fields, "price_limit_pct");

        items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                let is_st = Self::row_str(arr, i_is_st)
                    .map(|v| matches!(v.to_ascii_uppercase().as_str(), "Y" | "TRUE" | "1"))
                    .unwrap_or(false);
                let explicit_suspended = Self::row_str(arr, i_suspended)
                    .map(|v| matches!(v.to_ascii_uppercase().as_str(), "Y" | "TRUE" | "1"))
                    .unwrap_or(false);
                let suspend_type = Self::row_str(arr, i_suspend_type).unwrap_or("");
                let price_limit_pct = Self::optional_f64(Self::row_value(arr, i_limit_pct))
                    .or_else(|| {
                        let up = Self::optional_f64(Self::row_value(arr, i_up_limit))?;
                        let close = Self::optional_f64(Self::row_value(arr, i_close))?;
                        if close == 0.0 {
                            None
                        } else {
                            Some(((up / close - 1.0) * 100.0 * 100.0).round() / 100.0)
                        }
                    });

                Some(SecurityDailyStatus {
                    code: Self::row_str(arr, i_code)?.to_string(),
                    trade_date: Self::optional_date(Self::row_value(arr, i_date))?,
                    listed_days: Self::optional_i32(Self::row_value(arr, i_listed_days)),
                    is_st,
                    is_suspended: explicit_suspended || !suspend_type.trim().is_empty(),
                    price_limit_pct,
                    available_at: fetched_at,
                    ingested_at: fetched_at,
                    availability_quality: AvailabilityQuality::Observed,
                    source: Self::source(),
                })
            })
            .collect()
    }

    fn assemble_security_statuses(
        trade_date: NaiveDate,
        fetched_at: DateTime<Utc>,
        daily: &Value,
        limits: &Value,
        suspensions: &Value,
        masters: &Value,
        namechanges: &Value,
    ) -> Vec<SecurityDailyStatus> {
        let daily_fields = daily["fields"].as_array().cloned().unwrap_or_default();
        let i_daily_code = Self::field_index(&daily_fields, "ts_code");
        let i_daily_close = Self::field_index(&daily_fields, "close");
        let close_by_code = daily["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some((
                    Self::row_str(arr, i_daily_code)?.to_string(),
                    Self::optional_f64(Self::row_value(arr, i_daily_close))?,
                ))
            })
            .collect::<HashMap<_, _>>();

        let limit_fields = limits["fields"].as_array().cloned().unwrap_or_default();
        let i_limit_code = Self::field_index(&limit_fields, "ts_code");
        let i_up_limit = Self::field_index(&limit_fields, "up_limit");
        let i_down_limit = Self::field_index(&limit_fields, "down_limit");
        let limit_by_code = limits["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some((
                    Self::row_str(arr, i_limit_code)?.to_string(),
                    (
                        Self::optional_f64(Self::row_value(arr, i_up_limit)),
                        Self::optional_f64(Self::row_value(arr, i_down_limit)),
                    ),
                ))
            })
            .collect::<HashMap<_, _>>();

        let suspension_fields = suspensions["fields"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let i_suspension_code = Self::field_index(&suspension_fields, "ts_code");
        let suspended = suspensions["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                row.as_array()
                    .and_then(|arr| Self::row_str(arr, i_suspension_code))
                    .map(|v| v.to_string())
            })
            .collect::<HashSet<_>>();

        let master_fields = masters["fields"].as_array().cloned().unwrap_or_default();
        let i_master_code = Self::field_index(&master_fields, "ts_code");
        let i_list_date = Self::field_index(&master_fields, "list_date");
        let listed_by_code = masters["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some((
                    Self::row_str(arr, i_master_code)?.to_string(),
                    Self::optional_date(Self::row_value(arr, i_list_date))?,
                ))
            })
            .collect::<HashMap<_, _>>();

        let namechange_fields = namechanges["fields"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let i_namechange_code = Self::field_index(&namechange_fields, "ts_code");
        let i_name = Self::field_index(&namechange_fields, "name");
        let i_start_date = Self::field_index(&namechange_fields, "start_date");
        let i_end_date = Self::field_index(&namechange_fields, "end_date");
        let i_change_reason = Self::field_index(&namechange_fields, "change_reason");
        let mut st_codes = HashSet::new();
        for row in namechanges["items"].as_array().cloned().unwrap_or_default() {
            let Some(arr) = row.as_array() else { continue };
            let Some(code) = Self::row_str(arr, i_namechange_code) else {
                continue;
            };
            let name = Self::row_str(arr, i_name).unwrap_or("");
            let start =
                Self::optional_date(Self::row_value(arr, i_start_date)).unwrap_or(NaiveDate::MIN);
            let end =
                Self::optional_date(Self::row_value(arr, i_end_date)).unwrap_or(NaiveDate::MAX);
            let reason = Self::row_str(arr, i_change_reason).unwrap_or("");
            if start <= trade_date
                && trade_date <= end
                && (name.to_ascii_uppercase().contains("ST")
                    || reason.to_ascii_uppercase().contains("ST"))
            {
                st_codes.insert(code.to_string());
            }
        }

        let codes = close_by_code
            .keys()
            .chain(suspended.iter())
            .cloned()
            .collect::<BTreeSet<_>>();

        codes
            .into_iter()
            .map(|code| {
                let price_limit_pct = close_by_code.get(&code).and_then(|close| {
                    let (up, _down) = limit_by_code.get(&code)?;
                    let up = (*up)?;
                    if *close == 0.0 {
                        None
                    } else {
                        Some(((up / close - 1.0) * 100.0 * 100.0).round() / 100.0)
                    }
                });
                let listed_days = listed_by_code.get(&code).map(|list_date| {
                    trade_date
                        .signed_duration_since(*list_date)
                        .num_days()
                        .saturating_add(1) as i32
                });
                SecurityDailyStatus {
                    code: code.clone(),
                    trade_date,
                    listed_days,
                    is_st: st_codes.contains(&code),
                    is_suspended: suspended.contains(&code),
                    price_limit_pct,
                    available_at: fetched_at,
                    ingested_at: fetched_at,
                    availability_quality: AvailabilityQuality::Observed,
                    source: Self::source(),
                }
            })
            .collect()
    }

    fn parse_index_daily_bars(data: &Value, fetched_at: DateTime<Utc>) -> Vec<IndexDailyBar> {
        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();
        let i_code = Self::field_index(&fields, "ts_code");
        let i_date = Self::field_index(&fields, "trade_date");
        let i_close = Self::field_index(&fields, "close");
        let i_pct = Self::field_index(&fields, "pct_chg");
        let i_vol = Self::field_index(&fields, "vol");
        let i_amount = Self::field_index(&fields, "amount");

        items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(IndexDailyBar {
                    code: Self::row_str(arr, i_code)?.to_string(),
                    trade_date: Self::optional_date(Self::row_value(arr, i_date))?,
                    close: Self::optional_f64(Self::row_value(arr, i_close))?,
                    change_pct: Self::optional_f64(Self::row_value(arr, i_pct)),
                    volume: Self::row_value(arr, i_vol).map(Self::safe_i64),
                    amount: Self::optional_f64(Self::row_value(arr, i_amount)),
                    available_at: fetched_at,
                    ingested_at: fetched_at,
                    availability_quality: AvailabilityQuality::Observed,
                    source: Self::source(),
                })
            })
            .collect()
    }

    #[allow(dead_code)]
    pub fn to_sina_code(&self, tushare_code: &str) -> String {
        if let Some((num, market)) = tushare_code.split_once('.') {
            match market {
                "SH" => format!("sh{}", num),
                "SZ" => format!("sz{}", num),
                _ => tushare_code.to_lowercase().replace('.', ""),
            }
        } else {
            tushare_code.to_string()
        }
    }
}

#[async_trait]
impl PointInTimeDataProvider for TushareClient {
    async fn probe_capabilities(&self) -> Result<PointInTimeCapabilities> {
        let capabilities = Self::probe_capabilities_with(|api_name, params, fields| async move {
            self.call(api_name, params, fields).await
        })
        .await;
        *self.point_in_time_capabilities.write().await = Some(capabilities.clone());
        Ok(capabilities)
    }

    async fn get_security_master_versions(&self) -> Result<Vec<SecurityMasterVersion>> {
        self.require_capability("security_master_history").await?;
        let fetched_at = Utc::now();
        let mut rows = Vec::new();
        for status in ["L", "D", "P"] {
            let data = self
                .call(
                    "stock_basic",
                    json!({ "exchange": "", "list_status": status }),
                    "ts_code,name,market,exchange,list_status,list_date,delist_date",
                )
                .await?;
            rows.extend(Self::parse_security_master_versions(&data, fetched_at));
        }
        Ok(rows)
    }

    async fn get_corporate_actions(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<CorporateAction>> {
        self.require_capability("corporate_actions").await?;
        let fetched_at = Utc::now();
        let data = self
            .call(
                "dividend",
                json!({
                    "start_date": start.format("%Y%m%d").to_string(),
                    "end_date": end.format("%Y%m%d").to_string(),
                }),
                "ts_code,ann_date,record_date,ex_date,pay_date,cash_div,stk_div,stk_bo_rate,stk_co_rate",
            )
            .await?;
        Ok(Self::parse_corporate_actions(&data, fetched_at))
    }

    async fn get_adjustment_factors(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<AdjustmentFactor>> {
        self.require_capability("adjustment_factors").await?;
        let fetched_at = Utc::now();
        let data = self
            .call(
                "adj_factor",
                json!({
                    "start_date": start.format("%Y%m%d").to_string(),
                    "end_date": end.format("%Y%m%d").to_string(),
                }),
                "ts_code,trade_date,adj_factor",
            )
            .await?;
        Ok(Self::parse_adjustment_factors(&data, fetched_at))
    }

    async fn get_daily_basics(&self, trade_date: NaiveDate) -> Result<Vec<DailyBasicSnapshot>> {
        self.require_capability("daily_basic").await?;
        let fetched_at = Utc::now();
        let data = self
            .call(
                "daily_basic",
                json!({ "trade_date": trade_date.format("%Y%m%d").to_string() }),
                "ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,ps,total_share,float_share,total_mv,circ_mv",
            )
            .await?;
        Ok(Self::parse_daily_basics(&data, fetched_at))
    }

    async fn get_security_statuses(
        &self,
        trade_date: NaiveDate,
    ) -> Result<Vec<SecurityDailyStatus>> {
        self.require_capability("daily_security_status").await?;
        let fetched_at = Utc::now();
        let date = trade_date.format("%Y%m%d").to_string();

        let daily = self
            .call(
                "daily",
                json!({ "trade_date": date }),
                "ts_code,trade_date,close",
            )
            .await?;
        let limits = self
            .call(
                "stk_limit",
                json!({ "trade_date": trade_date.format("%Y%m%d").to_string() }),
                "ts_code,trade_date,up_limit,down_limit",
            )
            .await?;
        let suspensions = self
            .call(
                "suspend_d",
                json!({ "suspend_date": trade_date.format("%Y%m%d").to_string() }),
                "ts_code,suspend_date,suspend_type",
            )
            .await?;
        let masters = self
            .call(
                "stock_basic",
                json!({ "exchange": "", "list_status": "L" }),
                "ts_code,list_date",
            )
            .await?;
        let namechanges = self
            .call(
                "namechange",
                json!({}),
                "ts_code,name,start_date,end_date,change_reason",
            )
            .await?;

        Ok(Self::assemble_security_statuses(
            trade_date,
            fetched_at,
            &daily,
            &limits,
            &suspensions,
            &masters,
            &namechanges,
        ))
    }

    async fn get_index_daily_range(
        &self,
        codes: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<IndexDailyBar>> {
        self.require_capability("historical_index_bars").await?;
        let fetched_at = Utc::now();
        let mut rows = Vec::new();
        for code in codes {
            let data = self
                .call(
                    "index_daily",
                    json!({
                        "ts_code": code,
                        "start_date": start.format("%Y%m%d").to_string(),
                        "end_date": end.format("%Y%m%d").to_string(),
                    }),
                    "ts_code,trade_date,close,pct_chg,vol,amount",
                )
                .await?;
            rows.extend(Self::parse_index_daily_bars(&data, fetched_at));
        }
        Ok(rows)
    }

    async fn get_sector_memberships(
        &self,
        _as_of_date: NaiveDate,
    ) -> Result<Vec<SectorMembership>> {
        self.require_capability("historical_sector_membership")
            .await?;
        Err(AppError::DataProvider(
            "Tushare has no verified historical as_of sector membership provider".to_string(),
        ))
    }
}

#[async_trait]
impl CompanyDataProvider for TushareClient {
    async fn financial_reports(
        &self,
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<FinancialReport>> {
        if start > end {
            return Err(AppError::BadRequest(format!(
                "company financial window starts after it ends: {start} > {end}"
            )));
        }
        let params = Self::company_window_params(code, start, end);
        let (income, indicator) = tokio::try_join!(
            self.call("income", params.clone(), Self::income_fields()),
            self.call("fina_indicator", params, Self::indicator_fields()),
        )?;
        Self::parse_financial_reports(&income, &indicator, Utc::now())
    }

    async fn dividends(
        &self,
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<DividendRecord>> {
        if start > end {
            return Err(AppError::BadRequest(format!(
                "company dividend window starts after it ends: {start} > {end}"
            )));
        }
        let data = self
            .call(
                "dividend",
                Self::company_window_params(code, start, end),
                Self::dividend_fields(),
            )
            .await?;
        Self::parse_dividend_records(&data, Utc::now())
    }
}

#[async_trait]
impl DataProvider for TushareClient {
    fn name(&self) -> &'static str {
        "tushare"
    }

    async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
        let data = self
            .call(
                "stock_basic",
                json!({ "exchange": "", "list_status": "L" }),
                "ts_code,symbol,name,market,industry",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| -> usize {
            fields
                .iter()
                .position(|f| f.as_str() == Some(name))
                .unwrap_or(999)
        };
        let i_code = idx("ts_code");
        let i_name = idx("name");
        let i_market = idx("market");
        let i_industry = idx("industry");

        let stocks = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(StockInfo {
                    code: arr.get(i_code)?.as_str()?.to_string(),
                    name: arr.get(i_name)?.as_str()?.to_string(),
                    market: arr.get(i_market)?.as_str().unwrap_or("").to_string(),
                    industry: arr
                        .get(i_industry)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                })
            })
            .collect();

        Ok(stocks)
    }

    async fn get_daily_bars_by_date(&self, trade_date: NaiveDate) -> Result<Vec<(String, Candle)>> {
        let date_str = trade_date.format("%Y%m%d").to_string();

        let data = self
            .call(
                "daily",
                json!({ "trade_date": date_str }),
                "ts_code,trade_date,open,high,low,close,vol,amount",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| {
            fields
                .iter()
                .position(|f| f.as_str() == Some(name))
                .unwrap_or(999)
        };
        let (i_code, i_date, i_open, i_high, i_low, i_close, i_vol, i_amt) = (
            idx("ts_code"),
            idx("trade_date"),
            idx("open"),
            idx("high"),
            idx("low"),
            idx("close"),
            idx("vol"),
            idx("amount"),
        );

        let bars = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                let code = arr.get(i_code)?.as_str()?.to_string();
                let date = Self::parse_date(arr.get(i_date)?.as_str()?)?;
                Some((
                    code,
                    Candle {
                        trade_date: date,
                        open: Self::safe_f64(arr.get(i_open)?),
                        high: Self::safe_f64(arr.get(i_high)?),
                        low: Self::safe_f64(arr.get(i_low)?),
                        close: Self::safe_f64(arr.get(i_close)?),
                        volume: Self::volume_lots_to_shares(arr.get(i_vol)?),
                        amount: Self::safe_f64(arr.get(i_amt)?) * 1000.0, // thousands -> yuan
                        turnover: None,
                        pe: None,
                        pb: None,
                    },
                ))
            })
            .collect();

        Ok(bars)
    }

    async fn get_daily_bars_for_stock(
        &self,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Candle>> {
        let data = self
            .call(
                "daily",
                json!({
                    "ts_code": code,
                    "start_date": start_date.format("%Y%m%d").to_string(),
                    "end_date": end_date.format("%Y%m%d").to_string(),
                }),
                "trade_date,open,high,low,close,vol,amount",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| {
            fields
                .iter()
                .position(|f| f.as_str() == Some(name))
                .unwrap_or(999)
        };
        let (i_date, i_open, i_high, i_low, i_close, i_vol, i_amt) = (
            idx("trade_date"),
            idx("open"),
            idx("high"),
            idx("low"),
            idx("close"),
            idx("vol"),
            idx("amount"),
        );

        let mut bars: Vec<Candle> = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(Candle {
                    trade_date: Self::parse_date(arr.get(i_date)?.as_str()?)?,
                    open: Self::safe_f64(arr.get(i_open)?),
                    high: Self::safe_f64(arr.get(i_high)?),
                    low: Self::safe_f64(arr.get(i_low)?),
                    close: Self::safe_f64(arr.get(i_close)?),
                    volume: Self::volume_lots_to_shares(arr.get(i_vol)?),
                    amount: Self::safe_f64(arr.get(i_amt)?) * 1000.0,
                    turnover: None,
                    pe: None,
                    pb: None,
                })
            })
            .collect();

        bars.sort_by_key(|b| b.trade_date);
        Ok(bars)
    }

    async fn get_trading_dates(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
        let data = self
            .call(
                "trade_cal",
                json!({
                    "exchange": "SSE",
                    "start_date": start.format("%Y%m%d").to_string(),
                    "end_date": end.format("%Y%m%d").to_string(),
                    "is_open": "1"
                }),
                "cal_date",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let dates = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Self::parse_date(arr.first()?.as_str()?)
            })
            .collect();

        Ok(dates)
    }

    async fn get_limit_up_stocks(&self, trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        let date_str = trade_date.format("%Y%m%d").to_string();
        let data = self
            .call(
                "limit_list_d",
                json!({ "trade_date": date_str, "limit_type": "U" }),
                "ts_code,name,trade_date,close,pct_chg,fd_amount,first_time,last_time,open_times,strth,limit",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| {
            fields
                .iter()
                .position(|f| f.as_str() == Some(name))
                .unwrap_or(999)
        };
        let (
            i_code,
            i_name,
            i_date,
            i_close,
            i_pct,
            i_fd,
            i_first,
            i_last,
            i_open,
            i_strth,
            i_limit,
        ) = (
            idx("ts_code"),
            idx("name"),
            idx("trade_date"),
            idx("close"),
            idx("pct_chg"),
            idx("fd_amount"),
            idx("first_time"),
            idx("last_time"),
            idx("open_times"),
            idx("strth"),
            idx("limit"),
        );

        let stocks = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some(LimitUpStock {
                    code: arr.get(i_code)?.as_str()?.to_string(),
                    name: arr.get(i_name)?.as_str().unwrap_or("").to_string(),
                    trade_date: Self::parse_date(arr.get(i_date)?.as_str()?)?,
                    close: Self::safe_f64(arr.get(i_close)?),
                    pct_chg: Self::safe_f64(arr.get(i_pct)?),
                    fd_amount: Self::safe_f64(arr.get(i_fd)?),
                    first_time: arr
                        .get(i_first)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    last_time: arr
                        .get(i_last)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    open_times: Self::safe_i64(arr.get(i_open)?) as i32,
                    strth: Self::safe_f64(arr.get(i_strth)?),
                    limit: arr
                        .get(i_limit)
                        .and_then(|v| v.as_str())
                        .unwrap_or("U")
                        .to_string(),
                })
            })
            .collect();

        Ok(stocks)
    }

    async fn get_index_daily(
        &self,
        code: &str,
        trade_date: NaiveDate,
    ) -> Result<Option<IndexData>> {
        let date_str = trade_date.format("%Y%m%d").to_string();
        let data = self
            .call(
                "index_daily",
                json!({ "ts_code": code, "trade_date": date_str }),
                "ts_code,trade_date,close,pct_chg,vol,amount",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| {
            fields
                .iter()
                .position(|f| f.as_str() == Some(name))
                .unwrap_or(999)
        };
        let (i_code, i_date, i_close, i_pct, i_vol, i_amt) = (
            idx("ts_code"),
            idx("trade_date"),
            idx("close"),
            idx("pct_chg"),
            idx("vol"),
            idx("amount"),
        );

        let names = [
            ("000001.SH", "上证指数"),
            ("399001.SZ", "深证成指"),
            ("399006.SZ", "创业板指"),
            ("000688.SH", "科创50"),
        ];
        let display_name = names
            .iter()
            .find(|(c, _)| *c == code)
            .map(|(_, n)| *n)
            .unwrap_or(code);

        Ok(items.first().and_then(|row| {
            let arr = row.as_array()?;
            Some(IndexData {
                code: arr.get(i_code)?.as_str()?.to_string(),
                name: display_name.to_string(),
                trade_date: Self::parse_date(arr.get(i_date)?.as_str()?)?,
                close: Self::safe_f64(arr.get(i_close)?),
                change_pct: Self::safe_f64(arr.get(i_pct)?),
                volume: Self::safe_i64(arr.get(i_vol)?),
                amount: Self::safe_f64(arr.get(i_amt)?),
            })
        }))
    }

    async fn get_sector_data(&self, trade_date: NaiveDate) -> Result<Vec<SectorData>> {
        let date_str = trade_date.format("%Y%m%d").to_string();
        let names = match self.get_sector_name_map().await {
            Ok(map) => map,
            Err(e) => {
                warn!(
                    "Failed to load THS index names, falling back to code labels: {}",
                    e
                );
                HashMap::new()
            }
        };

        let data = self
            .call(
                "ths_daily",
                json!({ "trade_date": date_str }),
                "ts_code,trade_date,pct_change,turnover_rate,total_mv",
            )
            .await?;

        let items = data["items"].as_array().cloned().unwrap_or_default();
        let fields = data["fields"].as_array().cloned().unwrap_or_default();

        let idx = |name: &str| {
            fields
                .iter()
                .position(|f| f.as_str() == Some(name))
                .unwrap_or(999)
        };
        let (i_code, i_date, i_pct, i_mv) = (
            idx("ts_code"),
            idx("trade_date"),
            idx("pct_change"),
            idx("total_mv"),
        );

        let sectors = items
            .iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                let code = arr.get(i_code)?.as_str()?.to_string();
                let sector_type = if code.starts_with("88") {
                    "industry"
                } else {
                    "concept"
                }
                .to_string();
                Some(SectorData {
                    name: names.get(&code).cloned().unwrap_or_else(|| code.clone()),
                    code,
                    sector_type,
                    change_pct: Self::safe_f64(arr.get(i_pct)?),
                    amount: Self::safe_f64(arr.get(i_mv)?),
                    trade_date: Self::parse_date(arr.get(i_date)?.as_str()?)?,
                })
            })
            .collect();

        Ok(sectors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::market_snapshot::AvailabilityQuality;
    use crate::data::company::{CompanyDataProvider, FinancialFrequency};
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn income_fixture() -> Value {
        serde_json::json!({
            "fields": [
                "update_flag", "total_profit", "ts_code", "revenue", "end_date",
                "operate_profit", "ann_date", "n_income_attr_p", "report_type",
                "f_ann_date", "basic_eps", "total_revenue", "diluted_eps"
            ],
            "items": [
                ["1", "110000000000.1250", "600519.SH", "127554000000.4321", "20241231",
                 "90000000000.2500", "20250329", "86240000000.1234", "1",
                 "20250330", "49.93", "130000000000.9876", null],
                ["0", null, "600519.SH", null, "20240930", null, "20241026", null,
                 "1", "20241027", null, null, "36.50"],
                ["0", "100", "000001.SZ", "200", "20241231", "90", "20250301",
                 "80", "5", null, "1.25", "220", "1.20"]
            ]
        })
    }

    fn indicator_fixture() -> Value {
        serde_json::json!({
            "fields": [
                "netprofit_margin", "ts_code", "profit_dedt", "roe", "end_date",
                "report_type", "grossprofit_margin", "tr_yoy", "netprofit_yoy",
                "ann_date", "update_flag"
            ],
            "items": [
                ["52.345678", "600519.SH", "83000000000.4567", "31.200001", "20241231",
                 null, "91.234567", "12.345678", "15.765432", "20250330", "1"],
                [null, "000001.SZ", null, null, "20240630", "1", null, null, null,
                 "20240801", "0"]
            ]
        })
    }

    fn dividend_fixture() -> Value {
        serde_json::json!({
            "fields": [
                "cash_div_tax", "ts_code", "div_proc", "ann_date", "stk_co_rate",
                "end_date", "pay_date", "record_date", "stk_div", "imp_ann_date",
                "cash_div", "ex_date", "stk_bo_rate", "base_date"
            ],
            "items": [
                ["3.08", "600519.SH", "预案", "20250329", "0.10", "20241231", null,
                 null, "0.30", null, "2.76", null, "0.20", "20241231"],
                ["3.08", "600519.SH", "股东大会通过", "20250329", "0.10", "20241231",
                 null, null, "0.30", null, "2.76", null, "0.20", "20241231"],
                ["3.08", "600519.SH", "实施", "20250329", "0.10", "20241231",
                 "20250701", "20250625", "0.30", "20250618", "2.76", "20250626",
                 "0.20", "20241231"],
                [null, "000001.SZ", "取消分红", "20240401", null, "20231231", null,
                 null, null, null, null, null, null, null]
            ]
        })
    }

    fn reorder_table(data: &Value) -> Value {
        let fields = data["fields"].as_array().unwrap();
        let items = data["items"].as_array().unwrap();
        let order: Vec<usize> = (0..fields.len()).rev().collect();
        serde_json::json!({
            "fields": order.iter().map(|index| fields[*index].clone()).collect::<Vec<_>>(),
            "items": items
                .iter()
                .map(|row| {
                    let row = row.as_array().unwrap();
                    order.iter().map(|index| row[*index].clone()).collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
    }

    #[test]
    fn parses_company_financial_and_dividend_fixtures() {
        fn assert_provider<T: CompanyDataProvider>() {}
        assert_provider::<TushareClient>();

        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let rows = TushareClient::parse_financial_reports(
            &income_fixture(),
            &indicator_fixture(),
            fetched_at,
        )
        .unwrap();

        let annual = rows
            .iter()
            .find(|row| row.code == "600519.SH" && row.end_date.to_string() == "2024-12-31")
            .unwrap();
        assert_eq!(annual.frequency, FinancialFrequency::Annual);
        assert_eq!(annual.report_type, "1");
        assert_eq!(annual.announcement_date.unwrap().to_string(), "2025-03-30");
        assert_eq!(annual.total_revenue, Some(decimal("130000000000.9876")));
        assert_eq!(annual.revenue, Some(decimal("127554000000.4321")));
        assert_eq!(annual.operating_profit, Some(decimal("90000000000.2500")));
        assert_eq!(annual.total_profit, Some(decimal("110000000000.1250")));
        assert_eq!(annual.net_profit_parent, Some(decimal("86240000000.1234")));
        assert_eq!(
            annual.deducted_net_profit,
            Some(decimal("83000000000.4567"))
        );
        assert_eq!(annual.basic_eps, Some(decimal("49.93")));
        assert_eq!(annual.diluted_eps, None);
        assert_eq!(annual.roe, Some(decimal("31.200001")));
        assert_eq!(annual.gross_margin, Some(decimal("91.234567")));
        assert_eq!(annual.net_margin, Some(decimal("52.345678")));
        assert_eq!(annual.revenue_yoy, Some(decimal("12.345678")));
        assert_eq!(annual.net_profit_yoy, Some(decimal("15.765432")));
        assert_eq!(
            annual.available_at.to_rfc3339(),
            "2025-03-30T00:00:00+00:00"
        );
        assert_eq!(annual.ingested_at, fetched_at);
        assert_eq!(annual.source_revision.len(), 64);
        assert_eq!(
            annual.raw_payload["income"]["n_income_attr_p"],
            "86240000000.1234"
        );
        assert_eq!(annual.raw_payload["indicator"]["roe"], "31.200001");

        let quarter = rows
            .iter()
            .find(|row| row.code == "600519.SH" && row.end_date.to_string() == "2024-09-30")
            .unwrap();
        assert_eq!(quarter.frequency, FinancialFrequency::Quarterly);
        assert_eq!(quarter.total_revenue, None);
        assert_eq!(quarter.roe, None);
        assert!(quarter.raw_payload["indicator"].is_null());

        let adjusted = rows
            .iter()
            .find(|row| row.code == "000001.SZ" && row.report_type == "5")
            .unwrap();
        assert_eq!(adjusted.frequency, FinancialFrequency::Annual);
        assert_eq!(adjusted.deducted_net_profit, None);

        let indicator_only = rows
            .iter()
            .find(|row| row.code == "000001.SZ" && row.end_date.to_string() == "2024-06-30")
            .unwrap();
        assert_eq!(indicator_only.frequency, FinancialFrequency::Quarterly);
        assert_eq!(indicator_only.total_revenue, None);
        assert_eq!(indicator_only.raw_payload["income"], Value::Null);

        let dividends =
            TushareClient::parse_dividend_records(&dividend_fixture(), fetched_at).unwrap();
        assert_eq!(dividends.len(), 4);
        assert_eq!(dividends[0].implementation_status, "proposed");
        assert_eq!(dividends[1].implementation_status, "approved");
        assert_eq!(dividends[2].implementation_status, "implemented");
        assert_eq!(dividends[3].implementation_status, "unknown");
        assert_eq!(dividends[2].cash_dividend, Some(decimal("2.76")));
        assert_eq!(dividends[2].cash_dividend_tax, Some(decimal("3.08")));
        assert_eq!(dividends[2].stock_ratio, Some(decimal("0.30")));
        assert_eq!(dividends[0].action_key, dividends[2].action_key);
        assert_ne!(dividends[0].source_revision, dividends[2].source_revision);
        assert_eq!(
            dividends[0].available_at.to_rfc3339(),
            "2025-03-29T00:00:00+00:00"
        );
        assert_eq!(
            dividends[2].available_at.to_rfc3339(),
            "2025-06-18T00:00:00+00:00"
        );
        assert_eq!(dividends[2].raw_payload["stk_bo_rate"], "0.20");
        assert_eq!(dividends[2].raw_payload["stk_co_rate"], "0.10");
    }

    #[test]
    fn company_parsers_are_field_order_independent_and_revision_deterministic() {
        let first_fetch = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let later_fetch = Utc.with_ymd_and_hms(2026, 7, 20, 1, 5, 0).unwrap();
        let original = TushareClient::parse_financial_reports(
            &income_fixture(),
            &indicator_fixture(),
            first_fetch,
        )
        .unwrap();
        let reordered = TushareClient::parse_financial_reports(
            &reorder_table(&income_fixture()),
            &reorder_table(&indicator_fixture()),
            later_fetch,
        )
        .unwrap();

        assert_eq!(original.len(), reordered.len());
        for row in &original {
            let other = reordered
                .iter()
                .find(|candidate| {
                    candidate.code == row.code
                        && candidate.end_date == row.end_date
                        && candidate.report_type == row.report_type
                        && candidate.source_revision == row.source_revision
                })
                .unwrap();
            assert_eq!(other.available_at, row.available_at);
            assert_eq!(other.raw_payload, row.raw_payload);
            assert_eq!(other.ingested_at, later_fetch);
        }

        let original_dividends =
            TushareClient::parse_dividend_records(&dividend_fixture(), first_fetch).unwrap();
        let reordered_dividends =
            TushareClient::parse_dividend_records(&reorder_table(&dividend_fixture()), later_fetch)
                .unwrap();
        assert_eq!(original_dividends.len(), reordered_dividends.len());
        for row in &original_dividends {
            let other = reordered_dividends
                .iter()
                .find(|candidate| candidate.source_revision == row.source_revision)
                .unwrap();
            assert_eq!(other.action_key, row.action_key);
            assert_eq!(other.available_at, row.available_at);
            assert_eq!(other.raw_payload, row.raw_payload);
        }
    }

    #[test]
    fn company_parsers_tolerate_missing_optional_fields_and_reject_malformed_values() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let minimal_income = serde_json::json!({
            "fields": ["ts_code", "end_date", "report_type", "ann_date"],
            "items": [["600000.SH", "20240331", "1", null]]
        });
        let empty_indicators = serde_json::json!({ "fields": [], "items": [] });
        let rows =
            TushareClient::parse_financial_reports(&minimal_income, &empty_indicators, fetched_at)
                .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].frequency, FinancialFrequency::Quarterly);
        assert_eq!(rows[0].announcement_date, None);
        assert_eq!(rows[0].available_at, fetched_at);
        assert_eq!(rows[0].total_revenue, None);

        let malformed_decimal = serde_json::json!({
            "fields": ["ts_code", "end_date", "report_type", "total_revenue"],
            "items": [["600000.SH", "20241231", "1", "not-a-decimal"]]
        });
        let error = TushareClient::parse_financial_reports(
            &malformed_decimal,
            &empty_indicators,
            fetched_at,
        )
        .unwrap_err();
        assert!(error.to_string().contains("income.total_revenue"));
        assert!(error.to_string().contains("not-a-decimal"));

        let malformed_date = serde_json::json!({
            "fields": ["ts_code", "end_date", "report_type"],
            "items": [["600000.SH", "20240230", "1"]]
        });
        let error =
            TushareClient::parse_financial_reports(&malformed_date, &empty_indicators, fetched_at)
                .unwrap_err();
        assert!(error.to_string().contains("income.end_date"));
        assert!(error.to_string().contains("20240230"));

        let malformed_dividend = serde_json::json!({
            "fields": ["ts_code", "end_date", "ann_date", "div_proc", "cash_div"],
            "items": [["600000.SH", "20231231", "20240401", "实施", {}]]
        });
        let error =
            TushareClient::parse_dividend_records(&malformed_dividend, fetched_at).unwrap_err();
        assert!(error.to_string().contains("dividend.cash_div"));
    }

    #[test]
    fn company_parser_preserves_exact_json_numeric_tokens() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let income: Value = serde_json::from_str(
            r#"{
                "fields":["ts_code","end_date","report_type","total_revenue"],
                "items":[["600000.SH","20241231","1",12345678901234567890.1234]]
            }"#,
        )
        .unwrap();
        let indicators = serde_json::json!({ "fields": [], "items": [] });

        let rows =
            TushareClient::parse_financial_reports(&income, &indicators, fetched_at).unwrap();

        assert_eq!(
            rows[0].total_revenue,
            Some(decimal("12345678901234567890.1234"))
        );

        let oversized_scientific: Value = serde_json::from_str(
            r#"{
                "fields":["ts_code","end_date","report_type","total_revenue"],
                "items":[["600000.SH","20241231","1",1.234567890123456789012345678901e0]]
            }"#,
        )
        .unwrap();
        let error =
            TushareClient::parse_financial_reports(&oversized_scientific, &indicators, fetched_at)
                .unwrap_err();
        assert!(error.to_string().contains("income.total_revenue"));
    }

    #[test]
    fn company_financial_parser_pairs_revisions_without_cross_joining() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let income = serde_json::json!({
            "fields": [
                "ts_code", "end_date", "report_type", "update_flag", "f_ann_date",
                "n_income_attr_p"
            ],
            "items": [
                ["600000.SH", "20241231", "1", "1", "20250401", "101.01"],
                ["600000.SH", "20241231", "1", "0", "20250330", "99.99"]
            ]
        });
        let indicators = serde_json::json!({
            "fields": [
                "roe", "ann_date", "update_flag", "report_type", "end_date", "ts_code"
            ],
            "items": [
                ["20.20", "20250330", "0", "1", "20241231", "600000.SH"],
                ["21.21", "20250401", "1", "1", "20241231", "600000.SH"]
            ]
        });

        let rows =
            TushareClient::parse_financial_reports(&income, &indicators, fetched_at).unwrap();

        assert_eq!(rows.len(), 2);
        let current = rows
            .iter()
            .find(|row| row.net_profit_parent == Some(decimal("101.01")))
            .unwrap();
        assert_eq!(current.roe, Some(decimal("21.21")));
        let previous = rows
            .iter()
            .find(|row| row.net_profit_parent == Some(decimal("99.99")))
            .unwrap();
        assert_eq!(previous.roe, Some(decimal("20.20")));
    }

    #[test]
    fn company_request_and_provider_error_shapes_are_explicit() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        assert_eq!(
            TushareClient::company_window_params("600519.SH", start, end),
            serde_json::json!({
                "ts_code": "600519.SH",
                "start_date": "20240101",
                "end_date": "20241231"
            })
        );
        for field in [
            "total_revenue",
            "revenue",
            "operate_profit",
            "total_profit",
            "n_income_attr_p",
            "basic_eps",
            "diluted_eps",
            "update_flag",
        ] {
            assert!(TushareClient::income_fields()
                .split(',')
                .any(|item| item == field));
        }
        for field in [
            "profit_dedt",
            "roe",
            "grossprofit_margin",
            "netprofit_margin",
            "tr_yoy",
            "netprofit_yoy",
            "update_flag",
        ] {
            assert!(TushareClient::indicator_fields()
                .split(',')
                .any(|item| item == field));
        }
        assert!(!TushareClient::indicator_fields()
            .split(',')
            .any(|item| item == "report_type"));
        for field in [
            "div_proc",
            "cash_div",
            "cash_div_tax",
            "stk_div",
            "stk_bo_rate",
            "stk_co_rate",
            "imp_ann_date",
            "base_date",
        ] {
            assert!(TushareClient::dividend_fields()
                .split(',')
                .any(|item| item == field));
        }

        let provider_error = TushareClient::response_data(
            "income",
            &serde_json::json!({ "code": -2001, "msg": "invalid token", "data": null }),
        )
        .unwrap_err();
        assert_eq!(
            provider_error.to_string(),
            "Data provider error: Tushare income [-2001]: invalid token"
        );

        let rate_limit = TushareClient::response_data(
            "fina_indicator",
            &serde_json::json!({ "code": -2002, "msg": "每分钟最多访问60次", "data": null }),
        )
        .unwrap_err();
        assert_eq!(
            rate_limit.to_string(),
            "Data provider error: Tushare fina_indicator [-2002]: 每分钟最多访问60次"
        );

        let malformed = TushareClient::response_data(
            "dividend",
            &serde_json::json!({ "code": 0, "msg": null }),
        )
        .unwrap_err();
        assert!(malformed.to_string().contains("missing data"));
    }

    #[test]
    fn test_tushare_code_convert() {
        let client = TushareClient::new("test".to_string(), None);
        assert_eq!(client.to_sina_code("000001.SZ"), "sz000001");
        assert_eq!(client.to_sina_code("600036.SH"), "sh600036");
    }

    #[test]
    fn test_safe_f64() {
        assert_eq!(TushareClient::safe_f64(&serde_json::json!(1.5)), 1.5);
        assert_eq!(TushareClient::safe_f64(&serde_json::json!("2.3")), 2.3);
        assert_eq!(TushareClient::safe_f64(&serde_json::json!(null)), 0.0);
    }

    #[test]
    fn safe_i64_accepts_decimal_json_numbers() {
        assert_eq!(
            TushareClient::safe_i64(&serde_json::json!(30_828.0)),
            30_828
        );
    }

    #[test]
    fn volume_lots_to_shares_preserves_fractional_lots() {
        assert_eq!(
            TushareClient::volume_lots_to_shares(&serde_json::json!(0.03)),
            3
        );
        assert_eq!(
            TushareClient::volume_lots_to_shares(&serde_json::json!(30_828.0)),
            3_082_800
        );
    }

    #[test]
    fn parses_security_master_fixture() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 10, 9, 30, 0).unwrap();
        let data = serde_json::json!({
            "fields": ["ts_code", "name", "market", "exchange", "list_status", "list_date", "delist_date"],
            "items": [["600000.SH", "浦发银行", "主板", "SSE", "L", "19991110", null]]
        });

        let rows = TushareClient::parse_security_master_versions(&data, fetched_at);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].code, "600000.SH");
        assert_eq!(rows[0].name, "浦发银行");
        assert_eq!(rows[0].list_status, "L");
        assert_eq!(rows[0].available_at, fetched_at);
        assert_eq!(rows[0].availability_quality, AvailabilityQuality::Observed);
    }

    #[test]
    fn parses_corporate_actions_fixture() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 10, 9, 30, 0).unwrap();
        let data = serde_json::json!({
            "fields": ["ts_code", "ann_date", "record_date", "ex_date", "pay_date", "cash_div", "stk_div", "stk_bo_rate", "stk_co_rate", "base_date"],
            "items": [["600000.SH", "20240520", "20240605", "20240606", "20240606", 0.32, 0.1, 0.2, 0.3, "20231231"]]
        });

        let rows = TushareClient::parse_corporate_actions(&data, fetched_at);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].code, "600000.SH");
        assert_eq!(rows[0].action_key, "600000.SH:20240520:20240606");
        assert_eq!(rows[0].action_type, "dividend");
        assert_eq!(rows[0].cash_dividend, Some(0.32));
        assert_eq!(rows[0].stock_ratio, Some(0.6));
        assert_eq!(rows[0].available_at, fetched_at);
        assert_eq!(rows[0].availability_quality, AvailabilityQuality::Observed);
    }

    #[test]
    fn parses_daily_basics_fixture() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 10, 9, 30, 0).unwrap();
        let data = serde_json::json!({
            "fields": ["ts_code", "trade_date", "turnover_rate", "volume_ratio", "pe", "pb", "ps", "total_share", "float_share", "total_mv", "circ_mv"],
            "items": [["600000.SH", "20260710", 1.1, 0.9, 8.2, 0.7, 2.1, 2935208.04, 2810376.39, 3023264.28, 2894687.68]]
        });

        let rows = TushareClient::parse_daily_basics(&data, fetched_at);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].code, "600000.SH");
        assert_eq!(rows[0].turnover_rate, Some(1.1));
        assert_eq!(rows[0].total_mv, Some(3023264.28));
        assert_eq!(rows[0].available_at, fetched_at);
        assert_eq!(rows[0].availability_quality, AvailabilityQuality::Observed);
    }

    #[test]
    fn parses_adjustment_factors_fixture() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 10, 9, 30, 0).unwrap();
        let data = serde_json::json!({
            "fields": ["ts_code", "trade_date", "adj_factor"],
            "items": [["600000.SH", "20260710", 1.2345]]
        });

        let rows = TushareClient::parse_adjustment_factors(&data, fetched_at);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].code, "600000.SH");
        assert_eq!(rows[0].adj_factor, 1.2345);
        assert_eq!(rows[0].available_at, fetched_at);
        assert_eq!(rows[0].availability_quality, AvailabilityQuality::Observed);
    }

    #[test]
    fn parses_security_status_fixture() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 10, 9, 30, 0).unwrap();
        let data = serde_json::json!({
            "fields": ["ts_code", "trade_date", "list_days", "is_st", "suspend_type", "up_limit", "down_limit", "close"],
            "items": [["600000.SH", "20260710", 6760, "N", "", 11.0, 9.0, 10.0]]
        });

        let rows = TushareClient::parse_security_statuses(&data, fetched_at);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].code, "600000.SH");
        assert_eq!(rows[0].listed_days, Some(6760));
        assert!(!rows[0].is_st);
        assert!(!rows[0].is_suspended);
        assert_eq!(rows[0].price_limit_pct, Some(10.0));
        assert_eq!(rows[0].available_at, fetched_at);
        assert_eq!(rows[0].availability_quality, AvailabilityQuality::Observed);
    }

    #[test]
    fn security_statuses_include_suspensions_absent_from_daily() {
        let trade_date = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 10, 9, 30, 0).unwrap();
        let daily = serde_json::json!({
            "fields": ["ts_code", "trade_date", "close"],
            "items": [["600000.SH", "20260710", 10.0]]
        });
        let limits = serde_json::json!({
            "fields": ["ts_code", "trade_date", "up_limit", "down_limit"],
            "items": [["600000.SH", "20260710", 11.0, 9.0]]
        });
        let suspensions = serde_json::json!({
            "fields": ["ts_code", "suspend_date", "suspend_type"],
            "items": [["000001.SZ", "20260710", "P"]]
        });
        let masters = serde_json::json!({
            "fields": ["ts_code", "list_date"],
            "items": [
                ["600000.SH", "19991110"],
                ["000001.SZ", "19910403"]
            ]
        });
        let namechanges = serde_json::json!({
            "fields": ["ts_code", "name", "start_date", "end_date", "change_reason"],
            "items": []
        });

        let rows = TushareClient::assemble_security_statuses(
            trade_date,
            fetched_at,
            &daily,
            &limits,
            &suspensions,
            &masters,
            &namechanges,
        );

        let suspended_only = rows
            .iter()
            .find(|row| row.code == "000001.SZ")
            .expect("suspended-only security should be emitted");
        assert!(suspended_only.is_suspended);
        assert!(!suspended_only.is_st);
        assert_eq!(suspended_only.price_limit_pct, None);
        assert_eq!(
            suspended_only.availability_quality,
            AvailabilityQuality::Observed
        );

        let daily_row = rows
            .iter()
            .find(|row| row.code == "600000.SH")
            .expect("daily security should still be emitted");
        assert!(!daily_row.is_suspended);
        assert_eq!(daily_row.price_limit_pct, Some(10.0));
    }

    #[test]
    fn unauthorized_historical_sector_membership_is_reported_as_unsupported() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let capabilities = runtime.block_on({
            let calls = calls.clone();
            async move {
                TushareClient::probe_capabilities_with(|api_name, _params, _fields| {
                    let calls = calls.clone();
                    async move {
                        calls.lock().unwrap().push(api_name.to_string());
                        if api_name == "ths_member" {
                            Err(AppError::DataProvider(
                                "Tushare ths_member: unauthorized endpoint".to_string(),
                            ))
                        } else {
                            Ok(serde_json::json!({ "fields": [], "items": [] }))
                        }
                    }
                })
                .await
            }
        });

        assert!(!capabilities.historical_sector_membership);
        assert!(capabilities.details["historical_sector_membership"].contains("unauthorized"));
        assert!(calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call == "ths_member"));
    }

    #[test]
    fn daily_security_status_probe_requires_all_dependencies() {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let capabilities = runtime.block_on(async {
            TushareClient::probe_capabilities_with(|api_name, _params, _fields| async move {
                if api_name == "stock_basic" {
                    Err(AppError::DataProvider(
                        "Tushare stock_basic: unauthorized endpoint".to_string(),
                    ))
                } else {
                    Ok(serde_json::json!({ "fields": [], "items": [] }))
                }
            })
            .await
        });

        assert!(!capabilities.daily_security_status);
        let detail = &capabilities.details["daily_security_status"];
        assert!(detail.contains("stock_basic"));
        assert!(detail.contains("unauthorized"));
    }
}
