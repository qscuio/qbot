use async_trait::async_trait;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use reqwest::{Client, StatusCode};
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
use crate::data::chip::{OfficialChipBucket, OfficialChipPerformance, OfficialChipProvider};
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
const CHIP_PERFORMANCE_FIELDS: &str = "ts_code,trade_date,his_low,his_high,cost_5pct,cost_15pct,cost_50pct,cost_85pct,cost_95pct,weight_avg,winner_rate";
const CHIP_DISTRIBUTION_FIELDS: &str = "ts_code,trade_date,price,percent";
const OFFICIAL_CHIP_FIRST_DATE: (i32, u32, u32) = (2018, 1, 1);
const CHIP_PERFORMANCE_MAX_CALENDAR_DAYS: i64 = 366;
const CHIP_DISTRIBUTION_MAX_CALENDAR_DAYS: i64 = 45;
const TUSHARE_CHIP_ROW_LIMIT: usize = 6_000;

pub struct TushareClient {
    token: String,
    client: Client,
    point_in_time_capabilities: RwLock<Option<PointInTimeCapabilities>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FinancialJoinIdentity {
    code: String,
    end_date: NaiveDate,
    report_type: Option<String>,
    update_flag: Option<String>,
    announcement_date: Option<NaiveDate>,
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
            .map_err(|error| {
                AppError::DataProvider(format!("Tushare {api_name} HTTP request failed: {error}"))
            })?;

        let status = resp.status();
        let response_body = resp.text().await.map_err(|error| {
            AppError::DataProvider(format!(
                "Tushare {api_name} HTTP {status} body read failed: {error}"
            ))
        })?;
        Self::decode_response_body(api_name, status, &response_body)
    }

    fn decode_response_body(api_name: &str, status: StatusCode, body: &str) -> Result<Value> {
        let response: Value = serde_json::from_str(body).map_err(|error| {
            let excerpt = Self::bounded_response_excerpt(body);
            AppError::DataProvider(format!(
                "Tushare {api_name} HTTP {} returned non-JSON data: {error}; body: {excerpt}",
                status.as_u16()
            ))
        })?;

        if !status.is_success() {
            let code = response
                .get("code")
                .and_then(Value::as_i64)
                .map(|code| code.to_string())
                .unwrap_or_else(|| "unavailable".to_string());
            let message = response
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            let excerpt = Self::bounded_response_excerpt(body);
            return Err(AppError::DataProvider(format!(
                "Tushare {api_name} HTTP {}; provider code {code}: {message}; body: {excerpt}",
                status.as_u16(),
            )));
        }
        Self::response_data(api_name, &response)
    }

    fn bounded_response_excerpt(body: &str) -> String {
        let mut characters = body.chars();
        let excerpt: String = characters.by_ref().take(200).collect();
        if characters.next().is_some() {
            format!("{excerpt}…")
        } else {
            excerpt
        }
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

    fn chip_performance_fields() -> &'static str {
        CHIP_PERFORMANCE_FIELDS
    }

    fn chip_distribution_fields() -> &'static str {
        CHIP_DISTRIBUTION_FIELDS
    }

    fn company_window_params(code: &str, start: NaiveDate, end: NaiveDate) -> Value {
        json!({
            "ts_code": code,
            "start_date": start.format("%Y%m%d").to_string(),
            "end_date": end.format("%Y%m%d").to_string(),
        })
    }

    fn official_chip_window_params(code: &str, start: NaiveDate, end: NaiveDate) -> Value {
        json!({
            "ts_code": code,
            "start_date": start.format("%Y%m%d").to_string(),
            "end_date": end.format("%Y%m%d").to_string(),
        })
    }

    fn malformed_chip_data(endpoint: &str, detail: impl std::fmt::Display) -> AppError {
        AppError::DataProvider(format!("Tushare {endpoint} data is malformed: {detail}"))
    }

    fn official_chip_first_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(
            OFFICIAL_CHIP_FIRST_DATE.0,
            OFFICIAL_CHIP_FIRST_DATE.1,
            OFFICIAL_CHIP_FIRST_DATE.2,
        )
        .expect("the documented official-chip availability date is valid")
    }

    fn valid_tushare_stock_code(code: &str) -> bool {
        let Some((digits, exchange)) = code.split_once('.') else {
            return false;
        };
        digits.len() == 6
            && digits.bytes().all(|byte| byte.is_ascii_digit())
            && matches!(exchange, "SH" | "SZ" | "BJ")
    }

    fn validate_official_chip_window(
        endpoint: &str,
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
        maximum_calendar_days: i64,
    ) -> Result<()> {
        if !Self::valid_tushare_stock_code(code) {
            return Err(AppError::BadRequest(format!(
                "{endpoint} requires a valid Tushare stock code"
            )));
        }
        if start > end {
            return Err(AppError::BadRequest(format!(
                "{endpoint} window starts after it ends: {start} > {end}"
            )));
        }
        if start < Self::official_chip_first_date() {
            return Err(AppError::BadRequest(format!(
                "{endpoint} official history is unavailable before 2018-01-01"
            )));
        }
        let calendar_days = (end - start).num_days() + 1;
        if calendar_days > maximum_calendar_days {
            return Err(AppError::BadRequest(format!(
                "{endpoint} window contains {calendar_days} calendar days; maximum is {maximum_calendar_days}"
            )));
        }
        Ok(())
    }

    fn ensure_complete_chip_response(data: &Value, endpoint: &str) -> Result<()> {
        let items = data
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| Self::malformed_chip_data(endpoint, "missing items array"))?;
        let has_more = match data.get("has_more") {
            None => false,
            Some(Value::Bool(value)) => *value,
            Some(_) => {
                return Err(Self::malformed_chip_data(
                    endpoint,
                    "has_more must be a boolean",
                ))
            }
        };
        let total = match data.get("total") {
            None => None,
            Some(value) => Some(value.as_u64().ok_or_else(|| {
                Self::malformed_chip_data(endpoint, "total must be a non-negative integer")
            })?),
        };
        if has_more
            || items.len() >= TUSHARE_CHIP_ROW_LIMIT
            || total.is_some_and(|declared| declared > items.len() as u64)
        {
            return Err(AppError::DataProvider(format!(
                "Tushare {endpoint} response is truncated at the provider row limit"
            )));
        }
        Ok(())
    }

    fn chip_rows(
        data: &Value,
        endpoint: &str,
        required_fields: &[&str],
    ) -> Result<Vec<serde_json::Map<String, Value>>> {
        let fields = data
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| Self::malformed_chip_data(endpoint, "missing fields array"))?;
        let mut seen = HashSet::with_capacity(fields.len());
        for field in fields {
            let name = field
                .as_str()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    Self::malformed_chip_data(endpoint, "field names must be non-empty strings")
                })?;
            if !seen.insert(name) {
                return Err(Self::malformed_chip_data(
                    endpoint,
                    format!("duplicate field {name}"),
                ));
            }
        }
        for required in required_fields {
            if !seen.contains(required) {
                return Err(Self::malformed_chip_data(
                    endpoint,
                    format!("missing required field {required}"),
                ));
            }
        }
        Self::company_rows(data, endpoint)
    }

    fn required_chip_text(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
        field: &str,
    ) -> Result<String> {
        let value = row
            .get(field)
            .ok_or_else(|| Self::malformed_chip_data(endpoint, format!("missing {field}")))?;
        match value {
            Value::String(value) if !value.trim().is_empty() => Ok(value.trim().to_string()),
            _ => Err(Self::malformed_chip_data(
                endpoint,
                format!("{field} must be non-blank text"),
            )),
        }
    }

    fn required_chip_number(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
        field: &str,
    ) -> Result<f64> {
        let value = row
            .get(field)
            .ok_or_else(|| Self::malformed_chip_data(endpoint, format!("missing {field}")))?;
        let parsed = match value {
            Value::Number(value) => value.as_f64(),
            Value::String(value) if !value.trim().is_empty() => value.trim().parse::<f64>().ok(),
            _ => None,
        }
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            Self::malformed_chip_data(endpoint, format!("{field} must be a finite number"))
        })?;
        Ok(parsed)
    }

    fn required_chip_date(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
    ) -> Result<NaiveDate> {
        let value = Self::required_chip_text(row, endpoint, "trade_date")?;
        NaiveDate::parse_from_str(&value, "%Y%m%d").map_err(|_| {
            Self::malformed_chip_data(endpoint, format!("trade_date is not YYYYMMDD: {value}"))
        })
    }

    fn required_chip_code(row: &serde_json::Map<String, Value>, endpoint: &str) -> Result<String> {
        let code = Self::required_chip_text(row, endpoint, "ts_code")?;
        if !Self::valid_tushare_stock_code(&code) {
            return Err(Self::malformed_chip_data(
                endpoint,
                format!("invalid ts_code {code}"),
            ));
        }
        Ok(code)
    }

    fn parse_chip_performance(data: &Value) -> Result<Vec<OfficialChipPerformance>> {
        let required = [
            "ts_code",
            "trade_date",
            "his_low",
            "his_high",
            "cost_5pct",
            "cost_15pct",
            "cost_50pct",
            "cost_85pct",
            "cost_95pct",
            "weight_avg",
            "winner_rate",
        ];
        let mut seen = HashSet::new();
        let mut parsed = Self::chip_rows(data, "cyq_perf", &required)?
            .into_iter()
            .map(|row| {
                let code = Self::required_chip_code(&row, "cyq_perf")?;
                let trade_date = Self::required_chip_date(&row, "cyq_perf")?;
                if !seen.insert((code.clone(), trade_date)) {
                    return Err(Self::malformed_chip_data(
                        "cyq_perf",
                        format!("duplicate row for {code} on {trade_date}"),
                    ));
                }
                let historical_low = Self::required_chip_number(&row, "cyq_perf", "his_low")?;
                let historical_high = Self::required_chip_number(&row, "cyq_perf", "his_high")?;
                let cost_5pct = Self::required_chip_number(&row, "cyq_perf", "cost_5pct")?;
                let cost_15pct = Self::required_chip_number(&row, "cyq_perf", "cost_15pct")?;
                let cost_50pct = Self::required_chip_number(&row, "cyq_perf", "cost_50pct")?;
                let cost_85pct = Self::required_chip_number(&row, "cyq_perf", "cost_85pct")?;
                let cost_95pct = Self::required_chip_number(&row, "cyq_perf", "cost_95pct")?;
                let average_cost = Self::required_chip_number(&row, "cyq_perf", "weight_avg")?;
                let winner_rate = Self::required_chip_number(&row, "cyq_perf", "winner_rate")?;
                let ordered_costs = [
                    historical_low,
                    cost_5pct,
                    cost_15pct,
                    cost_50pct,
                    cost_85pct,
                    cost_95pct,
                    historical_high,
                ];
                if ordered_costs.iter().any(|value| *value <= 0.0)
                    || ordered_costs.windows(2).any(|pair| pair[0] > pair[1])
                    || average_cost < historical_low
                    || average_cost > historical_high
                    || !(0.0..=100.0).contains(&winner_rate)
                {
                    return Err(Self::malformed_chip_data(
                        "cyq_perf",
                        format!("invalid cost or percentage ranges for {code} on {trade_date}"),
                    ));
                }
                Ok(OfficialChipPerformance {
                    code,
                    trade_date,
                    historical_low,
                    historical_high,
                    cost_5pct,
                    cost_15pct,
                    cost_50pct,
                    cost_85pct,
                    cost_95pct,
                    average_cost,
                    winner_rate,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        parsed.sort_by(|left, right| {
            left.trade_date
                .cmp(&right.trade_date)
                .then_with(|| left.code.cmp(&right.code))
        });
        Ok(parsed)
    }

    fn parse_official_chip_distribution(data: &Value) -> Result<Vec<OfficialChipBucket>> {
        let required = ["ts_code", "trade_date", "price", "percent"];
        let mut seen = HashSet::new();
        let mut groups: BTreeMap<(String, NaiveDate), Vec<(f64, f64)>> = BTreeMap::new();
        for row in Self::chip_rows(data, "cyq_chips", &required)? {
            let code = Self::required_chip_code(&row, "cyq_chips")?;
            let trade_date = Self::required_chip_date(&row, "cyq_chips")?;
            let price = Self::required_chip_number(&row, "cyq_chips", "price")?;
            let percent = Self::required_chip_number(&row, "cyq_chips", "percent")?;
            if price <= 0.0 || !(0.0..=100.0).contains(&percent) {
                return Err(Self::malformed_chip_data(
                    "cyq_chips",
                    format!("invalid price or percent for {code} on {trade_date}"),
                ));
            }
            if !seen.insert((code.clone(), trade_date, price.to_bits())) {
                return Err(Self::malformed_chip_data(
                    "cyq_chips",
                    format!("duplicate price {price} for {code} on {trade_date}"),
                ));
            }
            groups
                .entry((code, trade_date))
                .or_default()
                .push((price, percent / 100.0));
        }

        let mut parsed = Vec::new();
        for ((code, trade_date), mut buckets) in groups {
            let total = buckets.iter().map(|(_, weight)| weight).sum::<f64>();
            if !total.is_finite() || total <= 0.0 {
                return Err(Self::malformed_chip_data(
                    "cyq_chips",
                    format!("zero or invalid total mass for {code} on {trade_date}"),
                ));
            }
            buckets.sort_by(|left, right| left.0.total_cmp(&right.0));
            parsed.extend(
                buckets
                    .into_iter()
                    .map(|(price, weight)| OfficialChipBucket {
                        code: code.clone(),
                        trade_date,
                        price,
                        weight: weight / total,
                    }),
            );
        }
        parsed.sort_by(|left, right| {
            left.trade_date
                .cmp(&right.trade_date)
                .then_with(|| left.code.cmp(&right.code))
                .then_with(|| left.price.total_cmp(&right.price))
        });
        Ok(parsed)
    }

    fn ensure_chip_rows_match_request<T, Code, Date>(
        rows: &[T],
        endpoint: &str,
        requested_code: &str,
        start: NaiveDate,
        end: NaiveDate,
        code_of: Code,
        date_of: Date,
    ) -> Result<()>
    where
        Code: Fn(&T) -> &str,
        Date: Fn(&T) -> NaiveDate,
    {
        for row in rows {
            let code = code_of(row);
            if code != requested_code {
                return Err(Self::malformed_chip_data(
                    endpoint,
                    format!("response code {code} does not match requested {requested_code}"),
                ));
            }
        }
        if start == end && !rows.iter().any(|row| date_of(row) == start) {
            return Err(AppError::DataProvider(format!(
                "Tushare {endpoint} data for {requested_code} on {start} is not ready"
            )));
        }
        for row in rows {
            let date = date_of(row);
            if date < start || date > end {
                return Err(Self::malformed_chip_data(
                    endpoint,
                    format!("response date {date} is outside requested window {start}..{end}"),
                ));
            }
        }
        Ok(())
    }

    fn company_report_periods(start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
        if start > end {
            return Err(AppError::BadRequest(format!(
                "company financial window starts after it ends: {start} > {end}"
            )));
        }
        if (end - start).num_days() > 366 {
            return Err(AppError::BadRequest(format!(
                "company financial report-period window exceeds one year: {start}..{end}"
            )));
        }

        let mut periods = Vec::new();
        for year in start.year()..=end.year() {
            for (month, day) in [(3, 31), (6, 30), (9, 30), (12, 31)] {
                let period = NaiveDate::from_ymd_opt(year, month, day)
                    .expect("supported report-period dates are valid");
                if period >= start && period <= end {
                    periods.push(period);
                }
            }
        }
        Ok(periods)
    }

    fn income_period_params(code: &str, period: NaiveDate) -> Value {
        json!({
            "ts_code": code,
            "period": period.format("%Y%m%d").to_string(),
        })
    }

    fn dividend_params(code: &str) -> Value {
        json!({ "ts_code": code })
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

    fn company_data_from_rows(rows: Vec<serde_json::Map<String, Value>>) -> Value {
        let fields: BTreeSet<String> = rows.iter().flat_map(|row| row.keys().cloned()).collect();
        let fields: Vec<String> = fields.into_iter().collect();
        let items: Vec<Vec<Value>> = rows
            .into_iter()
            .map(|row| {
                fields
                    .iter()
                    .map(|field| row.get(field).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        json!({ "fields": fields, "items": items })
    }

    fn ensure_complete_company_history(data: &Value, endpoint: &str) -> Result<()> {
        let has_more = data
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let item_count = data
            .get("items")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        let declared_total_exceeds_items = data
            .get("total")
            .and_then(Value::as_u64)
            .is_some_and(|total| total > item_count as u64);
        if has_more || declared_total_exceeds_items {
            return Err(AppError::DataProvider(format!(
                "Tushare {endpoint} history is truncated and the endpoint exposes no documented pagination contract"
            )));
        }
        Ok(())
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

    fn availability_semantics(
        source_event: Option<(&'static str, NaiveDate)>,
        fetched_at: DateTime<Utc>,
    ) -> (DateTime<Utc>, Value) {
        let (date, metadata) = match source_event {
            Some((field, date)) => (
                date,
                json!({
                    "kind": "source_event_date",
                    "source_field": field,
                    "date": date.to_string(),
                }),
            ),
            None => {
                let observed_on = fetched_at
                    .with_timezone(&crate::market_time::beijing_tz())
                    .date_naive();
                (
                    observed_on,
                    json!({
                        "kind": "observation_date",
                        "observed_on": observed_on.to_string(),
                    }),
                )
            }
        };
        let available_at = date
            .and_hms_opt(0, 0, 0)
            .expect("a valid date has a midnight")
            .and_local_timezone(crate::market_time::beijing_tz())
            .single()
            .expect("a fixed UTC+08:00 midnight is unambiguous")
            .with_timezone(&Utc);
        (available_at, metadata)
    }

    fn beijing_date(timestamp: DateTime<Utc>) -> NaiveDate {
        timestamp
            .with_timezone(&crate::market_time::beijing_tz())
            .date_naive()
    }

    fn content_revision(payload: &Value) -> String {
        let bytes = serde_json::to_vec(payload).expect("JSON values always serialize");
        format!("{:x}", Sha256::digest(bytes))
    }

    fn financial_join_identity(
        row: &serde_json::Map<String, Value>,
        endpoint: &str,
    ) -> Result<FinancialJoinIdentity> {
        let announcement_date = if endpoint == "income" {
            Self::optional_company_date(row, endpoint, "f_ann_date")?
                .or(Self::optional_company_date(row, endpoint, "ann_date")?)
        } else {
            Self::optional_company_date(row, endpoint, "ann_date")?
        };
        Ok(FinancialJoinIdentity {
            code: Self::required_company_text(row, endpoint, "ts_code")?,
            end_date: Self::required_company_date(row, endpoint, "end_date")?,
            report_type: Self::optional_company_text(row, endpoint, "report_type")?,
            update_flag: Self::optional_company_text(row, endpoint, "update_flag")?,
            announcement_date,
        })
    }

    fn financial_identities_match(
        income: &FinancialJoinIdentity,
        indicator: &FinancialJoinIdentity,
    ) -> bool {
        income.code == indicator.code
            && income.end_date == indicator.end_date
            && indicator
                .report_type
                .as_ref()
                .is_none_or(|report_type| income.report_type.as_ref() == Some(report_type))
            && income.update_flag == indicator.update_flag
            && income.announcement_date == indicator.announcement_date
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
            Some(row) => match Self::optional_company_date(row, "income", "f_ann_date")? {
                Some(date) => Some(("income.f_ann_date", date)),
                None => Self::optional_company_date(row, "income", "ann_date")?
                    .map(|date| ("income.ann_date", date)),
            },
            None => None,
        };
        let indicator_announcement = match indicator {
            Some(row) => Self::optional_company_date(row, "fina_indicator", "ann_date")?
                .map(|date| ("fina_indicator.ann_date", date)),
            None => None,
        };
        let announcement_date = income_announcement
            .map(|(_, date)| date)
            .or(indicator_announcement.map(|(_, date)| date));
        let source_event = match (income_announcement, indicator_announcement) {
            (Some(income), Some(indicator)) if indicator.1 > income.1 => Some(indicator),
            (Some(income), _) => Some(income),
            (None, indicator) => indicator,
        };
        let (available_at, availability) = Self::availability_semantics(source_event, fetched_at);

        let raw_payload = json!({
            "income": income.cloned().map(Value::Object).unwrap_or(Value::Null),
            "indicator": indicator.cloned().map(Value::Object).unwrap_or(Value::Null),
            "availability": availability,
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
            available_at,
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
        let mut indicator_for_income = vec![None; income_rows.len()];
        let mut typed_indicator_only = Vec::new();
        for (indicator_index, indicator_identity) in indicator_identities.iter().enumerate() {
            let candidates: Vec<usize> = income_identities
                .iter()
                .enumerate()
                .filter(|(_, income_identity)| {
                    Self::financial_identities_match(income_identity, indicator_identity)
                })
                .map(|(index, _)| index)
                .collect();

            if candidates.is_empty() && indicator_identity.report_type.is_some() {
                typed_indicator_only.push(indicator_index);
                continue;
            }
            if candidates.len() != 1 {
                return Err(Self::malformed_company_data(
                    "financial",
                    &format!(
                        "ambiguous fina_indicator identity for {}/{}: matched {} income rows",
                        indicator_identity.code,
                        indicator_identity.end_date,
                        candidates.len()
                    ),
                ));
            }

            let income_index = candidates[0];
            if indicator_for_income[income_index]
                .replace(indicator_index)
                .is_some()
            {
                let identity = &income_identities[income_index];
                return Err(Self::malformed_company_data(
                    "financial",
                    &format!(
                        "ambiguous income identity for {}/{}: matched multiple fina_indicator rows",
                        identity.code, identity.end_date
                    ),
                ));
            }
        }

        let mut reports = income_rows
            .iter()
            .enumerate()
            .map(|(index, income)| {
                Self::parse_financial_report_row(
                    Some(income),
                    indicator_for_income[index].map(|indicator| &indicator_rows[indicator]),
                    fetched_at,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        for indicator_index in typed_indicator_only {
            reports.push(Self::parse_financial_report_row(
                None,
                Some(&indicator_rows[indicator_index]),
                fetched_at,
            )?);
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
                let source_event = match implementation_status {
                    "implemented" => {
                        implementation_announcement.map(|date| ("dividend.imp_ann_date", date))
                    }
                    "proposed" => announcement_date.map(|date| ("dividend.ann_date", date)),
                    "approved" | "unknown" => None,
                    _ => unreachable!("dividend statuses are normalized"),
                };
                let (available_at, availability) =
                    Self::availability_semantics(source_event, fetched_at);
                let raw_payload = json!({
                    "dividend": Value::Object(row),
                    "availability": availability,
                });
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
                    available_at,
                    ingested_at: fetched_at,
                })
            })
            .collect()
    }

    async fn financial_reports_with<C, Fut>(
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
        fetched_at: DateTime<Utc>,
        mut call: C,
    ) -> Result<Vec<FinancialReport>>
    where
        C: FnMut(&'static str, Value, &'static str) -> Fut,
        Fut: Future<Output = Result<Value>>,
    {
        let periods = Self::company_report_periods(start, end)?;
        let mut income_rows = Vec::new();
        for period in periods {
            let data = call(
                "income",
                Self::income_period_params(code, period),
                Self::income_fields(),
            )
            .await?;
            income_rows.extend(Self::company_rows(&data, "income")?);
        }
        let indicator = call(
            "fina_indicator",
            Self::company_window_params(code, start, end),
            Self::indicator_fields(),
        )
        .await?;
        let income = Self::company_data_from_rows(income_rows);
        Self::parse_financial_reports(&income, &indicator, fetched_at)
    }

    async fn dividends_with<C, Fut>(
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
        fetched_at: DateTime<Utc>,
        mut call: C,
    ) -> Result<Vec<DividendRecord>>
    where
        C: FnMut(&'static str, Value, &'static str) -> Fut,
        Fut: Future<Output = Result<Value>>,
    {
        if start > end {
            return Err(AppError::BadRequest(format!(
                "company dividend window starts after it ends: {start} > {end}"
            )));
        }
        let data = call(
            "dividend",
            Self::dividend_params(code),
            Self::dividend_fields(),
        )
        .await?;
        Self::ensure_complete_company_history(&data, "dividend")?;
        let mut records = Self::parse_dividend_records(&data, fetched_at)?;
        records.retain(|record| {
            let effective_source_date = Self::beijing_date(record.available_at);
            effective_source_date >= start && effective_source_date <= end
        });
        Ok(records)
    }

    async fn chip_performance_with<C, Fut>(
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
        mut call: C,
    ) -> Result<Vec<OfficialChipPerformance>>
    where
        C: FnMut(&'static str, Value, &'static str) -> Fut,
        Fut: Future<Output = Result<Value>>,
    {
        Self::validate_official_chip_window(
            "cyq_perf",
            code,
            start,
            end,
            CHIP_PERFORMANCE_MAX_CALENDAR_DAYS,
        )?;
        let data = call(
            "cyq_perf",
            Self::official_chip_window_params(code, start, end),
            Self::chip_performance_fields(),
        )
        .await?;
        Self::ensure_complete_chip_response(&data, "cyq_perf")?;
        let rows = Self::parse_chip_performance(&data)?;
        Self::ensure_chip_rows_match_request(
            &rows,
            "cyq_perf",
            code,
            start,
            end,
            |row| row.code.as_str(),
            |row| row.trade_date,
        )?;
        Ok(rows)
    }

    async fn chip_distribution_with<C, Fut>(
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
        mut call: C,
    ) -> Result<Vec<OfficialChipBucket>>
    where
        C: FnMut(&'static str, Value, &'static str) -> Fut,
        Fut: Future<Output = Result<Value>>,
    {
        Self::validate_official_chip_window(
            "cyq_chips",
            code,
            start,
            end,
            CHIP_DISTRIBUTION_MAX_CALENDAR_DAYS,
        )?;
        let data = call(
            "cyq_chips",
            Self::official_chip_window_params(code, start, end),
            Self::chip_distribution_fields(),
        )
        .await?;
        Self::ensure_complete_chip_response(&data, "cyq_chips")?;
        let rows = Self::parse_official_chip_distribution(&data)?;
        Self::ensure_chip_rows_match_request(
            &rows,
            "cyq_chips",
            code,
            start,
            end,
            |row| row.code.as_str(),
            |row| row.trade_date,
        )?;
        Ok(rows)
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
        Self::financial_reports_with(code, start, end, Utc::now(), |api_name, params, fields| {
            self.call(api_name, params, fields)
        })
        .await
    }

    async fn dividends(
        &self,
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<DividendRecord>> {
        Self::dividends_with(code, start, end, Utc::now(), |api_name, params, fields| {
            self.call(api_name, params, fields)
        })
        .await
    }
}

#[async_trait]
impl OfficialChipProvider for TushareClient {
    async fn chip_performance(
        &self,
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<OfficialChipPerformance>> {
        Self::chip_performance_with(code, start, end, |api_name, params, fields| {
            self.call(api_name, params, fields)
        })
        .await
    }

    async fn chip_distribution(
        &self,
        code: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<OfficialChipBucket>> {
        Self::chip_distribution_with(code, start, end, |api_name, params, fields| {
            self.call(api_name, params, fields)
        })
        .await
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
    use crate::data::chip::OfficialChipProvider;
    use crate::data::company::{CompanyDataProvider, FinancialFrequency};
    use crate::storage::company_repository::CompanyRepository;
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};

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
                 null, "91.234567", "12.345678", "15.765432", "20250330", "1"]
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

    fn cyq_perf_fixture() -> Value {
        serde_json::json!({
            "fields": [
                "winner_rate", "cost_50pct", "ts_code", "his_high", "trade_date",
                "cost_5pct", "weight_avg", "cost_95pct", "his_low", "cost_15pct",
                "cost_85pct"
            ],
            "items": [
                ["72.5", "1500", "600519.SH", "1800", "20260717", "1200",
                 "1512.40", "1750", "1000", "1300", "1650"],
                [60, 1490, "600519.SH", 1790, "20260716", 1190,
                 1502.25, 1740, 990, 1290, 1640]
            ]
        })
    }

    fn cyq_chips_fixture() -> Value {
        serde_json::json!({
            "fields": ["percent", "trade_date", "price", "ts_code"],
            "items": [
                ["70", "20260716", "1510", "600519.SH"],
                ["30", "20260716", "1500", "600519.SH"],
                ["3", "20260717", "1520", "600519.SH"],
                ["1", "20260717", "1510", "600519.SH"]
            ]
        })
    }

    fn replace_table_cell(data: &Value, row: usize, field: &str, value: Value) -> Value {
        let mut changed = data.clone();
        let field_index = changed["fields"]
            .as_array()
            .unwrap()
            .iter()
            .position(|candidate| candidate.as_str() == Some(field))
            .unwrap();
        changed["items"][row][field_index] = value;
        changed
    }

    #[test]
    fn parses_official_chip_fixtures_by_name_and_normalizes_each_date() {
        fn assert_provider<T: OfficialChipProvider>() {}
        assert_provider::<TushareClient>();

        let performance = TushareClient::parse_chip_performance(&cyq_perf_fixture()).unwrap();
        assert_eq!(performance.len(), 2);
        assert_eq!(performance[0].trade_date.to_string(), "2026-07-16");
        assert_eq!(performance[1].average_cost, 1512.40);
        assert_eq!(performance[1].winner_rate, 72.5);
        assert_eq!(performance[1].cost_5pct, 1200.0);
        assert_eq!(performance[1].cost_95pct, 1750.0);

        let buckets =
            TushareClient::parse_official_chip_distribution(&cyq_chips_fixture()).unwrap();
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[0].trade_date.to_string(), "2026-07-16");
        assert_eq!(buckets[0].price, 1500.0);
        assert!((buckets[0].weight - 0.3).abs() < 1e-12);
        let by_date = buckets.iter().fold(BTreeMap::new(), |mut totals, bucket| {
            *totals.entry(bucket.trade_date).or_insert(0.0) += bucket.weight;
            totals
        });
        assert_eq!(by_date.len(), 2);
        assert!(by_date.values().all(|total| (total - 1.0).abs() < 1e-12));
        let latest: Vec<_> = buckets
            .iter()
            .filter(|bucket| bucket.trade_date.to_string() == "2026-07-17")
            .collect();
        assert_eq!(latest[0].price, 1510.0);
        assert!((latest[0].weight - 0.25).abs() < 1e-12);
        assert!((latest[1].weight - 0.75).abs() < 1e-12);
    }

    #[test]
    fn official_chip_parsers_reject_invalid_required_values() {
        for (field, value) in [
            ("weight_avg", Value::Null),
            ("weight_avg", serde_json::json!("  ")),
            ("weight_avg", serde_json::json!("not-a-number")),
            ("weight_avg", serde_json::json!("NaN")),
            ("weight_avg", serde_json::json!(-1)),
            ("winner_rate", serde_json::json!(101)),
            ("cost_15pct", serde_json::json!(1100)),
            ("weight_avg", serde_json::json!(1900)),
            ("trade_date", serde_json::json!("20260230")),
            ("ts_code", serde_json::json!("bad-code")),
        ] {
            let invalid = replace_table_cell(&cyq_perf_fixture(), 0, field, value);
            assert!(
                TushareClient::parse_chip_performance(&invalid).is_err(),
                "{field} should be rejected"
            );
        }

        for (field, value) in [
            ("percent", Value::Null),
            ("percent", serde_json::json!("")),
            ("percent", serde_json::json!("infinity")),
            ("percent", serde_json::json!(-1)),
            ("percent", serde_json::json!(101)),
            ("price", serde_json::json!(0)),
            ("trade_date", serde_json::json!("20261301")),
            ("ts_code", serde_json::json!("600519")),
        ] {
            let invalid = replace_table_cell(&cyq_chips_fixture(), 0, field, value);
            assert!(
                TushareClient::parse_official_chip_distribution(&invalid).is_err(),
                "{field} should be rejected"
            );
        }
    }

    #[test]
    fn official_chip_parsers_reject_bad_table_shapes_and_duplicates() {
        let mut missing = cyq_perf_fixture();
        missing["fields"].as_array_mut().unwrap().remove(0);
        for row in missing["items"].as_array_mut().unwrap() {
            row.as_array_mut().unwrap().remove(0);
        }
        assert!(TushareClient::parse_chip_performance(&missing).is_err());

        let mut duplicate_field = cyq_perf_fixture();
        duplicate_field["fields"][0] = serde_json::json!("cost_50pct");
        assert!(TushareClient::parse_chip_performance(&duplicate_field).is_err());

        let mut wrong_length = cyq_chips_fixture();
        wrong_length["items"][0].as_array_mut().unwrap().pop();
        assert!(TushareClient::parse_official_chip_distribution(&wrong_length).is_err());

        let mut duplicate_performance = cyq_perf_fixture();
        let row = duplicate_performance["items"][0].clone();
        duplicate_performance["items"]
            .as_array_mut()
            .unwrap()
            .push(row);
        assert!(TushareClient::parse_chip_performance(&duplicate_performance).is_err());

        let mut duplicate_bucket = cyq_chips_fixture();
        let row = duplicate_bucket["items"][0].clone();
        duplicate_bucket["items"].as_array_mut().unwrap().push(row);
        assert!(TushareClient::parse_official_chip_distribution(&duplicate_bucket).is_err());

        let mut zero_total = cyq_chips_fixture();
        let percent_index = zero_total["fields"]
            .as_array()
            .unwrap()
            .iter()
            .position(|field| field == "percent")
            .unwrap();
        for row in zero_total["items"].as_array_mut().unwrap() {
            row[percent_index] = serde_json::json!(0);
        }
        assert!(TushareClient::parse_official_chip_distribution(&zero_total).is_err());
    }

    #[tokio::test]
    async fn official_chip_provider_sends_exact_requests_and_accepts_weekend_ended_history() {
        let start = NaiveDate::from_ymd_opt(2026, 7, 16).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 7, 19).unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let performance = TushareClient::chip_performance_with(
            "600519.SH",
            start,
            end,
            move |api_name, params, fields| {
                recorded
                    .lock()
                    .unwrap()
                    .push((api_name.to_string(), params, fields.to_string()));
                async { Ok(cyq_perf_fixture()) }
            },
        )
        .await
        .unwrap();
        assert_eq!(performance.len(), 2);

        let recorded = calls.clone();
        let buckets = TushareClient::chip_distribution_with(
            "600519.SH",
            start,
            end,
            move |api_name, params, fields| {
                recorded
                    .lock()
                    .unwrap()
                    .push((api_name.to_string(), params, fields.to_string()));
                async { Ok(cyq_chips_fixture()) }
            },
        )
        .await
        .unwrap();
        assert_eq!(buckets.len(), 4);

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "cyq_perf");
        assert_eq!(
            calls[0].1,
            serde_json::json!({
                "ts_code": "600519.SH", "start_date": "20260716", "end_date": "20260719"
            })
        );
        assert_eq!(calls[0].2, TushareClient::chip_performance_fields());
        assert_eq!(calls[1].0, "cyq_chips");
        assert_eq!(calls[1].1, calls[0].1);
        assert_eq!(calls[1].2, TushareClient::chip_distribution_fields());
    }

    #[tokio::test]
    async fn official_chip_provider_rejects_invalid_windows_before_calling_upstream() {
        let calls = Arc::new(Mutex::new(0));
        for (start, end, distribution) in [
            (
                NaiveDate::from_ymd_opt(2017, 12, 31).unwrap(),
                NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(),
                false,
            ),
            (
                NaiveDate::from_ymd_opt(2026, 7, 19).unwrap(),
                NaiveDate::from_ymd_opt(2026, 7, 18).unwrap(),
                false,
            ),
            (
                NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 3, 1).unwrap(),
                true,
            ),
        ] {
            let recorded = calls.clone();
            let result = if distribution {
                TushareClient::chip_distribution_with("600519.SH", start, end, move |_, _, _| {
                    *recorded.lock().unwrap() += 1;
                    async { Ok(cyq_chips_fixture()) }
                })
                .await
                .map(|_| ())
            } else {
                TushareClient::chip_performance_with("600519.SH", start, end, move |_, _, _| {
                    *recorded.lock().unwrap() += 1;
                    async { Ok(cyq_perf_fixture()) }
                })
                .await
                .map(|_| ())
            };
            assert!(result.is_err());
        }
        assert_eq!(*calls.lock().unwrap(), 0);

        let result = TushareClient::chip_performance_with(
            "",
            NaiveDate::from_ymd_opt(2026, 7, 17).unwrap(),
            NaiveDate::from_ymd_opt(2026, 7, 17).unwrap(),
            |_, _, _| async { Ok(cyq_perf_fixture()) },
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn official_chip_provider_rejects_wrong_code_dates_and_stale_daily_results() {
        let date = NaiveDate::from_ymd_opt(2026, 7, 17).unwrap();
        let wrong_code = replace_table_cell(
            &cyq_perf_fixture(),
            0,
            "ts_code",
            serde_json::json!("000001.SZ"),
        );
        let error = TushareClient::chip_performance_with(
            "600519.SH",
            NaiveDate::from_ymd_opt(2026, 7, 16).unwrap(),
            date,
            move |_, _, _| {
                let response = wrong_code.clone();
                async move { Ok(response) }
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("600519.SH"));

        let empty = serde_json::json!({ "fields": [
            "ts_code", "trade_date", "his_low", "his_high", "cost_5pct", "cost_15pct",
            "cost_50pct", "cost_85pct", "cost_95pct", "weight_avg", "winner_rate"
        ], "items": [] });
        let error =
            TushareClient::chip_performance_with("600519.SH", date, date, move |_, _, _| {
                let response = empty.clone();
                async move { Ok(response) }
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not ready"));

        let mut stale = cyq_perf_fixture();
        let stale_row = stale["items"][1].clone();
        stale["items"] = serde_json::json!([stale_row]);
        let error =
            TushareClient::chip_performance_with("600519.SH", date, date, move |_, _, _| {
                let response = stale.clone();
                async move { Ok(response) }
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not ready"));

        let out_of_window = replace_table_cell(
            &cyq_chips_fixture(),
            0,
            "trade_date",
            serde_json::json!("20260715"),
        );
        let error = TushareClient::chip_distribution_with(
            "600519.SH",
            NaiveDate::from_ymd_opt(2026, 7, 16).unwrap(),
            date,
            move |_, _, _| {
                let response = out_of_window.clone();
                async move { Ok(response) }
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("outside"));
    }

    #[tokio::test]
    async fn official_chip_provider_rejects_truncated_responses() {
        let start = NaiveDate::from_ymd_opt(2026, 7, 16).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 7, 17).unwrap();
        for response in [
            serde_json::json!({ "fields": [], "items": [], "has_more": true }),
            serde_json::json!({ "fields": [], "items": [], "total": 1 }),
            serde_json::json!({
                "fields": ["ts_code", "trade_date", "price", "percent"],
                "items": vec![serde_json::json!(["600519.SH", "20260716", 1500, 1]); 6000]
            }),
        ] {
            let error =
                TushareClient::chip_distribution_with("600519.SH", start, end, move |_, _, _| {
                    let response = response.clone();
                    async move { Ok(response) }
                })
                .await
                .unwrap_err();
            assert!(error.to_string().contains("truncated"));
        }
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
            "2025-03-29T16:00:00+00:00"
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
            "2025-03-28T16:00:00+00:00"
        );
        assert_eq!(
            dividends[2].available_at.to_rfc3339(),
            "2025-06-17T16:00:00+00:00"
        );
        assert_eq!(dividends[2].raw_payload["dividend"]["stk_bo_rate"], "0.20");
        assert_eq!(dividends[2].raw_payload["dividend"]["stk_co_rate"], "0.10");
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
        for row in original_dividends.iter().filter(|row| {
            matches!(
                row.implementation_status.as_str(),
                "proposed" | "implemented"
            )
        }) {
            let other = reordered_dividends
                .iter()
                .find(|candidate| candidate.source_revision == row.source_revision)
                .unwrap();
            assert_eq!(other.action_key, row.action_key);
            assert_eq!(other.available_at, row.available_at);
            assert_eq!(other.raw_payload, row.raw_payload);
        }
        let same_day_reordered = TushareClient::parse_dividend_records(
            &reorder_table(&dividend_fixture()),
            Utc.with_ymd_and_hms(2026, 7, 19, 15, 0, 0).unwrap(),
        )
        .unwrap();
        for row in original_dividends
            .iter()
            .filter(|row| matches!(row.implementation_status.as_str(), "approved" | "unknown"))
        {
            let other = same_day_reordered
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
        assert_eq!(
            rows[0].available_at,
            Utc.with_ymd_and_hms(2026, 7, 18, 16, 0, 0).unwrap()
        );
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
    fn missing_source_events_use_replay_safe_observation_day_versions() {
        let first = Utc.with_ymd_and_hms(2026, 7, 19, 0, 15, 0).unwrap();
        let same_day = Utc.with_ymd_and_hms(2026, 7, 19, 15, 45, 0).unwrap();
        let next_day = Utc.with_ymd_and_hms(2026, 7, 19, 16, 5, 0).unwrap();
        let income = serde_json::json!({
            "fields": ["ts_code", "end_date", "report_type", "update_flag"],
            "items": [["600000.SH", "20241231", "1", "1"]]
        });
        let indicators = serde_json::json!({ "fields": [], "items": [] });

        let financial_first =
            TushareClient::parse_financial_reports(&income, &indicators, first).unwrap();
        let financial_same =
            TushareClient::parse_financial_reports(&income, &indicators, same_day).unwrap();
        let financial_next =
            TushareClient::parse_financial_reports(&income, &indicators, next_day).unwrap();
        assert_eq!(
            financial_first[0].source_revision,
            financial_same[0].source_revision
        );
        assert_eq!(
            financial_first[0].available_at,
            financial_same[0].available_at
        );
        assert_ne!(
            financial_first[0].source_revision,
            financial_next[0].source_revision
        );
        assert_eq!(
            financial_first[0].available_at,
            Utc.with_ymd_and_hms(2026, 7, 18, 16, 0, 0).unwrap()
        );
        assert_eq!(
            financial_next[0].available_at,
            Utc.with_ymd_and_hms(2026, 7, 19, 16, 0, 0).unwrap()
        );
        assert_eq!(
            financial_first[0].raw_payload["availability"],
            serde_json::json!({
                "kind": "observation_date",
                "observed_on": "2026-07-19"
            })
        );

        let approved = serde_json::json!({
            "fields": ["ts_code", "end_date", "ann_date", "div_proc"],
            "items": [["600000.SH", "20241231", "20250329", "股东大会通过"]]
        });
        let dividend_first = TushareClient::parse_dividend_records(&approved, first).unwrap();
        let dividend_same = TushareClient::parse_dividend_records(&approved, same_day).unwrap();
        let dividend_next = TushareClient::parse_dividend_records(&approved, next_day).unwrap();
        assert_eq!(
            dividend_first[0].source_revision,
            dividend_same[0].source_revision
        );
        assert_eq!(
            dividend_first[0].available_at,
            dividend_same[0].available_at
        );
        assert_ne!(
            dividend_first[0].source_revision,
            dividend_next[0].source_revision
        );
        assert_eq!(
            TushareClient::beijing_date(dividend_first[0].available_at),
            NaiveDate::from_ymd_opt(2026, 7, 19).unwrap()
        );
        assert_eq!(
            dividend_first[0].raw_payload["availability"],
            serde_json::json!({
                "kind": "observation_date",
                "observed_on": "2026-07-19"
            })
        );
    }

    #[test]
    fn observation_versions_follow_beijing_day_across_utc_boundaries() {
        let income = serde_json::json!({
            "fields": ["ts_code", "end_date", "report_type", "update_flag"],
            "items": [["600000.SH", "20241231", "1", "1"]]
        });
        let indicators = serde_json::json!({ "fields": [], "items": [] });
        let before_utc_midnight = Utc.with_ymd_and_hms(2026, 7, 19, 23, 59, 0).unwrap();
        let after_utc_midnight = Utc.with_ymd_and_hms(2026, 7, 20, 0, 1, 0).unwrap();
        let before_beijing_midnight = Utc.with_ymd_and_hms(2026, 7, 19, 15, 59, 0).unwrap();
        let after_beijing_midnight = Utc.with_ymd_and_hms(2026, 7, 19, 16, 1, 0).unwrap();

        let before_utc =
            TushareClient::parse_financial_reports(&income, &indicators, before_utc_midnight)
                .unwrap();
        let after_utc =
            TushareClient::parse_financial_reports(&income, &indicators, after_utc_midnight)
                .unwrap();
        assert_eq!(before_utc[0].source_revision, after_utc[0].source_revision);
        assert_eq!(
            before_utc[0].available_at,
            Utc.with_ymd_and_hms(2026, 7, 19, 16, 0, 0).unwrap()
        );
        assert_eq!(
            before_utc[0].raw_payload["availability"]["observed_on"],
            "2026-07-20"
        );

        let before_beijing =
            TushareClient::parse_financial_reports(&income, &indicators, before_beijing_midnight)
                .unwrap();
        let after_beijing =
            TushareClient::parse_financial_reports(&income, &indicators, after_beijing_midnight)
                .unwrap();
        assert_ne!(
            before_beijing[0].source_revision,
            after_beijing[0].source_revision
        );
        assert_eq!(
            before_beijing[0].raw_payload["availability"]["observed_on"],
            "2026-07-19"
        );
        assert_eq!(
            after_beijing[0].raw_payload["availability"]["observed_on"],
            "2026-07-20"
        );
    }

    #[tokio::test]
    async fn dividend_provider_filters_observations_by_beijing_day() {
        let beijing_day = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 23, 59, 0).unwrap();
        let approved = serde_json::json!({
            "fields": ["ts_code", "end_date", "ann_date", "div_proc"],
            "items": [["600000.SH", "20241231", "20250329", "股东大会通过"]]
        });

        let records = TushareClient::dividends_with(
            "600000.SH",
            beijing_day,
            beijing_day,
            fetched_at,
            move |_, _, _| {
                let approved = approved.clone();
                async move { Ok(approved) }
            },
        )
        .await
        .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].available_at,
            Utc.with_ymd_and_hms(2026, 7, 19, 16, 0, 0).unwrap()
        );
        assert_eq!(
            records[0].raw_payload["availability"]["observed_on"],
            "2026-07-20"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn financial_observation_versions_replay_cleanly_in_company_repository(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let repo = CompanyRepository::new(pool);
        let income = serde_json::json!({
            "fields": ["ts_code", "end_date", "report_type", "update_flag"],
            "items": [["600000.SH", "20241231", "1", "1"]]
        });
        let indicators = serde_json::json!({ "fields": [], "items": [] });
        let times = [
            Utc.with_ymd_and_hms(2026, 7, 19, 0, 15, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 7, 19, 15, 45, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 7, 19, 16, 5, 0).unwrap(),
        ];
        let versions = times
            .iter()
            .map(|time| TushareClient::parse_financial_reports(&income, &indicators, *time))
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(repo.upsert_financial_reports(&versions[0]).await?, 1);
        assert_eq!(repo.upsert_financial_reports(&versions[1]).await?, 0);
        assert_eq!(repo.upsert_financial_reports(&versions[2]).await?, 1);
        let history = repo
            .financial_history("600000.SH", FinancialFrequency::Annual, 10, None)
            .await?;
        assert_eq!(history.items[0].revision_count, 2);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn dividend_observation_versions_replay_cleanly_in_company_repository(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let repo = CompanyRepository::new(pool);
        let approved = serde_json::json!({
            "fields": ["ts_code", "end_date", "ann_date", "div_proc"],
            "items": [["600000.SH", "20241231", "20250329", "股东大会通过"]]
        });
        let times = [
            Utc.with_ymd_and_hms(2026, 7, 19, 0, 15, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 7, 19, 15, 45, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 7, 19, 16, 5, 0).unwrap(),
        ];
        let versions = times
            .iter()
            .map(|time| TushareClient::parse_dividend_records(&approved, *time))
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(repo.upsert_dividends(&versions[0]).await?, 1);
        assert_eq!(repo.upsert_dividends(&versions[1]).await?, 0);
        assert_eq!(repo.upsert_dividends(&versions[2]).await?, 1);
        let history = repo.dividend_history("600000.SH", 10, None).await?;
        assert_eq!(history.items[0].revision_count, 2);
        Ok(())
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

        let representable_scientific: Value = serde_json::from_str(
            r#"{
                "fields":["ts_code","end_date","report_type","total_revenue","revenue"],
                "items":[["600000.SH","20241231","1",1.2345e+3,-4.321e-2]]
            }"#,
        )
        .unwrap();
        let rows = TushareClient::parse_financial_reports(
            &representable_scientific,
            &indicators,
            fetched_at,
        )
        .unwrap();
        assert_eq!(rows[0].total_revenue, Some(decimal("1234.5")));
        assert_eq!(rows[0].revenue, Some(decimal("-0.04321")));
    }

    #[test]
    fn company_http_response_errors_include_endpoint_and_status_context() {
        let rate_limit = TushareClient::decode_response_body(
            "income",
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "rate limit exceeded",
        )
        .unwrap_err();
        assert!(rate_limit.to_string().contains("income"));
        assert!(rate_limit.to_string().contains("429"));
        assert!(rate_limit.to_string().contains("rate limit exceeded"));

        let non_json = TushareClient::decode_response_body(
            "dividend",
            reqwest::StatusCode::BAD_GATEWAY,
            "<html>upstream unavailable</html>",
        )
        .unwrap_err();
        assert!(non_json.to_string().contains("dividend"));
        assert!(non_json.to_string().contains("502"));
        assert!(non_json.to_string().contains("non-JSON"));

        let json_rate_limit = TushareClient::decode_response_body(
            "fina_indicator",
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            r#"{"code":-2002,"msg":"每分钟最多访问60次","data":null}"#,
        )
        .unwrap_err();
        let json_rate_limit = json_rate_limit.to_string();
        assert!(json_rate_limit.contains("fina_indicator"));
        assert!(json_rate_limit.contains("429"));
        assert!(json_rate_limit.contains("-2002"));
        assert!(json_rate_limit.contains("每分钟最多访问60次"));
        assert!(json_rate_limit.contains("\"code\":-2002"));

        let unsuccessful_zero = TushareClient::decode_response_body(
            "dividend",
            reqwest::StatusCode::BAD_GATEWAY,
            r#"{"code":0,"msg":"upstream rejected request","data":{"fields":[],"items":[]}}"#,
        )
        .unwrap_err();
        let unsuccessful_zero = unsuccessful_zero.to_string();
        assert!(unsuccessful_zero.contains("dividend"));
        assert!(unsuccessful_zero.contains("502"));
        assert!(unsuccessful_zero.contains("code 0"));
        assert!(unsuccessful_zero.contains("upstream rejected request"));
        assert!(unsuccessful_zero.contains("\"items\":[]"));

        let long_body = "x".repeat(300);
        let excerpt = TushareClient::bounded_response_excerpt(&long_body);
        assert_eq!(excerpt.chars().count(), 201);
        assert!(excerpt.ends_with('…'));
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
                "roe", "ann_date", "update_flag", "end_date", "ts_code"
            ],
            "items": [
                ["20.20", "20250330", "0", "20241231", "600000.SH"],
                ["21.21", "20250401", "1", "20241231", "600000.SH"]
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
    fn company_financial_parser_rejects_ambiguous_official_indicator_identity() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let income = serde_json::json!({
            "fields": [
                "ts_code", "end_date", "report_type", "update_flag", "f_ann_date"
            ],
            "items": [
                ["600000.SH", "20241231", "1", "1", "20250401"],
                ["600000.SH", "20241231", "5", "1", "20250401"]
            ]
        });
        let indicators = serde_json::json!({
            "fields": ["ts_code", "end_date", "update_flag", "ann_date", "roe"],
            "items": [["600000.SH", "20241231", "1", "20250401", "21.21"]]
        });

        let error =
            TushareClient::parse_financial_reports(&income, &indicators, fetched_at).unwrap_err();

        assert!(error.to_string().contains("ambiguous"));
        assert!(error.to_string().contains("600000.SH"));
        assert!(error.to_string().contains("2024-12-31"));
    }

    #[test]
    fn typed_indicator_never_crosses_an_income_report_type() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let income = serde_json::json!({
            "fields": [
                "ts_code", "end_date", "report_type", "update_flag", "f_ann_date",
                "total_revenue"
            ],
            "items": [["600000.SH", "20241231", "1", "1", "20250401", "100.01"]]
        });
        let indicators = serde_json::json!({
            "fields": [
                "ts_code", "end_date", "report_type", "update_flag", "ann_date", "roe"
            ],
            "items": [["600000.SH", "20241231", "5", "1", "20250401", "21.21"]]
        });

        let rows =
            TushareClient::parse_financial_reports(&income, &indicators, fetched_at).unwrap();

        assert_eq!(rows.len(), 2);
        let income_only = rows.iter().find(|row| row.report_type == "1").unwrap();
        assert_eq!(income_only.total_revenue, Some(decimal("100.01")));
        assert_eq!(income_only.roe, None);
        assert!(income_only.raw_payload["indicator"].is_null());
        let indicator_only = rows.iter().find(|row| row.report_type == "5").unwrap();
        assert_eq!(indicator_only.total_revenue, None);
        assert_eq!(indicator_only.roe, Some(decimal("21.21")));
        assert!(indicator_only.raw_payload["income"].is_null());
    }

    #[test]
    fn typed_indicator_only_row_preserves_its_source_identity() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let income = serde_json::json!({ "fields": [], "items": [] });
        let indicators = serde_json::json!({
            "fields": [
                "ts_code", "end_date", "report_type", "update_flag", "ann_date", "roe"
            ],
            "items": [["000001.SZ", "20240630", "1", "0", "20240801", "10.10"]]
        });

        let rows =
            TushareClient::parse_financial_reports(&income, &indicators, fetched_at).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].code, "000001.SZ");
        assert_eq!(rows[0].report_type, "1");
        assert_eq!(rows[0].frequency, FinancialFrequency::Quarterly);
        assert_eq!(rows[0].total_revenue, None);
        assert_eq!(rows[0].roe, Some(decimal("10.10")));
        assert!(rows[0].raw_payload["income"].is_null());
        assert_eq!(rows[0].raw_payload["indicator"]["report_type"], "1");
        assert_eq!(
            rows[0].available_at,
            Utc.with_ymd_and_hms(2024, 7, 31, 16, 0, 0).unwrap()
        );
        assert_eq!(
            rows[0].source_revision,
            TushareClient::content_revision(&rows[0].raw_payload)
        );
    }

    #[test]
    fn mixed_typed_and_untyped_indicators_join_only_unique_revision_identities() {
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let income = serde_json::json!({
            "fields": [
                "ts_code", "end_date", "report_type", "update_flag", "f_ann_date",
                "n_income_attr_p"
            ],
            "items": [
                ["600000.SH", "20241231", "1", "1", "20250401", "101.01"],
                ["600000.SH", "20241231", "5", "1", "20250401", "105.05"],
                ["600000.SH", "20241231", "1", "0", "20250330", "99.99"]
            ]
        });
        let indicators = serde_json::json!({
            "fields": [
                "ts_code", "end_date", "report_type", "update_flag", "ann_date", "roe"
            ],
            "items": [
                ["600000.SH", "20241231", "5", "1", "20250401", "25.25"],
                ["600000.SH", "20241231", null, "0", "20250330", "20.20"]
            ]
        });

        let rows =
            TushareClient::parse_financial_reports(&income, &indicators, fetched_at).unwrap();

        assert_eq!(rows.len(), 3);
        let typed = rows
            .iter()
            .find(|row| row.net_profit_parent == Some(decimal("105.05")))
            .unwrap();
        assert_eq!(typed.report_type, "5");
        assert_eq!(typed.roe, Some(decimal("25.25")));
        let untyped = rows
            .iter()
            .find(|row| row.net_profit_parent == Some(decimal("99.99")))
            .unwrap();
        assert_eq!(untyped.report_type, "1");
        assert_eq!(untyped.roe, Some(decimal("20.20")));
        let unmatched_income = rows
            .iter()
            .find(|row| row.net_profit_parent == Some(decimal("101.01")))
            .unwrap();
        assert_eq!(unmatched_income.roe, None);
    }

    #[tokio::test]
    async fn financial_provider_uses_period_income_requests_and_returns_later_announced_fy() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let fetched_at = Utc.with_ymd_and_hms(2025, 4, 2, 8, 0, 0).unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();

        let reports = TushareClient::financial_reports_with(
            "600519.SH",
            start,
            end,
            fetched_at,
            move |api_name, params, fields| {
                let recorded = recorded.clone();
                async move {
                    recorded.lock().unwrap().push((
                        api_name.to_string(),
                        params.clone(),
                        fields.to_string(),
                    ));
                    match api_name {
                        "income" if params["period"] == "20241231" => Ok(serde_json::json!({
                            "fields": [
                                "ts_code", "end_date", "report_type", "update_flag",
                                "f_ann_date", "total_revenue"
                            ],
                            "items": [[
                                "600519.SH", "20241231", "1", "1", "20250330", "100.01"
                            ]]
                        })),
                        "income" => Ok(serde_json::json!({ "fields": [], "items": [] })),
                        "fina_indicator" => Ok(serde_json::json!({
                            "fields": [
                                "ts_code", "end_date", "update_flag", "ann_date", "roe"
                            ],
                            "items": [["600519.SH", "20241231", "1", "20250330", "20.02"]]
                        })),
                        _ => unreachable!(),
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].end_date, end);
        assert_eq!(
            reports[0].announcement_date.unwrap().to_string(),
            "2025-03-30"
        );
        assert_eq!(reports[0].total_revenue, Some(decimal("100.01")));
        assert_eq!(reports[0].roe, Some(decimal("20.02")));

        let calls = calls.lock().unwrap();
        let income_params: Vec<_> = calls
            .iter()
            .filter(|(api_name, _, _)| api_name == "income")
            .map(|(_, params, _)| params.clone())
            .collect();
        assert_eq!(
            income_params,
            vec![
                serde_json::json!({"ts_code": "600519.SH", "period": "20240331"}),
                serde_json::json!({"ts_code": "600519.SH", "period": "20240630"}),
                serde_json::json!({"ts_code": "600519.SH", "period": "20240930"}),
                serde_json::json!({"ts_code": "600519.SH", "period": "20241231"}),
            ]
        );
        assert!(income_params
            .iter()
            .all(|params| params.get("start_date").is_none() && params.get("end_date").is_none()));
        let indicator_params = calls
            .iter()
            .find(|(api_name, _, _)| api_name == "fina_indicator")
            .map(|(_, params, _)| params)
            .unwrap();
        assert_eq!(
            indicator_params,
            &serde_json::json!({
                "ts_code": "600519.SH",
                "start_date": "20240101",
                "end_date": "20241231"
            })
        );
    }

    #[tokio::test]
    async fn dividend_provider_requests_documented_stock_history_and_filters_locally() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let captured = Arc::new(Mutex::new(None));
        let recorded = captured.clone();

        let records = TushareClient::dividends_with(
            "600519.SH",
            start,
            end,
            fetched_at,
            move |api_name, params, fields| {
                *recorded.lock().unwrap() =
                    Some((api_name.to_string(), params, fields.to_string()));
                async { Ok(dividend_fixture()) }
            },
        )
        .await
        .unwrap();

        assert_eq!(
            captured.lock().unwrap().as_ref().unwrap().1,
            serde_json::json!({"ts_code": "600519.SH"})
        );
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| record.code == "600519.SH"));
        assert!(records.iter().all(|record| {
            let effective_date = TushareClient::beijing_date(record.available_at);
            effective_date >= start && effective_date <= end
        }));
    }

    #[tokio::test]
    async fn dividend_provider_rejects_explicitly_truncated_history() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        let fetched_at = Utc.with_ymd_and_hms(2026, 7, 19, 12, 30, 0).unwrap();
        let error =
            TushareClient::dividends_with("600519.SH", start, end, fetched_at, |_, _, _| async {
                Ok(serde_json::json!({
                    "fields": [],
                    "items": [],
                    "has_more": true
                }))
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("truncated"));
        assert!(error.to_string().contains("dividend"));
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
        let oversized_window = TushareClient::company_report_periods(
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
        )
        .unwrap_err();
        assert!(oversized_window.to_string().contains("exceeds one year"));
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
