use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use tokio::sync::RwLock;
use tracing::warn;

use crate::analysis::market_snapshot::{
    AdjustmentFactor, AvailabilityQuality, CorporateAction, DailyBasicSnapshot, IndexDailyBar,
    SectorMembership, SecurityDailyStatus, SecurityMasterVersion,
};
use crate::data::point_in_time_provider::{PointInTimeCapabilities, PointInTimeDataProvider};
use crate::data::provider::DataProvider;
use crate::data::types::*;
use crate::error::{AppError, Result};

const TUSHARE_URL: &str = "https://api.tushare.pro";

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

        if json["code"].as_i64().unwrap_or(-1) != 0 {
            let msg = json["msg"].as_str().unwrap_or("unknown error");
            return Err(AppError::DataProvider(format!(
                "Tushare {}: {}",
                api_name, msg
            )));
        }

        Ok(json["data"].clone())
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
            Value::Number(n) => n.as_i64().unwrap_or(0),
            Value::String(s) => s.parse().unwrap_or(0),
            _ => 0,
        }
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

        let close_by_code = daily["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some((
                    arr.first()?.as_str()?.to_string(),
                    Self::optional_f64(arr.get(2))?,
                ))
            })
            .collect::<HashMap<_, _>>();

        let limit_by_code = limits["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some((
                    arr.first()?.as_str()?.to_string(),
                    (
                        Self::optional_f64(arr.get(2)),
                        Self::optional_f64(arr.get(3)),
                    ),
                ))
            })
            .collect::<HashMap<_, _>>();

        let suspended = suspensions["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| row.as_array()?.first()?.as_str().map(|v| v.to_string()))
            .collect::<HashSet<_>>();

        let listed_by_code = masters["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let arr = row.as_array()?;
                Some((
                    arr.first()?.as_str()?.to_string(),
                    Self::optional_date(arr.get(1))?,
                ))
            })
            .collect::<HashMap<_, _>>();

        let mut st_codes = HashSet::new();
        for row in namechanges["items"].as_array().cloned().unwrap_or_default() {
            let Some(arr) = row.as_array() else { continue };
            let Some(code) = arr.first().and_then(|v| v.as_str()) else {
                continue;
            };
            let name = arr.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let start = Self::optional_date(arr.get(2)).unwrap_or(NaiveDate::MIN);
            let end = Self::optional_date(arr.get(3)).unwrap_or(NaiveDate::MAX);
            let reason = arr.get(4).and_then(|v| v.as_str()).unwrap_or("");
            if start <= trade_date
                && trade_date <= end
                && (name.to_ascii_uppercase().contains("ST")
                    || reason.to_ascii_uppercase().contains("ST"))
            {
                st_codes.insert(code.to_string());
            }
        }

        let mut rows = Vec::new();
        for (code, close) in close_by_code {
            let price_limit_pct = limit_by_code.get(&code).and_then(|(up, _down)| {
                let up = (*up)?;
                if close == 0.0 {
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
            rows.push(SecurityDailyStatus {
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
            });
        }

        Ok(rows)
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
                        volume: Self::safe_i64(arr.get(i_vol)?) * 100, // lots -> shares
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
                    volume: Self::safe_i64(arr.get(i_vol)?) * 100,
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
    use chrono::{TimeZone, Utc};

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
