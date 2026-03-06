use anyhow::Context;
use async_trait::async_trait;
use chrono::NaiveDate;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::warn;

use crate::data::provider::DataProvider;
use crate::data::types::*;
use crate::error::{AppError, Result};

const TUSHARE_URL: &str = "https://api.tushare.pro";

pub struct TushareClient {
    token: String,
    client: Client,
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
        }
    }

    /// Convert Tushare code (000001.SZ) to Sina code (sz000001)
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
}
