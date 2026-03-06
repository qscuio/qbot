use async_trait::async_trait;
use chrono::NaiveDate;
use reqwest::Client;
use serde_json::Value;
use std::cmp::min;
use tokio::task::JoinSet;

use crate::data::provider::DataProvider;
use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
use crate::error::{AppError, Result};

const EASTMONEY_KLINE_URL: &str = "https://push2his.eastmoney.com/api/qt/stock/kline/get";
const EASTMONEY_CLIST_URL: &str = "https://82.push2.eastmoney.com/api/qt/clist/get";
const EASTMONEY_ZTPOOL_URL: &str = "https://push2ex.eastmoney.com/getTopicZTPool";
const HIST_FALLBACK_CONCURRENCY: usize = 32;

pub struct EastmoneyProvider {
    client: Client,
}

impl EastmoneyProvider {
    pub fn new(proxy: Option<&str>) -> Self {
        let mut builder = Client::builder().timeout(std::time::Duration::from_secs(20));
        if let Some(proxy_url) = proxy {
            if let Ok(proxy) = reqwest::Proxy::all(proxy_url) {
                builder = builder.proxy(proxy);
            }
        }
        Self {
            client: builder.build().unwrap_or_default(),
        }
    }

    fn to_tushare_code(code: &str) -> String {
        if code.contains('.') {
            return code.to_uppercase();
        }
        let code = code.trim().to_uppercase();
        if code.starts_with('6') || code.starts_with('5') || code.starts_with('9') {
            format!("{}.SH", code)
        } else {
            format!("{}.SZ", code)
        }
    }

    fn to_secid(code: &str) -> Option<String> {
        let code = Self::to_tushare_code(code);
        let (num, market) = code.split_once('.')?;
        match market {
            "SH" => Some(format!("1.{}", num)),
            "SZ" => Some(format!("0.{}", num)),
            "BJ" => Some(format!("0.{}", num)),
            _ => None,
        }
    }

    fn parse_f64(s: &str) -> f64 {
        s.parse::<f64>().unwrap_or(0.0)
    }

    fn parse_i64(s: &str) -> i64 {
        s.parse::<i64>().unwrap_or(0)
    }

    fn value_to_string(v: &Value) -> String {
        match v {
            Value::String(s) => s.to_string(),
            Value::Number(n) => n.to_string(),
            _ => String::new(),
        }
    }

    fn parse_value_f64(v: Option<&Value>) -> f64 {
        v.map(Self::value_to_string)
            .map(|s| Self::parse_f64(&s))
            .unwrap_or(0.0)
    }

    fn parse_value_i64(v: Option<&Value>) -> i64 {
        v.map(Self::value_to_string)
            .map(|s| Self::parse_i64(&s))
            .unwrap_or(0)
    }

    fn parse_tushare_from_market(code: &str, market_id: i64) -> String {
        let suffix = if market_id == 1 { "SH" } else { "SZ" };
        format!("{}.{}", code, suffix)
    }

    fn parse_time_hms(raw: i64) -> Option<String> {
        if raw <= 0 {
            return None;
        }
        let s = format!("{:06}", raw);
        if s.len() != 6 {
            return None;
        }
        Some(format!("{}:{}:{}", &s[0..2], &s[2..4], &s[4..6]))
    }

    async fn fetch_kline_rows(
        &self,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Vec<String>>> {
        Self::fetch_kline_rows_with_client(&self.client, code, start_date, end_date).await
    }

    async fn fetch_kline_rows_with_client(
        client: &Client,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Vec<String>>> {
        let secid = Self::to_secid(code).ok_or_else(|| {
            AppError::DataProvider(format!("eastmoney unsupported code: {}", code))
        })?;

        let resp = client
            .get(EASTMONEY_KLINE_URL)
            .query(&[
                ("secid", secid.as_str()),
                ("klt", "101"),
                ("fqt", "1"),
                ("beg", &start_date.format("%Y%m%d").to_string()),
                ("end", &end_date.format("%Y%m%d").to_string()),
                ("fields1", "f1,f2,f3,f4,f5,f6"),
                ("fields2", "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"),
            ])
            .send()
            .await
            .map_err(AppError::Http)?;

        let json: Value = resp.json().await.map_err(AppError::Http)?;
        let klines = json
            .get("data")
            .and_then(|d| d.get("klines"))
            .and_then(|k| k.as_array())
            .cloned()
            .unwrap_or_default();

        let mut rows = Vec::new();
        for item in klines {
            if let Some(line) = item.as_str() {
                let parts: Vec<String> = line.split(',').map(|s| s.to_string()).collect();
                if parts.len() >= 7 {
                    rows.push(parts);
                }
            }
        }
        Ok(rows)
    }

    async fn fetch_clist_page(
        &self,
        fs: &str,
        fields: &str,
        page: i64,
        page_size: i64,
    ) -> Result<(i64, Vec<Value>)> {
        let resp = self
            .client
            .get(EASTMONEY_CLIST_URL)
            .query(&[
                ("pn", page.to_string()),
                ("pz", page_size.to_string()),
                ("po", "1".to_string()),
                ("np", "1".to_string()),
                ("fltt", "2".to_string()),
                ("invt", "2".to_string()),
                ("fid", "f3".to_string()),
                ("fs", fs.to_string()),
                ("fields", fields.to_string()),
            ])
            .send()
            .await
            .map_err(AppError::Http)?;

        let json: Value = resp.json().await.map_err(AppError::Http)?;
        let total = json
            .get("data")
            .and_then(|d| d.get("total"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let diff = json
            .get("data")
            .and_then(|d| d.get("diff"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok((total, diff))
    }

    async fn fetch_sector_data_by_fs(
        &self,
        fs: &str,
        sector_type: &str,
        trade_date: NaiveDate,
    ) -> Result<Vec<SectorData>> {
        let (_total, rows) = self.fetch_clist_page(fs, "f12,f14,f3,f6", 1, 2000).await?;

        Ok(rows
            .into_iter()
            .filter_map(|item| {
                let code = item.get("f12")?.as_str()?.to_string();
                let name = item
                    .get("f14")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let change_pct = item
                    .get("f3")
                    .map(|v| v.to_string())
                    .map(|s| s.replace('"', ""))
                    .map(|s| Self::parse_f64(&s))
                    .unwrap_or(0.0);
                let amount = item
                    .get("f6")
                    .map(|v| v.to_string())
                    .map(|s| s.replace('"', ""))
                    .map(|s| Self::parse_f64(&s))
                    .unwrap_or(0.0);

                Some(SectorData {
                    code,
                    name,
                    sector_type: sector_type.to_string(),
                    change_pct,
                    amount,
                    trade_date,
                })
            })
            .collect())
    }

    fn row_to_candle(row: &[String]) -> Option<Candle> {
        if row.len() < 7 {
            return None;
        }
        let trade_date = NaiveDate::parse_from_str(&row[0], "%Y-%m-%d").ok()?;
        Some(Candle {
            trade_date,
            open: Self::parse_f64(&row[1]),
            close: Self::parse_f64(&row[2]),
            high: Self::parse_f64(&row[3]),
            low: Self::parse_f64(&row[4]),
            volume: Self::parse_i64(&row[5]),
            amount: Self::parse_f64(&row[6]),
            turnover: row.get(10).map(|s| Self::parse_f64(s)),
            pe: None,
            pb: None,
        })
    }
}

#[async_trait]
impl DataProvider for EastmoneyProvider {
    fn name(&self) -> &'static str {
        "eastmoney"
    }

    async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
        let fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23";
        let fields = "f12,f14,f13";
        let page_size = 5000i64;
        let mut page = 1i64;
        let mut total = i64::MAX;
        let mut stocks = Vec::new();

        while (page - 1) * page_size < total {
            let (t, rows) = self.fetch_clist_page(fs, fields, page, page_size).await?;
            total = min(total, t.max(0));
            if rows.is_empty() {
                break;
            }

            for item in rows {
                let code = match item.get("f12").and_then(|v| v.as_str()) {
                    Some(v) => v,
                    None => continue,
                };
                let market_id = item.get("f13").and_then(|v| v.as_i64()).unwrap_or(0);
                let ts_code = Self::parse_tushare_from_market(code, market_id);
                let name = item
                    .get("f14")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                stocks.push(StockInfo {
                    code: ts_code,
                    name,
                    market: if market_id == 1 { "SH" } else { "SZ" }.to_string(),
                    industry: None,
                });
            }
            page += 1;
        }

        Ok(stocks)
    }

    async fn get_daily_bars_by_date(&self, trade_date: NaiveDate) -> Result<Vec<(String, Candle)>> {
        if trade_date == crate::market_time::beijing_today() {
            let fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23";
            let fields = "f12,f13,f17,f15,f16,f2,f5,f6,f8";
            let page_size = 5000i64;
            let mut page = 1i64;
            let mut total = i64::MAX;
            let mut out: Vec<(String, Candle)> = Vec::new();

            while (page - 1) * page_size < total {
                let (t, rows) = self.fetch_clist_page(fs, fields, page, page_size).await?;
                total = min(total, t.max(0));
                if rows.is_empty() {
                    break;
                }

                for item in rows {
                    let raw_code = match item.get("f12").and_then(|v| v.as_str()) {
                        Some(v) => v,
                        None => continue,
                    };
                    let market_id = item.get("f13").and_then(|v| v.as_i64()).unwrap_or(0);
                    let code = Self::parse_tushare_from_market(raw_code, market_id);

                    let close = Self::parse_value_f64(item.get("f2"));
                    if close <= 0.0 {
                        continue;
                    }

                    let open = Self::parse_value_f64(item.get("f17"));
                    let mut high = Self::parse_value_f64(item.get("f15"));
                    let mut low = Self::parse_value_f64(item.get("f16"));
                    if high <= 0.0 {
                        high = close.max(open);
                    }
                    if low <= 0.0 {
                        low = close.min(open.max(0.0));
                    }

                    let volume_lots = Self::parse_value_i64(item.get("f5"));
                    let amount = Self::parse_value_f64(item.get("f6"));
                    let turnover = Some(Self::parse_value_f64(item.get("f8")));

                    out.push((
                        code,
                        Candle {
                            trade_date,
                            open,
                            high,
                            low,
                            close,
                            volume: volume_lots * 100, // lots -> shares
                            amount,
                            turnover,
                            pe: None,
                            pb: None,
                        },
                    ));
                }
                page += 1;
            }

            return Ok(out);
        }

        // Historical fallback: resolve bars by querying each stock's kline for target date.
        // This path is slower but allows backfill when Tushare is unavailable.
        let stocks = self.get_stock_list().await?;
        let mut out: Vec<(String, Candle)> = Vec::new();
        let client = self.client.clone();
        let mut set: JoinSet<Option<(String, Candle)>> = JoinSet::new();
        let mut iter = stocks.into_iter();

        let spawn_one =
            |set: &mut JoinSet<Option<(String, Candle)>>, stock: StockInfo, client: Client| {
                set.spawn(async move {
                    let code = stock.code;
                    let rows =
                        Self::fetch_kline_rows_with_client(&client, &code, trade_date, trade_date)
                            .await
                            .ok()?;
                    let candle = rows.first().and_then(|r| Self::row_to_candle(r))?;
                    if candle.trade_date == trade_date {
                        Some((code, candle))
                    } else {
                        None
                    }
                });
            };

        for _ in 0..HIST_FALLBACK_CONCURRENCY {
            if let Some(stock) = iter.next() {
                spawn_one(&mut set, stock, client.clone());
            } else {
                break;
            }
        }

        while let Some(joined) = set.join_next().await {
            if let Ok(Some(hit)) = joined {
                out.push(hit);
            }
            if let Some(stock) = iter.next() {
                spawn_one(&mut set, stock, client.clone());
            }
        }

        if out.is_empty() {
            return Err(AppError::DataProvider(format!(
                "eastmoney returned no bars for {}",
                trade_date
            )));
        }

        Ok(out)
    }

    async fn get_daily_bars_for_stock(
        &self,
        code: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Candle>> {
        let rows = self.fetch_kline_rows(code, start_date, end_date).await?;
        let mut bars: Vec<Candle> = rows.iter().filter_map(|r| Self::row_to_candle(r)).collect();
        bars.sort_by_key(|b| b.trade_date);
        Ok(bars)
    }

    async fn get_trading_dates(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
        let rows = self.fetch_kline_rows("000001.SH", start, end).await?;
        let mut dates = Vec::new();
        for row in rows {
            if let Ok(d) = NaiveDate::parse_from_str(&row[0], "%Y-%m-%d") {
                dates.push(d);
            }
        }
        dates.sort();
        dates.dedup();
        Ok(dates)
    }

    async fn get_limit_up_stocks(&self, trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        let date = trade_date.format("%Y%m%d").to_string();
        let resp = self
            .client
            .get(EASTMONEY_ZTPOOL_URL)
            .query(&[
                ("ut", "7eea3edcaed734bea9cbfc24409ed989"),
                ("dpt", "wz.ztzt"),
                ("Pageindex", "0"),
                ("pagesize", "10000"),
                ("sort", "fbt:asc"),
                ("date", date.as_str()),
            ])
            .send()
            .await
            .map_err(AppError::Http)?;

        let json: Value = resp.json().await.map_err(AppError::Http)?;
        let pool = json
            .get("data")
            .and_then(|d| d.get("pool"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let stocks = pool
            .into_iter()
            .filter_map(|item| {
                let raw_code = item.get("c")?.as_str()?;
                let name = item.get("n").and_then(|v| v.as_str()).unwrap_or("");
                let p = item.get("p").and_then(|v| v.as_i64()).unwrap_or(0) as f64;
                let close = if p > 0.0 { p / 1000.0 } else { 0.0 };
                let pct_chg = item
                    .get("zdp")
                    .map(|v| v.to_string())
                    .map(|s| s.replace('"', ""))
                    .map(|s| Self::parse_f64(&s))
                    .unwrap_or(0.0);
                let fd_amount = item
                    .get("fund")
                    .or_else(|| item.get("amount"))
                    .map(|v| v.to_string())
                    .map(|s| s.replace('"', ""))
                    .map(|s| Self::parse_f64(&s))
                    .unwrap_or(0.0);
                let first_time = item
                    .get("fbt")
                    .and_then(|v| v.as_i64())
                    .and_then(Self::parse_time_hms);
                let last_time = item
                    .get("lbt")
                    .and_then(|v| v.as_i64())
                    .and_then(Self::parse_time_hms);
                let open_times = item.get("zbc").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                let strth = item
                    .get("hs")
                    .map(|v| v.to_string())
                    .map(|s| s.replace('"', ""))
                    .map(|s| Self::parse_f64(&s))
                    .unwrap_or(0.0);

                Some(LimitUpStock {
                    code: Self::to_tushare_code(raw_code),
                    name: name.to_string(),
                    trade_date,
                    close,
                    pct_chg,
                    fd_amount,
                    first_time,
                    last_time,
                    open_times,
                    strth,
                    limit: "U".to_string(),
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
        let rows = self.fetch_kline_rows(code, trade_date, trade_date).await?;
        let row = match rows.first() {
            Some(r) => r,
            None => return Ok(None),
        };

        let close = row.get(2).map(|v| Self::parse_f64(v)).unwrap_or(0.0);
        let volume = row.get(5).map(|v| Self::parse_i64(v)).unwrap_or(0);
        let amount = row.get(6).map(|v| Self::parse_f64(v)).unwrap_or(0.0);
        let change_pct = row.get(8).map(|v| Self::parse_f64(v)).unwrap_or(0.0);

        Ok(Some(IndexData {
            code: Self::to_tushare_code(code),
            name: Self::to_tushare_code(code),
            trade_date,
            close,
            change_pct,
            volume,
            amount,
        }))
    }

    async fn get_sector_data(&self, trade_date: NaiveDate) -> Result<Vec<SectorData>> {
        let mut sectors = Vec::new();
        // Eastmoney board list: industry
        sectors.extend(
            self.fetch_sector_data_by_fs("m:90+t:2", "industry", trade_date)
                .await?,
        );
        // Eastmoney board list: concept
        sectors.extend(
            self.fetch_sector_data_by_fs("m:90+t:3", "concept", trade_date)
                .await?,
        );
        Ok(sectors)
    }
}
