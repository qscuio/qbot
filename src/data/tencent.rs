use async_trait::async_trait;
use chrono::NaiveDate;
use reqwest::Client;
use serde_json::Value;
use tokio::task::JoinSet;

use crate::data::eastmoney::EastmoneyProvider;
use crate::data::provider::DataProvider;
use crate::data::types::{Candle, IndexData, LimitUpStock, SectorData, StockInfo};
use crate::error::{AppError, Result};
use crate::market_time::beijing_today;

const TENCENT_KLINE_URL: &str = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get";
const HIST_FALLBACK_CONCURRENCY: usize = 32;

pub struct TencentProvider {
    client: Client,
    eastmoney: EastmoneyProvider,
}

impl TencentProvider {
    pub fn new(proxy: Option<&str>) -> Self {
        let mut builder = Client::builder().timeout(std::time::Duration::from_secs(20));
        if let Some(proxy_url) = proxy {
            if let Ok(proxy) = reqwest::Proxy::all(proxy_url) {
                builder = builder.proxy(proxy);
            }
        }

        Self {
            client: builder.build().unwrap_or_default(),
            eastmoney: EastmoneyProvider::new(proxy),
        }
    }

    fn to_tushare_code(code: &str) -> String {
        if code.contains('.') {
            return code.to_uppercase();
        }

        let raw = code.trim().to_uppercase();
        if raw.starts_with('6') || raw.starts_with('5') || raw.starts_with('9') {
            format!("{}.SH", raw)
        } else {
            format!("{}.SZ", raw)
        }
    }

    fn to_tencent_code(code: &str) -> String {
        let ts = Self::to_tushare_code(code);
        match ts.split_once('.') {
            Some((num, "SH")) => format!("sh{}", num),
            Some((num, "SZ")) => format!("sz{}", num),
            Some((num, _)) => num.to_lowercase(),
            None => ts.to_lowercase(),
        }
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
        let symbol = Self::to_tencent_code(code);
        let param = format!(
            "{},day,{},{},320,qfq",
            symbol,
            start_date.format("%Y-%m-%d"),
            end_date.format("%Y-%m-%d")
        );

        let resp = client
            .get(TENCENT_KLINE_URL)
            .query(&[("param", param)])
            .send()
            .await
            .map_err(AppError::Http)?;

        let json: Value = resp.json().await.map_err(AppError::Http)?;
        let data = json
            .get("data")
            .and_then(|v| v.get(&symbol))
            .ok_or_else(|| AppError::DataProvider(format!("tencent: missing data for {}", code)))?;

        let rows = data
            .get("qfqday")
            .or_else(|| data.get("day"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let parsed = rows
            .into_iter()
            .filter_map(|row| row.as_array().cloned())
            .map(|arr| {
                arr.into_iter()
                    .map(|v| match v {
                        Value::String(s) => s,
                        Value::Number(n) => n.to_string(),
                        _ => String::new(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Ok(parsed)
    }

    fn parse_f64(raw: Option<&String>) -> f64 {
        raw.and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0)
    }

    fn parse_i64(raw: Option<&String>) -> i64 {
        raw.and_then(|s| s.parse::<i64>().ok()).unwrap_or(0)
    }

    fn row_to_candle(row: &[String]) -> Option<Candle> {
        if row.len() < 6 {
            return None;
        }

        let trade_date = NaiveDate::parse_from_str(row.first()?, "%Y-%m-%d").ok()?;
        let open = Self::parse_f64(row.get(1));
        let close = Self::parse_f64(row.get(2));
        let high = Self::parse_f64(row.get(3));
        let low = Self::parse_f64(row.get(4));

        Some(Candle {
            trade_date,
            open,
            high,
            low,
            close,
            volume: Self::parse_i64(row.get(5)) * 100,
            amount: Self::parse_f64(row.get(6)),
            turnover: None,
            pe: None,
            pb: None,
        })
    }
}

#[async_trait]
impl DataProvider for TencentProvider {
    fn name(&self) -> &'static str {
        "tencent"
    }

    async fn get_stock_list(&self) -> Result<Vec<StockInfo>> {
        self.eastmoney.get_stock_list().await
    }

    async fn get_daily_bars_by_date(&self, trade_date: NaiveDate) -> Result<Vec<(String, Candle)>> {
        if trade_date == beijing_today() {
            return self.eastmoney.get_daily_bars_by_date(trade_date).await;
        }

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
                "tencent returned no bars for {}",
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
        let mut bars = rows
            .iter()
            .filter_map(|r| Self::row_to_candle(r))
            .collect::<Vec<_>>();
        bars.sort_by_key(|b| b.trade_date);
        Ok(bars)
    }

    async fn get_trading_dates(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
        let rows = self.fetch_kline_rows("000001.SH", start, end).await?;
        let mut dates = rows
            .iter()
            .filter_map(|r| r.first())
            .filter_map(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
            .collect::<Vec<_>>();
        dates.sort();
        dates.dedup();
        Ok(dates)
    }

    async fn get_limit_up_stocks(&self, trade_date: NaiveDate) -> Result<Vec<LimitUpStock>> {
        self.eastmoney.get_limit_up_stocks(trade_date).await
    }

    async fn get_index_daily(
        &self,
        code: &str,
        trade_date: NaiveDate,
    ) -> Result<Option<IndexData>> {
        let rows = self.fetch_kline_rows(code, trade_date, trade_date).await?;
        let Some(candle) = rows.first().and_then(|r| Self::row_to_candle(r)) else {
            return Ok(None);
        };

        let change_pct = if candle.open > 0.0 {
            (candle.close - candle.open) / candle.open * 100.0
        } else {
            0.0
        };

        Ok(Some(IndexData {
            code: Self::to_tushare_code(code),
            name: Self::to_tushare_code(code),
            trade_date,
            close: candle.close,
            change_pct,
            volume: candle.volume,
            amount: candle.amount,
        }))
    }

    async fn get_sector_data(&self, trade_date: NaiveDate) -> Result<Vec<SectorData>> {
        self.eastmoney.get_sector_data(trade_date).await
    }
}
