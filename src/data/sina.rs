use chrono::NaiveDateTime;
use reqwest::Client;
use std::collections::HashMap;

use crate::data::types::Quote;
use crate::error::{AppError, Result};
use crate::market_time::beijing_now;

const SINA_URL: &str = "http://hq.sinajs.cn/list=";

pub struct SinaClient {
    client: Client,
}

impl SinaClient {
    pub fn new() -> Self {
        SinaClient {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Convert Tushare code (000001.SZ) to Sina code (sz000001)
    pub fn sina_code(tushare_code: &str) -> String {
        if let Some((num, market)) = tushare_code.split_once('.') {
            match market {
                "SH" => format!("sh{}", num),
                "SZ" => format!("sz{}", num),
                _ => tushare_code.to_lowercase(),
            }
        } else {
            tushare_code.to_string()
        }
    }

    /// Fetch real-time quotes for a batch of Tushare codes
    pub async fn get_quotes(&self, codes: &[&str]) -> Result<HashMap<String, Quote>> {
        if codes.is_empty() {
            return Ok(HashMap::new());
        }

        let sina_codes: Vec<String> = codes.iter().map(|c| Self::sina_code(c)).collect();
        let query = sina_codes.join(",");

        let resp = self
            .client
            .get(format!("{}{}", SINA_URL, query))
            .header("Referer", "http://finance.sina.com.cn")
            .send()
            .await
            .map_err(AppError::Http)?;

        let text = resp.text().await.map_err(AppError::Http)?;
        let mut result = HashMap::new();

        for (i, line) in text.lines().enumerate() {
            if let Some(quote) = Self::parse_line(line, codes.get(i).copied().unwrap_or("")) {
                result.insert(quote.code.clone(), quote);
            }
        }

        Ok(result)
    }

    /// Parse a Sina quote line:
    /// var hq_str_sz000001="平安银行,10.50,10.48,...,2024-01-15,15:00:00";
    fn parse_line(line: &str, tushare_code: &str) -> Option<Quote> {
        let start = line.find('"')? + 1;
        let end = line.rfind('"')?;
        let data = &line[start..end];
        let parts: Vec<&str> = data.split(',').collect();

        if parts.len() < 32 {
            return None;
        }

        let price: f64 = parts[3].parse().unwrap_or(0.0);
        let prev_close: f64 = parts[2].parse().unwrap_or(0.0);
        let change_pct = if prev_close > 0.0 {
            (price - prev_close) / prev_close * 100.0
        } else {
            0.0
        };

        let date_str = format!("{} {}", parts[30], parts[31]);
        let timestamp = NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|_| beijing_now().naive_local());

        Some(Quote {
            code: tushare_code.to_string(),
            name: parts[0].to_string(),
            price,
            open: parts[1].parse().unwrap_or(0.0),
            high: parts[4].parse().unwrap_or(0.0),
            low: parts[5].parse().unwrap_or(0.0),
            prev_close,
            change_pct,
            volume: parts[8].parse::<f64>().unwrap_or(0.0) as i64,
            amount: parts[9].parse().unwrap_or(0.0),
            timestamp,
        })
    }
}

impl Default for SinaClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sina_code_convert() {
        assert_eq!(SinaClient::sina_code("000001.SZ"), "sz000001");
        assert_eq!(SinaClient::sina_code("600519.SH"), "sh600519");
    }
}
