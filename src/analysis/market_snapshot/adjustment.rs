use crate::analysis::market_snapshot::AdjustmentFactor;
use crate::data::types::Candle;
use crate::error::{AppError, Result};

pub fn adjust_candles(bars: &[Candle], factors: &[AdjustmentFactor]) -> Result<Vec<Candle>> {
    let latest = factors
        .iter()
        .max_by_key(|row| row.trade_date)
        .ok_or_else(|| AppError::Internal("missing adjustment factors".into()))?
        .adj_factor;

    let by_date: std::collections::HashMap<_, _> = factors
        .iter()
        .map(|row| (row.trade_date, row.adj_factor))
        .collect();

    bars.iter()
        .map(|bar| {
            let factor = by_date.get(&bar.trade_date).ok_or_else(|| {
                AppError::Internal(format!("missing adjustment factor for {}", bar.trade_date))
            })?;
            let ratio = factor / latest;
            Ok(Candle {
                open: bar.open * ratio,
                high: bar.high * ratio,
                low: bar.low * ratio,
                close: bar.close * ratio,
                ..bar.clone()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::market_snapshot::AvailabilityQuality;
    use chrono::{NaiveDate, TimeZone, Utc};

    fn candle(trade_date: &str, open: f64, high: f64, low: f64, close: f64, volume: i64) -> Candle {
        Candle {
            trade_date: NaiveDate::parse_from_str(trade_date, "%Y-%m-%d").unwrap(),
            open,
            high,
            low,
            close,
            volume,
            amount: 12_345.0,
            turnover: Some(1.23),
            pe: Some(10.0),
            pb: Some(2.0),
        }
    }

    fn factor(trade_date: &str, adj_factor: f64) -> AdjustmentFactor {
        let timestamp = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();

        AdjustmentFactor {
            code: "000001.SZ".to_string(),
            trade_date: NaiveDate::parse_from_str(trade_date, "%Y-%m-%d").unwrap(),
            adj_factor,
            available_at: timestamp,
            ingested_at: timestamp,
            availability_quality: AvailabilityQuality::Observed,
            source: "test".to_string(),
        }
    }

    #[test]
    fn adjusts_ohlc_to_latest_factor_preserving_non_price_fields() {
        let bars = vec![
            candle("2026-07-08", 10.0, 11.0, 9.0, 10.5, 1000),
            candle("2026-07-09", 11.0, 12.0, 10.0, 11.5, 1200),
        ];
        let factors = vec![factor("2026-07-08", 1.0), factor("2026-07-09", 2.0)];

        let adjusted = adjust_candles(&bars, &factors).unwrap();

        assert_eq!(adjusted[0].open, 5.0);
        assert_eq!(adjusted[0].high, 5.5);
        assert_eq!(adjusted[0].low, 4.5);
        assert_eq!(adjusted[0].close, 5.25);
        assert_eq!(adjusted[1].close, 11.5);
        assert_eq!(adjusted[0].trade_date, bars[0].trade_date);
        assert_eq!(adjusted[1].trade_date, bars[1].trade_date);
        assert_eq!(adjusted[0].volume, bars[0].volume);
        assert_eq!(adjusted[1].volume, bars[1].volume);
        assert_eq!(adjusted[0].amount, bars[0].amount);
        assert_eq!(adjusted[1].amount, bars[1].amount);
        assert_eq!(adjusted[0].turnover, bars[0].turnover);
        assert_eq!(adjusted[1].turnover, bars[1].turnover);
        assert_eq!(adjusted[0].pe, bars[0].pe);
        assert_eq!(adjusted[1].pe, bars[1].pe);
        assert_eq!(adjusted[0].pb, bars[0].pb);
        assert_eq!(adjusted[1].pb, bars[1].pb);
    }

    #[test]
    fn rejects_missing_factor_for_a_bar() {
        let error =
            adjust_candles(&[candle("2026-07-08", 10.0, 11.0, 9.0, 10.5, 1000)], &[]).unwrap_err();

        assert!(error.to_string().contains("missing adjustment factor"));
    }

    #[test]
    fn rejects_missing_factor_for_a_bar_when_factors_are_not_empty() {
        let error = adjust_candles(
            &[candle("2026-07-08", 10.0, 11.0, 9.0, 10.5, 1000)],
            &[factor("2026-07-09", 2.0)],
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("missing adjustment factor for 2026-07-08"));
    }
}
