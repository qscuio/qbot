use super::base::{avg_volume, sma, SignalDetector, SignalResult, StockContext};
use crate::data::types::Candle;

fn closes(bars: &[Candle]) -> Vec<f64> {
    bars.iter().map(|b| b.close).collect()
}

/// 均线多头 — MA5 > MA10 > MA20, all rising
pub struct MaBullishSignal;
impl SignalDetector for MaBullishSignal {
    fn signal_id(&self) -> &'static str {
        "ma_bullish"
    }
    fn display_name(&self) -> &'static str {
        "均线多头"
    }
    fn icon(&self) -> &'static str {
        "🐂"
    }
    fn group(&self) -> &'static str {
        "trend"
    }
    fn min_bars(&self) -> usize {
        21
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let c = closes(bars);
        let (ma5, ma10, ma20) = match (sma(&c, 5), sma(&c, 10), sma(&c, 20)) {
            (Some(a), Some(b), Some(c)) => (a, b, c),
            _ => return SignalResult::no(),
        };
        if ma5 > ma10 && ma10 > ma20 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 均线回踩 — price pulled back to MA20 and bouncing
pub struct MaPullbackSignal;
impl SignalDetector for MaPullbackSignal {
    fn signal_id(&self) -> &'static str {
        "ma_pullback"
    }
    fn display_name(&self) -> &'static str {
        "均线回踩"
    }
    fn icon(&self) -> &'static str {
        "🔄"
    }
    fn group(&self) -> &'static str {
        "trend"
    }
    fn min_bars(&self) -> usize {
        25
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 25 {
            return SignalResult::no();
        }
        let c = closes(bars);
        let ma20 = match sma(&c, 20) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        let today = &bars[n - 1];
        let yesterday = &bars[n - 2];
        let touched = today.low <= ma20 * 1.01 || yesterday.low <= ma20 * 1.01;
        let bouncing = today.close > ma20 && today.close > today.open;
        if touched && bouncing {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 强势回调 — above MA60, moderate pullback
pub struct StrongPullbackSignal;
impl SignalDetector for StrongPullbackSignal {
    fn signal_id(&self) -> &'static str {
        "strong_pullback"
    }
    fn display_name(&self) -> &'static str {
        "强势回调"
    }
    fn icon(&self) -> &'static str {
        "💪"
    }
    fn group(&self) -> &'static str {
        "trend"
    }
    fn min_bars(&self) -> usize {
        65
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 65 {
            return SignalResult::no();
        }
        let c = closes(bars);
        let ma20 = match sma(&c, 20) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        let ma60 = match sma(&c, 60) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        let today = &bars[n - 1];
        if today.close > ma60 && today.low <= ma20 * 1.02 && today.close > today.open {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 上升突破 — breakout from rising channel
pub struct UptrendBreakoutSignal;
impl SignalDetector for UptrendBreakoutSignal {
    fn signal_id(&self) -> &'static str {
        "uptrend_breakout"
    }
    fn display_name(&self) -> &'static str {
        "上升突破"
    }
    fn icon(&self) -> &'static str {
        "🚀"
    }
    fn group(&self) -> &'static str {
        "trend"
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 21 {
            return SignalResult::no();
        }
        let recent_high = bars[n - 21..n - 1]
            .iter()
            .map(|b| b.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let today = &bars[n - 1];
        if today.close > recent_high && today.close > today.open {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 下降反转 — breaks descending trendline with volume
pub struct DowntrendReversalSignal;
impl SignalDetector for DowntrendReversalSignal {
    fn signal_id(&self) -> &'static str {
        "downtrend_reversal"
    }
    fn display_name(&self) -> &'static str {
        "下降反转"
    }
    fn icon(&self) -> &'static str {
        "↗️"
    }
    fn group(&self) -> &'static str {
        "trend"
    }
    fn min_bars(&self) -> usize {
        30
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 30 {
            return SignalResult::no();
        }
        let c = closes(bars);
        let ma5 = match sma(&c, 5) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        let ma10 = match sma(&c, 10) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        let today = &bars[n - 1];
        let was_bear = ma5 < ma10;
        let crossing = today.close > ma5 && today.close > today.open;
        let volume_ok = today.volume as f64 > avg_volume(&bars[..n - 1], 10) * 1.2;
        if was_bear && crossing && volume_ok {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 线性回归 — linear regression slope is positive and accelerating
pub struct LinRegSignal;
impl SignalDetector for LinRegSignal {
    fn signal_id(&self) -> &'static str {
        "linreg"
    }
    fn display_name(&self) -> &'static str {
        "线性回归"
    }
    fn icon(&self) -> &'static str {
        "📐"
    }
    fn group(&self) -> &'static str {
        "trend"
    }
    fn min_bars(&self) -> usize {
        21
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 21 {
            return SignalResult::no();
        }
        let window: Vec<f64> = bars[n - 20..].iter().map(|b| b.close).collect();
        let len = window.len() as f64;
        let x_mean = (len - 1.0) / 2.0;
        let y_mean: f64 = window.iter().sum::<f64>() / len;
        let num: f64 = window
            .iter()
            .enumerate()
            .map(|(i, &y)| (i as f64 - x_mean) * (y - y_mean))
            .sum();
        let den: f64 = (0..window.len()).map(|i| (i as f64 - x_mean).powi(2)).sum();
        let slope = if den > 0.0 { num / den } else { 0.0 };
        if slope > 0.0 && slope / y_mean * 100.0 > 0.1 {
            SignalResult::yes().with_meta("slope_pct", serde_json::json!(slope / y_mean * 100.0))
        } else {
            SignalResult::no()
        }
    }
}

/// 做T信号 — 趋势向上且波动充足，适合做T滚动
pub struct TTradeSignal;
impl SignalDetector for TTradeSignal {
    fn signal_id(&self) -> &'static str {
        "t_trade"
    }
    fn display_name(&self) -> &'static str {
        "做T信号"
    }
    fn icon(&self) -> &'static str {
        "🔁"
    }
    fn group(&self) -> &'static str {
        "trend"
    }
    fn min_bars(&self) -> usize {
        60
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 60 {
            return SignalResult::no();
        }

        let today = &bars[n - 1];
        let prev = &bars[n - 2];
        if prev.close <= 0.0 || today.close <= 0.0 || today.high <= today.low {
            return SignalResult::no();
        }

        let c = closes(bars);
        let ma10 = match sma(&c, 10) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        let ma20 = match sma(&c, 20) {
            Some(v) => v,
            None => return SignalResult::no(),
        };

        let gain_pct = (today.close - prev.close) / prev.close * 100.0;
        let amplitude_pct = (today.high - today.low) / prev.close * 100.0;
        let close_pos = (today.close - today.low) / (today.high - today.low);
        let vol_ratio = today.volume as f64 / avg_volume(&bars[..n - 1], 10).max(1.0);

        let trend_ok = today.close >= ma20 && ma10 >= ma20 * 0.995;
        let amplitude_ok = (3.5..=12.0).contains(&amplitude_pct);
        let gain_ok = (-2.5..=5.5).contains(&gain_pct);
        let vol_ok = (1.1..=4.5).contains(&vol_ratio);
        let close_ok = (0.35..=0.9).contains(&close_pos);

        if trend_ok && amplitude_ok && gain_ok && vol_ok && close_ok {
            SignalResult::yes()
                .with_meta("gain_pct", serde_json::json!(gain_pct))
                .with_meta("amplitude_pct", serde_json::json!(amplitude_pct))
                .with_meta("vol_ratio", serde_json::json!(vol_ratio))
                .with_meta("close_pos", serde_json::json!(close_pos))
        } else {
            SignalResult::no()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn candle(open: f64, high: f64, low: f64, close: f64, volume: i64) -> Candle {
        Candle {
            trade_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            open,
            high,
            low,
            close,
            volume,
            amount: close * volume as f64,
            turnover: None,
            pe: None,
            pb: None,
        }
    }

    fn rising_bars() -> Vec<Candle> {
        let mut bars = Vec::new();
        for i in 0..59 {
            let close = 10.0 + i as f64 * 0.03;
            bars.push(candle(
                close * 0.998,
                close * 1.01,
                close * 0.99,
                close,
                1_000_000,
            ));
        }
        bars
    }

    #[test]
    fn t_trade_signal_triggers_when_trend_and_volatility_match() {
        let mut bars = rising_bars();
        let prev_close = bars.last().unwrap().close;
        let close = prev_close * 1.018;
        bars.push(candle(
            close * 0.992,
            close * 1.03,
            close * 0.965,
            close,
            2_000_000,
        ));

        let sig = TTradeSignal;
        let ctx = StockContext {
            code: "600000.SH".to_string(),
            name: "Test".to_string(),
        };
        let result = sig.detect(&bars, &ctx);
        assert!(result.triggered);
        assert!(result.metadata.contains_key("amplitude_pct"));
    }

    #[test]
    fn t_trade_signal_rejects_low_amplitude_days() {
        let mut bars = rising_bars();
        let prev_close = bars.last().unwrap().close;
        let close = prev_close * 1.01;
        bars.push(candle(
            close * 0.999,
            close * 1.004,
            close * 0.996,
            close,
            2_000_000,
        ));

        let sig = TTradeSignal;
        let ctx = StockContext {
            code: "600000.SH".to_string(),
            name: "Test".to_string(),
        };
        assert!(!sig.detect(&bars, &ctx).triggered);
    }
}
