use super::base::{avg_volume, sma, SignalDetector, SignalResult, StockContext};
use crate::data::types::Candle;

fn closes(bars: &[Candle]) -> Vec<f64> {
    bars.iter().map(|b| b.close).collect()
}

/// 突破信号 — close breaks 20-day high
pub struct BreakoutSignal;
impl SignalDetector for BreakoutSignal {
    fn signal_id(&self) -> &'static str {
        "breakout"
    }
    fn display_name(&self) -> &'static str {
        "突破信号"
    }
    fn icon(&self) -> &'static str {
        "🔺"
    }
    fn group(&self) -> &'static str {
        "momentum"
    }
    fn min_bars(&self) -> usize {
        21
    }
    fn priority(&self) -> i32 {
        10
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 21 {
            return SignalResult::no();
        }
        let high_20 = bars[n - 21..n - 1]
            .iter()
            .map(|b| b.high)
            .fold(f64::NEG_INFINITY, f64::max);
        if bars[n - 1].close > high_20 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 启动信号 — volume surge + MA alignment
pub struct StartupSignal;
impl SignalDetector for StartupSignal {
    fn signal_id(&self) -> &'static str {
        "startup"
    }
    fn display_name(&self) -> &'static str {
        "启动信号"
    }
    fn icon(&self) -> &'static str {
        "🚦"
    }
    fn group(&self) -> &'static str {
        "momentum"
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
        let ma5 = sma(&c, 5).unwrap_or(0.0);
        let ma10 = sma(&c, 10).unwrap_or(0.0);
        let today = &bars[n - 1];
        let vol_ok = today.volume as f64 > avg_volume(&bars[..n - 1], 10) * 1.5;
        let ma_ok = ma5 > ma10 && today.close > ma5;
        let price_ok = today.close > today.open;
        if vol_ok && ma_ok && price_ok {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 狂飙信号 — explosive: >7% gain with >3x volume
pub struct KuangbiaoSignal;
impl SignalDetector for KuangbiaoSignal {
    fn signal_id(&self) -> &'static str {
        "kuangbiao"
    }
    fn display_name(&self) -> &'static str {
        "狂飙信号"
    }
    fn icon(&self) -> &'static str {
        "🌪️"
    }
    fn group(&self) -> &'static str {
        "momentum"
    }
    fn min_bars(&self) -> usize {
        21
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 21 {
            return SignalResult::no();
        }
        let today = &bars[n - 1];
        let prev = &bars[n - 2];
        if prev.close == 0.0 {
            return SignalResult::no();
        }
        let gain_pct = (today.close - prev.close) / prev.close * 100.0;
        let vol_ratio = today.volume as f64 / avg_volume(&bars[..n - 1], 20).max(1.0);
        if gain_pct >= 7.0 && vol_ratio >= 3.0 {
            SignalResult::yes()
                .with_meta("gain_pct", serde_json::json!(format!("{:.1}%", gain_pct)))
                .with_meta("vol_ratio", serde_json::json!(format!("{:.1}x", vol_ratio)))
        } else {
            SignalResult::no()
        }
    }
}

/// 超跌反弹 — RSI washout followed by volume-backed MA5 reclaim
pub struct OversoldReboundSignal;
impl SignalDetector for OversoldReboundSignal {
    fn signal_id(&self) -> &'static str {
        "oversold_rebound"
    }
    fn display_name(&self) -> &'static str {
        "超跌反弹"
    }
    fn icon(&self) -> &'static str {
        "🩹"
    }
    fn group(&self) -> &'static str {
        "momentum"
    }
    fn min_bars(&self) -> usize {
        35
    }
    fn priority(&self) -> i32 {
        78
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 35 {
            return SignalResult::no();
        }

        let today = &bars[n - 1];
        let prev = &bars[n - 2];
        if prev.close <= 0.0 || today.close <= 0.0 || today.high <= today.low {
            return SignalResult::no();
        }

        let c = closes(bars);
        let ma5 = match sma(&c, 5) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        let ma20 = match sma(&c, 20) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        if ma20 <= 0.0 {
            return SignalResult::no();
        }

        let rsi14 = match rsi_at(&c, n, 14) {
            Some(v) => v,
            None => return SignalResult::no(),
        };
        let min_rsi5 = (0..5)
            .filter_map(|offset| rsi_at(&c, n - offset, 14))
            .fold(f64::INFINITY, f64::min);
        if !min_rsi5.is_finite() {
            return SignalResult::no();
        }

        let high_20 = bars[n - 21..n - 1]
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);
        if !high_20.is_finite() || high_20 <= 0.0 {
            return SignalResult::no();
        }

        let drawdown_20_pct = (today.close / high_20 - 1.0) * 100.0;
        let bias_ma20_pct = (today.close / ma20 - 1.0) * 100.0;
        let gain_pct = (today.close / prev.close - 1.0) * 100.0;
        let close_pos = (today.close - today.low) / (today.high - today.low);
        let vol_ratio = today.volume as f64 / avg_volume(&bars[..n - 1], 10).max(1.0);
        let avg20_amount = avg_amount(&bars[n - 21..n - 1]);

        let oversold_washout = min_rsi5 <= 30.0 && rsi14 <= 58.0;
        let deep_enough = drawdown_20_pct <= -15.0 && bias_ma20_pct <= 6.0;
        let rebound_confirmed = today.close > today.open
            && today.close > ma5
            && (2.0..=9.3).contains(&gain_pct)
            && close_pos >= 0.65;
        let volume_repair = (1.2..=3.8).contains(&vol_ratio);
        let liquid_enough = today.amount >= 50_000_000.0 && avg20_amount >= 30_000_000.0;

        if oversold_washout && deep_enough && rebound_confirmed && volume_repair && liquid_enough {
            SignalResult::yes()
                .with_meta("rsi14", serde_json::json!(round2(rsi14)))
                .with_meta("min_rsi5", serde_json::json!(round2(min_rsi5)))
                .with_meta(
                    "drawdown_20_pct",
                    serde_json::json!(round2(drawdown_20_pct)),
                )
                .with_meta("bias_ma20_pct", serde_json::json!(round2(bias_ma20_pct)))
                .with_meta("gain_pct", serde_json::json!(round2(gain_pct)))
                .with_meta("close_pos", serde_json::json!(round2(close_pos)))
                .with_meta("vol_ratio", serde_json::json!(round2(vol_ratio)))
                .with_meta(
                    "avg20_amount_yi",
                    serde_json::json!(round2(avg20_amount / 100_000_000.0)),
                )
        } else {
            SignalResult::no()
        }
    }
}

fn rsi_at(closes: &[f64], end: usize, period: usize) -> Option<f64> {
    if period == 0 || end <= period || end > closes.len() {
        return None;
    }

    let start = end - period - 1;
    let mut gain_sum = 0.0;
    let mut loss_sum = 0.0;
    for idx in start + 1..end {
        let diff = closes[idx] - closes[idx - 1];
        if diff > 0.0 {
            gain_sum += diff;
        } else {
            loss_sum += -diff;
        }
    }

    let avg_gain = gain_sum / period as f64;
    let avg_loss = loss_sum / period as f64;
    if avg_loss <= f64::EPSILON {
        return Some(100.0);
    }
    let rs = avg_gain / avg_loss;
    Some(100.0 - 100.0 / (1.0 + rs))
}

fn avg_amount(bars: &[Candle]) -> f64 {
    if bars.is_empty() {
        return 0.0;
    }
    bars.iter().map(|bar| bar.amount).sum::<f64>() / bars.len() as f64
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn candle(close: f64, volume: i64) -> Candle {
        Candle {
            trade_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            open: close * 0.99,
            high: close * 1.02,
            low: close * 0.98,
            close,
            volume,
            amount: close * volume as f64,
            turnover: None,
            pe: None,
            pb: None,
        }
    }

    fn ctx() -> StockContext {
        StockContext {
            code: "600000.SH".to_string(),
            name: "浦发银行".to_string(),
        }
    }

    #[test]
    fn oversold_rebound_triggers_after_rsi_washout_and_volume_reclaim() {
        let closes = [
            20.0, 19.6, 19.1, 18.7, 18.2, 17.8, 17.3, 16.9, 16.5, 16.0, 15.4, 14.9, 14.4, 13.8,
            13.3, 12.8, 12.3, 11.9, 11.5, 11.1, 10.8, 10.6, 10.4, 10.2, 10.0, 9.9, 9.8, 9.7, 9.65,
            9.7, 9.8, 9.9, 10.0, 10.1,
        ];
        let mut bars: Vec<Candle> = closes
            .iter()
            .map(|close| candle(*close, 10_000_000))
            .collect();
        bars.push(Candle {
            open: 10.25,
            high: 10.95,
            low: 10.20,
            close: 10.90,
            volume: 18_000_000,
            amount: 196_200_000.0,
            ..candle(10.90, 18_000_000)
        });

        let result = OversoldReboundSignal.detect(&bars, &ctx());

        assert!(result.triggered);
        assert!(result.metadata.contains_key("rsi14"));
        assert!(result.metadata.contains_key("min_rsi5"));
        assert!(result.metadata.contains_key("drawdown_20_pct"));
        assert!(result.metadata.contains_key("vol_ratio"));
    }

    #[test]
    fn oversold_rebound_rejects_continuing_selloff_without_reclaim() {
        let closes = [
            20.0, 19.6, 19.1, 18.7, 18.2, 17.8, 17.3, 16.9, 16.5, 16.0, 15.4, 14.9, 14.4, 13.8,
            13.3, 12.8, 12.3, 11.9, 11.5, 11.1, 10.8, 10.6, 10.4, 10.2, 10.0, 9.9, 9.8, 9.7, 9.65,
            9.6, 9.55, 9.5, 9.45, 9.4, 9.3,
        ];
        let bars: Vec<Candle> = closes
            .iter()
            .map(|close| candle(*close, 18_000_000))
            .collect();

        let result = OversoldReboundSignal.detect(&bars, &ctx());

        assert!(!result.triggered);
    }
}
