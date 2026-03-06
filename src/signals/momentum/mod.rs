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
