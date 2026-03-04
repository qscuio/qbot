use crate::data::types::Candle;
use super::base::{SignalDetector, SignalResult, StockContext, sma, avg_volume};

fn closes(bars: &[Candle]) -> Vec<f64> { bars.iter().map(|b| b.close).collect() }

/// 底部快速启动 — bottom reversal: was below MA20, now breaking up with volume
pub struct BottomQuickStartSignal;
impl SignalDetector for BottomQuickStartSignal {
    fn signal_id(&self) -> &'static str { "bottom_quick_start" }
    fn display_name(&self) -> &'static str { "底部快速启动" }
    fn icon(&self) -> &'static str { "⬆️" }
    fn group(&self) -> &'static str { "comprehensive" }
    fn min_bars(&self) -> usize { 30 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 30 { return SignalResult::no(); }
        let c = closes(bars);
        let ma20 = sma(&c, 20).unwrap_or(0.0);
        let today = &bars[n-1];
        let was_below = bars[n-5..n-1].iter().any(|b| b.close < ma20);
        let above_now = today.close > ma20;
        let vol_surge = today.volume as f64 > avg_volume(&bars[..n-1], 10) * 1.8;
        if was_below && above_now && vol_surge { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 长周期反转 — weekly-level bottom: 60-day low area, breaking up
pub struct LongCycleReversalSignal;
impl SignalDetector for LongCycleReversalSignal {
    fn signal_id(&self) -> &'static str { "long_cycle_reversal" }
    fn display_name(&self) -> &'static str { "长周期反转" }
    fn icon(&self) -> &'static str { "🌅" }
    fn group(&self) -> &'static str { "comprehensive" }
    fn min_bars(&self) -> usize { 65 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 65 { return SignalResult::no(); }
        let low_60 = bars[n-60..n-1].iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
        let today = &bars[n-1];
        let near_bottom = (today.low - low_60) / low_60 * 100.0 < 10.0;
        let c = closes(bars);
        let ma5 = sma(&c, 5).unwrap_or(0.0);
        let reversing = today.close > today.open && today.close > ma5;
        if near_bottom && reversing { SignalResult::yes() } else { SignalResult::no() }
    }
}

/// 低位积累启动 — narrow range consolidation followed by breakout
pub struct LowAccumulationSignal;
impl SignalDetector for LowAccumulationSignal {
    fn signal_id(&self) -> &'static str { "low_accumulation" }
    fn display_name(&self) -> &'static str { "低位积累启动" }
    fn icon(&self) -> &'static str { "🏗️" }
    fn group(&self) -> &'static str { "comprehensive" }
    fn min_bars(&self) -> usize { 25 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 25 { return SignalResult::no(); }
        let window = &bars[n-16..n-1];
        let high = window.iter().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max);
        let low = window.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
        if low == 0.0 { return SignalResult::no(); }
        let range_pct = (high - low) / low * 100.0;
        let consolidated = range_pct < 8.0;
        let today = &bars[n-1];
        let breakout = today.close > high && today.close > today.open;
        let vol_surge = today.volume as f64 > avg_volume(&bars[..n-1], 15) * 1.5;
        if consolidated && breakout && vol_surge { SignalResult::yes() } else { SignalResult::no() }
    }
}
