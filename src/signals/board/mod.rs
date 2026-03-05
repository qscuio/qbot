use crate::data::types::Candle;
use super::base::{SignalDetector, SignalResult, StockContext, sma, avg_volume};

const LIMIT_UP_PCT: f64 = 9.8;

fn is_limit_up(bar: &Candle) -> bool {
    if bar.open == 0.0 { return false; }
    (bar.close - bar.open) / bar.open * 100.0 >= LIMIT_UP_PCT
}

/// 炸板反包 — yesterday hit limit-up but closed below it; today recovers
pub struct BrokenBoardSignal;
impl SignalDetector for BrokenBoardSignal {
    fn signal_id(&self) -> &'static str { "broken_board" }
    fn display_name(&self) -> &'static str { "炸板反包" }
    fn icon(&self) -> &'static str { "💥" }
    fn group(&self) -> &'static str { "board" }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 3 { return SignalResult::no(); }
        let prev2 = &bars[n-3];
        let prev = &bars[n-2];
        let today = &bars[n-1];
        let high_hit_limit = prev2.open > 0.0
            && (prev2.high - prev2.open) / prev2.open * 100.0 >= LIMIT_UP_PCT;
        let closed_below = prev2.close < prev2.high * 0.98;
        let recovering = today.close > today.open && today.close > prev.close;
        if high_hit_limit && closed_below && recovering {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 强势首阴 — first down day after limit-up streak, still strong
pub struct StrongFirstNegSignal;
impl SignalDetector for StrongFirstNegSignal {
    fn signal_id(&self) -> &'static str { "strong_first_neg" }
    fn display_name(&self) -> &'static str { "强势首阴" }
    fn icon(&self) -> &'static str { "⚡" }
    fn group(&self) -> &'static str { "board" }
    fn min_bars(&self) -> usize { 5 }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 5 { return SignalResult::no(); }
        let prev = &bars[n-2];
        let today = &bars[n-1];
        let prev_was_limit = is_limit_up(prev);
        let mild_decline = today.close < today.open
            && (today.open - today.close) / today.open * 100.0 < 3.0;
        let above_ma5 = {
            let closes: Vec<f64> = bars[n-5..].iter().map(|b| b.close).collect();
            sma(&closes, 5).map(|ma| today.close > ma).unwrap_or(false)
        };
        if prev_was_limit && mild_decline && above_ma5 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}
