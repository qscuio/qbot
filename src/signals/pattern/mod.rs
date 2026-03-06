use super::base::{sma, SignalDetector, SignalResult, StockContext};
use crate::data::types::Candle;

fn closes(bars: &[Candle]) -> Vec<f64> {
    bars.iter().map(|b| b.close).collect()
}

/// 缓慢牛 — steady uptrend: close > MA5 > MA10 for 4+ of last 5 days
pub struct SlowBullSignal;
impl SignalDetector for SlowBullSignal {
    fn signal_id(&self) -> &'static str {
        "slow_bull"
    }
    fn display_name(&self) -> &'static str {
        "缓慢牛"
    }
    fn icon(&self) -> &'static str {
        "🐢"
    }
    fn group(&self) -> &'static str {
        "pattern"
    }
    fn min_bars(&self) -> usize {
        25
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 25 {
            return SignalResult::no();
        }
        let ok_days = (0..5)
            .filter(|&i| {
                let slice = &bars[..n - i];
                let c = closes(slice);
                let close = slice.last().unwrap().close;
                let ma5 = sma(&c, 5).unwrap_or(0.0);
                let ma10 = sma(&c, 10).unwrap_or(0.0);
                close > ma5 && ma5 > ma10
            })
            .count();
        if ok_days >= 4 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 小阳线 — 3+ consecutive small up candles (body < 2.5%)
pub struct SmallBullishSignal;
impl SignalDetector for SmallBullishSignal {
    fn signal_id(&self) -> &'static str {
        "small_bullish"
    }
    fn display_name(&self) -> &'static str {
        "小阳线"
    }
    fn icon(&self) -> &'static str {
        "🌱"
    }
    fn group(&self) -> &'static str {
        "pattern"
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 3 {
            return SignalResult::no();
        }
        let consecutive = bars[n - 3..]
            .iter()
            .all(|b| b.close > b.open && (b.close - b.open) / b.open * 100.0 < 2.5);
        if consecutive {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 三阳开泰 — 3 consecutive up days, each close > previous close
pub struct TripleBullishSignal;
impl SignalDetector for TripleBullishSignal {
    fn signal_id(&self) -> &'static str {
        "triple_bullish"
    }
    fn display_name(&self) -> &'static str {
        "三阳开泰"
    }
    fn icon(&self) -> &'static str {
        "🔥"
    }
    fn group(&self) -> &'static str {
        "pattern"
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 4 {
            return SignalResult::no();
        }
        let three = &bars[n - 3..];
        let ok = three.iter().all(|b| b.close > b.open)
            && three[1].close > three[0].close
            && three[2].close > three[1].close;
        if ok {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 反包 — today's up candle fully engulfs yesterday's down candle
pub struct FanbaoSignal;
impl SignalDetector for FanbaoSignal {
    fn signal_id(&self) -> &'static str {
        "fanbao"
    }
    fn display_name(&self) -> &'static str {
        "反包"
    }
    fn icon(&self) -> &'static str {
        "🔁"
    }
    fn group(&self) -> &'static str {
        "pattern"
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 2 {
            return SignalResult::no();
        }
        let prev = &bars[n - 2];
        let today = &bars[n - 1];
        let prev_was_down = prev.close < prev.open;
        let today_engulfs =
            today.close > today.open && today.open <= prev.close && today.close >= prev.open;
        if prev_was_down && today_engulfs {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 周月多头 — weekly MA5 > MA10 approximated from daily (MA25 > MA50)
pub struct WeeklyMonthlyBullishSignal;
impl SignalDetector for WeeklyMonthlyBullishSignal {
    fn signal_id(&self) -> &'static str {
        "weekly_monthly_bullish"
    }
    fn display_name(&self) -> &'static str {
        "周月多头"
    }
    fn icon(&self) -> &'static str {
        "🗓️"
    }
    fn group(&self) -> &'static str {
        "pattern"
    }
    fn min_bars(&self) -> usize {
        60
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 60 {
            return SignalResult::no();
        }
        let c = closes(bars);
        let ma25 = sma(&c, 25).unwrap_or(0.0);
        let ma50 = sma(&c, 50).unwrap_or(0.0);
        if ma25 > ma50 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}
