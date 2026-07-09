use super::base::{avg_volume, sma, SignalDetector, SignalResult, StockContext};
use crate::data::types::Candle;

fn closes(bars: &[Candle]) -> Vec<f64> {
    bars.iter().map(|b| b.close).collect()
}

/// 底部快速启动 — bottom reversal: was below MA20, now breaking up with volume
pub struct BottomQuickStartSignal;
impl SignalDetector for BottomQuickStartSignal {
    fn signal_id(&self) -> &'static str {
        "bottom_quick_start"
    }
    fn display_name(&self) -> &'static str {
        "底部快速启动"
    }
    fn icon(&self) -> &'static str {
        "⬆️"
    }
    fn group(&self) -> &'static str {
        "comprehensive"
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
        let ma20 = sma(&c, 20).unwrap_or(0.0);
        let today = &bars[n - 1];
        let was_below = bars[n - 5..n - 1].iter().any(|b| b.close < ma20);
        let above_now = today.close > ma20;
        let vol_surge = today.volume as f64 > avg_volume(&bars[..n - 1], 10) * 1.8;
        if was_below && above_now && vol_surge {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

/// 底部首启 — low base, quiet volume, first strong expansion candle
pub struct BottomEarlyStartSignal;
impl SignalDetector for BottomEarlyStartSignal {
    fn signal_id(&self) -> &'static str {
        "bottom_early_start"
    }
    fn display_name(&self) -> &'static str {
        "底部首启"
    }
    fn icon(&self) -> &'static str {
        "🌱"
    }
    fn group(&self) -> &'static str {
        "comprehensive"
    }
    fn min_bars(&self) -> usize {
        80
    }

    fn priority(&self) -> i32 {
        82
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 80 {
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
        let ma60 = match sma(&c, 60) {
            Some(v) => v,
            None => return SignalResult::no(),
        };

        let sixty_day = &bars[n - 61..n - 1];
        let low_60 = lowest_low(sixty_day);
        let high_60 = highest_high(sixty_day);
        if low_60 <= 0.0 || high_60 <= low_60 {
            return SignalResult::no();
        }

        let base = &bars[n - 16..n - 1];
        let base_low = lowest_low(base);
        let base_high = highest_high(base);
        if base_low <= 0.0 || base_high <= base_low {
            return SignalResult::no();
        }

        let bottom_position = (today.close - low_60) / (high_60 - low_60);
        let base_range_pct = (base_high - base_low) / base_low * 100.0;
        let gain_pct = (today.close / prev.close - 1.0) * 100.0;
        let close_pos = (today.close - today.low) / (today.high - today.low);
        let avg20_volume = avg_volume(&bars[..n - 1], 20).max(1.0);
        let prior5_volume = avg_volume(&bars[n - 6..n - 1], 5);
        let prior20_volume = avg_volume(&bars[n - 21..n - 1], 20).max(1.0);
        let vol_ratio = today.volume as f64 / avg20_volume;
        let avg20_amount = avg_amount(&bars[n - 21..n - 1]);
        let extension_ma20_pct = (today.close / ma20 - 1.0) * 100.0;

        let near_bottom = bottom_position <= 0.45 && today.close <= low_60 * 1.22;
        let consolidated = base_range_pct <= 14.0;
        let quiet_before_start = prior5_volume <= prior20_volume * 1.15;
        let volume_expansion = (1.6..=5.0).contains(&vol_ratio);
        let first_strong_candle =
            today.close > today.open && (1.8..=7.8).contains(&gain_pct) && close_pos >= 0.65;
        let early_trend_turn = today.close > ma5
            && today.close >= ma20 * 0.985
            && extension_ma20_pct <= 8.0
            && ma20 <= ma60 * 1.04;
        let liquid_enough = today.amount >= 50_000_000.0 && avg20_amount >= 30_000_000.0;

        if near_bottom
            && consolidated
            && quiet_before_start
            && volume_expansion
            && first_strong_candle
            && early_trend_turn
            && liquid_enough
        {
            SignalResult::yes()
                .with_meta(
                    "bottom_position",
                    serde_json::json!(round2(bottom_position)),
                )
                .with_meta("base_range_pct", serde_json::json!(round2(base_range_pct)))
                .with_meta("gain_pct", serde_json::json!(round2(gain_pct)))
                .with_meta("close_pos", serde_json::json!(round2(close_pos)))
                .with_meta("vol_ratio", serde_json::json!(round2(vol_ratio)))
                .with_meta(
                    "avg20_amount_yi",
                    serde_json::json!(round2(avg20_amount / 100_000_000.0)),
                )
                .with_meta(
                    "extension_ma20_pct",
                    serde_json::json!(round2(extension_ma20_pct)),
                )
        } else {
            SignalResult::no()
        }
    }
}

/// 长周期反转 — weekly-level bottom: 60-day low area, breaking up
pub struct LongCycleReversalSignal;
impl SignalDetector for LongCycleReversalSignal {
    fn signal_id(&self) -> &'static str {
        "long_cycle_reversal"
    }
    fn display_name(&self) -> &'static str {
        "长周期反转"
    }
    fn icon(&self) -> &'static str {
        "🌅"
    }
    fn group(&self) -> &'static str {
        "comprehensive"
    }
    fn min_bars(&self) -> usize {
        65
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 65 {
            return SignalResult::no();
        }
        let low_60 = bars[n - 60..n - 1]
            .iter()
            .map(|b| b.low)
            .fold(f64::INFINITY, f64::min);
        let today = &bars[n - 1];
        let near_bottom = (today.low - low_60) / low_60 * 100.0 < 10.0;
        let c = closes(bars);
        let ma5 = sma(&c, 5).unwrap_or(0.0);
        let reversing = today.close > today.open && today.close > ma5;
        if near_bottom && reversing {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}

fn highest_high(bars: &[Candle]) -> f64 {
    bars.iter()
        .map(|bar| bar.high)
        .fold(f64::NEG_INFINITY, f64::max)
}

fn lowest_low(bars: &[Candle]) -> f64 {
    bars.iter().map(|bar| bar.low).fold(f64::INFINITY, f64::min)
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

/// 低位积累启动 — narrow range consolidation followed by breakout
pub struct LowAccumulationSignal;
impl SignalDetector for LowAccumulationSignal {
    fn signal_id(&self) -> &'static str {
        "low_accumulation"
    }
    fn display_name(&self) -> &'static str {
        "低位积累启动"
    }
    fn icon(&self) -> &'static str {
        "🏗️"
    }
    fn group(&self) -> &'static str {
        "comprehensive"
    }
    fn min_bars(&self) -> usize {
        25
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 25 {
            return SignalResult::no();
        }
        let window = &bars[n - 16..n - 1];
        let high = window
            .iter()
            .map(|b| b.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let low = window.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
        if low == 0.0 {
            return SignalResult::no();
        }
        let range_pct = (high - low) / low * 100.0;
        let consolidated = range_pct < 8.0;
        let today = &bars[n - 1];
        let breakout = today.close > high && today.close > today.open;
        let vol_surge = today.volume as f64 > avg_volume(&bars[..n - 1], 15) * 1.5;
        if consolidated && breakout && vol_surge {
            SignalResult::yes()
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

    fn bottom_base_bars() -> Vec<Candle> {
        let mut bars = Vec::new();
        for i in 0..64 {
            let close = 13.0 - i as f64 * 0.055;
            bars.push(candle(
                close * 1.002,
                close * 1.015,
                close * 0.985,
                close,
                8_000_000,
            ));
        }

        for i in 0..15 {
            let close = 9.58 + (i % 4) as f64 * 0.035;
            bars.push(candle(
                close * 0.998,
                close * 1.012,
                close * 0.992,
                close,
                5_500_000,
            ));
        }
        bars
    }

    #[test]
    fn bottom_early_start_triggers_after_low_base_volume_expansion() {
        let mut bars = bottom_base_bars();
        bars.push(candle(9.70, 10.10, 9.66, 10.02, 18_000_000));

        let sig = BottomEarlyStartSignal;
        let ctx = StockContext {
            code: "300001.SZ".to_string(),
            name: "Test".to_string(),
        };
        let result = sig.detect(&bars, &ctx);

        assert!(result.triggered);
        assert!(result.metadata.contains_key("bottom_position"));
        assert!(result.metadata.contains_key("vol_ratio"));
    }

    #[test]
    fn bottom_early_start_rejects_overheated_rebound() {
        let mut bars = bottom_base_bars();
        bars.push(candle(9.80, 11.20, 9.74, 11.05, 22_000_000));

        let sig = BottomEarlyStartSignal;
        let ctx = StockContext {
            code: "300001.SZ".to_string(),
            name: "Test".to_string(),
        };

        assert!(!sig.detect(&bars, &ctx).triggered);
    }
}
