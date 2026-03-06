use super::base::{avg_volume, SignalDetector, SignalResult, StockContext};
use crate::data::types::Candle;

/// 放量突破 — today's volume > 2x 20-day average
pub struct VolumeSurgeSignal;
impl SignalDetector for VolumeSurgeSignal {
    fn signal_id(&self) -> &'static str {
        "volume_surge"
    }
    fn display_name(&self) -> &'static str {
        "放量突破"
    }
    fn icon(&self) -> &'static str {
        "📊"
    }
    fn group(&self) -> &'static str {
        "volume"
    }
    fn min_bars(&self) -> usize {
        22
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 22 {
            return SignalResult::no();
        }
        let today = &bars[n - 1];
        let avg = avg_volume(&bars[..n - 1], 20);
        if avg == 0.0 {
            return SignalResult::no();
        }
        let ratio = today.volume as f64 / avg;
        if ratio >= 2.0 && today.close > today.open {
            SignalResult::yes()
                .with_meta("volume_ratio", serde_json::json!(format!("{:.1}x", ratio)))
        } else {
            SignalResult::no()
        }
    }
}

/// 量价配合 — up-day volume consistently > down-day volume (5-day window)
pub struct VolumePriceSignal;
impl SignalDetector for VolumePriceSignal {
    fn signal_id(&self) -> &'static str {
        "volume_price"
    }
    fn display_name(&self) -> &'static str {
        "量价配合"
    }
    fn icon(&self) -> &'static str {
        "📈"
    }
    fn group(&self) -> &'static str {
        "volume"
    }
    fn min_bars(&self) -> usize {
        10
    }

    fn detect(&self, bars: &[Candle], _ctx: &StockContext) -> SignalResult {
        let n = bars.len();
        if n < 10 {
            return SignalResult::no();
        }
        let window = &bars[n - 5..];
        let (up_vol, down_vol): (f64, f64) = window.iter().fold((0.0, 0.0), |(u, d), b| {
            if b.close >= b.open {
                (u + b.volume as f64, d)
            } else {
                (u, d + b.volume as f64)
            }
        });
        if down_vol == 0.0 || up_vol / (up_vol + down_vol) >= 0.65 {
            SignalResult::yes()
        } else {
            SignalResult::no()
        }
    }
}
