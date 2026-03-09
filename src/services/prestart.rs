use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use serde::Serialize;

use crate::data::types::Candle;
use crate::error::Result;
use crate::services::scanner::{ScannerService, SignalHit};
use crate::signals::base::{avg_volume, sma};
use crate::state::AppState;
use crate::storage::postgres;
use crate::storage::redis_cache::RedisCache;

const PRESTART_SIGNAL_IDS: [&str; 5] = [
    "ma_bullish",
    "volume_price",
    "slow_bull",
    "small_bullish",
    "triple_bullish",
];
const MIN_AMOUNT_YUAN: f64 = 200_000_000.0;
const MAX_RANGE_15_PCT: f64 = 12.0;
const MIN_GAIN_PCT: f64 = 0.5;
const MAX_GAIN_PCT: f64 = 4.5;
const MAX_GAP_TO_HIGH_PCT: f64 = 4.0;
const MAX_BREAKOUT_OVERSHOOT_PCT: f64 = 1.5;
const MAX_MA20_EXTENSION_PCT: f64 = 8.0;
const MIN_VOL_RATIO_10: f64 = 0.9;
const CORE_SIGNAL_IDS: [&str; 2] = ["ma_bullish", "slow_bull"];
const AUX_SIGNAL_IDS: [&str; 3] = ["volume_price", "small_bullish", "triple_bullish"];

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "UPPERCASE")]
pub enum PrestartTier {
    B,
    A,
}

impl fmt::Display for PrestartTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrestartTier::A => write!(f, "A"),
            PrestartTier::B => write!(f, "B"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PrestartCandidate {
    pub code: String,
    pub name: String,
    pub tier: PrestartTier,
    pub score: f64,
    pub matched_signal_ids: Vec<String>,
    pub matched_signal_names: Vec<String>,
    pub reasons: Vec<String>,
    pub gain_pct: f64,
    pub amount_yi: f64,
    pub range_15_pct: f64,
    pub gap_to_high_pct: f64,
    pub ma20_extension_pct: f64,
}

pub struct PrestartService {
    state: Arc<AppState>,
}

pub fn is_prestart_signal(signal_id: &str) -> bool {
    PRESTART_SIGNAL_IDS.contains(&signal_id)
}

impl PrestartService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn list_candidates(&self, limit: usize) -> Result<Vec<PrestartCandidate>> {
        let scan_map = self.load_or_scan().await?;
        self.list_candidates_from_scan(&scan_map, limit).await
    }

    pub async fn list_candidates_from_scan(
        &self,
        scan_map: &HashMap<String, Vec<SignalHit>>,
        limit: usize,
    ) -> Result<Vec<PrestartCandidate>> {
        let mut grouped: HashMap<String, Vec<SignalHit>> = HashMap::new();

        for signal_id in PRESTART_SIGNAL_IDS {
            let Some(hits) = scan_map.get(signal_id) else {
                continue;
            };
            for hit in hits {
                grouped
                    .entry(hit.code.clone())
                    .or_default()
                    .push(hit.clone());
            }
        }

        let mut candidates = Vec::new();
        for (code, hits) in grouped {
            let bars = match postgres::get_stock_history(&self.state.db, &code, 90).await {
                Ok(v) if v.len() >= 60 => v,
                _ => continue,
            };

            if let Some(candidate) = build_prestart_candidate(&hits, &bars) {
                candidates.push(candidate);
            }
        }

        candidates.sort_by(|a, b| {
            b.tier
                .cmp(&a.tier)
                .then_with(|| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| a.code.cmp(&b.code))
        });
        candidates.truncate(limit.clamp(1, 50));
        Ok(candidates)
    }

    async fn load_or_scan(&self) -> Result<HashMap<String, Vec<SignalHit>>> {
        let mut cache = RedisCache::new(self.state.redis.clone());
        if let Some(raw) = cache.get_scan_results().await? {
            if let Ok(scan_map) = serde_json::from_value::<HashMap<String, Vec<SignalHit>>>(raw) {
                return Ok(scan_map);
            }
        }
        ScannerService::new(self.state.clone())
            .run_full_scan()
            .await
    }
}

fn build_prestart_candidate(hits: &[SignalHit], bars: &[Candle]) -> Option<PrestartCandidate> {
    if bars.len() < 60 {
        return None;
    }

    let today = bars.last()?;
    let prev = bars.get(bars.len().checked_sub(2)?)?;
    let name = hits.first()?.name.clone();
    if name.to_ascii_uppercase().contains("ST") {
        return None;
    }
    if today.amount < MIN_AMOUNT_YUAN {
        return None;
    }

    let range_15_pct = range_pct(&bars[bars.len() - 15..])?;
    if range_15_pct > MAX_RANGE_15_PCT {
        return None;
    }

    let gain_pct = pct_change(prev.close, today.close);
    if !(MIN_GAIN_PCT..=MAX_GAIN_PCT).contains(&gain_pct) {
        return None;
    }

    let recent_high_20 = bars[bars.len() - 21..bars.len() - 1]
        .iter()
        .map(|b| b.high)
        .fold(f64::NEG_INFINITY, f64::max);
    if !recent_high_20.is_finite() || recent_high_20 <= 0.0 {
        return None;
    }
    let gap_to_high_pct = (recent_high_20 - today.close) / recent_high_20 * 100.0;
    if gap_to_high_pct > MAX_GAP_TO_HIGH_PCT || gap_to_high_pct < -MAX_BREAKOUT_OVERSHOOT_PCT {
        return None;
    }

    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let ma20 = sma(&closes, 20)?;
    let ma20_extension_pct = pct_change(ma20, today.close);
    if ma20_extension_pct > MAX_MA20_EXTENSION_PCT {
        return None;
    }

    let vol_ratio_10 = today.volume as f64 / avg_volume(&bars[..bars.len() - 1], 10).max(1.0);
    if vol_ratio_10 < MIN_VOL_RATIO_10 {
        return None;
    }

    let tier = classify_tier(hits)?;
    let matched_signal_ids: Vec<String> = hits.iter().map(|hit| hit.signal_id.clone()).collect();
    let matched_signal_names: Vec<String> =
        hits.iter().map(|hit| hit.signal_name.clone()).collect();

    let mut reasons = vec![tier_reason(tier, hits)];
    reasons.push(format!("15日振幅 {:.1}%", range_15_pct));
    reasons.push(format!("距20日高点 {:.1}%", gap_to_high_pct));
    reasons.push(format!("成交额 {:.1}亿", today.amount / 100_000_000.0));
    reasons.push(format!("当日涨幅 {:+.1}%", gain_pct));

    let close_pos = if today.high > today.low {
        (today.close - today.low) / (today.high - today.low)
    } else {
        0.5
    };

    let mut score = hits.len() as f64 * 20.0;
    score += match tier {
        PrestartTier::A => 15.0,
        PrestartTier::B => 0.0,
    };
    score += (MAX_RANGE_15_PCT - range_15_pct).max(0.0) * 1.8;
    score += breakout_proximity_score(gap_to_high_pct);
    score += triangular_score(gain_pct, 2.2, MIN_GAIN_PCT, MAX_GAIN_PCT) * 12.0;
    score += close_pos * 10.0;
    score += (vol_ratio_10.min(2.0) - 0.9).max(0.0) * 8.0;
    score += ((today.amount / MIN_AMOUNT_YUAN).min(3.0) - 1.0).max(0.0) * 6.0;
    if ma20_extension_pct < 0.0 {
        score -= 6.0;
    }

    Some(PrestartCandidate {
        code: hits.first()?.code.clone(),
        name,
        tier,
        score: round2(score.max(0.0)),
        matched_signal_ids,
        matched_signal_names,
        reasons,
        gain_pct: round2(gain_pct),
        amount_yi: round2(today.amount / 100_000_000.0),
        range_15_pct: round2(range_15_pct),
        gap_to_high_pct: round2(gap_to_high_pct),
        ma20_extension_pct: round2(ma20_extension_pct),
    })
}

fn classify_tier(hits: &[SignalHit]) -> Option<PrestartTier> {
    let has_core = hits
        .iter()
        .any(|hit| CORE_SIGNAL_IDS.contains(&hit.signal_id.as_str()));
    let has_aux = hits
        .iter()
        .any(|hit| AUX_SIGNAL_IDS.contains(&hit.signal_id.as_str()));

    if hits.len() >= 3 {
        Some(PrestartTier::A)
    } else if has_core && has_aux {
        Some(PrestartTier::B)
    } else {
        None
    }
}

fn tier_reason(tier: PrestartTier, hits: &[SignalHit]) -> String {
    match tier {
        PrestartTier::A => format!("A档 {}/5 共振", hits.len()),
        PrestartTier::B => "B档 核心+辅助共振".to_string(),
    }
}

fn breakout_proximity_score(gap_to_high_pct: f64) -> f64 {
    if gap_to_high_pct >= 0.0 {
        triangular_score(gap_to_high_pct, 1.2, 0.0, MAX_GAP_TO_HIGH_PCT) * 14.0
    } else {
        triangular_score(-gap_to_high_pct, 0.3, 0.0, MAX_BREAKOUT_OVERSHOOT_PCT) * 8.0
    }
}

fn triangular_score(value: f64, center: f64, min: f64, max: f64) -> f64 {
    if value < min || value > max {
        return 0.0;
    }
    let width = (center - min).max(max - center).max(0.01);
    (1.0 - (value - center).abs() / width).max(0.0)
}

fn range_pct(window: &[Candle]) -> Option<f64> {
    let high = window
        .iter()
        .map(|b| b.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let low = window.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
    if !high.is_finite() || !low.is_finite() || low <= 0.0 {
        return None;
    }
    Some((high - low) / low * 100.0)
}

fn pct_change(base: f64, value: f64) -> f64 {
    if base.abs() < f64::EPSILON {
        0.0
    } else {
        (value - base) / base * 100.0
    }
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn candle(open: f64, high: f64, low: f64, close: f64, volume: i64, amount: f64) -> Candle {
        Candle {
            trade_date: NaiveDate::from_ymd_opt(2026, 3, 9).unwrap(),
            open,
            high,
            low,
            close,
            volume,
            amount,
            turnover: None,
            pe: None,
            pb: None,
        }
    }

    fn hit(signal_id: &str, signal_name: &str) -> SignalHit {
        SignalHit {
            code: "600001.SH".to_string(),
            name: "Alpha".to_string(),
            signal_id: signal_id.to_string(),
            signal_name: signal_name.to_string(),
            icon: "•".to_string(),
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn build_prestart_candidate_accepts_three_of_five_with_contraction() {
        let mut bars = Vec::new();
        for _ in 0..59 {
            bars.push(candle(10.0, 10.35, 9.95, 10.1, 900_000, 90_000_000.0));
        }
        bars.push(candle(10.12, 10.58, 10.08, 10.48, 1_300_000, 260_000_000.0));

        let candidate = build_prestart_candidate(
            &[
                hit("ma_bullish", "均线多头"),
                hit("volume_price", "量价配合"),
                hit("slow_bull", "缓慢牛"),
            ],
            &bars,
        )
        .unwrap();

        assert!(candidate.score > 60.0);
        assert_eq!(candidate.matched_signal_ids.len(), 3);
        assert_eq!(candidate.tier, PrestartTier::A);
    }

    #[test]
    fn build_prestart_candidate_accepts_core_plus_aux_as_b_tier() {
        let mut bars = Vec::new();
        for _ in 0..59 {
            bars.push(candle(10.0, 10.32, 9.96, 10.08, 920_000, 92_000_000.0));
        }
        bars.push(candle(10.1, 10.46, 10.05, 10.32, 1_100_000, 230_000_000.0));

        let candidate = build_prestart_candidate(
            &[
                hit("ma_bullish", "均线多头"),
                hit("volume_price", "量价配合"),
            ],
            &bars,
        )
        .unwrap();

        assert_eq!(candidate.tier, PrestartTier::B);
        assert!(candidate.score > 40.0);
    }

    #[test]
    fn build_prestart_candidate_rejects_aux_only_pair() {
        let mut bars = Vec::new();
        for _ in 0..59 {
            bars.push(candle(10.0, 10.32, 9.96, 10.08, 920_000, 92_000_000.0));
        }
        bars.push(candle(10.1, 10.46, 10.05, 10.32, 1_100_000, 230_000_000.0));

        assert!(build_prestart_candidate(
            &[
                hit("volume_price", "量价配合"),
                hit("small_bullish", "小阳蓄势"),
            ],
            &bars,
        )
        .is_none());
    }

    #[test]
    fn build_prestart_candidate_rejects_overheated_move() {
        let mut bars = Vec::new();
        for _ in 0..59 {
            bars.push(candle(10.0, 10.3, 9.9, 10.05, 1_000_000, 95_000_000.0));
        }
        bars.push(candle(10.2, 11.5, 10.15, 11.4, 2_000_000, 400_000_000.0));

        assert!(build_prestart_candidate(
            &[
                hit("ma_bullish", "均线多头"),
                hit("volume_price", "量价配合"),
                hit("triple_bullish", "三阳开泰"),
            ],
            &bars,
        )
        .is_none());
    }
}
