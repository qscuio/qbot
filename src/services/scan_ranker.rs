use std::collections::HashMap;

use crate::data::types::Candle;
use crate::services::scanner::SignalHit;
use crate::signals::base::sma;

pub const POOL_SHORT_A_ID: &str = "pool_short_a";
pub const POOL_SHORT_B_ID: &str = "pool_short_b";
pub const POOL_MID_A_ID: &str = "pool_mid_a";
pub const POOL_MID_B_ID: &str = "pool_mid_b";
pub const POOL_LONG_A_ID: &str = "pool_long_a";
pub const POOL_LONG_B_ID: &str = "pool_long_b";

pub const RANKED_POOL_IDS: [&str; 6] = [
    POOL_SHORT_A_ID,
    POOL_SHORT_B_ID,
    POOL_MID_A_ID,
    POOL_MID_B_ID,
    POOL_LONG_A_ID,
    POOL_LONG_B_ID,
];

#[derive(Debug, Clone)]
pub struct RankInput {
    pub code: String,
    pub name: String,
    pub bars: Vec<Candle>,
    pub hits: Vec<SignalHit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineType {
    Short,
    Mid,
    Long,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolTier {
    A,
    B,
}

#[derive(Debug, Clone)]
struct RankedCandidate {
    code: String,
    name: String,
    line_type: LineType,
    tier: PoolTier,
    trigger_id: &'static str,
    trigger_name: &'static str,
    score: f64,
    reasons: Vec<String>,
    risk_flags: Vec<String>,
    factor_breakdown: Vec<(String, f64)>,
    supporting_signals: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct Metrics {
    today_close: f64,
    gain_pct: f64,
    close_pos: f64,
    vol_ratio_10: f64,
    vol_ratio_20: f64,
    avg20_amount: f64,
    ma10: f64,
    ma20: f64,
    ma60: f64,
    ma20_prev: f64,
    extension_ma20_pct: f64,
    breakout_10_pct: f64,
    breakout_20_pct: f64,
    recent_20_range_pct: f64,
    recent_10_range_pct: f64,
    dist_from_60_low_pct: f64,
}

pub fn ranked_pool_meta() -> [(&'static str, &'static str, &'static str); 6] {
    [
        (POOL_SHORT_A_ID, "短线A档", "🔥"),
        (POOL_SHORT_B_ID, "短线B档", "🟠"),
        (POOL_MID_A_ID, "中线A档", "📈"),
        (POOL_MID_B_ID, "中线B档", "🧭"),
        (POOL_LONG_A_ID, "长线A档", "🏛️"),
        (POOL_LONG_B_ID, "长线B档", "🌱"),
    ]
}

pub fn empty_ranked_pool_map() -> HashMap<String, Vec<SignalHit>> {
    let mut pools = HashMap::new();
    for pool_id in RANKED_POOL_IDS {
        pools.insert(pool_id.to_string(), Vec::new());
    }
    pools
}

pub fn rank_scan_inputs(inputs: &[RankInput]) -> HashMap<String, Vec<SignalHit>> {
    let mut best_by_code: HashMap<&str, RankedCandidate> = HashMap::new();

    for input in inputs {
        let Some(metrics) = compute_metrics(&input.bars) else {
            continue;
        };
        if is_hard_reject(input, metrics) {
            continue;
        }

        for candidate in classify_input(input, metrics) {
            match best_by_code.get(input.code.as_str()) {
                Some(current) if current.score >= candidate.score => {}
                _ => {
                    best_by_code.insert(input.code.as_str(), candidate);
                }
            }
        }
    }

    let mut buckets = empty_ranked_pool_map();
    for candidate in best_by_code.into_values() {
        let pool_id = pool_id(candidate.line_type, candidate.tier).to_string();
        buckets
            .entry(pool_id)
            .or_default()
            .push(candidate.into_signal_hit());
    }

    for pool_id in RANKED_POOL_IDS {
        if let Some(items) = buckets.get_mut(pool_id) {
            items.sort_by(|a, b| {
                score_from_hit(b)
                    .partial_cmp(&score_from_hit(a))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.code.cmp(&b.code))
            });
            items.truncate(pool_limit(pool_id));
        }
    }

    buckets
}

impl RankedCandidate {
    fn into_signal_hit(self) -> SignalHit {
        let pool_id = pool_id(self.line_type, self.tier);
        let (signal_name, icon) = ranked_pool_meta()
            .into_iter()
            .find(|(id, _, _)| *id == pool_id)
            .map(|(_, name, icon)| (name.to_string(), icon.to_string()))
            .unwrap_or_else(|| (pool_id.to_string(), "•".to_string()));

        SignalHit {
            code: self.code,
            name: self.name,
            signal_id: pool_id.to_string(),
            signal_name,
            icon,
            metadata: serde_json::json!({
                "line_type": line_name(self.line_type),
                "tier": tier_name(self.tier),
                "trigger_id": self.trigger_id,
                "trigger_name": self.trigger_name,
                "score": round2(self.score),
                "reasons": self.reasons,
                "risk_flags": self.risk_flags,
                "factor_breakdown": self.factor_breakdown.into_iter().map(|(name, value)| {
                    serde_json::json!({"name": name, "score": round2(value)})
                }).collect::<Vec<_>>(),
                "supporting_signals": self.supporting_signals,
            }),
        }
    }
}

fn classify_input(input: &RankInput, metrics: Metrics) -> Vec<RankedCandidate> {
    let mut candidates = Vec::new();
    if let Some(candidate) = score_short_strong_reclaim(input, metrics) {
        candidates.push(candidate);
    }
    if let Some(candidate) = score_short_platform_breakout(input, metrics) {
        candidates.push(candidate);
    }
    if let Some(candidate) = score_mid_trend_breakout(input, metrics) {
        candidates.push(candidate);
    }
    if let Some(candidate) = score_mid_pullback_resume(input, metrics) {
        candidates.push(candidate);
    }
    if let Some(candidate) = score_long_box_breakout(input, metrics) {
        candidates.push(candidate);
    }
    if let Some(candidate) = score_long_reversal_repair(input, metrics) {
        candidates.push(candidate);
    }
    candidates
}

fn score_short_strong_reclaim(input: &RankInput, metrics: Metrics) -> Option<RankedCandidate> {
    if !has_any_signal(
        &input.hits,
        &["strong_first_neg", "broken_board", "fanbao", "kuangbiao"],
    ) {
        return None;
    }
    if input.bars.len() < 25
        || metrics.gain_pct < 1.5
        || metrics.gain_pct > 9.5
        || metrics.close_pos < 0.68
        || metrics.vol_ratio_10 < 1.4
        || metrics.today_close < metrics.ma10
    {
        return None;
    }

    let mut reasons = vec!["强势票分歧后重新转强".to_string()];
    let mut factors = vec![
        ("结构基础".to_string(), 58.0),
        ("收盘质量".to_string(), metrics.close_pos * 10.0),
        (
            "量能恢复".to_string(),
            metrics.vol_ratio_10.clamp(0.0, 2.5) * 4.0,
        ),
        ("强度表现".to_string(), metrics.gain_pct.clamp(0.0, 8.0)),
    ];
    if metrics.breakout_10_pct > 0.0 {
        factors.push(("短压突破".to_string(), 6.0));
        reasons.push("收盘已突破短期压力".to_string());
    }
    let (risk_flags, penalties) = common_penalties(metrics);
    let score = factor_sum(&factors) - penalties.iter().map(|(_, v)| *v).sum::<f64>();
    let tier = tier_for(LineType::Short, score)?;

    Some(RankedCandidate {
        code: input.code.clone(),
        name: input.name.clone(),
        line_type: LineType::Short,
        tier,
        trigger_id: "short_strong_reclaim",
        trigger_name: "强势分歧转强",
        score,
        reasons,
        risk_flags,
        factor_breakdown: extend_factors(factors, penalties),
        supporting_signals: supporting_signal_ids(&input.hits),
    })
}

fn score_short_platform_breakout(input: &RankInput, metrics: Metrics) -> Option<RankedCandidate> {
    if !has_any_signal(&input.hits, &["breakout", "uptrend_breakout", "startup"]) {
        return None;
    }
    if input.bars.len() < 25
        || metrics.breakout_10_pct <= 0.0
        || metrics.recent_10_range_pct > 12.0
        || metrics.vol_ratio_20 < 1.15
        || metrics.close_pos < 0.62
    {
        return None;
    }

    let factors = vec![
        ("结构基础".to_string(), 54.0),
        (
            "平台突破".to_string(),
            metrics.breakout_10_pct.clamp(0.0, 6.0) * 1.8,
        ),
        (
            "量能共振".to_string(),
            metrics.vol_ratio_20.clamp(0.0, 2.5) * 4.5,
        ),
        ("收盘质量".to_string(), metrics.close_pos * 10.0),
    ];
    let reasons = vec!["强势整理后再次突破平台".to_string()];
    let (risk_flags, penalties) = common_penalties(metrics);
    let score = factor_sum(&factors) - penalties.iter().map(|(_, v)| *v).sum::<f64>();
    let tier = tier_for(LineType::Short, score)?;

    Some(RankedCandidate {
        code: input.code.clone(),
        name: input.name.clone(),
        line_type: LineType::Short,
        tier,
        trigger_id: "short_platform_breakout",
        trigger_name: "强势平台再突破",
        score,
        reasons,
        risk_flags,
        factor_breakdown: extend_factors(factors, penalties),
        supporting_signals: supporting_signal_ids(&input.hits),
    })
}

fn score_mid_trend_breakout(input: &RankInput, metrics: Metrics) -> Option<RankedCandidate> {
    if !has_any_signal(
        &input.hits,
        &["breakout", "uptrend_breakout", "startup", "ma_bullish"],
    ) {
        return None;
    }
    if input.bars.len() < 65
        || metrics.today_close < metrics.ma20
        || metrics.ma20 < metrics.ma60 * 0.98
        || metrics.breakout_20_pct <= 0.0
        || metrics.vol_ratio_20 < 1.1
        || metrics.close_pos < 0.55
    {
        return None;
    }

    let trend_bonus = if metrics.ma20_prev > 0.0 && metrics.ma20 > metrics.ma20_prev {
        8.0
    } else {
        3.0
    };
    let factors = vec![
        ("结构基础".to_string(), 56.0),
        ("趋势状态".to_string(), trend_bonus),
        (
            "突破质量".to_string(),
            metrics.breakout_20_pct.clamp(0.0, 8.0) * 1.6,
        ),
        (
            "量能配合".to_string(),
            metrics.vol_ratio_20.clamp(0.0, 2.5) * 4.0,
        ),
        ("收盘质量".to_string(), metrics.close_pos * 8.0),
    ];
    let reasons = vec!["中期趋势完好并放量突破".to_string()];
    let (risk_flags, penalties) = common_penalties(metrics);
    let score = factor_sum(&factors) - penalties.iter().map(|(_, v)| *v).sum::<f64>();
    let tier = tier_for(LineType::Mid, score)?;

    Some(RankedCandidate {
        code: input.code.clone(),
        name: input.name.clone(),
        line_type: LineType::Mid,
        tier,
        trigger_id: "mid_trend_breakout",
        trigger_name: "趋势突破加速",
        score,
        reasons,
        risk_flags,
        factor_breakdown: extend_factors(factors, penalties),
        supporting_signals: supporting_signal_ids(&input.hits),
    })
}

fn score_mid_pullback_resume(input: &RankInput, metrics: Metrics) -> Option<RankedCandidate> {
    if !has_any_signal(
        &input.hits,
        &[
            "ma_pullback",
            "strong_pullback",
            "ma_bullish",
            "volume_price",
        ],
    ) {
        return None;
    }
    let today = input.bars.last()?;
    if input.bars.len() < 40
        || metrics.today_close < metrics.ma20
        || today.low > metrics.ma10 * 1.03
        || metrics.gain_pct <= 0.5
        || metrics.vol_ratio_10 < 1.0
    {
        return None;
    }

    let factors = vec![
        ("结构基础".to_string(), 53.0),
        ("均线回踩".to_string(), 8.0),
        (
            "恢复力度".to_string(),
            metrics.gain_pct.clamp(0.0, 5.0) * 2.0,
        ),
        (
            "量能配合".to_string(),
            metrics.vol_ratio_10.clamp(0.0, 2.0) * 4.5,
        ),
        ("收盘质量".to_string(), metrics.close_pos * 8.0),
    ];
    let reasons = vec!["趋势内回踩后重新转强".to_string()];
    let (risk_flags, penalties) = common_penalties(metrics);
    let score = factor_sum(&factors) - penalties.iter().map(|(_, v)| *v).sum::<f64>();
    let tier = tier_for(LineType::Mid, score)?;

    Some(RankedCandidate {
        code: input.code.clone(),
        name: input.name.clone(),
        line_type: LineType::Mid,
        tier,
        trigger_id: "mid_pullback_resume",
        trigger_name: "强趋势回踩再起",
        score,
        reasons,
        risk_flags,
        factor_breakdown: extend_factors(factors, penalties),
        supporting_signals: supporting_signal_ids(&input.hits),
    })
}

fn score_long_box_breakout(input: &RankInput, metrics: Metrics) -> Option<RankedCandidate> {
    if !has_any_signal(
        &input.hits,
        &[
            "low_accumulation",
            "bottom_quick_start",
            "weekly_monthly_bullish",
        ],
    ) {
        return None;
    }
    if input.bars.len() < 70
        || metrics.breakout_20_pct <= 0.0
        || metrics.recent_20_range_pct > 18.0
        || metrics.dist_from_60_low_pct > 35.0
        || metrics.vol_ratio_20 < 1.05
    {
        return None;
    }

    let factors = vec![
        ("结构基础".to_string(), 52.0),
        ("底部箱体".to_string(), 9.0),
        (
            "脱离底部".to_string(),
            (35.0 - metrics.dist_from_60_low_pct).max(0.0) * 0.25,
        ),
        (
            "量能恢复".to_string(),
            metrics.vol_ratio_20.clamp(0.0, 2.0) * 4.0,
        ),
        ("收盘质量".to_string(), metrics.close_pos * 7.0),
    ];
    let reasons = vec!["底部整理后放量脱离箱体".to_string()];
    let (risk_flags, penalties) = common_penalties(metrics);
    let score = factor_sum(&factors) - penalties.iter().map(|(_, v)| *v).sum::<f64>();
    let tier = tier_for(LineType::Long, score)?;

    Some(RankedCandidate {
        code: input.code.clone(),
        name: input.name.clone(),
        line_type: LineType::Long,
        tier,
        trigger_id: "long_box_breakout",
        trigger_name: "底部箱体脱离",
        score,
        reasons,
        risk_flags,
        factor_breakdown: extend_factors(factors, penalties),
        supporting_signals: supporting_signal_ids(&input.hits),
    })
}

fn score_long_reversal_repair(input: &RankInput, metrics: Metrics) -> Option<RankedCandidate> {
    if !has_any_signal(
        &input.hits,
        &[
            "downtrend_reversal",
            "long_cycle_reversal",
            "bottom_quick_start",
        ],
    ) {
        return None;
    }
    if input.bars.len() < 70
        || metrics.today_close < metrics.ma20
        || metrics.ma20 < metrics.ma20_prev * 0.99
        || metrics.dist_from_60_low_pct > 28.0
        || metrics.gain_pct < 0.5
    {
        return None;
    }

    let factors = vec![
        ("结构基础".to_string(), 52.0),
        ("均线修复".to_string(), 8.0),
        (
            "位置优势".to_string(),
            (28.0 - metrics.dist_from_60_low_pct).max(0.0) * 0.3,
        ),
        (
            "量能改善".to_string(),
            metrics.vol_ratio_20.clamp(0.0, 2.0) * 3.5,
        ),
        ("收盘质量".to_string(), metrics.close_pos * 7.0),
    ];
    let reasons = vec!["长期下跌后进入右侧修复阶段".to_string()];
    let (risk_flags, penalties) = common_penalties(metrics);
    let score = factor_sum(&factors) - penalties.iter().map(|(_, v)| *v).sum::<f64>();
    let tier = tier_for(LineType::Long, score)?;

    Some(RankedCandidate {
        code: input.code.clone(),
        name: input.name.clone(),
        line_type: LineType::Long,
        tier,
        trigger_id: "long_reversal_repair",
        trigger_name: "均线修复反转",
        score,
        reasons,
        risk_flags,
        factor_breakdown: extend_factors(factors, penalties),
        supporting_signals: supporting_signal_ids(&input.hits),
    })
}

fn compute_metrics(bars: &[Candle]) -> Option<Metrics> {
    if bars.len() < 25 {
        return None;
    }
    let n = bars.len();
    let today = bars.last()?;
    let prev = bars.get(n.saturating_sub(2))?;
    if prev.close <= 0.0 || today.close <= 0.0 || today.volume <= 0 {
        return None;
    }

    let closes: Vec<f64> = bars.iter().map(|bar| bar.close).collect();
    let ma10 = sma(&closes, 10).unwrap_or(today.close);
    let ma20 = sma(&closes, 20).unwrap_or(today.close);
    let ma60 = sma(&closes, 60).unwrap_or(ma20);
    let ma20_prev = if n > 20 {
        sma(&closes[..n - 1], 20).unwrap_or(ma20)
    } else {
        ma20
    };

    let avg10_vol = average_volume(&bars[..n - 1], 10).max(1.0);
    let avg20_vol = average_volume(&bars[..n - 1], 20).max(1.0);
    let avg20_amount = average_amount(&bars[..n - 1], 20);
    let recent_high10 = highest_high(&bars[n.saturating_sub(11)..n - 1]);
    let recent_high20 = highest_high(&bars[n.saturating_sub(21)..n - 1]);
    let recent_window20 = &bars[n.saturating_sub(21)..n - 1];
    let recent_window10 = &bars[n.saturating_sub(11)..n - 1];
    let low60_window = &bars[n.saturating_sub(61)..n - 1];
    let low60 = lowest_low(low60_window);
    let close_pos = if today.high > today.low {
        (today.close - today.low) / (today.high - today.low)
    } else {
        0.5
    };

    Some(Metrics {
        today_close: today.close,
        gain_pct: pct_change(prev.close, today.close),
        close_pos,
        vol_ratio_10: today.volume as f64 / avg10_vol,
        vol_ratio_20: today.volume as f64 / avg20_vol,
        avg20_amount,
        ma10,
        ma20,
        ma60,
        ma20_prev,
        extension_ma20_pct: pct_change(ma20.max(0.01), today.close),
        breakout_10_pct: pct_change(recent_high10.max(0.01), today.close),
        breakout_20_pct: pct_change(recent_high20.max(0.01), today.close),
        recent_20_range_pct: range_pct(recent_window20),
        recent_10_range_pct: range_pct(recent_window10),
        dist_from_60_low_pct: pct_change(low60.max(0.01), today.close),
    })
}

fn common_penalties(metrics: Metrics) -> (Vec<String>, Vec<(String, f64)>) {
    let mut risk_flags = Vec::new();
    let mut penalties = Vec::new();

    if metrics.today_close < 3.0 {
        risk_flags.push("低价股".to_string());
        penalties.push(("低价惩罚".to_string(), 12.0));
    } else if metrics.today_close < 5.0 {
        risk_flags.push("价格偏低".to_string());
        penalties.push(("低价惩罚".to_string(), 6.0));
    }

    if metrics.avg20_amount < 100_000_000.0 {
        risk_flags.push("流动性偏弱".to_string());
        penalties.push(("流动性惩罚".to_string(), 10.0));
    } else if metrics.avg20_amount < 200_000_000.0 {
        risk_flags.push("成交额一般".to_string());
        penalties.push(("流动性惩罚".to_string(), 5.0));
    }

    if metrics.extension_ma20_pct > 14.0 {
        risk_flags.push("偏离MA20过大".to_string());
        penalties.push(("位置过热".to_string(), 10.0));
    } else if metrics.extension_ma20_pct > 10.0 {
        risk_flags.push("偏离均线偏大".to_string());
        penalties.push(("位置过热".to_string(), 5.0));
    }

    if metrics.close_pos < 0.45 {
        risk_flags.push("收盘强度不足".to_string());
        penalties.push(("收盘弱势".to_string(), 6.0));
    }

    (risk_flags, penalties)
}

fn is_hard_reject(input: &RankInput, metrics: Metrics) -> bool {
    let name = input.name.trim().to_ascii_uppercase();
    name.contains("ST")
        || input.name.contains('退')
        || metrics.today_close <= 0.0
        || input.hits.is_empty()
}

fn has_any_signal(hits: &[SignalHit], signal_ids: &[&str]) -> bool {
    hits.iter().any(|hit| {
        signal_ids
            .iter()
            .any(|signal_id| hit.signal_id == *signal_id)
    })
}

fn supporting_signal_ids(hits: &[SignalHit]) -> Vec<String> {
    hits.iter().map(|hit| hit.signal_id.clone()).collect()
}

fn average_volume(bars: &[Candle], days: usize) -> f64 {
    if bars.is_empty() {
        return 0.0;
    }
    let start = bars.len().saturating_sub(days);
    let window = &bars[start..];
    window.iter().map(|bar| bar.volume as f64).sum::<f64>() / window.len() as f64
}

fn average_amount(bars: &[Candle], days: usize) -> f64 {
    if bars.is_empty() {
        return 0.0;
    }
    let start = bars.len().saturating_sub(days);
    let window = &bars[start..];
    window.iter().map(|bar| bar.amount).sum::<f64>() / window.len() as f64
}

fn highest_high(bars: &[Candle]) -> f64 {
    bars.iter()
        .map(|bar| bar.high)
        .fold(f64::NEG_INFINITY, f64::max)
}

fn lowest_low(bars: &[Candle]) -> f64 {
    bars.iter().map(|bar| bar.low).fold(f64::INFINITY, f64::min)
}

fn range_pct(bars: &[Candle]) -> f64 {
    let high = highest_high(bars);
    let low = lowest_low(bars);
    if !high.is_finite() || !low.is_finite() || low <= 0.0 {
        return 0.0;
    }
    (high - low) / low * 100.0
}

fn pool_id(line_type: LineType, tier: PoolTier) -> &'static str {
    match (line_type, tier) {
        (LineType::Short, PoolTier::A) => POOL_SHORT_A_ID,
        (LineType::Short, PoolTier::B) => POOL_SHORT_B_ID,
        (LineType::Mid, PoolTier::A) => POOL_MID_A_ID,
        (LineType::Mid, PoolTier::B) => POOL_MID_B_ID,
        (LineType::Long, PoolTier::A) => POOL_LONG_A_ID,
        (LineType::Long, PoolTier::B) => POOL_LONG_B_ID,
    }
}

fn pool_limit(pool_id: &str) -> usize {
    match pool_id {
        POOL_SHORT_A_ID => 3,
        POOL_SHORT_B_ID => 8,
        POOL_MID_A_ID => 5,
        POOL_MID_B_ID => 12,
        POOL_LONG_A_ID => 5,
        POOL_LONG_B_ID => 12,
        _ => 20,
    }
}

fn tier_for(line_type: LineType, score: f64) -> Option<PoolTier> {
    let rounded = round2(score);
    match line_type {
        LineType::Short if rounded >= 78.0 => Some(PoolTier::A),
        LineType::Short if rounded >= 65.0 => Some(PoolTier::B),
        LineType::Mid if rounded >= 76.0 => Some(PoolTier::A),
        LineType::Mid if rounded >= 62.0 => Some(PoolTier::B),
        LineType::Long if rounded >= 74.0 => Some(PoolTier::A),
        LineType::Long if rounded >= 60.0 => Some(PoolTier::B),
        _ => None,
    }
}

fn factor_sum(factors: &[(String, f64)]) -> f64 {
    factors.iter().map(|(_, value)| *value).sum()
}

fn extend_factors(
    mut factors: Vec<(String, f64)>,
    penalties: Vec<(String, f64)>,
) -> Vec<(String, f64)> {
    for (name, value) in penalties {
        factors.push((name, -value));
    }
    factors
}

fn score_from_hit(hit: &SignalHit) -> f64 {
    hit.metadata
        .get("score")
        .and_then(|value| value.as_f64())
        .unwrap_or(0.0)
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

fn line_name(line_type: LineType) -> &'static str {
    match line_type {
        LineType::Short => "short",
        LineType::Mid => "mid",
        LineType::Long => "long",
    }
}

fn tier_name(tier: PoolTier) -> &'static str {
    match tier {
        PoolTier::A => "A",
        PoolTier::B => "B",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn candle(open: f64, high: f64, low: f64, close: f64, volume: i64, amount: f64) -> Candle {
        Candle {
            trade_date: NaiveDate::from_ymd_opt(2026, 4, 22).unwrap(),
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

    fn hit(code: &str, name: &str, signal_id: &str, signal_name: &str) -> SignalHit {
        SignalHit {
            code: code.to_string(),
            name: name.to_string(),
            signal_id: signal_id.to_string(),
            signal_name: signal_name.to_string(),
            icon: "•".to_string(),
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn rank_scan_inputs_emits_short_a_pool_for_strong_reclaim_candidate() {
        let mut bars = Vec::new();
        for _ in 0..35 {
            bars.push(candle(10.0, 10.3, 9.9, 10.1, 1_000_000, 120_000_000.0));
        }
        bars.pop();
        bars.push(candle(10.2, 11.0, 10.1, 10.95, 2_800_000, 360_000_000.0));

        let pools = rank_scan_inputs(&[RankInput {
            code: "600000.SH".to_string(),
            name: "浦发银行".to_string(),
            bars,
            hits: vec![
                hit("600000.SH", "浦发银行", "strong_first_neg", "强势首阴"),
                hit("600000.SH", "浦发银行", "fanbao", "反包"),
            ],
        }]);

        assert!(!pools[POOL_SHORT_A_ID].is_empty());
    }

    #[test]
    fn rank_scan_inputs_emits_mid_pool_for_trend_breakout_candidate() {
        let mut bars = Vec::new();
        for i in 0..70 {
            let close = 8.0 + i as f64 * 0.05;
            bars.push(candle(
                close - 0.08,
                close + 0.10,
                close - 0.12,
                close,
                1_200_000,
                220_000_000.0,
            ));
        }
        bars.pop();
        bars.push(candle(11.4, 11.9, 11.3, 11.85, 2_400_000, 420_000_000.0));

        let pools = rank_scan_inputs(&[RankInput {
            code: "300001.SZ".to_string(),
            name: "特锐德".to_string(),
            bars,
            hits: vec![
                hit("300001.SZ", "特锐德", "breakout", "突破信号"),
                hit("300001.SZ", "特锐德", "ma_bullish", "均线多头"),
            ],
        }]);

        assert!(
            !pools[POOL_MID_A_ID].is_empty() || !pools[POOL_MID_B_ID].is_empty(),
            "expected at least one mid-tier pool hit"
        );
    }

    #[test]
    fn rank_scan_inputs_rejects_non_tradable_names() {
        let mut bars = Vec::new();
        for _ in 0..70 {
            bars.push(candle(5.0, 5.2, 4.9, 5.1, 2_000_000, 180_000_000.0));
        }

        let pools = rank_scan_inputs(&[RankInput {
            code: "300344.SZ".to_string(),
            name: "立方退".to_string(),
            bars,
            hits: vec![hit("300344.SZ", "立方退", "breakout", "突破信号")],
        }]);

        for pool_id in RANKED_POOL_IDS {
            assert!(pools[pool_id].is_empty(), "pool {pool_id} should be empty");
        }
    }
}
