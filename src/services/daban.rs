use std::sync::Arc;
use crate::data::types::LimitUpStock;
use crate::state::AppState;

#[derive(Debug, Clone, serde::Serialize)]
pub struct DabanScore {
    pub code: String,
    pub name: String,
    pub score: f64,
    pub seal_score: f64,
    pub time_score: f64,
    pub burst_penalty: f64,
    pub executability: String,
    pub verdict: String,
}

pub struct DabanService {
    pub state: Arc<AppState>,
}

impl DabanService {
    pub fn new(state: Arc<AppState>) -> Self {
        DabanService { state }
    }

    pub fn score_stock(&self, stock: &LimitUpStock) -> DabanScore {
        // Seal strength score (0-30): fd_amount / 1M normalized
        let seal_score = (stock.fd_amount / 1_000_000.0).min(30.0);

        // Timing score (0-25): earlier limit-up is better
        let time_score = if let Some(ref t) = stock.first_time {
            let hour: u32 = t.split(':').next().and_then(|h| h.parse().ok()).unwrap_or(15);
            let min: u32 = t.split(':').nth(1).and_then(|m| m.parse().ok()).unwrap_or(0);
            let minutes_since_open = (hour * 60 + min).saturating_sub(9 * 60 + 30);
            (25.0 - (minutes_since_open as f64 / 6.0 * 25.0 / 60.0)).max(0.0)
        } else {
            5.0
        };

        // Burst penalty
        let burst_penalty = (stock.open_times as f64) * 5.0;

        let raw_score = seal_score + time_score - burst_penalty;
        let score = raw_score.clamp(0.0, 100.0);

        let executability = if stock.open_times == 0 && stock.pct_chg >= 9.8 {
            "一字板".to_string()
        } else if stock.open_times > 2 {
            "多次炸板".to_string()
        } else if score >= 60.0 {
            "可打".to_string()
        } else {
            "观望".to_string()
        };

        let verdict = if score >= 80.0 { "强烈推荐" }
            else if score >= 60.0 { "推荐" }
            else if score >= 40.0 { "观望" }
            else { "回避" }.to_string();

        DabanScore {
            code: stock.code.clone(),
            name: stock.name.clone(),
            score,
            seal_score,
            time_score,
            burst_penalty,
            executability,
            verdict,
        }
    }

    pub fn score_all(&self, stocks: &[LimitUpStock]) -> Vec<DabanScore> {
        let mut scores: Vec<DabanScore> = stocks.iter().map(|s| self.score_stock(s)).collect();
        scores.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        scores
    }
}
