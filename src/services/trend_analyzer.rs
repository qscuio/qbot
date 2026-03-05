use crate::data::types::Candle;
use crate::signals::base::sma;

#[derive(Debug, Clone, serde::Serialize)]
pub enum TrendStatus {
    StrongBull,    // 强势多头
    Bull,          // 多头排列
    WeakBull,      // 弱势多头
    Consolidation, // 盘整
    WeakBear,      // 弱势空头
    Bear,          // 空头排列
    StrongBear,    // 强势空头
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum BuySignal {
    StrongBuy,  // 强烈买入
    Buy,        // 买入
    Hold,       // 持有
    Wait,       // 观望
    Sell,       // 卖出
    StrongSell, // 强烈卖出
}

#[derive(Debug, serde::Serialize)]
pub struct TrendAnalysis {
    pub code: String,
    pub trend_status: TrendStatus,
    pub buy_signal: BuySignal,
    pub score: f64,
    pub ma5: f64,
    pub ma10: f64,
    pub ma20: f64,
    pub ma60: f64,
    pub price: f64,
    pub bias_ma20: f64,
}

pub struct TrendAnalyzer;

impl TrendAnalyzer {
    pub fn analyze(code: &str, bars: &[Candle]) -> Option<TrendAnalysis> {
        if bars.len() < 61 {
            return None;
        }
        let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
        let price = *closes.last()?;
        let ma5 = sma(&closes, 5)?;
        let ma10 = sma(&closes, 10)?;
        let ma20 = sma(&closes, 20)?;
        let ma60 = sma(&closes, 60)?;

        let trend_status = if ma5 > ma10 && ma10 > ma20 && ma20 > ma60 {
            let spread = (ma5 - ma60) / ma60 * 100.0;
            if spread > 5.0 { TrendStatus::StrongBull } else { TrendStatus::Bull }
        } else if ma5 > ma10 {
            TrendStatus::WeakBull
        } else if (ma5 - ma10).abs() / ma10 * 100.0 < 1.0 {
            TrendStatus::Consolidation
        } else if ma5 < ma10 && ma10 > ma20 {
            TrendStatus::WeakBear
        } else if ma5 < ma10 && ma10 < ma20 && ma20 < ma60 {
            TrendStatus::StrongBear
        } else {
            TrendStatus::Bear
        };

        let bias_ma20 = (price - ma20) / ma20 * 100.0;

        let score = {
            let mut s = 50.0f64;
            if ma5 > ma10 { s += 10.0; }
            if ma10 > ma20 { s += 10.0; }
            if ma20 > ma60 { s += 10.0; }
            if price > ma5 { s += 5.0; }
            if bias_ma20 > 0.0 && bias_ma20 < 10.0 { s += 5.0; }
            if bias_ma20 < 0.0 { s -= 10.0; }
            s.clamp(0.0, 100.0)
        };

        let buy_signal = if score >= 85.0 { BuySignal::StrongBuy }
            else if score >= 70.0 { BuySignal::Buy }
            else if score >= 55.0 { BuySignal::Hold }
            else if score >= 40.0 { BuySignal::Wait }
            else if score >= 25.0 { BuySignal::Sell }
            else { BuySignal::StrongSell };

        Some(TrendAnalysis {
            code: code.to_string(),
            trend_status,
            buy_signal,
            score,
            ma5,
            ma10,
            ma20,
            ma60,
            price,
            bias_ma20,
        })
    }
}
