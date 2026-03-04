use std::collections::HashMap;
use crate::data::types::Candle;

/// Result of a signal detection
#[derive(Debug, Clone)]
pub struct SignalResult {
    pub triggered: bool,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl SignalResult {
    pub fn yes() -> Self {
        SignalResult { triggered: true, metadata: HashMap::new() }
    }

    pub fn no() -> Self {
        SignalResult { triggered: false, metadata: HashMap::new() }
    }

    pub fn with_meta(mut self, key: &str, value: serde_json::Value) -> Self {
        self.metadata.insert(key.to_string(), value);
        self
    }
}

/// Stock info passed to signal detectors
#[derive(Debug, Clone)]
pub struct StockContext {
    pub code: String,
    pub name: String,
}

/// All signals implement this trait
pub trait SignalDetector: Send + Sync {
    fn signal_id(&self) -> &'static str;
    fn display_name(&self) -> &'static str;
    fn icon(&self) -> &'static str;
    fn group(&self) -> &'static str;
    fn min_bars(&self) -> usize { 21 }
    fn priority(&self) -> i32 { 100 }
    fn enabled(&self) -> bool { true }
    fn count_in_multi(&self) -> bool { true }

    fn detect(&self, bars: &[Candle], ctx: &StockContext) -> SignalResult;
}

/// Helper: compute simple moving average
pub fn sma(values: &[f64], period: usize) -> Option<f64> {
    if values.len() < period { return None; }
    let sum: f64 = values[values.len()-period..].iter().sum();
    Some(sum / period as f64)
}

/// Helper: compute EMA (simplified)
pub fn ema(values: &[f64], period: usize) -> Option<f64> {
    if values.len() < period { return None; }
    let ema_val = values[values.len() - period..].iter().sum::<f64>() / period as f64;
    Some(ema_val)
}

/// Helper: average volume over last N bars
pub fn avg_volume(bars: &[Candle], n: usize) -> f64 {
    if bars.len() < n { return 0.0; }
    let sum: f64 = bars[bars.len()-n..].iter().map(|b| b.volume as f64).sum();
    sum / n as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sma() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(sma(&values, 3), Some(4.0));
        assert_eq!(sma(&values, 6), None);
    }
}
