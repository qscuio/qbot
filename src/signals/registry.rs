use super::base::SignalDetector;
use std::sync::OnceLock;

static REGISTRY: OnceLock<Vec<Box<dyn SignalDetector>>> = OnceLock::new();

pub struct SignalRegistry;

impl SignalRegistry {
    /// Initialize registry with all signals
    pub fn init() -> &'static Vec<Box<dyn SignalDetector>> {
        REGISTRY.get_or_init(|| {
            use super::board::*;
            use super::comprehensive::*;
            use super::momentum::*;
            use super::pattern::*;
            use super::trend::*;
            use super::volume::*;

            let signals: Vec<Box<dyn SignalDetector>> = vec![
                // Volume
                Box::new(VolumeSurgeSignal),
                Box::new(VolumePriceSignal),
                // Trend
                Box::new(MaBullishSignal),
                Box::new(MaPullbackSignal),
                Box::new(StrongPullbackSignal),
                Box::new(UptrendBreakoutSignal),
                Box::new(DowntrendReversalSignal),
                Box::new(LinRegSignal),
                // Pattern
                Box::new(SlowBullSignal),
                Box::new(SmallBullishSignal),
                Box::new(TripleBullishSignal),
                Box::new(FanbaoSignal),
                Box::new(WeeklyMonthlyBullishSignal),
                // Board
                Box::new(BrokenBoardSignal),
                Box::new(StrongFirstNegSignal),
                // Momentum
                Box::new(BreakoutSignal),
                Box::new(StartupSignal),
                Box::new(KuangbiaoSignal),
                // Comprehensive
                Box::new(BottomQuickStartSignal),
                Box::new(LongCycleReversalSignal),
                Box::new(LowAccumulationSignal),
            ];

            tracing::info!("Registered {} signals", signals.len());
            signals
        })
    }

    pub fn get_enabled() -> Vec<&'static dyn SignalDetector> {
        Self::init()
            .iter()
            .filter(|s| s.enabled())
            .map(|s| s.as_ref())
            .collect()
    }

    pub fn get_all() -> &'static Vec<Box<dyn SignalDetector>> {
        Self::init()
    }
}
