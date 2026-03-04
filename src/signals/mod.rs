pub mod base;
pub mod board;
pub mod comprehensive;
pub mod momentum;
pub mod pattern;
pub mod registry;
pub mod trend;
pub mod volume;

pub use base::{SignalDetector, SignalResult, StockContext};
pub use registry::SignalRegistry;
