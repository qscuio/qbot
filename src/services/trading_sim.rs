use std::sync::Arc;
use crate::state::AppState;

pub struct TradingSimService {
    pub state: Arc<AppState>,
}

impl TradingSimService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}
