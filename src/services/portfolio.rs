use std::sync::Arc;
use crate::state::AppState;

pub struct PortfolioService {
    pub state: Arc<AppState>,
}

impl PortfolioService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}
