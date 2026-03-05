use std::sync::Arc;
use crate::state::AppState;

pub struct WatchlistService {
    pub state: Arc<AppState>,
}

impl WatchlistService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}
