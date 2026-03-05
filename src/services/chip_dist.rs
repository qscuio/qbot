use std::sync::Arc;
use crate::state::AppState;

pub struct ChipDistService {
    pub state: Arc<AppState>,
}

impl ChipDistService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}
