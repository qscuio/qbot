use std::sync::Arc;
use crate::state::AppState;

pub struct DabanSimService {
    pub state: Arc<AppState>,
}

impl DabanSimService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}
