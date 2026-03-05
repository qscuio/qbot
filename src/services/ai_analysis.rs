use std::sync::Arc;
use crate::state::AppState;

pub struct AiAnalysisService {
    pub state: Arc<AppState>,
}

impl AiAnalysisService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}
