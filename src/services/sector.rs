use chrono::NaiveDate;
use std::sync::Arc;
use tracing::info;

use crate::data::provider::DataProvider;
use crate::error::Result;
use crate::state::AppState;
use crate::storage::postgres;

pub struct SectorService {
    state: Arc<AppState>,
    provider: Arc<dyn DataProvider>,
}

impl SectorService {
    pub fn new(state: Arc<AppState>, provider: Arc<dyn DataProvider>) -> Self {
        SectorService { state, provider }
    }

    pub async fn fetch_and_save(&self, date: NaiveDate) -> Result<()> {
        let sectors = self.provider.get_sector_data(date).await?;
        info!("Sectors: {} records for {}", sectors.len(), date);
        postgres::save_sector_data(&self.state.db, &sectors).await?;
        Ok(())
    }

    pub async fn get_top_sectors(&self, date: NaiveDate, limit: i64) -> Result<Vec<SectorRank>> {
        let rows: Vec<(
            String,
            Option<String>,
            Option<String>,
            Option<f64>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT code, name, sector_type, change_pct::float8, amount::float8
                   FROM sector_daily WHERE trade_date = $1
                   ORDER BY change_pct DESC LIMIT $2"#,
        )
        .bind(date)
        .bind(limit)
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(code, name, sector_type, change_pct, amount)| SectorRank {
                code,
                name: name.unwrap_or_default(),
                sector_type: sector_type.unwrap_or_default(),
                change_pct: change_pct.unwrap_or(0.0),
                amount: amount.unwrap_or(0.0),
            })
            .collect())
    }

    pub async fn get_bottom_sectors(&self, date: NaiveDate, limit: i64) -> Result<Vec<SectorRank>> {
        let rows: Vec<(
            String,
            Option<String>,
            Option<String>,
            Option<f64>,
            Option<f64>,
        )> = sqlx::query_as(
            r#"SELECT code, name, sector_type, change_pct::float8, amount::float8
                   FROM sector_daily WHERE trade_date = $1
                   ORDER BY change_pct ASC LIMIT $2"#,
        )
        .bind(date)
        .bind(limit)
        .fetch_all(&self.state.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(code, name, sector_type, change_pct, amount)| SectorRank {
                code,
                name: name.unwrap_or_default(),
                sector_type: sector_type.unwrap_or_default(),
                change_pct: change_pct.unwrap_or(0.0),
                amount: amount.unwrap_or(0.0),
            })
            .collect())
    }
}

#[derive(Debug, serde::Serialize)]
pub struct SectorRank {
    pub code: String,
    pub name: String,
    pub sector_type: String,
    pub change_pct: f64,
    pub amount: f64,
}
