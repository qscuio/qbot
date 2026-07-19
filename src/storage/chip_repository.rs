use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgPool};

use crate::data::chip::{
    ChipBucket, ChipModelState, ChipSnapshot, ChipSourceDecision, ChipValidationRun,
};
use crate::error::{AppError, Result};

const NORMALIZED_TOLERANCE: f64 = 1e-9;

#[derive(Debug, FromRow)]
struct SnapshotRow {
    code: String,
    trade_date: NaiveDate,
    distribution: Value,
    avg_cost: Option<f64>,
    profit_ratio: Option<f64>,
    concentration: Option<f64>,
    dominant_peak_price: Option<f64>,
    source: String,
    model_version: Option<String>,
    validated: bool,
    source_updated_at: DateTime<Utc>,
    distribution_format: String,
}

#[derive(Clone)]
pub struct ChipRepository {
    pool: PgPool,
}

impl ChipRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_snapshot(&self, snapshot: &ChipSnapshot) -> Result<bool> {
        validate_snapshot(snapshot)?;
        let distribution = serde_json::to_value(&snapshot.distribution)?;
        let result = sqlx::query(
            r#"INSERT INTO chip_distribution
               (code, trade_date, distribution, avg_cost, profit_ratio, concentration,
                dominant_peak_price, source, model_version, validated,
                source_updated_at, distribution_format, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                       NOW(), 'normalized_probability', NOW())
               ON CONFLICT (code, trade_date) DO UPDATE SET
                 distribution = EXCLUDED.distribution,
                 avg_cost = EXCLUDED.avg_cost,
                 profit_ratio = EXCLUDED.profit_ratio,
                 concentration = EXCLUDED.concentration,
                 dominant_peak_price = EXCLUDED.dominant_peak_price,
                 source = EXCLUDED.source,
                 model_version = EXCLUDED.model_version,
                 validated = EXCLUDED.validated,
                 source_updated_at = NOW(),
                 distribution_format = EXCLUDED.distribution_format,
                 updated_at = NOW()
               WHERE (chip_distribution.distribution,
                      chip_distribution.avg_cost,
                      chip_distribution.profit_ratio,
                      chip_distribution.concentration,
                      chip_distribution.dominant_peak_price,
                      chip_distribution.source,
                      chip_distribution.model_version,
                      chip_distribution.validated,
                      chip_distribution.distribution_format)
                     IS DISTINCT FROM
                     (EXCLUDED.distribution,
                      EXCLUDED.avg_cost,
                      EXCLUDED.profit_ratio,
                      EXCLUDED.concentration,
                      EXCLUDED.dominant_peak_price,
                      EXCLUDED.source,
                      EXCLUDED.model_version,
                      EXCLUDED.validated,
                      EXCLUDED.distribution_format)"#,
        )
        .bind(&snapshot.code)
        .bind(snapshot.trade_date)
        .bind(distribution)
        .bind(snapshot.average_cost)
        .bind(snapshot.winner_rate)
        .bind(snapshot.concentration)
        .bind(snapshot.dominant_peak_price)
        .bind(&snapshot.source)
        .bind(&snapshot.model_version)
        .bind(snapshot.validated)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn snapshot_at_or_before(
        &self,
        code: &str,
        requested_date: NaiveDate,
    ) -> Result<Option<ChipSnapshot>> {
        let row = sqlx::query_as::<_, SnapshotRow>(
            r#"SELECT code, trade_date, distribution,
                      avg_cost::float8 AS avg_cost,
                      profit_ratio::float8 AS profit_ratio,
                      concentration::float8 AS concentration,
                      dominant_peak_price::float8 AS dominant_peak_price,
                      source, model_version, validated, source_updated_at,
                      distribution_format
               FROM chip_distribution
               WHERE code = $1 AND trade_date <= $2
               ORDER BY trade_date DESC
               LIMIT 1"#,
        )
        .bind(code)
        .bind(requested_date)
        .fetch_optional(&self.pool)
        .await?;
        row.map(snapshot_from_row).transpose()
    }

    pub async fn latest_snapshot(&self, code: &str) -> Result<Option<ChipSnapshot>> {
        let row = sqlx::query_as::<_, SnapshotRow>(
            r#"SELECT code, trade_date, distribution,
                      avg_cost::float8 AS avg_cost,
                      profit_ratio::float8 AS profit_ratio,
                      concentration::float8 AS concentration,
                      dominant_peak_price::float8 AS dominant_peak_price,
                      source, model_version, validated, source_updated_at,
                      distribution_format
               FROM chip_distribution
               WHERE code = $1
               ORDER BY trade_date DESC
               LIMIT 1"#,
        )
        .bind(code)
        .fetch_optional(&self.pool)
        .await?;
        row.map(snapshot_from_row).transpose()
    }

    pub async fn save_model_state(&self, state: &ChipModelState) -> Result<bool> {
        if state.code.trim().is_empty() {
            return Err(AppError::BadRequest(
                "chip model state code is empty".to_string(),
            ));
        }
        if state.model_version.trim().is_empty() {
            return Err(AppError::BadRequest(
                "chip model state version is empty".to_string(),
            ));
        }
        if !state.last_adjustment_factor.is_finite() || state.last_adjustment_factor <= 0.0 {
            return Err(AppError::BadRequest(
                "chip model state adjustment factor must be finite and positive".to_string(),
            ));
        }
        validate_buckets(&state.distribution, true)?;
        let result = sqlx::query(
            r#"INSERT INTO chip_model_states
               (code, model_version, through_date, distribution,
                last_adjustment_factor, updated_at)
               VALUES ($1, $2, $3, $4, $5, NOW())
               ON CONFLICT (code, model_version) DO UPDATE SET
                 through_date = EXCLUDED.through_date,
                 distribution = EXCLUDED.distribution,
                 last_adjustment_factor = EXCLUDED.last_adjustment_factor,
                 updated_at = NOW()
               WHERE chip_model_states.through_date < EXCLUDED.through_date"#,
        )
        .bind(&state.code)
        .bind(&state.model_version)
        .bind(state.through_date)
        .bind(serde_json::to_value(&state.distribution)?)
        .bind(state.last_adjustment_factor)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn save_validation_run(&self, run: &ChipValidationRun) -> Result<bool> {
        let result = sqlx::query(
            r#"INSERT INTO chip_model_validation_runs
               (run_id, model_version, sample_definition, aggregate_metrics,
                subgroup_metrics, decision, started_at, completed_at, error_summary,
                recorded_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
               ON CONFLICT (run_id) DO NOTHING"#,
        )
        .bind(run.run_id)
        .bind(&run.model_version)
        .bind(&run.sample_definition)
        .bind(&run.aggregate_metrics)
        .bind(&run.subgroup_metrics)
        .bind(run.decision.map(ChipSourceDecision::as_str))
        .bind(run.started_at)
        .bind(run.completed_at)
        .bind(&run.error_summary)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn latest_validation_decision(
        &self,
        model_version: &str,
    ) -> Result<Option<ChipSourceDecision>> {
        let decision: Option<String> = sqlx::query_scalar(
            r#"SELECT decision
               FROM chip_model_validation_runs
               WHERE model_version = $1
                 AND completed_at IS NOT NULL
                 AND decision IS NOT NULL
                 AND error_summary IS NULL
               ORDER BY recorded_at DESC, run_id DESC
               LIMIT 1"#,
        )
        .bind(model_version)
        .fetch_optional(&self.pool)
        .await?;
        decision
            .map(|value| {
                ChipSourceDecision::from_storage(&value).ok_or_else(|| {
                    AppError::Internal(format!("unknown stored chip source decision: {value}"))
                })
            })
            .transpose()
    }
}

fn validate_snapshot(snapshot: &ChipSnapshot) -> Result<()> {
    if snapshot.code.trim().is_empty() {
        return Err(AppError::BadRequest(
            "chip snapshot code is empty".to_string(),
        ));
    }
    if !matches!(snapshot.source.as_str(), "qbot_estimate" | "tushare") {
        return Err(AppError::BadRequest(format!(
            "unsupported chip snapshot source: {}",
            snapshot.source
        )));
    }
    match snapshot.source.as_str() {
        "qbot_estimate"
            if snapshot
                .model_version
                .as_deref()
                .is_none_or(|version| version.trim().is_empty()) =>
        {
            return Err(AppError::BadRequest(
                "estimated chip snapshot requires a model version".to_string(),
            ));
        }
        "tushare" if snapshot.model_version.is_some() => {
            return Err(AppError::BadRequest(
                "official chip snapshot cannot carry an estimator model version".to_string(),
            ));
        }
        _ => {}
    }
    for (name, value) in [
        ("average cost", snapshot.average_cost),
        ("dominant peak price", snapshot.dominant_peak_price),
    ] {
        if !value.is_finite() || value <= 0.0 {
            return Err(AppError::BadRequest(format!(
                "chip snapshot {name} must be finite and positive"
            )));
        }
    }
    for (name, value) in [
        ("winner rate", snapshot.winner_rate),
        ("concentration", snapshot.concentration),
    ] {
        if !value.is_finite() || !(0.0..=100.0).contains(&value) {
            return Err(AppError::BadRequest(format!(
                "chip snapshot {name} must be between 0 and 100"
            )));
        }
    }
    validate_buckets(&snapshot.distribution, true)
}

fn validate_buckets(buckets: &[ChipBucket], require_normalized: bool) -> Result<()> {
    if buckets.is_empty() {
        return Err(AppError::BadRequest(
            "chip bucket distribution is empty".to_string(),
        ));
    }
    let mut sum = 0.0;
    for bucket in buckets {
        if !bucket.price.is_finite() || bucket.price <= 0.0 {
            return Err(AppError::BadRequest(
                "chip bucket price must be finite and positive".to_string(),
            ));
        }
        if !bucket.weight.is_finite() || bucket.weight < 0.0 {
            return Err(AppError::BadRequest(
                "chip bucket weight must be finite and non-negative".to_string(),
            ));
        }
        sum += bucket.weight;
    }
    if require_normalized && (!sum.is_finite() || (sum - 1.0).abs() > NORMALIZED_TOLERANCE) {
        return Err(AppError::BadRequest(format!(
            "chip bucket weights must sum to 1 (got {sum})"
        )));
    }
    Ok(())
}

fn snapshot_from_row(row: SnapshotRow) -> Result<ChipSnapshot> {
    let distribution = match row.distribution_format.as_str() {
        "normalized_probability" => {
            let buckets: Vec<ChipBucket> = serde_json::from_value(row.distribution)?;
            validate_buckets(&buckets, true)?;
            buckets
        }
        "legacy_peak_relative" => decode_legacy_buckets(row.distribution)?,
        other => {
            return Err(AppError::Internal(format!(
                "unknown chip distribution format: {other}"
            )))
        }
    };
    let dominant_peak_price = row.dominant_peak_price.unwrap_or_else(|| {
        distribution
            .iter()
            .max_by(|left, right| left.weight.total_cmp(&right.weight))
            .map_or(0.0, |bucket| bucket.price)
    });
    Ok(ChipSnapshot {
        code: row.code,
        trade_date: row.trade_date,
        distribution,
        average_cost: row.avg_cost.unwrap_or(0.0),
        winner_rate: row.profit_ratio.unwrap_or(0.0),
        concentration: row.concentration.unwrap_or(0.0),
        dominant_peak_price,
        source: row.source,
        model_version: row.model_version,
        validated: row.validated,
        source_updated_at: row.source_updated_at,
    })
}

fn decode_legacy_buckets(value: Value) -> Result<Vec<ChipBucket>> {
    if value == serde_json::json!({}) || value == serde_json::json!([]) {
        return Ok(Vec::new());
    }
    let rows = value.as_array().ok_or_else(|| {
        AppError::Internal("legacy chip distribution is neither an array nor empty object".into())
    })?;
    let mut buckets = Vec::with_capacity(rows.len());
    let mut total = 0.0;
    for row in rows {
        let price = row.get("price").and_then(Value::as_f64).ok_or_else(|| {
            AppError::Internal("legacy chip bucket has invalid price".to_string())
        })?;
        let relative_weight = row
            .get("percentage")
            .and_then(Value::as_f64)
            .ok_or_else(|| {
                AppError::Internal("legacy chip bucket has invalid percentage".to_string())
            })?;
        if !price.is_finite()
            || price < 0.0
            || !relative_weight.is_finite()
            || relative_weight < 0.0
        {
            return Err(AppError::Internal(
                "legacy chip bucket contains a non-finite or negative value".to_string(),
            ));
        }
        total += relative_weight;
        buckets.push(ChipBucket {
            price,
            weight: relative_weight,
        });
    }
    if total == 0.0 {
        return Ok(Vec::new());
    }
    for bucket in &mut buckets {
        bucket.weight /= total;
    }
    validate_buckets(&buckets, true)?;
    Ok(buckets)
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::ChipRepository;
    use crate::data::chip::{
        ChipBucket, ChipDayInput, ChipModelState, ChipSnapshot, ChipSourceDecision,
        ChipValidationRun,
    };

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn snapshot(trade_date: NaiveDate, weight: f64) -> ChipSnapshot {
        ChipSnapshot {
            code: "600519.SH".to_string(),
            trade_date,
            distribution: vec![
                ChipBucket {
                    price: 1_500.0,
                    weight,
                },
                ChipBucket {
                    price: 1_600.0,
                    weight: 1.0 - weight,
                },
            ],
            average_cost: 1_550.0,
            winner_rate: 72.5,
            concentration: 0.65,
            dominant_peak_price: 1_600.0,
            source: "qbot_estimate".to_string(),
            model_version: Some("qbot-chip-v2".to_string()),
            validated: true,
            source_updated_at: Utc.with_ymd_and_hms(2026, 7, 17, 10, 0, 0).unwrap(),
        }
    }

    fn validation_run(
        run_id: Uuid,
        completed_hour: Option<u32>,
        decision: Option<ChipSourceDecision>,
        error_summary: Option<&str>,
    ) -> ChipValidationRun {
        ChipValidationRun {
            run_id,
            model_version: "qbot-chip-v2".to_string(),
            sample_definition: json!({"stocks": 200, "dates": 24}),
            aggregate_metrics: json!({"median_cost_error": 0.02}),
            subgroup_metrics: json!({"high_turnover": {"bias": 0.01}}),
            decision,
            started_at: Utc.with_ymd_and_hms(2026, 7, 18, 1, 0, 0).unwrap(),
            completed_at: completed_hour
                .map(|hour| Utc.with_ymd_and_hms(2026, 7, 18, hour, 0, 0).unwrap()),
            error_summary: error_summary.map(str::to_string),
        }
    }

    #[test]
    fn shared_day_input_contract_keeps_full_model_inputs() {
        let input = ChipDayInput {
            code: "600519.SH".to_string(),
            trade_date: date(2026, 7, 17),
            open: 1_490.0,
            high: 1_620.0,
            low: 1_480.0,
            close: 1_600.0,
            volume: 10_000.0,
            turnover_rate: 3.2,
            adjustment_factor: 2.0,
        };
        assert_eq!(input.code, "600519.SH");
        assert_eq!(input.adjustment_factor, 2.0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn resolves_closest_prior_snapshot_and_returns_none_without_prior(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool);
        repo.upsert_snapshot(&snapshot(date(2026, 7, 17), 0.4))
            .await?;
        repo.upsert_snapshot(&snapshot(date(2026, 7, 21), 0.3))
            .await?;

        let resolved = repo
            .snapshot_at_or_before("600519.SH", date(2026, 7, 19))
            .await?
            .expect("closest prior snapshot");
        assert_eq!(resolved.trade_date, date(2026, 7, 17));
        assert_eq!(resolved.source, "qbot_estimate");
        assert!(repo
            .snapshot_at_or_before("600519.SH", date(2026, 7, 16))
            .await?
            .is_none());
        assert_eq!(
            repo.latest_snapshot("600519.SH")
                .await?
                .expect("latest snapshot")
                .trade_date,
            date(2026, 7, 21)
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn canonical_snapshot_replacement_is_idempotent_and_buckets_round_trip(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool.clone());
        let day = date(2026, 7, 17);
        repo.upsert_snapshot(&snapshot(day, 0.4)).await?;
        repo.upsert_snapshot(&snapshot(day, 0.25)).await?;
        repo.upsert_snapshot(&snapshot(day, 0.25)).await?;

        let stored = repo.latest_snapshot("600519.SH").await?.unwrap();
        assert_eq!(stored.distribution, snapshot(day, 0.25).distribution);
        assert_eq!(stored.model_version.as_deref(), Some("qbot-chip-v2"));
        assert!(stored.validated);
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM chip_distribution WHERE code = $1 AND trade_date = $2",
        )
        .bind("600519.SH")
        .bind(day)
        .fetch_one(&pool)
        .await?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn rejects_negative_non_finite_and_non_normalized_bucket_weights(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool);
        for weight in [-0.1, f64::NAN, f64::INFINITY] {
            let error = repo
                .upsert_snapshot(&snapshot(date(2026, 7, 17), weight))
                .await
                .expect_err("malformed bucket must be rejected");
            assert!(error.to_string().contains("bucket"));
        }

        let mut not_normalized = snapshot(date(2026, 7, 17), 0.5);
        not_normalized.distribution[1].weight = 0.25;
        assert!(repo.upsert_snapshot(&not_normalized).await.is_err());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_state_only_advances_and_stale_snapshot_writes_cannot_replace_it(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool.clone());
        let newer = ChipModelState {
            code: "600519.SH".to_string(),
            model_version: "qbot-chip-v2".to_string(),
            through_date: date(2026, 7, 18),
            distribution: snapshot(date(2026, 7, 18), 0.2).distribution,
            last_adjustment_factor: 2.5,
        };
        let stale = ChipModelState {
            through_date: date(2026, 7, 17),
            distribution: snapshot(date(2026, 7, 17), 0.8).distribution,
            last_adjustment_factor: 1.5,
            ..newer.clone()
        };

        assert!(repo.save_model_state(&newer).await?);
        assert!(!repo.save_model_state(&stale).await?);
        repo.upsert_snapshot(&snapshot(date(2026, 7, 17), 0.8))
            .await?;

        let (through_date, distribution): (NaiveDate, serde_json::Value) = sqlx::query_as(
            "SELECT through_date, distribution FROM chip_model_states WHERE code = $1 AND model_version = $2",
        )
        .bind("600519.SH")
        .bind("qbot-chip-v2")
        .fetch_one(&pool)
        .await?;
        assert_eq!(through_date, date(2026, 7, 18));
        assert_eq!(distribution, serde_json::to_value(&newer.distribution)?);
        let factor: f64 = sqlx::query_scalar(
            "SELECT last_adjustment_factor::float8 FROM chip_model_states WHERE code = $1 AND model_version = $2",
        )
        .bind("600519.SH")
        .bind("qbot-chip-v2")
        .fetch_one(&pool)
        .await?;
        assert_eq!(factor, 2.5);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn concurrent_model_state_writers_always_leave_the_newest_date(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool.clone());
        let newer = ChipModelState {
            code: "000001.SZ".to_string(),
            model_version: "qbot-chip-v2".to_string(),
            through_date: date(2026, 7, 18),
            distribution: snapshot(date(2026, 7, 18), 0.2).distribution,
            last_adjustment_factor: 2.5,
        };
        let stale = ChipModelState {
            through_date: date(2026, 7, 17),
            distribution: snapshot(date(2026, 7, 17), 0.8).distribution,
            last_adjustment_factor: 1.5,
            ..newer.clone()
        };

        let newer_repo = repo.clone();
        let stale_repo = repo.clone();
        let (newer_result, stale_result) = tokio::join!(
            newer_repo.save_model_state(&newer),
            stale_repo.save_model_state(&stale)
        );
        newer_result?;
        stale_result?;

        let stored_date: NaiveDate = sqlx::query_scalar(
            "SELECT through_date FROM chip_model_states WHERE code = $1 AND model_version = $2",
        )
        .bind("000001.SZ")
        .bind("qbot-chip-v2")
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored_date, date(2026, 7, 18));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn app_boundary_enforces_metric_provenance_and_model_state_invariants(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool);
        let day = date(2026, 7, 17);
        let mut invalid_snapshots = Vec::new();

        let mut zero_price = snapshot(day, 0.5);
        zero_price.distribution[0].price = 0.0;
        invalid_snapshots.push(zero_price);
        let mut non_finite_price = snapshot(day, 0.5);
        non_finite_price.distribution[0].price = f64::NAN;
        invalid_snapshots.push(non_finite_price);
        let mut zero_cost = snapshot(day, 0.5);
        zero_cost.average_cost = 0.0;
        invalid_snapshots.push(zero_cost);
        let mut non_finite_cost = snapshot(day, 0.5);
        non_finite_cost.average_cost = f64::INFINITY;
        invalid_snapshots.push(non_finite_cost);
        let mut zero_peak = snapshot(day, 0.5);
        zero_peak.dominant_peak_price = 0.0;
        invalid_snapshots.push(zero_peak);
        let mut non_finite_peak = snapshot(day, 0.5);
        non_finite_peak.dominant_peak_price = f64::NAN;
        invalid_snapshots.push(non_finite_peak);
        let mut bad_winner = snapshot(day, 0.5);
        bad_winner.winner_rate = 100.01;
        invalid_snapshots.push(bad_winner);
        let mut bad_concentration = snapshot(day, 0.5);
        bad_concentration.concentration = -0.01;
        invalid_snapshots.push(bad_concentration);
        let mut official_with_model = snapshot(day, 0.5);
        official_with_model.source = "tushare".to_string();
        invalid_snapshots.push(official_with_model);

        for invalid in invalid_snapshots {
            assert!(repo.upsert_snapshot(&invalid).await.is_err());
        }

        let buckets = snapshot(day, 0.5).distribution;
        for state in [
            ChipModelState {
                code: "".to_string(),
                model_version: "qbot-chip-v2".to_string(),
                through_date: day,
                distribution: buckets.clone(),
                last_adjustment_factor: 1.0,
            },
            ChipModelState {
                code: "600519.SH".to_string(),
                model_version: "".to_string(),
                through_date: day,
                distribution: buckets.clone(),
                last_adjustment_factor: 1.0,
            },
            ChipModelState {
                code: "600519.SH".to_string(),
                model_version: "qbot-chip-v2".to_string(),
                through_date: day,
                distribution: buckets.clone(),
                last_adjustment_factor: 0.0,
            },
            ChipModelState {
                code: "600519.SH".to_string(),
                model_version: "qbot-chip-v2".to_string(),
                through_date: day,
                distribution: buckets,
                last_adjustment_factor: f64::NAN,
            },
        ] {
            assert!(repo.save_model_state(&state).await.is_err());
        }
        let state_distribution = serde_json::to_value(snapshot(day, 0.5).distribution)?;
        assert!(sqlx::query(
            r#"INSERT INTO chip_model_states
               (code, model_version, through_date, distribution, last_adjustment_factor)
               VALUES ('600519.SH', 'direct-sql-nan', DATE '2026-07-17', $1,
                       'NaN'::numeric)"#,
        )
        .bind(state_distribution)
        .execute(&repo.pool)
        .await
        .is_err());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn exact_zero_and_hundred_percent_metrics_round_trip(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool);
        let mut lower = snapshot(date(2026, 7, 17), 0.5);
        lower.winner_rate = 0.0;
        lower.concentration = 0.0;
        repo.upsert_snapshot(&lower).await?;
        let mut upper = snapshot(date(2026, 7, 18), 0.5);
        upper.winner_rate = 100.0;
        upper.concentration = 100.0;
        repo.upsert_snapshot(&upper).await?;

        let stored = repo.latest_snapshot("600519.SH").await?.unwrap();
        assert_eq!(stored.winner_rate, 100.0);
        assert_eq!(stored.concentration, 100.0);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn database_constraints_reject_invalid_metrics_and_provenance(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let normalized = serde_json::to_value(snapshot(date(2026, 7, 17), 0.5).distribution)?;
        let insert = |code: &'static str,
                      avg_cost: f64,
                      winner_rate: f64,
                      concentration: f64,
                      peak: f64,
                      source: &'static str,
                      model: Option<&'static str>,
                      validated: bool,
                      format: &'static str| {
            let pool = pool.clone();
            let normalized = normalized.clone();
            async move {
                sqlx::query(
                    r#"INSERT INTO chip_distribution
                       (code, trade_date, distribution, avg_cost, profit_ratio, concentration,
                        dominant_peak_price, source, model_version, validated, distribution_format)
                       VALUES ($1, DATE '2026-07-17', $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
                )
                .bind(code)
                .bind(normalized)
                .bind(avg_cost)
                .bind(winner_rate)
                .bind(concentration)
                .bind(peak)
                .bind(source)
                .bind(model)
                .bind(validated)
                .bind(format)
                .execute(&pool)
                .await
            }
        };

        assert!(insert(
            "BAD001",
            0.0,
            50.0,
            50.0,
            10.0,
            "qbot_estimate",
            Some("v2"),
            false,
            "normalized_probability"
        )
        .await
        .is_err());
        assert!(insert(
            "BAD002",
            10.0,
            101.0,
            50.0,
            10.0,
            "qbot_estimate",
            Some("v2"),
            false,
            "normalized_probability"
        )
        .await
        .is_err());
        assert!(insert(
            "BAD003",
            10.0,
            50.0,
            -1.0,
            10.0,
            "qbot_estimate",
            Some("v2"),
            false,
            "normalized_probability"
        )
        .await
        .is_err());
        assert!(insert(
            "BAD004",
            10.0,
            50.0,
            50.0,
            0.0,
            "qbot_estimate",
            Some("v2"),
            false,
            "normalized_probability"
        )
        .await
        .is_err());
        assert!(insert(
            "BAD005",
            10.0,
            50.0,
            50.0,
            10.0,
            "legacy",
            Some("v2"),
            false,
            "legacy_peak_relative"
        )
        .await
        .is_err());
        assert!(insert(
            "BAD006",
            10.0,
            50.0,
            50.0,
            10.0,
            "legacy",
            None,
            true,
            "legacy_peak_relative"
        )
        .await
        .is_err());
        assert!(insert(
            "BAD007",
            10.0,
            50.0,
            50.0,
            10.0,
            "qbot_estimate",
            None,
            false,
            "normalized_probability"
        )
        .await
        .is_err());
        assert!(insert(
            "BAD008",
            10.0,
            50.0,
            50.0,
            10.0,
            "tushare",
            Some("v2"),
            true,
            "normalized_probability"
        )
        .await
        .is_err());
        assert!(insert(
            "BAD009",
            10.0,
            50.0,
            50.0,
            10.0,
            "tushare",
            None,
            true,
            "legacy_peak_relative"
        )
        .await
        .is_err());
        assert!(sqlx::query(
            r#"INSERT INTO chip_distribution
               (code, trade_date, distribution, avg_cost, profit_ratio, concentration,
                dominant_peak_price, source, model_version, validated, distribution_format)
               VALUES ('BAD010', DATE '2026-07-17', '[]', 'NaN'::numeric, 50, 50,
                       10, 'qbot_estimate', 'v2', FALSE, 'normalized_probability')"#,
        )
        .execute(&pool)
        .await
        .is_err());

        for (source_index, (source, model)) in [("qbot_estimate", Some("v2")), ("tushare", None)]
            .into_iter()
            .enumerate()
        {
            for (metric_index, null_column) in [
                "avg_cost",
                "profit_ratio",
                "concentration",
                "dominant_peak_price",
            ]
            .into_iter()
            .enumerate()
            {
                let code = format!("N{source_index}{metric_index}");
                let statement = r#"INSERT INTO chip_distribution
                       (code, trade_date, distribution, avg_cost, profit_ratio,
                        concentration, dominant_peak_price, source, model_version,
                        validated, distribution_format)
                       VALUES ($1, DATE '2026-07-18', $3,
                               CASE WHEN $2 = 'avg_cost' THEN NULL ELSE 10 END,
                               CASE WHEN $2 = 'profit_ratio' THEN NULL ELSE 50 END,
                               CASE WHEN $2 = 'concentration' THEN NULL ELSE 50 END,
                               CASE WHEN $2 = 'dominant_peak_price' THEN NULL ELSE 10 END,
                               $4, $5, FALSE, 'normalized_probability')"#;
                assert!(sqlx::query(statement)
                    .bind(&code)
                    .bind(null_column)
                    .bind(&normalized)
                    .bind(source)
                    .bind(model)
                    .execute(&pool)
                    .await
                    .is_err());
            }
        }

        for (code, invalid_distribution) in [("EMPTYOBJ", json!({})), ("EMPTYARR", json!([]))] {
            assert!(sqlx::query(
                r#"INSERT INTO chip_distribution
                   (code, trade_date, distribution, avg_cost, profit_ratio,
                    concentration, dominant_peak_price, source, model_version,
                    validated, distribution_format)
                   VALUES ($1, DATE '2026-07-19', $2, 10, 50, 50, 10,
                           'qbot_estimate', 'v2', FALSE,
                           'normalized_probability')"#,
            )
            .bind(code)
            .bind(invalid_distribution)
            .execute(&pool)
            .await
            .is_err());
        }

        sqlx::query(
            r#"INSERT INTO chip_distribution
               (code, trade_date, distribution, avg_cost, profit_ratio,
                concentration, dominant_peak_price, source, model_version,
                validated, distribution_format)
               VALUES ('LEGNULL', DATE '2026-07-17', '{}', NULL, NULL, NULL,
                       NULL, 'legacy', NULL, FALSE, 'legacy_peak_relative')"#,
        )
        .execute(&pool)
        .await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn validation_decision_uses_only_successful_completed_runs_and_stable_ties(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool.clone());
        let older = validation_run(
            Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            Some(2),
            Some(ChipSourceDecision::Estimate),
            None,
        );
        let failed = validation_run(
            Uuid::parse_str("00000000-0000-0000-0000-000000000004").unwrap(),
            Some(4),
            Some(ChipSourceDecision::Official),
            Some("provider timeout"),
        );
        let incomplete = validation_run(
            Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap(),
            None,
            Some(ChipSourceDecision::Official),
            None,
        );
        let tie_low = validation_run(
            Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap(),
            Some(3),
            Some(ChipSourceDecision::Estimate),
            None,
        );
        let tie_high = validation_run(
            Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap(),
            Some(3),
            Some(ChipSourceDecision::Official),
            None,
        );
        for run in [&older, &failed, &incomplete, &tie_low, &tie_high] {
            repo.save_validation_run(run).await?;
        }
        sqlx::query(
            "UPDATE chip_model_validation_runs SET recorded_at = TIMESTAMPTZ '2026-07-18 02:00:00Z' WHERE run_id = $1",
        )
        .bind(older.run_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "UPDATE chip_model_validation_runs SET recorded_at = TIMESTAMPTZ '2026-07-18 03:00:00Z' WHERE run_id IN ($1, $2)",
        )
        .bind(tie_low.run_id)
        .bind(tie_high.run_id)
        .execute(&pool)
        .await?;

        assert_eq!(
            repo.latest_validation_decision("qbot-chip-v2").await?,
            Some(ChipSourceDecision::Official)
        );
        let payload: (serde_json::Value, serde_json::Value, serde_json::Value, Option<String>) =
            sqlx::query_as(
                "SELECT sample_definition, aggregate_metrics, subgroup_metrics, error_summary FROM chip_model_validation_runs WHERE run_id = $1",
            )
            .bind(failed.run_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(payload.0, failed.sample_definition);
        assert_eq!(payload.1, failed.aggregate_metrics);
        assert_eq!(payload.2, failed.subgroup_metrics);
        assert_eq!(payload.3.as_deref(), Some("provider timeout"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn validation_decision_recency_uses_database_recording_time_not_caller_clock(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        let repo = ChipRepository::new(pool.clone());
        let future_completed = validation_run(
            Uuid::parse_str("10000000-0000-0000-0000-000000000001").unwrap(),
            Some(23),
            Some(ChipSourceDecision::Official),
            None,
        );
        repo.save_validation_run(&future_completed).await?;
        sqlx::query("SELECT pg_sleep(0.01)").execute(&pool).await?;
        let mut later_recorded = validation_run(
            Uuid::parse_str("10000000-0000-0000-0000-000000000002").unwrap(),
            Some(2),
            Some(ChipSourceDecision::Estimate),
            None,
        );
        later_recorded.started_at = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
        repo.save_validation_run(&later_recorded).await?;

        assert_eq!(
            repo.latest_validation_decision("qbot-chip-v2").await?,
            Some(ChipSourceDecision::Estimate)
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn migration_preserves_legacy_chip_rows_with_honest_provenance(
        pool: PgPool,
    ) -> crate::error::Result<()> {
        sqlx::query("DROP TABLE chip_model_validation_runs, chip_model_states")
            .execute(&pool)
            .await?;
        sqlx::query(
            "ALTER TABLE chip_distribution DROP COLUMN source, DROP COLUMN model_version, DROP COLUMN dominant_peak_price, DROP COLUMN validated, DROP COLUMN source_updated_at, DROP COLUMN distribution_format",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "ALTER TABLE chip_distribution ALTER COLUMN profit_ratio TYPE NUMERIC(6,4), ALTER COLUMN concentration TYPE NUMERIC(6,4)",
        )
        .execute(&pool)
        .await?;
        let legacy = json!([
            {"price": 10.0, "percentage": 100.0, "isProfit": true},
            {"price": 11.0, "percentage": 50.0, "isProfit": false}
        ]);
        sqlx::query(
            "INSERT INTO chip_distribution (code, trade_date, distribution, avg_cost, profit_ratio, concentration) VALUES ($1, $2, $3, 10.3, 0.4, 0.5)",
        )
        .bind("000001.SZ")
        .bind(date(2020, 1, 2))
        .bind(&legacy)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO chip_distribution (code, trade_date) VALUES ('LEGNULL', DATE '2020-01-03')",
        )
        .execute(&pool)
        .await?;

        sqlx::raw_sql(include_str!(
            "../../migrations/025_historical_chip_intelligence.sql"
        ))
        .execute(&pool)
        .await?;
        sqlx::raw_sql(include_str!(
            "../../migrations/025_historical_chip_intelligence.sql"
        ))
        .execute(&pool)
        .await?;

        let row: (serde_json::Value, String, bool, String, Option<String>) = sqlx::query_as(
            "SELECT distribution, source, validated, distribution_format, model_version FROM chip_distribution WHERE code = '000001.SZ'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, legacy);
        assert_eq!(row.1, "legacy");
        assert!(!row.2);
        assert_eq!(row.3, "legacy_peak_relative");
        assert!(row.4.is_none());

        let empty_legacy: (serde_json::Value, Option<f64>, Option<f64>, Option<f64>, String) =
            sqlx::query_as(
                "SELECT distribution, avg_cost::float8, profit_ratio::float8, concentration::float8, source FROM chip_distribution WHERE code = 'LEGNULL'",
            )
            .fetch_one(&pool)
            .await?;
        assert_eq!(empty_legacy.0, json!({}));
        assert_eq!(empty_legacy.1, None);
        assert_eq!(empty_legacy.2, None);
        assert_eq!(empty_legacy.3, None);
        assert_eq!(empty_legacy.4, "legacy");

        let decoded = ChipRepository::new(pool)
            .latest_snapshot("000001.SZ")
            .await?
            .expect("legacy row remains readable");
        assert_eq!(decoded.source, "legacy");
        assert!(!decoded.validated);
        assert!(
            (decoded
                .distribution
                .iter()
                .map(|bucket| bucket.weight)
                .sum::<f64>()
                - 1.0)
                .abs()
                < 1e-9
        );
        Ok(())
    }
}
