use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::data::chip::{ChipBucket, ChipSnapshot, ChipSourceDecision};
use crate::error::{AppError, Result};

const PERFORMANCE_STOCK_CAP: usize = 200;
const PERFORMANCE_DATE_CAP: usize = 24;
const DISTRIBUTION_STOCK_CAP: usize = 50;
const DISTRIBUTION_DATE_CAP: usize = 12;
const DISTRIBUTION_TOLERANCE: f64 = 1e-6;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationStock {
    pub code: String,
    pub exchange: String,
    pub market_value: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationObservation {
    pub code: String,
    pub trade_date: NaiveDate,
    pub turnover_rate: f64,
    pub volatility: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationCorporateAction {
    pub code: String,
    pub action_date: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChipValidationSampleStock {
    pub code: String,
    pub subgroup_keys: Vec<String>,
    pub performance_dates: Vec<NaiveDate>,
    pub distribution_dates: Vec<NaiveDate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChipValidationSample {
    pub model_version: String,
    pub stocks: Vec<ChipValidationSampleStock>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipComparison {
    pub code: String,
    pub trade_date: NaiveDate,
    pub average_cost_relative_error: f64,
    pub dominant_peak_relative_error: Option<f64>,
    pub winner_rate_absolute_error: f64,
    pub normalized_wasserstein_distance: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSummary {
    pub mean: f64,
    pub median: f64,
    pub p90: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipAggregateMetrics {
    pub performance_sample_count: usize,
    pub distribution_sample_count: usize,
    pub median_average_cost_relative_error: f64,
    pub median_dominant_peak_relative_error: f64,
    pub mean_winner_rate_absolute_error: f64,
    pub p90_average_cost_relative_error: f64,
    pub wasserstein: MetricSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipSubgroupMetrics {
    pub expected_sample_count: usize,
    pub sample_count: usize,
    pub median_average_cost_relative_error: f64,
    pub median_winner_rate_absolute_error: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipPerformancePoint {
    pub code: String,
    pub trade_date: NaiveDate,
    pub average_cost: f64,
    pub winner_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChipValidationReport {
    pub model_version: String,
    pub expected_performance_count: usize,
    pub expected_distribution_count: usize,
    pub complete: bool,
    pub aggregate: Option<ChipAggregateMetrics>,
    pub expected_subgroups: Vec<String>,
    pub subgroups: BTreeMap<String, ChipSubgroupMetrics>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StratumKey {
    exchange: String,
    market_value: &'static str,
    turnover: &'static str,
    volatility: &'static str,
    corporate_action: bool,
}

impl StratumKey {
    fn subgroup_keys(&self) -> Vec<String> {
        vec![
            format!("exchange:{}", self.exchange),
            format!("market_value:{}", self.market_value),
            format!("turnover:{}", self.turnover),
            format!("volatility:{}", self.volatility),
            format!(
                "corporate_action:{}",
                if self.corporate_action { "yes" } else { "no" }
            ),
        ]
    }
}

#[derive(Debug, Clone)]
struct SampleCandidate {
    code: String,
    stratum: StratumKey,
    dates: Vec<NaiveDate>,
}

/// Builds a reproducible validation sample without consulting external state.
///
/// Selection uses 64-bit FNV-1a over `model_version`, a purpose separator, and
/// the stock code. Unlike `DefaultHasher`, this algorithm has stable persisted
/// semantics across processes and Rust releases.
pub fn build_validation_sample(
    model_version: &str,
    universe: &[ValidationStock],
    observations: &[ValidationObservation],
    corporate_actions: &[ValidationCorporateAction],
) -> Result<ChipValidationSample> {
    if model_version.trim().is_empty() {
        return Err(invalid("model version is empty"));
    }
    if universe.is_empty() {
        return Err(invalid("validation universe is empty"));
    }

    let mut stocks = BTreeMap::<String, ValidationStock>::new();
    for stock in universe {
        validate_stock(stock)?;
        match stocks.get(&stock.code) {
            Some(existing) if existing != stock => {
                return Err(invalid("duplicate stock has conflicting metadata"));
            }
            Some(_) => {}
            None => {
                stocks.insert(stock.code.clone(), stock.clone());
            }
        }
    }

    let mut bars = BTreeMap::<(String, NaiveDate), ValidationObservation>::new();
    for observation in observations {
        if !stocks.contains_key(&observation.code) {
            return Err(invalid(
                "validation observation references an unknown stock",
            ));
        }
        validate_observation(observation)?;
        let key = (observation.code.clone(), observation.trade_date);
        match bars.get(&key) {
            Some(existing) if existing != observation => {
                return Err(invalid("duplicate observation has conflicting values"));
            }
            Some(_) => {}
            None => {
                bars.insert(key, observation.clone());
            }
        }
    }
    if bars.is_empty() {
        return Err(invalid("validation observations are empty"));
    }

    let mut action_codes = BTreeSet::new();
    let mut unique_actions = BTreeSet::new();
    for action in corporate_actions {
        if !stocks.contains_key(&action.code) {
            return Err(invalid("corporate action references an unknown stock"));
        }
        unique_actions.insert((action.code.clone(), action.action_date));
        action_codes.insert(action.code.clone());
    }

    let mut by_code = BTreeMap::<String, Vec<ValidationObservation>>::new();
    for observation in bars.into_values() {
        by_code
            .entry(observation.code.clone())
            .or_default()
            .push(observation);
    }

    let mut strata = BTreeMap::<StratumKey, Vec<SampleCandidate>>::new();
    for (code, code_bars) in by_code {
        let stock = stocks
            .get(&code)
            .expect("observations were checked against the universe");
        let average_turnover = checked_mean(
            &code_bars
                .iter()
                .map(|bar| bar.turnover_rate)
                .collect::<Vec<_>>(),
        )?;
        let average_volatility = checked_mean(
            &code_bars
                .iter()
                .map(|bar| bar.volatility)
                .collect::<Vec<_>>(),
        )?;
        let stratum = StratumKey {
            exchange: stock.exchange.clone(),
            market_value: market_value_bucket(stock.market_value),
            turnover: turnover_bucket(average_turnover),
            volatility: volatility_bucket(average_volatility),
            corporate_action: action_codes.contains(&code),
        };
        let dates = code_bars.iter().map(|bar| bar.trade_date).collect();
        strata
            .entry(stratum.clone())
            .or_default()
            .push(SampleCandidate {
                code,
                stratum,
                dates,
            });
    }

    for candidates in strata.values_mut() {
        candidates.sort_by(|left, right| {
            stable_selection_hash(model_version, "performance", &left.code)
                .cmp(&stable_selection_hash(
                    model_version,
                    "performance",
                    &right.code,
                ))
                .then_with(|| left.code.cmp(&right.code))
        });
    }
    let selected = round_robin_select(&strata, PERFORMANCE_STOCK_CAP);
    if selected.is_empty() {
        return Err(invalid(
            "validation universe has no stocks with observations",
        ));
    }

    let mut distribution_strata = BTreeMap::<StratumKey, Vec<SampleCandidate>>::new();
    for candidate in &selected {
        distribution_strata
            .entry(candidate.stratum.clone())
            .or_default()
            .push(candidate.clone());
    }
    for candidates in distribution_strata.values_mut() {
        candidates.sort_by(|left, right| {
            stable_selection_hash(model_version, "distribution", &left.code)
                .cmp(&stable_selection_hash(
                    model_version,
                    "distribution",
                    &right.code,
                ))
                .then_with(|| left.code.cmp(&right.code))
        });
    }
    let distribution_codes = round_robin_select(&distribution_strata, DISTRIBUTION_STOCK_CAP)
        .into_iter()
        .map(|candidate| candidate.code)
        .collect::<BTreeSet<_>>();

    let mut sample_stocks = selected
        .into_iter()
        .map(|candidate| {
            let performance_dates = evenly_spaced_dates(&candidate.dates, PERFORMANCE_DATE_CAP);
            let distribution_dates = if distribution_codes.contains(&candidate.code) {
                evenly_spaced_dates(&performance_dates, DISTRIBUTION_DATE_CAP)
            } else {
                Vec::new()
            };
            ChipValidationSampleStock {
                code: candidate.code,
                subgroup_keys: candidate.stratum.subgroup_keys(),
                performance_dates,
                distribution_dates,
            }
        })
        .collect::<Vec<_>>();
    sample_stocks.sort_by(|left, right| left.code.cmp(&right.code));

    Ok(ChipValidationSample {
        model_version: model_version.to_string(),
        stocks: sample_stocks,
    })
}

pub fn compare_chip_snapshots(
    local: &ChipSnapshot,
    official: &ChipSnapshot,
) -> Result<ChipComparison> {
    validate_snapshot_identity_and_metrics(local, official)?;
    validate_distribution(&local.distribution)?;
    validate_distribution(&official.distribution)?;

    Ok(ChipComparison {
        code: local.code.clone(),
        trade_date: local.trade_date,
        average_cost_relative_error: relative_error(local.average_cost, official.average_cost),
        dominant_peak_relative_error: Some(relative_error(
            local.dominant_peak_price,
            official.dominant_peak_price,
        )),
        winner_rate_absolute_error: (local.winner_rate - official.winner_rate).abs(),
        normalized_wasserstein_distance: Some(
            wasserstein_1(&local.distribution, &official.distribution) / official.average_cost,
        ),
    })
}

/// Compares performance metrics when the official full distribution was not
/// sampled. Full-distribution dates should use [`compare_chip_snapshots`].
pub fn compare_chip_performance(
    local: &ChipPerformancePoint,
    official: &ChipPerformancePoint,
) -> Result<ChipComparison> {
    validate_performance_points(local, official)?;
    Ok(ChipComparison {
        code: local.code.clone(),
        trade_date: local.trade_date,
        average_cost_relative_error: relative_error(local.average_cost, official.average_cost),
        dominant_peak_relative_error: None,
        winner_rate_absolute_error: (local.winner_rate - official.winner_rate).abs(),
        normalized_wasserstein_distance: None,
    })
}

pub fn aggregate_chip_comparisons(
    sample: &ChipValidationSample,
    comparisons: &[ChipComparison],
) -> Result<ChipValidationReport> {
    let mut expected = BTreeMap::<(String, NaiveDate), Vec<String>>::new();
    let mut expected_distributions = BTreeSet::new();
    let mut expected_subgroups = BTreeSet::new();
    for stock in &sample.stocks {
        for key in &stock.subgroup_keys {
            expected_subgroups.insert(key.clone());
        }
        for trade_date in &stock.performance_dates {
            if expected
                .insert(
                    (stock.code.clone(), *trade_date),
                    stock.subgroup_keys.clone(),
                )
                .is_some()
            {
                return Err(invalid(
                    "validation sample contains a duplicate performance pair",
                ));
            }
        }
        for trade_date in &stock.distribution_dates {
            let pair = (stock.code.clone(), *trade_date);
            if !expected.contains_key(&pair) {
                return Err(invalid("distribution pair is not a performance pair"));
            }
            if !expected_distributions.insert(pair) {
                return Err(invalid(
                    "validation sample contains a duplicate distribution pair",
                ));
            }
        }
    }

    let mut actual = BTreeMap::<(String, NaiveDate), &ChipComparison>::new();
    for comparison in comparisons {
        validate_comparison(comparison)?;
        let pair = (comparison.code.clone(), comparison.trade_date);
        if !expected.contains_key(&pair) {
            return Err(invalid("comparison is outside the expected sample"));
        }
        let expects_distribution = expected_distributions.contains(&pair);
        if comparison.dominant_peak_relative_error.is_some() != expects_distribution
            || comparison.normalized_wasserstein_distance.is_some() != expects_distribution
        {
            return Err(invalid(
                "comparison metric shape does not match the sampled pair type",
            ));
        }
        if actual.insert(pair, comparison).is_some() {
            return Err(invalid("duplicate comparison for a sampled stock date"));
        }
    }

    let complete = actual.len() == expected.len();

    let mut expected_subgroup_counts = BTreeMap::<String, usize>::new();
    for subgroup_keys in expected.values() {
        for key in subgroup_keys {
            *expected_subgroup_counts.entry(key.clone()).or_default() += 1;
        }
    }
    let mut subgroup_values = BTreeMap::<String, Vec<&ChipComparison>>::new();
    for (pair, comparison) in &actual {
        for key in expected.get(pair).expect("actual pairs were validated") {
            subgroup_values
                .entry(key.clone())
                .or_default()
                .push(*comparison);
        }
    }
    let mut subgroups = BTreeMap::new();
    for (key, values) in subgroup_values {
        let expected_sample_count = expected_subgroup_counts[&key];
        subgroups.insert(
            key,
            ChipSubgroupMetrics {
                expected_sample_count,
                sample_count: values.len(),
                median_average_cost_relative_error: checked_median(
                    &values
                        .iter()
                        .map(|value| value.average_cost_relative_error)
                        .collect::<Vec<_>>(),
                )?,
                median_winner_rate_absolute_error: checked_median(
                    &values
                        .iter()
                        .map(|value| value.winner_rate_absolute_error)
                        .collect::<Vec<_>>(),
                )?,
            },
        );
    }

    let distribution_comparisons = expected_distributions
        .iter()
        .filter_map(|pair| actual.get(pair).copied())
        .collect::<Vec<_>>();
    let wasserstein = distribution_comparisons
        .iter()
        .map(|comparison| {
            comparison
                .normalized_wasserstein_distance
                .expect("distribution metric shape was validated")
        })
        .collect::<Vec<_>>();
    let aggregate = if comparisons.is_empty() || wasserstein.is_empty() {
        None
    } else {
        let average_errors = comparisons
            .iter()
            .map(|comparison| comparison.average_cost_relative_error)
            .collect::<Vec<_>>();
        Some(ChipAggregateMetrics {
            performance_sample_count: comparisons.len(),
            distribution_sample_count: wasserstein.len(),
            median_average_cost_relative_error: checked_median(&average_errors)?,
            median_dominant_peak_relative_error: checked_median(
                &distribution_comparisons
                    .iter()
                    .map(|comparison| {
                        comparison
                            .dominant_peak_relative_error
                            .expect("distribution metric shape was validated")
                    })
                    .collect::<Vec<_>>(),
            )?,
            mean_winner_rate_absolute_error: checked_mean(
                &comparisons
                    .iter()
                    .map(|comparison| comparison.winner_rate_absolute_error)
                    .collect::<Vec<_>>(),
            )?,
            p90_average_cost_relative_error: checked_percentile(&average_errors, 0.90)?,
            wasserstein: MetricSummary {
                mean: checked_mean(&wasserstein)?,
                median: checked_median(&wasserstein)?,
                p90: checked_percentile(&wasserstein, 0.90)?,
            },
        })
    };

    Ok(ChipValidationReport {
        model_version: sample.model_version.clone(),
        expected_performance_count: expected.len(),
        expected_distribution_count: expected_distributions.len(),
        complete,
        aggregate,
        expected_subgroups: expected_subgroups.into_iter().collect(),
        subgroups,
    })
}

pub fn decide_chip_source(report: &ChipValidationReport) -> ChipSourceDecision {
    let Some(aggregate) = report.aggregate.as_ref() else {
        return ChipSourceDecision::Official;
    };
    if report.model_version.trim().is_empty()
        || !report.complete
        || report.expected_performance_count == 0
        || report.expected_distribution_count == 0
        || aggregate.performance_sample_count != report.expected_performance_count
        || aggregate.distribution_sample_count != report.expected_distribution_count
        || !aggregate_is_finite(aggregate)
        || aggregate.median_average_cost_relative_error > 0.03
        || aggregate.median_dominant_peak_relative_error > 0.03
        || aggregate.mean_winner_rate_absolute_error > 5.0
        || aggregate.p90_average_cost_relative_error > 0.08
    {
        return ChipSourceDecision::Official;
    }

    let expected = report
        .expected_subgroups
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let actual = report.subgroups.keys().cloned().collect::<BTreeSet<_>>();
    if expected.is_empty()
        || expected.len() != report.expected_subgroups.len()
        || expected != actual
        || !subgroup_counts_reconcile(report)
    {
        return ChipSourceDecision::Official;
    }
    for subgroup in report.subgroups.values() {
        if subgroup.expected_sample_count == 0
            || subgroup.sample_count != subgroup.expected_sample_count
            || !subgroup.median_average_cost_relative_error.is_finite()
            || !subgroup.median_winner_rate_absolute_error.is_finite()
            || subgroup.median_average_cost_relative_error > 0.06
            || subgroup.median_winner_rate_absolute_error > 10.0
        {
            return ChipSourceDecision::Official;
        }
    }
    ChipSourceDecision::Estimate
}

fn subgroup_counts_reconcile(report: &ChipValidationReport) -> bool {
    const DIMENSIONS: [&str; 5] = [
        "exchange",
        "market_value",
        "turnover",
        "volatility",
        "corporate_action",
    ];

    let mut expected_totals = BTreeMap::<&str, usize>::new();
    let mut sample_totals = BTreeMap::<&str, usize>::new();
    for key in &report.expected_subgroups {
        let Some((dimension, value)) = key.split_once(':') else {
            return false;
        };
        if !DIMENSIONS.contains(&dimension) || value.is_empty() || value.contains(':') {
            return false;
        }
        let Some(metrics) = report.subgroups.get(key) else {
            return false;
        };
        let Some(expected_total) = expected_totals
            .entry(dimension)
            .or_default()
            .checked_add(metrics.expected_sample_count)
        else {
            return false;
        };
        expected_totals.insert(dimension, expected_total);
        let Some(sample_total) = sample_totals
            .entry(dimension)
            .or_default()
            .checked_add(metrics.sample_count)
        else {
            return false;
        };
        sample_totals.insert(dimension, sample_total);
    }

    DIMENSIONS.iter().all(|dimension| {
        expected_totals.get(dimension) == Some(&report.expected_performance_count)
            && sample_totals.get(dimension) == Some(&report.expected_performance_count)
    })
}

pub fn checked_mean(values: &[f64]) -> Result<f64> {
    validate_stat_values(values)?;
    Ok(values.iter().sum::<f64>() / values.len() as f64)
}

pub fn checked_median(values: &[f64]) -> Result<f64> {
    checked_percentile(values, 0.5)
}

pub fn checked_percentile(values: &[f64], percentile: f64) -> Result<f64> {
    validate_stat_values(values)?;
    if !percentile.is_finite() || !(0.0..=1.0).contains(&percentile) {
        return Err(invalid(
            "percentile must be finite and between zero and one",
        ));
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = (sorted.len() - 1) as f64 * percentile;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let fraction = rank - lower as f64;
    Ok(sorted[lower] + (sorted[upper] - sorted[lower]) * fraction)
}

fn validate_stock(stock: &ValidationStock) -> Result<()> {
    if stock.code.trim().is_empty() || stock.exchange.trim().is_empty() {
        return Err(invalid("stock code and exchange must be non-empty"));
    }
    if !stock.market_value.is_finite() || stock.market_value < 0.0 {
        return Err(invalid("market value must be finite and non-negative"));
    }
    Ok(())
}

fn validate_observation(observation: &ValidationObservation) -> Result<()> {
    if !observation.turnover_rate.is_finite() || !(0.0..=100.0).contains(&observation.turnover_rate)
    {
        return Err(invalid(
            "turnover rate must be finite and between zero and 100",
        ));
    }
    if !observation.volatility.is_finite() || observation.volatility < 0.0 {
        return Err(invalid("volatility must be finite and non-negative"));
    }
    Ok(())
}

fn market_value_bucket(value: f64) -> &'static str {
    if value < 10_000_000_000.0 {
        "small"
    } else if value < 50_000_000_000.0 {
        "mid"
    } else {
        "large"
    }
}

fn turnover_bucket(value: f64) -> &'static str {
    if value < 1.0 {
        "low"
    } else if value < 5.0 {
        "mid"
    } else {
        "high"
    }
}

fn volatility_bucket(value: f64) -> &'static str {
    if value < 0.02 {
        "low"
    } else if value < 0.05 {
        "mid"
    } else {
        "high"
    }
}

fn round_robin_select(
    strata: &BTreeMap<StratumKey, Vec<SampleCandidate>>,
    cap: usize,
) -> Vec<SampleCandidate> {
    let mut selected = Vec::new();
    let mut offset = 0;
    while selected.len() < cap {
        let mut added = false;
        for candidates in strata.values() {
            if let Some(candidate) = candidates.get(offset) {
                selected.push(candidate.clone());
                added = true;
                if selected.len() == cap {
                    break;
                }
            }
        }
        if !added {
            break;
        }
        offset += 1;
    }
    selected
}

fn evenly_spaced_dates(dates: &[NaiveDate], cap: usize) -> Vec<NaiveDate> {
    let sorted = dates
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if sorted.len() <= cap {
        return sorted;
    }
    (0..cap)
        .map(|index| {
            let source_index = index * (sorted.len() - 1) / (cap - 1);
            sorted[source_index]
        })
        .collect()
}

fn stable_selection_hash(model_version: &str, purpose: &str, code: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in model_version
        .bytes()
        .chain([0xff])
        .chain(purpose.bytes())
        .chain([0xfe])
        .chain(code.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn validate_snapshot_identity_and_metrics(
    local: &ChipSnapshot,
    official: &ChipSnapshot,
) -> Result<()> {
    if local.code != official.code || local.trade_date != official.trade_date {
        return Err(invalid(
            "chip snapshots must refer to the same stock and date",
        ));
    }
    for (name, value) in [
        ("local average cost", local.average_cost),
        ("official average cost", official.average_cost),
        ("local dominant peak", local.dominant_peak_price),
        ("official dominant peak", official.dominant_peak_price),
    ] {
        if !value.is_finite() || value <= 0.0 {
            return Err(invalid(&format!("{name} must be finite and positive")));
        }
    }
    for (name, value) in [
        ("local winner rate", local.winner_rate),
        ("official winner rate", official.winner_rate),
    ] {
        if !value.is_finite() || !(0.0..=100.0).contains(&value) {
            return Err(invalid(&format!("{name} must be between zero and 100")));
        }
    }
    Ok(())
}

fn validate_performance_points(
    local: &ChipPerformancePoint,
    official: &ChipPerformancePoint,
) -> Result<()> {
    if local.code != official.code || local.trade_date != official.trade_date {
        return Err(invalid(
            "chip performance points must refer to the same stock and date",
        ));
    }
    for (name, value) in [
        ("local average cost", local.average_cost),
        ("official average cost", official.average_cost),
    ] {
        if !value.is_finite() || value <= 0.0 {
            return Err(invalid(&format!("{name} must be finite and positive")));
        }
    }
    for (name, value) in [
        ("local winner rate", local.winner_rate),
        ("official winner rate", official.winner_rate),
    ] {
        if !value.is_finite() || !(0.0..=100.0).contains(&value) {
            return Err(invalid(&format!("{name} must be between zero and 100")));
        }
    }
    Ok(())
}

fn validate_distribution(distribution: &[ChipBucket]) -> Result<()> {
    if distribution.is_empty() {
        return Err(invalid("chip distribution is empty"));
    }
    let mut prices = BTreeSet::new();
    let mut total = 0.0;
    for bucket in distribution {
        if !bucket.price.is_finite() || bucket.price <= 0.0 {
            return Err(invalid("chip bucket price must be finite and positive"));
        }
        if !bucket.weight.is_finite() || bucket.weight < 0.0 {
            return Err(invalid(
                "chip bucket weight must be finite and non-negative",
            ));
        }
        let price_bits = canonical_price_bits(bucket.price);
        if !prices.insert(price_bits) {
            return Err(invalid("chip distribution contains a duplicate price"));
        }
        total += bucket.weight;
    }
    if !total.is_finite() || (total - 1.0).abs() > DISTRIBUTION_TOLERANCE {
        return Err(invalid("chip distribution weights must sum to one"));
    }
    Ok(())
}

fn canonical_price_bits(price: f64) -> u64 {
    if price == 0.0 {
        0.0_f64.to_bits()
    } else {
        price.to_bits()
    }
}

fn relative_error(local: f64, official: f64) -> f64 {
    (local - official).abs() / official
}

fn wasserstein_1(local: &[ChipBucket], official: &[ChipBucket]) -> f64 {
    let mut points = local
        .iter()
        .chain(official)
        .map(|bucket| bucket.price)
        .collect::<Vec<_>>();
    points.sort_by(f64::total_cmp);
    points.dedup_by(|left, right| left.total_cmp(right).is_eq());

    let mut local_cdf = 0.0;
    let mut official_cdf = 0.0;
    let mut distance = 0.0;
    for (index, point) in points.iter().enumerate() {
        local_cdf += local
            .iter()
            .filter(|bucket| bucket.price == *point)
            .map(|bucket| bucket.weight)
            .sum::<f64>();
        official_cdf += official
            .iter()
            .filter(|bucket| bucket.price == *point)
            .map(|bucket| bucket.weight)
            .sum::<f64>();
        if let Some(next) = points.get(index + 1) {
            distance += (local_cdf - official_cdf).abs() * (*next - *point);
        }
    }
    distance
}

fn validate_comparison(comparison: &ChipComparison) -> Result<()> {
    if comparison.code.trim().is_empty() {
        return Err(invalid("comparison stock code is empty"));
    }
    for value in [
        comparison.average_cost_relative_error,
        comparison.winner_rate_absolute_error,
    ] {
        if !value.is_finite() || value < 0.0 {
            return Err(invalid("comparison metric must be finite and non-negative"));
        }
    }
    if comparison
        .dominant_peak_relative_error
        .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err(invalid(
            "dominant peak metric must be finite and non-negative",
        ));
    }
    if comparison
        .normalized_wasserstein_distance
        .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err(invalid(
            "Wasserstein metric must be finite and non-negative",
        ));
    }
    Ok(())
}

fn validate_stat_values(values: &[f64]) -> Result<()> {
    if values.is_empty() {
        return Err(invalid("statistic input is empty"));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(invalid("statistic input contains a non-finite value"));
    }
    Ok(())
}

fn aggregate_is_finite(aggregate: &ChipAggregateMetrics) -> bool {
    [
        aggregate.median_average_cost_relative_error,
        aggregate.median_dominant_peak_relative_error,
        aggregate.mean_winner_rate_absolute_error,
        aggregate.p90_average_cost_relative_error,
        aggregate.wasserstein.mean,
        aggregate.wasserstein.median,
        aggregate.wasserstein.p90,
    ]
    .iter()
    .all(|value| value.is_finite() && *value >= 0.0)
}

fn invalid(message: &str) -> AppError {
    AppError::BadRequest(format!("invalid chip validation data: {message}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{Datelike, NaiveDate, TimeZone, Utc};

    use super::*;
    use crate::data::chip::{ChipBucket, ChipSnapshot, ChipSourceDecision};

    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 1, day).unwrap()
    }

    fn stock(code: &str, exchange: &str, market_value: f64) -> ValidationStock {
        ValidationStock {
            code: code.to_string(),
            exchange: exchange.to_string(),
            market_value,
        }
    }

    fn observation(
        code: &str,
        day: u32,
        turnover_rate: f64,
        volatility: f64,
    ) -> ValidationObservation {
        ValidationObservation {
            code: code.to_string(),
            trade_date: date(day),
            turnover_rate,
            volatility,
        }
    }

    fn action(code: &str, day: u32) -> ValidationCorporateAction {
        ValidationCorporateAction {
            code: code.to_string(),
            action_date: date(day),
        }
    }

    fn snapshot(
        code: &str,
        average_cost: f64,
        winner_rate: f64,
        peak: f64,
        distribution: &[(f64, f64)],
    ) -> ChipSnapshot {
        ChipSnapshot {
            code: code.to_string(),
            trade_date: date(1),
            distribution: distribution
                .iter()
                .map(|(price, weight)| ChipBucket {
                    price: *price,
                    weight: *weight,
                })
                .collect(),
            average_cost,
            winner_rate,
            concentration: 50.0,
            dominant_peak_price: peak,
            source: "fixture".to_string(),
            model_version: None,
            validated: false,
            source_updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    fn comparison(code: &str, day: u32, average: f64, peak: f64, winner: f64) -> ChipComparison {
        ChipComparison {
            code: code.to_string(),
            trade_date: date(day),
            average_cost_relative_error: average,
            dominant_peak_relative_error: Some(peak),
            winner_rate_absolute_error: winner,
            normalized_wasserstein_distance: None,
        }
    }

    fn small_sample() -> ChipValidationSample {
        build_validation_sample(
            "model-v1",
            &[stock("600001.SH", "SH", 5_000_000_000.0)],
            &[
                observation("600001.SH", 1, 0.5, 0.01),
                observation("600001.SH", 2, 0.5, 0.01),
            ],
            &[],
        )
        .unwrap()
    }

    #[test]
    fn sample_is_order_independent_and_model_version_changes_ranking() {
        let mut universe = (1..=220)
            .map(|n| stock(&format!("{n:06}.SH"), "SH", 5_000_000_000.0))
            .collect::<Vec<_>>();
        let mut observations = universe
            .iter()
            .flat_map(|stock| {
                [
                    observation(&stock.code, 1, 0.5, 0.01),
                    observation(&stock.code, 2, 0.5, 0.01),
                ]
            })
            .collect::<Vec<_>>();

        let first = build_validation_sample("model-v1", &universe, &observations, &[]).unwrap();
        universe.reverse();
        observations.reverse();
        let reversed = build_validation_sample("model-v1", &universe, &observations, &[]).unwrap();
        let changed = build_validation_sample("model-v2", &universe, &observations, &[]).unwrap();

        assert_eq!(first, reversed);
        assert_eq!(first.stocks.len(), 200);
        assert_eq!(
            first
                .stocks
                .iter()
                .filter(|stock| !stock.distribution_dates.is_empty())
                .count(),
            50
        );
        assert_ne!(
            first.stocks.iter().map(|s| &s.code).collect::<Vec<_>>(),
            changed.stocks.iter().map(|s| &s.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn sample_caps_dates_includes_endpoints_and_keeps_sparse_history() {
        let observations = (1..=31)
            .map(|day| observation("600001.SH", day, 1.0, 0.02))
            .chain([
                observation("000001.SZ", 1, 1.0, 0.02),
                observation("000001.SZ", 3, 1.0, 0.02),
            ])
            .collect::<Vec<_>>();
        let sample = build_validation_sample(
            "model-v1",
            &[
                stock("600001.SH", "SH", 5_000_000_000.0),
                stock("000001.SZ", "SZ", 5_000_000_000.0),
            ],
            &observations,
            &[],
        )
        .unwrap();
        let dense = sample
            .stocks
            .iter()
            .find(|stock| stock.code == "600001.SH")
            .unwrap();
        let sparse = sample
            .stocks
            .iter()
            .find(|stock| stock.code == "000001.SZ")
            .unwrap();

        assert_eq!(dense.performance_dates.len(), 24);
        assert_eq!(dense.performance_dates.first(), Some(&date(1)));
        assert_eq!(dense.performance_dates.last(), Some(&date(31)));
        assert_eq!(dense.distribution_dates.len(), 12);
        assert!(dense
            .distribution_dates
            .iter()
            .all(|date| dense.performance_dates.contains(date)));
        assert_eq!(sparse.performance_dates, vec![date(1), date(3)]);
        assert_eq!(sparse.distribution_dates, vec![date(1), date(3)]);
        assert_eq!(
            dense
                .performance_dates
                .iter()
                .collect::<BTreeSet<_>>()
                .len(),
            dense.performance_dates.len()
        );
    }

    #[test]
    fn sample_deduplicates_identical_rows_and_rejects_conflicts() {
        let duplicate_stock = stock("600001.SH", "SH", 5_000_000_000.0);
        let duplicate_observation = observation("600001.SH", 1, 1.0, 0.02);
        let sample = build_validation_sample(
            "model-v1",
            &[duplicate_stock.clone(), duplicate_stock],
            &[duplicate_observation.clone(), duplicate_observation],
            &[action("600001.SH", 1), action("600001.SH", 1)],
        )
        .unwrap();
        assert_eq!(sample.stocks.len(), 1);
        assert_eq!(sample.stocks[0].performance_dates, vec![date(1)]);

        let conflicting_stock = build_validation_sample(
            "model-v1",
            &[
                stock("600001.SH", "SH", 5_000_000_000.0),
                stock("600001.SH", "SZ", 5_000_000_000.0),
            ],
            &[observation("600001.SH", 1, 1.0, 0.02)],
            &[],
        );
        assert!(conflicting_stock.is_err());

        let conflicting_bar = build_validation_sample(
            "model-v1",
            &[stock("600001.SH", "SH", 5_000_000_000.0)],
            &[
                observation("600001.SH", 1, 1.0, 0.02),
                observation("600001.SH", 1, 2.0, 0.02),
            ],
            &[],
        );
        assert!(conflicting_bar.is_err());
    }

    #[test]
    fn sample_represents_sparse_strata_and_all_dimensions_affect_grouping() {
        let mut universe = (1..=210)
            .map(|n| stock(&format!("{n:06}.SH"), "SH", 5_000_000_000.0))
            .collect::<Vec<_>>();
        universe.extend([
            stock("900001.SZ", "SZ", 5_000_000_000.0),
            stock("900002.SH", "SH", 20_000_000_000.0),
            stock("900003.SH", "SH", 5_000_000_000.0),
            stock("900004.SH", "SH", 5_000_000_000.0),
        ]);
        let mut observations = universe
            .iter()
            .map(|stock| observation(&stock.code, 1, 0.5, 0.01))
            .collect::<Vec<_>>();
        observations
            .iter_mut()
            .find(|o| o.code == "900003.SH")
            .unwrap()
            .turnover_rate = 8.0;
        observations
            .iter_mut()
            .find(|o| o.code == "900004.SH")
            .unwrap()
            .volatility = 0.10;
        let sample = build_validation_sample(
            "model-v1",
            &universe,
            &observations,
            &[action("900001.SZ", 1)],
        )
        .unwrap();
        let codes = sample
            .stocks
            .iter()
            .map(|stock| stock.code.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(sample.stocks.len(), 200);
        assert!(codes.contains("900001.SZ"));
        assert!(codes.contains("900002.SH"));
        assert!(codes.contains("900003.SH"));
        assert!(codes.contains("900004.SH"));

        let keys = sample
            .stocks
            .iter()
            .flat_map(|stock| stock.subgroup_keys.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        assert!(keys.contains("exchange:SZ"));
        assert!(keys.contains("market_value:mid"));
        assert!(keys.contains("turnover:high"));
        assert!(keys.contains("volatility:high"));
        assert!(keys.contains("corporate_action:yes"));
    }

    #[test]
    fn sample_rejects_empty_unknown_and_non_finite_inputs() {
        assert!(build_validation_sample("model-v1", &[], &[], &[]).is_err());
        assert!(build_validation_sample(
            "",
            &[stock("600001.SH", "SH", 1.0)],
            &[observation("600001.SH", 1, 1.0, 0.02)],
            &[],
        )
        .is_err());
        assert!(build_validation_sample(
            "model-v1",
            &[stock("600001.SH", "SH", f64::NAN)],
            &[observation("600001.SH", 1, 1.0, 0.02)],
            &[],
        )
        .is_err());
        assert!(build_validation_sample(
            "model-v1",
            &[stock("600001.SH", "SH", 1.0)],
            &[observation("missing", 1, 1.0, 0.02)],
            &[],
        )
        .is_err());
        assert!(build_validation_sample(
            "model-v1",
            &[stock("600001.SH", "SH", 1.0)],
            &[observation("600001.SH", 1, -1.0, 0.02)],
            &[],
        )
        .is_err());
    }

    #[test]
    fn snapshot_comparison_computes_exact_errors_and_wasserstein() {
        let local = snapshot("600001.SH", 102.0, 74.0, 98.0, &[(10.0, 0.5), (20.0, 0.5)]);
        let official = snapshot(
            "600001.SH",
            100.0,
            70.0,
            100.0,
            &[(10.0, 0.25), (20.0, 0.25), (30.0, 0.5)],
        );
        let result = compare_chip_snapshots(&local, &official).unwrap();

        assert!((result.average_cost_relative_error - 0.02).abs() < 1e-12);
        assert!((result.winner_rate_absolute_error - 4.0).abs() < 1e-12);
        assert!((result.dominant_peak_relative_error.unwrap() - 0.02).abs() < 1e-12);
        assert!((result.normalized_wasserstein_distance.unwrap() - 0.075).abs() < 1e-12);

        let identical = compare_chip_snapshots(&official, &official).unwrap();
        assert_eq!(identical.normalized_wasserstein_distance, Some(0.0));
    }

    #[test]
    fn performance_comparison_omits_distribution_only_metrics() {
        let local = ChipPerformancePoint {
            code: "600001.SH".to_string(),
            trade_date: date(1),
            average_cost: 102.0,
            winner_rate: 74.0,
        };
        let official = ChipPerformancePoint {
            code: "600001.SH".to_string(),
            trade_date: date(1),
            average_cost: 100.0,
            winner_rate: 70.0,
        };
        let result = compare_chip_performance(&local, &official).unwrap();
        assert!((result.average_cost_relative_error - 0.02).abs() < 1e-12);
        assert_eq!(result.winner_rate_absolute_error, 4.0);
        assert_eq!(result.dominant_peak_relative_error, None);
        assert_eq!(result.normalized_wasserstein_distance, None);
    }

    #[test]
    fn snapshot_comparison_rejects_mismatch_and_malformed_snapshots() {
        let valid = snapshot("600001.SH", 10.0, 50.0, 10.0, &[(10.0, 1.0)]);
        let mut malformed = valid.clone();
        malformed.code = "000001.SZ".to_string();
        assert!(compare_chip_snapshots(&valid, &malformed).is_err());
        malformed = valid.clone();
        malformed.trade_date = date(2);
        assert!(compare_chip_snapshots(&valid, &malformed).is_err());
        malformed = valid.clone();
        malformed.average_cost = 0.0;
        assert!(compare_chip_snapshots(&valid, &malformed).is_err());
        malformed = valid.clone();
        malformed.winner_rate = 101.0;
        assert!(compare_chip_snapshots(&valid, &malformed).is_err());
        malformed = valid.clone();
        malformed.distribution = vec![
            ChipBucket {
                price: 10.0,
                weight: 0.5,
            },
            ChipBucket {
                price: 10.0,
                weight: 0.5,
            },
        ];
        assert!(compare_chip_snapshots(&valid, &malformed).is_err());
        malformed = valid.clone();
        malformed.distribution[0].weight = f64::NAN;
        assert!(compare_chip_snapshots(&valid, &malformed).is_err());
    }

    #[test]
    fn statistic_helpers_cover_mean_medians_percentiles_and_invalid_inputs() {
        assert_eq!(checked_mean(&[1.0, 2.0, 3.0]).unwrap(), 2.0);
        assert_eq!(checked_median(&[3.0, 1.0, 2.0]).unwrap(), 2.0);
        assert_eq!(checked_median(&[4.0, 1.0, 3.0, 2.0]).unwrap(), 2.5);
        assert_eq!(
            checked_percentile(&[0.0, 10.0, 20.0, 30.0], 0.25).unwrap(),
            7.5
        );
        assert!(checked_mean(&[]).is_err());
        assert!(checked_median(&[f64::INFINITY]).is_err());
        assert!(checked_percentile(&[1.0], 1.1).is_err());
    }

    #[test]
    fn aggregation_checks_exact_expected_pairs_and_builds_all_subgroups() {
        let sample = small_sample();
        let mut first = comparison("600001.SH", 1, 0.01, 0.02, 3.0);
        first.normalized_wasserstein_distance = Some(0.04);
        let mut second = comparison("600001.SH", 2, 0.03, 0.04, 5.0);
        second.normalized_wasserstein_distance = Some(0.06);
        let report = aggregate_chip_comparisons(&sample, &[first, second]).unwrap();

        assert!(report.complete);
        assert_eq!(
            report.aggregate.as_ref().unwrap().performance_sample_count,
            2
        );
        assert_eq!(
            report.aggregate.as_ref().unwrap().distribution_sample_count,
            2
        );
        assert!(
            (report
                .aggregate
                .as_ref()
                .unwrap()
                .median_average_cost_relative_error
                - 0.02)
                .abs()
                < 1e-12
        );
        assert_eq!(
            report
                .aggregate
                .as_ref()
                .unwrap()
                .mean_winner_rate_absolute_error,
            4.0
        );
        assert!((report.aggregate.as_ref().unwrap().wasserstein.median - 0.05).abs() < 1e-12);
        assert_eq!(report.subgroups.len(), 5);

        let mut only_first = comparison("600001.SH", 1, 0.01, 0.01, 1.0);
        only_first.normalized_wasserstein_distance = Some(0.01);
        let incomplete = aggregate_chip_comparisons(&sample, &[only_first]).unwrap();
        assert!(!incomplete.complete);
        assert_eq!(
            decide_chip_source(&incomplete),
            ChipSourceDecision::Official
        );
    }

    #[test]
    fn aggregation_rejects_distribution_metrics_on_performance_only_dates() {
        let observations = (1..=13)
            .map(|day| observation("600001.SH", day, 1.0, 0.02))
            .collect::<Vec<_>>();
        let sample = build_validation_sample(
            "model-v1",
            &[stock("600001.SH", "SH", 5_000_000_000.0)],
            &observations,
            &[],
        )
        .unwrap();
        let sample_stock = &sample.stocks[0];
        let distribution_dates = sample_stock
            .distribution_dates
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(sample_stock.performance_dates.len(), 13);
        assert_eq!(distribution_dates.len(), 12);

        let comparisons = sample_stock
            .performance_dates
            .iter()
            .map(|trade_date| {
                let mut value = comparison("600001.SH", trade_date.day(), 0.01, 0.01, 1.0);
                if distribution_dates.contains(trade_date) {
                    value.normalized_wasserstein_distance = Some(0.01);
                } else {
                    value.dominant_peak_relative_error = Some(999.0);
                }
                value
            })
            .collect::<Vec<_>>();

        assert!(aggregate_chip_comparisons(&sample, &comparisons).is_err());
    }

    #[test]
    fn aggregation_rejects_incomplete_distribution_metric_pairs() {
        let sample = small_sample();
        let mut missing_peak = comparison("600001.SH", 1, 0.01, 0.01, 1.0);
        missing_peak.dominant_peak_relative_error = None;
        missing_peak.normalized_wasserstein_distance = Some(0.01);
        assert!(aggregate_chip_comparisons(&sample, &[missing_peak]).is_err());

        let missing_wasserstein = comparison("600001.SH", 1, 0.01, 0.01, 1.0);
        assert!(aggregate_chip_comparisons(&sample, &[missing_wasserstein]).is_err());
    }

    fn report_at(average: f64, peak: f64, winner: f64, p90: f64) -> ChipValidationReport {
        let subgroup = || ChipSubgroupMetrics {
            expected_sample_count: 10,
            sample_count: 10,
            median_average_cost_relative_error: 0.01,
            median_winner_rate_absolute_error: 1.0,
        };
        let expected_subgroups = vec![
            "exchange:SH".to_string(),
            "market_value:small".to_string(),
            "turnover:low".to_string(),
            "volatility:low".to_string(),
            "corporate_action:no".to_string(),
        ];
        ChipValidationReport {
            model_version: "model-v1".to_string(),
            expected_performance_count: 10,
            expected_distribution_count: 5,
            complete: true,
            aggregate: Some(ChipAggregateMetrics {
                performance_sample_count: 10,
                distribution_sample_count: 5,
                median_average_cost_relative_error: average,
                median_dominant_peak_relative_error: peak,
                mean_winner_rate_absolute_error: winner,
                p90_average_cost_relative_error: p90,
                wasserstein: MetricSummary {
                    mean: 0.02,
                    median: 0.02,
                    p90: 0.03,
                },
            }),
            expected_subgroups: expected_subgroups.clone(),
            subgroups: expected_subgroups
                .into_iter()
                .map(|key| (key, subgroup()))
                .collect(),
        }
    }

    #[test]
    fn every_global_threshold_passes_at_equality_and_fails_above() {
        assert_eq!(
            decide_chip_source(&report_at(0.03, 0.03, 5.0, 0.08)),
            ChipSourceDecision::Estimate
        );
        assert_eq!(
            decide_chip_source(&report_at(0.030_000_1, 0.03, 5.0, 0.08)),
            ChipSourceDecision::Official
        );
        assert_eq!(
            decide_chip_source(&report_at(0.03, 0.030_000_1, 5.0, 0.08)),
            ChipSourceDecision::Official
        );
        assert_eq!(
            decide_chip_source(&report_at(0.03, 0.03, 5.000_001, 0.08)),
            ChipSourceDecision::Official
        );
        assert_eq!(
            decide_chip_source(&report_at(0.03, 0.03, 5.0, 0.080_001)),
            ChipSourceDecision::Official
        );
        assert_eq!(
            decide_chip_source(&report_at(0.029_999, 0.029_999, 4.999, 0.079_999)),
            ChipSourceDecision::Estimate
        );
    }

    #[test]
    fn subgroup_bias_fails_above_twice_global_limit_but_equality_passes() {
        let mut report = report_at(0.01, 0.01, 1.0, 0.02);
        report
            .subgroups
            .get_mut("exchange:SH")
            .unwrap()
            .median_average_cost_relative_error = 0.06;
        report
            .subgroups
            .get_mut("exchange:SH")
            .unwrap()
            .median_winner_rate_absolute_error = 10.0;
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Estimate);
        report
            .subgroups
            .get_mut("exchange:SH")
            .unwrap()
            .median_average_cost_relative_error = 0.060_001;
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);
        report
            .subgroups
            .get_mut("exchange:SH")
            .unwrap()
            .median_average_cost_relative_error = 0.01;
        report
            .subgroups
            .get_mut("exchange:SH")
            .unwrap()
            .median_winner_rate_absolute_error = 10.000_001;
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);
    }

    #[test]
    fn decision_fails_closed_for_missing_empty_non_finite_or_incomplete_report() {
        let mut report = report_at(0.01, 0.01, 1.0, 0.02);
        report.complete = false;
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);
        report = report_at(0.01, 0.01, 1.0, 0.02);
        report.aggregate = None;
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);
        report = report_at(0.01, 0.01, 1.0, 0.02);
        report.aggregate.as_mut().unwrap().performance_sample_count = 0;
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);
        report = report_at(f64::NAN, 0.01, 1.0, 0.02);
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);
        report = report_at(0.01, 0.01, 1.0, 0.02);
        report.subgroups.clear();
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);
    }

    #[test]
    fn decision_rejects_omitted_dimension_and_understated_dimension_counts() {
        let mut report = report_at(0.01, 0.01, 1.0, 0.02);
        report
            .expected_subgroups
            .retain(|key| !key.starts_with("volatility:"));
        report
            .subgroups
            .retain(|key, _| !key.starts_with("volatility:"));
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);

        let mut report = report_at(0.01, 0.01, 1.0, 0.02);
        let exchange = report.subgroups.get_mut("exchange:SH").unwrap();
        exchange.expected_sample_count = 9;
        exchange.sample_count = 9;
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Official);
    }

    #[test]
    fn decision_accepts_multiple_values_when_each_dimension_reconciles() {
        let mut report = report_at(0.01, 0.01, 1.0, 0.02);
        let sh = report.subgroups.get_mut("exchange:SH").unwrap();
        sh.expected_sample_count = 6;
        sh.sample_count = 6;
        report.expected_subgroups.push("exchange:SZ".to_string());
        report.subgroups.insert(
            "exchange:SZ".to_string(),
            ChipSubgroupMetrics {
                expected_sample_count: 4,
                sample_count: 4,
                median_average_cost_relative_error: 0.01,
                median_winner_rate_absolute_error: 1.0,
            },
        );
        assert_eq!(decide_chip_source(&report), ChipSourceDecision::Estimate);
    }

    #[test]
    fn decision_rejects_unknown_malformed_and_duplicate_subgroup_keys() {
        let mut unknown = report_at(0.01, 0.01, 1.0, 0.02);
        let metrics = unknown.subgroups.remove("turnover:low").unwrap();
        unknown
            .expected_subgroups
            .retain(|key| key != "turnover:low");
        unknown
            .expected_subgroups
            .push("sector:technology".to_string());
        unknown
            .subgroups
            .insert("sector:technology".to_string(), metrics);
        assert_eq!(decide_chip_source(&unknown), ChipSourceDecision::Official);

        let mut malformed = report_at(0.01, 0.01, 1.0, 0.02);
        let metrics = malformed.subgroups.remove("turnover:low").unwrap();
        malformed
            .expected_subgroups
            .retain(|key| key != "turnover:low");
        malformed.expected_subgroups.push("turnover:".to_string());
        malformed.subgroups.insert("turnover:".to_string(), metrics);
        assert_eq!(decide_chip_source(&malformed), ChipSourceDecision::Official);

        let mut duplicate = report_at(0.01, 0.01, 1.0, 0.02);
        duplicate.expected_subgroups.push("exchange:SH".to_string());
        assert_eq!(decide_chip_source(&duplicate), ChipSourceDecision::Official);
    }
}
