use chrono::{DateTime, Utc};

use crate::data::chip::{ChipBucket, ChipDayInput, ChipModelState, ChipSnapshot};
use crate::error::{AppError, Result};

pub const CHIP_MODEL_VERSION: &str = "qbot-chip-v2";

const NORMALIZATION_TOLERANCE: f64 = 1e-9;
const MIN_BUCKET_COUNT: usize = 3;

#[derive(Debug, Clone)]
pub struct ChipModelV2 {
    bucket_count: usize,
    state: Option<ChipModelState>,
}

impl ChipModelV2 {
    pub fn new(bucket_count: usize) -> Self {
        assert!(
            bucket_count >= MIN_BUCKET_COUNT,
            "chip model requires at least {MIN_BUCKET_COUNT} buckets"
        );
        Self {
            bucket_count,
            state: None,
        }
    }

    pub fn restore(state: ChipModelState) -> Result<Self> {
        validate_state(&state)?;
        Ok(Self {
            bucket_count: state.distribution.len(),
            state: Some(state),
        })
    }

    pub fn state(&self) -> Option<ChipModelState> {
        self.state.clone()
    }

    pub fn update(&mut self, input: ChipDayInput) -> Result<ChipSnapshot> {
        validate_input(&input, self.state.as_ref())?;

        let is_first_day = self.state.is_none();
        let retained_fraction = if is_first_day {
            0.0
        } else {
            (1.0 - input.turnover_rate / 100.0).clamp(0.0, 1.0)
        };
        let replacement_fraction = 1.0 - retained_fraction;

        let adjusted_old = self
            .state
            .as_ref()
            .map(|state| {
                let rebase = state.last_adjustment_factor / input.adjustment_factor;
                if !rebase.is_finite() || rebase <= 0.0 {
                    return Err(AppError::Internal(
                        "chip model adjustment rebase is non-finite".to_string(),
                    ));
                }
                Ok(state
                    .distribution
                    .iter()
                    .map(|bucket| ChipBucket {
                        price: bucket.price * rebase,
                        weight: bucket.weight * retained_fraction,
                    })
                    .collect::<Vec<_>>())
            })
            .transpose()?
            .unwrap_or_default();

        let (grid_low, grid_high) = grid_bounds(&adjusted_old, input.low, input.high);
        let typical_price = weighted_typical_price(&input);
        let prices = price_grid(grid_low, grid_high, typical_price, self.bucket_count);
        let mut weights = rebin_point_masses(&adjusted_old, &prices);
        allocate_triangular(
            &mut weights,
            &prices,
            replacement_fraction,
            input.low,
            input.high,
            typical_price,
        );
        normalize(&mut weights)?;

        let distribution = prices
            .into_iter()
            .zip(weights)
            .map(|(price, weight)| ChipBucket { price, weight })
            .collect::<Vec<_>>();
        validate_distribution(&distribution)?;

        let average_cost = distribution
            .iter()
            .map(|bucket| bucket.price * bucket.weight)
            .sum::<f64>();
        let winner_rate = distribution
            .iter()
            .filter(|bucket| bucket.price <= input.close)
            .map(|bucket| bucket.weight)
            .sum::<f64>()
            * 100.0;
        let dominant_index = dominant_bucket_index(&distribution);
        let dominant_peak_price = distribution[dominant_index].price;
        let concentration = top_bucket_concentration(&distribution);

        let next_state = ChipModelState {
            code: input.code.clone(),
            model_version: CHIP_MODEL_VERSION.to_string(),
            through_date: input.trade_date,
            distribution: distribution.clone(),
            last_adjustment_factor: input.adjustment_factor,
        };
        validate_state(&next_state)?;

        let snapshot = ChipSnapshot {
            code: input.code,
            trade_date: input.trade_date,
            distribution,
            average_cost,
            winner_rate: winner_rate.clamp(0.0, 100.0),
            concentration: concentration.clamp(0.0, 100.0),
            dominant_peak_price,
            source: "qbot_estimate".to_string(),
            model_version: Some(CHIP_MODEL_VERSION.to_string()),
            validated: false,
            source_updated_at: DateTime::<Utc>::from_naive_utc_and_offset(
                input
                    .trade_date
                    .and_hms_opt(0, 0, 0)
                    .expect("valid midnight"),
                Utc,
            ),
        };

        self.state = Some(next_state);
        Ok(snapshot)
    }
}

fn validate_input(input: &ChipDayInput, state: Option<&ChipModelState>) -> Result<()> {
    if input.code.trim().is_empty() {
        return Err(bad_input("stock code is empty"));
    }
    for (name, value) in [
        ("open", input.open),
        ("high", input.high),
        ("low", input.low),
        ("close", input.close),
        ("adjustment factor", input.adjustment_factor),
    ] {
        if !value.is_finite() || value <= 0.0 {
            return Err(bad_input(&format!("{name} must be finite and positive")));
        }
    }
    if !input.volume.is_finite() || input.volume < 0.0 {
        return Err(bad_input("volume must be finite and non-negative"));
    }
    if !input.turnover_rate.is_finite() || !(0.0..=100.0).contains(&input.turnover_rate) {
        return Err(bad_input("turnover rate must be between 0 and 100"));
    }
    if input.high < input.open.max(input.close) {
        return Err(bad_input("high is below open or close"));
    }
    if input.low > input.open.min(input.close) {
        return Err(bad_input("low is above open or close"));
    }
    if let Some(state) = state {
        if input.code != state.code {
            return Err(bad_input("stock code changed within model history"));
        }
        if input.trade_date <= state.through_date {
            return Err(bad_input("trade dates must be strictly increasing"));
        }
    }
    Ok(())
}

fn validate_state(state: &ChipModelState) -> Result<()> {
    if state.code.trim().is_empty() {
        return Err(bad_state("stock code is empty"));
    }
    if state.model_version != CHIP_MODEL_VERSION {
        return Err(bad_state("model version is not qbot-chip-v2"));
    }
    if !state.last_adjustment_factor.is_finite() || state.last_adjustment_factor <= 0.0 {
        return Err(bad_state(
            "last adjustment factor must be finite and positive",
        ));
    }
    validate_distribution(&state.distribution)
}

fn validate_distribution(distribution: &[ChipBucket]) -> Result<()> {
    if distribution.len() < MIN_BUCKET_COUNT {
        return Err(bad_state("distribution has fewer than three buckets"));
    }
    let mut total = 0.0;
    let mut previous = None;
    for bucket in distribution {
        if !bucket.price.is_finite() || bucket.price <= 0.0 {
            return Err(bad_state("bucket price must be finite and positive"));
        }
        if previous.is_some_and(|price| price > bucket.price) {
            return Err(bad_state("bucket prices must be ascending"));
        }
        if !bucket.weight.is_finite() || bucket.weight < 0.0 {
            return Err(bad_state("bucket weight must be finite and non-negative"));
        }
        previous = Some(bucket.price);
        total += bucket.weight;
    }
    if !total.is_finite() || (total - 1.0).abs() > NORMALIZATION_TOLERANCE {
        return Err(bad_state("bucket weights must sum to one"));
    }
    Ok(())
}

fn grid_bounds(old: &[ChipBucket], current_low: f64, current_high: f64) -> (f64, f64) {
    let mut low = current_low;
    let mut high = current_high;
    for bucket in old.iter().filter(|bucket| bucket.weight > 0.0) {
        low = low.min(bucket.price);
        high = high.max(bucket.price);
    }
    (low, high)
}

fn price_grid(low: f64, high: f64, anchor: f64, bucket_count: usize) -> Vec<f64> {
    if low == high {
        return vec![low; bucket_count];
    }
    let step = (high - low) / (bucket_count - 1) as f64;
    let mut prices = (0..bucket_count)
        .map(|index| {
            if index + 1 == bucket_count {
                high
            } else {
                low + step * index as f64
            }
        })
        .collect::<Vec<_>>();

    let anchor = anchor.clamp(low, high);
    if !prices.contains(&anchor) {
        let replace = (1..bucket_count - 1)
            .min_by(|left, right| {
                (prices[*left] - anchor)
                    .abs()
                    .total_cmp(&(prices[*right] - anchor).abs())
                    .then_with(|| left.cmp(right))
            })
            .expect("minimum bucket count leaves an interior grid point");
        prices[replace] = anchor;
        prices.sort_by(f64::total_cmp);
    }
    prices
}

fn rebin_point_masses(old: &[ChipBucket], prices: &[f64]) -> Vec<f64> {
    let mut result = vec![0.0; prices.len()];
    if prices.len() == 1 || prices[0] == prices[prices.len() - 1] {
        result[0] = old.iter().map(|bucket| bucket.weight).sum();
        return result;
    }

    for bucket in old {
        if bucket.weight == 0.0 {
            continue;
        }
        let right = prices.partition_point(|price| *price < bucket.price);
        if right == 0 {
            result[0] += bucket.weight;
            continue;
        }
        if right == prices.len() {
            result[prices.len() - 1] += bucket.weight;
            continue;
        }
        if prices[right] == bucket.price {
            result[right] += bucket.weight;
            continue;
        }
        let left = right - 1;
        let right_share = (bucket.price - prices[left]) / (prices[right] - prices[left]);
        result[left] += bucket.weight * (1.0 - right_share);
        result[right] += bucket.weight * right_share;
    }
    result
}

fn allocate_triangular(
    weights: &mut [f64],
    prices: &[f64],
    mass: f64,
    low: f64,
    high: f64,
    center: f64,
) {
    if mass == 0.0 {
        return;
    }
    if low == high {
        let index = nearest_price_index(prices, low);
        weights[index] += mass;
        return;
    }

    let mut shape = prices
        .iter()
        .map(|price| {
            if *price < low || *price > high {
                0.0
            } else if *price <= center {
                if center == low {
                    1.0
                } else {
                    (*price - low) / (center - low)
                }
            } else if center == high {
                1.0
            } else {
                (high - *price) / (high - center)
            }
        })
        .collect::<Vec<_>>();
    let shape_total = shape.iter().sum::<f64>();
    if shape_total <= 0.0 {
        weights[nearest_price_index(prices, center)] += mass;
        return;
    }
    for (weight, shape_weight) in weights.iter_mut().zip(shape.drain(..)) {
        *weight += mass * shape_weight / shape_total;
    }
}

fn nearest_price_index(prices: &[f64], target: f64) -> usize {
    prices
        .iter()
        .enumerate()
        .min_by(|(left_index, left), (right_index, right)| {
            let distance_order = (*left - target).abs().total_cmp(&(*right - target).abs());
            distance_order.then_with(|| left_index.cmp(right_index))
        })
        .map_or(0, |(index, _)| index)
}

fn weighted_typical_price(input: &ChipDayInput) -> f64 {
    ((input.open + input.high + input.low + 2.0 * input.close) / 5.0).clamp(input.low, input.high)
}

fn normalize(weights: &mut [f64]) -> Result<()> {
    let total = weights.iter().sum::<f64>();
    if !total.is_finite() || total <= 0.0 {
        return Err(AppError::Internal(
            "chip model produced an empty or non-finite distribution".to_string(),
        ));
    }
    for weight in weights.iter_mut() {
        *weight /= total;
    }
    Ok(())
}

fn dominant_bucket_index(distribution: &[ChipBucket]) -> usize {
    distribution
        .iter()
        .enumerate()
        .max_by(|(left_index, left), (right_index, right)| {
            left.weight
                .total_cmp(&right.weight)
                .then_with(|| right_index.cmp(left_index))
        })
        .map_or(0, |(index, _)| index)
}

fn top_bucket_concentration(distribution: &[ChipBucket]) -> f64 {
    let mut weights = distribution
        .iter()
        .map(|bucket| bucket.weight)
        .collect::<Vec<_>>();
    weights.sort_by(|left, right| right.total_cmp(left));
    weights.into_iter().take(5).sum::<f64>() * 100.0
}

fn bad_input(message: &str) -> AppError {
    AppError::BadRequest(format!("invalid chip day input: {message}"))
}

fn bad_state(message: &str) -> AppError {
    AppError::BadRequest(format!("invalid chip model state: {message}"))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::{ChipModelV2, CHIP_MODEL_VERSION};
    use crate::data::chip::{ChipBucket, ChipDayInput, ChipModelState};

    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 1, day).unwrap()
    }

    fn day(
        code: &str,
        trade_date: NaiveDate,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        turnover_rate: f64,
        adjustment_factor: f64,
    ) -> ChipDayInput {
        ChipDayInput {
            code: code.to_string(),
            trade_date,
            open,
            high,
            low,
            close,
            volume: 10_000.0,
            turnover_rate,
            adjustment_factor,
        }
    }

    fn flat(trade_date: NaiveDate, price: f64, turnover: f64, factor: f64) -> ChipDayInput {
        day(
            "600519.SH",
            trade_date,
            price,
            price,
            price,
            price,
            turnover,
            factor,
        )
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() <= 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    fn assert_valid(snapshot: &crate::data::chip::ChipSnapshot, bucket_count: usize) {
        assert_eq!(snapshot.distribution.len(), bucket_count);
        assert!(snapshot
            .distribution
            .windows(2)
            .all(|pair| pair[0].price <= pair[1].price));
        assert!(snapshot
            .distribution
            .iter()
            .all(|bucket| bucket.price.is_finite()
                && bucket.price > 0.0
                && bucket.weight.is_finite()
                && bucket.weight >= 0.0));
        assert_close(
            snapshot
                .distribution
                .iter()
                .map(|bucket| bucket.weight)
                .sum(),
            1.0,
        );
        let low = snapshot.distribution.first().unwrap().price;
        let high = snapshot.distribution.last().unwrap().price;
        assert!((low..=high).contains(&snapshot.average_cost));
        assert!((low..=high).contains(&snapshot.dominant_peak_price));
        assert!((0.0..=100.0).contains(&snapshot.winner_rate));
        assert!((0.0..=100.0).contains(&snapshot.concentration));
        assert_eq!(snapshot.source, "qbot_estimate");
        assert_eq!(snapshot.model_version.as_deref(), Some(CHIP_MODEL_VERSION));
        assert!(!snapshot.validated);
    }

    fn assert_state_close(actual: &ChipModelState, expected: &ChipModelState) {
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.model_version, expected.model_version);
        assert_eq!(actual.through_date, expected.through_date);
        assert_close(
            actual.last_adjustment_factor,
            expected.last_adjustment_factor,
        );
        assert_eq!(actual.distribution.len(), expected.distribution.len());
        for (actual, expected) in actual.distribution.iter().zip(&expected.distribution) {
            assert_close(actual.price, expected.price);
            assert_close(actual.weight, expected.weight);
        }
    }

    fn assert_snapshot_close(
        actual: &crate::data::chip::ChipSnapshot,
        expected: &crate::data::chip::ChipSnapshot,
    ) {
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.trade_date, expected.trade_date);
        assert_eq!(actual.source, expected.source);
        assert_eq!(actual.model_version, expected.model_version);
        assert_eq!(actual.validated, expected.validated);
        assert_eq!(actual.source_updated_at, expected.source_updated_at);
        assert_close(actual.average_cost, expected.average_cost);
        assert_close(actual.winner_rate, expected.winner_rate);
        assert_close(actual.concentration, expected.concentration);
        assert_close(actual.dominant_peak_price, expected.dominant_peak_price);
        assert_eq!(actual.distribution.len(), expected.distribution.len());
        for (actual, expected) in actual.distribution.iter().zip(&expected.distribution) {
            assert_close(actual.price, expected.price);
            assert_close(actual.weight, expected.weight);
        }
    }

    #[test]
    fn bootstraps_first_day_even_with_zero_turnover_and_normalizes_mass() {
        let mut model = ChipModelV2::new(30);
        let snapshot = model
            .update(day("600519.SH", date(1), 10.0, 12.0, 9.0, 11.0, 0.0, 1.0))
            .unwrap();

        assert_valid(&snapshot, 30);
        assert_eq!(snapshot.trade_date, date(1));
        assert_eq!(snapshot.source_updated_at.date_naive(), date(1));
        assert_eq!(model.state().unwrap().through_date, date(1));
    }

    #[test]
    fn zero_turnover_retains_old_mass_and_full_turnover_replaces_it() {
        let mut model = ChipModelV2::new(30);
        model.update(flat(date(1), 10.0, 100.0, 1.0)).unwrap();
        let retained = model.update(flat(date(2), 20.0, 0.0, 1.0)).unwrap();
        assert_close(retained.average_cost, 10.0);

        let replaced = model.update(flat(date(3), 30.0, 100.0, 1.0)).unwrap();
        assert_close(replaced.average_cost, 30.0);
        assert!(replaced
            .distribution
            .iter()
            .filter(|bucket| bucket.weight > 0.0)
            .all(|bucket| (bucket.price - 30.0).abs() <= 1e-9));
    }

    #[test]
    fn flat_bar_is_finite_and_has_a_single_deterministic_peak() {
        let mut model = ChipModelV2::new(30);
        let snapshot = model.update(flat(date(1), 8.25, 20.0, 1.0)).unwrap();

        assert_valid(&snapshot, 30);
        assert_close(snapshot.average_cost, 8.25);
        assert_close(snapshot.dominant_peak_price, 8.25);
        assert_close(snapshot.winner_rate, 100.0);
    }

    #[test]
    fn rebases_old_cost_by_last_factor_over_current_factor_in_both_directions() {
        let mut split = ChipModelV2::new(30);
        split.update(flat(date(1), 10.0, 100.0, 1.0)).unwrap();
        let halved = split.update(flat(date(2), 5.0, 0.0, 2.0)).unwrap();
        assert_close(halved.average_cost, 5.0);
        assert_close(halved.dominant_peak_price, 5.0);

        let mut reverse = ChipModelV2::new(30);
        reverse.update(flat(date(1), 10.0, 100.0, 2.0)).unwrap();
        let doubled = reverse.update(flat(date(2), 20.0, 0.0, 1.0)).unwrap();
        assert_close(doubled.average_cost, 20.0);
        assert_close(doubled.dominant_peak_price, 20.0);
    }

    #[test]
    fn adaptive_grid_covers_retained_and_new_ranges_without_losing_mass() {
        let mut model = ChipModelV2::new(17);
        model.update(flat(date(1), 10.0, 100.0, 1.0)).unwrap();
        let expanded = model
            .update(day("600519.SH", date(2), 25.0, 30.0, 20.0, 26.0, 50.0, 1.0))
            .unwrap();

        assert_valid(&expanded, 17);
        assert_close(expanded.distribution.first().unwrap().price, 10.0);
        assert_close(expanded.distribution.last().unwrap().price, 30.0);
        assert_close(expanded.distribution.first().unwrap().weight, 0.5);
    }

    #[test]
    fn adaptive_grid_represents_a_narrow_current_bar_inside_wide_retained_history() {
        let state = ChipModelState {
            code: "600519.SH".to_string(),
            model_version: CHIP_MODEL_VERSION.to_string(),
            through_date: date(1),
            distribution: vec![
                ChipBucket {
                    price: 1.0,
                    weight: 0.5,
                },
                ChipBucket {
                    price: 500.0,
                    weight: 0.0,
                },
                ChipBucket {
                    price: 1_000.0,
                    weight: 0.5,
                },
            ],
            last_adjustment_factor: 1.0,
        };
        let mut model = ChipModelV2::restore(state).unwrap();
        let snapshot = model
            .update(day(
                "600519.SH",
                date(2),
                10.25,
                11.0,
                10.0,
                10.75,
                50.0,
                1.0,
            ))
            .unwrap();

        assert_valid(&snapshot, 3);
        let current_bar_mass = snapshot
            .distribution
            .iter()
            .filter(|bucket| (10.0..=11.0).contains(&bucket.price))
            .map(|bucket| bucket.weight)
            .sum::<f64>();
        assert_close(current_bar_mass, 0.5);
        assert_close(snapshot.average_cost, 255.525);
        assert_close(
            snapshot
                .distribution
                .iter()
                .map(|bucket| bucket.weight)
                .sum(),
            1.0,
        );
    }

    #[test]
    fn rejects_bucket_counts_below_three_and_accepts_the_minimum() {
        for unsupported in 0..3 {
            assert!(
                std::panic::catch_unwind(|| ChipModelV2::new(unsupported)).is_err(),
                "bucket count {unsupported} must be rejected"
            );
        }
        assert!(std::panic::catch_unwind(|| ChipModelV2::new(3)).is_ok());

        let undersized_state = ChipModelState {
            code: "600519.SH".to_string(),
            model_version: CHIP_MODEL_VERSION.to_string(),
            through_date: date(1),
            distribution: vec![
                ChipBucket {
                    price: 10.0,
                    weight: 0.5,
                },
                ChipBucket {
                    price: 11.0,
                    weight: 0.5,
                },
            ],
            last_adjustment_factor: 1.0,
        };
        assert!(ChipModelV2::restore(undersized_state).is_err());
    }

    #[test]
    fn dominant_peak_ties_choose_the_lowest_price_and_metrics_are_deterministic() {
        let state = ChipModelState {
            code: "600519.SH".to_string(),
            model_version: CHIP_MODEL_VERSION.to_string(),
            through_date: date(1),
            distribution: vec![
                ChipBucket {
                    price: 10.0,
                    weight: 0.5,
                },
                ChipBucket {
                    price: 15.0,
                    weight: 0.0,
                },
                ChipBucket {
                    price: 20.0,
                    weight: 0.5,
                },
            ],
            last_adjustment_factor: 1.0,
        };
        let mut model = ChipModelV2::restore(state).unwrap();
        let snapshot = model
            .update(day("600519.SH", date(2), 15.0, 20.0, 10.0, 15.0, 0.0, 1.0))
            .unwrap();

        assert_close(snapshot.dominant_peak_price, 10.0);
        assert_close(snapshot.average_cost, 15.0);
        assert_close(snapshot.winner_rate, 50.0);
        assert_close(snapshot.concentration, 100.0);
    }

    #[test]
    fn retains_old_low_weight_mass_beyond_one_hundred_twenty_days() {
        let mut model = ChipModelV2::new(30);
        model.update(flat(date(1), 10.0, 100.0, 1.0)).unwrap();
        for offset in 1..=130 {
            let trade_date = date(1) + chrono::Duration::days(offset);
            model.update(flat(trade_date, 20.0, 0.1, 1.0)).unwrap();
        }

        let state = model.state().unwrap();
        assert!(state
            .distribution
            .iter()
            .any(|bucket| bucket.price < 11.0 && bucket.weight > 0.8));
        assert_close(
            state.distribution.iter().map(|bucket| bucket.weight).sum(),
            1.0,
        );
    }

    #[test]
    fn invalid_out_of_order_and_cross_stock_inputs_leave_state_unchanged() {
        let mut model = ChipModelV2::new(30);
        model.update(flat(date(2), 10.0, 20.0, 1.0)).unwrap();
        let original = model.state().unwrap();

        let mut invalid_inputs = vec![
            flat(date(3), 10.0, -0.1, 1.0),
            flat(date(3), 10.0, 100.1, 1.0),
            flat(date(3), 10.0, 20.0, 0.0),
            flat(date(3), 0.0, 20.0, 1.0),
            day("600519.SH", date(3), 12.0, 11.0, 9.0, 10.0, 20.0, 1.0),
            day("600519.SH", date(3), 10.0, 11.0, 10.5, 10.0, 20.0, 1.0),
            flat(date(3), 10.0, 20.0, 1.0),
        ];
        invalid_inputs.last_mut().unwrap().volume = -1.0;
        let mut non_finite = flat(date(3), 10.0, 20.0, 1.0);
        non_finite.close = f64::NAN;
        invalid_inputs.push(non_finite);
        invalid_inputs.push(flat(date(2), 10.0, 20.0, 1.0));
        invalid_inputs.push(flat(date(1), 10.0, 20.0, 1.0));
        invalid_inputs.push(day("000001.SZ", date(3), 10.0, 10.0, 10.0, 10.0, 20.0, 1.0));

        for input in invalid_inputs {
            assert!(model.update(input).is_err());
            assert_eq!(model.state().as_ref(), Some(&original));
        }
    }

    #[test]
    fn serialized_restore_resume_matches_uninterrupted_run_across_factor_change() {
        let inputs = [
            day("600519.SH", date(1), 10.0, 12.0, 9.0, 11.0, 25.0, 1.0),
            day("600519.SH", date(2), 11.0, 13.0, 10.0, 12.0, 10.0, 1.0),
            day("600519.SH", date(3), 5.5, 7.0, 5.0, 6.0, 15.0, 2.0),
        ];
        let mut uninterrupted = ChipModelV2::new(30);
        let mut uninterrupted_snapshot = None;
        for input in inputs.clone() {
            uninterrupted_snapshot = Some(uninterrupted.update(input).unwrap());
        }

        let mut resumed = ChipModelV2::new(30);
        for input in &inputs[..2] {
            resumed.update(input.clone()).unwrap();
        }
        let encoded = serde_json::to_string(&resumed.state().unwrap()).unwrap();
        let decoded: ChipModelState = serde_json::from_str(&encoded).unwrap();
        let mut resumed = ChipModelV2::restore(decoded).unwrap();
        let resumed_snapshot = resumed.update(inputs[2].clone()).unwrap();

        assert_state_close(&resumed.state().unwrap(), &uninterrupted.state().unwrap());
        assert_snapshot_close(&resumed_snapshot, &uninterrupted_snapshot.unwrap());
    }

    #[test]
    fn identical_inputs_produce_identical_outputs() {
        let inputs = [
            flat(date(1), 10.0, 0.0, 1.0),
            day("600519.SH", date(2), 10.0, 15.0, 8.0, 12.0, 37.5, 1.0),
        ];
        let mut left = ChipModelV2::new(31);
        let mut right = ChipModelV2::new(31);
        for input in inputs {
            assert_eq!(
                left.update(input.clone()).unwrap(),
                right.update(input).unwrap()
            );
        }
        assert_eq!(left.state(), right.state());
    }

    #[test]
    fn restore_rejects_malformed_state() {
        let state = ChipModelState {
            code: "600519.SH".to_string(),
            model_version: CHIP_MODEL_VERSION.to_string(),
            through_date: date(1),
            distribution: vec![ChipBucket {
                price: 10.0,
                weight: 0.5,
            }],
            last_adjustment_factor: 1.0,
        };
        assert!(ChipModelV2::restore(state).is_err());
    }
}
