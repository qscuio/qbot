use chrono::{DateTime, NaiveDate, Utc};

use super::TradingDateResolver;
use crate::error::Result;

pub(crate) fn manual_available_at(
    published_at: Option<DateTime<Utc>>,
    first_seen_at: DateTime<Utc>,
) -> DateTime<Utc> {
    published_at.unwrap_or(first_seen_at)
}

pub(crate) fn effective_trade_date_for_manual(
    resolver: &dyn TradingDateResolver,
    available_at: DateTime<Utc>,
) -> Result<NaiveDate> {
    resolver.effective_trade_date(available_at)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};

    use super::{effective_trade_date_for_manual, manual_available_at};
    use crate::analysis::events::AShareTradingDateResolver;

    #[test]
    fn manual_available_at_prefers_published_at() {
        let first_seen_at = dt(2026, 7, 10, 8, 0, 0);
        let published_at = dt(2026, 7, 10, 6, 30, 0);

        let available_at = manual_available_at(Some(published_at), first_seen_at);

        assert_eq!(available_at, published_at);
    }

    #[test]
    fn effective_trade_date_for_manual_uses_same_day_when_available_before_close() {
        let resolver = AShareTradingDateResolver;
        let available_at =
            manual_available_at(Some(dt(2026, 7, 10, 6, 30, 0)), dt(2026, 7, 10, 8, 0, 0));

        let trade_date = effective_trade_date_for_manual(&resolver, available_at).unwrap();

        assert_eq!(trade_date, NaiveDate::from_ymd_opt(2026, 7, 10).unwrap());
    }

    #[test]
    fn effective_trade_date_for_manual_rolls_after_close_to_next_open_date() {
        let resolver = AShareTradingDateResolver;
        let available_at =
            manual_available_at(Some(dt(2026, 7, 10, 7, 30, 0)), dt(2026, 7, 10, 8, 0, 0));

        let trade_date = effective_trade_date_for_manual(&resolver, available_at).unwrap();

        assert_eq!(trade_date, NaiveDate::from_ymd_opt(2026, 7, 13).unwrap());
    }

    #[test]
    fn effective_trade_date_for_manual_uses_first_seen_at_when_published_at_is_absent() {
        let resolver = AShareTradingDateResolver;
        let available_at = manual_available_at(None, dt(2026, 7, 11, 2, 0, 0));

        let trade_date = effective_trade_date_for_manual(&resolver, available_at).unwrap();

        assert_eq!(trade_date, NaiveDate::from_ymd_opt(2026, 7, 13).unwrap());
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .unwrap()
    }
}
