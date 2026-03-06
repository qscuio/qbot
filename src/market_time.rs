use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Timelike, Utc};

const BEIJING_OFFSET_SECS: i32 = 8 * 60 * 60;

pub fn beijing_tz() -> FixedOffset {
    FixedOffset::east_opt(BEIJING_OFFSET_SECS).expect("valid +08:00 offset")
}

pub fn beijing_now() -> DateTime<FixedOffset> {
    Utc::now().with_timezone(&beijing_tz())
}

pub fn beijing_today() -> NaiveDate {
    beijing_now().date_naive()
}

/// A-share continuous auction windows (China time):
/// - 09:30-11:30
/// - 13:00-15:00
pub fn is_a_share_trading_time(now: DateTime<FixedOffset>) -> bool {
    if now.weekday().number_from_monday() > 5 {
        return false;
    }

    let mins = now.hour() * 60 + now.minute();
    (570..=690).contains(&mins) || (780..=900).contains(&mins)
}

pub fn is_a_share_trading_now() -> bool {
    is_a_share_trading_time(beijing_now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_market_hours() {
        let tz = beijing_tz();

        let monday_open = tz.with_ymd_and_hms(2026, 3, 2, 9, 30, 0).unwrap();
        let monday_lunch = tz.with_ymd_and_hms(2026, 3, 2, 12, 0, 0).unwrap();
        let monday_pm = tz.with_ymd_and_hms(2026, 3, 2, 14, 45, 0).unwrap();
        let saturday = tz.with_ymd_and_hms(2026, 3, 7, 10, 0, 0).unwrap();

        assert!(is_a_share_trading_time(monday_open));
        assert!(!is_a_share_trading_time(monday_lunch));
        assert!(is_a_share_trading_time(monday_pm));
        assert!(!is_a_share_trading_time(saturday));
    }
}
