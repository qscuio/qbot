use chrono::{DateTime, FixedOffset, NaiveDate, Utc};

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
