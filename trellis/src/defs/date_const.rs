//! Day counts of `date` constants, for the one thing `validate` computes about
//! them: whether `DATE '2024-01-01' + 2147483647` (or a cast of the result)
//! raises, as Postgres does while planning. Postgres evaluates every value
//! that is not a constant; nothing here produces a stored value.
//!
//! A day count is the number of days since 2000-01-01 (Postgres's own
//! `date` representation), so the bounds below are Postgres's.

/// The earliest `date`, 4714-11-24 BC.
pub(crate) const MIN_DATE_DAYS: i64 = -2_451_545;
/// The latest `date`, 5874897-12-31.
pub(crate) const MAX_DATE_DAYS: i64 = 2_145_031_948;
/// The latest `date` that casts to `timestamp`, 294276-12-31. `timestamp` has
/// the same lower bound as `date` and a lower upper one.
pub(crate) const MAX_TIMESTAMP_DATE_DAYS: i64 = 106_751_982;

/// Days from 2000-01-01 to the proleptic Gregorian `year-month-day`
/// (`year` is an AD year, at least 1).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    // Days since 1970-01-01, then since 2000-01-01.
    era * 146_097 + doe - 719_468 - 10_957
}

/// The day count of a canonical `date` literal (`YYYY-MM-DD`, year 0001 to
/// 9999, already checked by the typed-literal allowlist), or `None` for any
/// other text.
pub(crate) fn days_of_canonical_date(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let part = |range: std::ops::Range<usize>| -> Option<i64> {
        let digits = text.get(range)?;
        digits
            .bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| digits.parse().ok())?
    };
    let (year, month, day) = (part(0..4)?, part(5..7)?, part(8..10)?);
    ((1..=12).contains(&month) && (1..=31).contains(&day) && year >= 1)
        .then(|| days_from_civil(year, month, day))
}

/// Microseconds in a day.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// The microseconds since 2000-01-01 00:00:00 of a canonical `timestamp`
/// literal (`YYYY-MM-DD HH:MM:SS[.F]`, year 0001 to 9999, already checked by
/// the typed-literal allowlist), or `None` for any other text.
pub(crate) fn micros_of_canonical_timestamp(text: &str) -> Option<i64> {
    let (date, time) = text.split_once(' ')?;
    let days = days_of_canonical_date(date)?;
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut parts = clock.split(':').map(|p| p.parse::<i64>().ok());
    let (h, m, s) = (parts.next()??, parts.next()??, parts.next()??);
    if parts.next().is_some() || fraction.len() > 6 || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let micros: i64 = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<6}").parse().ok()?
    };
    Some(days * MICROS_PER_DAY + ((h * 60 + m) * 60 + s) * 1_000_000 + micros)
}

/// The microseconds since 2000-01-01 of midnight on day count `days`: what
/// `CAST(date AS timestamp)` gives, for a date that casts.
pub(crate) fn midnight_micros(days: i64) -> i64 {
    days * MICROS_PER_DAY
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Day counts read from Postgres (`select date '…' - date '2000-01-01'`).
    #[test]
    fn day_counts_match_postgres() {
        for (text, days) in [
            ("2000-01-01", 0),
            ("0001-01-01", -730_119),
            ("9999-12-31", 2_921_939),
            ("2024-02-29", 8_825),
            ("1970-01-01", -10_957),
            ("1999-12-31", -1),
        ] {
            assert_eq!(days_of_canonical_date(text), Some(days), "{text}");
        }
        assert_eq!(days_of_canonical_date("2024-1-5"), None);
        assert_eq!(days_of_canonical_date("infinity"), None);
    }

    /// A timestamp literal's microseconds, with and without a fraction.
    #[test]
    fn timestamp_micros_match_postgres() {
        // select extract(epoch from timestamp '…' - timestamp '2000-01-01') * 1000000
        for (text, micros) in [
            ("2000-01-01 00:00:00", 0),
            ("2000-01-01 00:00:01", 1_000_000),
            ("2000-01-01 00:00:00.5", 500_000),
            ("2000-01-01 00:00:00.000001", 1),
            ("1999-12-31 23:59:59.999999", -1),
            ("2024-02-29 12:00:00", 762_523_200_000_000),
        ] {
            assert_eq!(micros_of_canonical_timestamp(text), Some(micros), "{text}");
        }
        assert_eq!(micros_of_canonical_timestamp("2000-01-01"), None);
        assert_eq!(
            midnight_micros(days_of_canonical_date("2024-02-29").unwrap()),
            762_480_000_000_000
        );
    }

    /// The bounds are the days Postgres's limits fall on: the ends of `date`
    /// and of what casts to `timestamp`, read from Postgres
    /// (`select date '5874897-12-31' - date '2000-01-01'`).
    #[test]
    fn the_bounds_are_the_days_postgres_limits_fall_on() {
        assert_eq!(MIN_DATE_DAYS, -2_451_545);
        assert_eq!(MAX_DATE_DAYS, 2_145_031_948);
        assert_eq!(MAX_TIMESTAMP_DATE_DAYS, 106_751_982);
    }
}
