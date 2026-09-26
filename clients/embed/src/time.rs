//! `SystemTime` crosses as signed microseconds since the Unix epoch, which a
//! host rehydrates to its own `DateTime`/`Time` (ADR-0010 decision 4).
//! Microseconds are Postgres's own `timestamptz` resolution, so every time
//! `trellis` reads back from its catalog crosses exactly.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use trellis::ErrorCode;

use crate::PlainError;

/// `time` as microseconds since the Unix epoch, negative before it.
///
/// Rounds toward negative infinity, so a pre-epoch time with a sub-microsecond
/// part lands on the earlier microsecond, the same as a post-epoch one does.
/// Saturates at `i64::MIN`/`i64::MAX`, about 292,000 years either side of
/// 1970. Postgres's `timestamptz` reaches a little further (to 294276 AD), so
/// a time in its last ~2,000 years would cross as `i64::MAX`; nothing
/// `trellis` reports (creation, poison and pause times) comes near it.
pub fn epoch_micros(time: SystemTime) -> i64 {
    let micros: i128 = match time.duration_since(UNIX_EPOCH) {
        // A `Duration` holds at most ~1.8e25 microseconds, well inside `i128`.
        Ok(after) => after.as_micros() as i128,
        Err(before) => {
            let before = before.duration();
            let whole = before.as_micros() as i128;
            let partial = i128::from(before.subsec_nanos() % 1_000 != 0);
            -(whole + partial)
        }
    };
    i64::try_from(micros).unwrap_or(if micros < 0 { i64::MIN } else { i64::MAX })
}

/// The inverse of [`epoch_micros`], for a time a host hands in (for example
/// `poisoned_since`'s watermark). A `validation` error if the platform's
/// `SystemTime` can't represent it.
pub fn system_time_from_epoch_micros(micros: i64) -> Result<SystemTime, PlainError> {
    let offset = Duration::from_micros(micros.unsigned_abs());
    let time = if micros >= 0 {
        UNIX_EPOCH.checked_add(offset)
    } else {
        UNIX_EPOCH.checked_sub(offset)
    };
    time.ok_or_else(|| {
        PlainError::new(
            ErrorCode::Validation,
            format!("{micros} microseconds from the Unix epoch is out of range"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_is_zero() {
        assert_eq!(epoch_micros(UNIX_EPOCH), 0);
        assert_eq!(system_time_from_epoch_micros(0).unwrap(), UNIX_EPOCH);
    }

    #[test]
    fn times_round_trip_on_both_sides_of_the_epoch() {
        for micros in [
            1,
            -1,
            1_727_222_400_123_456,
            -1_727_222_400_123_456,
            // Postgres's earliest `timestamptz`, 4713 BC.
            -210_866_803_200_000_000,
            i64::MAX,
            i64::MIN,
        ] {
            let time = system_time_from_epoch_micros(micros).unwrap();
            assert_eq!(epoch_micros(time), micros, "{micros}");
        }
    }

    #[test]
    fn sub_microsecond_parts_round_down() {
        assert_eq!(epoch_micros(UNIX_EPOCH + Duration::from_nanos(1_999)), 1);
        assert_eq!(epoch_micros(UNIX_EPOCH - Duration::from_nanos(1)), -1);
        assert_eq!(epoch_micros(UNIX_EPOCH - Duration::from_nanos(1_001)), -2);
        assert_eq!(epoch_micros(UNIX_EPOCH - Duration::from_nanos(2_000)), -2);
    }

    #[test]
    fn a_time_beyond_i64_microseconds_saturates() {
        let far = Duration::from_secs(u64::MAX / 1_000_000 + 1);
        if let Some(later) = UNIX_EPOCH.checked_add(far) {
            assert_eq!(epoch_micros(later), i64::MAX);
        }
        if let Some(earlier) = UNIX_EPOCH.checked_sub(far) {
            assert_eq!(epoch_micros(earlier), i64::MIN);
        }
    }
}
