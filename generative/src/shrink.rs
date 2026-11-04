//! How long a property's failure may shrink before proptest reports it.
//!
//! A case that fails by timing out (a quiesce, a convergence wait, a bounded
//! shutdown) costs its whole wait again on every shrink step, and proptest's
//! default shrink has no time limit. A deep run whose failing case took 30 s
//! shrank for hours, used up its round, and printed no error and saved no seed
//! (#1641). Each property's config sets `max_shrink_time` from here, so a
//! failure reports, and saves its seed, after at most this long.

/// The shrink budget a property gets unless `PROPTEST_MAX_SHRINK_TIME`
/// (milliseconds, proptest's own variable) says otherwise: five minutes,
/// several times a normal shrink and a quarter of a nightly round.
pub const DEFAULT_MAX_SHRINK_TIME_MS: u32 = 5 * 60 * 1000;

/// The `max_shrink_time` (milliseconds) for a property's config:
/// `PROPTEST_MAX_SHRINK_TIME` when set (`0` lifts the limit, as in proptest),
/// otherwise [`DEFAULT_MAX_SHRINK_TIME_MS`].
pub fn max_shrink_time_ms() -> u32 {
    std::env::var("PROPTEST_MAX_SHRINK_TIME")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_SHRINK_TIME_MS)
}
