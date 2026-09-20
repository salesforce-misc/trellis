//! Postgres's **temporal** types — `date`, `time`, `timetz`, `timestamp`,
//! `timestamptz` and `interval` (issue #113) — given the ordering and
//! arithmetic they need to hold key, `GROUP BY`, primary-key and aggregate
//! roles, on top of the passthrough classification issue #108 gave them.
//!
//! # Why there is no `ValueType::Temporal`
//!
//! Issues #111 and #112 each minted a new [`crate::defs::ValueType`] variant
//! ([`Integer(IntWidth)`](crate::defs::ValueType::Integer),
//! [`Float(FloatWidth)`](crate::defs::ValueType::Float)) because their
//! families needed a *payload*: a width that changes arithmetic results
//! (`int4 + int4` overflows where `int8 + int8` does not) and changes an
//! operator's result type. The temporal families need neither. Every role
//! this issue grants them — join/`GROUP BY`/primary key, `MIN`/`MAX`,
//! `SUM(interval)` — is a function of the family alone, which
//! [`PgType`](crate::defs::pg_type::PgType) already carries. So they stay
//! `ValueType::Other(PgType::X)` and this module supplies the behaviour,
//! rather than duplicating the enum-widening churn #111/#112 needed.
//!
//! # Text stability, which is the whole ballgame for the key roles
//!
//! Trellis matches keys by raw `::text` rendering
//! (`crate::defs::catalog::TEXT_STABLE_JOIN_KEY_TYPES`,
//! `crate::defs::eval`'s `to_rows_by_key`), so a family may hold a key role
//! only if `a::text = b::text` agrees with that family's own `=` for every
//! value. `docs/type-support.md` long marked the whole temporal block
//! `🎯 typed index`, on the assumption that #110's typed key index was the
//! prerequisite. For four of the six families that assumption was wrong, in
//! exactly the way #111 found it wrong for `oid` and #112 confirmed it was
//! *right* for floats: text-stability is a property of the rendering, not
//! of the operator set, and it has to be checked per family against a live
//! server rather than assumed from the family's name.
//!
//! Checked on PostgreSQL 17 with `DateStyle` pinned to `'ISO, YMD'` (which
//! [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`] already does):
//!
//! * **`date` — stable.** `date_out` under ISO emits `YYYY-MM-DD`, widening
//!   the year field as needed (`5874897-12-31`), suffixing ` BC`
//!   (`4713-01-01 BC`), and emitting `infinity`/`-infinity` for the two
//!   special values. Distinct `date`s render as distinct text and equal
//!   `date`s render identically, because a `date` *is* its day number and
//!   the rendering is a bijection on it. No GUC other than `DateStyle`
//!   participates.
//! * **`timestamp` — stable.** Same, plus `HH:MM:SS` and a trailing-zero-
//!   trimmed fractional part (`12:34:56.1`, never `12:34:56.100000`).
//!   Trimming is what makes it a bijection: `'12:34:56.1'` and
//!   `'12:34:56.100000'` are one value and render as one string.
//! * **`time` — stable, and not even `DateStyle`-dependent.** `time_out`
//!   emits `HH:MM:SS[.f…]` identically under `ISO`, `SQL`, `Postgres` and
//!   `German` (verified live). `24:00:00` is a distinct legal value from
//!   `00:00:00` and renders distinctly.
//! * **`timetz` — stable.** The surprise here is `timetz`'s `=`, which is
//!   *not* "same instant of day": `select '12:00:00+00'::timetz =
//!   '17:30:00+05:30'::timetz` is **false** (`timetz_cmp` returns `1`),
//!   because `timetz_cmp_internal` sorts by GMT-equivalent time *and then
//!   by zone*, so two values are equal only when both fields match. That
//!   makes `=` exactly identity on the stored `(time, zone)` pair — which
//!   is precisely what `timetz_out` renders. Bijective, therefore stable.
//! * **`timestamptz` — NOT stable, and pinning `TimeZone` does not fix it.**
//!   See "Why `timestamptz` is still refused" below.
//! * **`interval` — NOT stable, and no GUC can make it so.**
//!   `select '24 hours'::interval = '1 day'::interval` is **true**
//!   (`interval_cmp` is `0`) while `'24 hours'::interval::text` is
//!   `24:00:00` and `'1 day'::interval::text` is `1 day`. One value, two
//!   renderings — structurally the same defect as float `-0`/`0` that #112
//!   refused a key role for, except that here the equivalence classes are
//!   dense rather than a single pathological pair. `interval` can never be
//!   a text-matched key, with or without #110's typed key index changing
//!   the story (the index *would* fix it; `IntervalStyle` would not).
//!
//! ## Why `timestamptz` is still refused
//!
//! `timestamptz_out` renders the stored instant as wall-clock text in the
//! session's `TimeZone`, so the obvious move is to pin `TimeZone` to
//! `'UTC'` in [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`] the same way
//! `DateStyle` is pinned, and that *would* make the rendering a bijection
//! on the instant. Issue #113 investigated it and it does not work, for a
//! reason that has nothing to do with blast radius on application sessions
//! (Trellis owns its own connections — `pool::session_bootstrap` runs on
//! every one):
//!
//! **Trellis renders `timestamptz` on two different backends, and only
//! controls one of them.** Logical-decoding output is produced by the type's
//! own output function running *in the walsender backend*, under the
//! walsender's GUCs — verified live: the same slot peeked from a session
//! with `timezone='UTC'` yields `2024-01-01 12:00:00+00` and from one with
//! `timezone='Asia/Tokyo'` yields the Tokyo wall clock, on a server whose
//! own default is `America/New_York`. `pgwire_replication`'s
//! `ReplicationConfig` (v0.4) exposes no way to send startup runtime
//! parameters or to issue a `SET` on the replication connection, so the
//! walsender keeps the server/database/role default.
//!
//! Today those two renderers *agree*, by accident: both the pool and the
//! walsender fall back to the same server default. Pinning `TimeZone` on
//! the pool alone would replace that accidental symmetry with a guaranteed
//! asymmetry on every server whose default is not UTC — the CDC-decoded
//! text of an instant and a target-table read of the same instant would
//! disagree. That is strictly worse than the status quo, so this issue
//! declines the pin. The real unlocks are, in order of preference: a
//! replication transport that can pin session GUCs (then `TimeZone` joins
//! the constant and `timestamptz` becomes stable exactly like `date` did),
//! or #110's typed key index.
//!
//! Note the same asymmetry is a pre-existing, latent hazard for `DateStyle`
//! and `bytea_output`. It does not bite there because the pinned values
//! (`ISO` output, `hex`) are output-identical to a stock server's defaults,
//! so the walsender agrees with the pool unless an operator has deliberately
//! reconfigured the server. `TimeZone` has no such stock value to pin to.
//! `IntervalStyle` does (`postgres`), which is why this issue *does* add it.
//!
//! # `MIN`/`MAX`, and why `interval` is excluded from them
//!
//! `MIN`/`MAX` return one of their inputs verbatim, so all they need from
//! this module is an ordering — [`compare`] — never a renderer. For the
//! five families whose `=` is identity-on-the-rendering, that ordering also
//! makes the aggregate a *function of the input multiset*: a tie can only
//! be between two byte-identical strings, so which one is returned cannot
//! be observed.
//!
//! `interval` breaks that, and ADR-0013 is what makes it fatal. Verified
//! live against the multiset `{'1 day', '24 hours', '2 hours'}`:
//!
//! ```text
//! select max(v)::text from (select v from iv order by id)      -- 24:00:00
//! select max(v)::text from (select v from iv order by id desc) -- 1 day
//! ```
//!
//! Postgres's own `max(interval)` is not a function of its input — it
//! returns whichever tied representative the scan happened to see first
//! (`interval_larger` is `cmp < 0 ? arg1 : arg2`, a left fold). ADR-0013
//! establishes correctness by recomputing with independently-authored SQL
//! and comparing byte-exactly; an aggregate whose Postgres answer depends
//! on scan order cannot clear that bar no matter what Trellis computes. So
//! `MIN`/`MAX` over `interval` is refused at define time rather than shipped
//! with a known-flaky self-check. See `docs/type-support.md`.
//!
//! # `SUM(interval)`, and why it *is* invertible
//!
//! `interval` is stored as three independent integer fields — `months:
//! i32`, `days: i32`, `micros: i64` — and `interval_pl` adds them
//! fieldwise with overflow checks, applying no justification (`'1 mon' +
//! '30 days'` is `1 mon 30 days`, not `2 mons`). That makes interval
//! addition exact, commutative and associative, with exact subtraction as
//! its inverse — everything float addition is not, and the reason #112 put
//! float `SUM` on the recompute path while this issue puts interval `SUM`
//! on the delta path. Verified live: `sum(v)` over the multiset above is
//! `1 day 26:00:00` in either scan order.
//!
//! [`Interval::render`] reproduces `interval_out` under `IntervalStyle =
//! 'postgres'` exactly, including its three genuinely surprising rules:
//! pluralisation is `value != 1` (so `-1 month` renders `-1 years`-style
//! plural, `-1 mons`), a sign-flip between the date fields and the time
//! field emits an explicit `+` (`'-1 day 1 hour'` is `-1 days +01:00:00`),
//! and the time field is omitted entirely when it is zero *and* some date
//! field is not (`'1 mon 00:00:00'` is `1 mon`).

use std::cmp::Ordering;
use std::fmt;

use crate::defs::pg_type::PgType;

/// Microseconds in one second/minute/hour/day, as Postgres's own
/// `USECS_PER_*` macros define them. The day constant is what
/// `interval_cmp_value` uses to collapse an interval's three fields into one
/// comparable span, and it is also why `'24 hours' = '1 day'`.
const USECS_PER_SEC: i64 = 1_000_000;
const USECS_PER_MINUTE: i64 = 60 * USECS_PER_SEC;
const USECS_PER_HOUR: i64 = 60 * USECS_PER_MINUTE;
const USECS_PER_DAY: i64 = 24 * USECS_PER_HOUR;
/// Postgres's `DAYS_PER_MONTH` (`datetime.h`) — the fixed 30 days
/// `interval_cmp_value` charges a month, *only* for ordering. No arithmetic
/// in this module ever converts months into days.
const DAYS_PER_MONTH: i64 = 30;

/// A failure to parse Postgres's canonical output text for a temporal type.
///
/// Carries no detail beyond the family, deliberately: every caller in this
/// crate is parsing text Postgres itself produced (a CDC-decoded value, a
/// target-table read, or a literal `crate::defs::typed_literal` already
/// checked), so a parse failure means the engine's own invariants are
/// broken, not that a user typed something wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporalParseError {
    pub pg_type: PgType,
}

impl fmt::Display for TemporalParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "not a canonical Postgres `{}` rendering",
            self.pg_type.name()
        )
    }
}

impl std::error::Error for TemporalParseError {}

/// An `interval`'s arithmetic overflowed one of its three fields —
/// Postgres's `interval out of range` (SQLSTATE `22008`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntervalOutOfRange;

impl fmt::Display for IntervalOutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("interval out of range")
    }
}

impl std::error::Error for IntervalOutOfRange {}

/// Whether `pg_type` is one of the six temporal families this module
/// handles.
pub const fn is_temporal(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Date
            | PgType::Time
            | PgType::TimeTz
            | PgType::Timestamp
            | PgType::TimestampTz
            | PgType::Interval
    )
}

/// Whether `pg_type`'s canonical text rendering is a bijection on its
/// values — i.e. whether `a::text = b::text` agrees with the type's own `=`
/// for every value, which is what a raw-`::text`-matched key role requires.
///
/// See this module's doc comment for the per-family evidence. `interval` is
/// `false` because equal intervals can render differently (`'1 day'` vs
/// `'24 hours'`); `timestamptz` is `false` because its rendering depends on
/// a `TimeZone` Trellis cannot pin on the walsender that produces half of
/// it.
pub const fn is_text_stable(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Date | PgType::Time | PgType::TimeTz | PgType::Timestamp
    )
}

/// Whether `MIN`/`MAX` over `pg_type` is a function of its input multiset,
/// and therefore verifiable against an independently-authored SQL recompute
/// (ADR-0013).
///
/// True for every temporal family whose `=` is identity on the rendering —
/// which, unlike [`is_text_stable`], includes `timestamptz`: a tie there is
/// still between two values that any *single* session renders identically,
/// and `MIN`/`MAX` returns an input verbatim rather than a re-rendering, so
/// the walsender/pool `TimeZone` asymmetry that blocks the key role does not
/// make the aggregate order-dependent.
///
/// False for `interval` alone — see this module's doc comment for the live
/// demonstration that Postgres's own `max(interval)` returns different text
/// for different scan orders of the same rows.
pub const fn supports_min_max(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Date | PgType::Time | PgType::TimeTz | PgType::Timestamp | PgType::TimestampTz
    )
}

/// A temporal value's position in its family's total order, as a pair so
/// `timetz`'s two-level `timetz_cmp_internal` sort (GMT-equivalent time,
/// then zone) fits without a second comparison function.
///
/// `i128` rather than `i64` so `interval`'s span (`months * 30 days + days +
/// micros`, all at their `i32`/`i64` extremes) cannot overflow the key
/// itself — Postgres's `interval_cmp_value` widens to `INT128` for the same
/// reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OrderKey(i128, i128);

/// The sentinel [`OrderKey`] primary component for `infinity`, above every
/// finite value. `date`/`timestamp`/`timestamptz` all accept the two special
/// values, and Postgres orders them at the extremes of their type.
const INFINITY: i128 = i128::MAX;
const NEG_INFINITY: i128 = i128::MIN;

/// Orders two canonical Postgres renderings of the same temporal family,
/// reproducing that family's own comparison operator.
///
/// `None` when `pg_type` is not temporal or either text is not a canonical
/// rendering of it. Callers in this crate treat that as "leave the value
/// out of the fold", the same defensive posture `crate::defs::eval` takes
/// for a value whose type the validator should already have rejected.
///
/// Note `interval` *is* ordered here even though [`supports_min_max`]
/// refuses it: `interval_cmp` is perfectly well-defined (it compares total
/// spans, charging 30 days to a month and 24 hours to a day), and the
/// refusal is about which of several tied *renderings* an aggregate would
/// return, not about the order. Keeping the arm makes that distinction
/// testable.
pub fn compare(pg_type: PgType, a: &str, b: &str) -> Option<Ordering> {
    Some(
        order_key(pg_type, a)
            .ok()?
            .cmp(&order_key(pg_type, b).ok()?),
    )
}

/// The [`OrderKey`] for one canonical rendering.
pub fn order_key(pg_type: PgType, text: &str) -> Result<OrderKey, TemporalParseError> {
    let err = TemporalParseError { pg_type };
    match pg_type {
        PgType::Date => parse_date(text).ok_or(err),
        PgType::Timestamp | PgType::TimestampTz => parse_timestamp(pg_type, text).ok_or(err),
        PgType::Time => parse_time_of_day(text)
            .map(|us| OrderKey(us as i128, 0))
            .ok_or(err),
        PgType::TimeTz => parse_timetz(text).ok_or(err),
        PgType::Interval => Interval::parse(text).map(|iv| iv.order_key()).ok_or(err),
        _ => Err(err),
    }
}

/// `date_out` under `DateStyle = 'ISO, …'`: `infinity`, `-infinity`, or
/// `YYYY-MM-DD` with an optional ` BC` suffix and a year field that widens
/// past four digits (`5874897-12-31`).
///
/// Ordered by day number, computed as a Julian day so that the BC/AD
/// boundary (there is no year 0 in Postgres's calendar — `0001-01-01 BC` is
/// immediately followed by `0001-01-01`) orders correctly without special
/// cases.
fn parse_date(text: &str) -> Option<OrderKey> {
    match text {
        "infinity" => return Some(OrderKey(INFINITY, 0)),
        "-infinity" => return Some(OrderKey(NEG_INFINITY, 0)),
        _ => {}
    }
    let (y, m, d) = parse_ymd(text)?;
    Some(OrderKey(julian_day(y, m, d) as i128, 0))
}

/// `timestamp_out`/`timestamptz_out` under ISO: a [`parse_date`] calendar
/// part, a space, `HH:MM:SS` with an optional trailing-zero-trimmed
/// fraction, then — for `timestamptz` only — a `±HH[:MM[:SS]]` zone, and
/// then an optional ` BC`.
///
/// Ordered by microseconds from the proleptic Julian epoch, with the zone
/// (when present) subtracted so two renderings of one instant in different
/// zones compare equal. That last part is what makes `MIN`/`MAX` over
/// `timestamptz` correct even though its *key* role is refused: the ordering
/// never depends on which zone the text happens to be in.
fn parse_timestamp(pg_type: PgType, text: &str) -> Option<OrderKey> {
    match text {
        "infinity" => return Some(OrderKey(INFINITY, 0)),
        "-infinity" => return Some(OrderKey(NEG_INFINITY, 0)),
        _ => {}
    }
    let (rest, is_bc) = match text.strip_suffix(" BC") {
        Some(rest) => (rest, true),
        None => (text, false),
    };
    let (date_part, time_part) = rest.split_once(' ')?;
    let (y, m, d) = parse_ymd(date_part)?;
    let y = if is_bc { -(y - 1) } else { y };

    let (time_part, zone_secs) = if pg_type == PgType::TimestampTz {
        let (time_part, zone) = split_zone(time_part)?;
        (time_part, zone)
    } else {
        (time_part, 0)
    };
    let micros = parse_time_of_day(time_part)?;

    let days = julian_day(y, m, d) as i128;
    let total =
        days * USECS_PER_DAY as i128 + micros as i128 - zone_secs as i128 * USECS_PER_SEC as i128;
    Some(OrderKey(total, 0))
}

/// `timetz_out`: a [`parse_time_of_day`] clock reading followed by a
/// mandatory `±HH[:MM[:SS]]` zone.
///
/// Ordered exactly as `timetz_cmp_internal` does — by GMT-equivalent time
/// first, then by the zone itself, so `'12:00:00+00'` and `'17:30:00+05:30'`
/// are *not* equal despite naming the same instant of day (verified live:
/// `timetz_cmp` returns `1`). The secondary component is negated to match
/// Postgres's internal sign convention, where `zone` counts seconds **west**
/// of GMT while the rendered offset counts east.
fn parse_timetz(text: &str) -> Option<OrderKey> {
    let (time_part, zone_secs) = split_zone(text)?;
    let micros = parse_time_of_day(time_part)?;
    let gmt = micros as i128 - zone_secs as i128 * USECS_PER_SEC as i128;
    Some(OrderKey(gmt, -(zone_secs as i128)))
}

/// Splits a trailing `±HH[:MM[:SS]]` zone off `text`, returning the
/// remainder and the offset in seconds **east** of GMT (so `+05:30` is
/// `19800`).
///
/// Scans backwards for the sign rather than forwards for a `+`/`-`, because
/// the clock reading before it contains neither.
fn split_zone(text: &str) -> Option<(&str, i32)> {
    let sign_at = text.rfind(['+', '-'])?;
    let (head, zone) = text.split_at(sign_at);
    let negative = zone.starts_with('-');
    let mut parts = zone[1..].split(':');
    let hours: i32 = parse_fixed_width(parts.next()?, 2)?;
    let minutes: i32 = match parts.next() {
        Some(part) => parse_fixed_width(part, 2)?,
        None => 0,
    };
    let seconds: i32 = match parts.next() {
        Some(part) => parse_fixed_width(part, 2)?,
        None => 0,
    };
    if parts.next().is_some() || minutes > 59 || seconds > 59 {
        return None;
    }
    let total = hours * 3600 + minutes * 60 + seconds;
    Some((head, if negative { -total } else { total }))
}

/// `time_out`'s `HH:MM:SS[.f…]`, in microseconds since midnight.
///
/// `HH` may be `24` (Postgres's legal end-of-day `time` value, distinct from
/// `00:00:00`), and the fractional part is 1-6 digits — `time_out` trims
/// trailing zeros, so this never sees a padded one from Postgres, but
/// accepting a shorter-than-6 fraction is exactly what "trimmed" means.
fn parse_time_of_day(text: &str) -> Option<i64> {
    let (hms, fraction) = match text.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (text, None),
    };
    let mut parts = hms.split(':');
    let hours: i64 = parse_fixed_width(parts.next()?, 2)?;
    let minutes: i64 = parse_fixed_width(parts.next()?, 2)?;
    let seconds: i64 = parse_fixed_width(parts.next()?, 2)?;
    if parts.next().is_some() || hours > 24 || minutes > 59 || seconds > 59 {
        return None;
    }
    let mut micros = hours * USECS_PER_HOUR + minutes * USECS_PER_MINUTE + seconds * USECS_PER_SEC;
    if let Some(fraction) = fraction {
        micros += parse_fraction_micros(fraction)?;
    }
    Some(micros)
}

/// A 1-6 digit fractional-second field, scaled to microseconds.
fn parse_fraction_micros(fraction: &str) -> Option<i64> {
    if fraction.is_empty() || fraction.len() > 6 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits: i64 = fraction.parse().ok()?;
    Some(digits * 10i64.pow(6 - fraction.len() as u32))
}

/// `YYYY-MM-DD` with an optional ` BC`, returning a proleptic year (`1 BC`
/// is `0`, `2 BC` is `-1`) so arithmetic on it needs no era branch.
fn parse_ymd(text: &str) -> Option<(i64, i64, i64)> {
    let (rest, is_bc) = match text.strip_suffix(" BC") {
        Some(rest) => (rest, true),
        None => (text, false),
    };
    let mut parts = rest.split('-');
    let year_text = parts.next()?;
    // `date_out` widens the year field past four digits but never narrows
    // it below four, and never emits a sign (the era is the ` BC` suffix).
    if year_text.len() < 4 || !year_text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let year: i64 = year_text.parse().ok()?;
    let month: i64 = parse_fixed_width(parts.next()?, 2)?;
    let day: i64 = parse_fixed_width(parts.next()?, 2)?;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some((if is_bc { -(year - 1) } else { year }, month, day))
}

/// Postgres's own `date2j` (`src/backend/utils/adt/datetime.c`) — a
/// proleptic Gregorian day number. Used only for *ordering*, so the epoch it
/// counts from is irrelevant as long as it is monotone, but keeping
/// Postgres's exact formula means the BC/AD boundary and leap centuries need
/// no special-casing here.
fn julian_day(year: i64, month: i64, day: i64) -> i64 {
    let (mut y, mut m) = (year, month);
    if m > 2 {
        m += 1;
        y += 4800;
    } else {
        m += 13;
        y += 4799;
    }
    let century = y.div_euclid(100);
    let mut julian = y * 365 - 32167;
    julian += y / 4 - century + century / 4;
    julian + 7834 * m / 256 + day
}

/// Parses an exactly-`width`-digit unsigned decimal field.
fn parse_fixed_width<T: std::str::FromStr>(text: &str, width: usize) -> Option<T> {
    if text.len() != width || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// A Postgres `interval`, stored the way Postgres stores it: three
/// independent signed integer fields that are *never* normalised into one
/// another.
///
/// That independence is the whole reason `interval` behaves the way it does
/// here — it is why `'1 mon' + '30 days'` renders `1 mon 30 days` rather
/// than `2 mons`, why `SUM` is exactly invertible (each field is plain
/// checked integer addition), and why `=` is coarser than text identity
/// (comparison collapses the three fields into one span at 30 days/month and
/// 24 hours/day, but rendering does not).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    /// Postgres 17's `INTERVAL_NOEND` — the reserved all-fields-at-`MAX`
    /// triple that `interval_out` prints as `infinity`.
    ///
    /// This being a *sentinel triple* rather than a flag is what lets
    /// [`Interval`] stay three plain integers: `INTERVAL_IS_NOEND` tests
    /// all three fields, so an ordinary interval that merely saturates one
    /// of them is untouched — `select '2147483647 days'::interval::text` is
    /// `2147483647 days`, and `'-2147483648 months'` is `-178956970 years
    /// -8 mons`, neither of which is an infinity (both verified live).
    pub const INFINITY: Interval = Interval {
        months: i32::MAX,
        days: i32::MAX,
        micros: i64::MAX,
    };

    /// Postgres 17's `INTERVAL_NOBEGIN`, printing as `-infinity`.
    pub const NEG_INFINITY: Interval = Interval {
        months: i32::MIN,
        days: i32::MIN,
        micros: i64::MIN,
    };

    /// `INTERVAL_IS_NOEND`/`INTERVAL_IS_NOBEGIN`: whether this is one of
    /// the two reserved infinite values (Postgres 17+; on an older server
    /// no value can reach the triple, so this is simply never true).
    pub fn is_infinite(self) -> bool {
        self == Interval::INFINITY || self == Interval::NEG_INFINITY
    }

    /// `interval_cmp_value`: the total span in microseconds, charging
    /// [`DAYS_PER_MONTH`] to a month and 24 hours to a day. Widened to
    /// `i128` exactly as Postgres widens to `INT128`, because
    /// `i32::MAX` months in microseconds does not fit `i64`.
    pub fn order_key(self) -> OrderKey {
        if self == Interval::INFINITY {
            return OrderKey(INFINITY, 0);
        }
        if self == Interval::NEG_INFINITY {
            return OrderKey(NEG_INFINITY, 0);
        }
        let span = self.micros as i128
            + self.months as i128 * DAYS_PER_MONTH as i128 * USECS_PER_DAY as i128
            + self.days as i128 * USECS_PER_DAY as i128;
        OrderKey(span, 0)
    }

    /// `interval_pl`: fieldwise checked addition, raising
    /// [`IntervalOutOfRange`] on any field's overflow exactly as Postgres's
    /// `pg_add_s32_overflow`/`pg_add_s64_overflow` guards do (`select
    /// '2147483647 months'::interval + '1 month'::interval` is `ERROR:
    /// interval out of range`, verified live).
    ///
    /// No justification is applied, because `interval_pl` applies none:
    /// adding is not the same operation as `justify_interval`, and a `SUM`
    /// that silently justified would render text a server-side `sum()` never
    /// would.
    /// Infinities absorb, and the one pairing with no answer is an error:
    /// `select 'infinity'::interval + '1 day'::interval` is `infinity`,
    /// `'infinity' + 'infinity'` is `infinity`, and `'infinity' +
    /// '-infinity'` is `ERROR: interval out of range` — all three read off
    /// a live Postgres 17. Handled before the fieldwise addition below
    /// because the sentinel triples would otherwise overflow it and report
    /// the *right* error for the wrong reason on the first two.
    pub fn checked_add(self, other: Interval) -> Result<Interval, IntervalOutOfRange> {
        if self.is_infinite() || other.is_infinite() {
            return match (self.is_infinite(), other.is_infinite()) {
                (true, true) if self != other => Err(IntervalOutOfRange),
                (true, _) => Ok(self),
                _ => Ok(other),
            };
        }
        Ok(Interval {
            months: self
                .months
                .checked_add(other.months)
                .ok_or(IntervalOutOfRange)?,
            days: self
                .days
                .checked_add(other.days)
                .ok_or(IntervalOutOfRange)?,
            micros: self
                .micros
                .checked_add(other.micros)
                .ok_or(IntervalOutOfRange)?,
        })
    }

    /// Parses `interval_out`'s `IntervalStyle = 'postgres'` rendering — the
    /// style [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`] pins.
    ///
    /// Deliberately lenient about *which* unit words appear and in what
    /// combination (it accepts `1 year 2 mons 3 days 04:05:06` and any
    /// subset), but strict about the vocabulary, so it round-trips
    /// everything [`Self::render`] emits without also becoming a second,
    /// looser implementation of `interval_in` (which accepts `'1 week'`,
    /// `'P1Y'`, `'@ 1 day ago'` and much else `interval_out` never emits).
    pub fn parse(text: &str) -> Option<Interval> {
        match text {
            "infinity" => return Some(Interval::INFINITY),
            "-infinity" => return Some(Interval::NEG_INFINITY),
            _ => {}
        }
        let mut result = Interval::default();
        let mut tokens = text.split(' ').peekable();
        let mut saw_any = false;
        while let Some(token) = tokens.next() {
            if token.is_empty() {
                return None;
            }
            match tokens.peek() {
                // `<n> <unit>` — a date field.
                Some(unit)
                    if matches!(*unit, "year" | "years" | "mon" | "mons" | "day" | "days") =>
                {
                    let value: i64 = token.parse().ok()?;
                    let unit = tokens.next().expect("peeked");
                    match unit {
                        "year" | "years" => {
                            let months = value.checked_mul(12)?;
                            result.months =
                                result.months.checked_add(i32::try_from(months).ok()?)?;
                        }
                        "mon" | "mons" => {
                            result.months =
                                result.months.checked_add(i32::try_from(value).ok()?)?;
                        }
                        _ => {
                            result.days = result.days.checked_add(i32::try_from(value).ok()?)?;
                        }
                    }
                    saw_any = true;
                }
                // Anything else must be the single trailing time field.
                _ => {
                    result.micros = parse_interval_time(token)?;
                    saw_any = true;
                    if tokens.next().is_some() {
                        return None;
                    }
                }
            }
        }
        saw_any.then_some(result)
    }

    /// Reproduces `interval_out` under `IntervalStyle = 'postgres'`, i.e.
    /// `EncodeInterval`'s `INTSTYLE_POSTGRES` arm plus its
    /// `AddPostgresIntPart`/`AppendSeconds` helpers.
    ///
    /// Every rule below was read off a live server rather than the docs, and
    /// three of them are not guessable:
    ///
    /// * **Pluralisation is `value != 1`, not `|value| != 1`**, so `-1
    ///   month` renders `-1 mons` while `1 month` renders `1 mon`.
    /// * **A sign flip between the date fields and the time field emits an
    ///   explicit `+`**: `'-1 day 1 hour'::interval` is `-1 days +01:00:00`,
    ///   whereas `'1 day -1 hour'` is `1 day -01:00:00` (no `+` needed, the
    ///   `-` carries it). Postgres tracks this as `is_before`, set by the
    ///   last non-zero date field.
    /// * **The time field is omitted when it is zero and some date field is
    ///   not**: `'1 mon 00:00:00'::interval` is `1 mon`. An all-zero
    ///   interval is `00:00:00`.
    pub fn render(self) -> String {
        if self == Interval::INFINITY {
            return "infinity".to_string();
        }
        if self == Interval::NEG_INFINITY {
            return "-infinity".to_string();
        }
        let mut out = String::new();
        let mut is_zero = true;
        let mut is_before = false;

        let years = self.months / 12;
        let months = self.months % 12;
        for (value, unit) in [
            (years as i64, "year"),
            (months as i64, "mon"),
            (self.days as i64, "day"),
        ] {
            if value == 0 {
                continue;
            }
            if !is_zero {
                out.push(' ');
            }
            if is_before && value > 0 {
                out.push('+');
            }
            out.push_str(&value.to_string());
            out.push(' ');
            out.push_str(unit);
            if value != 1 {
                out.push('s');
            }
            is_before = value < 0;
            is_zero = false;
        }

        let micros = self.micros;
        if is_zero || micros != 0 {
            // `i64::MIN.abs()` would panic; Postgres's own fields cannot
            // reach it through `interval_in`, but a hand-built value could,
            // and `unsigned_abs` is exact for every input.
            let magnitude = micros.unsigned_abs();
            let hours = magnitude / USECS_PER_HOUR as u64;
            let minutes = (magnitude % USECS_PER_HOUR as u64) / USECS_PER_MINUTE as u64;
            let seconds = (magnitude % USECS_PER_MINUTE as u64) / USECS_PER_SEC as u64;
            let fraction = magnitude % USECS_PER_SEC as u64;
            if !is_zero {
                out.push(' ');
            }
            if micros < 0 {
                out.push('-');
            } else if is_before {
                out.push('+');
            }
            out.push_str(&format!("{hours:02}:{minutes:02}:{seconds:02}"));
            if fraction != 0 {
                let mut digits = format!("{fraction:06}");
                while digits.ends_with('0') {
                    digits.pop();
                }
                out.push('.');
                out.push_str(&digits);
            }
        }
        out
    }
}

/// `interval_out`'s time field: `[-|+]HH:MM:SS[.f…]`, where `HH` is
/// unbounded (a `9223372036854775807 usecs` interval renders
/// `2562047788:00:54.775807`) and the sign, when present, applies to the
/// whole field.
fn parse_interval_time(text: &str) -> Option<i64> {
    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (hms, fraction) = match rest.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (rest, None),
    };
    let mut parts = hms.split(':');
    let hours_text = parts.next()?;
    if hours_text.len() < 2 || !hours_text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hours: i64 = hours_text.parse().ok()?;
    let minutes: i64 = parse_fixed_width(parts.next()?, 2)?;
    let seconds: i64 = parse_fixed_width(parts.next()?, 2)?;
    if parts.next().is_some() || minutes > 59 || seconds > 59 {
        return None;
    }
    let mut micros = hours.checked_mul(USECS_PER_HOUR)?;
    micros = micros.checked_add(minutes * USECS_PER_MINUTE)?;
    micros = micros.checked_add(seconds * USECS_PER_SEC)?;
    if let Some(fraction) = fraction {
        micros = micros.checked_add(parse_fraction_micros(fraction)?)?;
    }
    Some(if negative { -micros } else { micros })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmp(pg_type: PgType, a: &str, b: &str) -> Ordering {
        compare(pg_type, a, b).unwrap_or_else(|| panic!("{pg_type}: {a} vs {b} must compare"))
    }

    #[test]
    fn date_orders_bc_before_ad_and_infinities_at_the_extremes() {
        use PgType::Date as D;
        assert_eq!(cmp(D, "0001-01-01 BC", "0001-01-01"), Ordering::Less);
        assert_eq!(cmp(D, "4713-01-01 BC", "0001-01-01 BC"), Ordering::Less);
        assert_eq!(cmp(D, "2024-01-01", "2024-01-02"), Ordering::Less);
        assert_eq!(cmp(D, "2024-02-29", "2024-03-01"), Ordering::Less);
        assert_eq!(cmp(D, "2024-01-01", "5874897-12-31"), Ordering::Less);
        assert_eq!(cmp(D, "-infinity", "4713-01-01 BC"), Ordering::Less);
        assert_eq!(cmp(D, "5874897-12-31", "infinity"), Ordering::Less);
        assert_eq!(cmp(D, "infinity", "infinity"), Ordering::Equal);
        assert_eq!(cmp(D, "2024-01-01", "2024-01-01"), Ordering::Equal);
    }

    #[test]
    fn timestamp_orders_by_instant_including_the_fraction() {
        use PgType::Timestamp as T;
        assert_eq!(
            cmp(T, "2024-01-01 00:00:00", "2024-01-01 00:00:01"),
            Ordering::Less
        );
        assert_eq!(
            cmp(T, "2024-01-01 00:00:00.5", "2024-01-01 00:00:00.45"),
            Ordering::Greater
        );
        assert_eq!(
            cmp(T, "2024-01-01 00:00:00.1", "2024-01-01 00:00:00.1"),
            Ordering::Equal
        );
        assert_eq!(
            cmp(T, "4714-11-24 00:00:00 BC", "2024-01-01 00:00:00"),
            Ordering::Less
        );
        assert_eq!(
            cmp(T, "294276-12-31 23:59:59.999999", "infinity"),
            Ordering::Less
        );
    }

    /// The zone is subtracted, so two renderings of one instant compare
    /// equal — which is exactly why `MIN`/`MAX` over `timestamptz` is a
    /// function of its input even though its key role is refused.
    #[test]
    fn timestamptz_orders_by_instant_across_zones() {
        use PgType::TimestampTz as T;
        assert_eq!(
            cmp(T, "2024-01-01 12:00:00+00", "2024-01-01 07:00:00-05"),
            Ordering::Equal
        );
        assert_eq!(
            cmp(T, "2024-01-01 17:45:00+05:45", "2024-01-01 12:00:01+00"),
            Ordering::Less
        );
    }

    #[test]
    fn time_orders_including_the_legal_end_of_day() {
        use PgType::Time as T;
        assert_eq!(cmp(T, "00:00:00", "24:00:00"), Ordering::Less);
        assert_eq!(cmp(T, "12:34:56.1", "12:34:56.09"), Ordering::Greater);
        assert_eq!(cmp(T, "12:34:56", "12:34:56"), Ordering::Equal);
    }

    /// Pinned against a live `timetz_cmp`, which returns `1` for this pair:
    /// same GMT-equivalent time, different zone, so they are *not* equal.
    #[test]
    fn timetz_breaks_a_gmt_tie_by_zone_like_postgres() {
        use PgType::TimeTz as T;
        assert_eq!(cmp(T, "12:00:00+00", "17:30:00+05:30"), Ordering::Greater);
        assert_eq!(cmp(T, "12:00:00+00", "12:00:00+00"), Ordering::Equal);
        assert_eq!(cmp(T, "11:00:00+00", "17:30:00+05:30"), Ordering::Less);
        assert_eq!(
            cmp(T, "12:00:00+05:30:15", "12:00:00+05:30"),
            Ordering::Less
        );
    }

    /// `interval_cmp` compares total spans, charging 30 days to a month and
    /// 24 hours to a day — so these pairs are *equal* despite rendering
    /// differently, which is the whole reason `interval` is refused every
    /// key role and `MIN`/`MAX`.
    #[test]
    fn interval_compares_by_span_not_by_field() {
        use PgType::Interval as I;
        assert_eq!(cmp(I, "24:00:00", "1 day"), Ordering::Equal);
        assert_eq!(cmp(I, "30 days", "1 mon"), Ordering::Equal);
        assert_eq!(cmp(I, "1 day", "25:00:00"), Ordering::Less);
        assert_eq!(cmp(I, "-1 days", "00:00:00"), Ordering::Less);
    }

    #[test]
    fn non_temporal_and_non_canonical_text_do_not_compare() {
        assert_eq!(compare(PgType::Jsonb, "1", "2"), None);
        assert_eq!(compare(PgType::Date, "2024-1-5", "2024-01-06"), None);
        assert_eq!(compare(PgType::Date, "today", "2024-01-06"), None);
        assert_eq!(compare(PgType::Timestamp, "2024-01-01", "2024-01-02"), None);
        assert_eq!(compare(PgType::TimeTz, "12:00:00", "13:00:00"), None);
    }

    /// Every string here is `interval_out`'s own rendering, copied from a
    /// live `select (x::interval)::text` run — so this pins both halves of
    /// the round trip against the oracle rather than against itself.
    const INTERVAL_RENDERINGS: &[(&str, i32, i32, i64)] = &[
        ("00:00:00", 0, 0, 0),
        ("1 year", 12, 0, 0),
        ("2 years", 24, 0, 0),
        ("1 mon", 1, 0, 0),
        ("2 mons", 2, 0, 0),
        ("1 year 1 mon", 13, 0, 0),
        ("-1 years -1 mons", -13, 0, 0),
        ("11 mons", 11, 0, 0),
        ("-11 mons", -11, 0, 0),
        ("1 day", 0, 1, 0),
        ("2 days", 0, 2, 0),
        ("-1 days", 0, -1, 0),
        ("01:00:00", 0, 0, 3_600_000_000),
        ("01:02:03", 0, 0, 3_723_000_000),
        ("-01:02:03", 0, 0, -3_723_000_000),
        ("00:00:00.5", 0, 0, 500_000),
        ("-00:00:00.5", 0, 0, -500_000),
        ("00:00:00.000001", 0, 0, 1),
        ("-00:00:00.000001", 0, 0, -1),
        ("00:00:01.12", 0, 0, 1_120_000),
        ("100:00:00", 0, 0, 360_000_000_000),
        ("1 year 1 mon 1 day 01:00:00", 13, 1, 3_600_000_000),
        (
            "-1 years -1 mons -1 days -01:00:00",
            -13,
            -1,
            -3_600_000_000,
        ),
        ("1 day -01:00:00", 0, 1, -3_600_000_000),
        ("-1 days +01:00:00", 0, -1, 3_600_000_000),
        ("-1 mons +1 day", -1, 1, 0),
        ("1 mon -1 days +01:00:00", 1, -1, 3_600_000_000),
        ("-1 mons -1 days +01:00:00", -1, -1, 3_600_000_000),
        ("1 mon", 1, 0, 0),
        ("1 day 26:00:00", 0, 1, 93_600_000_000),
        ("178000000 years", 2_136_000_000, 0, 0),
        ("-178956970 years -8 mons", i32::MIN, 0, 0),
        ("2147483647 days", 0, i32::MAX, 0),
        ("-2147483648 days", 0, i32::MIN, 0),
        ("2562047788:00:54.775807", 0, 0, i64::MAX),
        ("1 day 00:00:00.5", 0, 1, 500_000),
        ("-1 days -00:00:00.000001", 0, -1, -1),
        // Postgres 17's infinite intervals, which are ordinary sentinel
        // triples rather than a separate representation.
        ("infinity", i32::MAX, i32::MAX, i64::MAX),
        ("-infinity", i32::MIN, i32::MIN, i64::MIN),
    ];

    /// `select 'infinity'::interval + '1 day'::interval` is `infinity`,
    /// `+ 'infinity'` is `infinity`, and `+ '-infinity'` is `ERROR:
    /// interval out of range` — all three on a live Postgres 17.
    #[test]
    fn infinite_intervals_absorb_and_the_mixed_pair_is_an_error() {
        let inf = Interval::INFINITY;
        let neg = Interval::NEG_INFINITY;
        let day = Interval::parse("1 day").unwrap();
        assert_eq!(inf.checked_add(day), Ok(inf));
        assert_eq!(day.checked_add(inf), Ok(inf));
        assert_eq!(inf.checked_add(inf), Ok(inf));
        assert_eq!(neg.checked_add(neg), Ok(neg));
        assert_eq!(inf.checked_add(neg), Err(IntervalOutOfRange));
        assert_eq!(neg.checked_add(inf), Err(IntervalOutOfRange));
        assert_eq!(
            compare(PgType::Interval, "-infinity", "infinity"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(PgType::Interval, "2147483647 days", "infinity"),
            Some(Ordering::Less)
        );
    }

    /// `INTERVAL_IS_NOEND` tests all three fields, so an interval that
    /// merely saturates one of them is an ordinary finite value — both of
    /// these render as themselves on a live server, not as an infinity.
    #[test]
    fn a_single_saturated_field_is_not_an_infinity() {
        for (months, days, micros) in [(0, i32::MAX, 0), (i32::MIN, 0, 0), (0, 0, i64::MAX)] {
            let interval = Interval {
                months,
                days,
                micros,
            };
            assert!(!interval.is_infinite(), "{interval:?}");
            assert_ne!(interval.render(), "infinity");
            assert_ne!(interval.render(), "-infinity");
        }
    }

    #[test]
    fn interval_render_reproduces_interval_out() {
        for &(text, months, days, micros) in INTERVAL_RENDERINGS {
            let interval = Interval {
                months,
                days,
                micros,
            };
            assert_eq!(interval.render(), text, "render of {interval:?}");
        }
    }

    #[test]
    fn interval_parse_is_render_s_inverse() {
        for &(text, months, days, micros) in INTERVAL_RENDERINGS {
            assert_eq!(
                Interval::parse(text),
                Some(Interval {
                    months,
                    days,
                    micros
                }),
                "parse of {text:?}"
            );
        }
    }

    #[test]
    fn interval_parse_rejects_non_canonical_spellings() {
        for text in [
            "",
            " ",
            "1 week",
            "P1Y",
            "@ 1 day",
            "1 days 2 days 3:00:00 4:00:00",
            "1",
            "day",
            "1 day 1:00:00",
            "1 day 01:00",
            "01:00:00 1 day",
        ] {
            assert_eq!(Interval::parse(text), None, "{text:?} must not parse");
        }
    }

    /// Fieldwise, never justified: `'1 mon' + '30 days'` is `1 mon 30 days`
    /// on a live server, not `2 mons`, even though the two are `=`.
    #[test]
    fn interval_addition_is_fieldwise_and_unjustified() {
        let a = Interval::parse("1 mon").unwrap();
        let b = Interval::parse("30 days").unwrap();
        assert_eq!(a.checked_add(b).unwrap().render(), "1 mon 30 days");
        let day = Interval::parse("1 day").unwrap();
        assert_eq!(day.checked_add(day).unwrap().render(), "2 days");
    }

    #[test]
    fn interval_addition_is_commutative_and_associative() {
        let values: Vec<Interval> = ["1 mon", "30 days", "24:00:00", "-01:00:00", "1 year 1 day"]
            .iter()
            .map(|t| Interval::parse(t).unwrap())
            .collect();
        for a in &values {
            for b in &values {
                assert_eq!(a.checked_add(*b), b.checked_add(*a));
                for c in &values {
                    let left = a.checked_add(*b).unwrap().checked_add(*c).unwrap();
                    let right = a.checked_add(b.checked_add(*c).unwrap()).unwrap();
                    assert_eq!(left, right);
                }
            }
        }
    }

    /// `select '2147483647 months'::interval + '1 month'::interval` is
    /// `ERROR: interval out of range` on a live server.
    #[test]
    fn interval_addition_overflow_is_an_error_not_a_wrap() {
        let max_months = Interval {
            months: i32::MAX,
            days: 0,
            micros: 0,
        };
        let one_month = Interval {
            months: 1,
            days: 0,
            micros: 0,
        };
        assert_eq!(max_months.checked_add(one_month), Err(IntervalOutOfRange));
        let max_micros = Interval {
            months: 0,
            days: 0,
            micros: i64::MAX,
        };
        assert_eq!(
            max_micros.checked_add(Interval {
                months: 0,
                days: 0,
                micros: 1
            }),
            Err(IntervalOutOfRange)
        );
    }

    #[test]
    fn the_role_predicates_agree_with_the_module_doc() {
        for pg_type in [
            PgType::Date,
            PgType::Time,
            PgType::TimeTz,
            PgType::Timestamp,
            PgType::TimestampTz,
            PgType::Interval,
        ] {
            assert!(is_temporal(pg_type), "{pg_type}");
        }
        assert!(!is_temporal(PgType::Jsonb));

        // `interval` and `timestamptz` are the two refusals, for two
        // different reasons — and `timestamptz` keeps `MIN`/`MAX`.
        assert!(!is_text_stable(PgType::Interval));
        assert!(!is_text_stable(PgType::TimestampTz));
        assert!(!supports_min_max(PgType::Interval));
        assert!(supports_min_max(PgType::TimestampTz));
        for pg_type in [
            PgType::Date,
            PgType::Time,
            PgType::TimeTz,
            PgType::Timestamp,
        ] {
            assert!(is_text_stable(pg_type), "{pg_type}");
            assert!(supports_min_max(pg_type), "{pg_type}");
        }
    }
}
