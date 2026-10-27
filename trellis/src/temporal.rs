//! Postgres's **temporal** types — `date`, `time`, `timetz`, `timestamp`,
//! `timestamptz` and `interval` — classified for the key,
//! `GROUP BY` and primary-key roles, on top of their passthrough
//! classification.
//!
//! # Why there is no `ValueType::Temporal`
//!
//! The integer and float families each have their own [`crate::defs::ValueType`]
//! variant ([`Integer(IntWidth)`](crate::defs::ValueType::Integer),
//! [`Float(FloatWidth)`](crate::defs::ValueType::Float)) because those
//! families needed a *payload*: a width that changes arithmetic results
//! (`int4 + int4` overflows where `int8 + int8` does not) and changes an
//! operator's result type. The temporal families need neither. Every role
//! they hold — join/`GROUP BY`/primary key — is a function of the family
//! alone, which [`PgType`](crate::defs::pg_type::PgType) already carries. So
//! they stay `ValueType::Other(PgType::X)` and this module supplies the
//! classification, rather than duplicating the enum-widening churn
//! the integer and float variants carry.
//!
//! # Text stability, which is the whole ballgame for the key roles
//!
//! Trellis matches keys by raw `::text` rendering
//! (`crate::defs::join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`), so a family may hold a key role
//! only if `a::text = b::text` agrees with that family's own `=` for every
//! value. A typed key index is not a prerequisite for most of them:
//! text-stability is a property of the rendering, not of the operator set
//! (it holds for `oid` and fails for floats), and it has to be checked per
//! family against a live server rather than assumed from the family's name.
//!
//! Checked on PostgreSQL 17 with `DateStyle` pinned to `'ISO, YMD'` and
//! `TimeZone` pinned to `'UTC'` (both via
//! [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`]):
//!
//! * **`date` — stable.** `date_out` under ISO emits `YYYY-MM-DD`, widening
//!   the year field as needed (`5874897-12-31`), suffixing ` BC`
//!   (`4713-01-01 BC`), and emitting `infinity`/`-infinity` for the two
//!   special values. Distinct `date`s render as distinct text and equal
//!   `date`s render identically, because a `date` *is* its day number and
//!   the rendering is a bijection on it. No GUC other than `DateStyle`
//!   participates.
//! * **`timestamp` — a bijection under `::text`, and admitted.**
//!   `timestamp_out` under ISO is as well-behaved as `date_out` (`HH:MM:SS`
//!   plus a trailing-zero-trimmed fraction — `12:34:56.1`, never
//!   `12:34:56.100000`, which is exactly what makes it injective). The
//!   *second* requirement — see "Two renderers" below — holds because every
//!   `to_jsonb`-based row decode in the engine renders each column the same
//!   way `::text` does.
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
//! * **`timestamptz` — a bijection under `::text` once `TimeZone` is
//!   pinned, and admitted.** `timestamptz_out` renders the stored
//!   instant in the session's `TimeZone`; under a *fixed* `TimeZone` that is
//!   as injective as `timestamp_out` (the numeric UTC offset in the output
//!   changes with the instant, so two distinct instants can never collide
//!   on one wall-clock spelling). It needs two things to hold — see "Two
//!   renderers" below for the first, and "Why `timestamptz` needs a pinned
//!   `TimeZone`" for the second.
//! * **`interval` — NOT stable, and no GUC can make it so.**
//!   `select '24 hours'::interval = '1 day'::interval` is **true**
//!   (`interval_cmp` is `0`) while `'24 hours'::interval::text` is
//!   `24:00:00` and `'1 day'::interval::text` is `1 day`. One value, two
//!   renderings — structurally the same defect as float `-0`/`0` that is
//!   refused a key role, except that here the equivalence classes are
//!   dense rather than a single pathological pair. `interval` can never be
//!   a text-matched key (a typed key index *would* fix it; `IntervalStyle`
//!   would not).
//!
//! ## Two renderers
//!
//! Everything above measures one renderer: `<col>::text`. That is necessary
//! but not sufficient: it only helps if **every path that turns a column
//! into text renders it the same way.** A whole-row render such as
//! `to_jsonb(t.*)` followed by `jsonb_each_text` would not (this is what the
//! live-row reads in `staging::apply`'s `live_rows_query`, in
//! `staging::quarantine`'s column sweep, and the old-image captures in
//! `staging::apply` must avoid).
//! `to_jsonb` does not call the type's output function for datetimes; it uses
//! `jsonb`'s own ISO-8601 writer. Read off a live server, one row, one
//! session, both renderings side by side:
//!
//! ```text
//!  column       ::text                   to_jsonb
//!  date         2024-06-15               2024-06-15                   same
//!  time         12:34:56                 12:34:56                     same
//!  timetz       12:34:56+00              12:34:56+00                  same
//!  interval     1 day 02:00:00           1 day 02:00:00               same
//!  timestamp    2024-06-15 12:34:56      2024-06-15T12:34:56          DIFFERS
//!  timestamptz  2024-06-15 12:34:56+00   2024-06-15T12:34:56+00:00    DIFFERS
//! ```
//!
//! (`numeric`, the integer widths, `uuid`, `bytea` and the floats were swept
//! too and all agree — this is a datetime-specific quirk of `to_jsonb`, not a
//! general property of it.)
//!
//! The `T` is not cosmetic. A `timestamp` `GROUP BY` key seeded once through
//! a backfill (`::text`) and once through CDC-then-live-read (`to_jsonb`)
//! would become **two target rows for one Postgres group**: a `timestamp`
//! group key seeded both ways yielded `total = 20` where the correct answer
//! is `15`.
//!
//! So no engine call site uses `to_jsonb(t.*)`: each uses an explicit
//! per-column `jsonb_build_object('<col>', <col>::text, ...)`
//! (`staging::apply::row_as_text_jsonb_sql`, over each
//! table's live `pg_catalog` column list, or — for `apply_target`'s old-image
//! capture — the target's own known `pk` + field columns), so every code path
//! Trellis itself uses to turn a row into text renders `timestamp`
//! exactly the way `::text` does. Postgres's own bare `to_jsonb(t.*)` still
//! spells a raw timestamp with a `T` (that is a Postgres builtin, not
//! something this crate can or needs to change) — nothing in the engine
//! calls it that way. `date`, `time`, `timetz` and `timestamp` hold their
//! full key roles on this basis; `timestamptz` needs a second, independent
//! guarantee on top of this one — see below.
//!
//! Note the shape of this defect: one value, two renderers, no arbiter.
//! `timestamptz` has a second such pair, addressed next.
//!
//! ## Why `timestamptz` needs a pinned `TimeZone`
//!
//! `timestamptz_out` renders the stored instant as wall-clock text in the
//! session's `TimeZone`, so it is a bijection on the instant only when every
//! renderer pins the same `TimeZone`. Trellis renders a value in two places:
//! on its own connections (`pool::session_bootstrap` runs on every one), and
//! in a capture function running inside the application's session, whose
//! `TimeZone` Trellis doesn't control. Trellis pins `TimeZone = 'UTC'`
//! in [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`], and every capture
//! function pins that same constant as `SET` clauses
//! (`crate::capture::sql::pinned_output_settings`), so both renderers agree
//! whatever the server, database, role or session default is. That is why
//! `timestamptz` joins `timestamp` in [`is_bijective_under_text`]/
//! [`is_render_consistent`] below.

use crate::defs::pg_type::PgType;

/// Whether `pg_type`'s text rendering is a bijection on its values **and is
/// the same bijection on every renderer inside the engine** — the property
/// a raw-`::text`-matched key role actually requires.
///
/// Both halves are load-bearing and the second one used to be the trap. The
/// first half (`a::text = b::text` agrees with the type's own `=`) is what
/// this module's doc comment establishes per family, and by that measure
/// `date`, `timestamp`, `time`, `timetz` and — once `TimeZone` is pinned —
/// `timestamptz` all pass. The second half — that every code path which
/// turns a column into text produces *that same* string — is what
/// `timestamp` would fail if an internal renderer disagreed (`to_jsonb`), and
/// what `timestamptz` would fail for a second, independent reason on top of
/// that (renderers that didn't all pin `TimeZone`). Both hold, which is why
/// `timestamptz` joins `date`/`time`/`timetz`/`timestamp` below.
///
/// See "Two renderers" in this module's
/// doc comment. `is_render_consistent` is the predicate for the second half
/// alone; this one is the conjunction, and the
/// `admission_is_exactly_the_conjunction_of_both_halves` test pins that it
/// stays one.
pub const fn is_text_stable(pg_type: PgType) -> bool {
    is_bijective_under_text(pg_type) && is_render_consistent(pg_type)
}

/// Half one: `a::text = b::text` agrees with `pg_type`'s own `=` for every
/// value, so a single renderer's output can stand in for the value.
///
/// `interval` is the only temporal family that fails this outright — equal
/// intervals can render differently (`'1 day'` vs `'24 hours'`), which no
/// GUC and no second renderer can repair. `timestamptz` would fail this
/// *across sessions* whenever their `TimeZone`s differed — a different
/// failure from `interval`'s, structurally "two renderers disagree" rather
/// than "one value, two spellings". Trellis pins `TimeZone = 'UTC'` on every
/// connection it opens and in every capture function
/// (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` — see that constant's doc
/// comment), which closes the cross-session gap: `timestamptz_out` under a
/// fixed `TimeZone` is a bijection on the stored instant, so `timestamptz`
/// is in the admitted list below.
const fn is_bijective_under_text(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Date | PgType::Time | PgType::TimeTz | PgType::Timestamp | PgType::TimestampTz
    )
}

/// Half two: every renderer the engine uses agrees on `pg_type`'s text.
///
/// Trellis must not have **two** internal renderers. Most paths use
/// `<col>::text`; a live-row read that built a whole row body at once with
/// `to_jsonb(t.*)` plus `jsonb_each_text` (as `staging::apply`'s
/// `live_rows_query`, `staging::quarantine`'s column sweep, and the
/// old-image captures could) would add a second.
/// `to_jsonb` renders a `timestamp` through `jsonb`'s own ISO-8601 datetime
/// writer, not through `timestamp_out`, and the two disagree (read
/// off a live server):
///
/// ```text
///  column     ::text                        to_jsonb
///  date       2024-06-15                    2024-06-15          same
///  time       12:34:56                      12:34:56            same
///  timetz     12:34:56+00                   12:34:56+00         same
///  interval   1 day 02:00:00                1 day 02:00:00      same
///  timestamp  2024-06-15 12:34:56           2024-06-15T12:34:56   DIFFERS
///  timestamptz 2024-06-15 12:34:56+00       2024-06-15T12:34:56+00:00  DIFFERS
/// ```
///
/// The `T` separator was not cosmetic. A `timestamp` key seeded once through
/// a backfill (`::text`) and once through CDC-then-live-read (`to_jsonb`)
/// would land as two distinct target rows for one Postgres group.
///
/// This is the same *shape* of defect as an unpinned `TimeZone` on one
/// renderer: one value, two renderers, no arbiter. The `to_jsonb` sites are
/// reconciled with `::text` (`staging::apply::row_as_text_jsonb_sql`, an
/// explicit per-column `jsonb_build_object` in place of `to_jsonb(t.*)`),
/// which is why `timestamp` is admitted here. `timestamptz` must pass *twice
/// over* — `to_jsonb` vs. `::text` (same as `timestamp`) and a pinned
/// `TimeZone` (the pool and every capture function pin
/// `crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`, so both render
/// `timestamptz_out` under `TimeZone = 'UTC'`). Both hold, so `timestamptz`
/// joins `date`/`time`/`timetz`/`timestamp`.
pub const fn is_render_consistent(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Date
            | PgType::Time
            | PgType::TimeTz
            | PgType::Interval
            | PgType::Timestamp
            | PgType::TimestampTz
    )
}
