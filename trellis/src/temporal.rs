//! Postgres's **temporal** types — `date`, `time`, `timetz`, `timestamp`,
//! `timestamptz` and `interval` (issue #113) — classified for the key,
//! `GROUP BY` and primary-key roles, on top of the passthrough
//! classification issue #108 gave them.
//!
//! # Why there is no `ValueType::Temporal`
//!
//! Issues #111 and #112 each minted a new [`crate::defs::ValueType`] variant
//! ([`Integer(IntWidth)`](crate::defs::ValueType::Integer),
//! [`Float(FloatWidth)`](crate::defs::ValueType::Float)) because their
//! families needed a *payload*: a width that changes arithmetic results
//! (`int4 + int4` overflows where `int8 + int8` does not) and changes an
//! operator's result type. The temporal families need neither. Every role
//! they hold — join/`GROUP BY`/primary key — is a function of the family
//! alone, which [`PgType`](crate::defs::pg_type::PgType) already carries. So
//! they stay `ValueType::Other(PgType::X)` and this module supplies the
//! classification, rather than duplicating the enum-widening churn
//! #111/#112 needed.
//!
//! # Text stability, which is the whole ballgame for the key roles
//!
//! Trellis matches keys by raw `::text` rendering
//! (`crate::defs::join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`,
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
//! Checked on PostgreSQL 17 with `DateStyle` pinned to `'ISO, YMD'` and, as
//! of issue #246, `TimeZone` pinned to `'UTC'` (both via
//! [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`]):
//!
//! * **`date` — stable.** `date_out` under ISO emits `YYYY-MM-DD`, widening
//!   the year field as needed (`5874897-12-31`), suffixing ` BC`
//!   (`4713-01-01 BC`), and emitting `infinity`/`-infinity` for the two
//!   special values. Distinct `date`s render as distinct text and equal
//!   `date`s render identically, because a `date` *is* its day number and
//!   the rendering is a bijection on it. No GUC other than `DateStyle`
//!   participates.
//! * **`timestamp` — a bijection under `::text`, and now admitted.**
//!   `timestamp_out` under ISO is as well-behaved as `date_out` (`HH:MM:SS`
//!   plus a trailing-zero-trimmed fraction — `12:34:56.1`, never
//!   `12:34:56.100000`, which is exactly what makes it injective). It used to
//!   fail the *second* requirement — see "Two renderers, and `timestamp`'s
//!   issue #248 fix" below — which issue #248 closed by making every
//!   `to_jsonb`-based row decode in the engine render each column the same
//!   way `::text` does, rather than pinning `timestamp` down further.
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
//!   pinned, and now admitted.** `timestamptz_out` renders the stored
//!   instant in the session's `TimeZone`; under a *fixed* `TimeZone` that is
//!   as injective as `timestamp_out` (the numeric UTC offset in the output
//!   changes with the instant, so two distinct instants can never collide
//!   on one wall-clock spelling). It used to fail for two compounding
//!   reasons — see "Two renderers, and `timestamp`'s issue #248 fix" below
//!   for the first, and "Why `timestamptz` needed issue #246, separately"
//!   for the second — both now closed.
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
//! ## Two renderers, and `timestamp`'s issue #248 fix
//!
//! Everything above measures one renderer: `<col>::text`. That is necessary
//! and it used not to be sufficient, because **Trellis used to turn columns
//! into text in two different ways.** Most paths cast per column, but the
//! live-row reads used to build a whole row body at once — `to_jsonb(t.*)`
//! followed by `jsonb_each_text` — in `staging::apply`'s
//! `read_live_rows_batch`, in `staging::quarantine`'s column sweep, and in the
//! `RETURNING to_jsonb(t.*)::text` old-image captures `staging::apply` used for
//! issue #196.
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
//! used to become **two target rows for one Postgres group** — reproduced
//! during #113's review: a `timestamp` group key seeded both ways yielded
//! `total = 20` where the correct answer is `15`.
//!
//! **Issue #248 fixed this** by replacing every `to_jsonb(t.*)` call site
//! above with an explicit per-column `jsonb_build_object('<col>',
//! <col>::text, ...)` (`staging::apply::row_as_text_jsonb_sql`, over each
//! table's live `pg_catalog` column list, or — for `apply_target`'s old-image
//! capture — the target's own known `pk` + field columns), so every code path
//! Trellis itself uses to turn a row into text now renders `timestamp`
//! exactly the way `::text` does. Postgres's own bare `to_jsonb(t.*)` still
//! spells a raw timestamp with a `T` (that is a Postgres builtin, not
//! something this crate can or needs to change) — the fix is that nothing in
//! the engine calls it that way anymore. `date`, `time`, `timetz` and
//! `timestamp` shipped with their full key roles from #248; `timestamptz`
//! needed a second, independent fix on top of this one — see below.
//!
//! Note the shape of this defect: one value, two renderers, no arbiter.
//! `timestamptz` had a second such pair, addressed next.
//!
//! ## Why `timestamptz` needed issue #246, separately
//!
//! `timestamptz_out` renders the stored instant as wall-clock text in the
//! session's `TimeZone`, so it is a bijection on the instant only when every
//! renderer pins the same `TimeZone`. Trellis renders a value in two places:
//! on its own connections (`pool::session_bootstrap` runs on every one), and
//! in a capture function running inside the application's session, whose
//! `TimeZone` Trellis doesn't control. Issue #246 pinned `TimeZone = 'UTC'`
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
/// `timestamp` used to fail, before issue #248 made every internal renderer
/// agree, and what `timestamptz` used to fail for a second, independent
/// reason on top of that (issue #246: its renderers didn't all pin
/// `TimeZone`). Both issues are now closed, which is why `timestamptz` joins
/// `date`/`time`/`timetz`/`timestamp` below.
///
/// See "Two renderers, and `timestamp`'s issue #248 fix" in this module's
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
/// GUC and no second renderer can repair. `timestamptz` used to fail this
/// *across sessions* whenever their `TimeZone`s differed — a different
/// failure from `interval`'s, structurally "two renderers disagree" rather
/// than "one value, two spellings" — which is why, back when nothing pinned
/// `TimeZone` at all, it was recorded on [`is_render_consistent`] instead of
/// here. Issue #246 pins `TimeZone = 'UTC'` on every connection Trellis
/// opens and in every capture function
/// (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` — see that constant's doc
/// comment), which closes the cross-session gap: `timestamptz_out` under a
/// fixed `TimeZone` is a bijection on the stored instant. `timestamptz`
/// joins the admitted list below as of issue #246.
const fn is_bijective_under_text(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Date | PgType::Time | PgType::TimeTz | PgType::Timestamp | PgType::TimestampTz
    )
}

/// Half two: every renderer the engine uses agrees on `pg_type`'s text.
///
/// Trellis used to have **two** internal renderers, not one. Most paths use
/// `<col>::text`, but the live-row reads used to build a whole row body at
/// once with `to_jsonb(t.*)` plus `jsonb_each_text` — `staging::apply`'s
/// `read_live_rows_batch`, `staging::quarantine`'s column sweep, and the
/// `RETURNING to_jsonb(t.*)::text` old-image captures issue #196 relies on.
/// `to_jsonb` renders a `timestamp` through `jsonb`'s own ISO-8601 datetime
/// writer, not through `timestamp_out`, and the two used to disagree (read
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
/// used to land as two distinct target rows for one Postgres group.
///
/// This is the same *shape* of defect issue #246 fixed one layer out
/// (`TimeZone` not pinned on every renderer): one value, two renderers, no
/// arbiter. **Issue #248 reconciled the `to_jsonb` sites with `::text`**
/// (`staging::apply::row_as_text_jsonb_sql`, an explicit per-column
/// `jsonb_build_object` in place of `to_jsonb(t.*)`), which is why `timestamp`
/// was admitted here first. `timestamptz` used to fail this predicate *twice
/// over* — `to_jsonb` vs. `::text` (closed by #248, same as `timestamp`) and an
/// unpinned `TimeZone` (closed by #246: the pool and every capture function pin
/// `crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`, so both render
/// `timestamptz_out` under `TimeZone = 'UTC'`). Both closed, so `timestamptz`
/// now joins `date`/`time`/`timetz`/`timestamp`.
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
