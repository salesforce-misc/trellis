//! Invertibility gate for aggregate delta maintenance (issue #11 groundwork).
//!
//! Issue #11 (stage 05 — apply and exactly-once deltas) wants aggregate
//! transforms maintained via INSERT/DELETE/UPDATE deltas rather than a full
//! recompute, but only for aggregates whose new value can be derived from
//! the old value plus the folded change (an "inverse"). Its invertibility
//! gate rule:
//!
//! > `count(*)`, `count(col)`, `sum`/`avg` over int/numeric are delta-able;
//! > `min`/`max` are **not** (probe-assisted recompute path); floats kept in
//! > exact decimal, `Inf`/`NaN` tracked as counts. Enumerate which measures
//! > are invertible; everything else goes on the recompute path — never
//! > approximate an inverse.
//!
//! This module implements only that classification, as a pure function over
//! [`super::registry::AGGREGATE_FUNCTIONS`] and [`super::ast::ValueType`] —
//! no dependency on an `Aggregate` [`super::ast::KeySpace`] variant, which
//! does not exist in the grammar yet (see `KeySpace`'s doc comment and
//! `crate::staging::apply`'s module doc). It is intentionally unwired: the
//! actual delta computation (folding new/old images, grain migration,
//! version fencing, `apply.rs` integration) is deferred until that grammar
//! work lands and is *not* attempted here.
//!
//! # Design decisions
//!
//! - **`COUNT(*)` vs `COUNT(col)`**: both are invertible regardless of the
//!   column's [`ValueType`] — counting rows or non-null occurrences needs no
//!   arithmetic on the value itself, just a +1/-1 per fold, so there is no
//!   type restriction to encode.
//! - **`SUM`/`AVG` over `Text`/`Boolean`/`Uuid`/`Other`**: `SUM`/`AVG` over
//!   any non-`Numeric` [`ValueType`] (including issue #108's [`ValueType::Other`]
//!   passthrough types) isn't a type error this module is positioned to
//!   raise — that belongs to the validator (`super::validate`), which
//!   type-checks aggregate arguments once the grammar accepts them ([`super::registry::AGGREGATE_FUNCTION_SPECS`]
//!   is `Numeric`-only, so this shape can't actually reach here through a
//!   parsed definition). This gate answers a narrower question ("if this
//!   aggregate call were valid, is it delta-able?"), so a non-`Numeric`
//!   argument to `SUM`/`AVG` is classified as
//!   [`Invertibility::RecomputeOnly`] rather than a distinct error variant:
//!   it is certainly not invertible, and folding it into the same
//!   recompute path as `MIN`/`MAX` means every caller has exactly one
//!   fallback to implement, rather than two (recompute vs. reject).
//! - **`MIN`/`MAX`**: never invertible, regardless of argument type — a
//!   deleted row might have held the current min/max, and there's no way to
//!   recover the next-best value from the aggregate's current state alone.
//!   These always route to the probe-assisted recompute path the issue
//!   describes.
//! - **Composite partials**: `AVG` isn't a single running value — dividing
//!   requires both the running sum and the running count, so its inverse
//!   needs both maintained as hidden partial fields. [`PartialField`]
//!   enumerates the partial-field roles a given invertible aggregate needs;
//!   `SUM`/`COUNT` need only their own value (no hidden partials), `AVG`
//!   needs `Sum` and `Count`.
//! - **`SUM`/`AVG` over a float** (issue #112, now that
//!   [`ValueType::Float`] exists): **not invertible**, unlike every exact
//!   numeric type. Issue #11's gate rule anticipated this ("floats kept in
//!   exact decimal, `Inf`/`NaN` tracked as counts"); with a real float type
//!   in hand the honest verdict is `RecomputeOnly`, because binary float
//!   addition has no exact inverse. Three independent reasons, each checked
//!   against a live server rather than reasoned from IEEE:
//!
//!   1. **It isn't associative, so a delta isn't well-defined.**
//!      `select (1e16::float8 + 1) - 1e16::float8` is `0`, while
//!      `select (1e16::float8 - 1e16::float8) + 1` is `1`. "Old sum minus
//!      the deleted row" and "recompute the group" are different numbers,
//!      and the ADR-0013 self-check compares against the latter.
//!   2. **It isn't order-independent either.** `1e16 + 1 + 1 + 1 + 1` is
//!      `1e+16` but `1 + 1 + 1 + 1 + 1e16` is `1.0000000000000004e+16`, so
//!      even a pure insert stream would drift from a server-side `sum()`
//!      whose input order differs.
//!   3. **`NaN` and `Infinity` are absorbing.** Once a group's running sum
//!      is `NaN`, no subtraction recovers it: `('NaN'::float8 + 1) -
//!      'NaN'::float8` is `NaN`, and `('Infinity'::float8 + 1) -
//!      'Infinity'::float8` is `NaN` too. Deleting the row that introduced
//!      the special value must recompute.
//!
//!   The "`Inf`/`NaN` tracked as counts" half of issue #11's note is a
//!   possible *future* refinement — track how many rows in the group are
//!   special, and delta the finite part — but it does not rescue reasons 1
//!   and 2, so it would still need a bounded-error contract this engine has
//!   not chosen to offer. Never approximate an inverse.

use super::ast::ValueType;

/// Whether an aggregate measure's new value can be derived from its old
/// value plus a folded change (invertible → delta-able), or whether it must
/// be recomputed from scratch on every affected group (recompute-only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invertibility {
    /// Delta-able: the aggregate's new value is a pure function of its old
    /// value and the folded change, so no full recompute is needed.
    Invertible,
    /// Not delta-able: the aggregate's new value cannot be derived from its
    /// old value alone (e.g. `MIN`/`MAX` after a delete) and must go through
    /// the probe-assisted recompute path instead.
    RecomputeOnly,
}

/// A hidden partial value a composite invertible aggregate must maintain
/// alongside its visible measure in order to fold new/old images (per the
/// issue's "Composite measures fold hidden partials" rule).
///
/// `SUM` and `COUNT` need none of these — their visible value is itself the
/// only state a fold needs to update. `AVG` needs both [`PartialField::Sum`]
/// and [`PartialField::Count`], since `avg = sum / count` and neither half
/// alone is invertible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialField {
    /// The running sum of the aggregated values in the group.
    Sum,
    /// The running count of rows (or non-null values) in the group.
    Count,
}

/// Whether an argument to `COUNT` is `*` (every row) or a specific column
/// (non-null values of that column) — the two forms fold identically for
/// invertibility purposes but are called out because `docs/transforms.md`
/// and issue #11 both track them as distinct forms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountArg {
    Star,
    /// `COUNT(<column>)` (issue #120): counts non-null occurrences of a
    /// specific column/expression rather than every row.
    Column,
}

/// An aggregate call's argument, as far as the invertibility gate needs to
/// know about it: `COUNT` cares whether it's `*` or a column; every other
/// aggregate in [`super::registry::AGGREGATE_FUNCTIONS`] takes exactly one
/// column argument, so it's classified by that column's [`ValueType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateArg {
    /// `COUNT(*)` or `COUNT(col)` — see [`CountArg`].
    Count(CountArg),
    /// `SUM`/`AVG`/`MIN`/`MAX` over a column of this [`ValueType`].
    Column(ValueType),
}

/// The invertibility gate's verdict for one aggregate call: whether it's
/// delta-able, and if so, what hidden partial fields its delta model must
/// maintain beyond the visible measure itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verdict {
    pub invertibility: Invertibility,
    /// Hidden partials the delta model needs to fold, in addition to the
    /// aggregate's own visible value. Empty for non-composite invertible
    /// aggregates (`SUM`, `COUNT`) and for anything [`Invertibility::RecomputeOnly`].
    pub partials: &'static [PartialField],
}

impl Verdict {
    const fn invertible(partials: &'static [PartialField]) -> Self {
        Verdict {
            invertibility: Invertibility::Invertible,
            partials,
        }
    }

    const fn recompute_only() -> Self {
        Verdict {
            invertibility: Invertibility::RecomputeOnly,
            partials: &[],
        }
    }

    pub fn is_invertible(&self) -> bool {
        self.invertibility == Invertibility::Invertible
    }
}

/// Classifies one aggregate call against the invertibility gate.
///
/// `function` is expected to be one of [`super::registry::AGGREGATE_FUNCTIONS`]
/// (`"SUM"`, `"COUNT"`, `"AVG"`, `"MIN"`, `"MAX"`), matched case-sensitively
/// against that registry's canonical uppercased spelling — callers that hold
/// a parsed identifier should uppercase it first, the same way
/// [`super::registry::lookup_function`] expects. `None` is returned for any
/// other name: this gate only knows how to classify the registry's known
/// aggregate functions, not general-purpose unknown-function rejection
/// (that's [`super::error::ParseError::UnsupportedFunction`]'s job once the
/// grammar accepts aggregate calls at all).
///
/// `arg` describes the call's single argument per [`AggregateArg`]. Passing
/// [`AggregateArg::Count`] to a non-`COUNT` function or [`AggregateArg::Column`]
/// to `COUNT` is a caller error signaled by returning `None`, since it
/// describes a call shape the grammar could never produce.
pub fn classify(function: &str, arg: AggregateArg) -> Option<Verdict> {
    match (function, arg) {
        ("COUNT", AggregateArg::Count(_)) => Some(Verdict::invertible(&[])),

        // Issue #111: an exact-integer argument is as invertible as a
        // `numeric` one, and for the same reason — `SUM`/`AVG` fold through
        // `staging::apply_aggregate`'s `numeric` running partials either
        // way (`sum(int2|int4)` is a `bigint` column fed by a `numeric`
        // delta, `avg(<int>)` is `numeric` outright). Enumerating it here
        // rather than leaving it to the `_ => None` fallback below is the
        // point: `None` means "a call shape the grammar could never
        // produce", and since #111 an integer-argument aggregate is a shape
        // the grammar produces routinely.
        ("SUM", AggregateArg::Column(ValueType::Numeric | ValueType::Integer(_))) => {
            Some(Verdict::invertible(&[]))
        }
        // Issue #113: `SUM(interval)` is **recompute-only**, landing in the
        // same arm as `SUM(<float>)` below — and the reasoning is worth
        // spelling out, because interval addition looks invertible and on
        // the ordinary values it is.
        //
        // A Postgres `interval` is three independent signed integers
        // (`months: i32`, `days: i32`, `micros: i64`) and `interval_pl`
        // adds them fieldwise, applying no justification. Over finite,
        // in-range values that addition really is exact, commutative and
        // associative with an exact inverse — every property float addition
        // lacks, and `sum(v)` over `{'1 day', '24 hours', '2 hours'}` is
        // `1 day 26:00:00` in either scan order on a live server.
        //
        // The monoid is **partial**, though, and a delta cannot represent
        // the gaps. Both failures were reproduced against Postgres 17:
        //
        // 1. **Infinities do not subtract.** `'infinity'::interval` is a
        //    legitimate value (PG 17+), `'infinity' + '1 day'` is
        //    `infinity`, and `'infinity' + '-infinity'` is `ERROR: interval
        //    out of range`. A group holding `{infinity, 1 day}` whose
        //    infinity row is then deleted has a perfectly well-defined true
        //    sum of `1 day`, but the delta model computes it as
        //    `infinity + (-infinity)` and raises — and because the delta is
        //    replayed identically on every retry, that is not a
        //    quarantine-and-continue, it is a drain that never makes
        //    progress again.
        // 2. **Overflow is scan-order dependent.** `sum(v)` over
        //    `{'2147483647 days', '1 days', '-1 days'}` raises ascending
        //    and returns `2147483647 days` descending, on the server
        //    itself. A delta accumulates in whatever order rows happen to
        //    arrive, so it can raise where the recompute it is checked
        //    against succeeds.
        //
        // Recompute-only removes both: `apply_aggregate`'s
        // `probe_recompute_fields_bulk` renders `(sum(<col>))::text` and
        // lets Postgres fold the group in one pass, so the engine raises
        // exactly when and only when a server-side `sum()` over the same
        // rows would. This is the same conclusion #112 reached for float
        // `SUM`/`AVG` by a different route — there the arithmetic is total
        // but inexact, here it is exact but partial, and either way there
        // is no inverse to delta with. Never approximate an inverse.
        (
            "SUM",
            AggregateArg::Column(
                ValueType::Float(_)
                | ValueType::Text
                | ValueType::Boolean
                | ValueType::Uuid
                | ValueType::Other(_),
            ),
        ) => Some(Verdict::recompute_only()),

        ("AVG", AggregateArg::Column(ValueType::Numeric | ValueType::Integer(_))) => {
            Some(Verdict::invertible(&[
                PartialField::Sum,
                PartialField::Count,
            ]))
        }
        (
            "AVG",
            AggregateArg::Column(
                ValueType::Float(_)
                | ValueType::Text
                | ValueType::Boolean
                | ValueType::Uuid
                | ValueType::Other(_),
            ),
        ) => Some(Verdict::recompute_only()),

        ("MIN", AggregateArg::Column(_)) | ("MAX", AggregateArg::Column(_)) => {
            Some(Verdict::recompute_only())
        }

        // Issue #119: `bool_and`/`bool_or` are **recompute-only**, for
        // exactly `MIN`/`MAX`'s reason, not `SUM`/`AVG`'s. Postgres's own
        // fold looks tempting to delta the same way `SUM` is: `bool_and` is
        // `AND` across the group, `bool_or` is `OR`, and both are
        // commutative, associative, and total over `{true, false}` — no
        // `NaN`/overflow/partial-monoid trap the float or interval `SUM`
        // stories have.
        //
        // The trap is deletion, and it is the same one `MIN`/`MAX` have: a
        // running `Invertibility::Invertible` model here would only ever
        // carry the aggregate's *current visible value* — one bit — as its
        // state (`SUM`/`COUNT`/`AVG`'s own invertible arms above need
        // nothing more than the running sum/count either). One bit is not
        // enough to invert a delete. Concretely: a group folds to
        // `bool_and = false` because it holds `{false, true, true}`; delete
        // the one `false` row and the true new value is `true` — but delete
        // one of the *other* two rows instead and the true new value is
        // still `false`. Both deletions look identical to a delta model
        // that only has "the old aggregate was `false`" to go on — exactly
        // [`Invertibility::RecomputeOnly`]'s doc comment's "a deleted row
        // might have held the current min/max, and there's no way to
        // recover the next-best value from the aggregate's current state
        // alone," verbatim, with `false`/`true` in place of a min/max
        // candidate.
        //
        // A design that tracked hidden `true`-count/`false`-count partials
        // (this *would* be genuinely invertible — `bool_and` is exactly
        // "false-count `== 0`", decrementable on delete) is real and was
        // considered, but it is a new composite-aggregate shape this
        // module's [`PartialField`] enum was never built for (`AVG`'s
        // `Sum`/`Count` partials are counting the *fold*, not one bit's
        // occurrences per value) and would need matching new plumbing in
        // `staging::apply_aggregate`'s carrier/probe machinery — a
        // deliberately larger change this issue's scope (wire the two
        // aggregates up, per the epic's own framing) does not take on
        // speculatively. Recompute-only costs nothing incremental here
        // regardless: [`super::registry::aggregate_result_type`] makes
        // `bool_and`/`bool_or` reach `staging::apply_aggregate`'s ordinary
        // `AggFieldKind::RecomputeOnly` fallback with no further wiring, the
        // same free ride `SUM(interval)` and float `SUM`/`AVG` get.
        ("BOOL_AND", AggregateArg::Column(_)) | ("BOOL_OR", AggregateArg::Column(_)) => {
            Some(Verdict::recompute_only())
        }

        // Issue #118: `bit_and`/`bit_or` are **recompute-only**, for exactly
        // `bool_and`/`bool_or`'s reason (`MIN`/`MAX`'s reason, not `SUM`'s),
        // generalized from two possible per-column values to a bit
        // string's `2^n`. Both are per-*bit-position* `AND`/`OR` folds —
        // commutative, associative, total over any fixed-width bit domain,
        // no overflow/rounding/partial-monoid trap — which looks exactly as
        // delta-able as `SUM` on first glance, and fails for the same
        // structural reason `bool_and`/`bool_or` do: the only state a
        // running `Invertible` model here could maintain is the aggregate's
        // *current visible value* (the folded bit string itself), and that
        // is not enough to invert a delete, regardless of how many bits wide
        // it is — more possible values per column doesn't rescue the
        // argument, it only makes the concrete counterexample slightly
        // bigger to write down. Verified live rather than assumed: a
        // `bit(2)` group `{01, 10, 11}` folds to `bit_and = 00`; deleting the
        // `01` row leaves `{10, 11}`, `bit_and = 10`; deleting the `10` row
        // *instead*, from the very same starting group, leaves `{01, 11}`,
        // `bit_and = 01`. Two different single-row deletions from one
        // starting aggregate (`00`) land at two different true answers (`10`
        // vs `01`), and "the old aggregate was `00`" alone cannot tell a
        // delta model which case it is in — [`Invertibility::RecomputeOnly`]'s
        // own "a deleted row might have held the current min/max, and
        // there's no way to recover the next-best value from the
        // aggregate's current state alone," verbatim, with a bit string
        // standing in for a min/max candidate. See `bool_and`/`bool_or`'s
        // own arm above for why a hidden-partials design that *would* be
        // invertible (a per-bit-position true/false population count,
        // generalizing bool_and's false-count idea to `n` independent
        // per-position counters) is real but out of this issue's scope —
        // the same `PartialField`/`staging::apply_aggregate` plumbing gap,
        // now `n`-wide instead of one bit. Recompute-only costs nothing
        // incremental to wire up: `registry::aggregate_result_type` routes
        // `bit_and`/`bit_or` into `staging::apply_aggregate`'s ordinary
        // `AggFieldKind::RecomputeOnly` fallback, the same free ride
        // `bool_and`/`bool_or` and float `SUM`/`AVG` get.
        ("BIT_AND", AggregateArg::Column(_)) | ("BIT_OR", AggregateArg::Column(_)) => {
            Some(Verdict::recompute_only())
        }

        // Issue #115: `jsonb_agg` is **recompute-only**, on `MIN`/`MAX`'s
        // reasoning rather than `SUM`'s — a running "invertible" model here
        // could only ever be the aggregate's own current folded array, and
        // there is no way to subtract one deleted row's contribution back
        // out of it (unlike `SUM`, whose running total *is* enough state to
        // invert a delete). This is the same structural trap `bool_and`/
        // `bit_and` hit, one level up: the state a delta model would need to
        // keep is the *input multiset itself*, not a summary of it, which is
        // exactly what `RecomputeOnly` already means. See `crate::jsonb`'s
        // module doc for why `jsonb_agg` is safe to admit at all despite its
        // `STABLE` marking (restricting the argument to `jsonb` itself
        // excludes the GUC-dependent hazard that marking is really about),
        // and for the residual, non-corrupting ordering caveat this
        // `RecomputeOnly` routing does *not* resolve (tracked toward #120).
        ("JSONB_AGG", AggregateArg::Column(_)) => Some(Verdict::recompute_only()),

        // Shapes the grammar could never produce: COUNT with a column-typed
        // arg description, or a non-COUNT aggregate with a `*` arg.
        ("COUNT", AggregateArg::Column(_)) => None,
        (
            "SUM" | "AVG" | "MIN" | "MAX" | "BOOL_AND" | "BOOL_OR" | "BIT_AND" | "BIT_OR"
            | "JSONB_AGG",
            AggregateArg::Count(_),
        ) => None,

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_star_is_invertible_with_no_partials() {
        let verdict = classify("COUNT", AggregateArg::Count(CountArg::Star)).unwrap();
        assert!(verdict.is_invertible());
        assert_eq!(verdict.partials, &[] as &[PartialField]);
    }

    #[test]
    fn count_column_is_invertible_with_no_partials() {
        let verdict = classify("COUNT", AggregateArg::Count(CountArg::Column)).unwrap();
        assert!(verdict.is_invertible());
        assert_eq!(verdict.partials, &[] as &[PartialField]);
    }

    #[test]
    fn sum_over_numeric_is_invertible_with_no_partials() {
        let verdict = classify("SUM", AggregateArg::Column(ValueType::Numeric)).unwrap();
        assert!(verdict.is_invertible());
        assert_eq!(verdict.partials, &[] as &[PartialField]);
    }

    #[test]
    fn avg_over_numeric_is_invertible_with_sum_and_count_partials() {
        let verdict = classify("AVG", AggregateArg::Column(ValueType::Numeric)).unwrap();
        assert!(verdict.is_invertible());
        assert_eq!(verdict.partials, &[PartialField::Sum, PartialField::Count]);
    }

    /// Issue #111: every exact-integer width classifies exactly as `numeric`
    /// does — `None` here would mean the gate had silently fallen through to
    /// its "shape the grammar could never produce" arm for a shape the
    /// grammar produces on any `SUM(<int column>)`.
    #[test]
    fn sum_and_avg_over_every_integer_width_classify_like_numeric() {
        for width in crate::integer::IntWidth::ALL {
            let arg = AggregateArg::Column(ValueType::Integer(width));
            let sum =
                classify("SUM", arg).unwrap_or_else(|| panic!("SUM over {width} must classify"));
            assert!(sum.is_invertible(), "SUM over {width}");
            assert_eq!(sum.partials, &[] as &[PartialField]);

            let avg =
                classify("AVG", arg).unwrap_or_else(|| panic!("AVG over {width} must classify"));
            assert!(avg.is_invertible(), "AVG over {width}");
            assert_eq!(avg.partials, &[PartialField::Sum, PartialField::Count]);

            for name in ["MIN", "MAX"] {
                let verdict = classify(name, arg)
                    .unwrap_or_else(|| panic!("{name} over {width} must classify"));
                assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
            }
        }
    }

    #[test]
    fn min_is_always_recompute_only_regardless_of_type() {
        for ty in [ValueType::Numeric, ValueType::Text, ValueType::Boolean] {
            let verdict = classify("MIN", AggregateArg::Column(ty)).unwrap();
            assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
            assert_eq!(verdict.partials, &[] as &[PartialField]);
        }
    }

    #[test]
    fn max_is_always_recompute_only_regardless_of_type() {
        for ty in [ValueType::Numeric, ValueType::Text, ValueType::Boolean] {
            let verdict = classify("MAX", AggregateArg::Column(ty)).unwrap();
            assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
            assert_eq!(verdict.partials, &[] as &[PartialField]);
        }
    }

    /// Issue #112: floats are the one *numeric* argument type `SUM`/`AVG`
    /// are not invertible over. `Invertible` here would mean the delta path
    /// silently drifting from a server-side `sum()` — see this module's doc
    /// comment for the three live-server counterexamples.
    #[test]
    fn sum_and_avg_over_floats_are_recompute_only() {
        for width in crate::float::FloatWidth::ALL {
            let arg = AggregateArg::Column(ValueType::Float(width));
            for name in ["SUM", "AVG", "MIN", "MAX"] {
                let verdict = classify(name, arg)
                    .unwrap_or_else(|| panic!("{name} over {width} must classify"));
                assert_eq!(
                    verdict.invertibility,
                    Invertibility::RecomputeOnly,
                    "{name} over {width}"
                );
                assert_eq!(verdict.partials, &[] as &[PartialField]);
            }
        }
    }

    #[test]
    fn sum_over_text_or_boolean_is_recompute_only() {
        for ty in [ValueType::Text, ValueType::Boolean] {
            let verdict = classify("SUM", AggregateArg::Column(ty)).unwrap();
            assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
        }
    }

    #[test]
    fn avg_over_text_or_boolean_is_recompute_only() {
        for ty in [ValueType::Text, ValueType::Boolean] {
            let verdict = classify("AVG", AggregateArg::Column(ty)).unwrap();
            assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
        }
    }

    /// Issue #119: `bool_and`/`bool_or` are recompute-only regardless of
    /// argument type, the same way `MIN`/`MAX` are — this module's gate
    /// answers "if this call were valid, is it delta-able?", and it is not,
    /// independent of what a hand-built AST might pass as the argument.
    #[test]
    fn bool_and_and_bool_or_are_always_recompute_only() {
        for name in ["BOOL_AND", "BOOL_OR"] {
            let verdict = classify(name, AggregateArg::Column(ValueType::Boolean)).unwrap();
            assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
            assert_eq!(verdict.partials, &[] as &[PartialField]);
        }
    }

    #[test]
    fn unknown_function_returns_none() {
        assert!(classify("ROUND", AggregateArg::Column(ValueType::Numeric)).is_none());
    }

    #[test]
    fn count_with_column_arg_shape_returns_none() {
        assert!(classify("COUNT", AggregateArg::Column(ValueType::Numeric)).is_none());
    }

    #[test]
    fn non_count_with_star_arg_shape_returns_none() {
        assert!(classify("SUM", AggregateArg::Count(CountArg::Star)).is_none());
        assert!(classify("MIN", AggregateArg::Count(CountArg::Column)).is_none());
    }

    #[test]
    fn every_aggregate_function_in_the_registry_is_classified() {
        // Pins that this gate's match covers every name in
        // super::registry::AGGREGATE_FUNCTIONS, not a hardcoded subset —
        // if a new aggregate is added there without updating this gate,
        // this test should be extended to cover it.
        for &name in super::super::registry::AGGREGATE_FUNCTIONS {
            let arg = if name == "COUNT" {
                AggregateArg::Count(CountArg::Star)
            } else {
                AggregateArg::Column(ValueType::Numeric)
            };
            assert!(
                classify(name, arg).is_some(),
                "expected {name} to be classified"
            );
        }
    }
}
