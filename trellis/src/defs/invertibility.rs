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
//! - **Float `Inf`/`NaN` tracking**: deferred. The issue's note that floats
//!   should be "kept in exact decimal" with `Inf`/`NaN` "tracked as counts"
//!   has nothing to hang off today, since [`ValueType`] has no float variant
//!   (only `Numeric`, which this codebase treats as exact/decimal already).
//!   Revisit this module once a float `ValueType` variant exists.

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
    /// `COUNT(<column>)`. The grammar does not accept this yet (see the
    /// module doc and ADR-0004), so nothing outside this module's own tests
    /// constructs it — it is modelled here because `COUNT`'s invertibility
    /// verdict genuinely differs between the two argument forms.
    #[allow(dead_code)]
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
        (
            "SUM",
            AggregateArg::Column(
                ValueType::Text | ValueType::Boolean | ValueType::Uuid | ValueType::Other(_),
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
                ValueType::Text | ValueType::Boolean | ValueType::Uuid | ValueType::Other(_),
            ),
        ) => Some(Verdict::recompute_only()),

        ("MIN", AggregateArg::Column(_)) | ("MAX", AggregateArg::Column(_)) => {
            Some(Verdict::recompute_only())
        }

        // Shapes the grammar could never produce: COUNT with a column-typed
        // arg description, or a non-COUNT aggregate with a `*` arg.
        ("COUNT", AggregateArg::Column(_)) => None,
        ("SUM" | "AVG" | "MIN" | "MAX", AggregateArg::Count(_)) => None,

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
