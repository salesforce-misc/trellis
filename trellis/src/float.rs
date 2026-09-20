//! Postgres's **IEEE binary floating-point** types — `real`/`double
//! precision` (issue #112) — as a real value model, split out of the
//! arbitrary-precision [`crate::numeric::Numeric`] both of them used to
//! collapse into.
//!
//! # Why this is not `Numeric`
//!
//! Issue #111 took the exact integer types out of `ValueType::Numeric`.
//! `real` and `double precision` were the two left behind, and they are the
//! *least* like `numeric` of the six: `numeric` is an exact,
//! arbitrary-precision decimal, while these are fixed-width binary floats.
//! ADR-0004 makes Postgres the correctness oracle, and the oracle disagrees
//! with an exact-decimal model on every axis that matters:
//!
//! 1. **Arithmetic isn't exact, and isn't even associative.** In Postgres,
//!    `select (1e16::float8 + 1) - 1e16::float8` is `0` while
//!    `select (1e16::float8 - 1e16::float8) + 1` is `1`. A `Numeric` model
//!    computes `1` for both — so the engine's incremental result and a
//!    backfill's server-side `SELECT a + b` were free to disagree in the
//!    last digits, with nothing to flag it.
//! 2. **Special values exist.** `'NaN'`, `'Infinity'` and `'-Infinity'` are
//!    ordinary float values Postgres ingests, computes with and renders.
//!    `Numeric::parse` has no representation for any of them.
//! 3. **Range is finite.** `select 3.4e38::float4 + 3.4e38::float4` raises
//!    `22003` (`value out of range: overflow`); arbitrary precision never
//!    does. Same class of divergence #111 fixed for `int4 + int4`.
//! 4. **Result type.** Postgres types `float4 + float4` as `real`, not
//!    `numeric`, and `avg(real)` as `double precision`. A derived column
//!    declared `numeric` for an expression Postgres types `real` is the lie
//!    #108 removed from the ingest side and #111 removed from the computed
//!    side for integers.
//!
//! # Postgres's float semantics are *not* IEEE 754's
//!
//! This is the decision issue #112 asks for, and it is not a free one —
//! Postgres already made it, and `docs/type-support.md`'s "IEEE edge cases
//! need a decision" resolves to "do what the oracle does":
//!
//! * **`NaN = NaN` is true**, and **`NaN` sorts greater than every other
//!   value, including `Infinity`.** IEEE 754 says `NaN != NaN` and that
//!   every comparison against `NaN` is false; Postgres deliberately departs
//!   from that (`float8_cmp_internal` in `src/backend/utils/adt/float.c`) so
//!   that floats have a *total* order and can be used in `ORDER BY`,
//!   `GROUP BY`, `DISTINCT` and btree indexes at all. Verified against a
//!   live server: `select 'NaN'::float8 = 'NaN'::float8` is `t`, and
//!   `order by v` over `{NaN, 1, Infinity, -Infinity}` yields
//!   `-Infinity, 1, Infinity, NaN`.
//! * **`-0 = 0` is true** — here Postgres *does* keep IEEE's behaviour, for
//!   both widths. Verified: `select (-0.0::float8) = (0.0::float8)` is `t`.
//!
//! [`compare`] implements exactly that order, and it is the only comparison
//! this crate should use on a float. See also
//! [`crate::defs::eval::Value`]'s hand-written `PartialEq`, which routes
//! through it rather than through `f64`'s IEEE `==`.
//!
//! # ±0 is why floats still aren't keys
//!
//! `-0` and `0` are *equal* but render as different text (`'-0'` vs `'0'`;
//! verified on a live server), and Trellis matches join/primary/`GROUP BY`
//! keys by raw `::text` today (`catalog::is_text_stable_join_key_type`).
//! That is precisely the text-instability bar `numeric` already fails on
//! (`1.0` vs `1.00`), so `real`/`double precision` stay off
//! `catalog::TEXT_STABLE_JOIN_KEY_TYPES` and are rejected as `GROUP BY`
//! keys. The unlock is #110's typed key index — comparing decoded values
//! through [`compare`] — not a new encoding here. See
//! `docs/type-support.md`.
//!
//! # Width is a payload, not two variants
//!
//! Same shape, and the same reasoning, as [`crate::integer::IntWidth`]: the
//! width *is* the overflow boundary and the rounding grid (`float4` carries
//! 24 significand bits, `float8` 53), and it is the result type of
//! `float4 + float4`, so a width-less "binary float" model could not match
//! the oracle. But two sibling `ValueType` variants would double every match
//! arm in the engine for a distinction most sites don't care about, so
//! [`FloatWidth`] rides as a payload on one `ValueType::Float` variant.

use std::cmp::Ordering;
use std::fmt;

/// Which of Postgres's two binary float widths a value has.
///
/// Ordered narrowest-to-widest, and that order is load-bearing: [`Ord`] is
/// what [`FloatWidth::wider`] uses to pick a mixed-width arithmetic result's
/// type, matching Postgres's `float48pl`/`float84pl` pair (the wider operand
/// wins).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FloatWidth {
    /// `real` / `float4` — IEEE binary32.
    Float4,
    /// `double precision` / `float8` — IEEE binary64.
    Float8,
}

/// Why a float operation failed. Both variants correspond to a real Postgres
/// error on the same input, raised with Postgres's own wording so a Trellis
/// failure and the oracle's failure on the same expression read the same:
/// [`FloatError::OutOfRange`] to `22003` (`value out of range: overflow`)
/// and [`FloatError::Invalid`] to `22P02`
/// (`invalid input syntax for type double precision: "..."`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FloatError {
    /// A computed result left the width's finite range.
    OutOfRange { width: FloatWidth },
    /// Text that isn't a canonical float rendering at all.
    Invalid { width: FloatWidth, text: String },
}

impl fmt::Display for FloatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Postgres's own message for SQLSTATE 22003 on `float4pl`/
            // `float8pl` overflow — notably *not* parameterized by the type
            // name, unlike the integer family's "integer out of range".
            FloatError::OutOfRange { .. } => f.write_str("value out of range: overflow"),
            FloatError::Invalid { width, text } => {
                write!(f, "invalid input syntax for type {}: \"{text}\"", {
                    width.pg_name()
                })
            }
        }
    }
}

impl std::error::Error for FloatError {}

impl FloatWidth {
    /// Every width, narrowest first — so tests and exhaustive tables stay
    /// exhaustive by construction rather than by a hand-maintained list.
    pub const ALL: [FloatWidth; 2] = [FloatWidth::Float4, FloatWidth::Float8];

    /// The Postgres spelling of this width (`real` / `double precision`) —
    /// the SQL keyword a derived column is declared with, the token the
    /// catalog persists, and the keyword the typed-literal grammar accepts.
    ///
    /// Deliberately the *standard* spelling rather than the `float4`/`float8`
    /// alias, matching [`crate::integer::IntWidth::pg_name`]'s reasoning: it
    /// has to double as a `format_type` rendering, and `format_type` emits
    /// `real` and `double precision`.
    ///
    /// Note `double precision` is the only two-word type keyword anywhere in
    /// this grammar; [`crate::defs::parser`] joins the two identifiers back
    /// together before looking a typed literal up.
    pub const fn pg_name(self) -> &'static str {
        match self {
            FloatWidth::Float4 => "real",
            FloatWidth::Float8 => "double precision",
        }
    }

    /// The inverse of [`Self::pg_name`], for decoding a persisted catalog
    /// token. `None` for anything else, so an unknown token becomes a named
    /// `CatalogError::UnknownValueType` rather than a silent misparse — the
    /// same forward-compat guard [`crate::integer::IntWidth::from_pg_name`]
    /// and [`crate::defs::pg_type::PgType::from_name`] give their tokens.
    pub fn from_pg_name(text: &str) -> Option<FloatWidth> {
        Some(match text {
            "real" => FloatWidth::Float4,
            "double precision" => FloatWidth::Float8,
            _ => return None,
        })
    }

    /// The wider of two widths — the result width of a mixed-width binary
    /// operation, matching Postgres's `float48pl`/`float84pl`, both of which
    /// return `double precision`.
    pub fn wider(self, other: FloatWidth) -> FloatWidth {
        self.max(other)
    }

    /// The number of significant decimal digits below which Postgres prints
    /// a float in fixed rather than scientific notation — C's `FLT_DIG` (6)
    /// and `DBL_DIG` (15). See [`render`] for how it's used.
    const fn decimal_digits(self) -> i32 {
        match self {
            FloatWidth::Float4 => 6,
            FloatWidth::Float8 => 15,
        }
    }

    /// Rounds `value` onto this width's representable grid, the way an
    /// assignment to a column of this type does. A no-op for
    /// [`FloatWidth::Float8`] (values are already `f64`); a `f64 -> f32 ->
    /// f64` round-trip for [`FloatWidth::Float4`].
    ///
    /// Carrying every float as an `f64` and narrowing here — rather than a
    /// separate `f32` payload — keeps one arithmetic path, and matches how
    /// Postgres itself computes: `float4pl` adds as `float4`, but every
    /// `float4`/`float8` mixed operator widens to `double` first.
    pub fn round(self, value: f64) -> f64 {
        match self {
            FloatWidth::Float4 => value as f32 as f64,
            FloatWidth::Float8 => value,
        }
    }
}

impl fmt::Display for FloatWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.pg_name())
    }
}

/// Postgres's total order on floats — `float8_cmp_internal`
/// (`src/backend/utils/adt/float.c`), which is what backs `<`/`=`/`ORDER BY`/
/// `GROUP BY`/btree for `real` and `double precision` alike.
///
/// It is deliberately **not** IEEE 754's comparison:
///
/// * `NaN` compares **equal to itself**, so `'NaN'::float8 = 'NaN'::float8`
///   is true and two `NaN` rows land in one `GROUP BY` group.
/// * `NaN` compares **greater than every non-`NaN` value**, `Infinity`
///   included, so it sorts last and `max()` over a group containing one is
///   `NaN`.
/// * `-0` compares **equal to `0`** (this part *is* IEEE), so they land in
///   one group too.
///
/// Every one of those is verified against a live server in
/// `trellis/tests/defs_floats.rs` rather than taken from this comment.
pub fn compare(a: f64, b: f64) -> Ordering {
    // Ordinary (non-NaN) comparison first, exactly as Postgres does — this
    // arm is also what makes `-0 == 0`, since IEEE `==` already says so.
    if a < b {
        return Ordering::Less;
    }
    if a > b {
        return Ordering::Greater;
    }
    if a == b {
        return Ordering::Equal;
    }
    // At least one operand is NaN (the only way to reach here).
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        // Unreachable: `!(a<b) && !(a>b) && !(a==b)` implies a NaN operand.
        (false, false) => Ordering::Equal,
    }
}

/// Postgres's float equality — [`compare`] reduced to a bool, so
/// `NaN = NaN` is true and `-0 = 0` is true.
pub fn equal(a: f64, b: f64) -> bool {
    compare(a, b) == Ordering::Equal
}

/// `a + b` for binary floats, with Postgres's `float4pl`/`float8pl`/
/// `float48pl`/`float84pl` semantics: the result type is the *wider
/// operand's* type, the sum is computed in `double` and rounded onto that
/// width, and a sum that overflows to infinity from two finite operands
/// raises `22003` rather than returning `Infinity`.
///
/// That last clause is Postgres's `CHECKFLOATVAL(result, isinf(arg1) ||
/// isinf(arg2), true)`: an infinite result is an error only when neither
/// operand was already infinite (`select 'Infinity'::float4 + 1e38::float4`
/// is `Infinity`, `select 3.4e38::float4 + 3.4e38::float4` raises). `NaN`
/// propagates silently — it is a value, not an error.
pub fn checked_add(
    a: f64,
    a_width: FloatWidth,
    b: f64,
    b_width: FloatWidth,
) -> Result<(f64, FloatWidth), FloatError> {
    let width = a_width.wider(b_width);
    let sum = width.round(a + b);
    if sum.is_infinite() && a.is_finite() && b.is_finite() {
        return Err(FloatError::OutOfRange { width });
    }
    Ok((sum, width))
}

/// Parses Postgres's canonical text rendering of a float, rejecting anything
/// [`render`] would not itself have produced for that width.
///
/// The bar is *canonical form*, not "`float8in` would accept it", and it is
/// the same bar [`crate::defs::typed_literal`] sets for every other family:
/// a value travels through the staging ring as text, so a spelling the
/// evaluator accepts but renders back differently would make the evaluator
/// and the SQL oracle disagree — exactly the divergence ADR-0013 exists to
/// catch. `float8in` accepts `1.5e1`, `+1.5`, ` 1.5 `, `inf`, `nan` and
/// `1.50`; none of those is what `float8out` emits, so none is accepted
/// here. The check is implemented as "parse, then round-trip through
/// [`render`] and require byte equality", which makes the two functions
/// inverses by construction rather than by two hand-maintained grammars.
///
/// `NaN`, `Infinity` and `-Infinity` *are* accepted: they are precisely what
/// `float8out` emits for those values.
pub fn parse(text: &str, width: FloatWidth) -> Result<f64, FloatError> {
    let invalid = || FloatError::Invalid {
        width,
        text: text.to_string(),
    };
    let value = match text {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        _ => {
            let parsed: f64 = text.parse().map_err(|_| invalid())?;
            // Rust's `f64::from_str` accepts `inf`, `NaN`, `+1.5` and
            // similar spellings Postgres would never emit; the round-trip
            // check below rejects them, but a non-finite result here would
            // round-trip to the three canonical spellings already handled
            // above, so reject it outright rather than let it slip through.
            if !parsed.is_finite() {
                return Err(invalid());
            }
            let narrowed = width.round(parsed);
            if narrowed.is_infinite() {
                // `select 1e300::float4` -> `"1e300" is out of range for
                // type real`, which is a 22003, not a syntax error.
                return Err(FloatError::OutOfRange { width });
            }
            narrowed
        }
    };
    if render(value, width) != text {
        return Err(invalid());
    }
    Ok(value)
}

/// Renders a float exactly as Postgres's `float4out`/`float8out` do under
/// `extra_float_digits >= 1` — the shortest decimal that round-trips,
/// formatted with Postgres's own fixed-vs-scientific rule.
///
/// `extra_float_digits` is pinned to `1` on every Trellis connection by
/// [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`], for the same reason
/// `DateStyle` and `bytea_output` are: the default is `1` on Postgres 12+,
/// but a server-level or role-level `SET` can change it, and a value that
/// renders differently on two connections breaks the round-trip identity
/// every text-carried value depends on.
///
/// # The algorithm
///
/// Postgres uses a vendored Ryu (`src/common/shortest_dec.c`) to get the
/// shortest round-tripping digit string, then chooses notation by the
/// value's decimal exponent `e` (the exponent in `d.ddd × 10^e` form):
/// fixed notation when `-4 <= e < DIG`, scientific otherwise, where `DIG` is
/// C's `FLT_DIG` (6) / `DBL_DIG` (15). Scientific notation's exponent is
/// signed and zero-padded to at least two digits (`1e+30`, `1e-08`,
/// `5e-324`).
///
/// Rust's `{:e}` already produces the same shortest digit string (it is the
/// same shortest-round-trip problem, and `f32`'s formatter solves it for
/// `f32`), just with Rust's own bare-exponent formatting — so this function
/// takes those digits and re-formats them with Postgres's rule rather than
/// vendoring a second Ryu. The `rendering_matches_postgres_over_a_value_grid`
/// test in `trellis/tests/defs_floats.rs` checks the result against a live
/// `float4out`/`float8out` over a grid spanning both notations, both signs,
/// the subnormal floor and the finite ceiling, so the equivalence is
/// verified, not assumed.
pub fn render(value: f64, width: FloatWidth) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }

    // Shortest round-tripping digits, at this width's precision.
    let sci = match width {
        FloatWidth::Float4 => format!("{:e}", value as f32),
        FloatWidth::Float8 => format!("{value:e}"),
    };
    let (mantissa, exponent) = sci
        .split_once('e')
        .expect("Rust's LowerExp for f32/f64 always emits an 'e'");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust's LowerExp always emits a decimal exponent");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", mantissa),
    };
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let len = digits.len() as i32;

    let body = if exponent >= -4 && exponent < width.decimal_digits() {
        // Fixed notation.
        if exponent >= 0 {
            let int_len = exponent + 1;
            if len <= int_len {
                format!("{digits}{}", "0".repeat((int_len - len) as usize))
            } else {
                let (int_part, frac_part) = digits.split_at(int_len as usize);
                format!("{int_part}.{frac_part}")
            }
        } else {
            format!("0.{}{digits}", "0".repeat((-exponent - 1) as usize))
        }
    } else {
        // Scientific notation, with Postgres's signed, >=2-digit exponent.
        let (first, rest) = digits.split_at(1);
        let mantissa = if rest.is_empty() {
            first.to_string()
        } else {
            format!("{first}.{rest}")
        };
        let exp_sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{exp_sign}{:02}", exponent.abs())
    };
    format!("{sign}{body}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_names_round_trip() {
        for width in FloatWidth::ALL {
            assert_eq!(FloatWidth::from_pg_name(width.pg_name()), Some(width));
        }
        assert_eq!(FloatWidth::from_pg_name("float8"), None);
        assert_eq!(FloatWidth::from_pg_name("numeric"), None);
    }

    #[test]
    fn wider_picks_the_wider_operand() {
        assert_eq!(
            FloatWidth::Float4.wider(FloatWidth::Float8),
            FloatWidth::Float8
        );
        assert_eq!(
            FloatWidth::Float8.wider(FloatWidth::Float4),
            FloatWidth::Float8
        );
        assert_eq!(
            FloatWidth::Float4.wider(FloatWidth::Float4),
            FloatWidth::Float4
        );
    }

    /// Postgres's order, not IEEE's. The live-server counterpart is
    /// `nan_and_signed_zero_follow_postgres_not_ieee` in
    /// `trellis/tests/defs_floats.rs`.
    #[test]
    fn nan_equals_itself_and_sorts_above_everything() {
        assert_eq!(compare(f64::NAN, f64::NAN), Ordering::Equal);
        assert!(equal(f64::NAN, f64::NAN));
        for other in [f64::INFINITY, f64::NEG_INFINITY, 0.0, -1.0, 1e308] {
            assert_eq!(
                compare(f64::NAN, other),
                Ordering::Greater,
                "NaN vs {other}"
            );
            assert_eq!(compare(other, f64::NAN), Ordering::Less, "{other} vs NaN");
        }
    }

    #[test]
    fn signed_zeros_are_equal() {
        assert!(equal(-0.0, 0.0));
        assert_eq!(compare(-0.0, 0.0), Ordering::Equal);
        // ...but they still render differently, which is exactly why floats
        // are not admitted as a text-matched key.
        assert_eq!(render(-0.0, FloatWidth::Float8), "-0");
        assert_eq!(render(0.0, FloatWidth::Float8), "0");
    }

    #[test]
    fn ordinary_order_is_unchanged() {
        assert_eq!(compare(1.0, 2.0), Ordering::Less);
        assert_eq!(compare(f64::INFINITY, 1e308), Ordering::Greater);
        assert_eq!(compare(f64::NEG_INFINITY, -1e308), Ordering::Less);
    }

    /// Spot checks for [`render`]; the exhaustive comparison is against a
    /// live `float8out`/`float4out` in `trellis/tests/defs_floats.rs`.
    #[test]
    fn rendering_uses_postgres_notation_rules() {
        use FloatWidth::{Float4, Float8};
        assert_eq!(render(f64::NAN, Float8), "NaN");
        assert_eq!(render(f64::INFINITY, Float8), "Infinity");
        assert_eq!(render(f64::NEG_INFINITY, Float8), "-Infinity");
        assert_eq!(render(1.0, Float8), "1");
        assert_eq!(render(0.1, Float8), "0.1");
        assert_eq!(render(0.1 + 0.2, Float8), "0.30000000000000004");
        assert_eq!(render(1e-4, Float8), "0.0001");
        assert_eq!(render(1e-5, Float8), "1e-05");
        assert_eq!(render(1e14, Float8), "100000000000000");
        assert_eq!(render(1e15, Float8), "1e+15");
        assert_eq!(
            render(1.234567890123456e15, Float8),
            "1.234567890123456e+15"
        );
        assert_eq!(render(1e30, Float8), "1e+30");
        assert_eq!(render(5e-324, Float8), "5e-324");
        assert_eq!(render(-1.5e-10, Float8), "-1.5e-10");
        // `real` switches to scientific three orders of magnitude earlier
        // (FLT_DIG is 6, DBL_DIG is 15).
        assert_eq!(render(999_999.0, Float4), "999999");
        assert_eq!(render(1e6, Float4), "1e+06");
        assert_eq!(render(16_777_216.0, Float4), "1.6777216e+07");
        assert_eq!(render(1e6, Float8), "1000000");
    }

    #[test]
    fn parse_accepts_only_what_render_emits() {
        use FloatWidth::{Float4, Float8};
        assert_eq!(parse("1", Float8), Ok(1.0));
        assert_eq!(parse("0.1", Float8), Ok(0.1));
        assert_eq!(parse("1e+30", Float8), Ok(1e30));
        assert_eq!(parse("-0", Float8), Ok(-0.0));
        assert!(parse("NaN", Float8).unwrap().is_nan());
        assert_eq!(parse("Infinity", Float8), Ok(f64::INFINITY));
        assert_eq!(parse("-Infinity", Float8), Ok(f64::NEG_INFINITY));
        // Spellings `float8in` accepts but `float8out` never emits.
        for bad in [
            "", "+1", " 1", "1 ", "1.0", "1.50", "1e30", "1E+30", "inf", "nan", "Inf", "1.5e1",
            "abc", "0x1p3",
        ] {
            assert!(
                parse(bad, Float8).is_err(),
                "{bad:?} is not canonical float8out output"
            );
        }
        // A `real` literal must be canonical *at `real`'s precision*: `0.1`
        // is fine (it is what `float4out` emits for the nearest float4), but
        // float8's 17-digit spelling of that same neighbourhood is not.
        assert_eq!(parse("0.1", Float4), Ok(0.1f32 as f64));
        assert!(parse("0.30000000000000004", Float4).is_err());
    }

    #[test]
    fn parse_rejects_a_value_outside_the_width() {
        assert_eq!(
            parse("1e+300", FloatWidth::Float4),
            Err(FloatError::OutOfRange {
                width: FloatWidth::Float4
            })
        );
        assert_eq!(parse("1e+300", FloatWidth::Float8), Ok(1e300));
    }

    #[test]
    fn every_render_round_trips_through_parse() {
        for width in FloatWidth::ALL {
            for value in [
                0.0,
                -0.0,
                1.0,
                -1.0,
                0.1,
                1e-5,
                1e6,
                1234.5678,
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ] {
                let value = width.round(value);
                let text = render(value, width);
                let back = parse(&text, width)
                    .unwrap_or_else(|e| panic!("{text:?} at {width} did not re-parse: {e}"));
                assert!(
                    equal(back, value),
                    "{text:?} at {width} round-tripped to a different value"
                );
            }
        }
    }

    #[test]
    fn addition_overflows_where_postgres_does() {
        use FloatWidth::{Float4, Float8};
        // `select 3.4e38::float4 + 3.4e38::float4` -> 22003.
        assert_eq!(
            checked_add(3.4e38, Float4, 3.4e38, Float4),
            Err(FloatError::OutOfRange { width: Float4 })
        );
        // ...but widening one operand makes it `float48pl`, which is fine.
        let (sum, width) = checked_add(3.4e38, Float4, 3.4e38, Float8).unwrap();
        assert_eq!(width, Float8);
        assert!((sum - 6.8e38).abs() < 1e32);
        assert_eq!(
            checked_add(1e308, Float8, 1e308, Float8),
            Err(FloatError::OutOfRange { width: Float8 })
        );
        // An already-infinite operand is not an overflow.
        assert_eq!(
            checked_add(f64::INFINITY, Float4, 1e38, Float4),
            Ok((f64::INFINITY, Float4))
        );
        // NaN propagates as a value, never as an error.
        let (sum, _) = checked_add(f64::NAN, Float8, 1.0, Float8).unwrap();
        assert!(sum.is_nan());
    }

    #[test]
    fn out_of_range_message_is_postgres_wording() {
        assert_eq!(
            FloatError::OutOfRange {
                width: FloatWidth::Float4
            }
            .to_string(),
            "value out of range: overflow"
        );
        assert_eq!(
            FloatError::Invalid {
                width: FloatWidth::Float8,
                text: "abc".to_string(),
            }
            .to_string(),
            "invalid input syntax for type double precision: \"abc\""
        );
    }

    #[test]
    fn float4_addition_rounds_onto_float4s_grid() {
        // `select 16777216::float4 + 1::float4` is 16777216 in Postgres —
        // the sum is not representable as a float4 and rounds back down.
        let (sum, width) = checked_add(16_777_216.0, FloatWidth::Float4, 1.0, FloatWidth::Float4)
            .expect("no overflow");
        assert_eq!(width, FloatWidth::Float4);
        assert_eq!(sum, 16_777_216.0);
    }
}
