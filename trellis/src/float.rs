//! Postgres's **IEEE binary floating-point** widths and text forms:
//! `real`/`double precision`.
//!
//! [`FloatWidth`] is the width payload of `ValueType::Float`. Postgres types
//! `float4 + float4` as `real` and `avg(real)` as `double precision`, so a
//! derived target column is declared with the type Postgres infers, and the
//! width is the overflow boundary the define-time constant folding checks
//! ([`checked_add`], [`literal_to_width`]).
//!
//! Postgres's float order is not IEEE's (`NaN = NaN` is true and `NaN` sorts
//! above `Infinity`), and `-0` and `0` are equal but render as different
//! text. Trellis matches join, primary and `GROUP BY` keys by raw `::text`
//! (`join_key_types::is_text_stable_join_key_type`), so `real` and
//! `double precision` stay off `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`.
//! See `docs/type-support.md`.

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

/// `a - b`, with the same overflow rule as [`checked_add`].
pub fn checked_sub(
    a: f64,
    a_width: FloatWidth,
    b: f64,
    b_width: FloatWidth,
) -> Result<(f64, FloatWidth), FloatError> {
    let width = a_width.wider(b_width);
    let difference = width.round(a - b);
    if difference.is_infinite() && a.is_finite() && b.is_finite() {
        return Err(FloatError::OutOfRange { width });
    }
    Ok((difference, width))
}

/// `a * b` with Postgres's `float4mul`/`float8mul` semantics: an infinite
/// result from two finite operands is an overflow, and a zero result from two
/// nonzero operands is an underflow; both raise `22003`.
pub fn checked_mul(
    a: f64,
    a_width: FloatWidth,
    b: f64,
    b_width: FloatWidth,
) -> Result<(f64, FloatWidth), FloatError> {
    let width = a_width.wider(b_width);
    let product = width.round(a * b);
    if (product.is_infinite() && a.is_finite() && b.is_finite())
        || (product == 0.0 && a != 0.0 && b != 0.0)
    {
        return Err(FloatError::OutOfRange { width });
    }
    Ok((product, width))
}

/// `a / b` with Postgres's `float4div`/`float8div` semantics: a zero divisor
/// raises `22012` unless the dividend is `NaN`; an infinite result from two
/// finite operands is an overflow; a zero result from a nonzero dividend and a
/// finite divisor is an underflow (`1 / Infinity` is a plain `0`). Every
/// failure is reported as [`FloatError::OutOfRange`], the caller having
/// refused a zero divisor already.
pub fn checked_div(
    a: f64,
    a_width: FloatWidth,
    b: f64,
    b_width: FloatWidth,
) -> Result<(f64, FloatWidth), FloatError> {
    let width = a_width.wider(b_width);
    if b == 0.0 && !a.is_nan() {
        return Err(FloatError::OutOfRange { width });
    }
    let quotient = width.round(a / b);
    if (quotient.is_infinite() && a.is_finite() && b.is_finite())
        || (quotient == 0.0 && a != 0.0 && b.is_finite())
    {
        return Err(FloatError::OutOfRange { width });
    }
    Ok((quotient, width))
}

/// Parses Postgres's canonical text rendering of a float, rejecting anything
/// [`render`] would not itself have produced for that width.
///
/// The bar is *canonical form*, not "`float8in` would accept it", and it is
/// the same bar [`crate::defs::typed_literal`] sets for every other family:
/// a value travels through the staging ring as text, so a spelling
/// Postgres accepts but renders back differently would make the target and
/// the SQL oracle disagree — exactly the divergence ADR-0013 exists to
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

/// A bare number literal (`0.5`, `3000000000`: digits with an optional
/// fraction, no sign or exponent) as the `double precision` constant
/// Postgres casts it to when it meets a float operand: correctly rounded,
/// and an error where `float8in` errors (overflow past the finite range, or
/// a nonzero value that underflows to zero; subnormals are accepted).
pub fn literal_to_f64(text: &str) -> Result<f64, FloatError> {
    let width = FloatWidth::Float8;
    let value: f64 = text.parse().map_err(|_| FloatError::Invalid {
        width,
        text: text.to_string(),
    })?;
    let nonzero_digits = text.bytes().any(|b| (b'1'..=b'9').contains(&b));
    if !value.is_finite() || (value == 0.0 && nonzero_digits) {
        return Err(FloatError::OutOfRange { width });
    }
    Ok(value)
}

/// [`literal_to_f64`] narrowed to `width`: the constant Postgres casts a bare
/// number literal to when the other `COALESCE` argument is that float type.
/// For `real` it errors where `float4in` does: a value past `real`'s finite
/// range, or a nonzero one that underflows to zero. The text is rounded to
/// `real` once, as `float4in`'s `strtof` rounds it: rounding to `double
/// precision` first would round twice, and a literal just inside `real`'s
/// limits could land on a halfway point and round out of range.
pub fn literal_to_width(text: &str, width: FloatWidth) -> Result<f64, FloatError> {
    match width {
        FloatWidth::Float8 => literal_to_f64(text),
        FloatWidth::Float4 => {
            let value: f32 = text.parse().map_err(|_| FloatError::Invalid {
                width,
                text: text.to_string(),
            })?;
            let nonzero_digits = text.bytes().any(|b| (b'1'..=b'9').contains(&b));
            if !value.is_finite() || (value == 0.0 && nonzero_digits) {
                return Err(FloatError::OutOfRange { width });
            }
            Ok(f64::from(value))
        }
    }
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

    /// Each case's verdict is what Postgres 17 gives for
    /// `COALESCE(real_col, <literal>)`. The first and last two lie within a
    /// `double precision` step of a halfway point at `real`'s limits, where
    /// rounding through `double precision` would round them out of range.
    #[test]
    fn a_literal_narrows_to_real_as_float4in_rounds_it() {
        let half_ulp_of_the_smallest_subnormal = format!(
            "0.{}70064923216240853546186479164495806564013097093825788587853414194489554134293031",
            "0".repeat(45)
        );
        for (text, in_range) in [
            ("340282356779733661637539395458142568447", true),
            ("340282346638528859811704183484516925440", true),
            ("340282356779733661637539395458142568448", false),
            ("1000000000000000000000000000000000000000", false),
            (half_ulp_of_the_smallest_subnormal.as_str(), true),
            ("0.0000000000000000000000000000000000000000000007006", false),
            ("0.000000000000000000000000000000000000000000001", true),
            ("0.0", true),
        ] {
            assert_eq!(
                literal_to_width(text, FloatWidth::Float4).is_ok(),
                in_range,
                "{text}"
            );
        }
    }

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

    #[test]
    fn signed_zeros_render_differently() {
        // `-0 = 0` in Postgres but the text differs, which is why floats are
        // not admitted as a text-matched key.
        assert_eq!(render(-0.0, FloatWidth::Float8), "-0");
        assert_eq!(render(0.0, FloatWidth::Float8), "0");
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
                    back == value || (back.is_nan() && value.is_nan()),
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
