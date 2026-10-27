//! Postgres's **exact integer** types — `smallint`/`integer`/`bigint`
//! — as a real value model, split out of the arbitrary-precision
//! [`crate::numeric::Numeric`] every one of them used to collapse into.
//!
//! # Why this is not `Numeric`
//!
//! Before this module, `smallint`, `integer`, `bigint`, `numeric`, `real` and
//! `double precision` all classified as one `ValueType::Numeric`
//! ([`crate::defs::pg_type`]), and every value of all six was carried as a
//! [`crate::numeric::Numeric`] — a *sign, digit-vector, scale* decimal with no
//! upper bound. That is the right model for `numeric`. It is the wrong model
//! for the exact integer types in two ways that ADR-0004 cares about, because
//! ADR-0004 makes Postgres itself the correctness oracle:
//!
//! 1. **Range.** `int4 + int4` in Postgres is `int4pl`, which raises
//!    `22003 numeric_value_out_of_range` (`"integer out of range"`) the moment
//!    the sum leaves `int4`. Arbitrary precision silently produces the
//!    mathematically-correct-but-untypeable answer instead, so Trellis's
//!    incremental evaluator and a backfill's server-side `SELECT a + b`
//!    disagreed about whether the very same definition errors at all.
//! 2. **Result type.** Postgres types `int4 + int4` as `int4`, not `numeric`.
//!    A derived target column declared `numeric` for an expression Postgres
//!    types `integer` is the same class of lie the *ingest*
//!    side avoids; this module avoids it on the *computed* side.
//!
//! # Width is a payload, not three variants
//!
//! The overflow boundary is per-width — `32767::smallint + 1::smallint`
//! raises, `32767::integer + 1` does not — so a single width-less "exact
//! integer" model could not match the oracle. But three sibling `ValueType`
//! variants would triple every match arm in the engine for a distinction most
//! of them don't care about. [`IntWidth`] is therefore a payload on one
//! variant, exactly the shape of
//! `ValueType::Other(PgType)`: a site that cares matches
//! `ValueType::Integer(IntWidth::Int2)`, and the (many) sites that don't
//! match `ValueType::Integer(_)`.
//!
//! # What is deliberately *not* here
//!
//! `oid` is not an [`IntWidth`]. Postgres gives `oid` no arithmetic operators
//! at all (there is no `oidpl`; `pg_operator` carries only the comparison
//! family for it) and it is an *unsigned* 32-bit type, so folding it into a
//! signed, addable width would invent semantics Postgres does not have. It
//! stays `PgType::Oid` and gains only the roles it can honestly hold — see
//! `docs/type-support.md` and [`crate::defs::pg_type::PgType::Oid`].

use std::fmt;

/// Which of Postgres's three exact integer widths a value has.
///
/// Ordered narrowest-to-widest, and that order is load-bearing: [`Ord`] is
/// what [`IntWidth::wider`] uses to pick a mixed-width arithmetic result's
/// type, matching Postgres's own `int24pl`/`int28pl`/`int42pl`/… family
/// (the wider operand wins).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IntWidth {
    /// `smallint` / `int2`.
    Int2,
    /// `integer` / `int4`.
    Int4,
    /// `bigint` / `int8`.
    Int8,
}

/// Why an exact-integer operation failed. Both variants correspond to a real
/// Postgres error on the same input: [`IntegerError::OutOfRange`] to
/// `22003 numeric_value_out_of_range`, raised with Postgres's own wording
/// (`"integer out of range"`) so a Trellis failure and the oracle's failure
/// on the same expression read the same.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegerError {
    /// A result (or an ingested value) doesn't fit its type's range.
    OutOfRange { width: IntWidth },
    /// Text that isn't a valid integer literal at all.
    Invalid { text: String },
}

impl fmt::Display for IntegerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Postgres's own message for SQLSTATE 22003 on these types.
            IntegerError::OutOfRange { width } => write!(f, "{} out of range", width.pg_name()),
            IntegerError::Invalid { text } => {
                write!(f, "invalid input syntax for type integer: '{text}'")
            }
        }
    }
}

impl std::error::Error for IntegerError {}

impl IntWidth {
    /// Every width, narrowest first — so tests and exhaustive tables stay
    /// exhaustive by construction rather than by a hand-maintained list.
    pub const ALL: [IntWidth; 3] = [IntWidth::Int2, IntWidth::Int4, IntWidth::Int8];

    /// The Postgres spelling of this width (`smallint`/`integer`/`bigint`) —
    /// the SQL keyword a derived column is declared with, the token the
    /// catalog persists, and the word Postgres puts in its own out-of-range
    /// message. Deliberately the *standard* spelling rather than the
    /// `int2`/`int4`/`int8` alias, because it has to double as a
    /// `format_type` rendering for
    /// [`crate::defs::join_key_types::is_text_stable_join_key_type`]'s allowlist,
    /// which matches what `format_type` actually emits.
    pub const fn pg_name(self) -> &'static str {
        match self {
            IntWidth::Int2 => "smallint",
            IntWidth::Int4 => "integer",
            IntWidth::Int8 => "bigint",
        }
    }

    /// The inverse of [`Self::pg_name`], for decoding a persisted catalog
    /// token. `None` for anything else, so an unknown token becomes a named
    /// `CatalogError::UnknownValueType` rather than a silent misparse — the
    /// same forward-compat guard [`crate::defs::pg_type::PgType::from_name`]
    /// gives its own tokens.
    pub fn from_pg_name(text: &str) -> Option<IntWidth> {
        Some(match text {
            "smallint" => IntWidth::Int2,
            "integer" => IntWidth::Int4,
            "bigint" => IntWidth::Int8,
            _ => return None,
        })
    }

    /// This width's inclusive value range, as Postgres defines it.
    pub const fn range(self) -> (i64, i64) {
        match self {
            IntWidth::Int2 => (i16::MIN as i64, i16::MAX as i64),
            IntWidth::Int4 => (i32::MIN as i64, i32::MAX as i64),
            IntWidth::Int8 => (i64::MIN, i64::MAX),
        }
    }

    /// Whether `value` fits this width.
    pub const fn fits(self, value: i64) -> bool {
        let (min, max) = self.range();
        value >= min && value <= max
    }

    /// The width Postgres gives an *unadorned integer literal* of this value:
    /// `integer` when it fits, else `bigint` (`select pg_typeof(1)` is
    /// `integer`, `pg_typeof(3000000000)` is `bigint`). A literal too wide
    /// for `bigint` is `numeric` in Postgres, which is why the caller — not
    /// this function — decides that case (see `eval`'s `number_literal` and
    /// `validate`'s `number_literal_type`, which must agree).
    pub fn narrowest_for(value: i64) -> IntWidth {
        if IntWidth::Int4.fits(value) {
            IntWidth::Int4
        } else {
            IntWidth::Int8
        }
    }

    /// The wider of two widths — the result width of a mixed-width binary
    /// operation, matching Postgres's `int24pl`/`int42pl`/`int28pl`/`int82pl`/
    /// `int48pl`/`int84pl` family, all of which return the wider operand's
    /// type.
    ///
    /// Note this is *not* "the narrowest width that holds the answer":
    /// `32767::smallint + 1::smallint` is `int2pl`, whose result type is
    /// `smallint`, so Postgres raises rather than quietly widening to
    /// `integer`. [`checked_add`] enforces exactly that.
    pub fn wider(self, other: IntWidth) -> IntWidth {
        self.max(other)
    }
}

impl fmt::Display for IntWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.pg_name())
    }
}

/// Parses Postgres's canonical text rendering of an exact integer, rejecting
/// anything outside `width`'s range.
///
/// Postgres's integer output is canonical — an optional `-`, then digits,
/// with no `+`, no leading zeros, no thousands separator and no surrounding
/// whitespace — which is precisely why these types are already on
/// [`crate::defs::join_key_types::is_text_stable_join_key_type`]'s allowlist. This
/// parser is correspondingly strict rather than lenient: accepting `007` or
/// `" 7 "` here would mean two spellings of one value could reach a key
/// comparison, which is the exact hazard that allowlist exists to rule out.
/// (An *input* literal written in a definition's text is a different thing,
/// and is typed by the parser, not by this function.)
#[cfg_attr(not(any(test, feature = "test-util")), allow(dead_code))]
pub fn parse(text: &str, width: IntWidth) -> Result<i64, IntegerError> {
    let invalid = || IntegerError::Invalid {
        text: text.to_string(),
    };
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    // Canonical form: no leading zeros, and `-0` is not a rendering Postgres
    // ever produces (it prints `0`).
    if digits.len() > 1 && digits.starts_with('0') {
        return Err(invalid());
    }
    if text.starts_with('-') && digits == "0" {
        return Err(invalid());
    }
    let value: i64 = text
        .parse()
        .map_err(|_| IntegerError::OutOfRange { width })?;
    if !width.fits(value) {
        return Err(IntegerError::OutOfRange { width });
    }
    Ok(value)
}

/// `a + b` for exact integers, with Postgres's overflow semantics: the result
/// type is the *wider operand's* type, and a sum that leaves that type's
/// range raises rather than widening (Postgres's `int2pl`/`int4pl`/`int8pl`
/// and the mixed-width family all `ereport(ERROR, errcode(22003))` on
/// overflow — they do not promote).
#[cfg_attr(not(any(test, feature = "test-util")), allow(dead_code))]
pub fn checked_add(
    a: i64,
    a_width: IntWidth,
    b: i64,
    b_width: IntWidth,
) -> Result<(i64, IntWidth), IntegerError> {
    let width = a_width.wider(b_width);
    let sum = a.checked_add(b).ok_or(IntegerError::OutOfRange { width })?;
    if !width.fits(sum) {
        return Err(IntegerError::OutOfRange { width });
    }
    Ok((sum, width))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_names_round_trip() {
        for width in IntWidth::ALL {
            assert_eq!(IntWidth::from_pg_name(width.pg_name()), Some(width));
        }
        assert_eq!(IntWidth::from_pg_name("int4"), None);
        assert_eq!(IntWidth::from_pg_name("numeric"), None);
    }

    #[test]
    fn ranges_match_postgres() {
        assert_eq!(IntWidth::Int2.range(), (-32768, 32767));
        assert_eq!(IntWidth::Int4.range(), (-2147483648, 2147483647));
        assert_eq!(IntWidth::Int8.range(), (i64::MIN, i64::MAX));
    }

    #[test]
    fn addition_overflows_at_the_result_type_not_at_i64() {
        // Postgres: `select 32767::smallint + 1::smallint` -> 22003.
        assert_eq!(
            checked_add(32767, IntWidth::Int2, 1, IntWidth::Int2),
            Err(IntegerError::OutOfRange {
                width: IntWidth::Int2
            })
        );
        // ...but `32767::smallint + 1::integer` is `int24pl` -> integer, fine.
        assert_eq!(
            checked_add(32767, IntWidth::Int2, 1, IntWidth::Int4),
            Ok((32768, IntWidth::Int4))
        );
        assert_eq!(
            checked_add(2147483647, IntWidth::Int4, 1, IntWidth::Int4),
            Err(IntegerError::OutOfRange {
                width: IntWidth::Int4
            })
        );
        assert_eq!(
            checked_add(2147483647, IntWidth::Int4, 1, IntWidth::Int8),
            Ok((2147483648, IntWidth::Int8))
        );
        assert_eq!(
            checked_add(i64::MAX, IntWidth::Int8, 1, IntWidth::Int8),
            Err(IntegerError::OutOfRange {
                width: IntWidth::Int8
            })
        );
    }

    #[test]
    fn out_of_range_message_is_postgres_wording() {
        assert_eq!(
            IntegerError::OutOfRange {
                width: IntWidth::Int4
            }
            .to_string(),
            "integer out of range"
        );
    }

    #[test]
    fn parse_accepts_only_postgres_canonical_form() {
        assert_eq!(parse("0", IntWidth::Int4), Ok(0));
        assert_eq!(parse("-42", IntWidth::Int4), Ok(-42));
        assert_eq!(parse("2147483647", IntWidth::Int4), Ok(2147483647));
        for bad in ["", "-", "007", "+7", " 7", "7 ", "1.0", "1e3", "-0", "x"] {
            assert!(
                parse(bad, IntWidth::Int4).is_err(),
                "{bad:?} is not canonical Postgres integer output"
            );
        }
    }

    #[test]
    fn parse_rejects_values_outside_the_columns_width() {
        assert_eq!(
            parse("32768", IntWidth::Int2),
            Err(IntegerError::OutOfRange {
                width: IntWidth::Int2
            })
        );
        assert_eq!(parse("32768", IntWidth::Int4), Ok(32768));
        // Wider than i64 entirely.
        assert!(matches!(
            parse("99999999999999999999", IntWidth::Int8),
            Err(IntegerError::OutOfRange { .. })
        ));
    }

    #[test]
    fn wider_picks_the_wider_operand() {
        assert_eq!(IntWidth::Int2.wider(IntWidth::Int8), IntWidth::Int8);
        assert_eq!(IntWidth::Int8.wider(IntWidth::Int2), IntWidth::Int8);
        assert_eq!(IntWidth::Int4.wider(IntWidth::Int4), IntWidth::Int4);
    }

    #[test]
    fn literal_typing_matches_postgres() {
        assert_eq!(IntWidth::narrowest_for(1), IntWidth::Int4);
        assert_eq!(IntWidth::narrowest_for(-32769), IntWidth::Int4);
        assert_eq!(IntWidth::narrowest_for(3_000_000_000), IntWidth::Int8);
    }
}
