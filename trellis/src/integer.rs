//! Postgres's **exact integer** widths: `smallint`/`integer`/`bigint`.
//!
//! [`IntWidth`] is the width payload of `ValueType::Integer`, which type
//! inference needs because Postgres types `int4 + int4` as `int4` and raises
//! `22003` when the sum leaves `int4`; a derived target column is declared
//! with the type Postgres infers. A width is a payload on one variant, not
//! three variants, because only the sites that care about the overflow
//! boundary look at it.
//!
//! `oid` is not an [`IntWidth`]: Postgres gives it no arithmetic operators and
//! it is unsigned, so it stays `PgType::Oid` (see `docs/type-support.md`).

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
    /// this function — decides that case (see `validate`'s `number_literal_type`).
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
    /// `integer`. The SQL the engine renders keeps that: an out-of-range sum raises at run time.
    pub fn wider(self, other: IntWidth) -> IntWidth {
        self.max(other)
    }
}

impl fmt::Display for IntWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.pg_name())
    }
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
