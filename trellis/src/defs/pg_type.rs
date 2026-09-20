//! The PG-OID → [`ValueType`] registry (issue #108).
//!
//! Before this module, a column's [`ValueType`] was inferred from
//! [`pg_catalog.format_type`]'s *rendered text* (`"character varying(255)"`,
//! `"numeric(10,2)"`), matched against a handful of hardcoded strings after
//! stripping any `(...)` modifier — two independent, drifting copies of that
//! logic lived in `catalog.rs` and `staging::apply`. Anything the match
//! missed silently fell through to `ValueType::Text`, so a `jsonb` or
//! `timestamptz` column was indistinguishable from an actual `text` column
//! by the time it reached the evaluator.
//!
//! This registry instead keys off the column's `pg_attribute.atttypid` —
//! the raw, stable Postgres type OID — which sidesteps the modifier-stripping
//! entirely (`numeric(10,2)` and `numeric` share one OID; only `atttypmod`
//! differs) and gives every well-known builtin type an honest classification
//! instead of a lie. Per `docs/type-support.md`'s matrix, most of these
//! families ([`PgType`]'s variants) are still passthrough-only here — no
//! operators, casts, key, or aggregate role — that's deliberately left to
//! each family's own epic child (#111–#122); this issue's job is only to
//! stop mislabeling them, not to promote them.
//!
//! OIDs below are Postgres's own fixed `pg_type.oid` catalog constants
//! (identical across every supported server version — see
//! `src/include/catalog/pg_type.dat` upstream), not something that can
//! drift with a server upgrade.

use std::fmt;

use super::ast::ValueType;
use crate::float::FloatWidth;
use crate::integer::IntWidth;

/// A recognized Postgres type family that doesn't (yet) have its own
/// first-class [`ValueType`] variant. Carried inside [`ValueType::Other`]
/// and [`super::eval::Value::Other`] so passthrough/ingest (issue #108's
/// scope) can tag a value with its real type instead of collapsing it into
/// `Text`, while filtering/keys/casts/aggregates for it stay gated until the
/// matching epic child ([`docs/type-support.md`]) lands.
///
/// Deliberately a unit-variant-only enum (no OID/name payload) so
/// [`ValueType`] and [`super::eval::Value`]'s `Other` case can stay cheap and
/// `Copy`-friendly like every other bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgType {
    /// `oid` — an *unsigned* 32-bit row/object identifier.
    ///
    /// Issue #111 decided it does **not** join [`ValueType::Integer`]'s
    /// family (see [`crate::integer`]'s module doc): Postgres gives `oid` no
    /// arithmetic operators at all — `pg_operator` carries only the
    /// comparison family for it, there is no `oidpl` — and its range is
    /// unsigned, so modelling it as a signed, addable `IntWidth` would
    /// invent semantics the oracle doesn't have.
    ///
    /// It stays a passthrough [`PgType`] and gains only the roles it can
    /// honestly hold. Its text rendering *is* canonical (unsigned decimal,
    /// no sign, no leading zeros), so unlike every other family here it is
    /// genuinely text-stable: #111 admits it as a relationship/join key, a
    /// primary key (`catalog::TEXT_STABLE_JOIN_KEY_TYPES`), a `GROUP BY` key
    /// (`validate`'s key gate) and a typed literal
    /// (`typed_literal::TYPED_LITERALS`).
    Oid,
    Bytea,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
    Json,
    Jsonb,
    Inet,
    Cidr,
    MacAddr,
    MacAddr8,
    Bit,
    VarBit,
    Money,
    Xml,
    TsVector,
    TsQuery,
    /// Any OID the registry doesn't specifically recognize: arrays, ranges,
    /// composite/row types, geometric types, enum types (whose OIDs are
    /// assigned dynamically per `CREATE TYPE`, not fixed builtin constants),
    /// domains, and extension types (e.g. `citext`). `docs/type-support.md`
    /// defers most of these (#122) or hands them to their own child (#117
    /// for enums); until then this is strictly more honest than the old
    /// `_ => Text` fallthrough even though it's *operationally* still
    /// passthrough-only, same as every other variant here.
    Unrecognized,
}

impl PgType {
    /// The stable token this family serializes to in
    /// `transform_definitions.source_columns` (`catalog::encode_value_type`)
    /// — a persistence/diagnostic identity only. Every token below is
    /// unambiguous against [`ValueType`]'s own `numeric`/`text`/`boolean`/
    /// `uuid` tokens, so the two namespaces can share one column.
    ///
    /// **Not** the token to emit into generated SQL: [`Self::Unrecognized`]'s
    /// `"unrecognized"` is a deliberate non-type, so use
    /// [`Self::sql_type_name`] for DDL and `::` casts.
    pub const fn name(self) -> &'static str {
        match self {
            PgType::Oid => "oid",
            PgType::Bytea => "bytea",
            PgType::Date => "date",
            PgType::Time => "time",
            PgType::TimeTz => "timetz",
            PgType::Timestamp => "timestamp",
            PgType::TimestampTz => "timestamptz",
            PgType::Interval => "interval",
            PgType::Json => "json",
            PgType::Jsonb => "jsonb",
            PgType::Inet => "inet",
            PgType::Cidr => "cidr",
            PgType::MacAddr => "macaddr",
            PgType::MacAddr8 => "macaddr8",
            PgType::Bit => "bit",
            PgType::VarBit => "varbit",
            PgType::Money => "money",
            PgType::Xml => "xml",
            PgType::TsVector => "tsvector",
            PgType::TsQuery => "tsquery",
            PgType::Unrecognized => "unrecognized",
        }
    }

    /// The Postgres type keyword this family renders as in generated SQL —
    /// a target-table column's declared type (`super::ddl::pg_type_name`)
    /// and the cast that turns a staged text value back into its native
    /// type (`staging::apply`'s `field_pg_types`, `$n::text::<name>`).
    ///
    /// Identical to [`Self::name`] for every *recognized* family — each of
    /// those tokens is a real `pg_type.typname`, so `'...'::<name>` always
    /// parses. [`Self::Unrecognized`] is the exception and the reason this
    /// is a separate method: `"unrecognized"` is not a Postgres type, and
    /// interpolating it produced a raw `type "unrecognized" does not exist`
    /// (SQLSTATE 42704) out of `create table`/`insert` (issue #108 review).
    /// It renders as `text` instead — exactly what the pre-#108
    /// `value_type_from_pg` `_ => ValueType::Text` fallthrough emitted for
    /// these same types, so an unrecognized *to-side* relationship column
    /// (an enum, an array, a domain, `citext`, ...) keeps behaving as the
    /// verbatim text passthrough it always was, rather than newly failing.
    /// The *from-side* never reaches here at all: `Trellis::source_columns`
    /// drops unrecognized-typed source columns outright, the same as before
    /// this issue (see that method).
    pub const fn sql_type_name(self) -> &'static str {
        match self {
            PgType::Unrecognized => "text",
            other => other.name(),
        }
    }

    /// The inverse of [`Self::name`], for decoding a persisted
    /// `source_columns` entry back into a [`PgType`]. `None` for a token
    /// this build doesn't recognize (a forward-compat guard, same as
    /// [`ValueType`]'s own decode sites — a newer writer's token reaching an
    /// older reader should be a named [`super::catalog::CatalogError::UnknownValueType`],
    /// not a silent misparse).
    pub fn from_name(text: &str) -> Option<PgType> {
        Some(match text {
            "oid" => PgType::Oid,
            "bytea" => PgType::Bytea,
            "date" => PgType::Date,
            "time" => PgType::Time,
            "timetz" => PgType::TimeTz,
            "timestamp" => PgType::Timestamp,
            "timestamptz" => PgType::TimestampTz,
            "interval" => PgType::Interval,
            "json" => PgType::Json,
            "jsonb" => PgType::Jsonb,
            "inet" => PgType::Inet,
            "cidr" => PgType::Cidr,
            "macaddr" => PgType::MacAddr,
            "macaddr8" => PgType::MacAddr8,
            "bit" => PgType::Bit,
            "varbit" => PgType::VarBit,
            "money" => PgType::Money,
            "xml" => PgType::Xml,
            "tsvector" => PgType::TsVector,
            "tsquery" => PgType::TsQuery,
            "unrecognized" => PgType::Unrecognized,
            _ => return None,
        })
    }
}

impl fmt::Display for PgType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

// Well-known builtin `pg_type.oid` values this registry classifies. Every
// one of these is a fixed Postgres catalog constant (`src/include/catalog/pg_type.dat`
// upstream), not something that varies by server version or installation.
mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const XML: u32 = 142;
    pub const CIDR: u32 = 650;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const MACADDR8: u32 = 774;
    pub const MONEY: u32 = 790;
    pub const MACADDR: u32 = 829;
    pub const INET: u32 = 869;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const INTERVAL: u32 = 1186;
    pub const TIMETZ: u32 = 1266;
    pub const BIT: u32 = 1560;
    pub const VARBIT: u32 = 1562;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const TSVECTOR: u32 = 3614;
    pub const TSQUERY: u32 = 3615;
    pub const JSONB: u32 = 3802;
}

/// Classifies a column's raw `pg_attribute.atttypid` into the evaluator's
/// [`ValueType`] — the OID-based replacement for the old
/// `format_type`-text-matching `value_type_from_pg` helpers duplicated in
/// `catalog.rs` and `staging::apply`.
///
/// Anything not recognized below (an array/range/composite/geometric type,
/// a dynamically-OID'd enum, a domain, or an extension type like `citext`)
/// becomes [`ValueType::Other(PgType::Unrecognized)`] rather than silently
/// becoming `Text` — still passthrough-only in practice today, but no longer
/// a lie about what the column actually is.
pub fn value_type_for_oid(type_oid: u32) -> ValueType {
    match type_oid {
        oid::BOOL => ValueType::Boolean,
        oid::UUID => ValueType::Uuid,
        // Issues #111 and #112: the six types that used to share one
        // `ValueType::Numeric` bucket are now three families. `numeric` —
        // and only `numeric` — is the arbitrary-precision decimal the
        // bucket was always named after.
        oid::INT2 => ValueType::Integer(IntWidth::Int2),
        oid::INT4 => ValueType::Integer(IntWidth::Int4),
        oid::INT8 => ValueType::Integer(IntWidth::Int8),
        oid::FLOAT4 => ValueType::Float(FloatWidth::Float4),
        oid::FLOAT8 => ValueType::Float(FloatWidth::Float8),
        oid::NUMERIC => ValueType::Numeric,
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME => ValueType::Text,
        oid::OID => ValueType::Other(PgType::Oid),
        oid::BYTEA => ValueType::Other(PgType::Bytea),
        oid::DATE => ValueType::Other(PgType::Date),
        oid::TIME => ValueType::Other(PgType::Time),
        oid::TIMETZ => ValueType::Other(PgType::TimeTz),
        oid::TIMESTAMP => ValueType::Other(PgType::Timestamp),
        oid::TIMESTAMPTZ => ValueType::Other(PgType::TimestampTz),
        oid::INTERVAL => ValueType::Other(PgType::Interval),
        oid::JSON => ValueType::Other(PgType::Json),
        oid::JSONB => ValueType::Other(PgType::Jsonb),
        oid::INET => ValueType::Other(PgType::Inet),
        oid::CIDR => ValueType::Other(PgType::Cidr),
        oid::MACADDR => ValueType::Other(PgType::MacAddr),
        oid::MACADDR8 => ValueType::Other(PgType::MacAddr8),
        oid::BIT => ValueType::Other(PgType::Bit),
        oid::VARBIT => ValueType::Other(PgType::VarBit),
        oid::MONEY => ValueType::Other(PgType::Money),
        oid::XML => ValueType::Other(PgType::Xml),
        oid::TSVECTOR => ValueType::Other(PgType::TsVector),
        oid::TSQUERY => ValueType::Other(PgType::TsQuery),
        _ => ValueType::Other(PgType::Unrecognized),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_known_builtins_classify_as_expected() {
        assert_eq!(value_type_for_oid(oid::BOOL), ValueType::Boolean);
        assert_eq!(value_type_for_oid(oid::UUID), ValueType::Uuid);
        assert_eq!(value_type_for_oid(oid::TEXT), ValueType::Text);
        assert_eq!(value_type_for_oid(oid::VARCHAR), ValueType::Text);
        assert_eq!(
            value_type_for_oid(oid::INT2),
            ValueType::Integer(IntWidth::Int2)
        );
        assert_eq!(
            value_type_for_oid(oid::INT4),
            ValueType::Integer(IntWidth::Int4)
        );
        assert_eq!(
            value_type_for_oid(oid::INT8),
            ValueType::Integer(IntWidth::Int8)
        );
        assert_eq!(value_type_for_oid(oid::NUMERIC), ValueType::Numeric);
        // Issue #112: `real`/`double precision` are their own family now,
        // no longer aliases of the arbitrary-precision decimal.
        assert_eq!(
            value_type_for_oid(oid::FLOAT4),
            ValueType::Float(FloatWidth::Float4)
        );
        assert_eq!(
            value_type_for_oid(oid::FLOAT8),
            ValueType::Float(FloatWidth::Float8)
        );
        assert_eq!(
            value_type_for_oid(oid::BYTEA),
            ValueType::Other(PgType::Bytea)
        );
        assert_eq!(
            value_type_for_oid(oid::JSONB),
            ValueType::Other(PgType::Jsonb)
        );
        assert_eq!(
            value_type_for_oid(oid::TIMESTAMPTZ),
            ValueType::Other(PgType::TimestampTz)
        );
    }

    #[test]
    fn unrecognized_oid_is_tagged_not_silently_text() {
        // A made-up OID with no builtin meaning (e.g. an enum's dynamically
        // assigned type) must not collapse into `Text`.
        assert_eq!(
            value_type_for_oid(999_999),
            ValueType::Other(PgType::Unrecognized)
        );
    }

    /// Every [`PgType`] variant, so the tests below stay exhaustive by
    /// construction.
    const ALL: [PgType; 21] = [
        PgType::Oid,
        PgType::Bytea,
        PgType::Date,
        PgType::Time,
        PgType::TimeTz,
        PgType::Timestamp,
        PgType::TimestampTz,
        PgType::Interval,
        PgType::Json,
        PgType::Jsonb,
        PgType::Inet,
        PgType::Cidr,
        PgType::MacAddr,
        PgType::MacAddr8,
        PgType::Bit,
        PgType::VarBit,
        PgType::Money,
        PgType::Xml,
        PgType::TsVector,
        PgType::TsQuery,
        PgType::Unrecognized,
    ];

    #[test]
    fn every_pg_type_name_round_trips() {
        for pg_type in ALL {
            assert_eq!(PgType::from_name(pg_type.name()), Some(pg_type));
        }
    }

    #[test]
    fn sql_type_name_is_never_the_unrecognized_non_type() {
        for pg_type in ALL {
            let sql = pg_type.sql_type_name();
            assert_ne!(
                sql, "unrecognized",
                "{pg_type} must not render a fake pg type into SQL"
            );
            if pg_type == PgType::Unrecognized {
                assert_eq!(sql, "text");
            } else {
                assert_eq!(
                    sql,
                    pg_type.name(),
                    "a recognized family's SQL keyword is its persisted token"
                );
            }
        }
    }

    #[test]
    fn unknown_persisted_token_does_not_parse() {
        assert_eq!(PgType::from_name("frobnicate"), None);
    }
}
