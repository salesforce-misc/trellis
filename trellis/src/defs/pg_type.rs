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

use std::collections::HashSet;
use std::fmt;
use std::sync::{Mutex, OnceLock};

use super::ast::ValueType;
use crate::float::FloatWidth;
use crate::integer::IntWidth;
use crate::pool::Client;

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
    /// A user-defined enum type (`CREATE TYPE ... AS ENUM (...)`, issue
    /// #117) — the one variant here that is not a single, universal
    /// Postgres family. Every other [`PgType`] variant names exactly one
    /// fixed builtin type across every Postgres installation (`bytea` is
    /// always OID 17); `CREATE TYPE` instead mints an arbitrary, per-schema
    /// type with a dynamically-assigned OID, and a database can hold
    /// arbitrarily many of them (`status_enum`, `priority_enum`, ...) that
    /// must never be conflated with one another — two different enum
    /// columns are no more comparable than a `bytea` column and a `uuid`
    /// one. Deliberately breaks this enum's own "unit-variant-only" module
    /// doc invariant for that reason: unlike every family above, "enum-ness"
    /// alone is not enough identity to carry.
    ///
    /// The payload is an **interned** (see [`intern_enum_token`]) copy of
    /// this type's *own persisted token* (`"enum:<schema>.<typname>"`,
    /// [`Self::name`]'s exact return value for this variant) — not the raw
    /// OID. Two reasons OID was rejected as the identity, both concrete
    /// rather than theoretical:
    ///
    /// * `ALTER TYPE ... ADD VALUE` (adding a label, possibly
    ///   `BEFORE`/`AFTER` an existing one) never changes the type's own
    ///   OID — only `DROP TYPE`/`CREATE TYPE` does — so an OID identity
    ///   would already have been stable across the exact schema change this
    ///   issue's "type-versioning" framing worries about. That is not why
    ///   it was rejected.
    /// * The real reason: every `transform_definitions.source_columns`
    ///   entry this crate ever persists is expected to remain meaningfully
    ///   decodable after a restart, and Postgres does **not** guarantee a
    ///   user-defined type's OID survives a `pg_dump`/`pg_restore` cycle —
    ///   an ordinary operational event, not a schema change at all — the
    ///   way it guarantees a *name* survives. A restart that silently
    ///   remapped a persisted OID to a *different*, wrong (or dangling) enum
    ///   type would be a far worse failure mode than anything `ALTER TYPE`
    ///   can cause. The schema-qualified *name* (the same identity ADR-0007
    ///   already uses for every table this crate tracks) has no such gap.
    ///
    /// Interning keeps this variant, and therefore [`PgType`] and
    /// [`ValueType`], `Copy` — the same cheap-by-value shape every sibling
    /// variant already has — at the cost of leaking one small string the
    /// first time this process ever classifies a given enum type. That is
    /// bounded by the number of *distinct* enum types this process
    /// classifies over its lifetime (typically a handful, stable across a
    /// long-running service), never by the number of rows or classification
    /// calls.
    Enum(&'static str),
    /// Any OID the registry doesn't specifically recognize: arrays, ranges,
    /// composite/row types, geometric types, domains, and extension types
    /// (e.g. `citext`). `docs/type-support.md` defers these (#122); until
    /// then this is strictly more honest than the old `_ => Text`
    /// fallthrough even though it's *operationally* still passthrough-only,
    /// same as every other variant here.
    Unrecognized,
}

/// The persisted-token prefix that marks [`PgType::Enum`]'s namespace within
/// the shared token space [`Self::name`]/[`Self::from_name`] share with
/// every other [`PgType`] variant and with [`ValueType`]'s own tokens
/// (`catalog::encode_value_type`/`decode_value_type`). No fixed keyword
/// used by any other variant contains a `:` (or a `.`, for that matter), so
/// this can never collide with one — but the prefix is checked explicitly
/// in [`PgType::from_name`] rather than relying on that alone, so a
/// genuinely corrupt/future-version token still decodes to `None` (a named
/// `CatalogError::UnknownValueType`) instead of being silently misparsed as
/// an enum reference.
const ENUM_TOKEN_PREFIX: &str = "enum:";

/// Interns `qualified_name` (a `"schema.typname"` string with no embedded
/// `.` in either component — see [`intake::publication::qualify`]) as this
/// process's single, shared `&'static str` for that enum type's persisted
/// token, deduplicating repeat classifications of the same type so this
/// process never leaks more than one string per *distinct* enum type it
/// ever sees (see [`PgType::Enum`]'s own doc comment for why interning was
/// chosen over threading an owned `String` through, which would cost
/// [`PgType`]/[`ValueType`] their `Copy`-ness).
fn intern_enum_token(qualified_name: &str) -> &'static str {
    static INTERNED: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let token = format!("{ENUM_TOKEN_PREFIX}{qualified_name}");
    let set = INTERNED.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = set.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = guard.get(token.as_str()) {
        return existing;
    }
    let leaked: &'static str = Box::leak(token.into_boxed_str());
    guard.insert(leaked);
    leaked
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
            // Issue #117: already the exact persisted token — see
            // `PgType::Enum`'s own doc comment and `ENUM_TOKEN_PREFIX`.
            PgType::Enum(token) => token,
            PgType::Unrecognized => "unrecognized",
        }
    }

    /// The bare, schema-qualified name this enum type was introspected
    /// under (`"public.status_enum"`, no `enum:` prefix) — `None` for every
    /// other variant. A cheap slice of the already-interned
    /// [`Self::name`] token (`ENUM_TOKEN_PREFIX`'s own length is fixed and
    /// ASCII, so this can never land mid-character), not a fresh
    /// allocation.
    pub fn enum_qualified_name(self) -> Option<&'static str> {
        match self {
            PgType::Enum(token) => Some(&token[ENUM_TOKEN_PREFIX.len()..]),
            _ => None,
        }
    }

    /// Builds a [`PgType::Enum`] for the schema-qualified enum type name
    /// `qualified_name` (`"schema.typname"`, as returned by
    /// [`lookup_enum_qualified_name`]) — the sole constructor for that
    /// variant, so every [`PgType::Enum`] in this process is guaranteed to
    /// hold an [`intern_enum_token`]-interned token.
    pub(crate) fn for_enum(qualified_name: &str) -> PgType {
        PgType::Enum(intern_enum_token(qualified_name))
    }

    /// The Postgres type keyword this family renders as in generated SQL —
    /// a target-table column's declared type (`super::ddl::pg_type_name`)
    /// and the cast that turns a staged text value back into its native
    /// type (`staging::apply`'s `field_pg_types`, `$n::text::<name>`).
    ///
    /// Identical to [`Self::name`] for every *recognized* family except
    /// [`Self::Enum`] — each fixed-keyword family's token *is* a real
    /// `pg_type.typname`, so `'...'::<name>` always parses without
    /// quoting. [`Self::Unrecognized`] is the first exception: `"unrecognized"`
    /// is not a Postgres type at all, and interpolating it produced a raw
    /// `type "unrecognized" does not exist` (SQLSTATE 42704) out of `create
    /// table`/`insert` (issue #108 review); it renders as `text` instead,
    /// exactly what the pre-#108 `value_type_from_pg` `_ => ValueType::Text`
    /// fallthrough emitted for these same types.
    ///
    /// [`Self::Enum`] is the second, newer exception (issue #117): its bare
    /// [`Self::enum_qualified_name`] (`public.status_enum`) is *usually* a
    /// valid bare identifier pair, but nothing about a `CREATE TYPE ... AS
    /// ENUM` name guarantees that — an operator can quote arbitrary
    /// characters (case, whitespace, a reserved word) into either the
    /// schema or the type name at creation time — so this quotes both
    /// components (`crate::pool::quote_ident`) rather than assume the
    /// common case, the same "quote every identifier this crate didn't
    /// choose itself" discipline every DDL site in this crate already
    /// follows. Unlike every other variant, this allocates: an enum's
    /// rendering is a function of *which* enum type this is, not a fixed
    /// `&'static str` the way a builtin keyword is.
    pub fn sql_type_name(self) -> std::borrow::Cow<'static, str> {
        match self {
            PgType::Unrecognized => std::borrow::Cow::Borrowed("text"),
            PgType::Enum(_) => {
                let qualified = self
                    .enum_qualified_name()
                    .expect("PgType::Enum always has a qualified name");
                let (schema, typname) = qualified
                    .split_once('.')
                    .expect("interned enum tokens are always schema.typname-shaped");
                std::borrow::Cow::Owned(format!(
                    "{}.{}",
                    crate::pool::quote_ident(schema),
                    crate::pool::quote_ident(typname)
                ))
            }
            other => std::borrow::Cow::Borrowed(other.name()),
        }
    }

    /// The inverse of [`Self::name`], for decoding a persisted
    /// `source_columns` entry back into a [`PgType`]. `None` for a token
    /// this build doesn't recognize (a forward-compat guard, same as
    /// [`ValueType`]'s own decode sites — a newer writer's token reaching an
    /// older reader should be a named [`super::catalog::CatalogError::UnknownValueType`],
    /// not a silent misparse). Issue #117: a token carrying
    /// [`ENUM_TOKEN_PREFIX`] decodes to [`PgType::Enum`] rather than falling
    /// through to `None` — see that prefix's own doc comment for why this
    /// stays a forward-compat-safe, unambiguous check rather than "anything
    /// unrecognized must be an enum".
    pub fn from_name(text: &str) -> Option<PgType> {
        if let Some(qualified) = text.strip_prefix(ENUM_TOKEN_PREFIX) {
            return Some(PgType::Enum(intern_enum_token(qualified)));
        }
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
/// [`ValueType`], for every *fixed builtin* family — the OID-based
/// replacement for the old `format_type`-text-matching `value_type_from_pg`
/// helpers duplicated in `catalog.rs` and `staging::apply`. Pure and
/// DB-less: every OID below is a fixed Postgres catalog constant, so no
/// live lookup is ever needed to classify one.
///
/// Anything not recognized below (an array/range/composite/geometric type,
/// a dynamically-OID'd enum, a domain, or an extension type like `citext`)
/// becomes [`ValueType::Other(PgType::Unrecognized)`] rather than silently
/// becoming `Text` — still passthrough-only in practice today, but no longer
/// a lie about what the column actually is. [`value_type_for_oid`] is the
/// public, DB-aware entry point every real caller uses — it falls back to
/// this function first and only reaches for a connection when `type_oid`
/// isn't one of the fixed constants here, to also recognize issue #117's
/// enum types before giving up and calling something `Unrecognized`.
fn value_type_for_builtin_oid(type_oid: u32) -> ValueType {
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

/// Classifies a column's raw `pg_attribute.atttypid` into the evaluator's
/// [`ValueType`] (issue #117) — [`value_type_for_builtin_oid`], widened to
/// also recognize a user-defined enum type rather than collapsing it into
/// [`PgType::Unrecognized`] the way every other unrecognized OID still does.
///
/// Every fixed builtin OID resolves with **no query at all** — the common
/// case, and the only case before this issue existed. Only an OID this
/// process hasn't seen classified as a builtin reaches for `client`, to ask
/// `pg_catalog` directly whether it names a live enum type
/// (`pg_type.typtype = 'e'`) and, if so, what its schema-qualified name is —
/// the identity [`PgType::for_enum`] interns. A `DROP TYPE`'d OID, an array/
/// range/composite/domain/extension type, or any other OID this registry
/// still doesn't place resolves to [`PgType::Unrecognized`] exactly as
/// before this issue, unchanged: this crate has no existing precedent for
/// treating "a referenced type disappeared" specially for any other
/// [`PgType`] family (there is no live mechanism that notices a `bytea`
/// column's table got dropped out from under a still-live definition
/// either), so this issue doesn't invent one for enums specifically — see
/// `docs/type-support.md`'s enum section.
pub async fn value_type_for_oid(
    client: &Client,
    type_oid: u32,
) -> Result<ValueType, tokio_postgres::Error> {
    let builtin = value_type_for_builtin_oid(type_oid);
    if builtin != ValueType::Other(PgType::Unrecognized) {
        return Ok(builtin);
    }
    Ok(match lookup_enum_qualified_name(client, type_oid).await? {
        Some(qualified) => ValueType::Other(PgType::for_enum(&qualified)),
        None => ValueType::Other(PgType::Unrecognized),
    })
}

/// Looks up `type_oid` in `pg_catalog` and returns its schema-qualified name
/// (`"schema.typname"`, [`intake::publication::qualify`]'s joining
/// convention) iff it names a live enum type (`pg_type.typtype = 'e'`) —
/// `None` for anything else (a non-enum type, or an OID pointing at nothing
/// live at all, e.g. a since-`DROP TYPE`'d one).
///
/// `n.nspname`/`t.typname` are read straight off `pg_catalog`, which never
/// stores an embedded `.` in either an identifier's raw (unquoted) name —
/// unlike a value, a Postgres identifier component is inherently `.`-free
/// (a literal `.` in one requires double-quoting *and* is vanishingly rare
/// in practice); [`intake::publication::qualify`]'s own dot-rejection guard
/// exists for exactly this same class of identifier and is reused here
/// rather than hand-joining with `format!("{schema}.{typname}")`, so this
/// stays the single place that decides what "qualified" means. A
/// hypothetical dotted schema/type name degrades to [`PgType::Unrecognized`]
/// (via [`value_type_for_oid`]'s caller) rather than panicking or silently
/// mis-joining.
async fn lookup_enum_qualified_name(
    client: &Client,
    type_oid: u32,
) -> Result<Option<String>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "select n.nspname::text, t.typname::text \
             from pg_catalog.pg_type t \
             join pg_catalog.pg_namespace n on n.oid = t.typnamespace \
             where t.oid = $1 and t.typtype = 'e'",
            &[&type_oid],
        )
        .await?;
    Ok(row.and_then(|row| {
        let schema: String = row.get(0);
        let typname: String = row.get(1);
        crate::intake::publication::qualify(&schema, &typname).ok()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_known_builtins_classify_as_expected() {
        assert_eq!(value_type_for_builtin_oid(oid::BOOL), ValueType::Boolean);
        assert_eq!(value_type_for_builtin_oid(oid::UUID), ValueType::Uuid);
        assert_eq!(value_type_for_builtin_oid(oid::TEXT), ValueType::Text);
        assert_eq!(value_type_for_builtin_oid(oid::VARCHAR), ValueType::Text);
        assert_eq!(
            value_type_for_builtin_oid(oid::INT2),
            ValueType::Integer(IntWidth::Int2)
        );
        assert_eq!(
            value_type_for_builtin_oid(oid::INT4),
            ValueType::Integer(IntWidth::Int4)
        );
        assert_eq!(
            value_type_for_builtin_oid(oid::INT8),
            ValueType::Integer(IntWidth::Int8)
        );
        assert_eq!(value_type_for_builtin_oid(oid::NUMERIC), ValueType::Numeric);
        // Issue #112: `real`/`double precision` are their own family now,
        // no longer aliases of the arbitrary-precision decimal.
        assert_eq!(
            value_type_for_builtin_oid(oid::FLOAT4),
            ValueType::Float(FloatWidth::Float4)
        );
        assert_eq!(
            value_type_for_builtin_oid(oid::FLOAT8),
            ValueType::Float(FloatWidth::Float8)
        );
        assert_eq!(
            value_type_for_builtin_oid(oid::BYTEA),
            ValueType::Other(PgType::Bytea)
        );
        assert_eq!(
            value_type_for_builtin_oid(oid::JSONB),
            ValueType::Other(PgType::Jsonb)
        );
        assert_eq!(
            value_type_for_builtin_oid(oid::TIMESTAMPTZ),
            ValueType::Other(PgType::TimestampTz)
        );
    }

    #[test]
    fn unrecognized_oid_is_tagged_not_silently_text() {
        // A made-up OID with no builtin meaning (e.g. an enum's dynamically
        // assigned type) must not collapse into `Text`.
        assert_eq!(
            value_type_for_builtin_oid(999_999),
            ValueType::Other(PgType::Unrecognized)
        );
    }

    /// A representative [`PgType::Enum`], for the tests below that need one
    /// — a plain string literal (`&'static str`) is a perfectly valid
    /// interned-token payload, no call into [`intern_enum_token`] required.
    const DEMO_ENUM: PgType = PgType::Enum("enum:public.demo_enum");

    /// Every [`PgType`] variant, so the tests below stay exhaustive by
    /// construction.
    const ALL: [PgType; 22] = [
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
        DEMO_ENUM,
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
            match pg_type {
                PgType::Unrecognized => assert_eq!(sql, "text"),
                // Issue #117: the one family whose SQL rendering isn't its
                // persisted token verbatim — it's quoted-and-qualified,
                // since an enum type's name is chosen by whoever ran
                // `CREATE TYPE`, not by this crate. See `PgType::sql_type_name`'s
                // own doc comment.
                PgType::Enum(_) => assert_eq!(sql, "\"public\".\"demo_enum\""),
                _ => assert_eq!(
                    sql,
                    pg_type.name(),
                    "a recognized family's SQL keyword is its persisted token"
                ),
            }
        }
    }

    #[test]
    fn unknown_persisted_token_does_not_parse() {
        assert_eq!(PgType::from_name("frobnicate"), None);
    }

    #[test]
    fn enum_qualified_name_strips_the_persisted_prefix() {
        assert_eq!(DEMO_ENUM.enum_qualified_name(), Some("public.demo_enum"));
        assert_eq!(PgType::Bytea.enum_qualified_name(), None);
    }

    #[test]
    fn two_distinct_enum_types_are_not_the_same_pgtype() {
        // Issue #117's own stated risk: "enum-ness" alone must not be
        // conflated across distinct `CREATE TYPE`s.
        let status = PgType::for_enum("public.status_enum");
        let priority = PgType::for_enum("public.priority_enum");
        assert_ne!(status, priority);
        assert_ne!(ValueType::Other(status), ValueType::Other(priority));
    }

    #[test]
    fn for_enum_interns_so_repeat_classifications_share_one_token() {
        let a = PgType::for_enum("public.status_enum");
        let b = PgType::for_enum("public.status_enum");
        assert_eq!(a, b);
        let (PgType::Enum(ta), PgType::Enum(tb)) = (a, b) else {
            unreachable!("for_enum always returns PgType::Enum");
        };
        // Same *pointer*, not just equal contents — the whole point of
        // interning.
        assert!(std::ptr::eq(ta, tb));
    }
}
