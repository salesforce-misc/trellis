//! Re-validating a definition's key columns after define (issue #760).
//!
//! Define refuses a key column whose type or collation Trellis can't match
//! by: a type off the key allowlist ([`super::catalog::is_text_stable_join_key_type`],
//! [`super::ddl::source_primary_key`]), a `GROUP BY` type
//! [`super::validate::reject_unsupported_group_by_key_type`] refuses, or a
//! nondeterministic collation (#590, #638). An `ALTER COLUMN ... TYPE` or
//! `... COLLATE` after define can give a key column such a type, or render
//! the keys Trellis already stored differently, and a table rewrite fires no
//! capture trigger.
//!
//! A key column is one Trellis matches rows by ([`KeyUse`]): the source's
//! row-identity key, a `GROUP BY` key, and, for each relationship the
//! definition reads through, its join columns and its to-side's key.
//!
//! The staging worker's capture pass checks each one
//! (`staging::schema_change::pause_readers_of_retyped`) and pauses the
//! definition when
//!
//! 1. **define would refuse it now** ([`refusal`]); or
//! 2. **its new type renders the values already stored differently**
//!    ([`renders_differently`]), against the type recorded when the
//!    definition was accepted or last resumed, or when a pass last accepted
//!    a change to the column without a pause (`definition_key_types`,
//!    [`record`], [`rerecord`]). The target, ledger and projection rows built from the old
//!    rendering would never match the new one: a `timestamp` key becomes
//!    `timestamptz` (`'… 10:00:00'` reads back as `'… 15:00:00+00'` after an
//!    `ALTER` in a New York session), `date` becomes `timestamp`, `text`
//!    becomes `uuid` or `integer` (`'007'` reads back as `7`), `varchar`
//!    becomes `character(n)` (padded), a `numeric` scale or a temporal
//!    precision narrows (the rewrite rounds), or a `varchar(n)` narrows (the
//!    rewrite strips trailing spaces past the new length).
//!
//! The same pass also pauses on a relationship whose two join columns no
//! longer have the same type, modifier and collation (#590, through
//! `catalog::validate_join_pair`), and on a widening of a column Trellis
//! keeps a typed copy of ([`super::copies`], [`widens`]). A widening that
//! changes only the catalog ([`catalog_only`]) re-types the copy in place
//! instead.
//!
//! Routine changes that need neither: widening an integer or string key
//! that no copy holds (an aggregate's source key, a to-many join's to-side),
//! a `numeric` precision change or wider scale on a `GROUP BY` key (its copy
//! is unconstrained `numeric`), a wider temporal precision on a `GROUP BY`
//! key (its copy is the type's full precision), `varchar` to `text`, and a
//! change between deterministic collations. None of them changes an existing
//! value's identity, and every copy still holds every value.
//!
//! A resume re-records these types ([`record_definition`]), after it has
//! re-validated the definition and brought its copies to their live types.
//! So does the capture pass, for each key column whose type changed in one
//! of these routine ways or by a widening it re-typed in place, unless it
//! pauses the definition ([`rerecord`]). The definition stores keys of the
//! new type from then on, so the next change is measured from it: a
//! `GROUP BY` key widened from `numeric(10,2)` to `numeric(10,3)` and then
//! narrowed back to `numeric(10,2)` rounds the keys stored in between, and
//! pauses.

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use tokio_postgres::GenericClient;

use super::ast::{GroupByKey, KeySpace, TransformDef};

/// How a definition uses one of its key columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyUse {
    /// Part of the definition's source's row-identity key.
    SourceKey,
    /// A `GROUP BY` key: a source column, or a to-side column read through
    /// a relationship.
    GroupBy,
    /// A join column of relationship `rel`.
    JoinColumn { rel: String },
    /// Part of relationship `rel`'s to-side's row-identity key.
    EndpointKey { rel: String },
}

impl fmt::Display for KeyUse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyUse::SourceKey => write!(f, "part of this definition's source key"),
            KeyUse::GroupBy => write!(f, "one of its GROUP BY keys"),
            KeyUse::JoinColumn { rel, .. } => write!(f, "a join column of relationship '{rel}'"),
            KeyUse::EndpointKey { rel } => {
                write!(f, "part of the key of relationship '{rel}''s to-side")
            }
        }
    }
}

/// A relationship a definition reads through, as [`key_uses`] needs it.
#[derive(Debug, Clone)]
pub(crate) struct RelRef<'a> {
    pub name: &'a str,
    pub from_col: &'a str,
    pub to_col: &'a str,
    /// The qualified to-side.
    pub to_table: &'a str,
}

/// The key columns `def` (sourced from the qualified `source`) uses on
/// `table`, and how. `table_key` is `table`'s row-identity key columns and
/// `rels` the relationships on `source` that `def` reads through (others are
/// ignored). A column can appear once per use.
pub(crate) fn key_uses(
    def: &TransformDef,
    source: &str,
    rels: &[RelRef<'_>],
    table: &str,
    table_key: &[String],
) -> Vec<(String, KeyUse)> {
    let read: BTreeSet<String> = super::eval::relationship_references(def)
        .into_iter()
        .map(|(rel, _)| rel)
        .collect();
    let group_by: &[GroupByKey] = match &def.key_space {
        KeySpace::Aggregate { group_by } => group_by,
        _ => &[],
    };
    let mut uses = Vec::new();
    if source == table {
        uses.extend(table_key.iter().map(|c| (c.clone(), KeyUse::SourceKey)));
        for key in group_by {
            if let GroupByKey::Column(column) = key {
                uses.push((column.clone(), KeyUse::GroupBy));
            }
        }
        for rel in rels.iter().filter(|r| read.contains(r.name)) {
            uses.push((
                rel.from_col.to_string(),
                KeyUse::JoinColumn {
                    rel: rel.name.to_string(),
                },
            ));
        }
    }
    for rel in rels
        .iter()
        .filter(|r| r.to_table == table && read.contains(r.name))
    {
        uses.push((
            rel.to_col.to_string(),
            KeyUse::JoinColumn {
                rel: rel.name.to_string(),
            },
        ));
        uses.extend(table_key.iter().map(|c| {
            (
                c.clone(),
                KeyUse::EndpointKey {
                    rel: rel.name.to_string(),
                },
            )
        }));
        for key in group_by {
            if let GroupByKey::RelationshipPath { rel: name, column } = key
                && name == rel.name
            {
                uses.push((column.clone(), KeyUse::GroupBy));
            }
        }
    }
    uses
}

/// A column's type, as `definition_key_types` records it: the type's
/// `schema.typname` and the column's `atttypmod`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnType {
    pub type_name: String,
    pub typmod: i32,
}

impl ColumnType {
    fn base(&self) -> &str {
        self.type_name
            .strip_prefix("pg_catalog.")
            .unwrap_or(&self.type_name)
    }

    fn builtin(&self) -> bool {
        self.type_name.starts_with("pg_catalog.")
    }

    /// The width of a builtin integer type: 2, 4 or 8 bytes.
    fn int_width(&self) -> Option<u8> {
        match self.base() {
            "int2" if self.builtin() => Some(2),
            "int4" if self.builtin() => Some(4),
            "int8" if self.builtin() => Some(8),
            _ => None,
        }
    }

    /// A builtin string type's length bound: `Some(None)` for `text` and an
    /// unbounded `varchar`, `Some(Some(n))` for `varchar(n)`, `None` for any
    /// other type.
    fn string_bound(&self) -> Option<Option<i32>> {
        match self.base() {
            "text" if self.builtin() => Some(None),
            // `varchar(n)`'s `atttypmod` is `n + 4`.
            "varchar" if self.builtin() => Some((self.typmod >= 4).then(|| self.typmod - 4)),
            _ => None,
        }
    }

    /// A temporal type's fractional-second precision, `-1` meaning the
    /// default, 6.
    fn precision(&self) -> i32 {
        if self.typmod < 0 { 6 } else { self.typmod }
    }
}

/// Whether string bound `is` holds fewer characters than `was` (`None` is
/// unbounded).
fn narrower_bound(is: Option<i32>, was: Option<i32>) -> bool {
    match (is, was) {
        (Some(is), Some(was)) => is < was,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// Whether a key column that had type `old` when Trellis built its state
/// would render that state's values differently now that it has `new`
/// (see the module doc).
///
/// - **Integers** (`smallint`, `integer`, `bigint`) render every value the
///   same within their family. A narrowing `ALTER` fails on a value that
///   doesn't fit, rather than changing it.
/// - **Strings** (`text`, `varchar(n)`) render the same unless the new bound
///   is narrower: the rewrite drops trailing spaces past it, silently
///   (`'abc   '` becomes `'abc'` under `varchar(3)`), so the stored key and
///   the source's no longer match.
/// - **The same type with another modifier** renders the same for `bit
///   varying`, whose stored bits don't change, and for a `numeric` change
///   that keeps or widens the scale (only a `GROUP BY` key can be `numeric`;
///   its groups are matched as numbers, and a wider scale re-renders `1.50`
///   as `1.500`, the same as two rows already can). A precision too narrow
///   for a value fails the `ALTER`. A narrower scale, or any scale on a
///   column that had none, rounds the stored values instead: `1.555` and
///   `1.556` both become `1.56`, merging two groups without a capture
///   trigger firing ([`numeric_rounds`]).
/// - **A temporal precision change** rounds the stored values when it
///   narrows. A wider one keeps them; a typed copy that can't hold the new
///   precision is [`widens`]'s.
/// - **Anything else** (another type, `character(n)`'s length, `bit(n)`'s)
///   renders differently.
pub(crate) fn renders_differently(old: &ColumnType, new: &ColumnType) -> bool {
    if old == new {
        return false;
    }
    if old.int_width().is_some() && new.int_width().is_some() {
        return false;
    }
    if let (Some(was), Some(is)) = (old.string_bound(), new.string_bound()) {
        return narrower_bound(is, was);
    }
    if old.type_name != new.type_name || !old.builtin() {
        return true;
    }
    match old.base() {
        "numeric" => numeric_rounds(old.typmod, new.typmod),
        "varbit" => false,
        "time" | "timetz" | "timestamp" | "timestamptz" => new.precision() < old.precision(),
        _ => true,
    }
}

/// Whether a typed copy of type `copy` can't hold every value of its source
/// column's live type `live`, because `live` is a widening of it (#767):
///
/// - a wider integer (`smallint` → `integer` → `bigint`), or `real` →
///   `double precision`;
/// - an integer to a `numeric` with fractional digits, or with more integer
///   digits than the integer holds, or with no limit at all
///   ([`integer_outgrown_by_numeric`]): `bigint` → `numeric`;
/// - a longer string bound (`varchar(n)` → `varchar(m>n)`, `text`, or an
///   unbounded `varchar`);
/// - a `numeric` with room for more integer or fractional digits, or none
///   at all ([`numeric_widens`]);
/// - a wider temporal, `interval` or `bit varying` precision or length.
///
/// Anything else is not a widening: equal types, a narrowing (every value
/// still fits the copy), or another type altogether (a re-rendering,
/// [`renders_differently`]'s, for a key).
pub(crate) fn widens(copy: &ColumnType, live: &ColumnType) -> bool {
    if copy == live {
        return false;
    }
    if let (Some(was), Some(is)) = (copy.int_width(), live.int_width()) {
        return is > was;
    }
    if let (Some(was), Some(is)) = (copy.string_bound(), live.string_bound()) {
        return narrower_bound(was, is);
    }
    if copy.builtin() && live.builtin() && copy.base() == "float4" && live.base() == "float8" {
        return true;
    }
    if let Some(width) = copy.int_width()
        && live.builtin()
        && live.base() == "numeric"
    {
        return integer_outgrown_by_numeric(width, live.typmod);
    }
    if copy.type_name != live.type_name || !copy.builtin() {
        return false;
    }
    match copy.base() {
        "numeric" => numeric_widens(copy.typmod, live.typmod),
        "varbit" => live.typmod < 0 || (copy.typmod >= 0 && live.typmod > copy.typmod),
        "time" | "timetz" | "timestamp" | "timestamptz" => live.precision() > copy.precision(),
        "interval" => interval_widens(copy.typmod, live.typmod),
        _ => false,
    }
}

/// Whether a column of type `copy` can be re-typed to `live` by changing
/// only the catalog: Postgres neither rewrites the table nor changes a value
/// it holds, and each index on the column is kept. Exactly these widenings
/// (#824):
///
/// - `varchar(n)` to `varchar(m)` with `m > n`;
/// - `varchar(n)`, or an unbounded `varchar`, to `text`;
/// - `varchar(n)` to an unbounded `varchar`;
/// - `numeric(p,s)` to `numeric(q,s)` with `q > p`: the same scale.
///
/// Nothing else, though `pg_cast` calls more casts binary-coercible:
/// `text` to `varchar(n)` is one, and it narrows. A `character(n)` length
/// change isn't one either, since its values are blank-padded to the
/// length.
pub(crate) fn catalog_only(copy: &ColumnType, live: &ColumnType) -> bool {
    if copy == live || !copy.builtin() || !live.builtin() {
        return false;
    }
    match (copy.base(), live.base()) {
        ("varchar", "text") => true,
        ("varchar", "varchar") => {
            copy.typmod >= 4 && (live.typmod < 0 || live.typmod > copy.typmod)
        }
        ("numeric", "numeric") => {
            match (numeric_modifier(copy.typmod), numeric_modifier(live.typmod)) {
                (Some((p, s)), Some((q, t))) => t == s && q > p,
                _ => false,
            }
        }
        _ => false,
    }
}

/// A `numeric(p,s)` modifier's precision and scale. A modifier is
/// `((p << 16) | (s & 0x7ff)) + 4`, with `s` an 11-bit two's complement,
/// since Postgres 15 allows a negative scale. `None` for an unconstrained
/// `numeric` (`-1`).
fn numeric_modifier(typmod: i32) -> Option<(i32, i32)> {
    (typmod >= 4).then(|| {
        let precision = ((typmod - 4) >> 16) & 0xffff;
        let s = (typmod - 4) & 0x7ff;
        (precision, if s & 0x400 != 0 { s - 0x800 } else { s })
    })
}

/// Whether re-typing a `numeric` column from modifier `old` to `new` rounds
/// a value it holds: when `new` has a scale and `old` had none, or a larger
/// one. An unconstrained `numeric` (`-1`) keeps every value.
fn numeric_rounds(old: i32, new: i32) -> bool {
    match (numeric_modifier(old), numeric_modifier(new)) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some((_, was)), Some((_, is))) => is < was,
    }
}

/// Whether a `numeric` of modifier `live` holds a value an integer of
/// `width` bytes can't: it has no limit, or a fractional digit, or more
/// integer digits (`p - s`) than every value of the integer's range has (4,
/// 9 and 18 for 2, 4 and 8 bytes).
fn integer_outgrown_by_numeric(width: u8, live: i32) -> bool {
    let digits = match width {
        2 => 4,
        4 => 9,
        _ => 18,
    };
    match numeric_modifier(live) {
        None => true,
        Some((precision, scale)) => scale > 0 || precision - scale > digits,
    }
}

/// Whether a `numeric` of modifier `live` holds a value a copy of modifier
/// `copy` can't: it is unconstrained and the copy isn't, or it has more
/// fractional digits, or more integer digits (`p - s`).
fn numeric_widens(copy: i32, live: i32) -> bool {
    match (numeric_modifier(copy), numeric_modifier(live)) {
        (_, None) => copy >= 4,
        (None, Some(_)) => false,
        (Some((p, s)), Some((lp, ls))) => ls > s || lp - ls > p - s,
    }
}

/// Whether an `interval` of modifier `live` holds a value a copy of
/// modifier `copy` can't: its field range covers fields the copy's doesn't,
/// or its fractional-second precision is wider. A modifier is
/// `(range << 16) | precision`, the precision `0xffff` for the full one;
/// `-1` is the full range at full precision.
fn interval_widens(copy: i32, live: i32) -> bool {
    const FULL_PRECISION: i32 = 0xffff;
    let split = |typmod: i32| {
        if typmod < 0 {
            (0x7fff, 6)
        } else {
            let precision = typmod & 0xffff;
            (
                (typmod >> 16) & 0x7fff,
                if precision == FULL_PRECISION {
                    6
                } else {
                    precision
                },
            )
        }
    };
    let ((copy_range, copy_precision), (live_range, live_precision)) = (split(copy), split(live));
    live_range & !copy_range != 0 || live_precision > copy_precision
}

/// A live column's type, as [`refusal`] and [`renders_differently`] need it.
#[derive(Debug, Clone)]
pub(crate) struct LiveColumn {
    pub ty: ColumnType,
    /// `format_type`'s rendering, which the key allowlist is written in.
    pub display: String,
    pub type_oid: u32,
    pub is_enum: bool,
    /// The collation's name, when the column has one that is
    /// nondeterministic.
    pub nondeterministic_collation: Option<String>,
}

/// Every live column of `table` (an unquoted `schema.table`) with its type,
/// and the table's row-identity key ([`super::ddl::identity_key_columns`]).
/// Both empty for a table that doesn't exist.
pub(crate) async fn live_columns(
    client: &impl GenericClient,
    table: &str,
) -> Result<(HashMap<String, LiveColumn>, Vec<String>), tokio_postgres::Error> {
    let rows = client
        .query(
            "select a.attname::text, n.nspname::text || '.' || t.typname::text, a.atttypmod, \
                    pg_catalog.format_type(a.atttypid, a.atttypmod), a.atttypid, \
                    t.typtype = 'e', \
                    case when not coalesce(c.collisdeterministic, true) \
                         then c.collname::text end \
             from pg_catalog.pg_attribute a \
             join pg_catalog.pg_type t on t.oid = a.atttypid \
             join pg_catalog.pg_namespace n on n.oid = t.typnamespace \
             left join pg_catalog.pg_collation c on c.oid = a.attcollation \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 and not a.attisdropped",
            &[&super::ddl::regclass_arg(table)],
        )
        .await?;
    let columns: HashMap<String, LiveColumn> = rows
        .into_iter()
        .map(|row| {
            (
                row.get(0),
                LiveColumn {
                    ty: ColumnType {
                        type_name: row.get(1),
                        typmod: row.get(2),
                    },
                    display: row.get(3),
                    type_oid: row.get(4),
                    is_enum: row.get(5),
                    nondeterministic_collation: row.get(6),
                },
            )
        })
        .collect();
    if columns.is_empty() {
        return Ok((columns, Vec::new()));
    }
    let key = super::ddl::identity_key_columns(client, table)
        .await?
        .into_iter()
        .map(|c| c.name)
        .collect();
    Ok((columns, key))
}

/// Why define would refuse `column`, with `live`'s type, for `key_use`, or
/// `None` if it wouldn't. The sentence names the column, its use and its new
/// type or collation.
pub(crate) async fn refusal(
    client: &impl GenericClient,
    table: &str,
    column: &str,
    key_use: &KeyUse,
    live: &LiveColumn,
) -> Result<Option<String>, tokio_postgres::Error> {
    let supported = match key_use {
        KeyUse::GroupBy => {
            let value_type = super::pg_type::value_type_for_oid(client, live.type_oid).await?;
            super::validate::reject_unsupported_group_by_key_type(column, value_type).is_ok()
        }
        _ => super::catalog::is_text_stable_join_key_type(&live.display) || live.is_enum,
    };
    if !supported {
        return Ok(Some(format!(
            "column {column:?} of {table}, {key_use}, is now {}, a type Trellis refuses as such a \
             key at define: it can't match that type's values by their text",
            live.display
        )));
    }
    Ok(live.nondeterministic_collation.as_ref().map(|collation| {
        format!(
            "column {column:?} of {table}, {key_use}, now has the nondeterministic collation \
             {collation:?}, which Trellis refuses for a key at define: its `=` matches strings \
             that differ (e.g. by case), while Trellis matches keys by their exact text"
        )
    }))
}

/// The sentence for `column` of `table`, used as `key_use`, having changed
/// from type `was` (a `format_type` rendering) to `new`'s, which
/// [`renders_differently`].
pub(crate) fn retyped_error(
    table: &str,
    column: &str,
    key_use: &KeyUse,
    was: &str,
    new: &LiveColumn,
) -> String {
    format!(
        "column {column:?} of {table}, {key_use}, changed type from {was} to {}, which renders \
         its existing values differently, so the keys Trellis stored for this definition no \
         longer match",
        new.display
    )
}

/// Records, for definition `id`, the live type of each `(table, column)` in
/// `columns` that has none recorded yet (`definition_key_types`). A column
/// that doesn't exist is skipped.
pub(crate) async fn record(
    client: &impl GenericClient,
    id: i64,
    columns: &[(String, String)],
) -> Result<(), tokio_postgres::Error> {
    upsert(client, id, columns, "nothing").await
}

/// [`record`], replacing the type recorded for each column: for a key
/// column whose type changed without re-rendering the keys stored, so the
/// next change is measured from the type keys are stored under now.
pub(crate) async fn rerecord(
    client: &impl GenericClient,
    id: i64,
    columns: &[(String, String)],
) -> Result<(), tokio_postgres::Error> {
    upsert(
        client,
        id,
        columns,
        "update set type_name = excluded.type_name, typmod = excluded.typmod",
    )
    .await
}

/// [`record`] and [`rerecord`]: `on conflict do {on_conflict}`.
async fn upsert(
    client: &impl GenericClient,
    id: i64,
    columns: &[(String, String)],
    on_conflict: &str,
) -> Result<(), tokio_postgres::Error> {
    if columns.is_empty() {
        return Ok(());
    }
    let tables: Vec<&str> = columns.iter().map(|(t, _)| t.as_str()).collect();
    let regclasses: Vec<String> = columns
        .iter()
        .map(|(t, _)| super::ddl::regclass_arg(t))
        .collect();
    let names: Vec<&str> = columns.iter().map(|(_, c)| c.as_str()).collect();
    client
        .execute(
            &format!(
                "insert into definition_key_types \
                 (transform_id, table_name, column_name, type_name, typmod) \
             select $1, k.t, k.c, n.nspname::text || '.' || ty.typname::text, a.atttypmod \
             from unnest($2::text[], $3::text[], $4::text[]) as k(t, r, c) \
             join pg_catalog.pg_attribute a \
               on a.attrelid = pg_catalog.to_regclass(k.r) and a.attname = k.c \
              and a.attnum > 0 and not a.attisdropped \
             join pg_catalog.pg_type ty on ty.oid = a.atttypid \
             join pg_catalog.pg_namespace n on n.oid = ty.typnamespace \
             on conflict (transform_id, table_name, column_name) do {on_conflict}"
            ),
            &[&id, &tables, &regclasses, &names],
        )
        .await?;
    Ok(())
}

/// Records the live type of every key column definition `id` uses
/// ([`key_uses`]): on its qualified `source`, keyed by the source's
/// row-identity key, and on the to-side of each relationship in `rels`,
/// keyed by that table's. Define calls it with nothing recorded yet; a
/// resume with `replace`, which deletes what was recorded first, so the
/// types the capture pass compares against are the ones the rebuild builds
/// from.
pub(crate) async fn record_definition(
    client: &impl GenericClient,
    id: i64,
    def: &TransformDef,
    source: &str,
    rels: &[RelRef<'_>],
    replace: bool,
) -> Result<(), tokio_postgres::Error> {
    let mut tables: Vec<&str> = vec![source];
    for rel in rels {
        if !tables.contains(&rel.to_table) {
            tables.push(rel.to_table);
        }
    }
    let mut columns: Vec<(String, String)> = Vec::new();
    for table in tables {
        let key: Vec<String> = super::ddl::identity_key_columns(client, table)
            .await?
            .into_iter()
            .map(|c| c.name)
            .collect();
        for (column, _) in key_uses(def, source, rels, table, &key) {
            let entry = (table.to_string(), column);
            if !columns.contains(&entry) {
                columns.push(entry);
            }
        }
    }
    if replace {
        client
            .execute(
                "delete from definition_key_types where transform_id = $1",
                &[&id],
            )
            .await?;
    }
    record(client, id, &columns).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ty(type_name: &str, typmod: i32) -> ColumnType {
        let type_name = if type_name.contains('.') {
            type_name.to_string()
        } else {
            format!("pg_catalog.{type_name}")
        };
        ColumnType { type_name, typmod }
    }

    /// `varchar(n)`'s `atttypmod` is `n + 4`.
    fn varchar(n: i32) -> ColumnType {
        ty("varchar", n + 4)
    }

    #[test]
    fn routine_changes_render_the_same() {
        for (old, new) in [
            (ty("int4", -1), ty("int8", -1)),
            (ty("int2", -1), ty("int4", -1)),
            (ty("int8", -1), ty("int4", -1)),
            (varchar(50), varchar(100)),
            (varchar(50), ty("text", -1)),
            (ty("varchar", -1), ty("text", -1)),
            (ty("text", -1), ty("varchar", -1)),
            // numeric(10,2) -> numeric(12,2), -> numeric(10,3), -> numeric.
            (ty("numeric", 655366), ty("numeric", 786438)),
            (ty("numeric", 655366), ty("numeric", 655367)),
            (ty("numeric", 655366), ty("numeric", -1)),
            (ty("numeric", -1), ty("numeric", -1)),
            // numeric(10,-2) -> numeric(10,0).
            (ty("numeric", 657410), ty("numeric", 655364)),
            (ty("varbit", 8), ty("varbit", 16)),
            // A wider precision keeps the values.
            (ty("timestamp", 3), ty("timestamp", 6)),
            (ty("timestamp", 3), ty("timestamp", -1)),
            (ty("timetz", 0), ty("timetz", 6)),
            (ty("timestamp", 6), ty("timestamp", -1)),
        ] {
            assert!(!renders_differently(&old, &new), "{old:?} -> {new:?}");
        }
    }

    #[test]
    fn changes_that_re_render_existing_values_are_caught() {
        for (old, new) in [
            (ty("timestamp", -1), ty("timestamptz", -1)),
            (ty("timestamptz", -1), ty("timestamp", -1)),
            (ty("date", -1), ty("timestamp", -1)),
            (ty("time", -1), ty("timetz", -1)),
            (ty("text", -1), ty("uuid", -1)),
            (ty("uuid", -1), ty("text", -1)),
            (ty("text", -1), ty("int4", -1)),
            (ty("int4", -1), ty("text", -1)),
            (ty("int4", -1), ty("numeric", -1)),
            // A narrower or new scale rounds the stored values: numeric(10,3)
            // -> numeric(10,2), numeric -> numeric(12,2), numeric(10,0) ->
            // numeric(10,-2).
            (ty("numeric", 655367), ty("numeric", 655366)),
            (ty("numeric", -1), ty("numeric", 786438)),
            (ty("numeric", 655364), ty("numeric", 657410)),
            (ty("int4", -1), ty("oid", -1)),
            (ty("macaddr", -1), ty("macaddr8", -1)),
            (varchar(5), ty("bpchar", 9)),
            (ty("bpchar", 9), ty("bpchar", 12)),
            (ty("bit", 4), ty("bit", 8)),
            (ty("public.mood", -1), ty("text", -1)),
            (ty("text", -1), ty("public.mood", -1)),
            (ty("public.mood", -1), ty("public.feeling", -1)),
            // A narrower precision rounds the stored values.
            (ty("timestamp", -1), ty("timestamp", 3)),
            (ty("timestamp", 6), ty("timestamp", 0)),
            (ty("timestamptz", 3), ty("timestamptz", 0)),
            (ty("time", 6), ty("time", 2)),
            // A narrower string bound strips trailing spaces past it.
            (varchar(100), varchar(50)),
            (ty("text", -1), varchar(255)),
            (ty("varchar", -1), varchar(255)),
        ] {
            assert!(renders_differently(&old, &new), "{old:?} -> {new:?}");
        }
    }

    /// A user type named like a builtin family is not in it.
    #[test]
    fn a_user_type_is_never_in_a_builtin_family() {
        assert!(renders_differently(
            &ty("public.int4", -1),
            &ty("pg_catalog.int8", -1)
        ));
        assert!(renders_differently(
            &ty("public.text", -1),
            &ty("pg_catalog.text", -1)
        ));
        assert!(!widens(&ty("public.int4", -1), &ty("pg_catalog.int8", -1)));
    }

    /// `numeric(p,s)`'s `atttypmod`.
    fn numeric(precision: i32, scale: i32) -> ColumnType {
        ty("numeric", ((precision << 16) | (scale & 0x7ff)) + 4)
    }

    /// `interval`'s `atttypmod` for a field range and a precision.
    fn interval(range: i32, precision: i32) -> ColumnType {
        ty("interval", (range << 16) | precision)
    }

    #[test]
    fn a_copy_is_widened_only_by_a_wider_type_of_its_family() {
        for (copy, live) in [
            (ty("int2", -1), ty("int4", -1)),
            (ty("int4", -1), ty("int8", -1)),
            (ty("int2", -1), ty("int8", -1)),
            (ty("float4", -1), ty("float8", -1)),
            (ty("int4", -1), ty("numeric", -1)),
            (ty("int8", -1), ty("numeric", -1)),
            (ty("int8", -1), numeric(19, 0)),
            (ty("int4", -1), numeric(10, 0)),
            (ty("int4", -1), numeric(5, 2)),
            (ty("int2", -1), numeric(5, 0)),
            (ty("int2", -1), numeric(2, -3)),
            (varchar(50), varchar(100)),
            (varchar(50), ty("text", -1)),
            (varchar(50), ty("varchar", -1)),
            (numeric(10, 2), numeric(12, 2)),
            (numeric(10, 2), numeric(10, 3)),
            (numeric(10, 2), ty("numeric", -1)),
            (ty("timestamp", 3), ty("timestamp", 6)),
            (ty("timestamp", 3), ty("timestamp", -1)),
            (ty("timestamptz", 0), ty("timestamptz", 3)),
            (ty("time", 2), ty("time", -1)),
            (ty("timetz", 0), ty("timetz", 6)),
            (ty("varbit", 8), ty("varbit", 16)),
            (ty("varbit", 8), ty("varbit", -1)),
            // `interval second(3)` -> `interval second(6)`, and `interval
            // day` -> `interval`.
            (interval(0x1000, 3), interval(0x1000, 6)),
            (interval(0x0008, 0xffff), ty("interval", -1)),
        ] {
            assert!(widens(&copy, &live), "{copy:?} -> {live:?}");
        }
        for (copy, live) in [
            (ty("int4", -1), ty("int4", -1)),
            (ty("int8", -1), ty("int4", -1)),
            (varchar(100), varchar(50)),
            (ty("text", -1), varchar(50)),
            (ty("varchar", -1), ty("text", -1)),
            (ty("text", -1), ty("varchar", -1)),
            (numeric(12, 2), numeric(10, 2)),
            (ty("numeric", -1), numeric(10, 2)),
            (ty("timestamp", 6), ty("timestamp", 3)),
            (ty("timestamp", 6), ty("timestamp", -1)),
            (ty("timestamp", -1), ty("timestamptz", -1)),
            (ty("int4", -1), numeric(9, 0)),
            (ty("int8", -1), numeric(18, 0)),
            (ty("int2", -1), numeric(4, 0)),
            (ty("int4", -1), numeric(3, -2)),
            (ty("numeric", -1), ty("int8", -1)),
            (ty("int4", -1), ty("float8", -1)),
            (ty("public.int4", -1), ty("pg_catalog.numeric", -1)),
            (ty("float8", -1), ty("float4", -1)),
            (ty("public.float4", -1), ty("pg_catalog.float8", -1)),
            (ty("text", -1), ty("uuid", -1)),
            (ty("bpchar", 9), ty("bpchar", 12)),
            (ty("varbit", 16), ty("varbit", 8)),
            (ty("interval", -1), interval(0x0008, 0xffff)),
        ] {
            assert!(!widens(&copy, &live), "{copy:?} -> {live:?}");
        }
    }

    fn rel<'a>(name: &'a str, from_col: &'a str, to_col: &'a str, to: &'a str) -> RelRef<'a> {
        RelRef {
            name,
            from_col,
            to_col,
            to_table: to,
        }
    }

    #[test]
    fn a_one_to_one_definition_uses_its_source_key_and_join_columns() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM public.posts SELECT author.name AS author_name, title AS title",
        )
        .expect("parse");
        let rels = [
            rel("author", "author_id", "id", "public.users"),
            rel("unread", "editor_id", "id", "public.users"),
        ];
        let key = vec!["id".to_string()];
        assert_eq!(
            key_uses(&def, "public.posts", &rels, "public.posts", &key),
            vec![
                ("id".to_string(), KeyUse::SourceKey),
                (
                    "author_id".to_string(),
                    KeyUse::JoinColumn {
                        rel: "author".to_string(),
                    }
                ),
            ]
        );
        assert_eq!(
            key_uses(&def, "public.posts", &rels, "public.users", &key),
            vec![
                (
                    "id".to_string(),
                    KeyUse::JoinColumn {
                        rel: "author".to_string(),
                    }
                ),
                (
                    "id".to_string(),
                    KeyUse::EndpointKey {
                        rel: "author".to_string()
                    }
                ),
            ]
        );
        assert!(key_uses(&def, "public.posts", &rels, "public.other", &key).is_empty());
    }

    #[test]
    fn an_aggregate_uses_its_group_by_keys_on_either_side() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM public.posts GROUP BY kind, author.country SELECT COUNT(*) AS n",
        )
        .expect("parse");
        let rels = [rel("author", "author_id", "id", "public.users")];
        let key = vec!["id".to_string()];
        let on_source = key_uses(&def, "public.posts", &rels, "public.posts", &key);
        assert!(on_source.contains(&("id".to_string(), KeyUse::SourceKey)));
        assert!(on_source.contains(&("kind".to_string(), KeyUse::GroupBy)));
        let on_to_side = key_uses(&def, "public.posts", &rels, "public.users", &key);
        assert!(on_to_side.contains(&("country".to_string(), KeyUse::GroupBy)));
        assert!(on_to_side.contains(&(
            "id".to_string(),
            KeyUse::EndpointKey {
                rel: "author".to_string()
            }
        )));
    }
    /// The four catalog-only widenings #824 names, and none of their
    /// neighbours: each of those either rewrites the table or changes a
    /// value, or narrows.
    #[test]
    fn only_the_named_widenings_are_catalog_only() {
        for (copy, live) in [
            (varchar(10), varchar(40)),
            (varchar(10), ty("text", -1)),
            (ty("varchar", -1), ty("text", -1)),
            (varchar(10), ty("varchar", -1)),
            (numeric(10, 2), numeric(12, 2)),
            (numeric(5, 0), numeric(18, 0)),
        ] {
            assert!(catalog_only(&copy, &live), "{copy:?} -> {live:?}");
        }
        for (copy, live) in [
            (varchar(10), varchar(10)),
            (varchar(40), varchar(10)),
            // Binary-coercible in `pg_cast`, but it narrows.
            (ty("text", -1), varchar(10)),
            (ty("text", -1), ty("varchar", -1)),
            (ty("varchar", -1), varchar(10)),
            // Blank-padded: a length change changes the values.
            (ty("bpchar", 5), ty("bpchar", 9)),
            (ty("bpchar", 5), ty("text", -1)),
            (varchar(10), ty("bpchar", 14)),
            // A scale change, an unbounded `numeric`, a narrower precision.
            (numeric(10, 2), numeric(12, 3)),
            (numeric(10, 2), numeric(10, 3)),
            (numeric(10, 2), ty("numeric", -1)),
            (numeric(12, 2), numeric(10, 2)),
            (ty("numeric", -1), numeric(12, 2)),
            // Rewrites.
            (ty("int4", -1), ty("int8", -1)),
            (ty("float4", -1), ty("float8", -1)),
            (ty("int4", -1), ty("numeric", -1)),
            // Outside the named set, though Postgres changes only the
            // catalog for them too.
            (ty("timestamp", 3), ty("timestamp", 6)),
            (ty("varbit", 8), ty("varbit", 16)),
            // Not builtin.
            (ty("public.varchar", 14), ty("public.varchar", 44)),
        ] {
            assert!(!catalog_only(&copy, &live), "{copy:?} -> {live:?}");
        }
    }
}
