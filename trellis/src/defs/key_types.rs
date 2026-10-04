//! Re-validating a definition's key columns after define (issue #760).
//!
//! Define refuses a key column whose type or collation Trellis can't match
//! by: a type off the key allowlist ([`super::catalog::is_text_stable_join_key_type`],
//! [`super::ddl::source_primary_key`]), a `GROUP BY` type
//! [`super::validate::reject_unsupported_group_by_key_type`] refuses, or a
//! nondeterministic collation (#590, #638). Nothing stopped an
//! `ALTER COLUMN ... TYPE` or `... COLLATE` after define from giving a key
//! column such a type, and a key type off the allowlist halts the whole
//! instance on the next drain that reads it (`quarantine::classify`).
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
//!    definition was accepted (`definition_key_types`, [`record`]). A table
//!    rewrite fires no capture trigger, so the target, ledger and projection
//!    rows built from the old rendering would never match the new one: a
//!    `timestamp` key becomes `timestamptz` (`'… 10:00:00'` reads back as
//!    `'… 15:00:00+00'` after an `ALTER` in a New York session), `date`
//!    becomes `timestamp`, `text` becomes `uuid` or `integer` (`'007'` reads
//!    back as `7`), `varchar` becomes `character(n)` (padded).
//!
//! Routine changes don't fire: widening an integer key (`integer` to
//! `bigint`), any `varchar(n)`/`text` change, a `numeric` precision change
//! or wider scale on a `GROUP BY` key, a change between deterministic
//! collations. None of them changes an existing value's identity. A
//! narrower `numeric` scale does fire: it rounds the stored values. The issue's PR body
//! has the whole matrix and the evidence for each row.
//!
//! What isn't checked, and why:
//!
//! - **That a relationship's two join columns still have the same type,
//!   modifier and collation** (#590, `catalog::assert_joinable_as_is`).
//!   Widening both sides of a join takes two `ALTER`s, and between them the
//!   pair differs. Every key lookup casts the other side's key text to the
//!   looked-up column's own live type (`staging::apply::key_array_filter`),
//!   so the lookups keep working while the values fit both types.
//! - **A narrower column Trellis created keeping its old type.** A 1-1
//!   target's key, an aggregate's group key and a projection's key keep the
//!   type they had at define, so after `integer` becomes `bigint` a key
//!   above 2^31 fails its write (loudly, as a poisoned key) rather than
//!   being stored wrong. Re-typing those columns is a separate feature.

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use tokio_postgres::GenericClient;

use super::ast::{GroupByKey, KeySpace, TransformDef};

/// How a definition uses one of its key columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyUse {
    /// Part of the definition's source's row-identity key. `mirrored` when
    /// the definition is 1-1, so its target's key is a copy of the column.
    SourceKey { mirrored: bool },
    /// A `GROUP BY` key: a source column, or a to-side column read through
    /// a relationship.
    GroupBy,
    /// A join column of relationship `rel`. `mirrored` for a to-one
    /// relationship's to-side column, which its settled projection's key
    /// copies.
    JoinColumn { rel: String, mirrored: bool },
    /// Part of relationship `rel`'s to-side's row-identity key.
    EndpointKey { rel: String },
}

impl KeyUse {
    /// Whether a column Trellis created holds a typed copy of this one, so a
    /// change of precision the copy can't hold rounds a key silently.
    pub(crate) fn mirrored(&self) -> bool {
        match self {
            KeyUse::SourceKey { mirrored } | KeyUse::JoinColumn { mirrored, .. } => *mirrored,
            KeyUse::GroupBy | KeyUse::EndpointKey { .. } => false,
        }
    }
}

impl fmt::Display for KeyUse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyUse::SourceKey { .. } => write!(f, "part of this definition's source key"),
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
    pub to_one: bool,
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
        let mirrored = group_by.is_empty();
        uses.extend(
            table_key
                .iter()
                .map(|c| (c.clone(), KeyUse::SourceKey { mirrored })),
        );
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
                    mirrored: false,
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
                mirrored: rel.to_one,
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
}

/// Whether a key column that had type `old` when Trellis built its state
/// would render that state's values differently now that it has `new`
/// (see the module doc). `mirrored` is [`KeyUse::mirrored`].
///
/// - **Integers** (`smallint`, `integer`, `bigint`) and **strings**
///   (`text`, `varchar(n)`) render every value the same within their family.
///   A narrowing `ALTER` fails on a value that doesn't fit, rather than
///   changing it (except that `varchar(n)` drops trailing spaces past `n`).
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
///   narrows. When it widens, the values stay, but a mirrored copy keeps the
///   old precision and would round a new key into another's.
/// - **Anything else** (another type, `character(n)`'s length, `bit(n)`'s)
///   renders differently.
pub(crate) fn renders_differently(old: &ColumnType, new: &ColumnType, mirrored: bool) -> bool {
    if old == new {
        return false;
    }
    let family = |t: &ColumnType| match t.base() {
        "int2" | "int4" | "int8" if t.type_name.starts_with("pg_catalog.") => Some(0),
        "text" | "varchar" if t.type_name.starts_with("pg_catalog.") => Some(1),
        _ => None,
    };
    if family(old).is_some() && family(old) == family(new) {
        return false;
    }
    if old.type_name != new.type_name {
        return true;
    }
    if !old.type_name.starts_with("pg_catalog.") {
        return true;
    }
    match old.base() {
        "numeric" => numeric_rounds(old.typmod, new.typmod),
        "varbit" => false,
        "time" | "timetz" | "timestamp" | "timestamptz" => {
            // `-1` is the default precision, 6.
            let precision = |typmod: i32| if typmod < 0 { 6 } else { typmod };
            let (was, is) = (precision(old.typmod), precision(new.typmod));
            is < was || (is != was && mirrored)
        }
        _ => true,
    }
}

/// Whether re-typing a `numeric` column from modifier `old` to `new` rounds
/// a value it holds: when `new` has a scale and `old` had none, or a larger
/// one. An unconstrained `numeric` (`-1`) keeps every value.
///
/// A `numeric(p,s)` modifier is `((p << 16) | (s & 0x7ff)) + 4`, with `s` an
/// 11-bit two's complement, since Postgres 15 allows a negative scale.
fn numeric_rounds(old: i32, new: i32) -> bool {
    let scale = |typmod: i32| {
        (typmod >= 4).then(|| {
            let s = (typmod - 4) & 0x7ff;
            if s & 0x400 != 0 { s - 0x800 } else { s }
        })
    };
    match (scale(old), scale(new)) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(was), Some(is)) => is < was,
    }
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
            "insert into definition_key_types \
                 (transform_id, table_name, column_name, type_name, typmod) \
             select $1, k.t, k.c, n.nspname::text || '.' || ty.typname::text, a.atttypmod \
             from unnest($2::text[], $3::text[], $4::text[]) as k(t, r, c) \
             join pg_catalog.pg_attribute a \
               on a.attrelid = pg_catalog.to_regclass(k.r) and a.attname = k.c \
              and a.attnum > 0 and not a.attisdropped \
             join pg_catalog.pg_type ty on ty.oid = a.atttypid \
             join pg_catalog.pg_namespace n on n.oid = ty.typnamespace \
             on conflict do nothing",
            &[&id, &tables, &regclasses, &names],
        )
        .await?;
    Ok(())
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
            (varchar(100), varchar(50)),
            (varchar(50), ty("text", -1)),
            (ty("text", -1), varchar(255)),
            (ty("varchar", -1), ty("text", -1)),
            // numeric(10,2) -> numeric(12,2), -> numeric(10,3), -> numeric.
            (ty("numeric", 655366), ty("numeric", 786438)),
            (ty("numeric", 655366), ty("numeric", 655367)),
            (ty("numeric", 655366), ty("numeric", -1)),
            (ty("numeric", -1), ty("numeric", -1)),
            // numeric(10,-2) -> numeric(10,0).
            (ty("numeric", 657410), ty("numeric", 655364)),
            (ty("varbit", 8), ty("varbit", 16)),
        ] {
            for mirrored in [false, true] {
                assert!(
                    !renders_differently(&old, &new, mirrored),
                    "{old:?} -> {new:?} (mirrored: {mirrored})"
                );
            }
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
        ] {
            for mirrored in [false, true] {
                assert!(
                    renders_differently(&old, &new, mirrored),
                    "{old:?} -> {new:?} (mirrored: {mirrored})"
                );
            }
        }
    }

    /// A user type named like a builtin family is not in it.
    #[test]
    fn a_user_type_is_never_in_a_builtin_family() {
        assert!(renders_differently(
            &ty("public.int4", -1),
            &ty("pg_catalog.int8", -1),
            false
        ));
        assert!(renders_differently(
            &ty("public.text", -1),
            &ty("pg_catalog.text", -1),
            false
        ));
    }

    /// A wider precision keeps the values, but a mirrored copy keeps the
    /// old precision and would round a new key.
    #[test]
    fn a_wider_precision_is_caught_only_where_a_copy_would_round() {
        for (old, new) in [
            (ty("timestamp", 3), ty("timestamp", 6)),
            (ty("timestamp", 3), ty("timestamp", -1)),
            (ty("timetz", 0), ty("timetz", 6)),
        ] {
            assert!(
                !renders_differently(&old, &new, false),
                "{old:?} -> {new:?}"
            );
            assert!(renders_differently(&old, &new, true), "{old:?} -> {new:?}");
        }
        assert!(!renders_differently(
            &ty("timestamp", 6),
            &ty("timestamp", -1),
            true
        ));
    }

    fn rel<'a>(name: &'a str, from_col: &'a str, to_col: &'a str, to: &'a str) -> RelRef<'a> {
        RelRef {
            name,
            from_col,
            to_col,
            to_table: to,
            to_one: true,
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
                ("id".to_string(), KeyUse::SourceKey { mirrored: true }),
                (
                    "author_id".to_string(),
                    KeyUse::JoinColumn {
                        rel: "author".to_string(),
                        mirrored: false
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
                        mirrored: true
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
        assert!(on_source.contains(&("id".to_string(), KeyUse::SourceKey { mirrored: false })));
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
}
