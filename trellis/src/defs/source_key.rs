//! The key a definition keys each table it reads by (ADR-0026, #1745).
//!
//! A user table is keyed by its primary key, unless the definition names
//! another key with `KEY (<column>, ...)` directly after the table. With
//! neither, define refuses ([`DdlError::NoPrimaryKey`]): Trellis never keys a
//! user table by a unique index it picked itself. A `KEY` must be backed by a
//! unique index over exactly its columns ([`backing`]), matched by shape,
//! never by an index's name or oid, so the user may replace that index with
//! an equivalent one at any time. Another transform's target is keyed by the
//! key Trellis gave it, and a `KEY` on one is refused
//! ([`DdlError::KeyOnTrellisTarget`]). A table is keyed one way for every
//! transform that reads it ([`check_one_key`]).
//!
//! Define and `RESUME` both resolve the key here ([`admitted_key`], through
//! `catalog::validate_live_schema`), so a `RESUME` re-validates what define
//! validated against the table as it is now. From define on, the key is the
//! table's record (`definition_keys`, [`super::recorded_keys`]), which every
//! other read of the table's identity keeps to.

use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use super::ast::TransformDef;
use super::ddl::{self, DdlError, PrimaryKeyColumn};
use super::join::Side;

/// The key `def` declares with `KEY (...)` for the table it reads on
/// `side` (`None` for a definition that isn't a `JOIN`, whose only table is
/// its source).
pub(crate) fn declared_on(def: &TransformDef, side: Option<Side>) -> Option<&[String]> {
    match side {
        None | Some(Side::From) => def.source_key.as_deref(),
        Some(Side::Joined) => super::join::spec(def).and_then(|join| join.right_key.as_deref()),
    }
}

/// The key `def` declares for `table` (a qualified or bare name), which it
/// reads as its source or as a `JOIN`'s side, matched on the bare name as
/// [`super::join::side_of_table`] does.
pub(crate) fn declared_for<'a>(def: &'a TransformDef, table: &str) -> Option<&'a [String]> {
    match super::join::spec(def) {
        Some(_) => {
            super::join::side_of_table(def, table).and_then(|side| declared_on(def, Some(side)))
        }
        None => declared_on(def, None),
    }
}

/// The key a definition reading `table` as its stored `source` or `joined`
/// table (qualified names) declares for it; `None` when it declares none,
/// or doesn't read `table`.
pub(crate) fn declared_by_reader<'a>(
    def: &'a TransformDef,
    source: &str,
    joined: Option<&str>,
    table: &str,
) -> Option<&'a [String]> {
    if source == table {
        declared_on(def, super::join::spec(def).map(|_| Side::From))
    } else if joined == Some(table) {
        declared_on(def, Some(Side::Joined))
    } else {
        None
    }
}

/// The unique index backing `columns` as `table`'s key (an unquoted
/// `schema.table`), as each key column reads through it, in `columns`'
/// order: its name, type, nullability and the index's collation for it.
/// `None` when no index backs it.
///
/// An index backs a key when it is a unique index whose key columns are
/// exactly `columns`, in any order (an `INCLUDE` column is no key column),
/// and it is valid, immediate, not partial and over no expression, and every
/// key column is `NOT NULL` or the index is `NULLS NOT DISTINCT`. Any such
/// index does: the match is by shape, never by the index's name or oid, so
/// one built to replace it serves as well. One statement, so one snapshot.
pub(crate) async fn backing(
    client: &impl GenericClient,
    table: &str,
    columns: &[String],
) -> Result<Option<Vec<PrimaryKeyColumn>>, tokio_postgres::Error> {
    let rows = client
        .query_typed(
            &format!(
                "with backing as ( \
                     select i.indexrelid, i.indrelid, i.indkey, i.indnkeyatts, i.indcollation \
                     from pg_catalog.pg_index i \
                     where i.indrelid = pg_catalog.to_regclass($1) \
                       and i.indisunique and i.indisvalid and i.indisready and i.indislive \
                       and i.indimmediate and i.indpred is null and i.indexprs is null \
                       and i.indnkeyatts = pg_catalog.cardinality($2::text[]) \
                       and (select pg_catalog.array_agg(a.attname::text order by a.attname::text) \
                            from pg_catalog.unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                            join pg_catalog.pg_attribute a \
                              on a.attrelid = i.indrelid and a.attnum = k.attnum \
                            where k.ord <= i.indnkeyatts) \
                           = (select pg_catalog.array_agg(c order by c) \
                              from pg_catalog.unnest($2::text[]) c) \
                       and (i.indnullsnotdistinct or not exists ( \
                              select 1 \
                              from pg_catalog.unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                              join pg_catalog.pg_attribute a \
                                on a.attrelid = i.indrelid and a.attnum = k.attnum \
                              where k.ord <= i.indnkeyatts and not a.attnotnull)) \
                     order by i.indisprimary desc, i.indexrelid \
                     limit 1) \
                 select a.attname::text, {}, not a.attnotnull, \
                        (select pg_catalog.format('%I.%I', n.nspname, co.collname) \
                         from pg_catalog.pg_collation co \
                         join pg_catalog.pg_namespace n on n.oid = co.collnamespace \
                         where co.oid = b.indcollation[k.ord - 1]) \
                 from backing b \
                 cross join pg_catalog.unnest(b.indkey::int2[]) with ordinality k(attnum, ord) \
                 join pg_catalog.pg_attribute a \
                   on a.attrelid = b.indrelid and a.attnum = k.attnum \
                 where k.ord <= b.indnkeyatts \
                 order by pg_catalog.array_position($2::text[], a.attname::text)",
                super::copy_type::type_sql("a.atttypid")
            ),
            &[
                (&ddl::regclass_arg(table), Type::TEXT),
                (&columns, Type::TEXT_ARRAY),
            ],
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        rows.into_iter()
            .map(|row| PrimaryKeyColumn {
                name: row.get(0),
                data_type: row.get(1),
                nullable: row.get(2),
                collation: row.get(3),
            })
            .collect(),
    ))
}

/// Whether `table` (an unquoted `schema.table`) has a primary key.
async fn has_primary_key(
    client: &impl GenericClient,
    table: &str,
) -> Result<bool, tokio_postgres::Error> {
    Ok(client
        .query_one(
            "select exists (select 1 from pg_catalog.pg_index i \
                 where i.indrelid = pg_catalog.to_regclass($1) and i.indisprimary)",
            &[&ddl::regclass_arg(table)],
        )
        .await?
        .get(0))
}

/// Whether `table` (an unquoted `schema.table`) is a definition's target.
pub(crate) async fn is_trellis_target(
    client: &impl GenericClient,
    table: &str,
) -> Result<bool, tokio_postgres::Error> {
    Ok(client
        .query_one(
            "select exists (select 1 from transform_definitions where target_table = $1)",
            &[&table],
        )
        .await?
        .get(0))
}

/// Whether `table` (an unquoted `schema.table`) is a partitioned table.
async fn is_partitioned(
    client: &impl GenericClient,
    table: &str,
) -> Result<bool, tokio_postgres::Error> {
    Ok(client
        .query_one(
            "select exists (select 1 from pg_catalog.pg_class \
                 where oid = pg_catalog.to_regclass($1) and relkind = 'p')",
            &[&ddl::regclass_arg(table)],
        )
        .await?
        .get(0))
}

/// The key a definition keys `table` (an unquoted `schema.table`) by, given
/// the key it declares for it, and for each key column the allowlist entry
/// that admitted its type ([`ddl::admitted_primary_key`]'s second value).
///
/// - Another transform's target: the key Trellis gave it
///   ([`ddl::admitted_primary_key`]); a declared key is refused.
/// - A declared key: the columns as declared, read through the index that
///   backs them ([`backing`]); refused when none does.
/// - Otherwise the table's primary key, which must exist: no unique index
///   stands in for one.
pub(crate) async fn admitted_key(
    client: &impl GenericClient,
    table: &str,
    declared: Option<&[String]>,
) -> Result<(Vec<PrimaryKeyColumn>, Vec<super::join_key_types::JoinKey>), DdlError> {
    if is_trellis_target(client, table).await? {
        if let Some(key) = declared {
            return Err(DdlError::KeyOnTrellisTarget {
                table: table.to_string(),
                key: key.to_vec(),
            });
        }
        return ddl::admitted_primary_key(client, table).await;
    }
    if let Some(key) = declared {
        let Some(columns) = backing(client, table, key).await? else {
            return Err(DdlError::KeyNotBacked {
                table: table.to_string(),
                key: key.to_vec(),
                indexes: super::recorded_keys::unique_indexes(client, table).await?,
                partitioned: is_partitioned(client, table).await?,
            });
        };
        let admitted_by = ddl::admit_key_types(client, table, &columns).await?;
        return Ok((columns, admitted_by));
    }
    if !has_primary_key(client, table).await? {
        // A table with a record and no index over it reports that first:
        // its key was dropped after define.
        if let Some(missing) = super::recorded_keys::missing(client, table).await? {
            return Err(missing);
        }
        return Err(DdlError::NoPrimaryKey {
            source_table: table.to_string(),
        });
    }
    ddl::admitted_primary_key(client, table).await
}

/// How the transforms reading a table key it: by its primary key, or by a
/// declared `KEY (...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyForm {
    /// No `KEY`: the table's primary key.
    PrimaryKey,
    /// `KEY (...)` over these columns, in this order.
    Declared(Vec<String>),
}

impl std::fmt::Display for KeyForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyForm::PrimaryKey => write!(f, "its primary key, with no KEY"),
            KeyForm::Declared(columns) => write!(f, "KEY ({})", columns.join(", ")),
        }
    }
}

/// The key forms another definition already uses for `table` (an unquoted
/// `schema.table`), each with that definition's target, in id order. A
/// definition whose target is `this` (its own resume) doesn't count, nor one
/// whose text this binary can't read.
async fn forms_of_readers(
    client: &impl GenericClient,
    table: &str,
    this: Option<&str>,
) -> Result<Vec<(String, KeyForm)>, tokio_postgres::Error> {
    let rows = client
        .query(
            "select target_table, source_table, join_table, definition_text, grammar_version \
             from transform_definitions \
             where (source_table = $1 or join_table = $1) and target_table is distinct from $2 \
             order by id",
            &[&table, &this],
        )
        .await?;
    let mut forms = Vec::with_capacity(rows.len());
    for row in rows {
        let (target, source, joined, text, grammar): (String, String, Option<String>, String, i32) =
            (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
        let Ok(def) = super::grammar::load(&text, grammar) else {
            continue;
        };
        let form = match declared_by_reader(&def, &source, joined.as_deref(), table) {
            Some(key) => KeyForm::Declared(key.to_vec()),
            None => KeyForm::PrimaryKey,
        };
        forms.push((target, form));
    }
    Ok(forms)
}

/// One key per table: refuses `declared` (a definition's `KEY` for `table`,
/// or `None` for its primary key) when the table's other definitions key it
/// another way, naming the key to write ([`DdlError::KeyDiffersFromReaders`]).
/// Capture keys a table one way for all its readers, whether they read it as
/// a `FROM` or a `JOIN` side. `this` is the target of the definition being
/// resumed, which isn't another. A Trellis target is keyed by Trellis, so it
/// has no form to differ from.
pub(crate) async fn check_one_key(
    client: &impl GenericClient,
    table: &str,
    declared: Option<&[String]>,
    this: Option<&str>,
) -> Result<(), DdlError> {
    if is_trellis_target(client, table).await? {
        return Ok(());
    }
    let form = match declared {
        Some(key) => KeyForm::Declared(key.to_vec()),
        None => KeyForm::PrimaryKey,
    };
    for (reader, theirs) in forms_of_readers(client, table, this).await? {
        if theirs != form {
            return Err(DdlError::KeyDiffersFromReaders {
                table: table.to_string(),
                written: form,
                theirs,
                reader,
            });
        }
    }
    Ok(())
}
