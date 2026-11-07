//! The key Trellis keys each table by (`definition_keys`, #1719).
//!
//! A table's row identity ([`super::ddl::identity_key_columns`]) is a choice
//! among its unique indexes. Trellis makes it once per table, the first time
//! a definition keys by the table, and records it: one record per table,
//! whichever definitions read or write it. A target's key comes from its
//! definition (an aggregate's or `JOIN`'s group columns, a 1-1 target's
//! source key), never from its indexes; a source's and a `JOIN`'s joined
//! table's is its row identity when define admits it ([`record`]). From then
//! on the identity is the index over exactly the recorded columns, so a
//! user's unique index or constraint on the same table, older than the key
//! after a `REINDEX CONCURRENTLY` or created first on an adopted or restored
//! table, never becomes the key.
//!
//! # The tie to the relation
//!
//! A record holds the oid of the relation it was taken from and the oid of
//! `definition_keys` itself ([`tied_to`]). A record whose relation oid isn't
//! the one at the name now was taken from a table since dropped: it is no
//! record of the new table, so a define over a table dropped and created
//! again sees the new table's key, whatever definitions of the old one
//! remain. A table keeps its oid through everything else that changes its
//! indexes or its storage: `ALTER TABLE ... ADD PRIMARY KEY`, a `REINDEX
//! CONCURRENTLY`, a rewrite (`ALTER ... TYPE`, `VACUUM FULL`, `CLUSTER`),
//! `pg_upgrade`. A logical restore creates every table again, under new
//! oids, and `definition_keys` with them: a record whose stored
//! `definition_keys` oid isn't the live one came with a restore of the
//! tables at the same names, so it holds by name until it is tied to them
//! again ([`retie_restored`], at attach and on a detected restore). A
//! partition conversion is a drop and a create: the record is the old
//! table's.
//!
//! # A key that changed
//!
//! With no index over the recorded columns (dropped or redefined in place),
//! or a record of a dropped table, a resume decides ([`decide`]): a Trellis
//! target's key comes back only with its index; an aggregate or `JOIN`
//! reading a user table takes the table's primary key now (it re-records it
//! for every reader of the table, and rebuilds), and refuses when the table
//! has none rather than take another unique index; a 1-1 transform, whose
//! target is keyed by its source's key, refuses a different key
//! ([`CatalogError::SourceKeyChanged`]). Anything else that reads the key
//! (a build's start, a drain) refuses by name rather than key the table by a
//! guess ([`check`], [`super::ddl::DdlError::RecordedKeyMissing`]).
//!
//! Definitions stored before the record existed get theirs at attach
//! ([`backfill_in`]), from their parsed text. A definition built over a source
//! key with `INCLUDE` columns was built by an earlier release over the
//! included columns too (#1722): the backfill records the corrected key and
//! pauses it, and its `RESUME` re-keys a 1-1 target in place
//! ([`repair_target_key`]) and rebuilds.

use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use super::ast::{KeySpace, TransformDef};
use super::catalog::CatalogError;
use super::ddl::{self, DdlError};
use super::floor::Floors;
use super::model::Definition;
use crate::pool::quote_ident;

/// The SQL condition a `definition_keys` row `k` meets when it is a record of
/// the relation at the name now, given `regclass` (a parameter or literal
/// holding the table's [`ddl::regclass_arg`]): taken from that relation, or
/// brought with the tables by a logical restore (see the module doc).
pub(crate) fn tied_to(regclass: &str) -> String {
    format!(
        "(k.relation_oid = pg_catalog.to_regclass({regclass})::pg_catalog.oid \
          or k.catalog_oid <> k.tableoid)"
    )
}

/// The key columns `def`'s target is keyed by, in key order, given its
/// source's (a `JOIN`'s `FROM` table's) key columns: an aggregate's or
/// `JOIN`'s `GROUP BY` columns, or a 1-1 target's source key.
pub(crate) fn target_key(def: &TransformDef, source_key: &[String]) -> Vec<String> {
    match &def.key_space {
        KeySpace::Aggregate { group_by } => group_by.clone(),
        KeySpace::Join(join) => join.group_by.iter().map(|k| k.column.clone()).collect(),
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_) => source_key.to_vec(),
    }
}

/// Whether `def`'s target is keyed by its source's (a `JOIN`'s `FROM`
/// table's) key: a 1-1 transform, plain or `JOIN`.
fn keys_target_by_source(def: &TransformDef) -> bool {
    matches!(
        def.key_space,
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_)
    )
}

async fn key_names(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<String>, tokio_postgres::Error> {
    Ok(ddl::identity_key_columns(client, table)
        .await?
        .into_iter()
        .map(|c| c.name)
        .collect())
}

/// `table`'s key: the one recorded for it, else its row identity now. A 1-1
/// target an earlier release keyed by its source key's `INCLUDE` columns has
/// a record and no index over it until its `RESUME` re-keys it.
async fn key_of(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<String>, tokio_postgres::Error> {
    match recorded_for(client, table).await? {
        Some(key) => Ok(key),
        None => key_names(client, table).await,
    }
}

/// Records `key` as `table`'s (an unquoted `schema.table`), tied to the
/// relation at the name now, for every definition that keys by it: a record
/// it had, of this table or of one dropped since, is replaced. Nothing is
/// recorded for a table that doesn't exist or an empty key.
async fn write(
    client: &impl GenericClient,
    table: &str,
    key: &[String],
) -> Result<(), tokio_postgres::Error> {
    if key.is_empty() {
        return Ok(());
    }
    client
        .execute(
            "insert into definition_keys (table_name, key_columns, relation_oid, catalog_oid) \
             select $1, $2, r.oid, 'definition_keys'::pg_catalog.regclass::pg_catalog.oid \
             from (select pg_catalog.to_regclass($3)::pg_catalog.oid as oid) r \
             where r.oid is not null \
             on conflict (table_name) do update \
             set key_columns = excluded.key_columns, relation_oid = excluded.relation_oid, \
                 catalog_oid = excluded.catalog_oid",
            &[&table, &key, &ddl::regclass_arg(table)],
        )
        .await?;
    Ok(())
}

/// Records the key of each table a define of `def` keys by: its qualified
/// `source` and a `JOIN`'s qualified `joined` table (the `KEY (...)` `def`
/// declares for it, else its row-identity key now unless the table has a
/// record already, which it keeps) and its qualified `target`
/// ([`target_key`]), which the define keys and whose record is always its
/// own. A declared key is the table's for every reader of it: define
/// refuses one that differs from another reader's
/// ([`super::source_key::check_one_key`]).
pub(crate) async fn record(
    client: &impl GenericClient,
    def: &TransformDef,
    source: &str,
    joined: Option<&str>,
    target: &str,
) -> Result<(), tokio_postgres::Error> {
    record_except(client, def, source, joined, target, None).await
}

/// [`record`], recording nothing for `unkeyed`: a table an earlier release
/// keyed by a unique index that backs no key now ([`backfill_in`]), which
/// is no key to record.
async fn record_except(
    client: &impl GenericClient,
    def: &TransformDef,
    source: &str,
    joined: Option<&str>,
    target: &str,
    unkeyed: Option<&str>,
) -> Result<(), tokio_postgres::Error> {
    for table in std::iter::once(source).chain(joined) {
        if unkeyed == Some(table) {
            continue;
        }
        let declared = super::source_key::declared_by_reader(def, source, joined, table);
        let recorded = recorded_for(client, table).await?;
        match declared {
            Some(key) if recorded.as_deref() != Some(key) => write(client, table, key).await?,
            Some(_) => {}
            None if recorded.is_none() => {
                write(client, table, &key_names(client, table).await?).await?;
            }
            None => {}
        }
    }
    // A 1-1 target over a source left unkeyed is keyed by its own primary
    // key, the key its source had at define, not by whatever the source's
    // identity reads as now.
    let source_key = if unkeyed == Some(source) && keys_target_by_source(def) {
        primary_key_names(client, target).await?
    } else {
        key_of(client, source).await?
    };
    write(client, target, &target_key(def, &source_key)).await
}

/// The key recorded for `table` (an unquoted `schema.table`), if it has a
/// record tied to it ([`tied_to`]), the way [`ddl::identity_key_columns`]
/// reads it.
pub(crate) async fn recorded_for(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<Vec<String>>, tokio_postgres::Error> {
    Ok(client
        .query_typed_opt(
            &format!(
                "select k.key_columns from definition_keys k where k.table_name = $1 and {}",
                tied_to("$2")
            ),
            &[
                (&table, Type::TEXT),
                (&ddl::regclass_arg(table), Type::TEXT),
            ],
        )
        .await?
        .map(|row| row.get(0)))
}

/// Ties each record a logical restore brought (its stored `definition_keys`
/// oid isn't the live one) to the table at its name now, so a drop and
/// create after the restore unties it again. A record whose table doesn't
/// exist is left as it is. Run at attach ([`backfill_in`]) and by the
/// capture pass's response to a restore (`capture::restore`).
pub(crate) async fn retie_restored(
    client: &impl GenericClient,
) -> Result<(), tokio_postgres::Error> {
    let restored: Vec<String> = client
        .query(
            "select k.table_name from definition_keys k where k.catalog_oid <> k.tableoid",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for table in restored {
        client
            .execute(
                "update definition_keys k \
                 set relation_oid = pg_catalog.to_regclass($2)::pg_catalog.oid, \
                     catalog_oid = k.tableoid \
                 where k.table_name = $1 and pg_catalog.to_regclass($2) is not null",
                &[&table, &ddl::regclass_arg(&table)],
            )
            .await?;
    }
    Ok(())
}

/// `table`'s unique indexes, oldest first, each as `name (key columns)` with
/// its `INCLUDE` columns and whatever keeps it from being a row identity.
pub(crate) async fn unique_indexes(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<String>, tokio_postgres::Error> {
    Ok(client
        .query_typed(
            "select pg_catalog.format('%I (%s)%s%s', c.relname, \
                    (select string_agg(case when k.attnum = 0 then 'an expression' \
                                            else pg_catalog.quote_ident(a.attname) end, \
                                       ', ' order by k.ord) \
                     from unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                     left join pg_catalog.pg_attribute a \
                       on a.attrelid = i.indrelid and a.attnum = k.attnum \
                     where k.ord <= i.indnkeyatts), \
                    case when i.indnatts > i.indnkeyatts then \
                        ' include (' || \
                        (select string_agg(pg_catalog.quote_ident(a.attname), ', ' \
                                           order by k.ord) \
                         from unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                         join pg_catalog.pg_attribute a \
                           on a.attrelid = i.indrelid and a.attnum = k.attnum \
                         where k.ord > i.indnkeyatts) || ')' \
                    else '' end, \
                    concat_ws('', \
                        case when i.indisprimary then ', the primary key' end, \
                        case when not i.indisvalid then ', invalid' end, \
                        case when i.indpred is not null then ', partial' end, \
                        case when not i.indimmediate then ', deferrable' end, \
                        case when not i.indnullsnotdistinct and exists ( \
                                 select 1 \
                                 from unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                                 join pg_catalog.pg_attribute a \
                                   on a.attrelid = i.indrelid and a.attnum = k.attnum \
                                 where k.ord <= i.indnkeyatts and not a.attnotnull) \
                             then ', over a nullable column and not NULLS NOT DISTINCT' end)) \
             from pg_catalog.pg_index i \
             join pg_catalog.pg_class c on c.oid = i.indexrelid \
             where i.indrelid = pg_catalog.to_regclass($1) and i.indisunique \
             order by i.indexrelid",
            &[(&ddl::regclass_arg(table), Type::TEXT)],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// The refusal for `table` (an unquoted `schema.table`) when it has a record
/// tied to it and no index over the recorded key now: the table, the
/// recorded key and the unique indexes it has. `None` when it has one, has
/// no record, or doesn't exist (its own check reports that).
pub(crate) async fn missing(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<DdlError>, tokio_postgres::Error> {
    let Some(recorded) = recorded_for(client, table).await? else {
        return Ok(None);
    };
    if !ddl::identity_key_columns(client, table).await?.is_empty() {
        return Ok(None);
    }
    Ok(Some(DdlError::RecordedKeyMissing {
        table: table.to_string(),
        indexes: unique_indexes(client, table).await?,
        trellis_target: super::source_key::is_trellis_target(client, table).await?,
        recorded,
    }))
}

/// What a resume reads of one table a definition keys by ([`decide`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableKey {
    /// The recorded key columns, if the table's name has a record, and
    /// whether that record is tied to the relation at the name now
    /// ([`tied_to`]): `false` for a record of a table since dropped.
    pub(crate) recorded: Option<(Vec<String>, bool)>,
    /// Whether the table has a valid primary key or qualifying unique index
    /// over exactly the recorded columns.
    pub(crate) resolves: bool,
    /// The table's primary key's key columns, in key order; empty when it
    /// has none.
    pub(crate) primary_key: Vec<String>,
    /// Whether the table is a definition's target, keyed by its definition.
    pub(crate) trellis_target: bool,
    /// The `KEY (...)` the definition declares for the table (ADR-0026):
    /// its key whatever the table's primary key is, and `resolves` is then
    /// whether an index backs it ([`super::source_key::backing`]).
    pub(crate) declared: Option<Vec<String>>,
}

/// What a resume does with one table's key ([`decide`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Rekey {
    /// The record holds: nothing to do.
    Keep,
    /// Record these columns, tied to the relation at the name now, for
    /// every reader of the table.
    Record(Vec<String>),
    /// A Trellis target lost the index over its recorded key: only the index
    /// coming back repairs it.
    Missing { recorded: Vec<String> },
    /// A user table's key changed, and it has no primary key to take: no
    /// other unique index is taken instead.
    NoPrimaryKey { recorded: Vec<String> },
    /// No index backs the table's declared `KEY (...)`: only one built
    /// again repairs it.
    Unbacked { declared: Vec<String> },
    /// A 1-1 transform's source has another key than its target is keyed by.
    Changed {
        recorded: Vec<String>,
        primary_key: Vec<String>,
    },
}

/// What a resume of a definition does with the key of one table it keys by,
/// `key`: the rules of a key that changed (see the module doc). With
/// `keys_target`, the table is the source a 1-1 target is keyed by.
///
/// A record that holds (tied to the table, with its index) is kept. A
/// Trellis target keeps the key its definition gave it: the record is tied
/// again to a table with its index, and refused without one. A user table
/// whose key changed (no index over the record, a record of a dropped
/// table, or a primary key other than the record, whatever unique index
/// over the recorded columns is left) takes its primary key now, and
/// refuses without one; a 1-1 transform refuses a primary key other than
/// the recorded key. A user table with no record at all is keyed by its row
/// identity and keeps it.
pub(crate) fn decide(key: &TableKey, keys_target: bool) -> Rekey {
    // A declared `KEY` is the table's key while an index backs it, whatever
    // its primary key does: it is recorded again for a table at the name
    // that isn't the one recorded, and never replaced by the primary key.
    if let Some(declared) = &key.declared {
        if !key.resolves {
            return Rekey::Unbacked {
                declared: declared.clone(),
            };
        }
        return match &key.recorded {
            Some((recorded, true)) if recorded == declared => Rekey::Keep,
            _ => Rekey::Record(declared.clone()),
        };
    }
    let Some((recorded, tied)) = &key.recorded else {
        return Rekey::Keep;
    };
    // A user table's primary key is what capture keys it by: one other than
    // the record is a changed key even while a unique index over the
    // recorded columns is left.
    let primary_key_moved =
        !key.trellis_target && !key.primary_key.is_empty() && key.primary_key != *recorded;
    if key.resolves && (*tied || key.trellis_target) && !primary_key_moved {
        return if *tied {
            Rekey::Keep
        } else {
            Rekey::Record(recorded.clone())
        };
    }
    if key.trellis_target {
        return Rekey::Missing {
            recorded: recorded.clone(),
        };
    }
    if key.primary_key.is_empty() {
        return Rekey::NoPrimaryKey {
            recorded: recorded.clone(),
        };
    }
    if keys_target && key.primary_key != *recorded {
        return Rekey::Changed {
            recorded: recorded.clone(),
            primary_key: key.primary_key.clone(),
        };
    }
    Rekey::Record(key.primary_key.clone())
}

/// What [`decide`] reads of `table` (an unquoted `schema.table`); `None`
/// when it doesn't exist, which the re-validation reports itself.
async fn table_key(
    client: &impl GenericClient,
    table: &str,
    declared: Option<&[String]>,
) -> Result<Option<TableKey>, tokio_postgres::Error> {
    let regclass = ddl::regclass_arg(table);
    let Some(row) = client
        .query_typed_opt(
            &format!(
                "select (select k.key_columns from definition_keys k where k.table_name = $2), \
                        coalesce((select {} from definition_keys k where k.table_name = $2), \
                                 false), \
                        exists (select 1 from transform_definitions d \
                                where d.target_table = $2), \
                        array(select a.attname::text \
                              from pg_catalog.pg_index i \
                              cross join unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                              join pg_catalog.pg_attribute a \
                                on a.attrelid = i.indrelid and a.attnum = k.attnum \
                              where i.indrelid = r.oid and i.indisprimary \
                                and k.ord <= i.indnkeyatts \
                              order by k.ord) \
                 from (select pg_catalog.to_regclass($1) as oid) r \
                 where r.oid is not null",
                tied_to("$1")
            ),
            &[(&regclass, Type::TEXT), (&table, Type::TEXT)],
        )
        .await?
    else {
        return Ok(None);
    };
    let recorded: Option<Vec<String>> = row.get(0);
    let resolves = match (declared, &recorded) {
        (Some(key), _) => super::source_key::backing(client, table, key)
            .await?
            .is_some(),
        (None, Some(columns)) => !ddl::identity_key_columns_over(client, table, Some(columns))
            .await?
            .is_empty(),
        (None, None) => false,
    };
    Ok(Some(TableKey {
        recorded: recorded.map(|columns| (columns, row.get(1))),
        resolves,
        primary_key: row.get(3),
        trellis_target: row.get(2),
        declared: declared.map(<[String]>::to_vec),
    }))
}

/// The tables `definition` keys by, each with whether its target is keyed by
/// that table's key and the `KEY (...)` it declares for it: its source, a
/// `JOIN`'s joined table and its target.
async fn tables_of(
    client: &impl GenericClient,
    definition: &Definition,
) -> Result<Vec<(String, bool, Option<Vec<String>>)>, CatalogError> {
    let def = &definition.def;
    let mut tables = vec![(
        definition.source_table.clone(),
        keys_target_by_source(def),
        super::source_key::declared_on(
            def,
            super::join::spec(def).map(|_| super::join::Side::From),
        )
        .map(<[String]>::to_vec),
    )];
    if let Some(joined) = super::catalog::join_table_of(client, definition.id).await? {
        tables.push((
            joined,
            false,
            super::source_key::declared_on(def, Some(super::join::Side::Joined))
                .map(<[String]>::to_vec),
        ));
    }
    tables.push((definition.target_table.clone(), false, None));
    Ok(tables)
}

/// The refusal [`decide`]'s `rekey` makes for `table`, or `None` for one it
/// doesn't refuse.
async fn refusal(
    client: &impl GenericClient,
    table: &str,
    rekey: Rekey,
) -> Result<Option<CatalogError>, tokio_postgres::Error> {
    Ok(match rekey {
        Rekey::Keep | Rekey::Record(_) => None,
        Rekey::Missing { recorded } => Some(CatalogError::Ddl(DdlError::RecordedKeyMissing {
            table: table.to_string(),
            recorded,
            indexes: unique_indexes(client, table).await?,
            trellis_target: true,
        })),
        Rekey::NoPrimaryKey { recorded } => {
            Some(CatalogError::Ddl(DdlError::NoPrimaryKeyToRecord {
                table: table.to_string(),
                recorded,
                indexes: unique_indexes(client, table).await?,
            }))
        }
        Rekey::Unbacked { declared } => Some(CatalogError::Ddl(DdlError::KeyNotBacked {
            table: table.to_string(),
            indexes: unique_indexes(client, table).await?,
            partitioned: client
                .query_one(
                    "select exists (select 1 from pg_catalog.pg_class \
                         where oid = pg_catalog.to_regclass($1) and relkind = 'p')",
                    &[&ddl::regclass_arg(table)],
                )
                .await?
                .get(0),
            key: declared,
        })),
        Rekey::Changed {
            recorded,
            primary_key,
        } => Some(CatalogError::SourceKeyChanged {
            source_table: table.to_string(),
            target_key: recorded,
            source_key: primary_key,
        }),
    })
}

/// A resume's re-keying of `definition`, in `txn`, before it re-validates:
/// for each table it keys by, what [`decide`] says. A re-recorded key is the
/// table's, for every reader of it, so it commits only with the resume.
/// Refuses as [`decide`] does, writing nothing more.
///
/// A re-recorded key that differs from the recorded one also pauses every
/// other reader of the table still running ([`pause_running_readers`]): the
/// capture pass takes a primary key that is the table's record as every
/// reader's key (`staging::schema_change::pause_readers_of_missing`), so a
/// reader that wasn't paused when the key changed (this one was paused for
/// another reason, and resumed before the pass saw the change) would
/// otherwise read rows keyed by the new key into state keyed by the old.
pub(crate) async fn rekey_for_resume(
    txn: &tokio_postgres::Transaction<'_>,
    definition: &Definition,
) -> Result<(), CatalogError> {
    for Rerecord {
        table,
        old,
        columns,
    } in rekey(txn, definition, None).await?
    {
        write(txn, &table, &columns).await?;
        if old != columns {
            pause_running_readers(txn, definition.id, &table, &old, &columns).await?;
        }
        crate::instance_log::info!(
            transform = %definition.target_table,
            table = %table,
            key = %columns.join(", "),
            "recorded the key of a table whose key changed while its reader was paused"
        );
    }
    Ok(())
}

/// A key [`rekey_for_resume`] records for a table ([`Rekey::Record`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rerecord {
    /// The table (an unquoted `schema.table`).
    pub(crate) table: String,
    /// Its recorded key columns now.
    pub(crate) old: Vec<String>,
    /// The key columns recorded for it.
    pub(crate) columns: Vec<String>,
}

/// What [`rekey_for_resume`] does with `definition`'s keys, reading only:
/// the key it records for each table whose [`decide`] is a record, or the
/// refusal it makes. `repaired` names a target [`repair_target_key`] re-keys
/// first ([`target_repair`]): it then has the index over its recorded key.
/// A resume's outcome reports it before the rest of the re-validation
/// (`staging::pause_message`).
pub(crate) async fn rekey(
    client: &impl GenericClient,
    definition: &Definition,
    repaired: Option<&str>,
) -> Result<Vec<Rerecord>, CatalogError> {
    let mut records = Vec::new();
    for (table, keys_target, declared) in tables_of(client, definition).await? {
        let Some(mut key) = table_key(client, &table, declared.as_deref()).await? else {
            continue;
        };
        if repaired == Some(table.as_str()) {
            key.resolves = true;
        }
        match decide(&key, keys_target) {
            Rekey::Keep => {}
            Rekey::Record(columns) => records.push(Rerecord {
                old: key.recorded.map(|(old, _)| old).unwrap_or_default(),
                table,
                columns,
            }),
            rekey => {
                if let Some(err) = refusal(client, &table, rekey).await? {
                    return Err(err);
                }
            }
        }
    }
    Ok(records)
}

/// Records `rerecords` ([`rekey`]) in `txn` and nothing else: a resume's
/// outcome does it in a transaction it rolls back, so the re-validation it
/// reports reads the keys a resume's would (`staging::pause_message`).
///
/// Every record's row lock is taken first, without waiting: a record
/// another transaction holds (a resume or define writing it) fails it with
/// `lock_not_available` at once. Holding one record while it waits for
/// another could close a deadlock with a resume writing the same two
/// records in the other order (two `JOIN`s over one pair of tables), whose
/// deadlock check, having waited first, would fail the resume.
pub(crate) async fn write_rerecords(
    txn: &tokio_postgres::Transaction<'_>,
    rerecords: &[Rerecord],
) -> Result<(), tokio_postgres::Error> {
    let tables: Vec<&str> = rerecords.iter().map(|r| r.table.as_str()).collect();
    txn.execute(
        "select 1 from definition_keys where table_name = any($1) for update nowait",
        &[&tables],
    )
    .await?;
    for r in rerecords {
        write(txn, &r.table, &r.columns).await?;
    }
    Ok(())
}

/// Pauses every definition but `resumed` that reads `table` (its source or
/// a `JOIN`'s joined table) and is still running, as the capture pass pauses
/// the readers of a primary key redefined from `old` to `new`: they are
/// keyed by `old`, and their own resume rebuilds them over `new`. A paused
/// reader is left alone, so this never waits on a concurrent resume of one.
async fn pause_running_readers(
    txn: &tokio_postgres::Transaction<'_>,
    resumed: i64,
    table: &str,
    old: &[String],
    new: &[String],
) -> Result<(), CatalogError> {
    let running: Vec<i64> = txn
        .query(
            "select id from transform_definitions \
             where id <> $1 and (source_table = $2 or join_table = $2) and status = any($3) \
             order by id",
            &[
                &resumed,
                &table,
                &super::model::StoredStatus::dispatchable(),
            ],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let error = crate::staging::schema_change::key_changed_error(table, old, new);
    for id in running {
        if super::lifecycle::pause_for_capture_failure(txn, id, table, old, &error, None).await? {
            crate::instance_log::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    Ok(())
}

/// Refuses `definition` when a table it keys by has a key it can't key by
/// now: what [`decide`] refuses, and a record tied to the table that has no
/// index (a resume re-records it first, [`rekey_for_resume`]). A record of a
/// dropped table refuses only as [`decide`] does: the table at the name is
/// keyed by its row identity until a resume ties it. The re-validation and
/// a build's start run it, so the definition is never keyed by a guess.
pub(crate) async fn check(
    client: &impl GenericClient,
    definition: &Definition,
) -> Result<(), CatalogError> {
    for (table, keys_target, declared) in tables_of(client, definition).await? {
        let Some(key) = table_key(client, &table, declared.as_deref()).await? else {
            continue;
        };
        let tied = key.recorded.as_ref().is_some_and(|(_, tied)| *tied);
        match decide(&key, keys_target) {
            Rekey::Keep => {}
            Rekey::Record(_) if !tied => {}
            Rekey::Record(_) => {
                if let Some(err) = missing(client, &table).await? {
                    return Err(CatalogError::Ddl(err));
                }
            }
            rekey => {
                if let Some(err) = refusal(client, &table, rekey).await? {
                    return Err(err);
                }
            }
        }
    }
    Ok(())
}

/// `table`'s `INCLUDE` columns (an unquoted `schema.table`), in index order,
/// that an earlier release read as key columns: those of the index that
/// release keyed `table` by (its primary key, else its oldest immediate,
/// non-partial unique index that is `NULLS NOT DISTINCT` or over `NOT NULL`
/// columns only, `INCLUDE` columns counted), when that index's key columns
/// are `key`. Empty otherwise: any other index with `INCLUDE` columns, a
/// covering index beside the key among them, never keyed anything.
async fn included_columns(
    client: &impl GenericClient,
    table: &str,
    key: &[String],
) -> Result<Vec<String>, tokio_postgres::Error> {
    Ok(client
        .query_typed(
            "select array(select a.attname::text \
                          from unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                          join pg_catalog.pg_attribute a \
                            on a.attrelid = i.indrelid and a.attnum = k.attnum \
                          where k.ord > i.indnkeyatts \
                            and a.attname::text <> all($2) \
                          order by k.ord) \
             from (select i.* from pg_catalog.pg_index i \
                   where i.indrelid = pg_catalog.to_regclass($1) \
                     and (i.indisprimary \
                          or (i.indisunique and i.indimmediate and i.indpred is null \
                              and i.indexprs is null \
                              and (i.indnullsnotdistinct \
                                   or not exists ( \
                                       select 1 from pg_catalog.pg_attribute a \
                                       where a.attrelid = i.indrelid \
                                         and a.attnum = any(i.indkey) \
                                         and not a.attnotnull)))) \
                   order by i.indisprimary desc, i.indexrelid \
                   limit 1) i \
             where i.indnatts > i.indnkeyatts \
               and array(select a.attname::text \
                         from unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                         join pg_catalog.pg_attribute a \
                           on a.attrelid = i.indrelid and a.attnum = k.attnum \
                         where k.ord <= i.indnkeyatts \
                         order by k.ord) = $2",
            &[
                (&ddl::regclass_arg(table), Type::TEXT),
                (&key, Type::TEXT_ARRAY),
            ],
        )
        .await?
        .into_iter()
        .next()
        .map(|row| row.get(0))
        .unwrap_or_default())
}

/// The `capture_failure` of a 1-1 definition reading `source`, a 1-1 target
/// an earlier release keyed by `table`'s key with its `INCLUDE` columns
/// (#1722), so that release keyed this target by them too.
pub(crate) fn included_upstream_reason(
    target: &str,
    source: &str,
    table: &str,
    included: &[String],
) -> String {
    format!(
        "transform {target} reads {source}, which an earlier release built over {table}'s key \
         with its INCLUDE columns ({}) as if they were key columns, so it keyed {target} by \
         them too; they are not part of the key. RESUME {source} first, then RESUME this \
         transform, which re-keys it and rebuilds it (its target keeps its rows, user indexes \
         and grants, and loses the columns it no longer creates), or drop the transform and \
         define it again",
        included.join(", ")
    )
}

/// The `capture_failure` of a definition an earlier release built over the
/// `INCLUDE` columns of `table`'s key (#1722).
pub(crate) fn included_key_reason(target: &str, table: &str, included: &[String]) -> String {
    format!(
        "transform {target} was built by an earlier release over {table}'s key with its \
         INCLUDE columns ({}) as if they were key columns; they are not part of the key. \
         RESUME re-keys the transform and rebuilds it (a 1-1 target keeps its rows, user \
         indexes and grants, and loses the columns it no longer creates), or drop the \
         transform and define it again",
        included.join(", ")
    )
}

/// Ties the records a logical restore brought to the tables at their names
/// ([`retie_restored`]), then records the keys of every stored definition
/// whose target has no record (`definition_keys`): one a release before the
/// record existed stored. Run at attach
/// ([`super::grammar::pause_unloadable_in`]), in `txn`, after the
/// definitions this binary can't run or read are paused, which it skips.
///
/// In id order, so a definition reading another's target finds that target's
/// key recorded first. The target's key is the one its parsed definition
/// gives ([`target_key`]), even when the index today's rule would pick is a
/// user's: a definition whose target has no index over it then refuses its
/// next resume or build, by name ([`check`]). A source's and a joined table's
/// is its row identity now.
///
/// A definition whose source or joined table's key index has `INCLUDE`
/// columns was built over them as key columns (#1722): it is paused with a
/// `capture_failure` naming them ([`included_key_reason`]). So is a 1-1
/// definition reading such a definition's 1-1 target, at any depth, whose
/// target that release keyed by the same columns
/// ([`included_upstream_reason`]). Returns how many it paused.
pub(crate) async fn backfill_in(
    txn: &tokio_postgres::Transaction<'_>,
    floors: &Floors,
    oldest: i32,
    current: i32,
) -> Result<usize, CatalogError> {
    retie_restored(txn).await?;
    let rows = txn
        .query(
            "select d.id, d.source_table, d.target_table, d.join_table, d.definition_text, \
                    d.grammar_version, d.floor_version \
             from transform_definitions d \
             where not exists (select 1 from definition_keys k \
                               where k.table_name = d.target_table) \
             order by d.id",
            &[],
        )
        .await?;
    let mut paused = 0;
    // The 1-1 targets an earlier release keyed by an `INCLUDE` column, with
    // the table and the columns: a 1-1 reader of one is keyed by them too.
    let mut mis_keyed: Vec<(String, String, Vec<String>)> = Vec::new();
    for row in &rows {
        let (id, source, target, joined): (i64, String, String, Option<String>) =
            (row.get(0), row.get(1), row.get(2), row.get(3));
        let (text, grammar_version, floor): (&str, i32, &str) =
            (row.get(4), row.get(5), row.get(6));
        if floors
            .unrunnable_in(floor, grammar_version, current)
            .is_some()
        {
            continue;
        }
        let Ok(mut def) = super::grammar::load_in(text, grammar_version, oldest, current) else {
            continue;
        };
        let unkeyed =
            declare_released_keys(txn, id, &mut def, &source, joined.as_deref(), &target).await?;
        record_except(
            txn,
            &def,
            &source,
            joined.as_deref(),
            &target,
            unkeyed.as_ref().map(|(table, _)| table.as_str()),
        )
        .await?;
        if let Some((_, reason)) = &unkeyed
            && super::lifecycle::pause_for_included_key(txn, id, &source, reason).await?
        {
            paused += 1;
        }
        let mut included = Vec::new();
        for table in std::iter::once(&source).chain(joined.as_ref()) {
            let key = key_names(txn, table).await?;
            let columns = included_columns(txn, table, &key).await?;
            if !columns.is_empty() {
                included.push((table.clone(), columns));
            }
        }
        let one_to_one = matches!(
            def.key_space,
            KeySpace::OneToOne | KeySpace::JoinOneToOne(_)
        );
        let upstream = mis_keyed
            .iter()
            .find(|(upstream, _, _)| *upstream == source)
            .filter(|_| one_to_one)
            .map(|(_, table, columns)| (table.clone(), columns.clone()));
        let reason = if let Some((table, columns)) = &upstream {
            Some(included_upstream_reason(&target, &source, table, columns))
        } else {
            included
                .first()
                .map(|(table, columns)| included_key_reason(&target, table, columns))
        };
        if one_to_one {
            let from = upstream.or_else(|| {
                included
                    .iter()
                    .find(|(table, _)| *table == source)
                    .map(|(table, columns)| (table.clone(), columns.clone()))
            });
            if let Some((table, columns)) = from {
                mis_keyed.push((target.clone(), table, columns));
            }
        }
        if let Some(reason) = reason
            && super::lifecycle::pause_for_included_key(txn, id, &source, &reason).await?
        {
            paused += 1;
        }
    }
    Ok(paused)
}

/// The `capture_failure` of a definition reading `table`, which has no
/// primary key and which an earlier release keyed by a unique index that
/// backs no key now (ADR-0026); `indexes` are its unique indexes.
pub(crate) fn unkeyed_reason(target: &str, table: &str, indexes: &[String]) -> String {
    format!(
        "transform {target} reads {table}, which has no primary key. An earlier release \
         keyed it by a unique index it chose; Trellis now keys a table by its primary key or \
         by a KEY (column, ...) the definition names, backed by a valid, immediate, \
         non-partial unique index over exactly those columns and no expression, every \
         column NOT NULL or the index NULLS NOT DISTINCT, and none of {table}'s does ({}). \
         Add a primary key (ALTER TABLE {table} ADD PRIMARY KEY ...) and RESUME this \
         transform, or DROP TRANSFORM and define it again with a valid KEY (...)",
        if indexes.is_empty() {
            "it has no unique index".to_string()
        } else {
            indexes.join(", ")
        }
    )
}

/// The `capture_failure` of a 1-1 definition reading `table`, which has no
/// primary key, whose target is keyed by `built_on` (the primary key
/// `table` had at define) while the index an earlier release chose for
/// `table` is over `chosen` ([`declare_released_keys`]).
pub(crate) fn rebuilt_key_reason(
    target: &str,
    table: &str,
    chosen: &[String],
    built_on: &[String],
) -> String {
    format!(
        "transform {target} reads {table}, which has no primary key. Its target is keyed by \
         ({}), the key {table} had when the transform was defined, and the unique index an \
         earlier release chose for {table} is over ({}), another key. Add the primary key \
         back (ALTER TABLE {table} ADD PRIMARY KEY ({})) and RESUME this transform, or \
         DROP TRANSFORM and define it again with a valid KEY (...)",
        built_on.join(", "),
        chosen.join(", "),
        built_on.join(", "),
    )
}

/// `table`'s primary key columns, in key order; none when it has no primary
/// key or doesn't exist.
async fn primary_key_names(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<String>, tokio_postgres::Error> {
    Ok(client
        .query_one(
            "select array( \
                 select a.attname::text \
                 from pg_catalog.pg_index i \
                 cross join unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                 join pg_catalog.pg_attribute a \
                   on a.attrelid = i.indrelid and a.attnum = k.attnum \
                 where i.indrelid = pg_catalog.to_regclass($1) and i.indisprimary \
                   and k.ord <= i.indnkeyatts \
                 order by k.ord)",
            &[&ddl::regclass_arg(table)],
        )
        .await?
        .get(0))
}

/// A released definition over a user table with no primary key, which an
/// earlier release keyed by a unique index it chose (its oldest qualifying
/// one): the definition `id` gets that index's key columns as an explicit
/// `KEY (...)` for the table, written into its stored text, when that index
/// backs a key now ([`super::source_key::backing`], and the key types'
/// allowlist), so it keeps running on the same key (ADR-0026, #1745 rule
/// 8). Otherwise returns the table and its unique indexes, and nothing is
/// written: [`backfill_in`] pauses the definition with the returned
/// reason. A table that has a primary key, a `KEY` the definition declares
/// already, or that is another transform's target, is left as it is.
///
/// A 1-1 target (plain or `JOIN`) is keyed by its source's key as it was
/// at define, its primary key and its ledger over the same columns. Its
/// source can have no primary key now only because that key was dropped
/// after define, so the index the earlier release chose may be over other
/// columns than the target was built on. The index is taken only when its
/// columns are the target's primary key's; otherwise the definition is
/// paused, and adding the primary key back and `RESUME` repairs it. An
/// aggregate's or `JOIN`'s target is keyed by its group columns, which a
/// rebuild-free change of its source's key leaves alone.
async fn declare_released_keys(
    txn: &tokio_postgres::Transaction<'_>,
    id: i64,
    def: &mut TransformDef,
    source: &str,
    joined: Option<&str>,
    target: &str,
) -> Result<Option<(String, String)>, CatalogError> {
    use super::join::Side;
    let join = super::join::spec(def).is_some();
    let sides = std::iter::once((source, join.then_some(Side::From)))
        .chain(joined.map(|table| (table, Some(Side::Joined))));
    let mut declared = Vec::new();
    for (table, side) in sides {
        if super::source_key::declared_on(def, side).is_some()
            || super::source_key::is_trellis_target(txn, table).await?
        {
            continue;
        }
        let has_primary_key: Option<bool> = txn
            .query_opt(
                "select exists (select 1 from pg_catalog.pg_index i \
                     where i.indrelid = c.oid and i.indisprimary) \
                 from pg_catalog.pg_class c where c.oid = pg_catalog.to_regclass($1)",
                &[&ddl::regclass_arg(table)],
            )
            .await?
            .map(|row| row.get(0));
        if has_primary_key != Some(false) {
            continue;
        }
        // The index the earlier release chose: with no record yet, the
        // identity's fallback (the oldest qualifying unique index, no
        // expression, its key columns only), or the record an earlier
        // definition over the same table was just given here.
        let chosen = key_names(txn, table).await?;
        let backs = if chosen.is_empty() {
            None
        } else {
            super::source_key::backing(txn, table, &chosen).await?
        };
        let admitted = match &backs {
            Some(columns) => ddl::admit_key_types(txn, table, columns).await.is_ok(),
            None => false,
        };
        if !admitted {
            let indexes = unique_indexes(txn, table).await?;
            return Ok(Some((
                table.to_string(),
                unkeyed_reason(target, table, &indexes),
            )));
        }
        if keys_target_by_source(def) && side != Some(Side::Joined) {
            let built_on = primary_key_names(txn, target).await?;
            let mut chosen_set = chosen.clone();
            chosen_set.sort();
            let mut built_set = built_on.clone();
            built_set.sort();
            if chosen_set != built_set {
                return Ok(Some((
                    table.to_string(),
                    rebuilt_key_reason(target, table, &chosen, &built_on),
                )));
            }
        }
        declared.push((side, chosen));
    }
    if declared.is_empty() {
        return Ok(None);
    }
    for (side, key) in declared {
        match side {
            None | Some(Side::From) => def.source_key = Some(key),
            Some(Side::Joined) => {
                if let super::ast::KeySpace::Join(join) | super::ast::KeySpace::JoinOneToOne(join) =
                    &mut def.key_space
                {
                    join.right_key = Some(key);
                }
            }
        }
    }
    let text = super::ast::render_definition_text(def);
    txn.execute(
        "update transform_definitions set definition_text = $2 where id = $1",
        &[&id, &text],
    )
    .await?;
    crate::instance_log::info!(
        transform_id = id,
        definition = %text,
        "named the key an earlier release keyed a table with no primary key by, as KEY (...)"
    );
    Ok(None)
}

/// Re-keys a 1-1 target (plain or `JOIN`) an earlier release built over its
/// source key's `INCLUDE` columns (#1722), or over those of the 1-1 target it
/// reads, in `txn`, before a resume re-validates it: the target's key
/// constraint over the old identity (the recorded key followed by columns
/// the definition doesn't create, which only that release's key adds) is
/// replaced in place by one over the recorded key, and each of those former
/// key columns is dropped. The table, its
/// rows, the user's indexes and grants stay. Its ledger's entries, keyed by
/// the old key's text, are deleted: the rebuild re-derives every row, and
/// its sweep removes the target rows no source row backs.
///
/// Does nothing unless the target has no index over its recorded key, its
/// source's row identity is the recorded key, and the target has that old
/// key. Refuses, naming them, when an object other than the
/// key depends on a column it would drop: a user's view or index.
pub(crate) async fn repair_target_key(
    txn: &tokio_postgres::Transaction<'_>,
    definition: &Definition,
    source: &str,
) -> Result<bool, CatalogError> {
    let Some(repair) = target_repair(txn, definition, source).await? else {
        return Ok(false);
    };
    let TargetRepair {
        target,
        recorded,
        constraint,
        dropped,
        nullable,
        ..
    } = &repair;
    let mut actions = vec![
        format!("drop constraint {}", quote_ident(constraint)),
        format!(
            "add {} ({})",
            if *nullable {
                "unique nulls not distinct"
            } else {
                "primary key"
            },
            recorded
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ];
    actions.extend(
        dropped
            .iter()
            .map(|c| format!("drop column {}", quote_ident(c))),
    );
    txn.batch_execute(&format!(
        "alter table {} {}",
        ddl::qualified_target_table_ident(target),
        actions.join(", ")
    ))
    .await?;
    // A plain 1-1 target's ledger is keyed by the old key's text; a 1-1
    // `JOIN`'s hidden tables are recreated by the resume.
    if matches!(definition.def.key_space, KeySpace::OneToOne) {
        let ledger = ddl::qualified_target_table_ident(&super::ledger::ledger_table_name(target));
        txn.batch_execute(&format!("delete from {ledger}")).await?;
    }
    crate::instance_log::warn!(
        transform = %target,
        key = %recorded.join(", "),
        dropped = ?dropped,
        "re-keyed a 1-1 target an earlier release built over its source key's INCLUDE columns"
    );
    Ok(true)
}

/// The re-keying [`repair_target_key`] makes of a 1-1 target an earlier
/// release built over its source key's `INCLUDE` columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetRepair {
    /// The target (an unquoted `schema.table`).
    pub(crate) target: String,
    /// Its recorded key, which the new key constraint is over.
    pub(crate) recorded: Vec<String>,
    /// The key constraint over the old identity, which is replaced.
    constraint: String,
    /// The former key columns it drops.
    pub(crate) dropped: Vec<String>,
    /// Whether a recorded key column can be NULL: the new key is then a
    /// `unique nulls not distinct` constraint rather than a primary key.
    nullable: bool,
}

/// What [`repair_target_key`] would do to `definition`'s target, reading
/// only: `None` when it does nothing, the refusal when a user's object
/// depends on a column it would drop. A resume's outcome reports it
/// (`staging::pause_message`).
pub(crate) async fn target_repair(
    client: &impl GenericClient,
    definition: &Definition,
    source: &str,
) -> Result<Option<TargetRepair>, CatalogError> {
    if !matches!(
        definition.def.key_space,
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_)
    ) {
        return Ok(None);
    }
    let target = definition.target_table.as_str();
    let Some(recorded) = recorded_for(client, target).await? else {
        return Ok(None);
    };
    if !ddl::identity_key_columns(client, target).await?.is_empty() {
        return Ok(None);
    }
    if key_names(client, source).await? != recorded {
        return Ok(None);
    }
    let fields: Vec<&str> = definition
        .def
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    // The target's key constraint over the old identity: the recorded key
    // followed by columns that are neither key columns nor fields. Define
    // creates no other column, so only an earlier release's key over its
    // source key's `INCLUDE` columns (or, for a reader of such a target, over
    // that target's) has them.
    let mut old = None;
    for row in client
        .query_typed(
            "select c.oid, c.conname::text, c.conindid, \
                    array(select a.attname::text \
                          from unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                          join pg_catalog.pg_attribute a \
                            on a.attrelid = i.indrelid and a.attnum = k.attnum \
                          where k.ord <= i.indnkeyatts \
                          order by k.ord) \
             from pg_catalog.pg_constraint c \
             join pg_catalog.pg_index i on i.indexrelid = c.conindid \
             where c.conrelid = pg_catalog.to_regclass($1) and c.contype in ('p', 'u') \
               and i.indexprs is null \
             order by c.contype, c.oid",
            &[(&ddl::regclass_arg(target), Type::TEXT)],
        )
        .await?
    {
        let key: Vec<String> = row.get(3);
        let Some(extra) = key.strip_prefix(recorded.as_slice()) else {
            continue;
        };
        if !extra.is_empty()
            && extra
                .iter()
                .all(|c| !fields.contains(&c.as_str()) && !recorded.contains(c))
        {
            old = Some((
                row.get::<_, u32>(0),
                row.get::<_, String>(1),
                row.get::<_, u32>(2),
                extra.to_vec(),
            ));
            break;
        }
    }
    let Some((constraint_oid, constraint, index_oid, extra)) = old else {
        return Ok(None);
    };
    for column in &extra {
        // Every object depending on the column but the key constraint, its
        // index and the column's own `NOT NULL`: a user's view (through its
        // rewrite rule), index, constraint or statistics object.
        let blocking: Vec<String> = client
            .query_typed(
                "select distinct case \
                     when r.oid is not null then pg_catalog.pg_describe_object( \
                         'pg_catalog.pg_class'::pg_catalog.regclass, r.ev_class, 0) \
                     else pg_catalog.pg_describe_object(d.classid, d.objid, d.objsubid) end \
                 from pg_catalog.pg_depend d \
                 join pg_catalog.pg_attribute a \
                   on a.attrelid = d.refobjid and a.attnum = d.refobjsubid \
                 left join pg_catalog.pg_rewrite r \
                   on d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass \
                  and r.oid = d.objid \
                 where d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass \
                   and d.refobjid = pg_catalog.to_regclass($1) \
                   and a.attname = $2 \
                   and not (d.classid = 'pg_catalog.pg_constraint'::pg_catalog.regclass \
                            and (d.objid = $3 or exists ( \
                                select 1 from pg_catalog.pg_constraint c \
                                where c.oid = d.objid and c.contype = 'n'))) \
                   and not (d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass \
                            and d.objid = $4) \
                 order by 1",
                &[
                    (&ddl::regclass_arg(target), Type::TEXT),
                    (column, Type::TEXT),
                    (&constraint_oid, Type::OID),
                    (&index_oid, Type::OID),
                ],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        if !blocking.is_empty() {
            return Err(CatalogError::KeyRepairBlocked {
                table: target.to_string(),
                column: column.clone(),
                dependents: blocking,
            });
        }
    }
    let nullable: bool = client
        .query_typed_one(
            "select exists (select 1 from pg_catalog.pg_attribute a \
                            where a.attrelid = pg_catalog.to_regclass($1) \
                              and a.attname = any($2) and not a.attnotnull)",
            &[
                (&ddl::regclass_arg(target), Type::TEXT),
                (&recorded, Type::TEXT_ARRAY),
            ],
        )
        .await?
        .get(0);
    Ok(Some(TargetRepair {
        target: target.to_string(),
        recorded,
        constraint,
        dropped: extra,
        nullable,
    }))
}

/// For a unit test on a bare database, which has no catalog: an empty
/// `definition_keys` on its `search_path`, so
/// [`ddl::identity_key_columns`] reads no record.
#[cfg(test)]
pub(crate) const TEST_TABLE_DDL: &str = "create table if not exists public.definition_keys \
     (table_name text primary key, key_columns text[], relation_oid oid, catalog_oid oid)";

#[cfg(test)]
mod tests {
    use super::*;

    async fn connect(dsn: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(TEST_TABLE_DDL)
            .await
            .expect("definition_keys");
        client
    }

    /// `public.t`'s record: key `(a)`, tied to the table there now.
    const TIE_T_TO_A: &str = "insert into public.definition_keys values \
         ('public.t', '{a}', 'public.t'::regclass, 'public.definition_keys'::regclass)";

    async fn key(client: &tokio_postgres::Client, table: &str) -> Vec<String> {
        key_names(client, table).await.expect("the key")
    }

    /// A table without a primary key, with two qualifying unique keys: with
    /// no record, its identity is the older index, and a `REINDEX
    /// CONCURRENTLY` of that one makes the other older and swaps it. With
    /// the key recorded, the identity stays through the reindex, and
    /// through a user's unique index over the same columns plus another.
    #[tokio::test]
    async fn a_recorded_key_survives_a_reindex_of_the_other_key() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table public.t (a integer, b integer, \
                     constraint t_a unique nulls not distinct (a), \
                     constraint t_b unique nulls not distinct (b))",
            )
            .await
            .expect("the table");
        assert_eq!(key(&client, "public.t").await, ["a"]);
        client
            .batch_execute("reindex index concurrently public.t_a")
            .await
            .expect("reindex");
        assert_eq!(
            key(&client, "public.t").await,
            ["b"],
            "with no record, the oldest index wins"
        );

        for sql in [
            TIE_T_TO_A,
            "reindex index concurrently public.t_b",
            "create unique index t_ab on public.t (a, b) nulls not distinct",
        ] {
            client.batch_execute(sql).await.expect(sql);
        }
        assert_eq!(key(&client, "public.t").await, ["a"]);
        client
            .batch_execute("reindex index concurrently public.t_a")
            .await
            .expect("reindex the recorded key");
        assert_eq!(key(&client, "public.t").await, ["a"]);
        assert!(
            missing(&client, "public.t").await.expect("check").is_none(),
            "the recorded key has its index"
        );
    }

    /// With the recorded key's index gone, the table has no identity rather
    /// than another index's, and the refusal names the table, the recorded
    /// key and every unique index it has, with what keeps each one from
    /// serving.
    #[tokio::test]
    async fn a_recorded_key_without_its_index_is_refused_by_name() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table public.t (a integer not null, b integer, c integer); \
                 create table public.transform_definitions (target_table text); \
                 create unique index t_b on public.t (b) include (c) nulls not distinct; \
                 create unique index t_a_part on public.t (a) where c > 0; \
                 create unique index t_expr on public.t ((a + b))",
            )
            .await
            .expect("the table");
        client.batch_execute(TIE_T_TO_A).await.expect("the record");
        assert!(key(&client, "public.t").await.is_empty());
        let refusal = missing(&client, "public.t")
            .await
            .expect("check")
            .expect("refused")
            .to_string();
        for part in [
            "table 'public.t'",
            "recorded for it (a)",
            "t_b (b) include (c)",
            "t_a_part (a), partial",
            "t_expr (an expression)",
        ] {
            assert!(refusal.contains(part), "{part:?} in {refusal}");
        }
        assert!(
            missing(&client, "public.gone")
                .await
                .expect("check")
                .is_none(),
            "a table with no record isn't refused"
        );
    }

    /// A record of a table dropped since is no record of the table created
    /// at the name: its identity is its own primary key. A logical restore's
    /// record (the stored `definition_keys` oid isn't the live one) holds by
    /// name, and [`retie_restored`] ties it to the table there, so a drop
    /// and create after that unties it again.
    #[tokio::test]
    async fn a_record_is_of_the_relation_it_was_taken_from() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table public.t (a integer not null, b integer not null, \
                     unique (a), primary key (b))",
            )
            .await
            .expect("the table");
        client.batch_execute(TIE_T_TO_A).await.expect("the record");
        assert_eq!(key(&client, "public.t").await, ["a"], "the record holds");
        for sql in [
            "alter table public.t alter column b type bigint",
            "vacuum full public.t",
            "reindex table concurrently public.t",
        ] {
            client.batch_execute(sql).await.expect(sql);
        }
        assert_eq!(
            key(&client, "public.t").await,
            ["a"],
            "rewrites keep the oid"
        );

        client
            .batch_execute(
                "drop table public.t; \
                 create table public.t (a integer not null, b integer not null, \
                     unique (a), primary key (b))",
            )
            .await
            .expect("recreated");
        assert_eq!(
            key(&client, "public.t").await,
            ["b"],
            "the old table's record is no record of the new one"
        );
        assert_eq!(recorded_for(&client, "public.t").await.expect("read"), None);

        // As a restore leaves it: a record from another database's
        // `definition_keys`.
        client
            .batch_execute("update public.definition_keys set relation_oid = 0, catalog_oid = 0")
            .await
            .expect("restored");
        assert_eq!(key(&client, "public.t").await, ["a"], "held by name");
        retie_restored(&client).await.expect("retie");
        assert_eq!(key(&client, "public.t").await, ["a"], "tied again");
        client
            .batch_execute(
                "drop table public.t; \
                 create table public.t (a integer not null, b integer not null, \
                     unique (a), primary key (b))",
            )
            .await
            .expect("recreated");
        assert_eq!(key(&client, "public.t").await, ["b"], "untied by the drop");
    }

    fn table(recorded: Option<(&[&str], bool)>, resolves: bool, pk: &[&str]) -> TableKey {
        let names = |c: &[&str]| c.iter().map(|c| c.to_string()).collect::<Vec<_>>();
        TableKey {
            recorded: recorded.map(|(c, tied)| (names(c), tied)),
            resolves,
            primary_key: names(pk),
            trellis_target: false,
            declared: None,
        }
    }

    /// A declared `KEY` (ADR-0026) is the table's key while an index backs
    /// it, whatever its primary key is or becomes: a resume keeps it, ties
    /// it again to a table created again at the name, and never takes the
    /// primary key instead; with no index backing it, it refuses, for every
    /// kind of reader.
    #[test]
    fn a_resume_keeps_a_declared_key_and_never_takes_the_primary_key() {
        let k: &[&str] = &["k"];
        let declared = |recorded, resolves, pk| TableKey {
            declared: Some(cols(k)),
            ..table(recorded, resolves, pk)
        };
        for keys_target in [false, true] {
            for pk in [&[][..], &["id"][..], &["id", "g"][..], k] {
                assert_eq!(
                    decide(&declared(Some((k, true)), true, pk), keys_target),
                    Rekey::Keep,
                    "{pk:?}"
                );
                assert_eq!(
                    decide(&declared(Some((k, false)), true, pk), keys_target),
                    Rekey::Record(cols(k)),
                    "{pk:?}"
                );
                assert_eq!(
                    decide(&declared(None, true, pk), keys_target),
                    Rekey::Record(cols(k)),
                    "{pk:?}"
                );
                for recorded in [Some((k, true)), Some((k, false)), None] {
                    assert_eq!(
                        decide(&declared(recorded, false, pk), keys_target),
                        Rekey::Unbacked { declared: cols(k) },
                        "{pk:?}"
                    );
                }
            }
        }
    }

    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|c| c.to_string()).collect()
    }

    /// The re-validate-and-re-record decision a resume makes for each table.
    #[test]
    fn a_resume_keeps_re_records_or_refuses_a_key() {
        let id: &[&str] = &["id"];
        let wide: &[&str] = &["pid", "id"];
        // A record that holds is kept, by every kind of reader.
        for keys_target in [false, true] {
            assert_eq!(
                decide(&table(Some((id, true)), true, id), keys_target),
                Rekey::Keep
            );
        }
        // A primary key redefined in place, or the table created again with
        // another one: an aggregate or `JOIN` takes it, a 1-1 refuses it.
        for tied in [true, false] {
            let changed = table(Some((id, tied)), false, wide);
            assert_eq!(decide(&changed, false), Rekey::Record(cols(wide)));
            assert_eq!(
                decide(&changed, true),
                Rekey::Changed {
                    recorded: cols(id),
                    primary_key: cols(wide)
                }
            );
        }
        // Created again with the same key: tied to the new table, by any
        // reader, even with another unique index over the old columns left.
        for keys_target in [false, true] {
            assert_eq!(
                decide(&table(Some((id, false)), false, id), keys_target),
                Rekey::Record(cols(id))
            );
        }
        assert_eq!(
            decide(&table(Some((id, false)), true, wide), false),
            Rekey::Record(cols(wide)),
            "a new table's primary key, not the old key's surviving unique index"
        );
        // The primary key moved in place with a unique index over the old
        // columns left: capture keys the table by the primary key, so an
        // aggregate or `JOIN` takes it and a 1-1 refuses it.
        let moved = table(Some((id, true)), true, &["u"]);
        assert_eq!(decide(&moved, false), Rekey::Record(cols(&["u"])));
        assert_eq!(
            decide(&moved, true),
            Rekey::Changed {
                recorded: cols(id),
                primary_key: cols(&["u"])
            }
        );
        // No primary key to take: refused, whatever unique index it has.
        for (tied, resolves) in [(true, false), (false, false), (false, true)] {
            assert_eq!(
                decide(&table(Some((id, tied)), resolves, &[]), false),
                Rekey::NoPrimaryKey { recorded: cols(id) }
            );
        }
        // No record: keyed by its row identity, as before.
        assert_eq!(decide(&table(None, false, &[]), false), Rekey::Keep);

        // A Trellis target keeps its definition's key: tied again when it
        // has its index, refused without, whatever primary key it has.
        let target = |tied, resolves, pk| TableKey {
            trellis_target: true,
            ..table(Some((&["ga"], tied)), resolves, pk)
        };
        assert_eq!(decide(&target(true, true, &[]), false), Rekey::Keep);
        assert_eq!(
            decide(&target(false, true, &[]), false),
            Rekey::Record(cols(&["ga"]))
        );
        for tied in [true, false] {
            assert_eq!(
                decide(&target(tied, false, &["n"]), false),
                Rekey::Missing {
                    recorded: cols(&["ga"])
                }
            );
        }
    }

    /// A resume outcome's re-records ([`write_rerecords`]) of two records,
    /// the second held by another transaction (a resume that wrote it and
    /// is about to write the first): they fail at once, rather than hold
    /// the first while they wait for the second, which would deadlock with
    /// that resume.
    #[tokio::test]
    async fn rerecords_fail_at_once_on_a_record_another_transaction_holds() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        let mut holder = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table public.a (x int primary key); \
                 create table public.b (x int primary key); \
                 insert into public.definition_keys values \
                     ('public.a', '{y}', 'public.a'::regclass, 0), \
                     ('public.b', '{y}', 'public.b'::regclass, 0)",
            )
            .await
            .expect("the tables and their records");
        let held = holder.transaction().await.expect("begin");
        held.batch_execute(
            "update public.definition_keys set key_columns = '{x}' where table_name = 'public.b'",
        )
        .await
        .expect("hold b's record");
        let rerecords = ["public.a", "public.b"].map(|table| Rerecord {
            table: table.to_string(),
            old: vec!["y".to_string()],
            columns: vec!["x".to_string()],
        });
        let txn = client.transaction().await.expect("begin");
        let written = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            write_rerecords(&txn, &rerecords),
        )
        .await
        .expect("the re-records don't wait for b's record");
        let err = written.expect_err("b's record is held");
        assert_eq!(
            err.code(),
            Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
            "{err}"
        );
        txn.rollback().await.expect("rollback");
        held.rollback().await.expect("release b's record");
    }
}
