//! The key space Trellis chose for each table a definition keys by
//! (`definition_keys`, #1719).
//!
//! A table's row identity ([`super::ddl::identity_key_columns`]) is a choice
//! among its unique indexes. Define makes it once per table and records it:
//! the target's key (an aggregate's or `JOIN`'s group columns, a 1-1
//! target's source key), and the source's and a `JOIN`'s joined table's
//! row-identity key as define admitted it ([`record`]). From then on the
//! identity is the index over exactly the recorded columns, so a user's unique
//! index or constraint on the same table, older than the key after a `REINDEX
//! CONCURRENTLY` or created first on an adopted or restored table, never
//! becomes the key.
//!
//! With no index over the recorded columns (dropped, or never created by a
//! restore), the table has no identity: a resume and a build refuse, naming
//! the table, the recorded key and the indexes it has ([`check`],
//! [`super::ddl::DdlError::RecordedKeyMissing`]).
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

/// Records, for definition `id`, the key of each table it keys by: its
/// qualified `source`, a `JOIN`'s qualified `joined` table (each its
/// row-identity key now, which is the recorded one when another definition
/// already keys by the table) and its qualified `target` ([`target_key`]). A
/// table already recorded for `id` keeps its record, and an empty key is not
/// recorded.
pub(crate) async fn record(
    client: &impl GenericClient,
    id: i64,
    def: &TransformDef,
    source: &str,
    joined: Option<&str>,
    target: &str,
) -> Result<(), tokio_postgres::Error> {
    let source_key = key_names(client, source).await?;
    let mut keys = vec![
        (target.to_string(), target_key(def, &source_key)),
        (source.to_string(), source_key),
    ];
    if let Some(joined) = joined {
        keys.push((joined.to_string(), key_names(client, joined).await?));
    }
    for (table, key) in keys {
        if key.is_empty() {
            continue;
        }
        client
            .execute(
                "insert into definition_keys (transform_id, table_name, key_columns) \
                 values ($1, $2, $3) on conflict (transform_id, table_name) do nothing",
                &[&id, &table, &key],
            )
            .await?;
    }
    Ok(())
}

/// The key recorded for `table` (an unquoted `schema.table`), if any, the
/// way [`ddl::identity_key_columns`] reads it.
async fn recorded_for(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<Vec<String>>, tokio_postgres::Error> {
    Ok(client
        .query_opt(
            "select key_columns from definition_keys where table_name = $1 \
             order by transform_id limit 1",
            &[&table],
        )
        .await?
        .map(|row| row.get(0)))
}

/// `table`'s unique indexes, oldest first, each as `name (key columns)` with
/// its `INCLUDE` columns and whatever keeps it from being a row identity.
async fn unique_indexes(
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
                        case when not i.indimmediate then ', deferrable' end)) \
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

/// The refusal for `table` (an unquoted `schema.table`) when Trellis
/// recorded a key for it and it has no index over that key now: the table,
/// the recorded key and the unique indexes it has. `None` when it has one,
/// has no record, or doesn't exist (its own check reports that).
pub(crate) async fn missing(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<DdlError>, tokio_postgres::Error> {
    let Some(recorded) = recorded_for(client, table).await? else {
        return Ok(None);
    };
    let exists: bool = client
        .query_typed_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[(&ddl::regclass_arg(table), Type::TEXT)],
        )
        .await?
        .get(0);
    if !exists || !ddl::identity_key_columns(client, table).await?.is_empty() {
        return Ok(None);
    }
    Ok(Some(DdlError::RecordedKeyMissing {
        table: table.to_string(),
        indexes: unique_indexes(client, table).await?,
        recorded,
    }))
}

/// Refuses definition `id` when a table it keys by has no index over the key
/// recorded for it ([`missing`]): a resume's re-validation and a build's
/// start run it, so the definition is never keyed by a guess.
pub(crate) async fn check(client: &impl GenericClient, id: i64) -> Result<(), CatalogError> {
    let tables: Vec<String> = client
        .query(
            "select table_name from definition_keys where transform_id = $1 \
             order by table_name",
            &[&id],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for table in tables {
        if let Some(err) = missing(client, &table).await? {
            return Err(CatalogError::Ddl(err));
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

/// Records the keys of every stored definition that has none
/// (`definition_keys`): one a release before the record existed stored. Run
/// at attach ([`super::grammar::pause_unloadable_in`]), in `txn`, after the
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
/// `capture_failure` naming them ([`included_key_reason`]). Returns how many
/// it paused.
pub(crate) async fn backfill_in(
    txn: &tokio_postgres::Transaction<'_>,
    floors: &Floors,
    oldest: i32,
    current: i32,
) -> Result<usize, CatalogError> {
    let rows = txn
        .query(
            "select d.id, d.source_table, d.target_table, d.join_table, d.definition_text, \
                    d.grammar_version, d.floor_version \
             from transform_definitions d \
             where not exists (select 1 from definition_keys k where k.transform_id = d.id) \
             order by d.id",
            &[],
        )
        .await?;
    let mut paused = 0;
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
        let Ok(def) = super::grammar::load_in(text, grammar_version, oldest, current) else {
            continue;
        };
        record(txn, id, &def, &source, joined.as_deref(), &target).await?;
        let mut included = Vec::new();
        for table in std::iter::once(&source).chain(joined.as_ref()) {
            let key = key_names(txn, table).await?;
            let columns = included_columns(txn, table, &key).await?;
            if !columns.is_empty() {
                included.push((table.clone(), columns));
            }
        }
        if let Some((table, columns)) = included.first() {
            let reason = included_key_reason(&target, table, columns);
            if super::lifecycle::pause_for_included_key(txn, id, &source, &reason).await? {
                paused += 1;
            }
        }
    }
    Ok(paused)
}

/// Re-keys a 1-1 target (plain or `JOIN`) an earlier release built over its
/// source key's `INCLUDE` columns (#1722), in `txn`, before a resume
/// re-validates it: the target's key constraint over the old identity (the
/// recorded key followed by the source key index's `INCLUDE` columns) is
/// replaced in place by one over the recorded key, and each former key column
/// the definition doesn't create (not a field) is dropped. The table, its
/// rows, the user's indexes and grants stay. Its ledger's entries, keyed by
/// the old key's text, are deleted: the rebuild re-derives every row, and
/// its sweep removes the target rows no source row backs.
///
/// Does nothing unless the target has no index over its recorded key and has
/// exactly that old key. Refuses, naming them, when an object other than the
/// key depends on a column it would drop: a user's view or index.
pub(crate) async fn repair_target_key(
    txn: &tokio_postgres::Transaction<'_>,
    definition: &Definition,
    source: &str,
) -> Result<bool, CatalogError> {
    if !matches!(
        definition.def.key_space,
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_)
    ) {
        return Ok(false);
    }
    let target = definition.target_table.as_str();
    let Some(recorded) = txn
        .query_opt(
            "select key_columns from definition_keys \
             where transform_id = $1 and table_name = $2",
            &[&definition.id, &target],
        )
        .await?
        .map(|row| row.get::<_, Vec<String>>(0))
    else {
        return Ok(false);
    };
    if !ddl::identity_key_columns(txn, target).await?.is_empty() {
        return Ok(false);
    }
    let source_key = key_names(txn, source).await?;
    let included = included_columns(txn, source, &source_key).await?;
    if included.is_empty() || source_key != recorded {
        return Ok(false);
    }
    let old: Vec<String> = recorded.iter().chain(&included).cloned().collect();
    // The target's key constraint over exactly the old identity.
    let Some(row) = txn
        .query_typed_opt(
            "select c.oid, c.conname::text, c.conindid \
             from pg_catalog.pg_constraint c \
             join pg_catalog.pg_index i on i.indexrelid = c.conindid \
             where c.conrelid = pg_catalog.to_regclass($1) and c.contype in ('p', 'u') \
               and array(select a.attname::text \
                         from unnest(i.indkey::int2[]) with ordinality k(attnum, ord) \
                         join pg_catalog.pg_attribute a \
                           on a.attrelid = i.indrelid and a.attnum = k.attnum \
                         where k.ord <= i.indnkeyatts \
                         order by k.ord) = $2 \
             order by c.contype, c.oid limit 1",
            &[
                (&ddl::regclass_arg(target), Type::TEXT),
                (&old, Type::TEXT_ARRAY),
            ],
        )
        .await?
    else {
        return Ok(false);
    };
    let (constraint_oid, constraint, index_oid): (u32, String, u32) =
        (row.get(0), row.get(1), row.get(2));
    let fields: Vec<&str> = definition
        .def
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    let dropped: Vec<&String> = included
        .iter()
        .filter(|c| !fields.contains(&c.as_str()) && !recorded.contains(c))
        .collect();
    for column in &dropped {
        // Every object depending on the column but the key constraint, its
        // index and the column's own `NOT NULL`: a user's view (through its
        // rewrite rule), index, constraint or statistics object.
        let blocking: Vec<String> = txn
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
                column: (*column).clone(),
                dependents: blocking,
            });
        }
    }
    let nullable: bool = txn
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
    let mut actions = vec![
        format!("drop constraint {}", quote_ident(&constraint)),
        format!(
            "add {} ({})",
            if nullable {
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

/// For a unit test on a bare database, which has no catalog: an empty
/// `definition_keys` on its `search_path`, so
/// [`ddl::identity_key_columns`] reads no record.
#[cfg(test)]
pub(crate) const TEST_TABLE_DDL: &str = "create table if not exists public.definition_keys \
     (transform_id bigint, table_name text, key_columns text[])";

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
            "insert into public.definition_keys values (1, 'public.t', '{a}')",
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
                 insert into public.definition_keys values (1, 'public.t', '{a}'); \
                 create unique index t_b on public.t (b) include (c) nulls not distinct; \
                 create unique index t_a_part on public.t (a) where c > 0; \
                 create unique index t_expr on public.t ((a + b))",
            )
            .await
            .expect("the table");
        assert!(key(&client, "public.t").await.is_empty());
        let refusal = missing(&client, "public.t")
            .await
            .expect("check")
            .expect("refused")
            .to_string();
        for part in [
            "table 'public.t'",
            "at define (a)",
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
}
