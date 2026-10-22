//! What a captured table's read columns looked like when Trellis last
//! looked, and the capture pass's response to a change no capture trigger
//! saw (#703 R1 and R3b).
//!
//! Capture knows a column by its name, and two kinds of DDL change what a
//! column holds without firing a DML trigger or changing its name:
//!
//! - **R1, the column's identity.** A column dropped and re-added under the
//!   same name has a new `attnum`, and every row holds its new default. A
//!   retype (`ALTER COLUMN ... TYPE`) can change every value's text: a
//!   `numeric` scale change re-renders `1.50` as `1.5000` or rounds it, a
//!   narrower `varchar` drops trailing spaces, `integer` to `double
//!   precision` changes what a definition can do with it. A pure widening
//!   ([`key_types::pure_widening`]: a wider integer, a longer string bound,
//!   more `numeric` precision at the same scale or none, a wider temporal or
//!   `interval` precision) keeps every value's text, and is acted on here
//!   only when it takes a `numeric`'s declared scale away, which a reader
//!   may need; a copy Trellis made that it outgrows is
//!   `pause_readers_of_retyped`'s.
//! - **R3b, a same-type value rewrite.** `ALTER COLUMN b TYPE integer USING
//!   b * 10` on an `integer` column rewrites the table with no DML trigger,
//!   and leaves the column's name, `attnum`, type and modifier as they were.
//!   It does move two things: the `xmin` of the column's `pg_attribute`
//!   row, and the table's file node. Each moves alone in routine work
//!   (`SET DEFAULT`, `SET NOT NULL` and `SET STATISTICS` move the row;
//!   `VACUUM FULL`, `CLUSTER`, pg_repack and a volatile `ADD COLUMN` move the
//!   file node), and both together when a metadata `ALTER` and a rewrite
//!   land in the same pass window, as framework migrations often do. So R3b
//!   also requires the rest of the row, with the column's default
//!   expression, to be unchanged ([`Column::hash`]): a rewrite that changed
//!   nothing else about the column. A catalog freeze doesn't change the
//!   `xmin` a query reads.
//!
//! # The baseline
//!
//! `capture_baselines` keeps one row per column each node images: the table
//! itself, or each leaf of a partitioned root, which has no storage of its
//! own (a `USING` on the root rewrites every leaf). An install, widen or
//! narrow records it in its own transaction ([`record`]): rows for the
//! columns and leaves its spec newly covers, and none for the columns it no
//! longer images. A column it already covered keeps its row, so a change
//! made before the install landed is still compared by the next pass. A
//! table captured with no baseline at all (its capture restored from a
//! dump; the baseline is runtime state a dump leaves out) gets one from the
//! next pass.
//!
//! # The pass's check
//!
//! Before it regenerates a table's functions, the capture pass compares the
//! live catalog with the baseline ([`check`]), and writes what it read back,
//! in the transaction of its response, for every row that differs: so a row
//! or a file node that moved alone becomes the next pass's baseline, and a
//! response that fails is tried again. A node or a column gone since is
//! forgotten (a column renamed or dropped is
//! `staging::schema_change::pause_readers_of_missing`'s).
//!
//! The response follows the definition's validity (the #703/#707 decision):
//!
//! - **R3b, and R1 where every definition reading the column is still valid
//!   as written:** only data was missed. The pass rebuilds every applying
//!   reader of the table, exactly what `Trellis::request_backfill(table)`
//!   does (`intake::markers::park_table_catch_ups`), and pauses nobody. It
//!   logs a `WARN` naming the table, the cause and the readers it rebuilt,
//!   and counts `capture_auto_rebuilds_total{source_table, cause}`
//!   ([`CaptureRebuildCause`]).
//! - **R1 where a definition reading the column may no longer be valid:**
//!   define would now refuse it (`defs::catalog::revalidate`: a `SUM` over
//!   a `numeric` that lost its declared scale), a column Trellis created for
//!   it from the column was outgrown by an integer or float width (a copy
//!   has no type modifier, ADR-0020, so no modifier change outgrows one), or
//!   the value type it evaluates the column as changed (`integer` to
//!   `double precision`). The pass pauses that definition with a
//!   `capture_failure` naming the change; `RESUME` re-validates, re-types
//!   and rebuilds. A widening that only took a declared scale away, and
//!   left every reader valid, changed no value: nothing rebuilds.
//!
//! R1's type check leaves alone a definition that uses the column as a
//! key: `pause_readers_of_retyped` owns a key column's type (#760). A key
//! column dropped and re-added is R1's.
//!
//! # A type renamed
//!
//! A type renamed or moved to another schema keeps its oid and its values,
//! so it is none of the above: [`follow_renamed_types`] re-records its name,
//! before the key-type check compares names, and nothing pauses or rebuilds.
//!
//! # What it misses, and what it rebuilds for nothing
//!
//! A metadata `ALTER` (`SET DEFAULT`, `SET STATISTICS`) on the same column,
//! in the same pass window as a `USING` rewrite of it, changes the row the
//! hash covers, so R3b misses the rewrite. So does the first rewrite of a
//! column added with a constant default: its row keeps that default for the
//! rows that predate it (`atthasmissing`) until the table is rewritten, and
//! any rewrite clears it, `VACUUM FULL` included, which is why the hash
//! can't leave it out. An identity rewrite (`USING
//! b + 0`), or an identical `SET DEFAULT` re-applied in the same window as a
//! rewrite, rebuilds the readers for nothing, as does a column retyped and
//! back between two passes (two rewrites that leave the row as it was).
//!
//! A pure widening with a `USING` that changes values (`integer` to `bigint
//! USING b * 10`) is never R1's, and its row changed, so it isn't R3b's
//! either: only a column Trellis created that it outgrows pauses its
//! readers. These are in `docs/known-correctness-gaps.md`.

use std::collections::{BTreeMap, BTreeSet};

use tokio_postgres::types::Type;
use tokio_postgres::{Client, GenericClient};

use super::CaptureError;
use super::columns::{CaptureCatalog, readers_of};
use crate::defs::ast::ValueType;
use crate::defs::catalog::CatalogError;
use crate::defs::key_types::{self, ColumnType};
use crate::metrics::CaptureRebuildCause;

/// One column of one node, as the catalog has it, or as the baseline
/// recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Column {
    /// The node's `pg_relation_filenode`.
    filenode: Option<u32>,
    attnum: i16,
    type_oid: u32,
    ty: ColumnType,
    /// `format_type`'s rendering, for messages. Not compared.
    display: String,
    /// The `xmin` of the column's `pg_attribute` row, as text: compared
    /// for equality only.
    xmin: String,
    /// A hash of the column's whole `pg_attribute` row and its default
    /// expression. The expression's `:location` fields (parse offsets into
    /// the statement that set it) are left out: PostgreSQL 16 and earlier
    /// store a column's default again on every `ALTER COLUMN ... TYPE`, with
    /// each location reset to -1, so the same expression would otherwise
    /// read as a changed one and a same-type rewrite of a column with a
    /// default would never be acted on.
    hash: String,
}

impl Column {
    fn retyped(&self, now: &Column) -> bool {
        (self.type_oid, self.ty.typmod) != (now.type_oid, now.ty.typmod)
    }
}

/// Columns by node oid and name.
type Columns = BTreeMap<(u32, String), Column>;

/// The live state of `columns` on every node of the captured table `$1`
/// (an unquoted `schema.table`): the table itself, or every leaf under a
/// partitioned root, walked through `pg_inherits` (not `pg_partition_tree`,
/// which locks every node).
const LIVE_COLUMNS: &str = "\
    with recursive tree(relid, relkind) as ( \
        select c.oid, c.relkind from pg_catalog.pg_class c \
        where c.oid = pg_catalog.to_regclass($1) \
        union all \
        select i.inhrelid, c.relkind from tree t \
        join pg_catalog.pg_inherits i on i.inhparent = t.relid and not i.inhdetachpending \
        join pg_catalog.pg_class c on c.oid = i.inhrelid \
        where t.relkind = 'p') \
    select t.relid, pg_catalog.pg_relation_filenode(t.relid), a.attname::text, a.attnum, \
           a.atttypid, n.nspname::text || '.' || ty.typname::text, a.atttypmod, \
           pg_catalog.format_type(a.atttypid, a.atttypmod), a.xmin::text, \
           pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to( \
               pg_catalog.to_jsonb(a)::text || coalesce(pg_catalog.regexp_replace( \
                   d.adbin::text, ' :location -?[0-9]+', '', 'g'), ''), 'UTF8')), 'hex') \
    from tree t \
    join pg_catalog.pg_attribute a \
      on a.attrelid = t.relid and a.attnum > 0 and not a.attisdropped \
     and a.attname = any($2) \
    join pg_catalog.pg_type ty on ty.oid = a.atttypid \
    join pg_catalog.pg_namespace n on n.oid = ty.typnamespace \
    left join pg_catalog.pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum \
    where t.relkind <> 'p'";

async fn live(
    client: &impl GenericClient,
    table: &str,
    columns: &[String],
) -> Result<Columns, tokio_postgres::Error> {
    Ok(client
        .query_typed(
            LIVE_COLUMNS,
            &[
                (&crate::defs::ddl::regclass_arg(table), Type::TEXT),
                (&columns, Type::TEXT_ARRAY),
            ],
        )
        .await?
        .into_iter()
        .map(|row| {
            (
                (row.get(0), row.get(2)),
                Column {
                    filenode: row.get(1),
                    attnum: row.get(3),
                    type_oid: row.get(4),
                    ty: ColumnType {
                        type_name: row.get(5),
                        typmod: row.get(6),
                    },
                    display: row.get(7),
                    xmin: row.get(8),
                    hash: row.get(9),
                },
            )
        })
        .collect())
}

async fn recorded(client: &impl GenericClient, table: &str) -> Result<Columns, CaptureError> {
    Ok(client
        .query_typed(
            "select node, column_name, filenode, attnum, type_oid, type_name, typmod, \
                    coalesce(pg_catalog.format_type(type_oid, typmod), type_name), \
                    attr_xmin, attr_hash \
             from capture_baselines where table_name = $1",
            &[(&table, Type::TEXT)],
        )
        .await?
        .into_iter()
        .map(|row| {
            (
                (row.get(0), row.get(1)),
                Column {
                    filenode: row.get(2),
                    attnum: row.get(3),
                    type_oid: row.get(4),
                    ty: ColumnType {
                        type_name: row.get(5),
                        typmod: row.get(6),
                    },
                    display: row.get(7),
                    xmin: row.get(8),
                    hash: row.get(9),
                },
            )
        })
        .collect())
}

/// Writes `rows` as `table`'s baseline rows, replacing any for the same node
/// and column.
async fn write(
    client: &impl GenericClient,
    table: &str,
    rows: &Columns,
) -> Result<(), tokio_postgres::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut nodes: Vec<u32> = Vec::with_capacity(rows.len());
    let mut names: Vec<&str> = Vec::with_capacity(rows.len());
    let mut filenodes: Vec<Option<u32>> = Vec::with_capacity(rows.len());
    let mut attnums: Vec<i16> = Vec::with_capacity(rows.len());
    let mut type_oids: Vec<u32> = Vec::with_capacity(rows.len());
    let mut type_names: Vec<&str> = Vec::with_capacity(rows.len());
    let mut typmods: Vec<i32> = Vec::with_capacity(rows.len());
    let mut xmins: Vec<&str> = Vec::with_capacity(rows.len());
    let mut hashes: Vec<&str> = Vec::with_capacity(rows.len());
    for ((node, name), column) in rows {
        nodes.push(*node);
        names.push(name);
        filenodes.push(column.filenode);
        attnums.push(column.attnum);
        type_oids.push(column.type_oid);
        type_names.push(&column.ty.type_name);
        typmods.push(column.ty.typmod);
        xmins.push(&column.xmin);
        hashes.push(&column.hash);
    }
    client
        .execute_typed(
            "insert into capture_baselines as b \
                 (table_name, node, column_name, filenode, attnum, type_oid, type_name, typmod, \
                  attr_xmin, attr_hash) \
             select $1, r.* from unnest($2::oid[], $3::text[], $4::oid[], $5::int2[], \
                 $6::oid[], $7::text[], $8::int4[], $9::text[], $10::text[]) as r \
             on conflict (table_name, node, column_name) do update set \
                 filenode = excluded.filenode, attnum = excluded.attnum, \
                 type_oid = excluded.type_oid, type_name = excluded.type_name, \
                 typmod = excluded.typmod, attr_xmin = excluded.attr_xmin, \
                 attr_hash = excluded.attr_hash",
            &[
                (&table, Type::TEXT),
                (&nodes, Type::OID_ARRAY),
                (&names, Type::TEXT_ARRAY),
                (&filenodes, Type::OID_ARRAY),
                (&attnums, Type::INT2_ARRAY),
                (&type_oids, Type::OID_ARRAY),
                (&type_names, Type::TEXT_ARRAY),
                (&typmods, Type::INT4_ARRAY),
                (&xmins, Type::TEXT_ARRAY),
                (&hashes, Type::TEXT_ARRAY),
            ],
        )
        .await?;
    Ok(())
}

/// Deletes `table`'s baseline rows for each of `gone`.
async fn delete(
    client: &impl GenericClient,
    table: &str,
    gone: &[(u32, String)],
) -> Result<(), tokio_postgres::Error> {
    if gone.is_empty() {
        return Ok(());
    }
    let (nodes, names): (Vec<u32>, Vec<&str>) = gone
        .iter()
        .map(|(node, name)| (*node, name.as_str()))
        .unzip();
    client
        .execute_typed(
            "delete from capture_baselines b \
             using unnest($2::oid[], $3::text[]) as g(node, column_name) \
             where b.table_name = $1 and b.node = g.node and b.column_name = g.column_name",
            &[
                (&table, Type::TEXT),
                (&nodes, Type::OID_ARRAY),
                (&names, Type::TEXT_ARRAY),
            ],
        )
        .await?;
    Ok(())
}

/// Records `table`'s baseline for the `columns` its capture images, in the
/// transaction of the install, widen or narrow that installs them (see "The
/// baseline"): a row for each node and column it has none for, and none for
/// a column it no longer images or a node gone. A row it already has is
/// kept, for the next pass to compare.
pub(crate) async fn record(
    client: &impl GenericClient,
    table: &str,
    columns: &[String],
) -> Result<(), tokio_postgres::Error> {
    let live = live(client, table, columns).await?;
    let nodes: Vec<u32> = live.keys().map(|(node, _)| *node).collect();
    client
        .execute_typed(
            "delete from capture_baselines \
             where table_name = $1 and (column_name <> all($2) or node <> all($3))",
            &[
                (&table, Type::TEXT),
                (&columns, Type::TEXT_ARRAY),
                (&nodes, Type::OID_ARRAY),
            ],
        )
        .await?;
    let kept: BTreeSet<(u32, String)> = client
        .query_typed(
            "select node, column_name from capture_baselines where table_name = $1",
            &[(&table, Type::TEXT)],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let new: Columns = live
        .into_iter()
        .filter(|(key, _)| !kept.contains(key))
        .collect();
    write(client, table, &new).await
}

/// Forgets `table`'s baseline: its capture is uninstalled.
pub(crate) async fn forget(
    client: &impl GenericClient,
    table: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute_typed(
            "delete from capture_baselines where table_name = $1",
            &[(&table, Type::TEXT)],
        )
        .await?;
    Ok(())
}

/// Forgets the baseline of every table but `keep`.
pub(crate) async fn forget_except(
    client: &impl GenericClient,
    keep: &[&str],
) -> Result<(), tokio_postgres::Error> {
    client
        .execute_typed(
            "delete from capture_baselines where table_name <> all($1)",
            &[(&keep, Type::TEXT_ARRAY)],
        )
        .await?;
    Ok(())
}

/// Re-records the name of a type renamed or moved to another schema since
/// the baseline (`ALTER TYPE ... RENAME TO`, `... SET SCHEMA`), in place
/// (#1204): a read column whose type oid and modifier are as the baseline
/// recorded and whose `schema.typname` is not. The type's values are what
/// they were, so nothing pauses, rebuilds or reinstalls; Trellis only has to
/// cast by the name the type has now.
///
/// A type's name is persisted in each definition's `source_columns` and
/// `join_columns` (as `enum:schema.typname`), in `definition_key_types`, and
/// in the baseline itself. They are rewritten in one transaction, for every
/// definition that records the old name for a column whose type is still
/// the renamed one (the rename is the type's, not this table's, and a
/// chained definition's key columns on a table Trellis creates have no
/// baseline of their own; a column of a new type created at the old name
/// keeps it), under those definitions' row locks taken in id order, the
/// locks a pass takes for any re-record. Each of them also asks for the
/// release of its keys held for `42704`, the failure of a cast by the name
/// the type no longer has (`retype_releases`, released by
/// `staging::quarantine::release_retyped_keys` after the pass).
///
/// Runs before `pause_readers_of_retyped`, which compares the name recorded
/// for a key column with the live one. A column whose oid or modifier moved
/// is not a rename: [`check`] and that function respond to it.
pub(crate) async fn follow_renamed_types(
    client: &mut Client,
    table: &str,
) -> Result<(), CaptureError> {
    let baseline = recorded(&*client, table).await?;
    if baseline.is_empty() {
        return Ok(());
    }
    let columns: Vec<String> = baseline
        .keys()
        .map(|(_, name)| name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let live = live(&*client, table, &columns).await?;
    // By type oid: a new type can take a renamed type's old name.
    let mut renamed: BTreeMap<u32, (String, String)> = BTreeMap::new();
    for (key, was) in &baseline {
        if let Some(now) = live.get(key)
            && (was.type_oid, was.ty.typmod) == (now.type_oid, now.ty.typmod)
            && was.ty.type_name != now.ty.type_name
        {
            renamed.insert(
                was.type_oid,
                (was.ty.type_name.clone(), now.ty.type_name.clone()),
            );
        }
    }
    if renamed.is_empty() {
        return Ok(());
    }
    // Whether column `{column}` of `{table}` (an unquoted `schema.table`)
    // still has the renamed type `{oid}`: a column of a new type created at
    // the old name keeps that name.
    const ITS_COLUMN: &str = "exists (select 1 from pg_catalog.pg_attribute a \
         join pg_catalog.pg_class c on c.oid = a.attrelid \
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
         where n.nspname::text || '.' || c.relname::text = {table} \
           and a.attname::text = {column} and a.attnum > 0 and not a.attisdropped \
           and a.atttypid = {oid})";
    let its_column = |table: &str, column: &str, oid: &str| {
        ITS_COLUMN
            .replace("{table}", table)
            .replace("{column}", column)
            .replace("{oid}", oid)
    };
    let txn = client.transaction().await?;
    for (oid, (old, new)) in &renamed {
        let (old_token, new_token) = (format!("enum:{old}"), format!("enum:{new}"));
        let in_source = its_column("d.source_table", "e.key", "$3");
        let in_join = its_column("d.join_table", "e.key", "$3");
        let candidates: Vec<i64> = txn
            .query(
                &format!(
                    "select id from transform_definitions d \
                     where exists (select 1 from jsonb_each_text(d.source_columns) e \
                                   where e.value = $1 and {in_source}) \
                        or exists (select 1 from jsonb_each_text(d.join_columns) e \
                                   where e.value = $1 and {in_join}) \
                        or exists (select 1 from definition_key_types k \
                                   where k.transform_id = d.id and k.type_name = $2 \
                                     and {})",
                    its_column("k.table_name", "k.column_name", "$3"),
                ),
                &[&old_token, old, oid],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        // Locked in id order; one dropped meanwhile is left out.
        let owners: Vec<i64> = if candidates.is_empty() {
            Vec::new()
        } else {
            txn.query(
                "select id from transform_definitions where id = any($1) order by id for update",
                &[&candidates],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect()
        };
        if !owners.is_empty() {
            let rename = |table: &str, entries: &str| {
                format!(
                    "(select coalesce(jsonb_object_agg(e.key, \
                         case when e.value = to_jsonb($2::text) and {} \
                              then to_jsonb($3::text) else e.value end), '{{}}'::jsonb) \
                      from jsonb_each({entries}) e)",
                    its_column(table, "e.key", "$4"),
                )
            };
            txn.execute(
                &format!(
                    "update transform_definitions d set \
                         source_columns = {}, \
                         join_columns = case when d.join_columns is null then null else {} end \
                     where d.id = any($1)",
                    rename("d.source_table", "d.source_columns"),
                    rename("d.join_table", "d.join_columns"),
                ),
                &[&owners, &old_token, &new_token, oid],
            )
            .await?;
            txn.execute(
                &format!(
                    "update definition_key_types k set type_name = $2 \
                     where k.transform_id = any($1) and k.type_name = $4 and {}",
                    its_column("k.table_name", "k.column_name", "$3"),
                ),
                &[&owners, new, oid, old],
            )
            .await?;
            txn.execute(
                "insert into retype_releases (transform_id, sqlstate) \
                 select id, '42704' from unnest($1::int8[]) as id \
                 on conflict (transform_id, sqlstate) do update set requested_at = now()",
                &[&owners],
            )
            .await?;
        }
        txn.execute(
            "update capture_baselines set type_name = $3 \
             where table_name = $1 and type_name = $2 and type_oid = $4",
            &[&table, old, new, oid],
        )
        .await?;
        crate::instance_log::info!(
            table = %table,
            from = %old,
            to = %new,
            definitions = owners.len(),
            "a type was renamed; recorded its new name"
        );
    }
    txn.commit().await?;
    Ok(())
}

/// What changed about one column since the baseline, on any of its nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Change {
    /// R3b: its values were rewritten in place.
    Rewritten,
    /// R1: a pure widening that takes away the declared scale of a `numeric`
    /// (`numeric(10,2)` to `numeric`): every value keeps its text, so nothing
    /// rebuilds, but a reader that needs the scale (a `SUM`) may no longer be
    /// valid, which only its re-validation tells.
    Widened { from: String, to: String },
    /// R1: its type or modifier changed, not by a pure widening, from the
    /// first rendering to the second.
    Retyped { from: String, to: String },
    /// R1: it was dropped and re-added, with its type before and now.
    Readded { from: String, to: String },
}

impl Change {
    /// Whether the column's type changed (not a pure widening).
    fn retyped(&self) -> bool {
        match self {
            Change::Rewritten => false,
            Change::Widened { .. } | Change::Retyped { .. } => true,
            Change::Readded { from, to } => from != to,
        }
    }

    /// The stronger of two changes seen on different nodes.
    fn rank(&self) -> u8 {
        match self {
            Change::Rewritten => 0,
            Change::Widened { .. } => 1,
            Change::Retyped { .. } => 2,
            Change::Readded { .. } => 3,
        }
    }
}

/// How a baseline row compares with the live column, by the module doc's
/// rules: `None` when nothing a reader depends on changed.
fn compare(was: &Column, now: &Column) -> Option<Change> {
    if was.attnum != now.attnum {
        return Some(Change::Readded {
            from: was.display.clone(),
            to: now.display.clone(),
        });
    }
    if was.retyped(now) {
        if key_types::pure_widening(&was.ty, &now.ty) {
            return key_types::loses_declared_scale(&was.ty, &now.ty).then(|| Change::Widened {
                from: was.display.clone(),
                to: now.display.clone(),
            });
        }
        return Some(Change::Retyped {
            from: was.display.clone(),
            to: now.display.clone(),
        });
    }
    (was.xmin != now.xmin && was.filenode != now.filenode && was.hash == now.hash)
        .then_some(Change::Rewritten)
}

/// What [`compare_all`] found: each changed column's change, the rows to
/// write back and the rows to forget.
#[derive(Debug, Default)]
struct Found {
    changes: BTreeMap<String, Change>,
    rewrite: Columns,
    gone: Vec<(u32, String)>,
}

fn compare_all(baseline: &Columns, live: &Columns) -> Found {
    let mut found = Found::default();
    for (key, was) in baseline {
        let Some(now) = live.get(key) else {
            found.gone.push(key.clone());
            continue;
        };
        if was == now {
            continue;
        }
        found.rewrite.insert(key.clone(), now.clone());
        if let Some(change) = compare(was, now) {
            let entry = found.changes.entry(key.1.clone()).or_insert(change.clone());
            if change.rank() > entry.rank() {
                *entry = change;
            }
        }
    }
    found
}

/// What one reader of a changed column needs (see the module doc).
enum Response {
    Rebuild,
    Pause(String),
}

/// The capture pass's check of `table`'s read columns against their
/// baseline (see the module doc). Returns whether it paused any definition,
/// in which case the caller leaves the table for the next pass, whose
/// catalog no longer counts it. A rebuild alone doesn't: the pass goes on to
/// bring the table's capture current.
///
/// Costs two catalog reads for a table whose columns didn't change.
pub(crate) async fn check(
    client: &mut Client,
    schema: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let baseline = recorded(&*client, table).await?;
    if baseline.is_empty() {
        // Capture installed with no baseline (restored from a dump, whose
        // functions and triggers come back as they were): record one now,
        // for the next pass to compare.
        if let super::install::Installed::Complete { spec, .. } =
            super::install::installed(&*client, schema, table).await?
        {
            record(&*client, table, spec.columns()).await?;
        }
        return Ok(false);
    }
    let columns: Vec<String> = baseline
        .keys()
        .map(|(_, name)| name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let live = live(&*client, table, &columns).await?;
    let found = compare_all(&baseline, &live);
    if found.rewrite.is_empty() && found.gone.is_empty() {
        return Ok(false);
    }

    let txn = client.transaction().await?;
    let mut pauses: BTreeMap<i64, Vec<(String, String)>> = BTreeMap::new();
    let mut causes: BTreeSet<CaptureRebuildCause> = BTreeSet::new();
    if !found.changes.is_empty() {
        let readers: Vec<i64> = readers_of(catalog, table, &BTreeSet::new(), true);
        if !readers.is_empty() {
            txn.execute(
                "select 1 from transform_definitions where id = any($1) order by id for update",
                &[&readers],
            )
            .await?;
        }
        let key = if found.changes.values().any(Change::retyped) {
            crate::capture::columns::live_columns(&txn, table).await?.1
        } else {
            Vec::new()
        };
        for (column, change) in &found.changes {
            if *change == Change::Rewritten {
                causes.insert(CaptureRebuildCause::ValueRewrite);
                continue;
            }
            let read_by = readers_of(catalog, table, &BTreeSet::from([column.clone()]), false);
            for reader in catalog
                .definitions
                .iter()
                .filter(|r| !r.capture_failed && read_by.contains(&r.id))
            {
                match respond(&txn, schema, catalog, reader, table, &key, column, change).await? {
                    Some(Response::Rebuild) => {
                        causes.insert(CaptureRebuildCause::ColumnChanged);
                    }
                    Some(Response::Pause(error)) => {
                        pauses
                            .entry(reader.id)
                            .or_default()
                            .push((column.clone(), error));
                    }
                    None => {}
                }
            }
        }
    }

    for (&id, reasons) in &pauses {
        let columns: Vec<String> = reasons.iter().map(|(c, _)| c.clone()).collect();
        let reasons: Vec<&str> = reasons.iter().map(|(_, r)| r.as_str()).collect();
        let error = format!(
            "{}. A resume re-validates the definition as define would, and refuses while define \
             would; otherwise it brings the columns Trellis created to the new types and \
             rebuilds it. Or drop the definition and define it again",
            reasons.join("; ")
        );
        if crate::defs::lifecycle::pause_for_capture_failure(
            &txn, id, table, &columns, &error, None,
        )
        .await?
        {
            crate::instance_log::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    let mut rebuilt_targets: Vec<String> = Vec::new();
    if !causes.is_empty() {
        let applying = crate::defs::catalog::applying_readers(&txn, table).await?;
        rebuilt_targets = catalog
            .definitions
            .iter()
            .filter(|r| applying.contains(&r.id))
            .map(|r| r.target.clone())
            .collect();
        let rebuilt =
            crate::intake::markers::park_table_catch_ups(&txn, &[table.to_string()], false).await?;
        crate::staging::build::stamp_rebuild_seg(&txn, &rebuilt).await?;
    }
    write(&txn, table, &found.rewrite).await?;
    delete(&txn, table, &found.gone).await?;
    txn.commit().await?;

    for cause in &causes {
        crate::metrics::increment_capture_auto_rebuilds(table, *cause);
    }
    if !causes.is_empty() {
        let changed: Vec<&str> = found.changes.keys().map(String::as_str).collect();
        let causes: Vec<&str> = causes.iter().map(|c| c.label()).collect();
        crate::instance_log::warn!(
            table = %table,
            cause = %causes.join(","),
            columns = ?changed,
            readers = ?rebuilt_targets,
            "a DDL changed what a read column holds without firing a capture trigger; \
             rebuilding the table's readers"
        );
    }
    Ok(!pauses.is_empty())
}

/// What `reader` needs for `change` to `column` of `table` (R1), by the
/// validity rule: `None` when its key use of the column leaves a type
/// change to `pause_readers_of_retyped`.
#[allow(clippy::too_many_arguments)]
async fn respond(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    catalog: &CaptureCatalog,
    reader: &super::columns::CaptureReader,
    table: &str,
    key: &[String],
    column: &str,
    change: &Change,
) -> Result<Option<Response>, CaptureError> {
    let (from, to, readded) = match change {
        Change::Rewritten => return Ok(Some(Response::Rebuild)),
        Change::Widened { from, to } | Change::Retyped { from, to } => (from, to, false),
        Change::Readded { from, to } => (from, to, true),
    };
    if !change.retyped() {
        return Ok(Some(Response::Rebuild));
    }
    let declared: Vec<&crate::defs::model::RelationshipDefinition> = catalog
        .relationships
        .iter()
        .filter(|r| r.qualified_from_table() == reader.source)
        .collect();
    let to_tables: Vec<String> = declared.iter().map(|r| r.qualified_to_table()).collect();
    let rels: Vec<key_types::RelRef<'_>> = declared
        .iter()
        .zip(&to_tables)
        .map(|(r, to_table)| key_types::RelRef {
            name: &r.def.name,
            from_col: &r.def.from_col,
            to_col: &r.def.to_col,
            to_table,
        })
        .collect();
    let key_use = key_types::key_uses(&reader.def, &reader.source, &rels, table, key)
        .into_iter()
        .any(|(c, _)| c == column);
    if key_use {
        // #760: a key column's type is `pause_readers_of_retyped`'s.
        return Ok(readded.then_some(Response::Rebuild));
    }
    let what = if readded {
        format!(
            "column {column:?} of {table}, which this definition reads, was dropped and re-added \
             as {to} (it was {from})"
        )
    } else {
        format!(
            "column {column:?} of {table}, which this definition reads, changed type from {from} to {to}"
        )
    };

    let Some(definition) = crate::defs::catalog::definition_by_id_in(txn, reader.id).await? else {
        return Ok(None);
    };
    let revalidated = match crate::defs::catalog::revalidate(txn, schema, &definition).await {
        Ok(revalidated) => revalidated,
        Err(err @ (CatalogError::Db(_) | CatalogError::Pool(_))) => return Err(err.into()),
        Err(reason) => {
            return Ok(Some(Response::Pause(format!(
                "{what}, and define would now refuse it: {reason}"
            ))));
        }
    };
    // An integer narrowed (`bigint` to `integer`) evaluates as it did: every
    // value still fits the width recorded.
    let evaluated_as = |ty: Option<&ValueType>| match ty {
        Some(ValueType::Integer(_)) => None,
        other => Some(other.cloned()),
    };
    // #1110: a `JOIN` evaluates its joined table's columns as their types
    // were recorded for it too.
    let (was, now) = if reader.source == table {
        (
            definition.source_columns.get(column).cloned(),
            revalidated.source_columns.get(column).cloned(),
        )
    } else if let Some(sides) = revalidated.join.as_ref().filter(|s| s.joined == table) {
        let recorded = crate::defs::catalog::join_side_of(txn, reader.id)
            .await?
            .and_then(|(_, columns)| columns.get(column).cloned());
        (
            recorded,
            sides.joined_columns.get(column).map(|info| info.value_type),
        )
    } else {
        (None, None)
    };
    if evaluated_as(was.as_ref()) != evaluated_as(now.as_ref()) {
        return Ok(Some(Response::Pause(format!(
            "{what}, which changes the type this definition evaluates it as"
        ))));
    }
    let copies: Vec<crate::defs::copies::TypedCopy> = crate::defs::copies::typed_copies(
        txn,
        schema,
        &reader.def,
        &reader.source,
        &reader.target,
        &declared,
    )
    .await?
    .into_iter()
    .filter(|c| c.columns_of(table).any(|c| c == column))
    .collect();
    // A copy has no type modifier (ADR-0020), so it holds every value of
    // the source column's type whatever its modifier; one an integer or
    // float width outgrew is `pause_readers_of_retyped`'s, which ran first.
    let drifted: Vec<String> = crate::defs::copies::inspect(txn, copies)
        .await?
        .into_iter()
        .filter(|s| s.drifted() && s.outgrown())
        .map(|s| {
            format!(
                "{} ({} now, {} for the new type)",
                s.copy.label(),
                s.copy_type.display,
                s.live_type.display
            )
        })
        .collect();
    if !drifted.is_empty() {
        return Ok(Some(Response::Pause(format!(
            "{what}, so columns Trellis created from it must be re-typed: {}",
            drifted.join(", ")
        ))));
    }
    // A widening that only took away a declared scale changed no value.
    if matches!(change, Change::Widened { .. }) {
        return Ok(None);
    }
    Ok(Some(Response::Rebuild))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(attnum: i16, ty: &str, typmod: i32, xmin: &str, filenode: u32, hash: &str) -> Column {
        Column {
            filenode: Some(filenode),
            attnum,
            type_oid: match ty {
                "int4" => 23,
                "int8" => 20,
                "numeric" => 1700,
                _ => 0,
            },
            ty: ColumnType {
                type_name: format!("pg_catalog.{ty}"),
                typmod,
            },
            display: ty.to_string(),
            xmin: xmin.to_string(),
            hash: hash.to_string(),
        }
    }

    #[test]
    fn a_same_type_rewrite_needs_the_row_and_the_file_node_to_move_and_nothing_else() {
        let was = column(3, "int4", -1, "100", 16384, "h");
        // The `USING` rewrite.
        assert_eq!(
            compare(&was, &column(3, "int4", -1, "200", 16390, "h")),
            Some(Change::Rewritten)
        );
        // `SET DEFAULT` alone, `VACUUM FULL` alone.
        assert_eq!(
            compare(&was, &column(3, "int4", -1, "200", 16384, "h2")),
            None
        );
        assert_eq!(
            compare(&was, &column(3, "int4", -1, "100", 16390, "h")),
            None
        );
        // `SET DEFAULT` and a rewrite of the table in one window.
        assert_eq!(
            compare(&was, &column(3, "int4", -1, "200", 16390, "h2")),
            None
        );
    }

    #[test]
    fn a_column_s_identity_changes_unless_it_widens_purely() {
        let was = column(3, "int4", -1, "100", 16384, "h");
        assert!(matches!(
            compare(&was, &column(9, "int4", -1, "200", 16384, "h2")),
            Some(Change::Readded { .. })
        ));
        assert_eq!(
            compare(&was, &column(3, "int8", -1, "200", 16390, "h2")),
            None
        );
        assert!(matches!(
            compare(&was, &column(3, "numeric", -1, "200", 16390, "h2")),
            Some(Change::Retyped { .. })
        ));
    }

    #[test]
    fn a_node_or_column_gone_is_forgotten_and_the_strongest_change_wins() {
        let baseline: Columns = [
            ((1, "a".to_string()), column(3, "int4", -1, "1", 10, "h")),
            ((2, "a".to_string()), column(3, "int4", -1, "1", 20, "h")),
            ((3, "a".to_string()), column(3, "int4", -1, "1", 30, "h")),
        ]
        .into();
        let live: Columns = [
            ((1, "a".to_string()), column(3, "int4", -1, "2", 11, "h")),
            ((2, "a".to_string()), column(4, "int4", -1, "2", 20, "h2")),
        ]
        .into();
        let found = compare_all(&baseline, &live);
        assert!(matches!(found.changes["a"], Change::Readded { .. }));
        assert_eq!(found.gone, vec![(3, "a".to_string())]);
        assert_eq!(found.rewrite.len(), 2);
    }
}
