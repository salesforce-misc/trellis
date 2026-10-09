//! The drain's side of a schema change (#622 C6).
//!
//! When a column a capture function images is renamed or dropped, the
//! function doesn't fail the application's statement. It appends a
//! `schema_changed` marker for the table, then images the rows over the
//! columns that are left (`capture::sql`). Those later rows lack a column
//! some definitions read, so no such definition may apply them.
//!
//! The marker is a barrier the drain handles before it folds anything:
//! [`pause_readers`] runs right after a drain's claim commits. For every
//! claimed segment whose seal found a marker (`segments.has_schema_change`),
//! it reads the markers from the segment's whole fenced window, not only the
//! worker's buckets, and pauses every definition that reads a missing column
//! (`capture::columns::readers_of`), recording why in `capture_failures`.
//! That commits before the drain's compute reads which definitions apply
//! (`catalog::dependents_of`, applying statuses only), so no page of these
//! segments, on this worker or a peer draining another bucket, applies a
//! later row to a paused definition. The ledger path is planned from the same
//! definition list, so the barrier covers it too.
//!
//! A marker and the partial images after it come from the same trigger call,
//! in the same transaction and ring slot, so every segment holding such a
//! row holds its marker too, and each drain handles its own. Pausing is
//! idempotent, so peers and retries repeat it harmlessly.
//!
//! What pausing drops is only the paused definition's share of these
//! segments. Resuming it is a rebuild from the source, as for any pause.
//!
//! Once the pause commits, capture no longer counts the paused definition's
//! columns, so the staging worker's next reconcile pass regenerates the
//! table's functions over what the remaining readers need, and the markers
//! stop.
//!
//! That pass also pauses readers of a missing column itself, before it
//! regenerates anything, whether or not a write has marked the change yet
//! ([`pause_readers_of_missing`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use tokio_postgres::types::ToSql;

use tokio_postgres::Client;

use super::apply::ApplyError;
use crate::capture::CaptureError;
use crate::capture::columns::{CaptureCatalog, load_catalog, read_columns, readers_of};
use crate::capture::install::{Installed, installed};
use crate::defs::catalog::CatalogError;
use crate::defs::model::{RelationshipDefinition, TransformStatus};
use crate::defs::validate::ValidationError;
use crate::defs::{copies, key_types};
use crate::pool::Pool;

/// What one table's markers in a drain's segments say is missing, or what
/// the capture pass found changed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Missing {
    columns: BTreeSet<String>,
    key_missing: bool,
    /// The installed capture's key and the table's primary key now, when the
    /// key was redefined with its old columns still there (#687). Only the
    /// capture pass sees this: the functions keep imaging the old key
    /// columns, so no write marks it.
    key_changed: Option<(Vec<String>, Vec<String>)>,
}

impl Missing {
    /// Whether every definition on the table must pause.
    fn whole_table(&self) -> bool {
        self.key_missing || self.key_changed.is_some()
    }
}

/// Pauses every definition a `schema_changed` marker in `seg_seqs` names a
/// missing column of (see the module doc). A no-op, costing one indexed
/// read, when no segment's seal found a marker.
pub(crate) async fn pause_readers(pool: &Pool, seg_seqs: &[i64]) -> Result<(), ApplyError> {
    let mut client = pool.get().await?;
    let marked: Vec<i64> = client
        .query(
            "select seg_seq from segments \
             where seg_seq = any($1) and has_schema_change order by seg_seq",
            &[&seg_seqs],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if marked.is_empty() {
        return Ok(());
    }

    let mut by_table: BTreeMap<String, Missing> = BTreeMap::new();
    for seg_seq in marked {
        let (window, params) =
            super::seal::fenced_window(&**client, seg_seq, "src_table, op, new_image").await?;
        let sql = format!(
            "select w.src_table, \
                    coalesce(array_agg(distinct m.c) filter (where m.c is not null), '{{}}'), \
                    coalesce(bool_or((w.new_image ->> 'key_missing')::boolean), false) \
             from ({window}) w \
             left join lateral jsonb_array_elements_text(w.new_image -> 'missing') as m(c) \
               on true \
             where w.op = '{op}' \
             group by w.src_table",
            op = crate::capture::sql::SCHEMA_CHANGED_OP,
        );
        let param_refs: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        for row in client.query(&sql, &param_refs).await? {
            let missing = by_table.entry(row.get(0)).or_default();
            missing.columns.extend(row.get::<_, Vec<String>>(1));
            missing.key_missing |= row.get::<_, bool>(2);
        }
    }

    let catalog = load_catalog(&**client, pool.schema()).await?;
    let txn = client.transaction().await?;
    for (table, missing) in &by_table {
        let readers = readers_of(&catalog, table, &missing.columns, missing.whole_table());
        if readers.is_empty() {
            crate::instance_log::info!(
                table = %table,
                columns = ?missing.columns,
                "a captured column was renamed or dropped, but no definition reads it"
            );
            continue;
        }
        pause(&txn, &catalog, table, missing, &readers).await?;
    }
    txn.commit().await?;
    Ok(())
}

/// Pauses each of `readers` of `table` for `missing`, recording why
/// ([`crate::defs::lifecycle::pause_for_capture_failure`]).
async fn pause(
    txn: &tokio_postgres::Transaction<'_>,
    catalog: &CaptureCatalog,
    table: &str,
    missing: &Missing,
    readers: &[i64],
) -> Result<(), CatalogError> {
    for &id in readers {
        let (columns, error) = match &missing.key_changed {
            Some((old, new)) => (old.clone(), key_changed_error(table, old, new)),
            None => {
                // The columns this definition reads, or every missing one
                // when it pauses only because the key went.
                let mut columns: Vec<String> = missing
                    .columns
                    .iter()
                    .filter(|c| {
                        readers_of(catalog, table, &BTreeSet::from([(*c).clone()]), false)
                            .contains(&id)
                    })
                    .cloned()
                    .collect();
                if columns.is_empty() {
                    columns = missing.columns.iter().cloned().collect();
                }
                let error = missing_columns_error(table, &columns);
                (columns, error)
            }
        };
        if crate::defs::lifecycle::pause_for_capture_failure(txn, id, table, &columns, &error, None)
            .await?
        {
            crate::instance_log::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    Ok(())
}

/// The `capture_failure` sentence for a definition paused because `columns`
/// of `table` it reads were renamed or dropped.
fn missing_columns_error(table: &str, columns: &[String]) -> String {
    format!(
        "column {} of {table}, which this definition reads, was renamed or dropped; its \
         capture now leaves it out. Restore the column and resume the definition to rebuild \
         it, or drop the definition",
        quoted_list(columns)
    )
}

/// The `capture_failure` sentence for a definition paused because `table`'s
/// primary key changed from `old` to `new` (#687).
fn key_changed_error(table: &str, old: &[String], new: &[String]) -> String {
    format!(
        "the primary key of {table} changed from ({}) to ({}), and this definition's rows \
         are keyed by the old one. Restore the key and resume the definition to rebuild it, \
         or drop the definition and define it again",
        quoted_list(old),
        quoted_list(new)
    )
}

fn quoted_list(columns: &[String]) -> String {
    let quoted: Vec<String> = columns.iter().map(|c| format!("{c:?}")).collect();
    quoted.join(", ")
}

/// The staging worker's capture pass's side of a schema change: pauses,
/// before [`crate::capture::columns::capture_spec`] regenerates `table`'s
/// functions, every definition that isn't paused yet and reads a column
/// `table` no longer has. Returns whether it paused any, in which case the
/// caller leaves the table for the next pass, whose catalog no longer counts
/// them.
///
/// "Missing" is what the installed functions image and the table lacks,
/// which is what their next write's marker would name, plus what an active
/// definition reads and the table lacks. Without this, two orderings go
/// wrong:
///
/// - **The pass runs between a rename and the table's next write.** A
///   renamed key column is not missing to `capture_spec`, which reads the
///   current primary key, so it would regenerate the functions keyed by the
///   new name. Every later write then images cleanly, no marker is ever
///   written, and the drain applies rows keyed by a column the definitions
///   don't know, failing every drain.
/// - **A definition resumes, or registers, while a column it reads is
///   missing.** The installed functions don't image the column, so no write
///   marks it; `capture_spec` fails on it every pass, so the table's capture
///   can't change for anyone, and the definition waits for its backfill with
///   nothing on its status.
///
/// It also pauses every definition on the table when its primary key was
/// redefined while the old key columns stayed (#687): the installed
/// functions still image them, so no write marks the change, and
/// `capture_spec` would widen capture to the new key. The drain would then
/// apply rows keyed by columns the definitions' targets aren't keyed by.
///
/// Pausing before a marker does is sound: a pause only ever drops the
/// definition's share, and its resume is the rebuild either way.
pub(crate) async fn pause_readers_of_missing(
    client: &mut Client,
    schema: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let (attnums, key) = crate::capture::columns::live_columns(&*client, table).await?;
    if attnums.is_empty() {
        // No such table (or no columns): `capture_spec` reports it.
        return Ok(false);
    }
    let present = |c: &String| attnums.contains_key(c);
    let mut missing = Missing::default();
    if let Installed::Complete { spec, .. } = installed(&*client, schema, table).await? {
        missing.key_missing = !spec.key().iter().all(present);
        // A table with no primary key now fails `capture_spec`, and the
        // pass reports that; the installed functions keep imaging it.
        if !missing.key_missing && !key.is_empty() && spec.key() != key.as_slice() {
            missing.key_changed = Some((spec.key().to_vec(), key));
        }
        missing
            .columns
            .extend(spec.columns().iter().filter(|c| !present(c)).cloned());
    }
    missing.columns.extend(
        read_columns(catalog, table)
            .columns
            .into_iter()
            .filter(|c| !present(c)),
    );
    if missing.columns.is_empty() && !missing.whole_table() {
        return Ok(false);
    }
    let readers: Vec<i64> = readers_of(catalog, table, &missing.columns, missing.whole_table())
        .into_iter()
        .filter(|id| {
            catalog
                .definitions
                .iter()
                .any(|r| r.id == *id && !r.capture_failed)
        })
        .collect();
    if readers.is_empty() {
        return Ok(false);
    }
    let txn = client.transaction().await?;
    pause(&txn, catalog, table, &missing, &readers).await?;
    txn.commit().await?;
    Ok(true)
}

/// The staging worker's capture pass's check for a table Trellis can't read
/// or write correctly. It pauses every definition that reads `table` and
/// isn't paused for a capture failure yet, recording why in
/// `capture_failures`, when
///
/// - the table's row-level security policies now apply to the ring's owner
///   or to this worker's own role, which read it (issue #745,
///   [`crate::defs::row_security`]); only to this worker's own role for a
///   table another definition targets, which no capture function reads
///   ([`crate::defs::row_security::Readers::Session`]). RLS can be enabled
///   or forced, or the table handed to another owner, after the definitions
///   were accepted; or
/// - a logical-replication subscription now replicates into it, whose
///   changes capture never sees (issue #751, [`crate::defs::subscription`]).
///   A subscription can be created, or refreshed to include the table,
///   after define.
///
/// And (issue #765) it pauses the definition whose target `table` is, when
/// its policies now apply to this worker's own role, which writes it
/// ([`crate::defs::row_security::Readers::Target`]): its applies' updates
/// and deletes would skip the rows they hide, and its inserts would fail.
/// The ring's owner isn't checked for that: it writes no target. One query
/// answers for the target's writer and its readers, which are the same
/// role.
///
/// Returns whether it paused any, in which case the caller leaves the table
/// for the next pass, whose catalog no longer counts them.
///
/// A definition only frozen (paused or quarantined) keeps its status but
/// gets the record, as for a schema change: its resume is the rebuild either
/// way, and the next pass pauses it again while the table is still
/// unsupported.
///
/// Costs no query for a table no unpaused definition reads or writes.
pub(crate) async fn pause_readers_of_unsupported(
    client: &mut Client,
    schema: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    use crate::defs::row_security::{Readers, RowSecurity, applying};
    let writers = unpaused_writers(catalog, table);
    let readers = unpaused_readers(catalog, table);
    if writers.is_empty() && readers.is_empty() {
        return Ok(false);
    }
    // A table another definition targets is fed by the seam, with no
    // capture function to read it as the ring's owner: only the workers
    // read and write it, as their own role, so one check of this worker's
    // role answers for its writer and its readers alike.
    let seam_fed = catalog.definitions.iter().any(|d| d.target == table);
    let roles = if seam_fed {
        Readers::Session
    } else {
        Readers::RingAndSession
    };
    let rls = applying(&*client, schema, table, roles).await?;
    let mut pauses: Vec<(i64, String)> = Vec::new();
    if let Some(rls) = &rls {
        let error = row_security_error(&RowSecurity {
            target: true,
            ..rls.clone()
        });
        pauses.extend(writers.into_iter().map(|id| (id, error.clone())));
    }
    if !readers.is_empty() {
        let error = match &rls {
            Some(rls) => Some(row_security_error(rls)),
            None => crate::defs::subscription::subscribed(&*client, table)
                .await?
                .map(|sub| subscribed_error(&sub)),
        };
        if let Some(error) = error {
            pauses.extend(readers.into_iter().map(|id| (id, error.clone())));
        }
    }
    if pauses.is_empty() {
        return Ok(false);
    }
    let txn = client.transaction().await?;
    for (id, error) in pauses {
        if crate::defs::lifecycle::pause_for_capture_failure(&txn, id, table, &[], &error, None)
            .await?
        {
            crate::instance_log::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    txn.commit().await?;
    Ok(true)
}

/// The staging worker's capture pass's check for a captured table in a
/// partition or inheritance hierarchy (issue #707,
/// [`crate::defs::hierarchy`]): a partitioned table, a partition, or an
/// inheritance parent or child, whose writes its statement triggers can't
/// all see. Define refuses such a table, by the same check, but the table
/// can join a hierarchy later (`ATTACH PARTITION`, `INHERIT`), or be dropped
/// and recreated under the same name as one.
///
/// Returns whether `table` is in one, in which case the caller installs or
/// changes no capture on it. Before it returns `true`, it pauses every
/// definition that reads `table` and isn't paused for a capture failure
/// yet, recording why in `capture_failures`, as
/// [`pause_readers_of_unsupported`] does. A definition only frozen keeps its
/// status but gets the record. A resume re-validates the definition as
/// define would, so it is refused until the table is plain again.
///
/// Only for a table the pass captures: a table another definition targets
/// is fed by the target-mutation seam, and define exempts it the same way.
/// Costs one catalog query.
pub(crate) async fn pause_readers_in_hierarchy(
    client: &mut Client,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let found = crate::defs::hierarchy::hierarchy(&*client, table).await?;
    if found.is_empty() {
        return Ok(false);
    }
    let readers = unpaused_readers(catalog, table);
    if readers.is_empty() {
        return Ok(true);
    }
    let error = hierarchy_error(&found);
    let txn = client.transaction().await?;
    for id in readers {
        if crate::defs::lifecycle::pause_for_capture_failure(&txn, id, table, &[], &error, None)
            .await?
        {
            crate::instance_log::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    txn.commit().await?;
    Ok(true)
}

/// The staging worker's capture pass's check of the types and collations
/// of `table`'s columns that a definition keys by, or that a column it
/// created takes its type from (issues #760, #767 and #824,
/// [`crate::defs::key_types`], [`crate::defs::copies`]). It pauses every
/// definition that reads `table`
/// and isn't paused for a capture failure yet when, on `table`,
///
/// 1. one of its key columns has a type or collation define would refuse
///    for that use now ([`key_types::refusal`]): an `ALTER COLUMN ... TYPE
///    character(n)` on a join column, a nondeterministic `COLLATE` on a
///    `GROUP BY` column;
/// 2. a relationship it reads through no longer joins two columns of the
///    same type, modifier and collation (#590,
///    [`crate::defs::catalog::validate_join_pair`]): one join column widened
///    and the other not (yet);
/// 3. one of its key columns changed type, since the definition was
///    accepted or last resumed, in a way that renders the values Trellis
///    already stored differently ([`key_types::renders_differently`]):
///    `timestamp` to `timestamptz`, `text` to `uuid`, a narrower
///    `varchar(n)`; or
/// 4. a column Trellis created for it, typed from columns of `table`, can't
///    hold every value of the type define would give it now
///    ([`crate::defs::copies::CopyState::outgrown`]): `integer` to `bigint`
///    under a 1-1 target's key, a `SUM`, a `MIN` or a calculated field,
///    `real` to `double precision` under a passthrough. A table whose every
///    such column widened by changing only the catalog (`varchar(50)` to
///    `text` under a passthrough or a to-one projection's column) is
///    re-typed in place instead, and pauses nothing ([`retype_in_place`],
///    #824).
///
/// The types the third check compares against are those recorded in
/// `definition_key_types`; a key column with none recorded (one that joined
/// the table's row-identity key after define, #687) gets the live one
/// recorded here instead.
///
/// The pause's `capture_failure` names every reason and what to do. When
/// `table` is another definition's target and every reason is about a
/// column that definition's resume re-typed (`retype_causes`, #828), it
/// names that resume instead, and records the upstream definition as the
/// pause's cause (`capture_failures.caused_by`), unless the definition was
/// already frozen for another reason or a resume would refuse it
/// ([`Pause::cause`]). The pass resumes it once that definition is live
/// (`staging::quarantine::resume_caused_definitions`, #970), or an operator's
/// resume clears it first. A resume re-validates the
/// definition as define would, and refuses while the first two hold;
/// otherwise it re-records the key types, brings every column it created to
/// the type define would give it now, and rebuilds
/// (`staging::quarantine::resume_transform`). A key column whose type
/// changed without a pause (a widening, a wider `numeric` scale) is
/// re-recorded here, as a resume would.
///
/// A key column the table no longer has is
/// [`pause_readers_of_missing`]'s, which the pass runs first. Returns whether
/// it paused any. Costs no query for a table no unpaused definition reads.
/// `instance` is the capture pass's key for its database and schema, which
/// keeps the in-place re-types that failed ([`retype_in_place`]).
pub(crate) async fn pause_readers_of_retyped(
    client: &mut Client,
    schema: &str,
    instance: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let readers = unpaused_readers(catalog, table);
    if readers.is_empty() {
        return Ok(false);
    }
    let (live, key) = key_types::live_columns(&*client, table).await?;
    if live.is_empty() {
        // No such table: `capture_spec` reports it.
        return Ok(false);
    }
    // Each recorded type, with its `format_type` rendering for the reason.
    let mut recorded: BTreeMap<(i64, String), (key_types::ColumnType, String)> = BTreeMap::new();
    for row in client
        .query(
            "select transform_id, column_name, type_name, typmod, \
                    coalesce(pg_catalog.format_type(pg_catalog.to_regtype(type_name), typmod), \
                             type_name) \
             from definition_key_types \
             where transform_id = any($1) and table_name = $2",
            &[&readers, &table],
        )
        .await?
    {
        recorded.insert(
            (row.get(0), row.get(1)),
            (
                key_types::ColumnType {
                    type_name: row.get(2),
                    typmod: row.get(3),
                },
                row.get(4),
            ),
        );
    }

    let mut checked: Vec<Checked> = Vec::new();
    let mut unrecorded: Vec<(i64, String)> = Vec::new();
    // Key columns whose live type differs from the recorded one without
    // re-rendering the keys stored: re-recorded below for each definition
    // this pass doesn't pause.
    let mut widened_keys: Vec<(i64, String)> = Vec::new();
    for reader in catalog
        .definitions
        .iter()
        .filter(|r| readers.contains(&r.id))
    {
        let declared: Vec<&RelationshipDefinition> = catalog
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
        let mut columns: Vec<String> = Vec::new();
        let mut reasons: Vec<String> = Vec::new();
        // Whether a resume would refuse: define refuses what it found.
        let mut refused = false;
        let mut flag = |column: &str, reason: String, columns: &mut Vec<String>| {
            if !columns.iter().any(|c| c == column) {
                columns.push(column.to_string());
            }
            reasons.push(reason);
        };

        let mut seen: Vec<String> = Vec::new();
        for (column, key_use) in
            key_types::key_uses(&reader.def, &reader.source, &rels, table, &key)
        {
            if seen.contains(&column) {
                continue;
            }
            seen.push(column.clone());
            let Some(now) = live.get(&column) else {
                continue;
            };
            if let Some(reason) =
                key_types::refusal(&*client, table, &column, &key_use, now).await?
            {
                refused = true;
                flag(&column, reason, &mut columns);
                continue;
            }
            match recorded.get(&(reader.id, column.clone())) {
                Some((old, was)) if key_types::renders_differently(old, &now.ty) => {
                    flag(
                        &column,
                        key_types::retyped_error(table, &column, &key_use, was, now),
                        &mut columns,
                    );
                }
                Some((old, _)) if old != &now.ty => widened_keys.push((reader.id, column.clone())),
                Some(_) => {}
                None => {
                    if !unrecorded.contains(&(reader.id, column.clone())) {
                        unrecorded.push((reader.id, column.clone()));
                    }
                }
            }
        }

        // #590's pairing, for each relationship it reads through with a
        // join column on this table. A join column refused on its own is
        // reported above; only the pairing is this check's.
        let read: BTreeSet<String> = crate::defs::eval::relationship_references(&reader.def)
            .into_iter()
            .map(|(rel, _)| rel)
            .collect();
        for (rel, to_table) in declared.iter().zip(&to_tables) {
            if !read.contains(&rel.def.name) {
                continue;
            }
            let column = if rel.qualified_from_table() == table {
                &rel.def.from_col
            } else if to_table == table {
                &rel.def.to_col
            } else {
                continue;
            };
            if columns.contains(column) || !live.contains_key(column) {
                continue;
            }
            match crate::defs::catalog::validate_join_pair(
                &*client,
                &rel.def,
                &rel.qualified_from_table(),
                to_table,
                false,
            )
            .await
            {
                Ok(_) => {}
                Err(CatalogError::Db(err)) => return Err(err.into()),
                Err(CatalogError::Pool(err)) => return Err(CatalogError::Pool(err).into()),
                Err(err @ CatalogError::Validate(ValidationError::RelationshipTypeMismatch(_))) => {
                    refused = true;
                    flag(column, err.to_string(), &mut columns);
                }
                // A refused type or collation on the other side is that
                // table's check's; a missing column is the missing-column
                // check's.
                Err(_) => {}
            }
        }

        // #767, #824: the columns Trellis created for it typed from columns
        // of this table, each with the type define would give it now.
        let copies: Vec<copies::TypedCopy> = copies::typed_copies(
            &*client,
            schema,
            &reader.def,
            &reader.source,
            &reader.target,
            &declared,
        )
        .await?
        .into_iter()
        .filter(|c| c.columns_of(table).next().is_some())
        .collect();
        let states = copies::inspect(&*client, copies).await?;
        checked.push(Checked {
            id: reader.id,
            columns,
            reasons,
            refused,
            states,
        });
    }

    // #824: each created table whose every drifted column widened by
    // changing only the catalog is re-typed here, and pauses nothing.
    let in_place = retype_in_place(client, instance, table, &checked).await?;

    let mut pauses: Vec<Pause> = Vec::new();
    for Checked {
        id,
        mut columns,
        mut reasons,
        refused,
        states,
    } in checked
    {
        // Without a refusal, every key column flagged above is one whose
        // change re-renders the keys stored.
        let rerendered: Vec<String> = if refused { Vec::new() } else { columns.clone() };
        // A column that outgrew the type define would give it now, on a
        // table not re-typed in place, by the columns of this table each is
        // typed from.
        let mut outgrown: BTreeMap<Vec<String>, Vec<copies::CopyState>> = BTreeMap::new();
        for state in states {
            if !state.outgrown() || in_place.handles(&state.copy.table) {
                continue;
            }
            let from: Vec<String> = state.copy.columns_of(table).map(str::to_string).collect();
            // A copy of a key column flagged above is reported once, there.
            if state.copy.inferred.is_none() && columns.contains(&from[0]) {
                continue;
            }
            outgrown.entry(from).or_default().push(state);
        }
        for (from, states) in &outgrown {
            let inputs = input_types(&*client, table, &live, states).await?;
            reasons.push(outgrown_reason(table, from, &inputs, states));
            for column in from {
                if !columns.contains(column) {
                    columns.push(column.clone());
                }
            }
        }

        if !reasons.is_empty() {
            let remedy = if refused {
                "A resume re-validates the definition as define would, and refuses until that \
                 is fixed. Fix it, then resume the definition to rebuild it, or drop the \
                 definition and define it again"
            } else {
                "Resume the definition: the resume brings Trellis's copies to the new types and \
                 rebuilds it. Or drop the definition and define it again"
            };
            let error = format!("{}. {remedy}", reasons.join("; "));
            pauses.push(Pause {
                id,
                columns,
                error,
                refused,
                rerendered,
                outgrown: outgrown.into_iter().collect(),
            });
        }
    }

    // Recorded on its own: the next pass compares against it whether or not
    // this one pauses anything. A key column whose change this pass accepted
    // without a pause is recorded at its new type for each definition left
    // unpaused, as a resume records it: the definition stores keys of that
    // type from now on, so a later change is measured from it. Measured from
    // the type define saw instead, `numeric(10,2)` widened to `(10,3)` and
    // narrowed back would round the keys stored in between unseen.
    let mut by_reader: BTreeMap<i64, Vec<(String, String)>> = BTreeMap::new();
    for (id, column) in unrecorded {
        by_reader
            .entry(id)
            .or_default()
            .push((table.to_string(), column));
    }
    for (id, columns) in &by_reader {
        key_types::record(&*client, *id, columns).await?;
    }
    let mut widened: BTreeMap<i64, Vec<(String, String)>> = BTreeMap::new();
    for (id, column) in widened_keys {
        if !pauses.iter().any(|p| p.id == id) {
            widened
                .entry(id)
                .or_default()
                .push((table.to_string(), column));
        }
    }
    for (id, columns) in &widened {
        key_types::rerecord(&*client, *id, columns).await?;
    }
    if pauses.is_empty() {
        return Ok(false);
    }
    // #828: a pause the re-type of an upstream definition's target caused,
    // in a resume of that definition.
    let causes = if pauses.iter().any(|p| !p.refused) {
        retype_causes(&*client, table, &live).await?
    } else {
        HashMap::new()
    };
    let txn = client.transaction().await?;
    for pause in pauses {
        let mut error = pause.error.clone();
        let mut caused_by = None;
        if let Some(cause) = pause.cause(&causes) {
            // A definition already frozen (paused by the operator,
            // quarantined) was paused for another reason first: it gets
            // the record, as before, but not the cause. Read under the lock
            // the pause's update takes on an unfrozen row, so the status
            // can't change before the update. A frozen row the update skips
            // is locked here too: this transaction bumps no version fence
            // and locks rows in id order, and a resume holding the row has
            // finished its fence wait (ADR-0002, #744).
            let frozen = txn
                .query_opt(
                    "select status from transform_definitions where id = $1 for no key update",
                    &[&pause.id],
                )
                .await?
                .and_then(|row| TransformStatus::from_persisted(row.get(0)))
                .is_some_and(TransformStatus::is_frozen);
            if !frozen {
                error = caused_error(table, &cause, &pause);
                caused_by = Some(cause.upstream);
            }
        }
        if crate::defs::lifecycle::pause_for_capture_failure(
            &txn,
            pause.id,
            table,
            &pause.columns,
            &error,
            caused_by,
        )
        .await?
        {
            crate::instance_log::warn!(transform_id = pause.id, table = %table, "definition paused: {error}");
        }
    }
    txn.commit().await?;
    Ok(true)
}

/// One definition [`pause_readers_of_retyped`] pauses, and why.
struct Pause {
    id: i64,
    /// The columns of the checked table its reasons are about.
    columns: Vec<String>,
    /// Its `capture_failure` when no upstream resume caused it.
    error: String,
    /// Whether a resume would refuse it: define refuses what was found.
    refused: bool,
    /// Its key columns whose change re-renders the keys it stored (none
    /// when `refused`).
    rerendered: Vec<String>,
    /// The columns Trellis created for it that outgrew their types, by the
    /// columns of the checked table each is typed from.
    outgrown: Vec<(Vec<String>, Vec<copies::CopyState>)>,
}

impl Pause {
    /// The upstream resume that caused this pause (#828): every reason it
    /// pauses for is about a column of the checked table one definition's
    /// resume re-typed (`causes`, by column, from [`retype_causes`]). A
    /// refused definition fails its own re-validation, so its own error
    /// stands, with no cause.
    fn cause(&self, causes: &HashMap<String, RetypeCause>) -> Option<RetypeCause> {
        if self.refused {
            return None;
        }
        let mut found: Vec<&RetypeCause> = Vec::new();
        for column in &self.rerendered {
            found.push(causes.get(column)?);
        }
        // A column typed from several columns of the table outgrew it
        // because one of them changed: the cause, when one was re-typed.
        for (from, _) in &self.outgrown {
            found.push(from.iter().find_map(|c| causes.get(c))?);
        }
        let first = *found.first()?;
        if found.iter().any(|c| c.upstream != first.upstream) || first.upstream == self.id {
            return None;
        }
        // Every column of the table the upstream resume re-typed that this
        // pause is about, for the message.
        Some(RetypeCause {
            upstream: first.upstream,
            name: first.name.clone(),
            columns: self
                .columns
                .iter()
                .filter_map(|c| causes.get(c).filter(|k| k.upstream == first.upstream))
                .flat_map(|k| k.columns.clone())
                .collect(),
        })
    }
}

/// A definition whose resume re-typed columns of its target, as
/// [`retype_causes`] reads it for one column, or [`Pause::cause`] gathers it
/// for a pause.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RetypeCause {
    upstream: i64,
    /// The upstream definition's bare target: its name in `RESUME
    /// TRANSFORM`.
    name: String,
    /// Each column re-typed, with its old and new type (`format_type`).
    columns: Vec<(String, String, String)>,
}

/// The columns of `table` a resume of the definition that writes it
/// re-typed (`retype_causes`, recorded by
/// `staging::quarantine::finish_requested_resumes`), by column. A record
/// counts only while its definition still writes `table` and the column
/// still has the type it re-typed it to (`live`): a later change to the
/// column is another cause.
async fn retype_causes(
    client: &impl tokio_postgres::GenericClient,
    table: &str,
    live: &HashMap<String, key_types::LiveColumn>,
) -> Result<HashMap<String, RetypeCause>, tokio_postgres::Error> {
    let mut causes = HashMap::new();
    for row in client
        .query(
            "select r.column_name, r.transform_id, split_part(d.target_table, '.', 2), \
                    r.old_type, r.new_type \
             from retype_causes r \
             join transform_definitions d \
               on d.id = r.transform_id and d.target_table = r.table_name \
             where r.table_name = $1",
            &[&table],
        )
        .await?
    {
        let column: String = row.get(0);
        let new_type: String = row.get(4);
        if live.get(&column).is_none_or(|now| now.display != new_type) {
            continue;
        }
        causes.insert(
            column.clone(),
            RetypeCause {
                upstream: row.get(1),
                name: row.get(2),
                columns: vec![(column, row.get(3), new_type)],
            },
        );
    }
    Ok(causes)
}

/// The `capture_failure` of a definition `pause` that `cause`'s resume
/// paused (#828), by re-typing columns of its target `table` the
/// definition reads. It names that resume, not the columns as if the
/// operator had altered them, and says the definition resumes on its own once
/// the upstream is live again (`staging::quarantine::resume_caused_definitions`,
/// #970): its rebuild reads the upstream's target.
fn caused_error(table: &str, cause: &RetypeCause, pause: &Pause) -> String {
    let retyped: Vec<String> = cause
        .columns
        .iter()
        .map(|(column, old, new)| format!("{table}.{column} from {old} to {new}"))
        .collect();
    let mut effects: Vec<String> = Vec::new();
    let outgrown: Vec<copies::CopyState> = pause
        .outgrown
        .iter()
        .flat_map(|(_, states)| states.iter().cloned())
        .collect();
    if !outgrown.is_empty() {
        effects.push(format!(
            "the columns Trellis created for this definition from {} can't hold every value \
             of the types define would give them now: {}",
            if retyped.len() == 1 { "it" } else { "them" },
            outgrown_columns(&outgrown)
        ));
    }
    if !pause.rerendered.is_empty() {
        effects.push(
            "the keys Trellis stored for this definition no longer match the new type's \
             rendering"
                .to_string(),
        );
    }
    format!(
        "the resume of transform {name} re-typed {retyped}, which this definition reads, so \
         {effects}. This definition resumes on its own once {name} is live again, \
         bringing its columns to the new types and rebuilding it. Or drop the definition and \
         define it again",
        name = cause.name,
        retyped = retyped.join(", "),
        effects = effects.join(", and "),
    )
}

/// What [`pause_readers_of_retyped`] found for one definition before it
/// decides what to pause: the key columns it flagged and why, and every
/// column Trellis created for it typed from the table being checked.
struct Checked {
    id: i64,
    columns: Vec<String>,
    reasons: Vec<String>,
    refused: bool,
    states: Vec<copies::CopyState>,
}

/// Re-types each table Trellis created whose every widened column, across
/// all of `checked`, widened by changing only the catalog
/// ([`copies::CopyState::catalog_only`]): `varchar(n)` to a longer
/// `varchar`, `text` or an unbounded `varchar`, or `numeric(p,s)` to more
/// precision at the same scale (#824). A table where some column also
/// outgrew its type by a widening that rewrites (`integer` to `bigint`) is
/// left to the pause and its resume, which re-type all of it. A drifted
/// column that wasn't widened (a narrowing, another type family) is left
/// as it is, as the pause leaves it, and doesn't stop the table's re-type.
///
/// One table per transaction, under the resume's own lock timeout
/// (`staging::quarantine::RETYPE_LOCK_TIMEOUT`). The `ALTER` is the
/// transaction's first statement, so it waits for the table's `ACCESS
/// EXCLUSIVE` while holding no lock of its own, and once it has it, takes
/// only the locks of that table's own indexes and TOAST table, which nothing
/// takes without the table's lock first. So it can't close a cycle with a
/// drain page, a build or a release, whatever order they take their locks
/// in: at worst a page queued behind it waits out the timeout.
///
/// - A table it re-types asks, in the same transaction, for the release of
///   each owning definition's keys held for a value its old types couldn't
///   hold (`retype_releases`, released by
///   `staging::quarantine::release_retyped_keys` after the pass): `22001`
///   for a `varchar`, `22003` for a `numeric`. A definition owns the table
///   when it has any column there, so every reader of a shared projection
///   does, since a projection write that fails is charged to each.
/// - A table whose re-type fails transiently
///   (`staging::quarantine::is_transient_error`: its lock not got within the
///   timeout, a deadlock, a statement timeout's cancel, a lost connection)
///   is left as it is, and nothing is paused for it: the next pass tries
///   again.
/// - A table whose re-type fails otherwise (a view on the column, say) is
///   left to the pause when some column there outgrew its type, as a
///   rewriting widening is. One whose columns all still hold every value
///   (an unbounded `varchar` to `text`) pauses nothing, since its writes
///   still succeed. Either way, the failure is remembered in this process
///   ([`FAILED_RETYPES`]), and while the table's drift asks for the same
///   statement for the same definitions, later passes don't try it again:
///   it would only take the table's lock, fail and log again every pass.
///
/// Returns the tables it re-typed, and those it left waiting for their
/// lock: no definition pauses for either.
async fn retype_in_place(
    client: &mut Client,
    instance: &str,
    checked_table: &str,
    checked: &[Checked],
) -> Result<InPlace, CaptureError> {
    // Every widened column by its table, once each: a projection's columns
    // are every reader's of the relationship. An unbounded `varchar` to
    // `text` holds no more values, so it isn't outgrown, but it is re-typed
    // here all the same, as a resume would.
    let mut widened: BTreeMap<&str, Vec<&copies::CopyState>> = BTreeMap::new();
    for state in checked.iter().flat_map(|c| &c.states) {
        if !state.outgrown() && !state.catalog_only() {
            continue;
        }
        let columns = widened.entry(state.copy.table.as_str()).or_default();
        if !columns.iter().any(|s| s.copy.column == state.copy.column) {
            columns.push(state);
        }
    }
    let mut in_place = InPlace::default();
    // Each table's re-type this pass, tried or not.
    let mut attempted: Vec<(String, FailedRetype)> = Vec::new();
    for (table, states) in widened {
        if !states.iter().all(|s| s.catalog_only()) {
            continue;
        }
        let owners: Vec<i64> = checked
            .iter()
            .filter(|c| c.states.iter().any(|s| s.copy.table == table))
            .map(|c| c.id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let sqlstates: Vec<String> = states
            .iter()
            .map(|s| {
                if s.copy_type.ty.type_name == "pg_catalog.numeric" {
                    "22003".to_string()
                } else {
                    "22001".to_string()
                }
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let owned: Vec<copies::CopyState> = states.iter().map(|s| (*s).clone()).collect();
        let Some((sql, labels)) = copies::retype_statements(&owned).into_iter().next() else {
            continue;
        };
        let key = (
            instance.to_string(),
            checked_table.to_string(),
            table.to_string(),
        );
        let retype = FailedRetype {
            owners: owners.clone(),
            sql: sql.clone(),
        };
        let failed_before = with_failed_retypes(|failed| failed.get(&key) == Some(&retype));
        attempted.push((key.2.clone(), retype.clone()));
        if failed_before {
            crate::instance_log::debug!(
                table = %checked_table,
                copies = ?labels,
                "not re-typing Trellis's columns in place: the same re-type failed before"
            );
            continue;
        }
        let txn = client.transaction().await?;
        crate::locks::set_local_lock_timeout(&txn, crate::staging::quarantine::RETYPE_LOCK_TIMEOUT)
            .await?;
        match txn.batch_execute(&sql).await {
            Ok(()) => {
                txn.execute(
                    "insert into retype_releases (transform_id, sqlstate) \
                     select id, sqlstate from unnest($1::int8[]) as id \
                     cross join unnest($2::text[]) as sqlstate \
                     on conflict (transform_id, sqlstate) do update set requested_at = now()",
                    &[&owners, &sqlstates],
                )
                .await?;
                txn.commit().await?;
                crate::instance_log::info!(
                    table = %checked_table,
                    copies = ?labels,
                    "re-typed Trellis's columns in place: the source widened them without a rewrite"
                );
                in_place.retyped.insert(table.to_string());
            }
            Err(err) if crate::staging::quarantine::is_transient_error(&err) => {
                drop(txn);
                crate::instance_log::info!(
                    table = %checked_table,
                    copies = ?labels,
                    error = %err.as_db_error().map(ToString::to_string).unwrap_or_else(|| err.to_string()),
                    "re-typing Trellis's columns in place failed transiently; retrying next pass"
                );
                in_place.waiting.insert(table.to_string());
            }
            Err(err) => {
                txn.rollback().await?;
                let outcome = if states.iter().any(|s| s.outgrown()) {
                    "pausing their definitions instead"
                } else {
                    "leaving them as they are, since they still hold every value"
                };
                crate::instance_log::warn!(
                    table = %checked_table,
                    copies = ?labels,
                    error = %err.as_db_error().map(ToString::to_string).unwrap_or_else(|| err.to_string()),
                    "couldn't re-type Trellis's columns in place; {outcome}"
                );
                with_failed_retypes(|failed| failed.insert(key, retype));
            }
        }
    }
    // A failure is forgotten once its table's drift no longer asks for the
    // statement that failed, for the same definitions: the drift went or
    // changed, or a definition joined or left the table.
    with_failed_retypes(|failed| {
        failed.retain(|(i, checked, table), retype| {
            i != instance
                || checked != checked_table
                || attempted.iter().any(|(t, r)| t == table && r == retype)
        })
    });
    Ok(in_place)
}

/// The in-place re-types ([`retype_in_place`]) that failed for good, by
/// capture instance (`capture::reconcile`'s key for a database and schema),
/// the table checked and the table Trellis created. Kept in this process
/// only, so a restarted worker tries each once more.
type FailedRetypes = BTreeMap<(String, String, String), FailedRetype>;

/// One in-place re-type that failed: the definitions that owned the table
/// then, and the statement. A definition defined again under the same
/// target has a new id, so it tries the re-type afresh instead of inheriting
/// a failure whose cause (a view since dropped) may be gone.
#[derive(Clone, PartialEq, Eq)]
struct FailedRetype {
    owners: Vec<i64>,
    sql: String,
}

static FAILED_RETYPES: std::sync::Mutex<FailedRetypes> = std::sync::Mutex::new(BTreeMap::new());

fn with_failed_retypes<T>(f: impl FnOnce(&mut FailedRetypes) -> T) -> T {
    let mut guard = FAILED_RETYPES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f(&mut guard)
}

/// Forgets every in-place re-type that failed for `instance`: its staging
/// worker in this process stopped, so a worker that takes over tries each
/// once more.
pub(crate) fn forget_failed_retypes(instance: &str) {
    with_failed_retypes(|failed| failed.retain(|(i, _, _), _| i != instance));
}

/// The tables [`retype_in_place`] re-typed, and those it left to retry next
/// pass after a transient failure.
#[derive(Default)]
struct InPlace {
    retyped: BTreeSet<String>,
    waiting: BTreeSet<String>,
}

impl InPlace {
    /// Whether `table`'s drift is the in-place re-type's, not a pause's.
    fn handles(&self, table: &str) -> bool {
        self.retyped.contains(table) || self.waiting.contains(table)
    }
}

/// Every source column `states` are typed from, once each, as
/// `schema.table.column` with its live type (`format_type`). `live` is
/// `table`'s columns; the others are read from the catalog. Queries only
/// when some state reads another table.
async fn input_types(
    client: &impl tokio_postgres::GenericClient,
    table: &str,
    live: &std::collections::HashMap<String, key_types::LiveColumn>,
    states: &[copies::CopyState],
) -> Result<Vec<(String, String)>, tokio_postgres::Error> {
    let mut reads: Vec<&(String, String)> = Vec::new();
    for read in states.iter().flat_map(|s| &s.copy.reads) {
        if !reads.contains(&read) {
            reads.push(read);
        }
    }
    let elsewhere: Vec<&&(String, String)> = reads.iter().filter(|(t, _)| t != table).collect();
    let mut other: std::collections::HashMap<usize, String> = std::collections::HashMap::new();
    if !elsewhere.is_empty() {
        let tables: Vec<String> = elsewhere
            .iter()
            .map(|(t, _)| crate::defs::ddl::regclass_arg(t))
            .collect();
        let names: Vec<&str> = elsewhere.iter().map(|(_, c)| c.as_str()).collect();
        for row in client
            .query(
                "select k.i, pg_catalog.format_type(a.atttypid, a.atttypmod) \
                 from unnest($1::text[], $2::text[]) with ordinality as k(t, c, i) \
                 join pg_catalog.pg_attribute a \
                   on a.attrelid = pg_catalog.to_regclass(k.t) and a.attname = k.c \
                  and a.attnum > 0 and not a.attisdropped",
                &[&tables, &names],
            )
            .await?
        {
            other.insert(row.get::<_, i64>(0) as usize - 1, row.get(1));
        }
    }
    let mut next = 0;
    Ok(reads
        .into_iter()
        .map(|(t, c)| {
            let display = if t == table {
                live.get(c).map(|now| now.display.clone())
            } else {
                next += 1;
                other.get(&(next - 1)).cloned()
            };
            (
                format!("{t}.{c}"),
                display.unwrap_or_else(|| "no longer there".to_string()),
            )
        })
        .collect())
}

/// The reason a definition is paused for `states`, the columns it created
/// typed from `columns` of `table` whose types define would now give a
/// wider type ([`copies::CopyState::outgrown`]). `inputs` are every source
/// column `states` are typed from, with its live type ([`input_types`]).
///
/// It names a column as the one that widened only when it is the only
/// column they're typed from. Otherwise it names each one with its type:
/// nothing records an expression's input types, so the pass can't tell
/// which of them changed, and it may be a column of another table.
fn outgrown_reason(
    table: &str,
    columns: &[String],
    inputs: &[(String, String)],
    states: &[copies::CopyState],
) -> String {
    let only_input = match (columns, inputs) {
        ([column], [(_, now)]) => Some((column, now)),
        _ => None,
    };
    if let Some((column, now)) = only_input {
        if states.iter().all(|s| s.copy.inferred.is_none()) {
            let held: Vec<String> = states
                .iter()
                .map(|s| format!("{} ({})", s.copy.label(), s.copy_type.display))
                .collect();
            return format!(
                "column {column:?} of {table} widened to {}, and Trellis keeps a copy of it \
                 that can't hold every value of that type: {}",
                states[0].live_type.display,
                held.join(", ")
            );
        }
        return format!(
            "column {column:?} of {table} widened to {now}, and Trellis keeps columns typed \
             from it that can't hold every value of the types define would give them now: {}",
            outgrown_columns(states)
        );
    }
    let named: Vec<String> = inputs
        .iter()
        .map(|(label, now)| format!("{label} ({now})"))
        .collect();
    format!(
        "Trellis keeps columns typed from {} that can't hold every value of the types define \
         would give them now: {}",
        named.join(", "),
        outgrown_columns(states)
    )
}

/// `states` as a reason lists them: each column, its type, and the type
/// define would give it now.
fn outgrown_columns(states: &[copies::CopyState]) -> String {
    states
        .iter()
        .map(|s| {
            format!(
                "{} ({}, now {})",
                s.copy.label(),
                s.copy_type.display,
                s.live_type.display
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The `capture_failure` sentence for a definition paused because `sub`
/// replicates into a table it reads.
fn subscribed_error(sub: &crate::defs::subscription::Subscribed) -> String {
    format!("{sub}; then resume the definition to rebuild it, or drop the definition")
}

/// The `capture_failure` sentence for a definition paused because `rls`
/// applies to a table it reads.
fn row_security_error(rls: &crate::defs::row_security::RowSecurity) -> String {
    format!("{rls}; then resume the definition to rebuild it, or drop the definition")
}

/// The `capture_failure` sentence for a definition paused because a table it
/// reads is in a hierarchy, every way it is in one.
fn hierarchy_error(found: &[crate::defs::hierarchy::Hierarchy]) -> String {
    let causes: Vec<String> = found.iter().map(ToString::to_string).collect();
    format!(
        "{}; then resume the definition to rebuild it, or drop the definition",
        causes.join("; ")
    )
}

/// Every definition whose target is `table` (at most one) and that isn't
/// paused for a capture failure yet.
fn unpaused_writers(catalog: &CaptureCatalog, table: &str) -> Vec<i64> {
    catalog
        .definitions
        .iter()
        .filter(|r| r.target == table && !r.capture_failed)
        .map(|r| r.id)
        .collect()
}

/// Every definition that reads `table` at all, as its source or a
/// relationship's to-side, and isn't paused for a capture failure yet.
fn unpaused_readers(catalog: &CaptureCatalog, table: &str) -> Vec<i64> {
    readers_of(catalog, table, &BTreeSet::new(), true)
        .into_iter()
        .filter(|id| {
            catalog
                .definitions
                .iter()
                .any(|r| r.id == *id && !r.capture_failed)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::copies::{CopyKind, CopyState, CopyType, TypedCopy};
    use crate::defs::key_types::ColumnType;

    fn ty(display: &str) -> CopyType {
        CopyType {
            oid: 0,
            typmod: -1,
            ty: ColumnType {
                type_name: format!("pg_catalog.{display}"),
                typmod: -1,
            },
            display: display.to_string(),
            collatable: false,
        }
    }

    fn state(column: &str, reads: &[(&str, &str)], inferred: bool) -> CopyState {
        CopyState {
            copy: TypedCopy {
                kind: if inferred {
                    CopyKind::Field
                } else {
                    CopyKind::Passthrough
                },
                table: "\"public\".\"mix\"".to_string(),
                table_name: "public.mix".to_string(),
                column: column.to_string(),
                reads: reads
                    .iter()
                    .map(|(t, c)| (t.to_string(), c.to_string()))
                    .collect(),
                inferred: inferred.then(|| "bigint".to_string()),
            },
            copy_type: ty("integer"),
            live_type: ty("bigint"),
            collation: None,
        }
    }

    fn input(label: &str, now: &str) -> (String, String) {
        (label.to_string(), now.to_string())
    }

    /// The resume of definition `upstream` (`up`) re-typed `column` of the
    /// checked table from `integer` to `bigint`.
    fn cause(upstream: i64, column: &str) -> (String, RetypeCause) {
        (
            column.to_string(),
            RetypeCause {
                upstream,
                name: "up".to_string(),
                columns: vec![(
                    column.to_string(),
                    "integer".to_string(),
                    "bigint".to_string(),
                )],
            },
        )
    }

    /// Definition 7, paused for `outgrown` columns typed from columns of
    /// `public.up` and for `rerendered` key columns.
    fn pause(refused: bool, rerendered: &[&str], outgrown: &[&[&str]]) -> Pause {
        let mut columns: Vec<String> = rerendered.iter().map(|c| c.to_string()).collect();
        for from in outgrown {
            for c in from.iter() {
                if !columns.iter().any(|seen| seen == c) {
                    columns.push(c.to_string());
                }
            }
        }
        Pause {
            id: 7,
            columns,
            error: "its own reason".to_string(),
            refused,
            rerendered: rerendered.iter().map(|c| c.to_string()).collect(),
            outgrown: outgrown
                .iter()
                .map(|from| {
                    let reads: Vec<(&str, &str)> = from.iter().map(|c| ("public.up", *c)).collect();
                    (
                        from.iter().map(|c| c.to_string()).collect(),
                        vec![state(from[0], &reads, from.len() > 1)],
                    )
                })
                .collect(),
        }
    }

    /// #828: a pause is the upstream resume's only when every reason it
    /// pauses for is about a column that resume re-typed, and the definition
    /// doesn't fail its own re-validation.
    #[test]
    fn a_pause_is_caused_by_an_upstream_resume_only_when_every_reason_is_its() {
        let causes: HashMap<String, RetypeCause> = [cause(3, "id"), cause(3, "n")].into();
        let caused = pause(false, &[], &[&["id"]])
            .cause(&causes)
            .expect("caused");
        assert_eq!(caused.upstream, 3);
        assert_eq!(
            caused.columns,
            vec![(
                "id".to_string(),
                "integer".to_string(),
                "bigint".to_string()
            )]
        );
        // A column typed from several columns of the table, one re-typed.
        assert!(
            pause(false, &[], &[&["id", "qty"]])
                .cause(&causes)
                .is_some()
        );
        // A key column whose keys render differently, re-typed upstream.
        assert!(pause(false, &["n"], &[]).cause(&causes).is_some());

        // Refused: its own re-validation fails, so its own error stands.
        assert_eq!(pause(true, &[], &[&["id"]]).cause(&causes), None);
        // A reason no resume caused.
        assert_eq!(pause(false, &[], &[&["id"], &["qty"]]).cause(&causes), None);
        assert_eq!(pause(false, &["qty"], &[&["id"]]).cause(&causes), None);
        // Two upstream resumes: nothing records which to wait for.
        let mixed: HashMap<String, RetypeCause> = [cause(3, "id"), cause(4, "n")].into();
        assert_eq!(pause(false, &[], &[&["id"], &["n"]]).cause(&mixed), None);
        // Nothing re-typed at all.
        assert_eq!(pause(false, &[], &[&["id"]]).cause(&HashMap::new()), None);
        // A definition's own resume isn't its upstream.
        let own: HashMap<String, RetypeCause> = [cause(7, "id")].into();
        assert_eq!(pause(false, &[], &[&["id"]]).cause(&own), None);
    }

    /// #828: the message of a pause an upstream resume caused names that
    /// resume, and says the definition resumes on its own once the upstream is
    /// live.
    #[test]
    fn a_caused_pause_names_the_upstream_resume() {
        let causes: HashMap<String, RetypeCause> = [cause(3, "id")].into();
        let paused = pause(false, &[], &[&["id"]]);
        let caused = paused.cause(&causes).expect("caused");
        assert_eq!(
            caused_error("public.up", &caused, &paused),
            "the resume of transform up re-typed public.up.id from integer to bigint, which \
             this definition reads, so the columns Trellis created for this definition from it \
             can't hold every value of the types define would give them now: public.mix.id \
             (integer, now bigint). This definition resumes on its own once up is live again, \
             bringing its columns to the new types and rebuilding it. Or drop the definition \
             and define it again"
        );
    }

    /// A column is named as the one that widened only when it is the only
    /// input. `qty + author.score`, after `score` widens, is checked on the
    /// source table too, where `qty` is its only column and hasn't changed.
    #[test]
    fn an_outgrown_reason_names_a_widened_column_only_when_it_is_the_only_input() {
        let qty = ["qty".to_string()];
        let only = [state("next", &[("public.posts", "qty")], true)];
        assert_eq!(
            outgrown_reason(
                "public.posts",
                &qty,
                &[input("public.posts.qty", "bigint")],
                &only
            ),
            "column \"qty\" of public.posts widened to bigint, and Trellis keeps columns typed \
             from it that can't hold every value of the types define would give them now: \
             public.mix.next (integer, now bigint)"
        );
        let copy = [state("qty", &[("public.posts", "qty")], false)];
        assert_eq!(
            outgrown_reason(
                "public.posts",
                &qty,
                &[input("public.posts.qty", "bigint")],
                &copy
            ),
            "column \"qty\" of public.posts widened to bigint, and Trellis keeps a copy of it \
             that can't hold every value of that type: public.mix.qty (integer)"
        );
        let mixed = [state(
            "z",
            &[("public.posts", "qty"), ("public.authors", "score")],
            true,
        )];
        let reason = outgrown_reason(
            "public.posts",
            &qty,
            &[
                input("public.posts.qty", "integer"),
                input("public.authors.score", "bigint"),
            ],
            &mixed,
        );
        assert_eq!(
            reason,
            "Trellis keeps columns typed from public.posts.qty (integer), \
             public.authors.score (bigint) that can't hold every value of the types define \
             would give them now: public.mix.z (integer, now bigint)"
        );
        assert!(!reason.contains("widened"), "{reason}");
    }
}
