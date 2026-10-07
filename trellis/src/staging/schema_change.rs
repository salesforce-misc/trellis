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

use std::collections::{BTreeMap, BTreeSet};

use tokio_postgres::types::ToSql;

use tokio_postgres::Client;

use super::apply::ApplyError;
use crate::capture::CaptureError;
use crate::capture::columns::{CaptureCatalog, load_catalog, read_columns, readers_of};
use crate::capture::install::{Installed, installed};
use crate::defs::catalog::CatalogError;
use crate::defs::model::RelationshipDefinition;
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
            tracing::info!(
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
        if crate::defs::lifecycle::pause_for_capture_failure(txn, id, table, &columns, &error)
            .await?
        {
            tracing::warn!(transform_id = id, table = %table, "definition paused: {error}");
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
        if crate::defs::lifecycle::pause_for_capture_failure(&txn, id, table, &[], &error).await? {
            tracing::warn!(transform_id = id, table = %table, "definition paused: {error}");
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
///    `varchar(50)` to `text` under a passthrough or a to-one projection's
///    column.
///
/// The types the third check compares against are those recorded in
/// `definition_key_types`; a key column with none recorded (one that joined
/// the table's row-identity key after define, #687) gets the live one
/// recorded here instead.
///
/// The pause's `capture_failure` names every reason and what to do. Only a
/// deliberate resume clears it (or a drop): Trellis re-types nothing on its
/// own. A resume re-validates the definition as define would, and refuses
/// while the first two hold; otherwise it re-records the key types, brings
/// every column it created to the type define would give it now, and
/// rebuilds
/// (`staging::quarantine::resume_transform`).
///
/// A key column the table no longer has is
/// [`pause_readers_of_missing`]'s, which the pass runs first. Returns whether
/// it paused any. Costs no query for a table no unpaused definition reads.
pub(crate) async fn pause_readers_of_retyped(
    client: &mut Client,
    schema: &str,
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

    let mut pauses: Vec<(i64, Vec<String>, String)> = Vec::new();
    let mut unrecorded: Vec<(i64, String)> = Vec::new();
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

        // #767, #824: a column Trellis created, typed from columns of this
        // table, that the type define would give it now outgrew.
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
        // By the columns of this table each is typed from.
        let mut outgrown: BTreeMap<Vec<String>, Vec<copies::CopyState>> = BTreeMap::new();
        for state in copies::inspect(&*client, copies).await? {
            if !state.outgrown() {
                continue;
            }
            let from: Vec<String> = state.copy.columns_of(table).map(str::to_string).collect();
            // A copy of a key column flagged above is reported once, there.
            if state.copy.inferred.is_none() && columns.contains(&from[0]) {
                continue;
            }
            outgrown.entry(from).or_default().push(state);
        }
        for (from, states) in outgrown {
            flag(
                &from[0],
                outgrown_reason(table, &from, &live, &states),
                &mut columns,
            );
            for column in &from[1..] {
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
            pauses.push((reader.id, columns, error));
        }
    }

    // Recorded on its own: the next pass compares against it whether or not
    // this one pauses anything.
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
    if pauses.is_empty() {
        return Ok(false);
    }
    let txn = client.transaction().await?;
    for (id, columns, error) in pauses {
        if crate::defs::lifecycle::pause_for_capture_failure(&txn, id, table, &columns, &error)
            .await?
        {
            tracing::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    txn.commit().await?;
    Ok(true)
}

/// The reason a definition is paused for `states`, the columns it created
/// typed from `columns` of `table` whose types define would now give a
/// wider type ([`copies::CopyState::outgrown`]). `live` is `table`'s
/// columns.
fn outgrown_reason(
    table: &str,
    columns: &[String],
    live: &std::collections::HashMap<String, key_types::LiveColumn>,
    states: &[copies::CopyState],
) -> String {
    if let ([column], true) = (columns, states.iter().all(|s| s.copy.inferred.is_none())) {
        let held: Vec<String> = states
            .iter()
            .map(|s| format!("{} ({})", s.copy.label(), s.copy_type.display))
            .collect();
        return format!(
            "column {column:?} of {table} widened to {}, and Trellis keeps a copy of it that \
             can't hold every value of that type: {}",
            states[0].live_type.display,
            held.join(", ")
        );
    }
    let held: Vec<String> = states
        .iter()
        .map(|s| {
            format!(
                "{} ({}, now {})",
                s.copy.label(),
                s.copy_type.display,
                s.live_type.display
            )
        })
        .collect();
    let changed = match columns {
        [column] => format!(
            "column {column:?} of {table} widened to {}",
            live.get(column)
                .map(|c| c.display.as_str())
                .unwrap_or("another type")
        ),
        _ => {
            let named: Vec<String> = columns
                .iter()
                .map(|c| match live.get(c) {
                    Some(now) => format!("{c:?} ({})", now.display),
                    None => format!("{c:?}"),
                })
                .collect();
            format!("columns {} of {table} changed type", named.join(", "))
        }
    };
    format!(
        "{changed}, and Trellis keeps columns typed from {} that can't hold every value of the \
         types define would give them now: {}",
        if columns.len() == 1 { "it" } else { "them" },
        held.join(", ")
    )
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
