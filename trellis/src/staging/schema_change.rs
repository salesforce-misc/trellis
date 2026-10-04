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
/// correctly: pauses every definition that reads `table` and isn't paused
/// for a capture failure yet, recording why in `capture_failures`, when
///
/// - the table's row-level security policies now apply to the ring's owner
///   or to this worker's own role, which read it (issue #745,
///   [`crate::defs::row_security`]). RLS can be enabled or forced, or the
///   table handed to another owner, after the definitions were accepted; or
/// - a logical-replication subscription now replicates into it, whose
///   changes capture never sees (issue #751, [`crate::defs::subscription`]).
///   A subscription can be created, or refreshed to include the table,
///   after define.
///
/// Returns whether it paused any, in which case the caller leaves the table
/// for the next pass, whose catalog no longer counts them.
///
/// A definition only frozen (paused or quarantined) keeps its status but
/// gets the record, as for a schema change: its resume is the rebuild either
/// way, and the next pass pauses it again while the table is still
/// unsupported.
///
/// Costs no query for a table no unpaused definition reads.
pub(crate) async fn pause_readers_of_unsupported(
    client: &mut Client,
    schema: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let readers = unpaused_readers(catalog, table);
    if readers.is_empty() {
        return Ok(false);
    }
    let error = if let Some(rls) = crate::defs::row_security::applying(
        &*client,
        schema,
        table,
        crate::defs::row_security::Readers::RingAndSession,
    )
    .await?
    {
        row_security_error(&rls)
    } else if let Some(sub) = crate::defs::subscription::subscribed(&*client, table).await? {
        subscribed_error(&sub)
    } else {
        return Ok(false);
    };
    let txn = client.transaction().await?;
    for id in readers {
        if crate::defs::lifecycle::pause_for_capture_failure(&txn, id, table, &[], &error).await? {
            tracing::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    txn.commit().await?;
    Ok(true)
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
