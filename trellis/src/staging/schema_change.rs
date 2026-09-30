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

/// What one table's markers in a drain's segments say is missing.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Missing {
    columns: BTreeSet<String>,
    key_missing: bool,
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
        let readers = readers_of(&catalog, table, &missing.columns, missing.key_missing);
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
        // The columns this definition reads, or every missing one when it
        // pauses only because the key went.
        let mut columns: Vec<String> = missing
            .columns
            .iter()
            .filter(|c| {
                readers_of(catalog, table, &BTreeSet::from([(*c).clone()]), false).contains(&id)
            })
            .cloned()
            .collect();
        if columns.is_empty() {
            columns = missing.columns.iter().cloned().collect();
        }
        if crate::defs::lifecycle::pause_for_capture_failure(txn, id, table, &columns).await? {
            tracing::warn!(
                transform_id = id,
                table = %table,
                columns = ?columns,
                "definition paused: a column it reads was renamed or dropped; \
                 resume it to rebuild once the column is back, or drop it"
            );
        }
    }
    Ok(())
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
/// Pausing before a marker does is sound: a pause only ever drops the
/// definition's share, and its resume is the rebuild either way.
pub(crate) async fn pause_readers_of_missing(
    client: &mut Client,
    schema: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let present: BTreeSet<String> = client
        .query(
            "select a.attname::text from pg_catalog.pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 and not a.attisdropped",
            &[&crate::defs::ddl::regclass_arg(table)],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if present.is_empty() {
        // No such table (or no columns): `capture_spec` reports it.
        return Ok(false);
    }
    let mut missing = Missing::default();
    if let Installed::Complete { spec, .. } = installed(&*client, schema, table).await? {
        missing.key_missing = spec.key().iter().any(|c| !present.contains(c));
        missing.columns.extend(
            spec.columns()
                .iter()
                .filter(|c| !present.contains(*c))
                .cloned(),
        );
    }
    missing.columns.extend(
        read_columns(catalog, table)
            .columns
            .into_iter()
            .filter(|c| !present.contains(c)),
    );
    if missing.columns.is_empty() && !missing.key_missing {
        return Ok(false);
    }
    let readers: Vec<i64> = readers_of(catalog, table, &missing.columns, missing.key_missing)
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
