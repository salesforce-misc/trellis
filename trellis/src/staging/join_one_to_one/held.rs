//! The rows of `a` a 1-1 `JOIN`'s fan-out holds in quarantine (ADR-0021).
//!
//! A `b` change recomputes every target row on its key ([`super::fan_out`]).
//! A field that fails on one of those rows with a data exception is the `a`
//! row's failure, not the `b` change's: the fan-out's bisection names the row
//! and the per-field probe its fields, and the page holds that row the way a
//! drain holds a key whose own change fails ([`hold`]), then commits the `b`
//! change and the rest of the fan-out, so `M` advances.
//!
//! - **Held in the page.** The `poison` row, a death, and a parked
//!   image-less Re-derive in `poison_held` are written in the page's
//!   transaction, so the row is held exactly when the `b` change commits.
//!   The row was reproduced in that transaction, on the `R` and `M` it
//!   commits with, so it is held on its first failure, as a build chunk
//!   holds a key (`super::super::quarantine::evict_build_key`). `poison`'s
//!   foreign key takes the definition's row `FOR KEY SHARE`, which the page
//!   took before its lock row ([`super::lock_definitions`]). The parked
//!   Re-derive carries the earliest origin of the page's `b` changes, so it
//!   holds back every watermark token those changes do (`converge`'s
//!   condition 4).
//! - **Left out after.** A held row's own changes are parked by the drain
//!   (`super::super::apply::compute`), a later fan-out leaves it out
//!   ([`not_held_sql`]), and so do the build's chunks and sweep. Its target
//!   row stays as it was.
//! - **Released by a Re-derive.** `release_key` (and the automatic retry)
//!   re-derives the `a` row in its own transaction ([`release`]), as a page
//!   of one `a` Re-derive: `R`'s entry from the live row, then the target
//!   row from it and the newest `M` entry on its key, under its `J` lock.
//!   It also stages a `Recompute` of the row, as for any held key. Leaving
//!   the Re-derive to that `Recompute` alone held the row again whenever a
//!   `b` change on its key drained first: its fan-out read the `R` entry the
//!   hold had left as it was, with the row's own changes parked, and failed
//!   on it again.
//! - **The fuses, after the commit** ([`charge_after_commit`]). Each failing
//!   field counts the row toward its column's fuse, as a drain's isolation
//!   charges a 1-1 column, and the definition's whole-transform fuse is
//!   checked over its held keys. Both run on their own transactions once the
//!   page has committed: a fuse's trip takes the definition's fences and
//!   rows, which the page's own locks would queue behind.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use crate::defs::alias_readers::AliasReaders;
use crate::defs::catalog;
use crate::defs::ledger as schema;
use crate::pool::{Pool, quote_ident};

use crate::defs::join::Side;
use crate::defs::model::Definition;

use super::super::apply::ApplyError;
use super::super::fold::FoldedChange;
use super::super::quarantine;
use super::{JoinOneToOnePlan, JoinShape};

/// One row of `a` a fan-out failed on: its `R` key, the error the row's
/// write failed with alone, and each field the per-field probe found failing,
/// with its error.
#[derive(Debug, Clone)]
pub(crate) struct HeldRow {
    pub(super) key: String,
    pub(super) error: String,
    pub(super) sqlstate: Option<String>,
    pub(super) fields: Vec<(String, String)>,
}

/// The origin a page's held rows are parked with: the earliest origin and
/// source-change time of the page's `b` changes, and their deepest `hop_gen`,
/// merged as a park merges a key's changes (`quarantine::HeldKey::merge`).
#[derive(Debug, Clone, Default)]
pub(crate) struct HeldOrigin {
    seen: bool,
    origin_lsn: Option<PgLsn>,
    src_changed: Option<SystemTime>,
    hop_gen: i32,
}

impl HeldOrigin {
    /// Adds a `b` change of the page.
    pub(super) fn add(&mut self, change: &FoldedChange) {
        if self.seen {
            self.origin_lsn =
                super::super::fold::earliest_origin(self.origin_lsn, change.origin_lsn);
            self.src_changed =
                super::super::apply::earliest_src_changed(self.src_changed, change.src_changed);
            self.hop_gen = self.hop_gen.max(change.hop_gen);
        } else {
            *self = HeldOrigin {
                seen: true,
                origin_lsn: change.origin_lsn,
                src_changed: change.src_changed,
                hop_gen: change.hop_gen,
            };
        }
    }
}

/// The rows one plan's page held, read once the page commits. Shared, so a
/// clone of the plan records into the same list.
#[derive(Debug, Clone, Default)]
pub(crate) struct Held(Arc<Mutex<Vec<HeldRow>>>);

impl Held {
    pub(super) fn record(&self, rows: Vec<HeldRow>) {
        if rows.is_empty() {
            return;
        }
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(rows);
    }

    fn take(&self) -> Vec<HeldRow> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// The SQL predicate over `R`'s row `alias`: its `a` row isn't held in
/// quarantine for the definition whose id is parameter `$id_param`, `a`
/// (canonical) being parameter `$table_param`. `poison`'s key is compared
/// under `R`'s key collation, `"C"`.
pub(super) fn not_held_sql(alias: &str, id_param: usize, table_param: usize) -> String {
    format!(
        "not exists (select 1 from poison p \
                     where p.transform_id = ${id_param}::bigint \
                       and p.src_table = ${table_param}::text \
                       and p.key collate \"C\" = {alias}.{key})",
        key = quote_ident(schema::KEY_COLUMN),
    )
}

/// Whether `shape`'s definition holds any row of `a` in quarantine: the
/// planted fan-out's condition (`crate::plant::Plant::Join1FanOutHeldRow`).
#[cfg(any(test, feature = "test-util"))]
pub(super) async fn holds_any(
    txn: &Transaction<'_>,
    shape: &JoinShape,
) -> Result<bool, ApplyError> {
    Ok(txn
        .query_opt(
            "select 1 from poison where transform_id = $1 and src_table = $2 limit 1",
            &[&shape.id, &shape.source_table],
        )
        .await?
        .is_some())
}

/// Holds `rows` of `a` in quarantine for `shape`'s definition, in the page's
/// transaction (the module doc): a `poison` row each, a death, and a parked
/// Re-derive in `poison_held` with the page's `seg_seq` and `origin`. Each
/// statement takes its rows in key order, as a park does
/// (`quarantine::park_batch_contribution`). A key held already is held again
/// with this failure, keeping the retry count its last release left.
pub(super) async fn hold(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    seg_seq: i64,
    origin: &HeldOrigin,
    rows: &[HeldRow],
) -> Result<(), ApplyError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut rows: Vec<&HeldRow> = rows.iter().collect();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    let keys: Vec<&str> = rows.iter().map(|r| r.key.as_str()).collect();
    let errors: Vec<&str> = rows.iter().map(|r| r.error.as_str()).collect();
    let codes: Vec<Option<&str>> = rows.iter().map(|r| r.sqlstate.as_deref()).collect();
    for row in &rows {
        crate::instance_log::warn!(
            transform = %shape.target,
            src_table = %shape.source_table,
            key = %row.key,
            last_error = %row.error,
            "holding a row of a 1-1 JOIN's FROM table in quarantine: a field fails on it in the \
             fan-out of a change of the joined table, which applies to every other row"
        );
    }
    let unnest = "unnest($3::text[], $4::text[], $5::text[]) as u(key, error, code)";
    txn.execute(
        &format!(
            "insert into key_deaths \
                 (transform_id, src_table, key, deaths, last_error, last_death_at) \
             select $1::bigint, $2::text, u.key, 1, u.error, now() from {unnest} \
             order by u.key collate \"C\" \
             on conflict (transform_id, src_table, key) do update set \
                 deaths = key_deaths.deaths + 1, \
                 last_error = excluded.last_error, \
                 last_death_at = now()"
        ),
        &[&shape.id, &shape.source_table, &keys, &errors, &codes],
    )
    .await?;
    txn.execute(
        &format!(
            "insert into poison \
                 (transform_id, src_table, key, last_error, sqlstate, retry_attempts) \
             select $1::bigint, $2::text, u.key, u.error, u.code, \
                    coalesce((select d.retry_attempts from key_deaths d \
                              where d.transform_id = $1 and d.src_table = $2 \
                                and d.key = u.key), 0) \
             from {unnest} order by u.key collate \"C\" \
             on conflict (transform_id, src_table, key) do update set \
                 last_error = excluded.last_error, sqlstate = excluded.sqlstate, \
                 poisoned_at = now(), retry_attempts = excluded.retry_attempts"
        ),
        &[&shape.id, &shape.source_table, &keys, &errors, &codes],
    )
    .await?;
    txn.execute(
        &format!(
            "insert into poison_held as h \
                 (transform_id, src_table, key, seg_seq, origin_lsn, src_changed, hop_gen) \
             select $1::bigint, $2::text, u.key, $4::bigint, $5::pg_lsn, $6::timestamptz, \
                    $7::integer \
             from unnest($3::text[]) as u(key) order by u.key collate \"C\" \
             {merge}",
            merge = quarantine::MERGE_HELD_ROW,
        ),
        &[
            &shape.id,
            &shape.source_table,
            &keys,
            &seg_seq,
            &origin.origin_lsn,
            &origin.src_changed,
            &origin.hop_gen,
        ],
    )
    .await?;
    Ok(())
}

/// After a page commits: charges the fuses for the rows each of `plans` held
/// (the module doc). A failing field is charged unless a field it reads by
/// alias fails on the row too, as a drain's isolation charges a 1-1 column.
/// A failure here leaves the rows held, and is logged: the page has
/// committed, and the next row a fuse is charged for checks it again.
pub(crate) async fn charge_after_commit<'a>(
    pool: &Pool,
    plans: impl IntoIterator<Item = &'a JoinOneToOnePlan>,
) {
    for plan in plans {
        let rows = plan.held.take();
        if rows.is_empty() {
            continue;
        }
        if let Err(err) = charge(pool, &plan.shape, &rows).await {
            crate::instance_log::warn!(
                transform = %plan.shape.target,
                error = %err,
                "could not charge the fuses for the rows a 1-1 JOIN's fan-out held; they stay \
                 held"
            );
        }
    }
}

async fn charge(pool: &Pool, shape: &JoinShape, rows: &[HeldRow]) -> Result<(), ApplyError> {
    let Some(definition) = catalog::definition_by_id(pool, shape.id).await? else {
        return Ok(());
    };
    let readers = AliasReaders::of(&definition.def);
    for row in rows {
        let mut explained: HashSet<String> = HashSet::new();
        for (field, _) in &row.fields {
            let mut closure = HashSet::from([field.clone()]);
            readers.close(&mut closure);
            closure.remove(field);
            explained.extend(closure);
        }
        for (field, error) in &row.fields {
            if explained.contains(field) {
                continue;
            }
            quarantine::charge_column_failure(
                pool,
                &definition.def.target,
                field,
                &shape.source_table,
                &row.key,
                error,
            )
            .await?;
        }
    }
    quarantine::trip_drain_fuse(pool, shape.id).await?;
    Ok(())
}

/// A release's (or the automatic retry's) Re-derive of `key` of `src_table`
/// (canonical) for `definition`, in the release's transaction, when
/// `definition` is a 1-1 `JOIN` that isn't frozen (a frozen one's `RESUME`
/// re-derives every row) and `src_table` its `FROM` table: a page of
/// that one `a` Re-derive, with the target write (the module doc). The
/// caller holds `a`'s version fence and the definition's row, and has
/// deleted nothing yet, so a page that holds the key's `J` row and holds the
/// row again meanwhile commits first, and the release then deletes that too.
///
/// A Re-derive that fails on a data exception (the row still fails) or on a
/// key that moved under it is rolled back to a savepoint and left to the
/// `Recompute` the release stages; a lock wait past the session's
/// `lock_timeout` is [`ApplyError::ReleaseLockTimeout`], changing nothing.
pub(crate) async fn release(
    txn: &Transaction<'_>,
    pool: &Pool,
    definition: &Definition,
    transform: &str,
    src_table: &str,
    key: &str,
) -> Result<(), ApplyError> {
    if !crate::defs::join::is_one_to_one(&definition.def)
        || definition.source_table != src_table
        || definition.status.is_frozen()
    {
        return Ok(());
    }
    // Planted bug: the release leaves the Re-derive to its `Recompute`. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if crate::plant::fires(crate::plant::Plant::Join1ReleaseNoRederive, true) {
        return Ok(());
    }
    let pk = crate::defs::ddl::source_primary_key(pool, &definition.source_table).await?;
    let paused = quarantine::paused_columns_for(pool, &definition.def).await?;
    let shape = Arc::new(JoinShape::load(pool, definition, pk, &paused).await?);
    txn.batch_execute("savepoint join1_release").await?;
    match super::build::rederive(txn, &shape, Side::From, &[key.to_string()], true).await {
        Ok(_) => {
            txn.batch_execute("release savepoint join1_release").await?;
            Ok(())
        }
        Err(err) if crate::locks::is_lock_not_available(&err) => {
            Err(ApplyError::ReleaseLockTimeout {
                transform: transform.to_string(),
                src_table: src_table.to_string(),
                key: key.to_string(),
            })
        }
        Err(err)
            if super::super::one_to_one_render::is_data_exception(&err)
                || quarantine::is_join_key_moved(&err) =>
        {
            crate::instance_log::info!(
                transform,
                src_table,
                key,
                error = %err,
                "a released row of a 1-1 JOIN's FROM table failed its Re-derive; the \
                 Recompute the release stages re-derives it"
            );
            txn.batch_execute(
                "rollback to savepoint join1_release; release savepoint join1_release",
            )
            .await?;
            Ok(())
        }
        Err(err) => Err(err),
    }
}
