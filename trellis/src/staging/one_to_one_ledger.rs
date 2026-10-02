//! 1-1 targets on the ledger (#623 D6; epic #556, ADR-0002 invariants I1,
//! I2 and I5).
//!
//! A 1-1 target's ledger (`<target>__ledger`, [`crate::defs::ledger`]) holds
//! one entry per source key with only the ordering state: `applied_lsn`,
//! `applied_seg`, `basis` and `tombstone`. The target row holds the values.
//! A page settles each 1-1 target's records in four steps
//! (`super::apply::apply_page`, step 3):
//!
//! 1. **Lock** ([`lock_entries`], I5): a placeholder for every key with no
//!    entry, then every entry `for update`, sorted by key. A key whose
//!    tombstone the GC collects between the two fails the page transiently,
//!    as on an aggregate ledger (`super::ledger::lock_entries`, #712).
//! 2. **Re-derive read** ([`read_rows`], I1): the Re-derived keys' source
//!    rows and `pg_current_snapshot()` in one statement, after the lock. The
//!    rows are evaluated in Rust, as Phase 2 evaluates an Apply's image.
//! 3. **The entries** ([`update_entries`], I2): a Re-derive sets `basis` to
//!    the read's snapshot and leaves `applied_lsn` alone (the D split's Q1);
//!    an Apply sets `applied_lsn`, but only if its transaction is not visible
//!    in `basis`, its `lsn` is above `applied_lsn` and it is above the
//!    target's truncate floor. Either makes a tombstone when the key has no
//!    row. Returns the keys it changed.
//! 4. The target rows of exactly those keys are upserted or deleted, as
//!    before (`super::apply::apply_target`).
//!
//! An Apply the predicate refuses was already reflected by a Re-derive whose
//! snapshot saw it, or overtaken by a later change: writing it would put an
//! older state over a newer one (#344, #392). The entry lock is what makes
//! the check and the write one step: every writer of a key's target row
//! holds its entry.
//!
//! `ALTER TRANSFORM`'s backfill (`crate::defs::backfill::backfill_altered_columns`)
//! and a column resume (`super::quarantine::recompute_column`) write target
//! rows outside a page; each locks the entries of the keys it writes the
//! same way and stamps them as a Re-derive does.

use std::collections::{HashMap, HashSet};

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::eval::Row;
use crate::defs::ledger as schema;
use crate::pool::quote_ident;

use super::apply::ApplyError;

/// A 1-1 target's ledger, quoted for SQL text, from the target's qualified
/// identity.
pub(crate) fn ledger_ident(target: &str) -> String {
    ddl::qualified_target_table_ident(&schema::ledger_table_name(target))
}

/// Locks the entries of `keys` on `target`'s ledger, sorted, inserting a
/// placeholder for a key with none first; see the module doc. `target` is
/// the target's qualified identity.
pub(crate) async fn lock_entries(
    txn: &Transaction<'_>,
    target: &str,
    keys: &[&str],
) -> Result<(), ApplyError> {
    let ledger = ledger_ident(target);
    let key_col = quote_ident(schema::KEY_COLUMN);
    let mut distinct = keys.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    txn.execute(
        &format!(
            "insert into {ledger} ({key_col}) \
             select k from unnest($1::text[]) as k order by k \
             on conflict do nothing"
        ),
        &[&distinct],
    )
    .await?;
    // Test-only pause point (#623 D7). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterPlaceholders,
        target,
    )
    .await?;
    let locked = txn
        .execute(
            &format!(
                "select 1 from {ledger} where {key_col} = any($1::text[]) \
                 order by {key_col} for update"
            ),
            &[&distinct],
        )
        .await?;
    if (locked as usize) < distinct.len() {
        return Err(ApplyError::LedgerEntryCollected {
            target: target.to_string(),
        });
    }
    // Test-only pause point (#623 D1). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(txn, super::interleave::PausePoint::AfterEntryLock, target).await?;
    Ok(())
}

/// The Re-derive read: the current source rows of `keys` (each `columns`
/// as text, keyed by the row's key text) and `pg_current_snapshot()`, in one
/// statement. A key with no row is absent from the map. `keys` must not be
/// empty.
pub(crate) async fn read_rows(
    txn: &Transaction<'_>,
    // Only the pause point below reads it.
    #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_variables))] target: &str,
    source_table: &str,
    pk: &[PrimaryKeyColumn],
    columns: &[String],
    keys: &[&str],
) -> Result<(HashMap<String, Row>, String), ApplyError> {
    let query = super::apply::live_rows_query(source_table, pk, columns, keys)?;
    let sql = format!(
        "select null::text, null::text, pg_catalog.pg_current_snapshot()::text \
         union all select m.k, e.key, e.value from ({}) m \
         cross join lateral jsonb_each_text(m.doc) e",
        query.docs
    );
    let mut rows: HashMap<String, Row> = HashMap::new();
    let mut snapshot = String::new();
    for row in txn.query(&sql, &query.params()).await? {
        match row.get::<_, Option<String>>(0) {
            None => snapshot = row.get(2),
            Some(key) => {
                rows.entry(key).or_default().insert(row.get(1), row.get(2));
            }
        }
    }
    // Test-only pause point (#623 D1), directly after the one
    // read-and-snapshot statement. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterRederiveRead,
        target,
    )
    .await?;
    Ok((rows, snapshot))
}

/// One locked key's change to its entry: a Re-derive (`apply` `None`) or an
/// Apply of the change at `(lsn, txid)`. `present` says whether the key has
/// a row after it.
#[derive(Debug)]
pub(crate) struct EntryChange<'a> {
    pub key: &'a str,
    pub apply: Option<(PgLsn, &'a str)>,
    pub present: bool,
}

/// Updates the locked entries of `changes` on `target`'s ledger, an Apply
/// only if ADR-0002's I2 holds for it; see the module doc. `snapshot` is the
/// Re-derive read's, and `seg_seq` the page's latest segment, which
/// `applied_seg` is raised to. Returns the keys it changed. `predicate`
/// false is the `stale_one_to_one_write` plant's: every Apply changes its
/// entry.
pub(crate) async fn update_entries(
    txn: &Transaction<'_>,
    target: &str,
    changes: &[EntryChange<'_>],
    snapshot: Option<&str>,
    seg_seq: i64,
    predicate: bool,
) -> Result<HashSet<String>, ApplyError> {
    if changes.is_empty() {
        return Ok(HashSet::new());
    }
    let q = |c: &str| quote_ident(c);
    let (key, basis, applied, seg, tombstone) = (
        q(schema::KEY_COLUMN),
        q(schema::BASIS_COLUMN),
        q(schema::APPLIED_LSN_COLUMN),
        q(schema::APPLIED_SEG_COLUMN),
        q(schema::TOMBSTONE_COLUMN),
    );
    let keys: Vec<&str> = changes.iter().map(|c| c.key).collect();
    let rederive: Vec<bool> = changes.iter().map(|c| c.apply.is_none()).collect();
    let lsns: Vec<Option<String>> = changes
        .iter()
        .map(|c| c.apply.map(|(lsn, _)| lsn.to_string()))
        .collect();
    let txids: Vec<Option<&str>> = changes.iter().map(|c| c.apply.map(|(_, t)| t)).collect();
    let present: Vec<bool> = changes.iter().map(|c| c.present).collect();
    let predicate = if predicate {
        super::ledger::apply_predicate(true)
    } else {
        "true".to_string()
    };
    let rows = txn
        .query(
            &format!(
                "with v as ( \
                     select u.__k, u.__rederive, u.__lsn::pg_lsn as __lsn, \
                            u.__txid::xid8 as __txid, u.__present \
                     from unnest($1::text[], $2::bool[], $3::text[], $4::text[], $5::bool[]) \
                          as u(__k, __rederive, __lsn, __txid, __present) \
                 ), \
                 fl as (select floor from ledger_truncate_floor where target_table = $6) \
                 update {ledger} l set \
                     {basis} = case when v.__rederive then $7::text::pg_snapshot else l.{basis} end, \
                     {applied} = case when v.__rederive then l.{applied} else v.__lsn end, \
                     {seg} = greatest(l.{seg}, $8::bigint), \
                     {tombstone} = not v.__present \
                 from v \
                 where l.{key} = v.__k and (v.__rederive or ({predicate})) \
                 returning l.{key}",
                ledger = ledger_ident(target),
            ),
            &[
                &keys, &rederive, &lsns, &txids, &present, &target, &snapshot, &seg_seq,
            ],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Empties a 1-1 target's ledger for a source `TRUNCATE` and raises its
/// truncate floor to `lsn`, the truncate's ring `lsn` (the D split's Q6, as
/// `super::ledger::truncate_ledger` does for an aggregate). The caller
/// clears the target rows.
pub(crate) async fn truncate(
    txn: &Transaction<'_>,
    target: &str,
    lsn: Option<PgLsn>,
) -> Result<(), ApplyError> {
    txn.batch_execute(&format!("truncate {}", ledger_ident(target)))
        .await?;
    if let Some(lsn) = lsn {
        txn.execute(
            "insert into ledger_truncate_floor (target_table, floor) values ($1, $2) \
             on conflict (target_table) do update \
             set floor = greatest(ledger_truncate_floor.floor, excluded.floor)",
            &[&target, &lsn],
        )
        .await?;
    }
    Ok(())
}
