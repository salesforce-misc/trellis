//! 1-1 targets on the ledger (#623 D6; epic #556, ADR-0002 invariants I1,
//! I2 and I5).
//!
//! A 1-1 target's ledger (`<target>__ledger`, [`crate::defs::ledger`]) holds
//! one entry per source key with only the ordering state: `applied_lsn`,
//! `applied_seg`, `basis` and `tombstone`. The target row holds the values.
//! A page settles each 1-1 target's records in four steps
//! (`super::apply::apply_page`, step 3):
//!
//! 1. **Lock** ([`lock_entries`], I5): an entry for every key with no
//!    entry, then every other entry `for update`, sorted by key. A new key's
//!    Apply is settled by its insert, since with no entry I2 is only the
//!    truncate floor; a Re-derive's is a placeholder. A key whose tombstone
//!    the GC collects between the two fails the page transiently, as on an
//!    aggregate ledger (`super::ledger::lock_entries`, #712).
//! 2. **Re-derive read** ([`read_rows`], I1): the Re-derived keys' source
//!    rows, `pg_current_snapshot()` and the newest segment in one statement,
//!    after the lock. The rows are evaluated in Rust, as Phase 2 evaluates
//!    an Apply's image.
//! 3. **The entries** ([`update_entries`], I2): a Re-derive sets `basis` to
//!    the read's snapshot and leaves `applied_lsn` alone (the D split's Q1);
//!    an Apply sets `applied_lsn`, but only if its transaction is not visible
//!    in `basis`, its `lsn` is above `applied_lsn` and it is above the
//!    target's truncate floor. Either makes a tombstone when the key has no
//!    row, and only a tombstone has its `applied_seg` raised: to the page's
//!    latest segment, or to the read's segment when that is newer (#742, see
//!    [`read_rows`]), so an Apply to a live entry can be HOT
//!    ([`schema::tombstone_seg_sql`], #775). Returns the keys it changed.
//!    Step 1's settled Applies skip it. A placeholder step 1 inserted that
//!    neither step wrote (an Apply at or below the floor, or a Re-derive the
//!    page skipped) is deleted ([`drop_placeholders`], #774): the tombstone
//!    GC collects only tombstones.
//! 4. The target rows of exactly those keys are upserted or deleted, as
//!    before (`super::apply::apply_target`).
//!
//! An Apply the predicate refuses was already reflected by a Re-derive whose
//! snapshot saw it, or overtaken by a later change: writing it would put an
//! older state over a newer one (#344, #392). The entry lock is what makes
//! the check and the write one step: every writer of a key's target row
//! holds its entry.
//!
//! The Re-derive build's 1-1 chunks (`super::build::one_to_one`) write target
//! rows outside a page; each locks the entries of the keys it writes the
//! same way. A whole build's chunk stamps them as a Re-derive does; a field
//! build's chunk (`ALTER TRANSFORM`, a column resume, #625 F8b) rewrites
//! some columns only and leaves them as they are.

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

/// The keys [`lock_entries`] inserted an entry for.
#[derive(Debug, Default)]
pub(crate) struct Inserted {
    /// Every key that had no entry. Its entry is this transaction's own
    /// row, so it is locked already.
    pub keys: HashSet<String>,
    /// The Apply keys among them whose change the insert recorded, as
    /// [`update_entries`] would have: with no entry, I2 holds unless the
    /// change is at or below the truncate floor.
    pub applied: HashSet<String>,
}

/// Locks the entries of `changes`' keys on `target`'s ledger, sorted; see
/// the module doc. A key with no entry gets one: an Apply's records its
/// change (`seg_seq` and the tombstone as [`update_entries`] sets them), and
/// a Re-derive's, or that of an Apply below the truncate floor, is a
/// placeholder, and a Re-derive's `present` is ignored. That spares a new
/// key's entry a second version and a lock (#623 D6's throughput miss).
/// Only the keys that had an entry are then locked `for update`. `target`
/// is the target's qualified identity; `predicate` is as for
/// [`update_entries`]. `skip_lock` is the `chunk_without_entry_lock`
/// plant's (#625 F3, a build chunk's only): the existing entries are read
/// without the lock.
pub(crate) async fn lock_entries(
    txn: &Transaction<'_>,
    target: &str,
    changes: &[EntryChange<'_>],
    seg_seq: i64,
    predicate: bool,
    skip_lock: bool,
) -> Result<Inserted, ApplyError> {
    let ledger = ledger_ident(target);
    let q = |c: &str| quote_ident(c);
    let (key_col, applied, seg, tombstone) = (
        q(schema::KEY_COLUMN),
        q(schema::APPLIED_LSN_COLUMN),
        q(schema::APPLIED_SEG_COLUMN),
        q(schema::TOMBSTONE_COLUMN),
    );
    let keys: Vec<&str> = changes.iter().map(|c| c.key).collect();
    let lsns: Vec<Option<String>> = changes
        .iter()
        .map(|c| c.apply.map(|(lsn, _)| lsn.to_string()))
        .collect();
    let present: Vec<bool> = changes.iter().map(|c| c.present).collect();
    let above_floor = if predicate {
        "not exists (select 1 from fl where v.__lsn <= fl.floor)"
    } else {
        "true"
    };
    let rows = txn
        .query(
            &format!(
                "with v as ( \
                     select u.__k, u.__lsn::pg_lsn as __lsn, u.__present \
                     from unnest($1::text[], $2::text[], $3::bool[]) as u(__k, __lsn, __present) \
                 ), \
                 fl as (select floor from ledger_truncate_floor where target_table = $4) \
                 insert into {ledger} ({key_col}, {applied}, {seg}, {tombstone}) \
                 select v.__k, case when o.ok then v.__lsn end, \
                        {stamp}, o.ok and not v.__present \
                 from v cross join lateral \
                      (select v.__lsn is not null and {above_floor}) as o(ok) \
                 order by v.__k collate \"C\" \
                 on conflict do nothing \
                 returning {key_col}, {applied} is not null",
                stamp = schema::tombstone_seg_sql(None, "o.ok and not v.__present", "$5::bigint"),
            ),
            &[&keys, &lsns, &present, &target, &seg_seq],
        )
        .await?;
    let mut inserted = Inserted::default();
    for row in rows {
        let key: String = row.get(0);
        if row.get::<_, bool>(1) {
            inserted.applied.insert(key.clone());
        }
        inserted.keys.insert(key);
    }
    // Test-only pause point (#623 D7). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterPlaceholders,
        target,
    )
    .await?;
    let existing: Vec<&str> = keys
        .iter()
        .copied()
        .filter(|k| !inserted.keys.contains(*k))
        .collect();
    if !existing.is_empty() && !skip_lock {
        let locked = super::ledger::query_by_entry_key(
            txn,
            &super::ledger::lock_statement(&ledger),
            &[&existing],
        )
        .await?
        .len();
        if locked < existing.len() {
            return Err(ApplyError::LedgerEntryCollected {
                target: target.to_string(),
            });
        }
    }
    // Test-only pause point (#623 D1). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(txn, super::interleave::PausePoint::AfterEntryLock, target).await?;
    Ok(inserted)
}

/// What [`read_rows`] read: the rows, by key text, the snapshot, and the
/// newest segment the snapshot sees.
pub(crate) struct ReadRows {
    pub rows: HashMap<String, Row>,
    pub snapshot: String,
    pub seg: i64,
}

/// The Re-derive read: the current source rows of `keys` (each `columns`
/// as text, keyed by the row's key text), `pg_current_snapshot()` and the
/// newest segment (`max(seg_seq)`), in one statement. A key with no row is
/// absent from the map. `keys` must not be empty.
///
/// **The segment (#742).** The read is live, so its snapshot sees changes
/// in batches newer than the page's own, and the entries it writes must be
/// stamped with a segment at least as new as every batch holding a change
/// the snapshot saw. Otherwise the tombstone GC
/// (`super::retire::collect_tombstones`) can collect a Re-derive's tombstone
/// while such a batch is still pending: the key's later delete was refused
/// (the snapshot saw it), and the pending batch's older change then applies
/// to a fresh entry and brings the key back. Every change the snapshot sees
/// is in the newest segment it sees or an earlier one, as for a build
/// chunk's stamp (`super::ledger::chunk_statement`).
pub(crate) async fn read_rows(
    txn: &Transaction<'_>,
    // Only the pause point below reads it.
    #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_variables))] target: &str,
    source_table: &str,
    pk: &[PrimaryKeyColumn],
    columns: &[String],
    keys: &[&str],
) -> Result<ReadRows, ApplyError> {
    let query = super::apply::live_rows_query(source_table, pk, columns, keys)?;
    let sql = format!(
        "select null::text, (select coalesce(max(seg_seq), 0) from segments)::text, \
                pg_catalog.pg_current_snapshot()::text \
         union all select m.k, e.key, e.value from ({}) m \
         cross join lateral jsonb_each_text(m.doc) e",
        query.docs
    );
    let mut rows: HashMap<String, Row> = HashMap::new();
    let mut snapshot = String::new();
    let mut seg = 0;
    for row in super::ledger::query_by_entry_key(txn, &sql, &query.params()).await? {
        match row.get::<_, Option<String>>(0) {
            None => {
                seg = row
                    .get::<_, Option<String>>(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                snapshot = row.get(2);
            }
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
    Ok(ReadRows {
        rows,
        snapshot,
        seg,
    })
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
/// Re-derive read's, and `seg_seq` the page's latest segment, which a
/// tombstone's `applied_seg` is raised to. Returns the keys it changed. `predicate`
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
    let keys: Vec<&str> = changes.iter().map(|c| c.key).collect();
    let rederive: Vec<bool> = changes.iter().map(|c| c.apply.is_none()).collect();
    let lsns: Vec<Option<String>> = changes
        .iter()
        .map(|c| c.apply.map(|(lsn, _)| lsn.to_string()))
        .collect();
    let txids: Vec<Option<&str>> = changes.iter().map(|c| c.apply.map(|(_, t)| t)).collect();
    let present: Vec<bool> = changes.iter().map(|c| c.present).collect();
    let rows = super::ledger::query_by_entry_key(
        txn,
        &update_statement(target, predicate),
        &[
            &keys, &rederive, &lsns, &txids, &present, &target, &snapshot, &seg_seq,
        ],
    )
    .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// [`update_entries`]' statement on `target`'s ledger. Binds `$1` the keys,
/// `$2` the Re-derive flags, `$3` the `lsn`s and `$4` the `row_txid`s (null
/// for a Re-derive), `$5` whether each key has a row after its change, `$6`
/// the target, `$7` the Re-derive read's snapshot and `$8` the page's
/// latest segment. Returns the keys it changed. It reads the ledger only
/// through `$1`'s keys, and runs under
/// `super::ledger::ENTRY_PLAN_SETTINGS` (#778).
fn update_statement(target: &str, predicate: bool) -> String {
    let q = |c: &str| quote_ident(c);
    let (key, basis, applied, seg, tombstone) = (
        q(schema::KEY_COLUMN),
        q(schema::BASIS_COLUMN),
        q(schema::APPLIED_LSN_COLUMN),
        q(schema::APPLIED_SEG_COLUMN),
        q(schema::TOMBSTONE_COLUMN),
    );
    let predicate = if predicate {
        super::ledger::apply_predicate(true)
    } else {
        "true".to_string()
    };
    format!(
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
             {seg} = {stamp}, \
             {tombstone} = not v.__present \
         from v \
         where l.{key} = any($1::text[]) and l.{key} = v.__k \
           and (v.__rederive or ({predicate})) \
         returning l.{key}",
        ledger = ledger_ident(target),
        stamp =
            schema::tombstone_seg_sql(Some(&format!("l.{seg}")), "not v.__present", "$8::bigint"),
    )
}

/// Deletes the placeholders among `keys` on `target`'s ledger: the entries
/// [`lock_entries`] inserted that nothing then wrote (#774). `keys` are the
/// keys whose entry this transaction inserted and neither its insert nor
/// [`update_entries`] changed: an Apply at or below the truncate floor, and
/// a Re-derive the page skipped (re-staged, or a direct writer's row that
/// failed to evaluate). Such an entry has no `applied_lsn`, no `basis` and
/// no tombstone, so the tombstone GC would never collect it, and I2 treats
/// it as it treats no entry at all, so deleting it changes no later
/// change's outcome. The statement re-checks that state, so it can't delete
/// an entry something wrote.
pub(crate) async fn drop_placeholders(
    txn: &Transaction<'_>,
    target: &str,
    keys: &[&str],
) -> Result<(), ApplyError> {
    if keys.is_empty() {
        return Ok(());
    }
    let q = |c: &str| quote_ident(c);
    txn.execute(
        &format!(
            "delete from {ledger} where {key} = any($1::text[]) \
               and {applied} is null and {basis} is null and not {tombstone}",
            ledger = ledger_ident(target),
            key = q(schema::KEY_COLUMN),
            applied = q(schema::APPLIED_LSN_COLUMN),
            basis = q(schema::BASIS_COLUMN),
            tombstone = q(schema::TOMBSTONE_COLUMN),
        ),
        &[&keys],
    )
    .await?;
    Ok(())
}

/// The plans of a page's statements that read the 1-1 target `target`'s
/// ledger by entry key (#778), as `explain`'s text, each labelled, under the
/// settings a page runs them with: the entry lock
/// (`super::ledger::lock_statement`) and [`update_entries`]' statement, for
/// a page of Applies to `keys`, which already have entries. `target` is the
/// target's qualified identity. For tests of the plans' shape. It locks and
/// writes nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_page(
    pool: &crate::pool::Pool,
    target: &str,
    keys: &[&str],
) -> Result<Vec<(&'static str, String)>, ApplyError> {
    let n = keys.len();
    let rederive = vec![false; n];
    let lsns: Vec<Option<String>> = vec![Some("0/10".to_string()); n];
    let txids: Vec<Option<&str>> = vec![Some("100"); n];
    let present = vec![true; n];
    let snapshot: Option<String> = None;
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let explain = |rows: Vec<tokio_postgres::Row>| {
        rows.iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let lock = explain(
        super::ledger::query_by_entry_key(
            &txn,
            &format!(
                "explain {}",
                super::ledger::lock_statement(&ledger_ident(target))
            ),
            &[&keys],
        )
        .await?,
    );
    let update = explain(
        super::ledger::query_by_entry_key(
            &txn,
            &format!("explain {}", update_statement(target, true)),
            &[
                &keys, &rederive, &lsns, &txids, &present, &target, &snapshot, &1_i64,
            ],
        )
        .await?,
    );
    txn.rollback().await?;
    Ok(vec![("entry lock", lock), ("entry update", update)])
}

/// Empties a 1-1 target's ledger for a source `TRUNCATE` and raises its
/// truncate floor to `lsn`, the truncate's ring `lsn` (the D split's Q6, as
/// `super::ledger::truncate_ledger` does for an aggregate). The caller
/// clears the target rows. `lsn` is required: a truncate that raised no
/// floor would let a change from before it apply over it (#774).
pub(crate) async fn truncate(
    txn: &Transaction<'_>,
    target: &str,
    lsn: PgLsn,
) -> Result<(), ApplyError> {
    txn.batch_execute(&format!("truncate {}", ledger_ident(target)))
        .await?;
    txn.execute(
        "insert into ledger_truncate_floor (target_table, floor) values ($1, $2) \
         on conflict (target_table) do update \
         set floor = greatest(ledger_truncate_floor.floor, excluded.floor)",
        &[&target, &lsn],
    )
    .await?;
    Ok(())
}
