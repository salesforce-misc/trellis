//! The Re-derive build's primitives (#625 F1; epic #556, ADR-0002 "A build
//! is Re-derive over chunks, and applies from its first chunk"): a build
//! chunk ([`run_chunk`]) and the group-delta merger ([`merge_deltas`]).
//!
//! **Nothing in production calls these yet.** They are driven by hand from
//! the tests (`tests/build_interleavings.rs`) until #625 F2 schedules them,
//! behind a knob that is off by default.
//!
//! # The chunk
//!
//! A chunk re-derives every source key in a `(lo, hi]` range of the source's
//! primary key, in the caller's one transaction:
//!
//! 1. read the keys in the range;
//! 2. lock their ledger entries as a page does
//!    ([`super::ledger::lock_entries`]: placeholders for keys with no entry,
//!    then a sorted `for update`), under the short [`CHUNK_LOCK_TIMEOUT`] so
//!    a drain page holding one of the keys makes the chunk give up rather
//!    than wait (ADR-0002 I7, #625 Q4);
//! 3. one statement ([`super::ledger::chunk_statement`]) reads
//!    `pg_current_snapshot()`, the active segment and the locked keys' source
//!    rows, rewrites their entries from them (`basis` := the snapshot,
//!    `applied_seg` raised to the segment, `applied_lsn` left alone, a key
//!    with no row a tombstone), and appends the moves' per-group increments
//!    to `<target>__deltas`.
//!
//! The chunk never touches a group row, so two chunks, or a chunk and a
//! page, never wait on each other's groups (#617's failure mode). A key
//! inserted after step 1 isn't locked, and its row is ignored: its insert's
//! change applies it. A chunk is idempotent: run again, it finds every entry
//! equal to the live row and appends nothing.
//!
//! Why a chunk and Apply agree: both hold a key's entry lock while they read
//! and write the entry. A chunk's snapshot is taken after its lock, so it sees
//! every change an earlier Apply folded in. A change applied after the chunk
//! is either visible in the chunk's basis (skipped, ADR-0002 I2) or not
//! (applied over the entry the chunk wrote, which is the snapshot's state). A
//! key with no entry has contributed nothing anywhere, so an Apply that comes
//! first counts it from nothing and the chunk then moves it by the
//! difference.
//!
//! # The merger
//!
//! [`merge_deltas`] claims up to `limit` delta rows no other merger holds
//! (`for update skip locked`), deletes them, sums them per group and upserts
//! the sums in group order, all in one statement
//! ([`super::ledger::merge_statement`]), with the same upsert Apply uses.
//! Then it deletes the groups whose every accumulator is 0 and hands every
//! written group to the target-mutation seam, as Apply does
//! ([`super::ledger::finish_groups`]). A merger locks only delta rows it
//! claims without waiting and group rows in group order, so it can't
//! deadlock with another merger or with a page.
//!
//! While a target is being built its groups have two channels, Apply and the
//! merger, so a group can be transiently partial or even negative. B5 (a
//! group is deleted only when every accumulator is 0) keeps such a group's
//! owed sums. Delta rows are discarded only with the ledger: a source
//! truncate (`super::ledger::truncate_ledger`), a drop, or the one-pass build
//! (#625 B4).
//!
//! # Shapes
//!
//! Only targets [`super::ledger::route`] sends to the ledger with every field
//! maintained by increments: `SUM`/`AVG` over an exact argument and `COUNT`.
//! A recomputed field (`MIN`/`MAX` and the rest) needs the merger to
//! recompute the groups it writes, which is #625 F5. [`BuildPlan::load`]
//! returns `None` for anything else.

use std::time::Duration;

use tokio_postgres::Transaction;

use crate::defs::ddl;
use crate::pool::{Pool, quote_ident};

use super::apply::ApplyError;
use super::ledger::{self, LedgerTargetPlan, WrittenGroup};
use super::target_mutations::TargetMutations;

/// How long a chunk waits for its entry lock before giving up (#625 Q4).
/// A drain page waits up to `locks::LOCK_TIMEOUT` for the same locks, so a
/// page never queues for long behind a chunk, and a chunk never holds up a
/// page that got there first. The caller retries the chunk later.
pub const CHUNK_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

/// What a Re-derive build needs to know about one target.
#[derive(Debug, Clone)]
pub struct BuildPlan {
    ledger: LedgerTargetPlan,
}

impl BuildPlan {
    /// The plan for the definition whose target is `target` (its bare
    /// name, as `TRANSFORM <target>` names it), or `None` when there is no
    /// such definition or its target isn't one a Re-derive build serves yet
    /// (see the module doc's "Shapes").
    pub async fn load(pool: &Pool, target: &str) -> Result<Option<Self>, ApplyError> {
        let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await?
        else {
            return Ok(None);
        };
        let Some(shape) = ledger::route(&definition.def, &definition.source_columns) else {
            return Ok(None);
        };
        if shape.recomputes() {
            return Ok(None);
        }
        let pk = ddl::source_primary_key(pool, &definition.source_table).await?;
        let identity = {
            let client = pool.get().await?;
            ddl::identity_key_columns(&**client, &definition.target_table).await?
        };
        Ok(Some(Self {
            ledger: LedgerTargetPlan::new(
                &definition.target_table,
                &definition.source_table,
                pk,
                identity,
                shape,
            ),
        }))
    }

    /// The target's qualified identity (`schema.table`).
    pub fn target(&self) -> &str {
        &self.ledger.target
    }
}

/// What one [`run_chunk`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkOutcome {
    /// The source keys the chunk found in its range, and re-derived.
    pub keys: usize,
    /// The group-delta rows it appended: one per group whose increments
    /// weren't all 0.
    pub delta_rows: i64,
}

/// Runs one build chunk, `(lo, hi]` of the target's source primary key, in
/// `txn` (see the module doc). `lo` and `hi` are encoded keys, as
/// `backfill_chunks` stores a range's bounds; `lo` is `None` for the first
/// chunk. The caller commits.
///
/// A lock wait past [`CHUNK_LOCK_TIMEOUT`] fails the chunk with `55P03`
/// (`crate::locks::is_lock_not_available`), and the caller rolls back and
/// retries it later.
///
/// The range predicate is the 1-1 build's row comparison, which admits a key
/// with a `NULL` part when an earlier part decides it. Only a source keyed
/// by a nullable unique index (another aggregate's target) has such keys,
/// and the Re-derive build doesn't take seam-fed sources before #625 F6.
pub async fn run_chunk(
    txn: &Transaction<'_>,
    plan: &BuildPlan,
    lo: Option<&str>,
    hi: &str,
) -> Result<ChunkOutcome, ApplyError> {
    let ledger = &plan.ledger;
    let source_table = ledger.source_table();
    let pk = ledger.source_pk();
    let decode = |text: &str| -> Result<Vec<String>, ApplyError> {
        Ok(ddl::split_pk_key(pk, source_table, text)?
            .into_iter()
            .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
            .collect())
    };
    let lo = lo.map(decode).transpose()?;
    let hi = decode(hi)?;
    let pk_idents: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    let range_where = crate::defs::backfill::pk_range_where(&pk_idents, pk, &lo);
    let mut params = crate::defs::backfill::range_params(&lo, &hi);

    // 1. The keys in the range.
    let keys: Vec<String> = txn
        .query(
            &format!(
                "select {} from {} s where {range_where}",
                ddl::pk_key_sql_expr(pk, Some("s")),
                ddl::qualified_source_table(source_table),
            ),
            &params,
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if keys.is_empty() {
        return Ok(ChunkOutcome {
            keys: 0,
            delta_rows: 0,
        });
    }
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();

    // 2. The entry lock, under the chunk's own short lock timeout.
    let previous: String = txn
        .query_one("select current_setting('lock_timeout')", &[])
        .await?
        .get(0);
    crate::locks::set_local_lock_timeout(txn, CHUNK_LOCK_TIMEOUT).await?;
    ledger::lock_entries(txn, ledger, &key_refs, false).await?;
    txn.execute("select set_config('lock_timeout', $1, true)", &[&previous])
        .await?;

    // 3. The read, the entries and the deltas, in one statement.
    let keys_param = format!("${}", params.len() + 1);
    params.push(&key_refs);
    let sql = ledger::chunk_statement(ledger, &range_where, &keys_param);
    let row = txn.query_one(&sql, &params).await?;
    // Test-only pause point (#623 D1), directly after the chunk's one
    // read-and-write statement. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterRederiveRead,
        &ledger.target,
    )
    .await?;
    let delta_rows: i64 = row.get(1);
    tracing::debug!(
        target_table = %ledger.target,
        keys = keys.len(),
        delta_rows,
        "build chunk re-derived its range"
    );
    Ok(ChunkOutcome {
        keys: keys.len(),
        delta_rows,
    })
}

/// What one [`merge_deltas`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeOutcome {
    /// The delta rows it claimed and deleted. Fewer than the `limit` means
    /// none were left that no other merger held.
    pub claimed: i64,
    /// The groups it wrote and kept.
    pub written: usize,
    /// The groups it emptied and deleted.
    pub deleted: usize,
}

/// Folds up to `limit` of the target's group-delta rows into its groups, in
/// `txn` (see the module doc). The caller commits.
pub async fn merge_deltas(
    txn: &Transaction<'_>,
    plan: &BuildPlan,
    limit: i64,
) -> Result<MergeOutcome, ApplyError> {
    let ledger = &plan.ledger;
    let mut mutations = TargetMutations::new();
    let image_columns = mutations.image_columns(txn, &ledger.target).await?;
    let sql = ledger::merge_statement(ledger, image_columns.as_deref());
    let rows = txn.query(&sql, &[&limit]).await?;
    // Test-only pause point, after the merger's upsert, with its claimed
    // delta rows and its groups locked. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterGroupUpsert,
        &ledger.target,
    )
    .await?;
    let claimed: i64 = rows.first().map_or(0, |row| row.get(0));
    let groups: Vec<WrittenGroup> = rows
        .iter()
        .filter_map(|row| WrittenGroup::from_row(row, 1))
        .collect();
    let (written, deleted) =
        ledger::finish_groups(txn, ledger, groups, &mut mutations, |_| (0, None, None)).await?;
    mutations.flush(txn).await?;
    Ok(MergeOutcome {
        claimed,
        written,
        deleted,
    })
}
