//! The Re-derive build (#625; epic #556, ADR-0002 "A build is Re-derive over
//! chunks, and applies from its first chunk"): its primitives, a build chunk
//! ([`run_chunk`]) and the group-delta merger ([`merge_deltas`]) (F1), and
//! their scheduling (F2), behind `ClientOptions::rederive_build`, which is
//! off by default.
//!
//! # The scheduled build (F2)
//!
//! - **Start** ([`start_ready_builds`]). The staging worker's reconcile pass
//!   starts each ready definition that [`qualifies`] instead of parking it a
//!   registration marker: once its source's capture gate is clear, one
//!   transaction moves it `waiting_to_backfill -> backfilling` with
//!   `transform_definitions.build = 'rederive'` and enqueues its plan job.
//!   From that commit it applies (`Definition::applies`, B1): Apply folds its
//!   source's changes into its ledger and groups, so nothing needs a
//!   catch-up.
//! - **Plan** ([`run_plan`]). A drain worker walks the source's primary key
//!   and enqueues `rederive` chunks of `ClientOptions::build_chunk_rows`
//!   rows, [`PLAN_BATCH`] per transaction (Q13).
//! - **Work order** ([`work_once`], B6). A drain worker takes segments first,
//!   then a merge when a building target has deltas, then one chunk, and a
//!   chunk only under the ring's backlog bound ([`chunk_allowed`]). A chunk's
//!   entry lock gives up after [`CHUNK_LOCK_TIMEOUT`], and the chunk is
//!   retried after a short backoff without a charge.
//! - **Live** ([`try_complete`], B7). The flip is strict: the plan job is
//!   done, every chunk is done, and the delta table is empty, checked under
//!   the definition's row lock. No catch-up is parked.
//!
//! A pause freezes the build where it is: claims and merges skip a frozen
//! definition, and its deltas stay. A drop takes the chunk rows (`on delete
//! cascade`) and the delta table with the target.
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

use std::time::{Duration, Instant};

use tokio_postgres::{GenericClient, IsolationLevel, Transaction};

use crate::defs::chunk_queue::{self, ChunkQueueError, ChunkWork, ClaimFence, ClaimedChunk};
use crate::defs::model::{Definition, TransformStatus};
use crate::defs::{catalog, ddl};
use crate::metrics::{self, BuildStatement};
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
    #[cfg(any(test, feature = "internals"))]
    pub async fn load(pool: &Pool, target: &str) -> Result<Option<Self>, ApplyError> {
        let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await?
        else {
            return Ok(None);
        };
        Self::for_definition(pool, &definition).await
    }

    /// [`Self::load`] for a definition already read.
    pub(crate) async fn for_definition(
        pool: &Pool,
        definition: &Definition,
    ) -> Result<Option<Self>, ApplyError> {
        let Some(shape) = buildable_shape(definition) else {
            return Ok(None);
        };
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
/// `txn` must be `read committed`, as a page's is: step 3's snapshot has to
/// be its own statement's, taken after the entry lock (ADR-0002 I1). Under
/// `repeatable read` it would be the transaction's, from step 1, and the
/// chunk could rewrite an entry over a change a page applied between.
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
    let started = Instant::now();
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
    metrics::record_build_statement(BuildStatement::ChunkKeys, started.elapsed());
    if keys.is_empty() {
        return Ok(ChunkOutcome {
            keys: 0,
            delta_rows: 0,
        });
    }
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();

    // 2. The entry lock, under the chunk's own short lock timeout. Its time
    // is recorded whether or not the lock is had: a chunk that gives up
    // spent it waiting all the same.
    let started = Instant::now();
    let locked = async {
        let previous: String = txn
            .query_one("select current_setting('lock_timeout')", &[])
            .await?
            .get(0);
        crate::locks::set_local_lock_timeout(txn, CHUNK_LOCK_TIMEOUT).await?;
        ledger::lock_entries(txn, ledger, &key_refs, false).await?;
        txn.execute("select set_config('lock_timeout', $1, true)", &[&previous])
            .await?;
        Ok::<_, ApplyError>(())
    }
    .await;
    metrics::record_build_statement(BuildStatement::ChunkLock, started.elapsed());
    locked?;

    // 3. The read, the entries and the deltas, in one statement.
    let started = Instant::now();
    let keys_param = format!("${}", params.len() + 1);
    params.push(&key_refs);
    let sql = ledger::chunk_statement(ledger, &range_where, &keys_param);
    let row = txn.query_one(&sql, &params).await?;
    metrics::record_build_statement(BuildStatement::ChunkWrite, started.elapsed());
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
    let started = Instant::now();
    let rows = txn.query(&sql, &[&limit]).await?;
    metrics::record_build_statement(BuildStatement::MergeUpsert, started.elapsed());
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
    let started = Instant::now();
    let (written, deleted) =
        ledger::finish_groups(txn, ledger, groups, &mut mutations, |_| (0, None, None)).await?;
    mutations.flush(txn).await?;
    metrics::record_build_statement(BuildStatement::MergeFinish, started.elapsed());
    Ok(MergeOutcome {
        claimed,
        written,
        deleted,
    })
}

// ---------------------------------------------------------------------
// The scheduled build (#625 F2)
// ---------------------------------------------------------------------

/// Source rows per chunk unless the client says otherwise (#625 Q4).
pub const DEFAULT_CHUNK_ROWS: i64 = 10_000;

/// The most group-delta rows one merger pass claims (#625 Q4). A pass writes
/// at most this many groups.
pub const MERGE_BATCH: i64 = 5_000;

/// How many chunk boundaries the plan job commits per transaction (#625 Q13),
/// so the first chunks run while the walk goes on.
pub const PLAN_BATCH: usize = 100;

/// The `application_name` a build chunk's transaction runs under, so
/// `pg_stat_activity` tells its statements from a drain page's, which share
/// their text (`ledger::lock_entries`).
pub const CHUNK_APPLICATION_NAME: &str = "trellis build chunk";

/// The `application_name` a merger pass's transaction runs under.
pub const MERGE_APPLICATION_NAME: &str = "trellis build merge";

/// The `transform_definitions.build` word of a running Re-derive build.
const BUILD_REDERIVE: &str = "rederive";

/// The shape a Re-derive build serves, if `definition` has one (see the
/// module doc's "Shapes").
fn buildable_shape(definition: &Definition) -> Option<ledger::LedgerShape> {
    ledger::route(&definition.def, &definition.source_columns)
        .filter(ledger::LedgerShape::rederive_buildable)
}

/// Whether a Re-derive build may take `definition` (#625 F2): a target the
/// ledger maintains by increments over plain source columns
/// ([`buildable_shape`]), on a captured source. A source that is another
/// definition's target is fed by the target-mutation seam, whose writer can
/// commit after a chunk's snapshot; it needs a fence first (#625 F6).
pub async fn qualifies(
    client: &impl GenericClient,
    definition: &Definition,
) -> Result<bool, ApplyError> {
    if buildable_shape(definition).is_none() {
        return Ok(false);
    }
    Ok(!catalog::is_definition_target(client, &definition.source_table).await?)
}

/// Starts a Re-derive build for every definition in `ready` that
/// [`qualifies`] (#625 F2). `ready` is what the staging worker's capture pass
/// found dispatchable (`capture::reconcile`): the definitions it would
/// otherwise park a registration marker for. Returns the ones the old build
/// path must leave alone: those started, and those waiting on their source's
/// capture gate.
///
/// The gate is the discharge's (`intake::markers`, Q2(a)): while any change
/// to the source at or below the gate its capture install or widen recorded
/// is still pending, a definition that started applying would meet rows
/// imaged without a column it reads. Such a definition is left
/// `waiting_to_backfill` for a later pass; nothing waits here.
///
/// A start is one transaction ([`start`]). A definition whose ledger isn't
/// empty (one resumed after a pause) is left to the old path: re-deriving
/// over its entries needs #625 F3's sweep for the keys deleted meanwhile.
pub async fn start_ready_builds(
    client: &mut tokio_postgres::Client,
    pool: &Pool,
    ready: &[i64],
) -> Result<Vec<i64>, ApplyError> {
    let mut taken = Vec::new();
    for &id in ready {
        let Some(definition) = catalog::definition_by_id(pool, id).await? else {
            continue;
        };
        if definition.status != TransformStatus::WaitingToBackfill
            || !qualifies(&*client, &definition).await?
        {
            continue;
        }
        if capture_gate_holds(&*client, &definition.source_table).await? {
            tracing::debug!(
                definition_id = id,
                table = %definition.source_table,
                "re-derive build held by its source's capture gate"
            );
            taken.push(id);
            continue;
        }
        if start(client, &definition).await? {
            taken.push(id);
        }
    }
    Ok(taken)
}

/// Whether `table` has a capture gate a pending change still holds
/// (`pending_backfill.capture_gate_lsn`, as the discharge reads it).
async fn capture_gate_holds(client: &impl GenericClient, table: &str) -> Result<bool, ApplyError> {
    let gate: Option<crate::PgLsn> = client
        .query_opt(
            "select capture_gate_lsn from pending_backfill where table_name = $1",
            &[&table],
        )
        .await?
        .and_then(|row| row.get(0));
    Ok(match gate {
        Some(gate) => super::converge::table_changes_pending_through(client, table, gate).await?,
        None => false,
    })
}

/// Starts `definition`'s Re-derive build in one transaction (#625 F2, B1):
/// under its row lock, moves it `waiting_to_backfill -> backfilling` with
/// `build = 'rederive'` and enqueues its plan job. From that commit it
/// applies (`Definition::applies`): every page whose definition list is read
/// after it folds the source's changes into the ledger, and a change
/// committed before it is visible to every chunk's snapshot.
///
/// Returns `false`, writing nothing, when the definition left
/// `waiting_to_backfill` meanwhile or its ledger isn't empty.
async fn start(
    client: &mut tokio_postgres::Client,
    definition: &Definition,
) -> Result<bool, ApplyError> {
    let txn = client.transaction().await?;
    let status: Option<String> = txn
        .query_opt(
            "select status from transform_definitions where id = $1 for update",
            &[&definition.id],
        )
        .await?
        .map(|row| row.get(0));
    if status.as_deref() != Some(TransformStatus::WaitingToBackfill.as_str()) {
        txn.rollback().await?;
        return Ok(false);
    }
    let ledger = ddl::qualified_target_table_ident(&crate::defs::ledger::ledger_table_name(
        &definition.target_table,
    ));
    let empty: bool = txn
        .query_one(&format!("select not exists (select 1 from {ledger})"), &[])
        .await?
        .get(0);
    if !empty {
        txn.rollback().await?;
        return Ok(false);
    }
    txn.execute(
        "update transform_definitions set status = $2, build = $3 where id = $1",
        &[
            &definition.id,
            &TransformStatus::Backfilling.as_str(),
            &BUILD_REDERIVE,
        ],
    )
    .await?;
    txn.execute(
        "insert into backfill_chunks (definition_id, kind, fuse_rearmed_at, start_xid) \
         select id, $2, fuse_rearmed_at, pg_current_xact_id() \
         from transform_definitions where id = $1",
        &[&definition.id, &chunk_queue::KIND_PLAN],
    )
    .await?;
    txn.commit().await?;
    tracing::info!(
        definition_id = definition.id,
        target = %definition.target_table,
        from = %TransformStatus::WaitingToBackfill.as_str(),
        to = %TransformStatus::Backfilling.as_str(),
        "transform status transition: re-derive build started"
    );
    Ok(true)
}

/// How a drain worker runs the Re-derive build's work ([`work_once`]).
#[derive(Debug, Clone, Copy)]
pub struct WorkerOptions {
    /// Source rows per chunk the plan job enqueues
    /// (`ClientOptions::build_chunk_rows`).
    pub chunk_rows: i64,
    /// `ClientOptions::drain_batch_cap`: a chunk is claimed only while the
    /// sealed, undrained rows are under twice this (#625 Q4).
    pub drain_batch_cap: usize,
    /// How often a claimed plan job or chunk refreshes its claim.
    pub heartbeat_interval: Duration,
    /// The fleet's reclaim TTL ([`ClaimFence`]'s idle bound).
    pub reclaim_ttl: Duration,
}

/// What one [`work_once`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// No Re-derive build is running, or none had work for this worker.
    Idle,
    /// The ring's backlog held the build's chunks back (#625 B6).
    Backpressure,
    /// Folded one batch of group deltas.
    Merged,
    /// Ran (some of) a plan job.
    Planned,
    /// Ran one chunk.
    Chunk,
    /// Moved a finished build's definition to `live`.
    Completed,
}

impl Step {
    /// Whether the step did work, so the caller loops straight back.
    pub fn progressed(self) -> bool {
        !matches!(self, Step::Idle | Step::Backpressure)
    }
}

/// One step of the Re-derive builds' work for a drain worker, which runs it
/// after its segments (#625 B6): a merge when a building target has group
/// deltas, else a plan job, else one chunk if the ring's backlog allows
/// ([`chunk_allowed`]). A building definition with nothing left to do is
/// moved to `live` ([`try_complete`]).
///
/// A definition that is frozen gets nothing: neither its deltas merged nor
/// its work claimed. Its deltas stay (#625 B4).
pub async fn work_once(
    pool: &Pool,
    claimed_by: &str,
    options: &WorkerOptions,
) -> Result<Step, ChunkQueueError> {
    let building = building(pool).await?;
    if building.is_empty() {
        return Ok(Step::Idle);
    }
    for (id, target) in &building {
        // Rows other mergers hold are skipped: a pass that claims none
        // falls through to the next target, then to a chunk.
        if has_deltas(pool, target).await? && merge_once(pool, *id).await? > 0 {
            return Ok(Step::Merged);
        }
    }

    let claimed = {
        let client = pool.get().await?;
        chunk_queue::claim_chunks_of(&**client, claimed_by, 1, &[chunk_queue::KIND_PLAN]).await?
    };
    if let Some(chunk) = claimed.into_iter().next() {
        run_claimed(pool, &chunk, claimed_by, options).await;
        return Ok(Step::Planned);
    }

    let allowed = {
        let client = pool.get().await?;
        chunk_allowed(&**client, options.drain_batch_cap).await?
    };
    if allowed {
        let claimed = {
            let client = pool.get().await?;
            chunk_queue::claim_chunks_of(&**client, claimed_by, 1, &[chunk_queue::KIND_REDERIVE])
                .await?
        };
        if let Some(chunk) = claimed.into_iter().next() {
            run_claimed(pool, &chunk, claimed_by, options).await;
            return Ok(Step::Chunk);
        }
    }

    // Nothing to claim: a build whose last chunk and merge committed may be
    // done. Each of those checks after its own commit; this catches a worker
    // that died between the two.
    let mut completed = false;
    for (id, _) in &building {
        completed |= try_complete(pool, *id).await?;
    }
    Ok(if completed {
        Step::Completed
    } else if allowed {
        Step::Idle
    } else {
        Step::Backpressure
    })
}

/// Every definition a Re-derive build is running for that isn't frozen,
/// with its target, in id order.
async fn building(pool: &Pool) -> Result<Vec<(i64, String)>, ChunkQueueError> {
    let client = pool.get().await?;
    Ok(client
        .query(
            "select id, target_table from transform_definitions \
             where build = $1 and status = $2 order by id",
            &[&BUILD_REDERIVE, &TransformStatus::Backfilling.as_str()],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// Whether `target`'s group-delta table has a row. A table dropped since the
/// caller listed it has none.
async fn has_deltas(pool: &Pool, target: &str) -> Result<bool, ChunkQueueError> {
    let deltas = ddl::qualified_target_table_ident(&crate::defs::ledger::deltas_table_name(target));
    let client = pool.get().await?;
    match client
        .query_one(&format!("select exists (select 1 from {deltas})"), &[])
        .await
    {
        Ok(row) => Ok(row.get(0)),
        Err(err) if super::quarantine::is_undefined_table(&err) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// Whether a drain worker may claim a build chunk now (#625 B6, Q4): the
/// sealed, undrained rows are under twice `drain_batch_cap`, and the ring
/// has a free slot for the next seal (the seal's own refusal predicate,
/// `append::ring_slot_is_free`). A chunk claimed past either would compete
/// with the drains the backlog is waiting on, or hold off the seal.
pub async fn chunk_allowed(
    client: &impl GenericClient,
    drain_batch_cap: usize,
) -> Result<bool, ChunkQueueError> {
    let bound = i64::try_from(drain_batch_cap.max(1))
        .unwrap_or(i64::MAX / 2)
        .saturating_mul(2);
    Ok(client
        .query_one(
            "select coalesce((select sum(row_count)::bigint from segments \
                     where state in ('sealed', 'draining') \
                       and drained_mask <> ((1::bigint << bucket_count) - 1)), 0) < $1 \
                 and not exists (select 1 from segments \
                     where ring_slot = ((select ring_slot from segment_pointer) + 1) % $2)",
            &[&bound, &i32::from(super::append::RING_SIZE)],
        )
        .await?
        .get(0))
}

/// Runs a claimed plan job or chunk ([`chunk_queue::claim_chunks_of`]), and
/// gives it up through [`chunk_queue::fail_chunk`] if it fails, which
/// records, logs and backs it off (#616). [`work_once`] claims and runs one;
/// a test can claim one by hand and run it here.
pub async fn run_claimed(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    options: &WorkerOptions,
) {
    let ran = match &chunk.work {
        ChunkWork::Plan { cursor } => {
            run_plan(pool, chunk, claimed_by, cursor.clone(), options).await
        }
        ChunkWork::Rederive { lo, hi } => {
            run_rederive(pool, chunk, claimed_by, lo.as_deref(), hi, options).await
        }
        work => Err(ChunkQueueError::UnknownKind {
            kind: work.kind().to_string(),
        }),
    };
    let Err(err) = ran else {
        return;
    };
    if crate::locks::is_lock_not_available(&err) {
        metrics::increment_build_chunk_lock_timeouts();
    }
    if let Err(fail_err) = chunk_queue::fail_chunk(pool, chunk, claimed_by, &err).await {
        tracing::warn!(
            definition_id = chunk.definition_id,
            chunk_id = chunk.id,
            error = %err,
            record_error = %fail_err,
            "re-derive build step failed, and recording the failure failed too; the \
             stale-claim sweep frees it for a retry"
        );
    }
}

fn build_error(err: impl Into<ApplyError>) -> ChunkQueueError {
    ChunkQueueError::Build(Box::new(err.into()))
}

/// Runs one claimed Re-derive chunk (#625 F2): [`run_chunk`] and the chunk's
/// done mark in one `read committed` transaction (ADR-0002 I1: the chunk's
/// snapshot must be its read statement's own), fenced by its claim
/// ([`ClaimFence`]). The done mark commits with the entries and deltas it
/// stands for, so a chunk that dies before its commit runs again whole, and
/// one that committed is never run again. Then [`try_complete`].
async fn run_rederive(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    lo: Option<&str>,
    hi: &str,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    let setup_started = Instant::now();
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    let plan = BuildPlan::for_definition(pool, &definition)
        .await
        .map_err(build_error)?
        .ok_or_else(|| {
            build_error(crate::defs::backfill::BackfillError::Unsupported(
                "the definition's shape no longer takes a re-derive build".to_string(),
            ))
        })?;
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    let mut client = pool.get().await?;
    let started = Instant::now();
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&format!(
        "set local application_name = '{CHUNK_APPLICATION_NAME}'"
    ))
    .await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
    }
    metrics::record_build_statement(BuildStatement::ChunkSetup, setup_started.elapsed());
    let outcome = run_chunk(&txn, &plan, lo, hi).await.map_err(build_error)?;
    let commit_started = Instant::now();
    txn.execute(
        "update backfill_chunks set done = true, claimed_by = null, claimed_at = null \
         where id = $1",
        &[&chunk.id],
    )
    .await?;
    txn.commit().await?;
    metrics::record_build_statement(BuildStatement::ChunkCommit, commit_started.elapsed());
    metrics::record_build_chunk(
        started.elapsed(),
        outcome.keys as u64,
        u64::try_from(outcome.delta_rows).unwrap_or(0),
    );
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// Runs a claimed plan job (#625 F2, Q13): walks the source's primary key
/// from the job's cursor, [`PLAN_BATCH`] boundaries at a time, each batch
/// committed as `rederive` chunks together with the cursor's advance, so
/// chunks run while the walk goes on and a job that dies resumes where its
/// last batch committed. The batch that reaches the last row marks the job
/// done. Each commit is fenced by the claim ([`ClaimFence`]).
async fn run_plan(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    mut cursor: Option<String>,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    let pk = ddl::source_primary_key(pool, &definition.source_table)
        .await
        .map_err(build_error)?;
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    let mut client = pool.get().await?;
    loop {
        let started = Instant::now();
        let ranges = crate::defs::backfill::next_pk_ranges(
            &**client,
            &definition.source_table,
            &pk,
            cursor.as_deref(),
            options.chunk_rows,
            PLAN_BATCH,
        )
        .await
        .map_err(build_error)?;
        let finished = ranges.len() < PLAN_BATCH;
        let next = ranges.last().map(|(_, hi)| hi.clone()).or(cursor.clone());
        let txn = client.transaction().await?;
        if !fence.hold(&*txn).await? {
            txn.rollback().await?;
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        }
        let (los, his): (Vec<Option<&str>>, Vec<&str>) = ranges
            .iter()
            .map(|(lo, hi)| (lo.as_deref(), hi.as_str()))
            .unzip();
        txn.execute(
            "insert into backfill_chunks (definition_id, kind, lo, hi, fuse_rearmed_at) \
             select bc.definition_id, $4, r.lo, r.hi, bc.fuse_rearmed_at \
             from unnest($2::text[], $3::text[]) with ordinality as r(lo, hi, n) \
             cross join backfill_chunks bc \
             where bc.id = $1 \
             order by r.n",
            &[&chunk.id, &los, &his, &chunk_queue::KIND_REDERIVE],
        )
        .await?;
        if finished {
            txn.execute(
                "update backfill_chunks set lo = $2, done = true, claimed_by = null, \
                     claimed_at = null \
                 where id = $1",
                &[&chunk.id, &next],
            )
            .await?;
        } else {
            txn.execute(
                "update backfill_chunks set lo = $2 where id = $1",
                &[&chunk.id, &next],
            )
            .await?;
        }
        txn.commit().await?;
        metrics::record_build_statement(BuildStatement::Plan, started.elapsed());
        if finished {
            tracing::debug!(
                definition_id = chunk.definition_id,
                "re-derive build planned its last chunk"
            );
            break;
        }
        cursor = next;
    }
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// Folds one [`MERGE_BATCH`] of definition `id`'s group deltas into its
/// groups (#625 B3), in a transaction holding the definition row
/// `for key share` so a pause waits for it rather than racing it: a frozen
/// definition's deltas aren't merged. A target dropped meanwhile ends the
/// pass quietly. Then [`try_complete`] when the pass drained the table.
/// Returns how many delta rows it folded.
async fn merge_once(pool: &Pool, id: i64) -> Result<i64, ChunkQueueError> {
    let setup_started = Instant::now();
    let Some(definition) = catalog::definition_by_id(pool, id).await? else {
        return Ok(0);
    };
    let Some(plan) = BuildPlan::for_definition(pool, &definition)
        .await
        .map_err(build_error)?
    else {
        return Ok(0);
    };
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    txn.batch_execute(&format!(
        "set local application_name = '{MERGE_APPLICATION_NAME}'"
    ))
    .await?;
    let building = txn
        .query_opt(
            "select 1 from transform_definitions where id = $1 and status = $2 and build = $3 \
             for key share",
            &[&id, &TransformStatus::Backfilling.as_str(), &BUILD_REDERIVE],
        )
        .await?
        .is_some();
    if !building {
        txn.rollback().await?;
        return Ok(0);
    }
    metrics::record_build_statement(BuildStatement::MergeSetup, setup_started.elapsed());
    let outcome = match merge_deltas(&txn, &plan, MERGE_BATCH).await {
        Ok(outcome) => outcome,
        Err(ApplyError::Db(err)) if super::quarantine::is_undefined_table(&err) => {
            return Ok(0);
        }
        Err(err) => return Err(build_error(err)),
    };
    let started = Instant::now();
    txn.commit().await?;
    metrics::record_build_statement(BuildStatement::MergeCommit, started.elapsed());
    metrics::record_build_merge(u64::try_from(outcome.claimed).unwrap_or(0));
    if outcome.claimed < MERGE_BATCH {
        try_complete(pool, id).await?;
    }
    Ok(outcome.claimed)
}

/// Moves definition `id` `backfilling -> live` once its Re-derive build is
/// done (#625 B7, the strict flip): the plan job is done, no other chunk is
/// left undone (one held across a resume doesn't count, as for the old
/// build), and its group-delta table is empty. Returns whether it flipped.
///
/// Every transaction that can make that true (a chunk's, a merger's, the
/// plan job's last) calls this after it commits, so the last of them sees
/// all the others committed. It checks without a lock first, and only then
/// takes the definition row `for update` and checks again, so only a
/// possibly-last caller takes the lock. No catch-up is parked and no horizon
/// raised: the definition applied from its start.
pub async fn try_complete(pool: &Pool, id: i64) -> Result<bool, ChunkQueueError> {
    let mut client = pool.get().await?;
    if !build_done(&**client, id).await? {
        return Ok(false);
    }
    let txn = client.transaction().await?;
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&id],
    )
    .await?;
    if !build_done(&*txn, id).await? {
        txn.rollback().await?;
        return Ok(false);
    }
    txn.execute(
        "update transform_definitions set status = $2, build = null where id = $1",
        &[&id, &TransformStatus::Live.as_str()],
    )
    .await?;
    txn.commit().await?;
    tracing::info!(
        definition_id = id,
        from = %TransformStatus::Backfilling.as_str(),
        to = %TransformStatus::Live.as_str(),
        "transform status transition: re-derive build finished"
    );
    Ok(true)
}

/// [`try_complete`]'s test, in one statement: `id` is under a Re-derive
/// build, no current chunk of it is undone, and its target's group-delta
/// table is empty. A dropped target reads as not done.
async fn build_done(client: &impl GenericClient, id: i64) -> Result<bool, ChunkQueueError> {
    let Some(row) = client
        .query_opt(
            &format!(
                "select d.target_table, not exists ( \
                     select 1 from backfill_chunks bc \
                     where bc.definition_id = d.id and not bc.done \
                       and not ({}) \
                 ) \
                 from transform_definitions d \
                 where d.id = $1 and d.status = $2 and d.build = $3",
                chunk_queue::STALE
            ),
            &[&id, &TransformStatus::Backfilling.as_str(), &BUILD_REDERIVE],
        )
        .await?
    else {
        return Ok(false);
    };
    let target: String = row.get(0);
    if !row.get::<_, bool>(1) {
        return Ok(false);
    }
    let deltas =
        ddl::qualified_target_table_ident(&crate::defs::ledger::deltas_table_name(&target));
    match client
        .query_one(&format!("select not exists (select 1 from {deltas})"), &[])
        .await
    {
        Ok(row) => Ok(row.get(0)),
        Err(err) if super::quarantine::is_undefined_table(&err) => Ok(false),
        Err(err) => Err(err.into()),
    }
}
