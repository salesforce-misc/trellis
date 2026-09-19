//! The durable, claimable backfill-chunk work queue (public API design's
//! ADR-0007 amendment — see `docs/decisions/0007-direct-set-based-backfill.md`'s
//! "Backgrounding and resumability" section and `docs/decisions/0008-public-api-design.md`'s
//! decision 1).
//!
//! [`install_definition`](super::catalog::install_definition) no longer runs
//! a plain (non-relationship) 1-1 definition's PK-range chunks back-to-back
//! in one call: [`enqueue_one_to_one`] enumerates the same boundaries
//! [`super::backfill::plan_one_to_one_chunks`] always computed and persists
//! each as a row in the `backfill_chunks` table (`V20__backfill_chunks.sql`),
//! then returns immediately. [`claim_chunks`]/[`reclaim_stale_chunks`]/
//! [`release_chunk`] mirror `staging::claim`/`staging::liveness`'s
//! claim/reclaim-stale/release-on-error idiom for sealed ring segments,
//! collapsed onto one table (a chunk, unlike a segment, is never bucket-split
//! across several workers at once — see the migration's own doc comment).
//! [`run_claimed_chunk`] executes one claimed chunk's write — heartbeating
//! the claim out-of-band for the write's whole duration via the internal
//! `ChunkHeartbeat`, the same "keep a claim alive against wall-clock time,
//! not against how much work is left" idiom `staging::liveness::HeartbeatDaemon`
//! uses for segment claims, so a chunk write that outlives `reclaim_ttl`
//! isn't falsely reclaimed mid-write — and [`finish_chunk`] marks it done,
//! flipping the owning definition `backfilling` -> `live` (via
//! [`super::catalog::complete_direct_backfill`]) the moment every one of its
//! chunks is done — race-free under concurrent finishers via a `for update`
//! lock on the definition's own row (see that function's doc comment).
//!
//! A relationship-enriched 1-1 definition is *not* enqueued here at all: its
//! per-relationship staging tables are connection-scoped `TEMP TABLE`s, which
//! don't survive being read by independent drain workers on separate
//! connections — the same open problem the ADR amendment calls out for the
//! aggregate path's own staging table, applying verbatim. It (and the
//! aggregate key-space) instead still run fully synchronously inside
//! `install_definition`, unchanged from before this module existed — see
//! [`super::catalog::install_definition`]'s doc comment and
//! [`super::backfill`]'s module docs for the full shape-by-shape breakdown.

use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_postgres::GenericClient;

use crate::error_code::{self, ErrorCode};
use crate::pool::Pool;

use super::ast::TransformDef;
use super::backfill::{self, BackfillError};
use super::catalog::{self, CatalogError};
use super::model::TransformStatus;

/// Why claiming, executing, or completing a durable backfill chunk failed.
/// Composes [`BackfillError`] (the actual chunk write) and [`CatalogError`]
/// (definition lookup/status-flip) rather than reinventing either, matching
/// this crate's nested-`code()`-delegation convention (`docs/decisions/0008-public-api-design.md`,
/// decision 3).
#[derive(Debug)]
pub enum ChunkQueueError {
    /// A direct Postgres protocol/query error running the queue's own
    /// claim/reclaim/release/finish SQL.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// Executing a claimed chunk's write failed.
    Backfill(BackfillError),
    /// Looking up the claimed chunk's owning definition, or flipping it to
    /// `live`, failed.
    Catalog(CatalogError),
    /// A claimed chunk named a `definition_id` with no corresponding
    /// `transform_definitions` row — only reachable if a definition is
    /// dropped mid-backfill, which no API exposes today (`backfill_chunks`'
    /// `on delete cascade` would normally have removed the chunk row itself
    /// in that case, so this is a defensive "should not happen," not an
    /// expected outcome).
    DefinitionNotFound { definition_id: i64 },
}

impl ChunkQueueError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` wherever one
    /// nests here.
    // `ChunkQueueError` never escapes the crate (ADR-0012 tier 3), so nothing
    // calls this today. Kept because ADR-0008 decision 3 makes `code()` a
    // uniform obligation of *every* error type here: the moment this error
    // nests into one that does surface, the method has to already exist.
    #[allow(dead_code)]
    pub fn code(&self) -> ErrorCode {
        match self {
            ChunkQueueError::Db(err) => error_code::classify_pg_error(err),
            ChunkQueueError::Pool(err) => err.code(),
            ChunkQueueError::Backfill(err) => err.code(),
            ChunkQueueError::Catalog(err) => err.code(),
            ChunkQueueError::DefinitionNotFound { .. } => ErrorCode::Internal,
        }
    }
}

impl std::fmt::Display for ChunkQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkQueueError::Db(err) => {
                write!(f, "backfill chunk queue database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            ChunkQueueError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            ChunkQueueError::Backfill(err) => write!(f, "backfill chunk execution failed: {err}"),
            ChunkQueueError::Catalog(err) => write!(f, "backfill chunk completion failed: {err}"),
            ChunkQueueError::DefinitionNotFound { definition_id } => write!(
                f,
                "backfill chunk named definition {definition_id}, which no longer exists"
            ),
        }
    }
}

impl std::error::Error for ChunkQueueError {}

impl From<tokio_postgres::Error> for ChunkQueueError {
    fn from(err: tokio_postgres::Error) -> Self {
        ChunkQueueError::Db(err)
    }
}

impl From<crate::error::Error> for ChunkQueueError {
    fn from(err: crate::error::Error) -> Self {
        ChunkQueueError::Pool(err)
    }
}

impl From<BackfillError> for ChunkQueueError {
    fn from(err: BackfillError) -> Self {
        ChunkQueueError::Backfill(err)
    }
}

impl From<CatalogError> for ChunkQueueError {
    fn from(err: CatalogError) -> Self {
        ChunkQueueError::Catalog(err)
    }
}

/// One durable `backfill_chunks` row, as claimed by [`claim_chunks`]: a
/// plain 1-1 PK-range chunk. `lo` is `None` for the first chunk (`pk <= hi`,
/// no lower bound) and `Some` for every later one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedChunk {
    pub id: i64,
    pub definition_id: i64,
    pub lo: Option<String>,
    pub hi: String,
}

/// Enumerates and persists `def`'s direct-build work as durable
/// `backfill_chunks` rows for `definition_id` — the "plan, don't execute"
/// half of what `install_definition` used to do synchronously in-call (see
/// this module's doc comment). Only ever called for a plain (non-relationship)
/// `KeySpace::OneToOne` definition; `install_definition` itself decides that
/// shape check before calling this (a relationship-enriched 1-1 or aggregate
/// definition never reaches here — see [`super::backfill`]'s module docs).
///
/// A source table with zero rows enumerates zero chunks; since nothing would
/// ever claim-and-finish a "last chunk" to trigger the `Backfilling` ->
/// `Live` flip in that case, this immediately completes the definition itself
/// once it has confirmed there is truly nothing to wait for.
pub(crate) async fn enqueue_one_to_one(
    pool: &Pool,
    definition_id: i64,
    def: &TransformDef,
    source_table: &str,
) -> Result<TransformStatus, BackfillError> {
    let ranges = backfill::plan_one_to_one_chunks(pool, def, source_table).await?;

    if ranges.is_empty() {
        // Nothing to wait for — no chunk will ever be claimed-and-finished to
        // trigger the usual `Backfilling` -> `Live` flip, so do it directly,
        // through the same race-free lock-then-check-then-complete sequence
        // `finish_chunk` uses (there's nothing to race here — no chunk claim
        // exists for this definition yet — but reusing the exact path keeps
        // there being exactly one way a direct-build definition ever
        // completes).
        complete_if_no_chunks_remain(pool, definition_id)
            .await
            .map_err(catalog_completion_err_to_backfill)?;
        return Ok(TransformStatus::Live);
    }

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for (lo, hi) in &ranges {
        txn.execute(
            "insert into backfill_chunks (definition_id, lo, hi) values ($1, $2, $3)",
            &[&definition_id, lo, hi],
        )
        .await?;
    }
    txn.commit().await?;
    Ok(TransformStatus::Backfilling)
}

/// Claims up to `limit` unclaimed, undone chunks for `claimed_by` in one
/// statement — the backfill-chunk analogue of `staging::claim`/
/// `staging::next_claimable_segments`, collapsed onto a single table+statement
/// since a chunk is never bucket-split (see this module's doc comment). The
/// `for update skip locked` candidate selection means two workers racing this
/// call never claim the same row twice and never block on each other.
pub async fn claim_chunks(
    client: &impl GenericClient,
    claimed_by: &str,
    limit: i64,
) -> Result<Vec<ClaimedChunk>, ChunkQueueError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let rows = client
        .query(
            "with candidate as ( \
                 select id from backfill_chunks \
                 where not done and claimed_by is null \
                 order by id \
                 for update skip locked \
                 limit $2 \
             ) \
             update backfill_chunks c \
             set claimed_by = $1, claimed_at = now() \
             from candidate \
             where c.id = candidate.id \
             returning c.id, c.definition_id, c.lo, c.hi",
            &[&claimed_by, &limit],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| ClaimedChunk {
            id: row.get(0),
            definition_id: row.get(1),
            lo: row.get(2),
            hi: row.get(3),
        })
        .collect())
}

/// Sweeps every chunk claim whose `claimed_at` is older than `ttl` — the
/// `backfill_chunks` analogue of `staging::liveness::reclaim_stale` (same
/// `for update skip locked` "don't wait behind a live claimant's in-flight
/// write" shape), run periodically by `trellis::client`'s maintenance loop
/// alongside the ring's own reclaim sweep. Freeing a claim (rather than
/// deleting a row, since there's no separate claims table to delete from —
/// see this module's doc comment) makes the chunk claimable again on the very
/// next [`claim_chunks`] call.
pub async fn reclaim_stale_chunks(
    client: &impl GenericClient,
    ttl: Duration,
) -> Result<u64, ChunkQueueError> {
    let ttl_secs = ttl.as_secs_f64();
    let n = client
        .execute(
            "with dead as ( \
                 select id from backfill_chunks \
                 where claimed_by is not null and not done \
                   and claimed_at < now() - (interval '1 second' * $1) \
                 for update skip locked \
             ) \
             update backfill_chunks set claimed_by = null, claimed_at = null \
             from dead where backfill_chunks.id = dead.id",
            &[&ttl_secs],
        )
        .await?;
    Ok(n)
}

/// Releases a claim immediately on a chunk-execution error — the
/// `backfill_chunks` analogue of `staging::liveness::release`. Scoped
/// `claimed_by = $2` for the same reason that function is: a claim already
/// reclaimed out from under this caller (the TTL sweep, or another worker) is
/// left untouched.
pub async fn release_chunk(
    client: &impl GenericClient,
    id: i64,
    claimed_by: &str,
) -> Result<u64, ChunkQueueError> {
    let n = client
        .execute(
            "update backfill_chunks set claimed_by = null, claimed_at = null \
             where id = $1 and claimed_by = $2",
            &[&id, &claimed_by],
        )
        .await?;
    Ok(n)
}

/// Executes one claimed chunk's write against its target
/// ([`super::backfill::execute_one_to_one_chunk`]). Looks up the owning
/// definition fresh on every call (`super::catalog::definition_by_id`)
/// rather than caching it across chunks, since chunks of the same definition
/// may be claimed and run by entirely different processes with no shared
/// in-memory state.
///
/// Runs a [`ChunkHeartbeat`] for the duration of the write so a chunk whose
/// `INSERT … SELECT` takes longer than the fleet's `reclaim_ttl` (the whole
/// reason [`super::backfill::BACKFILL_CHUNK_ROWS`] bounds a chunk rather than
/// leaving it unbounded — a bound on *cost*, not on *wall-clock time*, which
/// can still grow with row width, index maintenance, or a loaded server)
/// isn't falsely reclaimed and concurrently re-executed by another worker
/// while this one is still working it. `heartbeat_interval` should be well
/// under whatever `reclaim_ttl` the caller's fleet uses — callers driven by
/// [`crate::client::Client`] pass its `ClientOptions::heartbeat`'s own
/// interval, matching the same margin the ring's own
/// [`crate::staging::HeartbeatDaemon`] keeps against `reclaim_ttl` there.
pub async fn run_claimed_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    target_schema: &str,
    claimed_by: &str,
    heartbeat_interval: Duration,
) -> Result<(), ChunkQueueError> {
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;

    // Dropped (aborting the background task) as soon as this function
    // returns, one way or another — see [`ChunkHeartbeat`]'s doc comment.
    let _heartbeat = ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        heartbeat_interval,
    );

    backfill::execute_one_to_one_chunk(
        pool,
        &definition.def,
        target_schema,
        &definition.source_table,
        chunk.lo.as_deref(),
        &chunk.hi,
    )
    .await?;
    Ok(())
}

/// Refreshes `claimed_at` for one in-flight chunk claim on a fresh pooled
/// connection — the chunk-queue analogue of [`super::backfill`]'s own
/// per-chunk write, but for the claim row rather than the target. Scoped
/// `claimed_by = $2 and not done` for the same reason every other
/// claim-scoped statement in this module is: a claim already reclaimed out
/// from under this caller (the TTL sweep, or another worker) or already
/// finished is left untouched rather than resurrected. Best-effort: a failed
/// refresh costs one heartbeat interval of staleness, not correctness — see
/// [`ChunkHeartbeat`].
async fn touch_chunk_claim(pool: &Pool, id: i64, claimed_by: &str) {
    if let Ok(client) = pool.get().await {
        let _ = client
            .execute(
                "update backfill_chunks set claimed_at = now() \
                 where id = $1 and claimed_by = $2 and not done",
                &[&id, &claimed_by],
            )
            .await;
    }
}

/// Keeps one claimed chunk's `claimed_at` fresh out-of-band for the whole
/// lifetime of one [`run_claimed_chunk`] call — the chunk-queue counterpart
/// of `staging::liveness::HeartbeatDaemon`, which does the same job for
/// `seg_claims` rows so a long-running segment drain isn't falsely reclaimed
/// mid-write (see doc 04, "Keeping a claim alive").
///
/// A sibling rather than a direct reuse of `HeartbeatDaemon`: that daemon's
/// refresh statement (`DAEMON_REFRESH_SQL`) is hardwired to `seg_claims`, and
/// its registry/lazy-connect/idle-exit design exists to amortize *many*
/// concurrently-registered claims sharing one process-wide dedicated
/// connection across a whole app-worker task's entire lifetime — machinery a
/// single chunk execution (exactly one claim, one bounded call with a clear
/// start and end) has no use for. This instead spawns a plain periodic task
/// scoped to exactly one chunk's execution, refreshing through the caller's
/// own connection pool rather than a dedicated connection, and stops simply
/// by being dropped (which aborts the task) once that call returns.
struct ChunkHeartbeat {
    task: tokio::task::JoinHandle<()>,
}

impl ChunkHeartbeat {
    fn spawn(pool: Pool, id: i64, claimed_by: String, interval: Duration) -> Self {
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                touch_chunk_claim(&pool, id, &claimed_by).await;
            }
        });
        ChunkHeartbeat { task }
    }
}

impl Drop for ChunkHeartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Marks `chunk` done — scoped `claimed_by = $2 and not done` so a claim
/// already reclaimed out from under this caller (the TTL sweep decided this
/// worker died) is left untouched rather than double-completed — and, if
/// every chunk for its definition is now done, flips that definition
/// `backfilling` -> `live` (`super::catalog::complete_direct_backfill`).
///
/// Race-free under two workers finishing different chunks of the *same*
/// definition near-simultaneously: this locks the definition's own
/// `transform_definitions` row (`for update`) before checking "any chunks
/// left?", so the second worker to reach that check always does so *after*
/// the first's own completed-chunk update has committed (it had to wait for
/// the lock), never against a stale snapshot that still shows the first
/// worker's chunk as pending. Exactly one of the two ever observes "zero
/// remaining" and performs the flip.
pub async fn finish_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
) -> Result<(), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    txn.query_opt(
        "select id from transform_definitions where id = $1 for update",
        &[&chunk.definition_id],
    )
    .await?;

    let done = txn
        .execute(
            "update backfill_chunks set done = true, claimed_by = null, claimed_at = null \
             where id = $1 and claimed_by = $2 and not done",
            &[&chunk.id, &claimed_by],
        )
        .await?;
    if done == 1 {
        complete_if_no_chunks_remain_in_txn(&txn, chunk.definition_id).await?;
    }

    txn.commit().await?;
    Ok(())
}

/// The lock-then-check-then-complete sequence [`finish_chunk`] runs inside
/// its own transaction after marking a chunk done, factored out so
/// [`enqueue_one_to_one`]'s zero-chunk case (nothing to mark done — every
/// chunk enumerated already is) can reach the exact same race-free
/// completion path via its own fresh transaction.
async fn complete_if_no_chunks_remain(
    pool: &Pool,
    definition_id: i64,
) -> Result<(), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    txn.query_opt(
        "select id from transform_definitions where id = $1 for update",
        &[&definition_id],
    )
    .await?;
    complete_if_no_chunks_remain_in_txn(&txn, definition_id).await?;
    txn.commit().await?;
    Ok(())
}

/// Maps a [`ChunkQueueError`] from the zero-chunk completion path back into a
/// [`BackfillError`] for [`enqueue_one_to_one`]'s signature — that path can
/// only realistically fail on a DB/pool error (the completion SQL, or
/// `complete_direct_backfill`'s own catalog/intake calls), never
/// `BackfillError::Unsupported`, so anything else is a defensive catch-all
/// rather than an expected case.
fn catalog_completion_err_to_backfill(err: ChunkQueueError) -> BackfillError {
    match err {
        ChunkQueueError::Db(e) => BackfillError::Db(e),
        ChunkQueueError::Pool(e) => BackfillError::Pool(e),
        ChunkQueueError::Backfill(e) => e,
        other => BackfillError::Unsupported(format!(
            "completing a definition with no backfill work failed: {other}"
        )),
    }
}

/// The shared "no chunks left? then flip to live" check both
/// [`finish_chunk`] and [`complete_if_no_chunks_remain`] run once they
/// already hold the definition row's `for update` lock.
async fn complete_if_no_chunks_remain_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
) -> Result<(), ChunkQueueError> {
    let remaining: bool = txn
        .query_one(
            "select exists(select 1 from backfill_chunks where definition_id = $1 and not done)",
            &[&definition_id],
        )
        .await?
        .get(0);
    if !remaining {
        catalog::complete_direct_backfill(txn, definition_id).await?;
    }
    Ok(())
}
