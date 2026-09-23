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
///
/// **A resume that raced the install (issue #360).** `install_definition`
/// commits the `backfilling` row before this runs, so an operator can pause
/// and resume the definition in between. That resume found no chunks to
/// discard and already parked the rebuild that covers the whole source, so
/// this enqueues nothing and completes nothing for a resumed definition — see
/// [`RESUMED`]. The check and the inserts share one transaction holding the
/// definition row's `for update` lock, the lock pause and resume take too, so
/// a resume either lands first (and this sees it) or waits and discards the
/// chunks inserted here as it would any paused definition's (#332).
///
/// Only [`CatalogError::DirectBackfill`] wrapping
/// [`BackfillError::Unsupported`] means "this shape can't be built directly",
/// the one error the caller answers by falling back to the ring. Every other
/// error is real and is returned as it is (issue #396).
pub(crate) async fn enqueue_one_to_one(
    pool: &Pool,
    definition_id: i64,
    def: &TransformDef,
    source_table: &str,
) -> Result<TransformStatus, CatalogError> {
    let ranges = backfill::plan_one_to_one_chunks(pool, def, source_table)
        .await
        .map_err(CatalogError::DirectBackfill)?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let locked = txn
        .query_opt(
            &format!(
                "select status, {RESUMED} from transform_definitions d \
                 where id = $1 for update"
            ),
            &[&definition_id],
        )
        .await?;
    if let Some(row) = locked
        && row.get::<_, bool>(1)
    {
        txn.commit().await?;
        let status_text: String = row.get(0);
        return Ok(
            TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            }),
        );
    }

    if ranges.is_empty() {
        // Nothing to wait for — no chunk will ever be claimed-and-finished to
        // trigger the usual `Backfilling` -> `Live` flip, so do it directly,
        // through the same lock-then-check-then-complete sequence
        // `finish_chunk` uses (the lock is already held above), keeping there
        // being exactly one way a direct-build definition ever completes.
        // Reports what the completion actually left the definition in: an
        // operator can already pause it by now (issue #331), and that pause
        // stands rather than being reported — or forced — `live`.
        let status = complete_if_no_chunks_remain_in_txn(&txn, definition_id).await?;
        txn.commit().await?;
        return Ok(status.unwrap_or(TransformStatus::Backfilling));
    }

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

/// SQL predicate over a `transform_definitions` row aliased `d`: whether the
/// definition has ever been resumed (issue #360). Every resume
/// (`staging::quarantine::resume_transform`) stamps `fuse_rearmed_at`, and
/// nothing else writes it.
///
/// A resumed definition's rebuild runs through its parked catch-up marker, not
/// through `backfill_chunks`, so none of its chunks should ever be claimable
/// again, and none of them completes it: resume deletes the unclaimed ones,
/// [`enqueue_one_to_one`] enqueues none after it, and [`release_chunk`]/
/// [`reclaim_stale_chunks`]/[`finish_chunk`] discard a chunk that was held
/// across the resume rather than freeing or completing it. Chunks are
/// only ever enqueued at install, so "resumed at all" is the same as "resumed
/// since these chunks were planned".
const RESUMED: &str = "d.fuse_rearmed_at is not null";

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
    // The allowlist, derived from the enum rather than spelled into the SQL
    // (issue #231) — see [`TransformStatus::dispatchable`].
    let dispatchable = TransformStatus::dispatchable();
    let rows = client
        .query(
            // The `exists` gate is issue #142 / ADR-0014's: the pause is what
            // quiesces in-flight work, and a *backfilling* definition's
            // in-flight work lives here rather than in the claim-time fold.
            // So this consults the same `transform_definitions.status` the
            // fold's own `status = 'live'` gate reads, and stops handing out
            // new chunks for a frozen definition. A chunk a worker already
            // holds is deliberately left alone: per the ADR it is released on
            // its own heartbeat/TTL ([`reclaim_stale_chunks`]), never
            // force-cleared out from under a running worker.
            //
            // `= any($3)` binds an **allowlist** of the statuses a definition
            // may still be handed work in, not a denylist of the one it may
            // not (issue #231): this gate used to read `status <> 'paused'`,
            // which would have silently re-opened dispatch to any
            // frozen-but-not-`paused` status added later. The allowlist is
            // computed from [`TransformStatus::ALL`] minus
            // [`TransformStatus::is_frozen`], the single predicate the
            // pause/resume/drop preconditions ask too, so a future frozen
            // state closes this gate by existing.
            //
            // It sits inside the candidate CTE rather than on the outer
            // `update` so a frozen definition's chunks never enter the
            // `for update skip locked` window at all — a frozen definition
            // can't starve its siblings out of the `limit $2` budget.
            "with candidate as ( \
                 select bc.id from backfill_chunks bc \
                 where not bc.done and bc.claimed_by is null \
                   and exists ( \
                       select 1 from transform_definitions d \
                       where d.id = bc.definition_id and d.status = any($3) \
                   ) \
                 order by bc.id \
                 for update skip locked \
                 limit $2 \
             ) \
             update backfill_chunks c \
             set claimed_by = $1, claimed_at = now() \
             from candidate \
             where c.id = candidate.id \
             returning c.id, c.definition_id, c.lo, c.hi",
            &[&claimed_by, &limit, &dispatchable],
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
///
/// A stale chunk whose definition was resumed while it was held is deleted
/// instead of freed (issue #360) — see [`free_or_discard_claims`].
/// Returns how many stale claims it dealt with either way.
pub async fn reclaim_stale_chunks(
    client: &mut impl GenericClient,
    ttl: Duration,
) -> Result<u64, ChunkQueueError> {
    let ttl_secs = ttl.as_secs_f64();
    let txn = client.transaction().await?;
    // `skip locked` on both tables: the sweep never waits, neither behind a
    // live claimant's in-flight write nor behind a pause, resume or
    // completion holding the definition row. A chunk skipped here is swept
    // on a later pass. (Discarding a chunk parks a marker, which can still
    // wait briefly on a concurrent park of the same table's marker row.)
    let rows = txn
        .query(
            &format!(
                "select bc.id, {RESUMED} from backfill_chunks bc \
                 join transform_definitions d on d.id = bc.definition_id \
                 where bc.claimed_by is not null and not bc.done \
                   and bc.claimed_at < now() - (interval '1 second' * $1) \
                 for update of bc skip locked \
                 for share of d skip locked"
            ),
            &[&ttl_secs],
        )
        .await?;
    let n = rows.len() as u64;
    free_or_discard_claims(&txn, &rows).await?;
    txn.commit().await?;
    Ok(n)
}

/// Releases a claim immediately on a chunk-execution error — the
/// `backfill_chunks` analogue of `staging::liveness::release`. Scoped
/// `claimed_by = $2` for the same reason that function is: a claim already
/// reclaimed out from under this caller (the TTL sweep, or another worker) is
/// left untouched.
///
/// A chunk whose definition was resumed while it was held is deleted instead
/// of freed (issue #360) — see [`free_or_discard_claims`]. Returns how many
/// claims (zero or one) it released or discarded.
pub async fn release_chunk(
    client: &mut impl GenericClient,
    id: i64,
    claimed_by: &str,
) -> Result<u64, ChunkQueueError> {
    let txn = client.transaction().await?;
    let rows = txn
        .query(
            &format!(
                "select bc.id, {RESUMED} from backfill_chunks bc \
                 join transform_definitions d on d.id = bc.definition_id \
                 where bc.id = $1 and bc.claimed_by = $2 and not bc.done \
                 for update of bc for share of d"
            ),
            &[&id, &claimed_by],
        )
        .await?;
    let n = rows.len() as u64;
    free_or_discard_claims(&txn, &rows).await?;
    txn.commit().await?;
    Ok(n)
}

/// Gives up the claims `rows` (each `(chunk id, resumed)`, locked by the
/// caller along with a share lock on each chunk's definition row) name.
///
/// A never-resumed definition's chunk is freed, claimable again by the next
/// [`claim_chunks`]. A chunk held across its definition's resume is deleted
/// instead (issue #360): resume discarded its unclaimed siblings and left
/// this one only so its worker could finish it (#332), and running it again
/// would redo work the resumed definition's rebuild already covers. Its
/// worker's write may still have landed some of its range after that rebuild
/// went live, so the catch-up marker the rerun's completion would have parked
/// ([`super::catalog::complete_direct_backfill`]) is parked here instead,
/// unless the definition is frozen again (its next resume parks its own).
///
/// The share lock is what makes `resumed` trustworthy: resume holds the
/// definition row `for update` while it deletes unclaimed chunks, so it runs
/// either wholly before this read (and `resumed` sees it) or wholly after the
/// caller commits (and its delete sees the chunk freed here).
async fn free_or_discard_claims(
    txn: &tokio_postgres::Transaction<'_>,
    rows: &[tokio_postgres::Row],
) -> Result<(), ChunkQueueError> {
    let (discard, free): (Vec<_>, Vec<_>) = rows.iter().partition(|row| row.get::<_, bool>(1));
    let free: Vec<i64> = free.iter().map(|row| row.get(0)).collect();
    let discard: Vec<i64> = discard.iter().map(|row| row.get(0)).collect();
    if !free.is_empty() {
        txn.execute(
            "update backfill_chunks set claimed_by = null, claimed_at = null \
             where id = any($1)",
            &[&free],
        )
        .await?;
    }
    discard_resumed_chunks(txn, &discard).await
}

/// Deletes the chunks `ids`, each held across its definition's resume, and
/// parks the catch-up marker for each one's source unless its definition is
/// frozen again (see [`free_or_discard_claims`] for why). The caller holds a
/// lock on each chunk row and on its definition row.
async fn discard_resumed_chunks(
    txn: &tokio_postgres::Transaction<'_>,
    ids: &[i64],
) -> Result<(), ChunkQueueError> {
    if ids.is_empty() {
        return Ok(());
    }
    let discarded = txn
        .query(
            "delete from backfill_chunks bc using transform_definitions d \
             where bc.id = any($1) and d.id = bc.definition_id \
             returning d.source_table, d.status",
            &[&ids],
        )
        .await?;
    let mut sources = std::collections::BTreeSet::new();
    for row in discarded {
        let status_text: String = row.get(1);
        let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
            panic!("transform_definitions.status held unrecognized value '{status_text}'")
        });
        if !status.is_frozen() {
            sources.insert(row.get::<_, String>(0));
        }
    }
    for source in sources {
        crate::intake::publication::park_marker(txn, &source).await?;
    }
    Ok(())
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
///
/// The target schema comes from the definition's own persisted, qualified
/// `target_table` identity, never from the running worker's configuration
/// (issue #370): `install_definition` resolved it once, honoring an explicit
/// `TRANSFORM custom.t` spelling over `Config::target_schema`, and created
/// the table there. A chunk re-deriving it from whatever schema the worker
/// happens to be configured with would write into the wrong (usually
/// nonexistent) table. Same derivation `catalog::alter_transform` uses.
pub async fn run_claimed_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    heartbeat_interval: Duration,
) -> Result<(), ChunkQueueError> {
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    let (target_schema, _) = definition
        .target_table
        .split_once('.')
        .expect("target_table is always schema-qualified (issue #73)");

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
///
/// **A chunk held across its definition's resume completes nothing (issue
/// #397).** The resumed definition goes live through its rebuild's discharge
/// (`intake::publication::run_pending_backfills`), which promotes it
/// `backfilling` before enumerating and flips it `live` only once that
/// enumeration commits. Completing it from here in between would flip it
/// `live` early. So the chunk is discarded instead, parking the same
/// catch-up marker its completion would have (see
/// [`free_or_discard_claims`]). [`RESUMED`] is read under this function's
/// `for update` lock, which resume takes too.
pub async fn finish_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
) -> Result<(), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let resumed = txn
        .query_opt(
            &format!("select {RESUMED} from transform_definitions d where id = $1 for update"),
            &[&chunk.definition_id],
        )
        .await?
        .is_some_and(|row| row.get::<_, bool>(0));
    if resumed {
        let held: Vec<i64> = txn
            .query(
                "select id from backfill_chunks \
                 where id = $1 and claimed_by = $2 and not done for update",
                &[&chunk.id, &claimed_by],
            )
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect();
        discard_resumed_chunks(&txn, &held).await?;
        txn.commit().await?;
        return Ok(());
    }

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

/// The shared "no chunks left? then flip to live" check both
/// [`finish_chunk`] and [`enqueue_one_to_one`]'s zero-chunk case run once they
/// already hold the definition row's `for update` lock. Returns the status
/// the completion left the definition in, or `None` while chunks remain —
/// "flip to live" only ever happens from `backfilling` (issue #331; see
/// `complete_direct_backfill`).
async fn complete_if_no_chunks_remain_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
) -> Result<Option<TransformStatus>, CatalogError> {
    let remaining: bool = txn
        .query_one(
            "select exists(select 1 from backfill_chunks where definition_id = $1 and not done)",
            &[&definition_id],
        )
        .await?
        .get(0);
    if remaining {
        return Ok(None);
    }
    Ok(Some(
        catalog::complete_direct_backfill(txn, definition_id).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::lifecycle::pause_transform;
    use crate::defs::parser::parse;
    use crate::staging::quarantine::resume_transform;
    use tokio_postgres::NoTls;

    const WORKER: &str = "issue-360-worker";

    /// A same-crate pool plus a raw connection onto `db` (testkit's own
    /// `db.pool` is the other crate instance's `Pool` type).
    async fn connect(db: &testkit::TestDatabase) -> (Pool, tokio_postgres::Client) {
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {}, public",
            crate::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("set search_path");
        (pool, raw)
    }

    /// Seeds `public.<source>` with `rows` rows and a `backfilling` definition
    /// `<target>` over it: the state `install_plain_one_to_one` has committed
    /// by the time it calls [`enqueue_one_to_one`].
    async fn seed_backfilling(
        raw: &tokio_postgres::Client,
        source: &str,
        target: &str,
        rows: i64,
    ) -> (i64, TransformDef) {
        let text = format!("TRANSFORM {target} FROM {source} SELECT a + a AS x");
        raw.batch_execute(&format!(
            "create table public.{source} (id bigint primary key, a numeric); \
             insert into public.{source} select g, g from generate_series(1, {rows}) g; \
             insert into source_table_versions (source_table, version) \
             values ('public.{source}', 1)"
        ))
        .await
        .expect("seed source");
        let id: i64 = raw
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ($1, $2, 1, $3, 'backfilling') returning id",
                &[
                    &format!("public.{target}"),
                    &format!("public.{source}"),
                    &text,
                ],
            )
            .await
            .expect("seed definition")
            .get(0);
        (id, parse(&text).expect("parse definition"))
    }

    async fn status_of(raw: &tokio_postgres::Client, id: i64) -> String {
        raw.query_one(
            "select status from transform_definitions where id = $1",
            &[&id],
        )
        .await
        .expect("read status")
        .get(0)
    }

    async fn chunk_count(raw: &tokio_postgres::Client, id: i64) -> i64 {
        raw.query_one(
            "select count(*) from backfill_chunks where definition_id = $1",
            &[&id],
        )
        .await
        .expect("count chunks")
        .get(0)
    }

    async fn marker_generation(raw: &tokio_postgres::Client, table: &str) -> Option<i64> {
        raw.query_opt(
            "select generation from pending_backfill where table_name = $1",
            &[&table],
        )
        .await
        .expect("read marker")
        .map(|row| row.get(0))
    }

    /// Issue #360, race 2: `install_definition` commits the `backfilling` row
    /// before `enqueue_one_to_one` inserts its chunks. A PAUSE and RESUME
    /// landing in that gap find no chunks to discard, so the chunks inserted
    /// afterward must not be enqueued at all: resume already parked the
    /// rebuild that covers them.
    #[tokio::test]
    async fn a_resume_that_raced_the_install_leaves_nothing_to_enqueue() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_backfilling(&raw, "orders", "order_doubles", 3).await;

        pause_transform(&pool, "order_doubles")
            .await
            .expect("pause");
        resume_transform(&pool, "order_doubles")
            .await
            .expect("resume");

        let status = enqueue_one_to_one(&pool, id, &def, "public.orders")
            .await
            .expect("enqueue");
        assert_eq!(status, TransformStatus::WaitingToBackfill);
        assert_eq!(
            chunk_count(&raw, id).await,
            0,
            "nothing is enqueued for a definition resume already rebuilds"
        );
    }

    /// Issue #360, race 2's zero-chunk arm: an empty source's install
    /// completes the definition directly. After a racing resume, that
    /// completion would flip the definition `live` in the middle of the
    /// rebuild's own discharge (which moves it `waiting_to_backfill` ->
    /// `backfilling` -> `live` around its enumeration).
    #[tokio::test]
    async fn a_resume_that_raced_an_empty_install_is_not_completed_by_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_backfilling(&raw, "orders", "order_doubles", 0).await;

        pause_transform(&pool, "order_doubles")
            .await
            .expect("pause");
        resume_transform(&pool, "order_doubles")
            .await
            .expect("resume");
        // The rebuild's discharge has promoted it and is enumerating.
        raw.execute(
            "update transform_definitions set status = 'backfilling' where id = $1",
            &[&id],
        )
        .await
        .expect("promote as the discharge does");

        let status = enqueue_one_to_one(&pool, id, &def, "public.orders")
            .await
            .expect("enqueue");
        assert_eq!(status, TransformStatus::Backfilling);
        assert_eq!(
            status_of(&raw, id).await,
            "backfilling",
            "the discharge, not the install, takes a resumed definition live"
        );
    }

    /// A pause that lands in the same gap without a resume still enqueues:
    /// the frozen definition's chunks are withheld from dispatch, and a later
    /// resume discards them as it does any paused definition's (#332).
    #[tokio::test]
    async fn a_pause_that_raced_the_install_still_enqueues_for_resume_to_discard() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_backfilling(&raw, "orders", "order_doubles", 3).await;

        pause_transform(&pool, "order_doubles")
            .await
            .expect("pause");
        let status = enqueue_one_to_one(&pool, id, &def, "public.orders")
            .await
            .expect("enqueue");
        assert_eq!(status, TransformStatus::Backfilling);
        assert_eq!(chunk_count(&raw, id).await, 1);

        resume_transform(&pool, "order_doubles")
            .await
            .expect("resume");
        assert_eq!(chunk_count(&raw, id).await, 0);
    }

    /// Enqueues `id`'s chunks, claims its one chunk for [`WORKER`], then
    /// pauses and resumes the definition while that claim is held (#332
    /// leaves a held chunk alone).
    async fn hold_a_chunk_across_a_resume(
        pool: &Pool,
        raw: &tokio_postgres::Client,
        id: i64,
        def: &TransformDef,
        target: &str,
        source: &str,
    ) -> ClaimedChunk {
        enqueue_one_to_one(pool, id, def, source)
            .await
            .expect("enqueue");
        let mut held = claim_chunks(raw, WORKER, 1).await.expect("claim");
        assert_eq!(held.len(), 1, "precondition: one chunk held");
        let held = held.remove(0);
        assert_eq!(held.definition_id, id);
        pause_transform(pool, target).await.expect("pause");
        resume_transform(pool, target).await.expect("resume");
        assert_eq!(
            chunk_count(raw, id).await,
            1,
            "precondition: resume leaves the held chunk to its worker"
        );
        held
    }

    /// Issue #360, race 1 (write failed): a chunk held across the resume and
    /// then released is discarded, not handed out again. Its failed write may
    /// have committed part of its range, so the catch-up marker the rerun's
    /// completion would have parked is parked here instead.
    #[tokio::test]
    async fn releasing_a_chunk_held_across_a_resume_discards_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let (id, def) = seed_backfilling(&raw, "orders", "order_doubles", 3).await;
        let held =
            hold_a_chunk_across_a_resume(&pool, &raw, id, &def, "order_doubles", "public.orders")
                .await;
        let parked = marker_generation(&raw, "public.orders").await;
        assert!(parked.is_some(), "precondition: resume parked a marker");

        let released = release_chunk(&mut raw, held.id, WORKER)
            .await
            .expect("release");
        assert_eq!(released, 1);

        assert_eq!(chunk_count(&raw, id).await, 0, "the chunk was discarded");
        assert!(
            claim_chunks(&raw, WORKER, 100)
                .await
                .expect("claim")
                .is_empty(),
            "nothing is claimable again"
        );
        assert!(
            marker_generation(&raw, "public.orders").await > parked,
            "the catch-up the rerun would have parked is parked"
        );
    }

    /// Issue #360, race 1 (worker died): the stale-claim sweep discards a
    /// chunk held across its definition's resume, and still frees a
    /// never-resumed sibling's stale claim for another worker.
    #[tokio::test]
    async fn reclaiming_a_chunk_held_across_a_resume_discards_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let (id, def) = seed_backfilling(&raw, "orders", "order_doubles", 3).await;
        let held =
            hold_a_chunk_across_a_resume(&pool, &raw, id, &def, "order_doubles", "public.orders")
                .await;
        let parked = marker_generation(&raw, "public.orders").await;

        let (sibling, sibling_def) = seed_backfilling(&raw, "items", "item_doubles", 3).await;
        enqueue_one_to_one(&pool, sibling, &sibling_def, "public.items")
            .await
            .expect("enqueue sibling");
        let sibling_held = claim_chunks(&raw, WORKER, 1).await.expect("claim sibling");
        assert_eq!(sibling_held.len(), 1);
        assert_eq!(sibling_held[0].definition_id, sibling);

        // A zero TTL makes every claim taken in an earlier transaction stale.
        let reclaimed = reclaim_stale_chunks(&mut raw, Duration::ZERO)
            .await
            .expect("reclaim");
        assert_eq!(reclaimed, 2, "both stale claims are dealt with");

        assert_eq!(chunk_count(&raw, id).await, 0, "the chunk was discarded");
        assert!(
            marker_generation(&raw, "public.orders").await > parked,
            "the catch-up the rerun would have parked is parked"
        );
        let reclaimable = claim_chunks(&raw, WORKER, 100).await.expect("claim");
        assert_eq!(
            reclaimable
                .iter()
                .map(|c| (c.id, c.definition_id))
                .collect::<Vec<_>>(),
            [(sibling_held[0].id, sibling)],
            "only the never-resumed sibling's chunk is claimable again, not {held:?}"
        );
    }

    /// Issue #397: a chunk held across the resume that *finishes* while the
    /// rebuild's discharge has promoted the definition `backfilling` (between
    /// its promotion and its enumeration committing) must not complete it:
    /// the discharge takes it `live` once its enumeration commits. The chunk
    /// is retired and the catch-up its completion would have parked is parked.
    #[tokio::test]
    async fn finishing_a_chunk_held_across_a_resume_leaves_the_flip_to_the_discharge() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_backfilling(&raw, "orders", "order_doubles", 3).await;
        let held =
            hold_a_chunk_across_a_resume(&pool, &raw, id, &def, "order_doubles", "public.orders")
                .await;
        let parked = marker_generation(&raw, "public.orders").await;
        raw.execute(
            "update transform_definitions set status = 'backfilling' where id = $1",
            &[&id],
        )
        .await
        .expect("promote as the discharge does");

        finish_chunk(&pool, &held, WORKER).await.expect("finish");

        assert_eq!(
            status_of(&raw, id).await,
            "backfilling",
            "the discharge, not the held chunk, takes a resumed definition live"
        );
        assert_eq!(chunk_count(&raw, id).await, 0, "the chunk is retired");
        assert!(
            marker_generation(&raw, "public.orders").await > parked,
            "the catch-up the completion would have parked is parked"
        );
    }

    /// Issue #396: a real error completing an empty install is returned as
    /// it is. It used to be mapped to `BackfillError::Unsupported`, which the
    /// install answers by silently falling back to the ring. Here the
    /// definition row vanishes before the completion reads it back.
    #[tokio::test]
    async fn a_failed_empty_install_completion_is_not_mistaken_for_unsupported() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_backfilling(&raw, "orders", "order_doubles", 0).await;
        raw.execute("delete from transform_definitions where id = $1", &[&id])
            .await
            .expect("delete the definition row");

        let err = enqueue_one_to_one(&pool, id, &def, "public.orders")
            .await
            .expect_err("completing a vanished definition fails");
        assert!(
            !matches!(
                err,
                CatalogError::DirectBackfill(BackfillError::Unsupported(_))
            ),
            "a real completion error must not read as Unsupported: {err:?}"
        );
    }
}
