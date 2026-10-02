//! The durable, claimable backfill-chunk work queue (public API design's
//! ADR-0007 amendment — see `docs/decisions/0007-direct-set-based-backfill.md`'s
//! "Backgrounding and resumability" section and `docs/decisions/0008-public-api-design.md`'s
//! decision 1).
//!
//! A plain (non-relationship) 1-1 definition's PK-range chunks are not run
//! back-to-back in one call: once the definition's capture point has passed,
//! the backfill discharge (`intake::markers::run_pending_backfills`,
//! ADR-0016) plans the same boundaries [`super::backfill::plan_one_to_one_chunks`]
//! always computed and [`dispatch_one_to_one`] persists each as a row in the
//! `backfill_chunks` table (`V20__backfill_chunks.sql`), in the discharge's own
//! transaction. That applies to a fresh registration and to a resumed
//! definition's rebuild alike. [`claim_chunks`]/[`reclaim_stale_chunks`]/
//! [`fail_chunk`] mirror `staging::claim`/`staging::liveness`'s
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
//! An aggregate or relationship-enriched 1-1 definition isn't split into
//! ranges: its build materializes connection-scoped `TEMP TABLE` staging,
//! which doesn't survive being read by independent drain workers on separate
//! connections (the open problem the ADR amendment calls out). The discharge
//! enqueues its whole direct build as one job instead ([`dispatch_direct_build`],
//! issue #419), a row with no bounds ([`ChunkWork::DirectBuild`]) that one
//! drain worker runs start to finish. Everything above applies to it
//! unchanged, except that a failed job hands its build back to the discharge
//! rather than being released for a retry ([`fail_chunk`]).
//!
//! A chunk that fails never retries silently (#616). [`fail_chunk`] records
//! the failure on the chunk's row, where `Trellis::status` reports it, logs
//! it at warn, and by its kind backs the chunk off for a retry, splits it to
//! narrow a failure on its data down to the key that causes it and
//! quarantines that key, or, once a failure that can't be narrowed has been
//! charged [`MAX_CHARGED_ATTEMPTS`] times, pauses the definition.

use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_postgres::GenericClient;

use crate::error_code::{self, ErrorCode};
use crate::pool::Pool;

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
    /// Quarantining the key a failed chunk narrowed to, or checking the
    /// whole-transform fuse after it, failed (#616).
    Quarantine(Box<crate::staging::ApplyError>),
    /// A Re-derive build's plan job or chunk failed (#625 F2,
    /// `staging::build`).
    Build(Box<crate::staging::ApplyError>),
    /// A claimed row's `kind` and bounds were not one this build knows
    /// (`backfill_chunks_kind_bounds` rules out a mismatch), or a Re-derive
    /// build's row reached the old build's runner.
    UnknownKind { kind: String },
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
            ChunkQueueError::Quarantine(err) => err.code(),
            ChunkQueueError::Build(err) => err.code(),
            ChunkQueueError::UnknownKind { .. } => ErrorCode::Internal,
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
            ChunkQueueError::Quarantine(err) => {
                write!(
                    f,
                    "quarantining a failed backfill chunk's key failed: {err}"
                )
            }
            ChunkQueueError::Build(err) => write!(f, "re-derive build step failed: {err}"),
            ChunkQueueError::UnknownKind { kind } => {
                write!(f, "claimed a backfill chunk of unknown kind {kind:?}")
            }
        }
    }
}

impl std::error::Error for ChunkQueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ChunkQueueError::Db(err) => Some(err),
            ChunkQueueError::Pool(err) => Some(err),
            ChunkQueueError::Backfill(err) => Some(err),
            ChunkQueueError::Catalog(err) => Some(err),
            ChunkQueueError::DefinitionNotFound { .. } => None,
            ChunkQueueError::Quarantine(err) => Some(err.as_ref()),
            ChunkQueueError::Build(err) => Some(err.as_ref()),
            ChunkQueueError::UnknownKind { .. } => None,
        }
    }
}

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

/// One durable `backfill_chunks` row, as claimed by [`claim_chunks`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedChunk {
    pub id: i64,
    pub definition_id: i64,
    pub work: ChunkWork,
}

/// What one `backfill_chunks` row builds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkWork {
    /// A plain 1-1 definition's primary-key range. `lo` is `None` for the
    /// first chunk (`pk <= hi`, no lower bound) and `Some` for every later
    /// one.
    Range { lo: Option<String>, hi: String },
    /// An aggregate or relationship-enriched 1-1 definition's whole direct
    /// set-based build (ADR-0007), run as one job (issue #419; the row's
    /// `hi` is null). See [`dispatch_direct_build`].
    DirectBuild,
    /// A Re-derive build's plan job (#625 F2, `staging::build`): walks the
    /// source's primary key from `cursor`, the last boundary it enqueued
    /// (`None` before the first), and enqueues [`ChunkWork::Rederive`] rows
    /// in batches.
    Plan { cursor: Option<String> },
    /// One `(lo, hi]` range of a Re-derive build (`staging::build::run_chunk`).
    Rederive { lo: Option<String>, hi: String },
    /// A rebuilt Re-derive build's sweep (#625 F3, `staging::build`): walks
    /// the target's ledger from `cursor`, the last entry key it has passed
    /// (`None` before the first), and re-derives every live entry the
    /// build's chunks didn't. It is claimed only once its build's plan job
    /// and every chunk are done ([`claim_chunks_of`]).
    Sweep { cursor: Option<String> },
}

impl ChunkWork {
    /// The `backfill_chunks.kind` word of this work (V64).
    pub fn kind(&self) -> &'static str {
        match self {
            ChunkWork::Range { .. } => KIND_RANGE,
            ChunkWork::DirectBuild => KIND_DIRECT,
            ChunkWork::Plan { .. } => KIND_PLAN,
            ChunkWork::Rederive { .. } => KIND_REDERIVE,
            ChunkWork::Sweep { .. } => KIND_SWEEP,
        }
    }
}

/// `backfill_chunks.kind` of a plain 1-1 definition's range chunk.
pub const KIND_RANGE: &str = "range";
/// `backfill_chunks.kind` of a direct-build job.
pub const KIND_DIRECT: &str = "direct";
/// `backfill_chunks.kind` of a Re-derive build's plan job.
pub const KIND_PLAN: &str = "plan";
/// `backfill_chunks.kind` of a Re-derive build's chunk.
pub const KIND_REDERIVE: &str = "rederive";
/// `backfill_chunks.kind` of a rebuild's sweep job (#625 F3).
pub const KIND_SWEEP: &str = "sweep";

/// The kinds the old builds' path claims ([`claim_chunks`]). A drain worker
/// runs these after its segments, as it does the Re-derive build's kinds
/// (`staging::build`, #625 B6).
pub const OLD_BUILD_KINDS: [&str; 2] = [KIND_RANGE, KIND_DIRECT];

/// Dispatches a plain (non-relationship) 1-1 definition's build onto the
/// durable queue, inside the backfill discharge's own transaction (ADR-0016):
/// moves `definition_id` `waiting_to_backfill` -> `backfilling` and persists
/// `ranges` (from [`backfill::plan_one_to_one_chunks`]) as its
/// `backfill_chunks` rows, so the status and the work that drives it commit
/// together (issue #404's rule) or not at all.
///
/// Each chunk records the definition's `fuse_rearmed_at` as of this dispatch
/// (see [`STALE`]), read under the row lock the status update takes, the same
/// lock pause and resume take.
///
/// A source with zero rows plans zero chunks; since nothing would ever finish
/// a "last chunk" to trigger the `backfilling` -> `live` flip, this completes
/// the definition itself, through the same completion `finish_chunk` uses.
///
/// Returns the status the dispatch left the definition in, or `None` when it
/// was no longer `waiting_to_backfill` (an operator paused it since the
/// discharge read it): it is left as it is, and its resume parks a marker of
/// its own.
pub(crate) async fn dispatch_one_to_one(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
    ranges: &[(Option<String>, String)],
) -> Result<Option<TransformStatus>, CatalogError> {
    if !start_backfilling(txn, definition_id).await? {
        return Ok(None);
    }

    if ranges.is_empty() {
        // Reports what the completion actually left the definition in,
        // through the same lock-then-check-then-complete sequence
        // `finish_chunk` uses (the status update above holds the lock),
        // keeping there being exactly one way a direct-build definition ever
        // completes.
        return Ok(Some(
            complete_if_no_chunks_remain_in_txn(txn, definition_id)
                .await?
                .unwrap_or(TransformStatus::Backfilling),
        ));
    }

    let (los, his): (Vec<Option<&str>>, Vec<&str>) = ranges
        .iter()
        .map(|(lo, hi)| (lo.as_deref(), hi.as_str()))
        .unzip();
    txn.execute(
        "insert into backfill_chunks (definition_id, kind, lo, hi, fuse_rearmed_at) \
         select d.id, 'range', r.lo, r.hi, d.fuse_rearmed_at \
         from unnest($2::text[], $3::text[]) with ordinality as r(lo, hi, n) \
         cross join transform_definitions d \
         where d.id = $1 \
         order by r.n",
        &[&definition_id, &los, &his],
    )
    .await?;
    tracing::info!(
        definition_id,
        chunks = ranges.len(),
        from = %TransformStatus::WaitingToBackfill.as_str(),
        to = %TransformStatus::Backfilling.as_str(),
        "transform status transition: backfill chunks enqueued"
    );
    Ok(Some(TransformStatus::Backfilling))
}

/// Moves `definition_id` `waiting_to_backfill` -> `backfilling`, taking its
/// row lock. Returns `false`, touching nothing, when it had already left
/// `waiting_to_backfill` (an operator paused it since the discharge read it).
///
/// Clears `build`: a definition paused during a Re-derive build and resumed
/// onto this path is no longer under one, so it must not apply while this
/// build runs (`defs::model::APPLYING_SQL`, #625 F2).
async fn start_backfilling(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
) -> Result<bool, CatalogError> {
    let promoted = txn
        .execute(
            "update transform_definitions set status = $1, build = null \
             where id = $2 and status = $3",
            &[
                &TransformStatus::Backfilling.as_str(),
                &definition_id,
                &TransformStatus::WaitingToBackfill.as_str(),
            ],
        )
        .await?;
    Ok(promoted == 1)
}

/// Dispatches an aggregate or relationship-enriched 1-1 definition's direct
/// set-based build (ADR-0007) as one background job, inside the backfill
/// discharge's own transaction (ADR-0016, issue #419): moves `definition_id`
/// `waiting_to_backfill` -> `backfilling` and persists the job as a
/// [`ChunkWork::DirectBuild`] row, so the status and the work that drives it
/// commit together (issue #404's rule) or not at all.
///
/// The job is a `backfill_chunks` row so it gets the queue's whole driver
/// contract for free: a drain thread claims it ([`claim_chunks`]), heartbeats
/// the claim for as long as the build runs ([`run_claimed_chunk`]), and flips
/// the definition `live` with its go-live catch-ups when it's done
/// ([`finish_chunk`]); a worker that dies holding it loses the claim to
/// [`reclaim_stale_chunks`], and another worker reruns it (the build's
/// writes are idempotent overwrites); a pause withholds it and a resume
/// supersedes it ([`STALE`]) exactly as for a chunk. A build that fails hands
/// itself back to the discharge ([`fail_chunk`]).
///
/// `prior_attempts` is the failure count of the marker this dispatch is
/// discharging, carried so a failed build backs off from there.
///
/// Returns the status the dispatch left the definition in, or `None` when it
/// was no longer `waiting_to_backfill`, as for [`dispatch_one_to_one`].
pub(crate) async fn dispatch_direct_build(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
    prior_attempts: i32,
) -> Result<Option<TransformStatus>, CatalogError> {
    if !start_backfilling(txn, definition_id).await? {
        return Ok(None);
    }
    txn.execute(
        "insert into backfill_chunks \
             (definition_id, kind, lo, hi, fuse_rearmed_at, prior_attempts) \
         select id, 'direct', null, null, fuse_rearmed_at, $2 \
         from transform_definitions where id = $1",
        &[&definition_id, &prior_attempts],
    )
    .await?;
    tracing::info!(
        definition_id,
        from = %TransformStatus::WaitingToBackfill.as_str(),
        to = %TransformStatus::Backfilling.as_str(),
        "transform status transition: direct build job enqueued"
    );
    Ok(Some(TransformStatus::Backfilling))
}

/// SQL predicate over a `backfill_chunks` row aliased `bc` joined to its
/// definition's `transform_definitions` row aliased `d`: whether the chunk was
/// planned before the definition's latest resume (issues #360/#418). Every
/// resume (`staging::quarantine::resume_transform`) stamps a new
/// `fuse_rearmed_at`, and every chunk copies the value it saw when it was
/// planned ([`dispatch_one_to_one`]), so a chunk from a build the resume
/// superseded carries a different one.
///
/// Resume deletes a stale definition's unclaimed chunks outright. A stale
/// chunk a worker still held across the resume is never claimable again and
/// never completes the definition, whose rebuild (the resume's own discharge,
/// which may enqueue fresh chunks) owns its way back to `live`:
/// [`fail_chunk`]/[`reclaim_stale_chunks`]/[`finish_chunk`] discard it
/// rather than freeing or completing it. It writes nothing after the resume
/// either ([`ClaimFence`]).
pub(crate) const STALE: &str = "bc.fuse_rearmed_at is distinct from d.fuse_rearmed_at";

/// The claim a drain worker holds on one chunk, which fences every target
/// write the chunk makes (issue #434).
///
/// A chunk's write bypasses the target-mutation seam
/// (`staging::target_mutations`), which is only safe while nothing reads the
/// target: while its definition is still being built. A chunk a worker holds
/// across a pause and a resume ([`STALE`]) would break that if its write
/// could land once the resumed definition's rebuild has finished, since
/// readers can attach from then on (`catching_up` is applying). A
/// relationship declared on the target, or a definition reading it, would
/// never hear of that write: when the source change it carries drains, apply
/// finds the target already current, and changes and stages nothing.
///
/// So every target write runs in a transaction that first [`hold`]s the
/// claim: it takes a `for key share` lock on the chunk's row and checks that
/// the claim is still this worker's and not [`STALE`]. Resume locks each
/// chunk a worker holds `for update` before it commits
/// (`staging::quarantine::resume_transform`), so the two serialize:
///
/// - a write in flight when the resume arrives commits before the resume
///   does, while the definition is still frozen, and the rebuild overwrites
///   it;
/// - a write that starts after the resume committed sees the chunk stale
///   and writes nothing ([`BackfillError::Superseded`]).
///
/// No write of a chunk from before a resume lands after it, so a discarded
/// chunk leaves nothing to repair. The fence also stops a worker whose claim
/// was reclaimed from under it (its heartbeat stalled) from writing after the
/// chunk's new holder has finished it.
///
/// `for key share` leaves the heartbeat's refresh of `claimed_at`
/// ([`touch_chunk_claim`], a non-key update) free to run alongside the
/// write, while the stale-claim sweep's `for update skip locked` passes the
/// chunk over.
///
/// Both of those waits last only as long as the write, provided its worker
/// keeps running. A worker that stalls inside the transaction (a frozen
/// process, or a network partition the server hasn't noticed) would hold the
/// lock until its session ended, which nothing else bounds. So [`hold`] sets
/// the transaction's `idle_in_transaction_session_timeout` to the fleet's
/// reclaim TTL first: the server ends a session left idle mid-transaction
/// that long, releasing its locks, at the point the stale-claim sweep would
/// have given up on the claim anyway.
///
/// [`hold`]: ClaimFence::hold
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClaimFence<'a> {
    chunk_id: i64,
    claimed_by: &'a str,
    /// The fleet's reclaim TTL: how long the fenced transaction may sit idle
    /// before the server ends its session.
    idle_timeout: Duration,
}

impl<'a> ClaimFence<'a> {
    /// The fence of `claimed_by`'s claim on chunk `chunk_id`, idle for at
    /// most `idle_timeout` inside a fenced transaction.
    pub(crate) fn new(chunk_id: i64, claimed_by: &'a str, idle_timeout: Duration) -> Self {
        Self {
            chunk_id,
            claimed_by,
            idle_timeout,
        }
    }

    /// Locks the claim for the rest of `txn` and returns whether it still
    /// holds: the chunk is undone, still claimed by this worker, and not
    /// [`STALE`]. The caller writes nothing in `txn` when it doesn't.
    ///
    /// Sets `txn`'s `idle_in_transaction_session_timeout` to the reclaim
    /// TTL before it takes the lock. `set local` is enough: the server arms
    /// that timer each time the session goes idle inside a transaction, from
    /// the setting's value at that moment.
    pub(crate) async fn hold(
        &self,
        txn: &impl GenericClient,
    ) -> Result<bool, tokio_postgres::Error> {
        // Zero would disable the timeout, and the setting is an `int` of
        // milliseconds.
        let idle_ms = self.idle_timeout.as_millis().clamp(1, i32::MAX as u128);
        txn.batch_execute(&format!(
            "set local idle_in_transaction_session_timeout = {idle_ms}"
        ))
        .await?;
        let claimed = txn
            .query_opt(
                "select 1 from backfill_chunks \
                 where id = $1 and claimed_by = $2 and not done for key share",
                &[&self.chunk_id, &self.claimed_by],
            )
            .await?
            .is_some();
        if !claimed {
            return Ok(false);
        }
        // A separate statement, so its snapshot is taken after the lock above
        // was granted: a resume that held the chunk row has committed, and
        // its `fuse_rearmed_at` stamp is visible here. Reading the definition
        // row in the locking statement would miss it, since a lock wait
        // re-checks only the rows the statement locks.
        let stale: bool = txn
            .query_one(
                &format!(
                    "select {STALE} from backfill_chunks bc \
                     join transform_definitions d on d.id = bc.definition_id \
                     where bc.id = $1"
                ),
                &[&self.chunk_id],
            )
            .await?
            .get(0);
        Ok(!stale)
    }
}

/// Claims up to `limit` unclaimed, undone chunks for `claimed_by` in one
/// statement — the backfill-chunk analogue of `staging::claim`/
/// `staging::next_claimable_segments`, collapsed onto a single table+statement
/// since a chunk is never bucket-split (see this module's doc comment). The
/// `for update skip locked` candidate selection means two workers racing this
/// call never claim the same row twice and never block on each other.
///
/// Claims only the old builds' kinds ([`OLD_BUILD_KINDS`]); a Re-derive
/// build's work is claimed by [`claim_chunks_of`] (#625 F2).
pub async fn claim_chunks(
    client: &impl GenericClient,
    claimed_by: &str,
    limit: i64,
) -> Result<Vec<ClaimedChunk>, ChunkQueueError> {
    claim_chunks_of(client, claimed_by, limit, &OLD_BUILD_KINDS).await
}

/// [`claim_chunks`] for the rows whose `kind` is one of `kinds`.
pub async fn claim_chunks_of(
    client: &impl GenericClient,
    claimed_by: &str,
    limit: i64,
    kinds: &[&str],
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
            // fold's own applying-status gate reads, and stops handing out
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
            //
            // A chunk whose last run failed waits out its backoff
            // (`next_attempt_at`, #616) before it is handed out again.
            //
            // A Re-derive build's sweep (#625 F3) waits for its build's plan
            // job and every chunk: it re-derives what they didn't, so it
            // reads their bases. A row held across a resume doesn't hold it.
            "with candidate as ( \
                 select bc.id from backfill_chunks bc \
                 where not bc.done and bc.claimed_by is null and bc.kind = any($4) \
                   and (bc.next_attempt_at is null or bc.next_attempt_at <= now()) \
                   and exists ( \
                       select 1 from transform_definitions d \
                       where d.id = bc.definition_id and d.status = any($3) \
                   ) \
                   and (bc.kind <> 'sweep' or not exists ( \
                       select 1 from backfill_chunks o \
                       join transform_definitions od on od.id = o.definition_id \
                       where o.definition_id = bc.definition_id \
                         and o.kind in ('plan', 'rederive') and not o.done \
                         and o.fuse_rearmed_at is not distinct from od.fuse_rearmed_at \
                   )) \
                 order by bc.id \
                 for update skip locked \
                 limit $2 \
             ) \
             update backfill_chunks c \
             set claimed_by = $1, claimed_at = now() \
             from candidate \
             where c.id = candidate.id \
             returning c.id, c.definition_id, c.lo, c.hi, c.kind",
            &[&claimed_by, &limit, &dispatchable, &kinds],
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            let kind: String = row.get(4);
            let (lo, hi): (Option<String>, Option<String>) = (row.get(2), row.get(3));
            let work = match (kind.as_str(), hi) {
                (KIND_RANGE, Some(hi)) => ChunkWork::Range { lo, hi },
                (KIND_DIRECT, None) => ChunkWork::DirectBuild,
                (KIND_PLAN, None) => ChunkWork::Plan { cursor: lo },
                (KIND_REDERIVE, Some(hi)) => ChunkWork::Rederive { lo, hi },
                (KIND_SWEEP, None) => ChunkWork::Sweep { cursor: lo },
                (kind, _) => {
                    return Err(ChunkQueueError::UnknownKind {
                        kind: kind.to_string(),
                    });
                }
            };
            Ok(ClaimedChunk {
                id: row.get(0),
                definition_id: row.get(1),
                work,
            })
        })
        .collect()
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
/// A chunk held across its definition's resume ([`STALE`]) is deleted
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
    // on a later pass.
    let rows = txn
        .query(
            &format!(
                "select bc.id, {STALE} from backfill_chunks bc \
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

/// Releases a claim immediately, recording nothing — the `backfill_chunks`
/// analogue of `staging::liveness::release`. A drain worker gives up a
/// failed chunk through [`fail_chunk`] instead, which records the failure
/// (#616); this is the bare release the tests drive a held chunk with.
/// Scoped `claimed_by = $2` for the same reason that function is: a claim
/// already reclaimed out from under this caller (the TTL sweep, or another
/// worker) is left untouched.
///
/// A chunk held across its definition's resume ([`STALE`]) is deleted instead
/// of freed (issue #360) — see [`free_or_discard_claims`]. Returns how many
/// claims (zero or one) it released or discarded.
#[cfg(any(test, feature = "internals"))]
pub async fn release_chunk(
    client: &mut impl GenericClient,
    id: i64,
    claimed_by: &str,
) -> Result<u64, ChunkQueueError> {
    let txn = client.transaction().await?;
    // The definition row first, then the chunk: the order resume takes them
    // in (it locks a held chunk to wait out its write, issue #434), so the
    // two can't deadlock.
    txn.execute(
        "select 1 from transform_definitions \
         where id = (select definition_id from backfill_chunks where id = $1) for share",
        &[&id],
    )
    .await?;
    let rows = txn
        .query(
            &format!(
                "select bc.id, {STALE} from backfill_chunks bc \
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

/// How many failures a chunk may be charged before its definition is paused
/// with the error (#616). A failure is charged when it is neither transient
/// nor narrowed to a key: it says nothing about any one row, and retrying it
/// without end would leave the definition `backfilling` forever with only a
/// log line to show for it. A direct-build job counts its handed-back
/// attempts against the same limit.
pub(crate) const MAX_CHARGED_ATTEMPTS: i32 = 5;

/// The backoff after a chunk's first failure, doubled per further failure.
const RETRY_BASE: Duration = Duration::from_secs(1);

/// The longest a failed chunk waits before it is claimed again.
const RETRY_CAP: Duration = Duration::from_secs(300);

/// The longest a failed Re-derive chunk or plan job waits before it is
/// claimed again (#625 F2). Its usual failure is its entry lock's short
/// timeout behind a drain page, which says nothing about the chunk.
const REDERIVE_RETRY_CAP: Duration = Duration::from_secs(5);

/// How long a chunk waits after its `attempts`th failure before it is
/// claimed again: [`RETRY_BASE`], doubled per further failure, capped at
/// [`RETRY_CAP`].
fn retry_delay(attempts: i32, cap: Duration) -> Duration {
    let doublings = u32::try_from(attempts.saturating_sub(1))
        .unwrap_or(0)
        .min(31);
    RETRY_BASE.saturating_mul(1 << doublings).min(cap)
}

/// What a chunk's failure says about how to retry it (#616).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureKind {
    /// A lost connection, a lock or serialization conflict, a busy server:
    /// the same failures the drain retries without charging a key
    /// (`staging::quarantine::is_transient_error`).
    Transient,
    /// Postgres rejected a value (SQLSTATE class `22`, a data exception such
    /// as an overflow or a division by zero) or a constraint (class `23`).
    /// Some row of the range caused it, so the chunk narrows itself to that
    /// row's key.
    Data,
    /// Anything else: a missing table or column, an unsupported shape, a
    /// failure with no SQLSTATE of its own. It would fail for every key of
    /// the range alike, so narrowing it would quarantine whichever key it
    /// tried alone; it is retried and charged instead.
    Unnarrowable,
}

impl FailureKind {
    fn of(err: &ChunkQueueError) -> Self {
        if crate::staging::quarantine::is_transient_error(err) {
            return FailureKind::Transient;
        }
        let mut link: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(err) = link {
            if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
                return match pg.code().map(|code| code.code().get(..2)) {
                    Some(Some("22" | "23")) => FailureKind::Data,
                    _ => FailureKind::Unnarrowable,
                };
            }
            link = err.source();
        }
        FailureKind::Unnarrowable
    }
}

/// What [`fail_chunk`] did with a failed chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkFailure {
    /// The caller's claim was already gone (reclaimed by the stale-claim
    /// sweep): the chunk is its new holder's, and nothing was recorded.
    NotHeld,
    /// The chunk was held across its definition's resume ([`STALE`]), or its
    /// definition was frozen while a direct-build job ran: it was deleted,
    /// and the resume's rebuild owns the work.
    Discarded,
    /// The chunk is claimable again after a backoff. `charged` says whether
    /// the failure counted toward [`MAX_CHARGED_ATTEMPTS`].
    Retrying { charged: bool, delay: Duration },
    /// A data failure: the chunk was split at `mid` into two chunks that run
    /// at once, narrowing the failure toward the key that causes it.
    Split { mid: String },
    /// A data failure narrowed to `key`, now quarantined
    /// (`staging::quarantine::evict_build_key`); the chunk runs again at once
    /// without it. `fuse_tripped` says whether that quarantined the
    /// definition too, as the whole-transform fuse does after
    /// [`crate::staging::quarantine::DEFAULT_TRANSFORM_DEATH_THRESHOLD`]
    /// keys.
    Quarantined { key: String, fuse_tripped: bool },
    /// The chunk was charged its [`MAX_CHARGED_ATTEMPTS`]th failure, so its
    /// definition was paused. The chunk stays, unclaimed, as the record
    /// `Trellis::status` reports until a resume replaces it.
    Paused,
    /// A direct-build job handed its build back to the backfill discharge,
    /// whose marker carries `attempts` failures.
    HandedBack { attempts: i32 },
}

/// Gives up `chunk` after running it failed with `error` — what a drain
/// worker calls instead of [`finish_chunk`] on a failure — and logs at warn
/// what it did, with the definition, the chunk, its range or key, the
/// attempt and when the next one is (#616).
///
/// A [`ChunkWork::Range`] chunk records the failure on its row (`attempts`,
/// `last_error`, `next_attempt_at`, which `Trellis::status` reports) and,
/// by the failure's [`FailureKind`]:
///
/// - **transient:** is released for a retry after a backoff, uncharged;
/// - **data:** is narrowed by bisection. While its range holds more than one
///   key it is split in two by key count, and both halves are claimable at
///   once; the half without the failing key finishes, and the other splits
///   again. A range down to one key quarantines that key
///   (`staging::quarantine::evict_build_key`) and runs again without it, so
///   the build finishes without the key, and the whole-transform fuse is
///   checked (`staging::quarantine::trip_build_fuse`);
/// - **anything else**, or a data failure whose range no longer holds a key:
///   is released after a backoff and charged, and its
///   [`MAX_CHARGED_ATTEMPTS`]th charge pauses the definition instead.
///
/// A [`ChunkWork::DirectBuild`] job **hands its build back to the backfill
/// discharge** (ADR-0016, issue #419), in one transaction: the job row is
/// deleted, the definition moves `backfilling` -> `waiting_to_backfill`, and
/// its source's marker is re-parked carrying the failure as its retry state
/// (`intake::markers::park_failed_build`). The marker's backoff (issue
/// #407) then paces the retry, rather than a drain worker re-running a
/// whole-table build as fast as it can fail, and [`crate::Trellis::status`]
/// reports the error through the marker like any failed discharge. The
/// retry is a fresh dispatch, so it re-plans the build by shape: a build that
/// failed [`BackfillError::Unsupported`] because the catalog changed under it
/// goes to the ring fallback. A transient failure isn't charged an attempt.
/// A build can't be narrowed, so its [`MAX_CHARGED_ATTEMPTS`]th charged
/// failure pauses the definition instead, keeping the job row as the record.
/// A definition that was paused or quarantined meanwhile only loses the job:
/// its resume parks a marker and rebuilds. A job held across a resume
/// ([`STALE`]) is discarded as usual.
///
/// Scoped to `claimed_by`'s current claim: a claim already reclaimed out
/// from under this caller is left to its new holder.
pub async fn fail_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    error: &ChunkQueueError,
) -> Result<ChunkFailure, ChunkQueueError> {
    let kind = FailureKind::of(error);
    let message = error.to_string();
    let (outcome, attempts, location) = match &chunk.work {
        ChunkWork::Range { lo, hi } => {
            let (outcome, attempts) = fail_range_chunk(
                pool,
                chunk,
                Some((lo.as_deref(), hi)),
                claimed_by,
                kind,
                &message,
                RETRY_CAP,
            )
            .await?;
            let location = match &outcome {
                ChunkFailure::Quarantined { key, .. } => format!("key {key}"),
                _ => format!("range ({}, {hi}]", lo.as_deref().unwrap_or("-infinity")),
            };
            (outcome, attempts, location)
        }
        // A Re-derive chunk narrows a data failure to a key as a 1-1 range
        // chunk does (#625 F5, F-A5): its range holds the same keys, and
        // it leaves a quarantined key out the same way. A transient failure
        // (the chunk's short entry-lock timeout, mostly) backs off for at
        // most [`REDERIVE_RETRY_CAP`], so a chunk that keeps meeting drain
        // pages isn't left out for minutes.
        ChunkWork::Rederive { lo, hi } => {
            let (outcome, attempts) = fail_range_chunk(
                pool,
                chunk,
                Some((lo.as_deref(), hi)),
                claimed_by,
                kind,
                &message,
                REDERIVE_RETRY_CAP,
            )
            .await?;
            let location = match &outcome {
                ChunkFailure::Quarantined { key, .. } => format!("key {key}"),
                _ => format!(
                    "re-derive range ({}, {hi}]",
                    lo.as_deref().unwrap_or("-infinity")
                ),
            };
            (outcome, attempts, location)
        }
        ChunkWork::Plan { .. } => {
            let (outcome, attempts) = fail_range_chunk(
                pool,
                chunk,
                None,
                claimed_by,
                kind,
                &message,
                REDERIVE_RETRY_CAP,
            )
            .await?;
            (outcome, attempts, "the re-derive build's plan".to_string())
        }
        ChunkWork::Sweep { cursor } => {
            let (outcome, attempts) = fail_range_chunk(
                pool,
                chunk,
                None,
                claimed_by,
                kind,
                &message,
                REDERIVE_RETRY_CAP,
            )
            .await?;
            let location = format!(
                "the re-derive build's sweep after {}",
                cursor.as_deref().unwrap_or("-infinity")
            );
            (outcome, attempts, location)
        }
        ChunkWork::DirectBuild => {
            let (outcome, attempts) =
                fail_direct_build(pool, chunk, claimed_by, kind, &message).await?;
            (outcome, attempts, "the whole source".to_string())
        }
    };
    log_failure(chunk, kind, &outcome, attempts, &location, &message);
    Ok(outcome)
}

/// [`fail_chunk`]'s log line for one failure. Returns nothing: the line is
/// the whole point.
fn log_failure(
    chunk: &ClaimedChunk,
    kind: FailureKind,
    outcome: &ChunkFailure,
    attempt: i32,
    location: &str,
    error: &str,
) {
    let definition_id = chunk.definition_id;
    let chunk_id = chunk.id;
    let kind = format!("{kind:?}").to_lowercase();
    match outcome {
        ChunkFailure::NotHeld => tracing::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, error = %error,
            "backfill chunk failed after its claim was reclaimed; its new holder retries it"
        ),
        ChunkFailure::Discarded => tracing::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, error = %error,
            "backfill chunk failed after its definition was frozen or resumed; discarded it"
        ),
        ChunkFailure::Retrying { charged, delay } => tracing::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt,
            charged, max_charged = MAX_CHARGED_ATTEMPTS,
            next_attempt_in_secs = delay.as_secs_f64(), error = %error,
            "backfill chunk failed; retrying it after a backoff"
        ),
        ChunkFailure::Split { mid } => tracing::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt,
            split_at = %mid, next_attempt_in_secs = 0.0, error = %error,
            "backfill chunk failed on its data; split it in two to narrow the failure to a key"
        ),
        ChunkFailure::Quarantined { key, fuse_tripped } => tracing::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt,
            key = %key, fuse_tripped, next_attempt_in_secs = 0.0, error = %error,
            "backfill chunk failure narrowed to one key; quarantined the key, and the build \
             goes on without it"
        ),
        ChunkFailure::Paused => tracing::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt,
            max_charged = MAX_CHARGED_ATTEMPTS, error = %error,
            "backfill chunk kept failing without narrowing to a key; paused the definition \
             (resume it once the cause is fixed)"
        ),
        ChunkFailure::HandedBack { attempts } => tracing::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt = *attempts,
            max_charged = MAX_CHARGED_ATTEMPTS, error = %error,
            "direct build failed; handing it back to the backfill discharge, which retries it \
             after a backoff"
        ),
    }
}

/// Records a failure on chunk `id`'s row and frees its claim: `attempts`
/// failures so far, `charged` of them charged, `error`, and claimable again
/// after `delay`.
async fn record_failure(
    txn: &tokio_postgres::Transaction<'_>,
    id: i64,
    attempts: i32,
    charged: i32,
    error: &str,
    delay: Duration,
) -> Result<(), tokio_postgres::Error> {
    txn.execute(
        "update backfill_chunks set claimed_by = null, claimed_at = null, \
             attempts = $2, charged = $3, last_error = $4, \
             next_attempt_at = now() + make_interval(secs => $5) \
         where id = $1",
        &[&id, &attempts, &charged, &error, &delay.as_secs_f64()],
    )
    .await?;
    Ok(())
}

/// Pauses definition `id` because its build failed in a way no retry or
/// narrowing gets past, in the caller's transaction, which holds its row. A
/// definition already frozen keeps its status.
async fn pause_for_build_failure(
    txn: &tokio_postgres::Transaction<'_>,
    id: i64,
) -> Result<(), tokio_postgres::Error> {
    let paused = txn
        .execute(
            "update transform_definitions set status = $2 where id = $1 and status = any($3)",
            &[
                &id,
                &TransformStatus::Paused.as_str(),
                &TransformStatus::dispatchable(),
            ],
        )
        .await?;
    if paused == 1 {
        tracing::warn!(
            definition_id = id,
            to = %TransformStatus::Paused.as_str(),
            "transform status transition: its build kept failing; paused"
        );
    }
    Ok(())
}

/// [`fail_chunk`] for a [`ChunkWork::Range`] chunk, and for a Re-derive
/// build's chunk or plan job. Returns what it did and the chunk's failure
/// count, this one included.
///
/// `narrow` is the range a data failure is narrowed within, or `None` when
/// the chunk isn't narrowed: its data failures are charged like any other.
/// `retry_cap` caps the backoff.
async fn fail_range_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    narrow: Option<(Option<&str>, &str)>,
    claimed_by: &str,
    kind: FailureKind,
    error: &str,
    retry_cap: Duration,
) -> Result<(ChunkFailure, i32), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // The definition row first, as `finish_chunk` locks it, then the chunk:
    // the order resume takes them in (issue #434).
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&chunk.definition_id],
    )
    .await?;
    let Some(row) = txn
        .query_opt(
            &format!(
                "select {STALE}, bc.attempts + 1, bc.charged, d.source_table, d.target_table \
                 from backfill_chunks bc \
                 join transform_definitions d on d.id = bc.definition_id \
                 where bc.id = $1 and bc.claimed_by = $2 and not bc.done \
                 for update of bc"
            ),
            &[&chunk.id, &claimed_by],
        )
        .await?
    else {
        return Ok((ChunkFailure::NotHeld, 0));
    };
    let attempts: i32 = row.get(1);
    if row.get::<_, bool>(0) {
        discard_resumed_chunks(&txn, &[chunk.id]).await?;
        txn.commit().await?;
        return Ok((ChunkFailure::Discarded, attempts));
    }
    let charged: i32 = row.get(2);
    let source_table: String = row.get(3);
    let target_table: String = row.get(4);

    let narrowed = match (kind, narrow) {
        (FailureKind::Data, Some((lo, hi))) => {
            Some(backfill::narrow_one_to_one_chunk(&*txn, &source_table, lo, hi).await?)
        }
        _ => None,
    };
    let outcome = match narrowed {
        Some(backfill::ChunkNarrowing::Split { mid }) => {
            // This row keeps the lower half, and a new one takes the upper
            // half, of the same kind (a 1-1 range or a Re-derive chunk). Both
            // carry the failure so far, so `status` keeps reporting it while
            // the build narrows, and the build's `fuse_rearmed_at`, so a
            // resume supersedes both alike.
            txn.execute(
                "insert into backfill_chunks \
                     (definition_id, kind, lo, hi, fuse_rearmed_at, attempts, charged, \
                      last_error, next_attempt_at) \
                 select definition_id, kind, $2, hi, fuse_rearmed_at, $3, charged, $4, now() \
                 from backfill_chunks where id = $1",
                &[&chunk.id, &mid, &attempts, &error],
            )
            .await?;
            txn.execute(
                "update backfill_chunks set hi = $2 where id = $1",
                &[&chunk.id, &mid],
            )
            .await?;
            record_failure(&txn, chunk.id, attempts, charged, error, Duration::ZERO).await?;
            ChunkFailure::Split { mid }
        }
        Some(backfill::ChunkNarrowing::Key(key)) => {
            crate::staging::quarantine::evict_build_key(&txn, &source_table, &key, error)
                .await
                .map_err(|err| ChunkQueueError::Quarantine(Box::new(err)))?;
            record_failure(&txn, chunk.id, attempts, charged, error, Duration::ZERO).await?;
            ChunkFailure::Quarantined {
                key,
                fuse_tripped: false,
            }
        }
        None if kind == FailureKind::Transient => {
            let delay = retry_delay(attempts, retry_cap);
            record_failure(&txn, chunk.id, attempts, charged, error, delay).await?;
            ChunkFailure::Retrying {
                charged: false,
                delay,
            }
        }
        // Unnarrowable, or a data failure in a range with no key left to
        // blame (they were deleted or quarantined since the chunk ran).
        Some(backfill::ChunkNarrowing::Empty) | None => {
            let charged = charged + 1;
            if charged >= MAX_CHARGED_ATTEMPTS {
                record_failure(&txn, chunk.id, attempts, charged, error, Duration::ZERO).await?;
                pause_for_build_failure(&txn, chunk.definition_id).await?;
                ChunkFailure::Paused
            } else {
                let delay = retry_delay(attempts, retry_cap);
                record_failure(&txn, chunk.id, attempts, charged, error, delay).await?;
                ChunkFailure::Retrying {
                    charged: true,
                    delay,
                }
            }
        }
    };
    txn.commit().await?;

    let outcome = match outcome {
        ChunkFailure::Quarantined { key, .. } => {
            let (_, target) = target_table
                .split_once('.')
                .expect("target_table is always schema-qualified (issue #73)");
            let fuse_tripped = crate::staging::quarantine::trip_build_fuse(
                pool,
                chunk.definition_id,
                target,
                &source_table,
            )
            .await
            .map_err(|err| ChunkQueueError::Quarantine(Box::new(err)))?;
            ChunkFailure::Quarantined { key, fuse_tripped }
        }
        outcome => outcome,
    };
    Ok((outcome, attempts))
}

/// [`fail_chunk`] for a [`ChunkWork::DirectBuild`] job. Returns what it did
/// and the build's failure count.
async fn fail_direct_build(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    kind: FailureKind,
    error: &str,
) -> Result<(ChunkFailure, i32), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // The definition row first, as `finish_chunk` locks it, then the job.
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&chunk.definition_id],
    )
    .await?;
    let Some(row) = txn
        .query_opt(
            &format!(
                "select {STALE}, bc.prior_attempts, d.source_table, d.status \
                 from backfill_chunks bc \
                 join transform_definitions d on d.id = bc.definition_id \
                 where bc.id = $1 and bc.claimed_by = $2 and not bc.done \
                 for update of bc"
            ),
            &[&chunk.id, &claimed_by],
        )
        .await?
    else {
        return Ok((ChunkFailure::NotHeld, 0));
    };
    let prior_attempts: i32 = row.get(1);
    // A transient failure isn't charged: the marker retries it with the
    // backoff its earlier failures earned.
    let attempts = match kind {
        FailureKind::Transient => prior_attempts,
        FailureKind::Data | FailureKind::Unnarrowable => prior_attempts.saturating_add(1),
    };
    if row.get::<_, bool>(0) {
        discard_resumed_chunks(&txn, &[chunk.id]).await?;
        txn.commit().await?;
        return Ok((ChunkFailure::Discarded, attempts));
    }
    let status_text: String = row.get(3);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    if status != TransformStatus::Backfilling {
        txn.execute("delete from backfill_chunks where id = $1", &[&chunk.id])
            .await?;
        txn.commit().await?;
        return Ok((ChunkFailure::Discarded, attempts));
    }
    if kind != FailureKind::Transient && attempts >= MAX_CHARGED_ATTEMPTS {
        record_failure(&txn, chunk.id, attempts, attempts, error, Duration::ZERO).await?;
        pause_for_build_failure(&txn, chunk.definition_id).await?;
        txn.commit().await?;
        return Ok((ChunkFailure::Paused, attempts));
    }
    txn.execute("delete from backfill_chunks where id = $1", &[&chunk.id])
        .await?;
    txn.execute(
        "update transform_definitions set status = $1 where id = $2",
        &[
            &TransformStatus::WaitingToBackfill.as_str(),
            &chunk.definition_id,
        ],
    )
    .await?;
    let source_table: String = row.get(2);
    crate::intake::markers::park_failed_build(&*txn, &source_table, attempts, error).await?;
    tracing::info!(
        definition_id = chunk.definition_id,
        from = %TransformStatus::Backfilling.as_str(),
        to = %TransformStatus::WaitingToBackfill.as_str(),
        "transform status transition: direct build failed; handed back to the discharge"
    );
    txn.commit().await?;
    Ok((ChunkFailure::HandedBack { attempts }, attempts))
}

/// Gives up the claims `rows` (each `(chunk id, stale)`, locked by the
/// caller along with a share lock on each chunk's definition row) name.
///
/// A current chunk is freed, claimable again by the next [`claim_chunks`]. A
/// chunk held across its definition's resume ([`STALE`]) is deleted
/// instead (issue #360): resume discarded its unclaimed siblings and left
/// this one only so its worker could finish it (#332), and running it again
/// would redo work the resumed definition's rebuild already covers.
///
/// The share lock is what makes `stale` trustworthy: resume holds the
/// definition row `for update` while it deletes unclaimed chunks, so it runs
/// either wholly before this read (and `stale` sees it) or wholly after the
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

/// Deletes the chunks `ids`, each held across its definition's resume. It
/// leaves nothing to repair: the chunk's writes all committed before the
/// resume did, if at all ([`ClaimFence`]), and the resume's rebuild re-reads
/// the source. The caller holds a lock on each chunk row and on its
/// definition row.
async fn discard_resumed_chunks(
    txn: &tokio_postgres::Transaction<'_>,
    ids: &[i64],
) -> Result<(), ChunkQueueError> {
    if !ids.is_empty() {
        txn.execute("delete from backfill_chunks where id = any($1)", &[&ids])
            .await?;
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
/// `reclaim_ttl` is that fleet TTL: a worker that stalls inside one of the
/// chunk's fenced write transactions loses its session after that long idle
/// ([`ClaimFence`]).
///
/// The target schema comes from the definition's own persisted, qualified
/// `target_table` identity, never from the running worker's configuration
/// (issue #370): `install_definition` resolved it once, honoring an explicit
/// `TRANSFORM custom.t` spelling over `Config::target_schema`, and created
/// the table there. A chunk re-deriving it from whatever schema the worker
/// happens to be configured with would write into the wrong (usually
/// nonexistent) table. Same derivation `catalog::alter_transform` uses.
///
/// Every target write is fenced by the claim ([`ClaimFence`], issue #434).
/// A chunk whose claim no longer holds, because its definition was resumed
/// or the claim was reclaimed, stops before its next write and returns
/// `Ok`: the caller's [`finish_chunk`] then discards it, or leaves it to its
/// new holder.
pub async fn run_claimed_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    heartbeat_interval: Duration,
    reclaim_ttl: Duration,
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

    let fence = ClaimFence {
        chunk_id: chunk.id,
        claimed_by,
        idle_timeout: reclaim_ttl,
    };
    let ran = match &chunk.work {
        ChunkWork::Range { lo, hi } => backfill::execute_one_to_one_chunk(
            pool,
            &definition.def,
            target_schema,
            &definition.source_table,
            lo.as_deref(),
            hi,
            fence,
        )
        .await
        .map_err(ChunkQueueError::from),
        ChunkWork::DirectBuild => run_direct_build(pool, &definition, target_schema, fence).await,
        // A Re-derive build's work runs through `staging::build`, which
        // claims it itself; `claim_chunks` never hands it out.
        work @ (ChunkWork::Plan { .. } | ChunkWork::Rederive { .. } | ChunkWork::Sweep { .. }) => {
            Err(ChunkQueueError::UnknownKind {
                kind: work.kind().to_string(),
            })
        }
    };
    match ran {
        // The fence stopped the write (issue #434). The caller's
        // `finish_chunk` then discards a chunk held across a resume, and
        // leaves one reclaimed from this worker to its new holder.
        Err(ChunkQueueError::Backfill(BackfillError::Superseded)) => {
            tracing::info!(
                chunk_id = chunk.id,
                definition_id = chunk.definition_id,
                "backfill chunk superseded before its write; wrote nothing further"
            );
            Ok(())
        }
        ran => ran,
    }
}

/// Runs a [`ChunkWork::DirectBuild`] job: the whole direct set-based build
/// of `definition` (ADR-0007, [`backfill::backfill_definition`]). An
/// aggregate's target then gets its extinct horizon raised (see
/// [`raise_extinct_horizon_after_build`]).
///
/// Moving the definition on to `catching_up` is [`finish_chunk`]'s job, as
/// for every chunk.
async fn run_direct_build(
    pool: &Pool,
    definition: &super::model::Definition,
    target_schema: &str,
    fence: ClaimFence<'_>,
) -> Result<(), ChunkQueueError> {
    backfill::backfill_definition_fenced(
        pool,
        &definition.def,
        target_schema,
        &definition.source_table,
        &definition.source_columns,
        Some(fence),
    )
    .await?;

    if matches!(
        definition.def.key_space,
        super::ast::KeySpace::Aggregate { .. }
    ) {
        let client = pool.get().await?;
        raise_extinct_horizon_after_build(&**client, &definition.target_table).await?;
    }
    Ok(())
}

/// Raises an aggregate target's extinct horizon (issue #321,
/// `aggregate_extinct_horizon`) to the WAL insert position after its direct
/// build read the source. The build is a live `GROUP BY` read, and it writes
/// no row for a group it found empty, so a delta for such a group that drains
/// after the definition goes live (the build runs after its source's capture
/// was installed, so a commit it read is captured too) is judged against this
/// value, the same way a delta on a group a forced recompute found empty is.
/// Raising a horizon is always safe: the worst it does is send a delta to
/// the re-deriving path.
async fn raise_extinct_horizon_after_build(
    client: &impl GenericClient,
    target_table: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "insert into aggregate_extinct_horizon (target_table, lsn) \
         values ($1, pg_current_wal_insert_lsn()) \
         on conflict (target_table) do update \
         set lsn = greatest(aggregate_extinct_horizon.lsn, excluded.lsn)",
            &[&target_table],
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
pub(crate) struct ChunkHeartbeat {
    task: tokio::task::JoinHandle<()>,
}

impl ChunkHeartbeat {
    pub(crate) fn spawn(pool: Pool, id: i64, claimed_by: String, interval: Duration) -> Self {
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
/// #397).** The resumed definition goes live through its rebuild, which the
/// resume's own discharge dispatches (fresh chunks, or a ring read). Completing
/// it from a chunk of the superseded build would flip it `live` early. So a
/// [`STALE`] chunk is discarded instead (see [`free_or_discard_claims`]),
/// with nothing to repair ([`ClaimFence`]). [`STALE`] is read
/// under this function's `for update` lock on the definition row, which
/// resume and the discharge's dispatch take too.
pub async fn finish_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
) -> Result<(), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&chunk.definition_id],
    )
    .await?;
    let stale = txn
        .query_opt(
            &format!(
                "select {STALE} from backfill_chunks bc \
                 join transform_definitions d on d.id = bc.definition_id \
                 where bc.id = $1"
            ),
            &[&chunk.id],
        )
        .await?
        .is_some_and(|row| row.get::<_, bool>(0));
    if stale {
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

/// Gives up `chunk` after its fence stopped a Re-derive build's write
/// (`staging::build`, #625 F2): deletes it if it was held across its
/// definition's resume ([`STALE`]), as [`finish_chunk`] does, and otherwise
/// leaves it alone (its claim was reclaimed, or it is done).
pub(crate) async fn discard_if_superseded(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
) -> Result<(), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // The definition row first, then the chunk: resume's order (#434).
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&chunk.definition_id],
    )
    .await?;
    let rows = txn
        .query(
            &format!(
                "select bc.id, {STALE} from backfill_chunks bc \
                 join transform_definitions d on d.id = bc.definition_id \
                 where bc.id = $1 and bc.claimed_by = $2 and not bc.done \
                 for update of bc"
            ),
            &[&chunk.id, &claimed_by],
        )
        .await?;
    let stale: Vec<i64> = rows
        .iter()
        .filter(|row| row.get::<_, bool>(1))
        .map(|row| row.get(0))
        .collect();
    discard_resumed_chunks(&txn, &stale).await?;
    txn.commit().await?;
    Ok(())
}

/// The shared "no chunks left? then flip to live" check both
/// [`finish_chunk`] and [`dispatch_one_to_one`]'s zero-chunk case run once they
/// already hold the definition row's `for update` lock. Returns the status
/// the completion left the definition in, or `None` while chunks remain —
/// "flip to live" only ever happens from `backfilling` (issue #331; see
/// `complete_direct_backfill`).
///
/// Only the current build's chunks count: a [`STALE`] chunk a worker still
/// holds from before a resume never completes, and is discarded however its
/// worker gives it up, so waiting on it would strand the rebuild.
async fn complete_if_no_chunks_remain_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
) -> Result<Option<TransformStatus>, CatalogError> {
    let remaining: bool = txn
        .query_one(
            &format!(
                "select exists( \
                     select 1 from backfill_chunks bc \
                     join transform_definitions d on d.id = bc.definition_id \
                     where bc.definition_id = $1 and not bc.done and not ({STALE}) \
                 )"
            ),
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
    use crate::defs::ast::TransformDef;
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

    /// Seeds `public.<source>` with `rows` rows and a `waiting_to_backfill`
    /// definition `<target>` over it: the state registration leaves behind
    /// for the discharge to dispatch.
    async fn seed_waiting(
        raw: &tokio_postgres::Client,
        source: &str,
        target: &str,
        rows: i64,
    ) -> (i64, TransformDef) {
        let text = format!("TRANSFORM {target} FROM {source} SELECT a + a AS x");
        raw.batch_execute(&format!(
            "create table public.{source} (id bigint primary key, a numeric); \
             insert into public.{source} select g, g from generate_series(1, {rows}) g; \
             create table public.{target} (id bigint primary key, x numeric); \
             insert into source_table_versions (source_table, version) \
             values ('public.{source}', 1)"
        ))
        .await
        .expect("seed source");
        let id: i64 = raw
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ($1, $2, 1, $3, 'waiting_to_backfill') returning id",
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

    /// Plans `def`'s chunks and dispatches them in one transaction, as the
    /// backfill discharge does.
    async fn dispatch(
        pool: &Pool,
        id: i64,
        def: &TransformDef,
        source: &str,
    ) -> Option<TransformStatus> {
        let mut client = pool.get().await.expect("connection");
        let ranges = backfill::plan_one_to_one_chunks(&**client, def, source)
            .await
            .expect("plan chunks");
        let txn = client.transaction().await.expect("begin");
        let status = dispatch_one_to_one(&txn, id, &ranges)
            .await
            .expect("dispatch");
        txn.commit().await.expect("commit");
        status
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
            "select count(*) from backfill_chunks where definition_id = $1 and not done",
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

    /// Issue #418: the dispatch moves the definition to `backfilling` in the
    /// same transaction as the chunks that drive it (#404).
    #[tokio::test]
    async fn dispatch_commits_backfilling_with_its_chunks() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;

        let status = dispatch(&pool, id, &def, "public.orders").await;
        assert_eq!(status, Some(TransformStatus::Backfilling));
        assert_eq!(status_of(&raw, id).await, "backfilling");
        assert_eq!(chunk_count(&raw, id).await, 1);
    }

    /// An empty source plans zero chunks, so nothing would ever finish a last
    /// chunk: the dispatch completes the definition itself, parking its
    /// go-live catch-up, whose discharge takes it `live` (issue #476).
    #[tokio::test]
    async fn dispatching_an_empty_source_completes_its_build() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 0).await;

        let status = dispatch(&pool, id, &def, "public.orders").await;
        assert_eq!(status, Some(TransformStatus::CatchingUp));
        assert_eq!(status_of(&raw, id).await, "catching_up");
        assert!(
            marker_generation(&raw, "public.orders").await.is_some(),
            "the go-live catch-up is parked"
        );
    }

    /// Issue #331: an operator pause that lands after the discharge read the
    /// definition stands. The dispatch enqueues nothing for it; its resume
    /// parks a marker of its own.
    #[tokio::test]
    async fn a_definition_paused_before_its_dispatch_is_left_paused() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        pause_transform(&pool, "order_doubles")
            .await
            .expect("pause");

        let status = dispatch(&pool, id, &def, "public.orders").await;
        assert_eq!(status, None);
        assert_eq!(status_of(&raw, id).await, "paused");
        assert_eq!(chunk_count(&raw, id).await, 0, "nothing is enqueued");
    }

    /// Dispatches `id`'s chunks, claims its one chunk for [`WORKER`], then
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
        dispatch(pool, id, def, source).await;
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
    /// then released is discarded, not handed out again. Nothing is parked
    /// for it: any part of its range its failed write committed landed before
    /// the resume (issue #434), and the resume's own marker rebuilds it.
    #[tokio::test]
    async fn releasing_a_chunk_held_across_a_resume_discards_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
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
        assert_eq!(
            marker_generation(&raw, "public.orders").await,
            parked,
            "the discard parks nothing"
        );
    }

    /// Issue #360, race 1 (worker died): the stale-claim sweep discards a
    /// chunk held across its definition's resume, and still frees a current
    /// sibling's stale claim for another worker.
    #[tokio::test]
    async fn reclaiming_a_chunk_held_across_a_resume_discards_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        let held =
            hold_a_chunk_across_a_resume(&pool, &raw, id, &def, "order_doubles", "public.orders")
                .await;
        let parked = marker_generation(&raw, "public.orders").await;

        let (sibling, sibling_def) = seed_waiting(&raw, "items", "item_doubles", 3).await;
        dispatch(&pool, sibling, &sibling_def, "public.items").await;
        let sibling_held = claim_chunks(&raw, WORKER, 1).await.expect("claim sibling");
        assert_eq!(sibling_held.len(), 1);
        assert_eq!(sibling_held[0].definition_id, sibling);

        // A zero TTL makes every claim taken in an earlier transaction stale.
        let reclaimed = reclaim_stale_chunks(&mut raw, Duration::ZERO)
            .await
            .expect("reclaim");
        assert_eq!(reclaimed, 2, "both stale claims are dealt with");

        assert_eq!(chunk_count(&raw, id).await, 0, "the chunk was discarded");
        assert_eq!(
            marker_generation(&raw, "public.orders").await,
            parked,
            "the discard parks nothing"
        );
        let reclaimable = claim_chunks(&raw, WORKER, 100).await.expect("claim");
        assert_eq!(
            reclaimable
                .iter()
                .map(|c| (c.id, c.definition_id))
                .collect::<Vec<_>>(),
            [(sibling_held[0].id, sibling)],
            "only the current sibling's chunk is claimable again, not {held:?}"
        );
    }

    /// Issues #397/#418: a resumed plain 1-1 definition's rebuild is chunked
    /// too. A chunk held across the resume that finishes while the rebuild's
    /// own chunks are outstanding must not complete the definition, and must
    /// not keep the rebuild from completing: the rebuild's last chunk
    /// completes it (`catching_up`, issue #476). The held chunk is retired,
    /// parking nothing.
    #[tokio::test]
    async fn a_chunk_held_across_a_resume_neither_completes_nor_blocks_the_rebuild() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        let held =
            hold_a_chunk_across_a_resume(&pool, &raw, id, &def, "order_doubles", "public.orders")
                .await;

        // The resume's discharge dispatches the rebuild's own chunks.
        assert_eq!(
            dispatch(&pool, id, &def, "public.orders").await,
            Some(TransformStatus::Backfilling)
        );
        let rebuild = claim_chunks(&raw, WORKER, 100)
            .await
            .expect("claim rebuild");
        assert_eq!(rebuild.len(), 1, "only the rebuild's chunk is claimable");
        assert_ne!(rebuild[0].id, held.id);

        let parked = marker_generation(&raw, "public.orders").await;
        finish_chunk(&pool, &held, WORKER)
            .await
            .expect("finish held");
        assert_eq!(
            status_of(&raw, id).await,
            "backfilling",
            "the held chunk doesn't complete the rebuild"
        );
        assert_eq!(
            marker_generation(&raw, "public.orders").await,
            parked,
            "the discard parks nothing"
        );

        run_claimed_chunk(
            &pool,
            &rebuild[0],
            WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect("run rebuild chunk");
        finish_chunk(&pool, &rebuild[0], WORKER)
            .await
            .expect("finish rebuild chunk");
        assert_eq!(status_of(&raw, id).await, "catching_up");
    }

    /// The rebuild's last chunk completes the definition even while a chunk
    /// from before the resume is still held: the held chunk is stale, so it
    /// doesn't count as outstanding work.
    #[tokio::test]
    async fn the_rebuild_completes_while_a_stale_chunk_is_still_held() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        let held =
            hold_a_chunk_across_a_resume(&pool, &raw, id, &def, "order_doubles", "public.orders")
                .await;
        dispatch(&pool, id, &def, "public.orders").await;
        let rebuild = claim_chunks(&raw, WORKER, 100)
            .await
            .expect("claim rebuild");
        assert_eq!(rebuild.len(), 1);

        run_claimed_chunk(
            &pool,
            &rebuild[0],
            WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect("run rebuild chunk");
        finish_chunk(&pool, &rebuild[0], WORKER)
            .await
            .expect("finish rebuild chunk");
        assert_eq!(status_of(&raw, id).await, "catching_up");

        release_chunk(&mut raw, held.id, WORKER)
            .await
            .expect("release held");
        assert_eq!(status_of(&raw, id).await, "catching_up");
        assert_eq!(
            chunk_count(&raw, id).await,
            0,
            "the held chunk is discarded"
        );
    }

    /// #616: a chunk of a composite-key source that fails on one row's data
    /// splits by key count down to that row's key, quarantines it under the
    /// key's ring encoding, and the build finishes without it. Driven one
    /// claim at a time.
    #[tokio::test]
    async fn a_composite_key_chunk_narrows_to_its_failing_key() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let text = "TRANSFORM pair_doubles FROM pairs SELECT x + x AS y";
        raw.batch_execute(&format!(
            "create table public.pairs (a int, b text, x int, primary key (a, b)); \
             insert into public.pairs values \
                 (1, 'a', 1), (1, 'b', {}), (2, 'a', 3), (2, 'b', 4), (3, 'a', 5); \
             create table public.pair_doubles (a int, b text, y int, primary key (a, b)); \
             insert into source_table_versions (source_table, version) \
             values ('public.pairs', 1)",
            i32::MAX
        ))
        .await
        .expect("seed source");
        let id: i64 = raw
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.pair_doubles', 'public.pairs', 1, $1, 'waiting_to_backfill') \
                 returning id",
                &[&text],
            )
            .await
            .expect("seed definition")
            .get(0);
        let def = parse(text).expect("parse definition");
        assert_eq!(
            dispatch(&pool, id, &def, "public.pairs").await,
            Some(TransformStatus::Backfilling)
        );

        let mut outcomes = Vec::new();
        for _ in 0..20 {
            let Some(chunk) = claim_chunks(&raw, WORKER, 1).await.expect("claim").pop() else {
                break;
            };
            match run_claimed_chunk(
                &pool,
                &chunk,
                WORKER,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            {
                Ok(()) => finish_chunk(&pool, &chunk, WORKER).await.expect("finish"),
                Err(err) => outcomes.push(
                    fail_chunk(&pool, &chunk, WORKER, &err)
                        .await
                        .expect("fail the chunk"),
                ),
            }
        }

        let key = ddl_key(&["1", "b"]);
        assert!(
            matches!(outcomes.first(), Some(ChunkFailure::Split { .. })),
            "{outcomes:?}"
        );
        assert_eq!(
            outcomes.last(),
            Some(&ChunkFailure::Quarantined {
                key: key.clone(),
                fuse_tripped: false
            }),
            "{outcomes:?}"
        );
        assert_eq!(status_of(&raw, id).await, "catching_up");
        let poisoned: Vec<String> = raw
            .query(
                "select key from poison where src_table = 'public.pairs'",
                &[],
            )
            .await
            .expect("read poison")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(poisoned, vec![key]);
        let built: Vec<(i32, String, i32)> = raw
            .query("select a, b, y from public.pair_doubles order by a, b", &[])
            .await
            .expect("read the target")
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect();
        assert_eq!(
            built,
            vec![
                (1, "a".to_string(), 2),
                (2, "a".to_string(), 6),
                (2, "b".to_string(), 8),
                (3, "a".to_string(), 10)
            ]
        );
    }

    /// A source keyed by a nullable `UNIQUE NULLS NOT DISTINCT` index (an
    /// aggregate target's grouping columns, issue #128) can hold a row with a
    /// `NULL` key part that a range's row comparison still admits, because an
    /// earlier part decides it: `(2, NULL) <= (3, 'a')`. No target row can
    /// represent that key, so the build leaves it out, as the drain does. It
    /// must not fail the chunk: the narrowing never counts such a row, so it
    /// would quarantine the innocent key beside it and then pause the
    /// definition.
    #[tokio::test]
    async fn a_chunk_leaves_out_a_row_with_a_null_key_part() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let text = "TRANSFORM pair_doubles FROM pairs SELECT x + x AS y";
        raw.batch_execute(
            "create table public.pairs (a int, b text, x int, unique nulls not distinct (a, b)); \
             insert into public.pairs values (1, 'a', 1), (2, null, 2), (3, 'a', 3); \
             create table public.pair_doubles (a int, b text, y int, primary key (a, b)); \
             insert into source_table_versions (source_table, version) \
             values ('public.pairs', 1)",
        )
        .await
        .expect("seed source");
        let id: i64 = raw
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.pair_doubles', 'public.pairs', 1, $1, 'waiting_to_backfill') \
                 returning id",
                &[&text],
            )
            .await
            .expect("seed definition")
            .get(0);
        let def = parse(text).expect("parse definition");
        assert_eq!(
            dispatch(&pool, id, &def, "public.pairs").await,
            Some(TransformStatus::Backfilling)
        );

        let mut outcomes = Vec::new();
        for _ in 0..20 {
            let Some(chunk) = claim_chunks(&raw, WORKER, 1).await.expect("claim").pop() else {
                break;
            };
            match run_claimed_chunk(
                &pool,
                &chunk,
                WORKER,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            {
                Ok(()) => finish_chunk(&pool, &chunk, WORKER).await.expect("finish"),
                Err(err) => outcomes.push(
                    fail_chunk(&pool, &chunk, WORKER, &err)
                        .await
                        .expect("fail the chunk"),
                ),
            }
        }

        assert_eq!(outcomes, vec![], "no chunk failed");
        assert_eq!(status_of(&raw, id).await, "catching_up");
        let poisoned: i64 = raw
            .query_one("select count(*) from poison", &[])
            .await
            .expect("read poison")
            .get(0);
        assert_eq!(poisoned, 0);
        let built: Vec<(i32, String, i32)> = raw
            .query("select a, b, y from public.pair_doubles order by a, b", &[])
            .await
            .expect("read the target")
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect();
        assert_eq!(
            built,
            vec![(1, "a".to_string(), 2), (3, "a".to_string(), 6)]
        );
    }

    /// A composite key's ring encoding of `parts`.
    fn ddl_key(parts: &[&str]) -> String {
        super::super::ddl::join_pk_key(parts)
    }

    /// Seeds `public.orders` (three rows over two groups) and a
    /// `waiting_to_backfill` aggregate definition `rollup` over it, creating
    /// its target table unless `with_target` is `false`.
    async fn seed_waiting_aggregate(
        pool: &Pool,
        raw: &tokio_postgres::Client,
        with_target: bool,
    ) -> i64 {
        let text = "TRANSFORM rollup FROM orders GROUP BY g SELECT g AS g, sum(a) AS total";
        raw.batch_execute(
            "create table public.orders (id bigint primary key, g numeric, a numeric); \
             insert into public.orders values (1, 1, 10), (2, 1, 20), (3, 2, 5); \
             insert into source_table_versions (source_table, version) \
             values ('public.orders', 1)",
        )
        .await
        .expect("seed source");
        if with_target {
            let columns: std::collections::HashMap<String, super::super::ast::ValueType> =
                ["id", "g", "a"]
                    .into_iter()
                    .map(|c| (c.to_string(), super::super::ast::ValueType::Numeric))
                    .collect();
            super::super::ddl::create_aggregate_target_table(
                pool,
                &parse(text).expect("parse"),
                "public",
                &columns,
            )
            .await
            .expect("create the target");
        }
        raw.query_one(
            "insert into transform_definitions \
             (target_table, source_table, source_version, definition_text, status, \
              source_columns) \
             values ('public.rollup', 'public.orders', 1, $1, 'waiting_to_backfill', \
                     '{\"id\": \"numeric\", \"g\": \"numeric\", \"a\": \"numeric\"}') \
             returning id",
            &[&text],
        )
        .await
        .expect("seed definition")
        .get(0)
    }

    /// Dispatches `id`'s direct-build job as the backfill discharge does.
    async fn dispatch_job(pool: &Pool, id: i64, prior_attempts: i32) {
        let mut client = pool.get().await.expect("connection");
        let txn = client.transaction().await.expect("begin");
        let status = dispatch_direct_build(&txn, id, prior_attempts)
            .await
            .expect("dispatch");
        txn.commit().await.expect("commit");
        assert_eq!(status, Some(TransformStatus::Backfilling));
    }

    /// Issue #419: a direct-build job whose worker dies is reclaimed and
    /// rerun by another worker, and finishing it moves the definition to
    /// `catching_up` with its go-live catch-up parked. The build records its read as the recompute
    /// horizon of every group row it writes and of the target itself, so a
    /// streamed delta for a commit it read re-derives its group rather than
    /// counting the commit twice.
    #[tokio::test]
    async fn a_direct_build_job_held_by_a_dead_worker_is_rerun_by_another() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let id = seed_waiting_aggregate(&pool, &raw, true).await;
        dispatch_job(&pool, id, 0).await;

        let dead = claim_chunks(&raw, "dead-worker", 10).await.expect("claim");
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].work, ChunkWork::DirectBuild);
        let reclaimed = reclaim_stale_chunks(&mut raw, Duration::ZERO)
            .await
            .expect("reclaim");
        assert_eq!(reclaimed, 1);

        let job = claim_chunks(&raw, WORKER, 10).await.expect("claim again");
        assert_eq!(job.len(), 1);
        run_claimed_chunk(
            &pool,
            &job[0],
            WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect("run the job");
        finish_chunk(&pool, &job[0], WORKER)
            .await
            .expect("finish the job");
        // A finish from the dead worker's claim is a no-op.
        finish_chunk(&pool, &dead[0], "dead-worker")
            .await
            .expect("finish a lost claim");

        assert_eq!(status_of(&raw, id).await, "catching_up");
        assert_eq!(chunk_count(&raw, id).await, 0);
        assert!(marker_generation(&raw, "public.orders").await.is_some());
        let rows: Vec<(String, String, bool)> = raw
            .query(
                "select g::text, total::text, __trellis_recompute_lsn is not null \
                 from public.rollup order by g",
                &[],
            )
            .await
            .expect("read the target")
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect();
        assert_eq!(
            rows,
            [
                ("1".to_string(), "30".to_string(), true),
                ("2".to_string(), "5".to_string(), true)
            ]
        );
        let extinct: i64 = raw
            .query_one(
                "select count(*) from aggregate_extinct_horizon where target_table = 'public.rollup'",
                &[],
            )
            .await
            .expect("read the extinct horizon")
            .get(0);
        assert_eq!(extinct, 1);
    }

    /// Issue #419: a direct build that fails hands its build back to the
    /// discharge in one transaction: the job is gone, the definition is
    /// `waiting_to_backfill` again, and its source's marker carries the error
    /// and a backoff, one attempt past the marker that dispatched the job.
    #[tokio::test]
    async fn a_failed_direct_build_is_handed_back_to_the_discharge() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        // No target table, so the build's write fails.
        let id = seed_waiting_aggregate(&pool, &raw, false).await;
        dispatch_job(&pool, id, 2).await;

        let job = claim_chunks(&raw, WORKER, 10).await.expect("claim");
        let error = run_claimed_chunk(
            &pool,
            &job[0],
            WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect_err("the build has no target to write");
        let given_up = fail_chunk(&pool, &job[0], WORKER, &error)
            .await
            .expect("fail the job");
        assert_eq!(given_up, ChunkFailure::HandedBack { attempts: 3 });

        assert_eq!(status_of(&raw, id).await, "waiting_to_backfill");
        let jobs: i64 = raw
            .query_one(
                "select count(*) from backfill_chunks where definition_id = $1",
                &[&id],
            )
            .await
            .expect("count jobs")
            .get(0);
        assert_eq!(jobs, 0);
        let marker = raw
            .query_one(
                "select attempts, last_error, next_attempt_at > now() \
                 from pending_backfill where table_name = 'public.orders'",
                &[],
            )
            .await
            .expect("the marker is parked");
        assert_eq!(marker.get::<_, i32>(0), 3);
        assert_eq!(marker.get::<_, Option<String>>(1), Some(error.to_string()));
        assert!(marker.get::<_, bool>(2), "the retry is backed off");
    }

    /// A job that fails after an operator paused its definition only loses
    /// the job: the definition stays paused, with no marker, and its resume
    /// parks one and rebuilds.
    #[tokio::test]
    async fn a_failed_direct_build_of_a_paused_definition_leaves_it_paused() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let id = seed_waiting_aggregate(&pool, &raw, false).await;
        dispatch_job(&pool, id, 0).await;
        let job = claim_chunks(&raw, WORKER, 10).await.expect("claim");
        pause_transform(&pool, "rollup").await.expect("pause");

        let boom = ChunkQueueError::Backfill(BackfillError::Unsupported("boom".to_string()));
        let given_up = fail_chunk(&pool, &job[0], WORKER, &boom)
            .await
            .expect("fail the job");

        assert_eq!(given_up, ChunkFailure::Discarded);
        assert_eq!(status_of(&raw, id).await, "paused");
        assert_eq!(chunk_count(&raw, id).await, 0);
        assert_eq!(marker_generation(&raw, "public.orders").await, None);
    }

    /// #616: a direct build can't be narrowed to a key, so the
    /// [`MAX_CHARGED_ATTEMPTS`]th failure in a row pauses its definition
    /// rather than handing it back again. The job stays as the record of the
    /// failure, and no marker is parked: the resume parks one and rebuilds.
    #[tokio::test]
    async fn a_direct_build_that_keeps_failing_pauses_its_definition() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        // No target table, so the build's write fails.
        let id = seed_waiting_aggregate(&pool, &raw, false).await;
        dispatch_job(&pool, id, MAX_CHARGED_ATTEMPTS - 1).await;

        let job = claim_chunks(&raw, WORKER, 10).await.expect("claim");
        let error = run_claimed_chunk(
            &pool,
            &job[0],
            WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect_err("the build has no target to write");
        let given_up = fail_chunk(&pool, &job[0], WORKER, &error)
            .await
            .expect("fail the job");

        assert_eq!(given_up, ChunkFailure::Paused);
        assert_eq!(status_of(&raw, id).await, "paused");
        assert_eq!(marker_generation(&raw, "public.orders").await, None);
        let record = raw
            .query_one(
                "select attempts, last_error, claimed_by from backfill_chunks \
                 where definition_id = $1",
                &[&id],
            )
            .await
            .expect("the job stays as the record");
        assert_eq!(record.get::<_, i32>(0), MAX_CHARGED_ATTEMPTS);
        assert_eq!(record.get::<_, Option<String>>(1), Some(error.to_string()));
        assert_eq!(record.get::<_, Option<String>>(2), None);

        resume_transform(&pool, "rollup").await.expect("resume");
        assert_eq!(
            chunk_count(&raw, id).await,
            0,
            "the resume clears the record"
        );
    }

    /// Issue #434: a chunk held across a pause and a completed resume must
    /// not write the target once the rebuild has finished. By then the
    /// target is applying (`catching_up`, then `live`), so readers can attach
    /// to it, here a relationship declared on it in that gap, and they only
    /// learn of its changes through the target-mutation seam, which a
    /// chunk's write bypasses.
    ///
    /// The source row changed after the rebuild read it, and its streamed
    /// delta hasn't drained yet. Had the held chunk written the new value,
    /// that delta's apply would find the target already current, write
    /// nothing and stage nothing for the relationship's consumers, and so
    /// would the discharge of the source's catch-up: they would keep the old
    /// value for good.
    #[tokio::test]
    async fn a_chunk_held_across_a_completed_resume_writes_nothing() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        let held =
            hold_a_chunk_across_a_resume(&pool, &raw, id, &def, "order_doubles", "public.orders")
                .await;

        // The resume's rebuild runs to completion around the held chunk.
        dispatch(&pool, id, &def, "public.orders").await;
        let rebuild = claim_chunks(&raw, "rebuild-worker", 100)
            .await
            .expect("claim rebuild");
        assert_eq!(rebuild.len(), 1);
        run_claimed_chunk(
            &pool,
            &rebuild[0],
            "rebuild-worker",
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect("run rebuild chunk");
        finish_chunk(&pool, &rebuild[0], "rebuild-worker")
            .await
            .expect("finish rebuild chunk");
        assert_eq!(status_of(&raw, id).await, "catching_up");

        // A relationship on the rebuilt target, declared in the gap: the
        // target is applying, so `create_relationship` accepts it.
        raw.batch_execute(
            "create table public.reports (id bigint primary key, oid bigint); \
             insert into public.reports values (1, 1)",
        )
        .await
        .expect("seed the relationship's from-side");
        catalog::create_relationship(
            &pool,
            "RELATIONSHIP rollup FROM reports.oid TO order_doubles.id",
        )
        .await
        .expect("declare a relationship on the rebuilt target");

        // A source change whose streamed delta hasn't drained yet.
        raw.execute("update public.orders set a = 100 where id = 1", &[])
            .await
            .expect("update the source");

        run_claimed_chunk(
            &pool,
            &held,
            WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect("run the held chunk");
        finish_chunk(&pool, &held, WORKER)
            .await
            .expect("finish the held chunk");

        let x: i64 = raw
            .query_one(
                "select x::bigint from public.order_doubles where id = 1",
                &[],
            )
            .await
            .expect("read the target")
            .get(0);
        assert_eq!(
            x, 2,
            "the held chunk wrote the applying target outside the seam; only the \
             delta's apply may move it, so its readers hear of the change"
        );
        assert_eq!(
            chunk_count(&raw, id).await,
            0,
            "the held chunk is discarded"
        );
    }

    /// Issue #434, for a direct-build job: a job held across a pause and a
    /// resume writes nothing when it runs, and is discarded.
    #[tokio::test]
    async fn a_direct_build_job_held_across_a_resume_writes_nothing() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let id = seed_waiting_aggregate(&pool, &raw, true).await;
        dispatch_job(&pool, id, 0).await;
        let held = claim_chunks(&raw, WORKER, 10).await.expect("claim");
        assert_eq!(held.len(), 1);
        pause_transform(&pool, "rollup").await.expect("pause");
        resume_transform(&pool, "rollup").await.expect("resume");

        run_claimed_chunk(
            &pool,
            &held[0],
            WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect("a superseded job is not a failure");
        let rows: i64 = raw
            .query_one("select count(*) from public.rollup", &[])
            .await
            .expect("count target rows")
            .get(0);
        assert_eq!(rows, 0, "the held job wrote nothing");

        finish_chunk(&pool, &held[0], WORKER)
            .await
            .expect("finish the held job");
        assert_eq!(chunk_count(&raw, id).await, 0, "the held job is discarded");
        assert_eq!(status_of(&raw, id).await, "waiting_to_backfill");
    }

    /// The fence also covers a claim reclaimed from a worker that is still
    /// running (its heartbeat stalled): that worker's late write lands
    /// nowhere, whoever holds the chunk now.
    #[tokio::test]
    async fn a_worker_whose_claim_was_reclaimed_writes_nothing() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        dispatch(&pool, id, &def, "public.orders").await;
        let stalled = claim_chunks(&raw, "stalled-worker", 1)
            .await
            .expect("claim");
        assert_eq!(stalled.len(), 1);
        reclaim_stale_chunks(&mut raw, Duration::ZERO)
            .await
            .expect("reclaim");
        let reclaimed = claim_chunks(&raw, WORKER, 1).await.expect("reclaim claim");
        assert_eq!(reclaimed.len(), 1);

        run_claimed_chunk(
            &pool,
            &stalled[0],
            "stalled-worker",
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect("a superseded chunk is not a failure");
        let rows: i64 = raw
            .query_one("select count(*) from public.order_doubles", &[])
            .await
            .expect("count target rows")
            .get(0);
        assert_eq!(rows, 0, "the stalled worker wrote nothing");
    }

    /// Issue #434: resume waits out a chunk write still in flight, so the
    /// write commits before the resume does or not at all. The in-flight
    /// write is stood in for by a transaction holding the chunk's fence, and
    /// the resume runs under a short `lock_timeout` so its wait shows up as an
    /// error instead of a hang.
    #[tokio::test]
    async fn a_resume_waits_out_a_chunk_write_in_flight() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        dispatch(&pool, id, &def, "public.orders").await;
        let held = claim_chunks(&raw, WORKER, 1).await.expect("claim");
        assert_eq!(held.len(), 1);
        pause_transform(&pool, "order_doubles")
            .await
            .expect("pause");
        let fence = ClaimFence {
            chunk_id: held[0].id,
            claimed_by: WORKER,
            idle_timeout: Duration::from_secs(60),
        };

        let impatient = Pool::new(
            &crate::config::Config::from_dsn(format!("{} options='-c lock_timeout=100'", db.dsn()))
                .expect("valid dsn"),
        )
        .expect("pool");
        let writing = raw.transaction().await.expect("begin");
        assert!(fence.hold(&writing).await.expect("hold"), "the claim holds");
        let err = resume_transform(&impatient, "order_doubles")
            .await
            .expect_err("the resume waits for the write in flight");
        assert!(
            err.to_string().contains("lock timeout"),
            "the resume timed out waiting for the chunk's lock, got: {err}"
        );
        writing.commit().await.expect("the write commits");
        assert_eq!(status_of(&raw, id).await, "paused");

        resume_transform(&impatient, "order_doubles")
            .await
            .expect("resume once the write is done");
        let writing = raw.transaction().await.expect("begin");
        assert!(
            !fence.hold(&writing).await.expect("hold"),
            "a write after the resume is fenced out"
        );
    }

    /// Issue #434: a write that reaches its fence while a resume holds the
    /// chunk's row waits for the resume to commit, and then sees it. The
    /// staleness check is a statement of its own for exactly this: in the
    /// locking statement, the lock wait would re-check only the chunk row,
    /// against a definition row read before the resume committed.
    ///
    /// The real resume is held after it has locked the chunk (at its marker
    /// park, behind a table lock the test holds), so the fence arrives while
    /// the chunk's row is locked, not before or after.
    #[tokio::test]
    async fn a_fence_that_waited_out_a_resume_sees_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        dispatch(&pool, id, &def, "public.orders").await;
        let held = claim_chunks(&raw, WORKER, 1).await.expect("claim");
        assert_eq!(held.len(), 1);
        pause_transform(&pool, "order_doubles")
            .await
            .expect("pause");

        let mut blocker = pool.get().await.expect("blocker connection");
        let blocking = blocker.transaction().await.expect("begin");
        blocking
            .batch_execute("lock table pending_backfill in exclusive mode")
            .await
            .expect("hold the resume at its marker park");
        let resuming = tokio::spawn({
            let pool = pool.clone();
            async move { resume_transform(&pool, "order_doubles").await }
        });
        let wait_for_lock_waiters = async |n: i64| {
            for _ in 0..500 {
                let waiting: i64 = raw
                    .query_one(
                        "select count(*) from pg_stat_activity \
                         where datname = current_database() and wait_event_type = 'Lock'",
                        &[],
                    )
                    .await
                    .expect("read pg_stat_activity")
                    .get(0);
                if waiting == n {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("never saw {n} lock waiters");
        };
        wait_for_lock_waiters(1).await;
        let fenced = tokio::spawn({
            let pool = pool.clone();
            let chunk_id = held[0].id;
            async move {
                let mut client = pool.get().await.expect("connection");
                let txn = client.transaction().await.expect("begin");
                let fence = ClaimFence {
                    chunk_id,
                    claimed_by: WORKER,
                    idle_timeout: Duration::from_secs(60),
                };
                fence.hold(&*txn).await.expect("hold")
            }
        });
        wait_for_lock_waiters(2).await;

        blocking.commit().await.expect("release the resume");
        resuming.await.expect("join").expect("resume");
        assert!(
            !fenced.await.expect("join"),
            "a fence that waited for the resume's lock sees the resume"
        );
    }

    /// A worker that stalls inside a fenced write transaction loses its
    /// session once it has sat idle for the reclaim TTL, so its lock can't
    /// keep a resume waiting or the chunk from being reclaimed past the point
    /// the stale-claim sweep would have given up on it anyway. The stalled
    /// worker is a transaction that holds the fence and then does nothing,
    /// under a 200 ms TTL.
    #[tokio::test]
    async fn a_fenced_transaction_left_idle_past_the_reclaim_ttl_loses_its_lock() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        let (id, def) = seed_waiting(&raw, "orders", "order_doubles", 3).await;
        dispatch(&pool, id, &def, "public.orders").await;
        let held = claim_chunks(&raw, WORKER, 1).await.expect("claim");
        assert_eq!(held.len(), 1);
        let fence = ClaimFence {
            chunk_id: held[0].id,
            claimed_by: WORKER,
            idle_timeout: Duration::from_millis(200),
        };

        let (_, mut stalled) = connect(&db).await;
        let stalled_pid: i32 = stalled
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("read the stalled session's pid")
            .get(0);
        let writing = stalled.transaction().await.expect("begin");
        assert!(fence.hold(&writing).await.expect("hold"), "the claim holds");
        assert_eq!(
            reclaim_stale_chunks(&mut raw, Duration::ZERO)
                .await
                .expect("reclaim"),
            0,
            "the sweep passes over a chunk whose write holds its fence"
        );

        // Waits for the server to end the idle session, not for a duration.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let alive: bool = raw
                .query_one(
                    "select exists (select 1 from pg_stat_activity where pid = $1)",
                    &[&stalled_pid],
                )
                .await
                .expect("read pg_stat_activity")
                .get(0);
            if !alive {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the idle fenced transaction was never ended"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            reclaim_stale_chunks(&mut raw, Duration::ZERO)
                .await
                .expect("reclaim"),
            1,
            "with the session gone, the claim is reclaimed"
        );
        assert_eq!(
            claim_chunks(&raw, "next-worker", 1)
                .await
                .expect("claim again")
                .len(),
            1,
            "and the chunk can be claimed again"
        );
        assert!(
            writing.commit().await.is_err(),
            "the stalled worker's transaction is gone with its session"
        );
    }
}
