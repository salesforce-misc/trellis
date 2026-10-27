//! The durable, claimable backfill-chunk work queue (ADR-0002's "A build
//! is Re-derive over chunks, and applies from its first chunk" and
//! `docs/decisions/0008-public-api-design.md`'s decision 1).
//!
//! A Re-derive build (`staging::build`) is a plan job, chunks and a sweep job,
//! each a row of the `backfill_chunks` table.
//! [`claim_chunks_of`]/[`reclaim_stale_chunks`]/[`fail_chunk`] mirror
//! `staging::claim`/`staging::liveness`'s claim/reclaim-stale/release-on-error
//! idiom for sealed ring segments, collapsed onto one table (a chunk, unlike a
//! segment, is never bucket-split across several workers at once). A running
//! chunk keeps its claim alive out-of-band for its whole duration via the
//! internal `ChunkHeartbeat`, the same "keep a claim alive against wall-clock
//! time, not against how much work is left" idiom
//! `staging::liveness::HeartbeatDaemon` uses for segment claims.
//!
//! A chunk that fails never retries silently. [`fail_chunk`] records
//! the failure on the chunk's row, where `Trellis::status` reports it, logs
//! it at warn, and by its kind backs the chunk off for a retry, splits it to
//! narrow a failure on its data down to the key that causes it and
//! quarantines that key, or, once a failure that can't be narrowed has been
//! charged [`MAX_CHARGED_ATTEMPTS`] times, pauses the definition.

use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use crate::pool::Pool;

use super::backfill::{self, BackfillError};
use super::catalog::CatalogError;
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
    /// whole-transform fuse after it, failed.
    Quarantine(Box<crate::staging::ApplyError>),
    /// A Re-derive build's plan job or chunk failed
    /// (`staging::build`).
    Build(Box<crate::staging::ApplyError>),
    /// A claimed row's `kind` and bounds were not one this build knows
    /// (`backfill_chunks_kind_bounds` rules out a mismatch), or a Re-derive
    /// build's row reached the old build's runner.
    UnknownKind { kind: String },
    /// A partition correction failed (ADR-0017 P4a,
    /// `staging::partition_correction`).
    Correction(Box<crate::staging::partition_correction::CorrectionError>),
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
            ChunkQueueError::Correction(err) => write!(f, "{err}"),
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
            ChunkQueueError::Correction(err) => Some(err.as_ref()),
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

impl From<crate::locks::ColumnPauseLockError> for ChunkQueueError {
    fn from(err: crate::locks::ColumnPauseLockError) -> Self {
        ChunkQueueError::Build(Box::new(err.into()))
    }
}

impl From<CatalogError> for ChunkQueueError {
    fn from(err: CatalogError) -> Self {
        ChunkQueueError::Catalog(err)
    }
}

/// One durable `backfill_chunks` row, as claimed by [`claim_chunks_of`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedChunk {
    pub id: i64,
    pub definition_id: i64,
    pub work: ChunkWork,
    /// A field build's scope (`backfill_chunks.fields`): the
    /// target columns a [`ChunkWork::Plan`] or [`ChunkWork::Rederive`] row
    /// writes. `None` for a whole build.
    pub fields: Option<Vec<String>>,
    /// The collations of the source key's columns this chunk's ranges were
    /// planned under (`backfill_chunks.key_collations`), which
    /// its range is read under; see `ddl::PrimaryKeyColumn::collation`.
    /// `None` until a Re-derive plan job's first batch records them, and for
    /// a kind with no key range.
    pub key_collations: Option<Vec<Option<String>>>,
}

/// What one `backfill_chunks` row builds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkWork {
    /// A Re-derive build's plan job (`staging::build`): walks the
    /// source's primary key from `cursor`, the last boundary it enqueued
    /// (`None` before the first), and enqueues [`ChunkWork::Rederive`] rows
    /// in batches.
    Plan { cursor: Option<String> },
    /// One `(lo, hi]` range of a Re-derive build (`staging::build::run_chunk`).
    Rederive { lo: Option<String>, hi: String },
    /// A rebuilt Re-derive build's sweep (`staging::build`): walks
    /// the target's ledger from `cursor`, the last entry key it has passed
    /// (`None` before the first), and re-derives every live entry the
    /// build's chunks didn't. It is claimed only once its build's plan job
    /// and every chunk are done ([`claim_chunks_of`]).
    Sweep { cursor: Option<String> },
    /// A partition correction for one aggregate definition (ADR-0017 P4a,
    /// `staging::partition_correction`): re-derives the key set of
    /// `partition_corrections` row `correction_id`, resuming from its
    /// `partition_correction_progress`.
    Correction { correction_id: i64 },
}

impl ChunkWork {
    /// The `backfill_chunks.kind` word of this work.
    #[cfg(any(test, feature = "internals"))]
    pub fn kind(&self) -> &'static str {
        match self {
            ChunkWork::Plan { .. } => KIND_PLAN,
            ChunkWork::Rederive { .. } => KIND_REDERIVE,
            ChunkWork::Sweep { .. } => KIND_SWEEP,
            ChunkWork::Correction { .. } => KIND_CORRECTION,
        }
    }
}

/// `backfill_chunks.kind` of a Re-derive build's plan job.
pub const KIND_PLAN: &str = "plan";
/// `backfill_chunks.kind` of a Re-derive build's chunk.
pub const KIND_REDERIVE: &str = "rederive";
/// `backfill_chunks.kind` of a rebuild's sweep job.
pub const KIND_SWEEP: &str = "sweep";
/// `backfill_chunks.kind` of a partition correction (ADR-0017 P4a).
pub const KIND_CORRECTION: &str = "correction";

/// SQL predicate over a `backfill_chunks` row aliased `bc` joined to its
/// definition's `transform_definitions` row aliased `d`: whether the chunk was
/// planned before the definition's latest resume. Every
/// resume (`staging::quarantine::resume_transform`) stamps a new
/// `fuse_rearmed_at`, and every chunk copies the value it saw when it was
/// planned, so a chunk from a build the resume superseded carries a different
/// one.
///
/// Resume deletes a stale definition's unclaimed chunks outright. A stale
/// chunk a worker still held across the resume is never claimable again and
/// never completes the definition, whose rebuild (started by the next
/// capture pass, which enqueues fresh chunks) owns its way back to `live`:
/// [`fail_chunk`]/[`reclaim_stale_chunks`]/[`discard_if_superseded`] discard
/// it rather than freeing or completing it. It writes nothing after the resume
/// either ([`ClaimFence`]).
pub(crate) const STALE: &str = "bc.fuse_rearmed_at is distinct from d.fuse_rearmed_at";

/// The claim a drain worker holds on one chunk, which fences every target
/// write the chunk makes.
///
/// A chunk's write bypasses the target-mutation seam
/// (`staging::target_mutations`), which is only safe while nothing reads the
/// target: while its definition is still being built. A chunk a worker holds
/// across a pause and a resume ([`STALE`]) would break that if its write
/// could land once the resumed definition's rebuild has finished, since
/// readers can attach from then on. A definition reading the target would
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
/// Resume and drop lock the definition row, then the chunk rows. A fenced
/// transaction that writes a row whose foreign key references the definition
/// (a plan job's `insert into backfill_chunks`) takes the definition row
/// `for key share` before [`hold`], not after: the foreign-key check would
/// otherwise wait on the definition row while holding the chunk, the reverse
/// order, and deadlock with them (`staging::build::run_plan`). So does a
/// group-basis build's write, whose groups a resume after a logical restore
/// locks before the chunk rows (`staging::build::run_group_structure`). Like
/// any lock taken ahead of the claim's, it comes after [`arm`](Self::arm).
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
        self.arm(txn).await?;
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

    /// Sets `txn`'s `idle_in_transaction_session_timeout` to the reclaim
    /// TTL, as [`hold`](Self::hold) does first. A fenced transaction that
    /// takes a lock before the claim's calls this before that lock, so the
    /// timeout bounds a stall that holds it too
    /// (`staging::build::field_build_ready`'s version-fence bump).
    pub(crate) async fn arm(&self, txn: &impl GenericClient) -> Result<(), tokio_postgres::Error> {
        // Zero would disable the timeout, and the setting is an `int` of
        // milliseconds.
        let idle_ms = self.idle_timeout.as_millis().clamp(1, i32::MAX as u128);
        txn.batch_execute(&format!(
            "set local idle_in_transaction_session_timeout = {idle_ms}"
        ))
        .await
    }
}

/// SQL over a `backfill_chunks bc` row: its definition is in a status a
/// chunk may be handed out in (`param` binds [`TransformStatus::dispatchable`]
/// as a `text[]`). One predicate for the claim ([`claim_chunks_of`]) and for
/// the idle probe (`staging::build::work_probe`), so the probe stays a
/// superset of the claim and the two cannot drift.
pub(crate) fn dispatchable_gate_sql(param: &str) -> String {
    format!(
        "exists (select 1 from transform_definitions d \
                 where d.id = bc.definition_id and d.status = any({param}))"
    )
}

/// Claims up to `limit` unclaimed, undone chunks of the given `kinds` for
/// `claimed_by` in one statement — the backfill-chunk analogue of
/// `staging::claim`/`staging::next_claimable_segments`, collapsed onto a
/// single table+statement since a chunk is never bucket-split. The `for
/// update skip locked` candidate selection means two workers racing this
/// call never claim the same row twice and never block on each other.
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
    // — see [`TransformStatus::dispatchable`].
    let dispatchable = TransformStatus::dispatchable();
    let rows = client
        .query_typed(
            // The `exists` gate is ADR-0014's: the pause is what
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
            // not: this gate used to read `status <> 'paused'`,
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
            // (`next_attempt_at`) before it is handed out again.
            //
            // A Re-derive build's sweep waits for its build's plan
            // job and every chunk: it re-derives what they didn't, so it
            // reads their bases. A row held across a resume doesn't hold it.
            &format!(
                "with candidate as ( \
                 select bc.id from backfill_chunks bc \
                 where not bc.done and bc.claimed_by is null and bc.kind = any($4) \
                   and (bc.next_attempt_at is null or bc.next_attempt_at <= now()) \
                   and {dispatchable_gate} \
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
             returning c.id, c.definition_id, c.lo, c.hi, c.kind, c.fields, \
                       c.key_collations, c.correction_id",
                dispatchable_gate = dispatchable_gate_sql("$3"),
            ),
            &[
                (&claimed_by, Type::TEXT),
                (&limit, Type::INT8),
                (&dispatchable, Type::TEXT_ARRAY),
                (&kinds, Type::TEXT_ARRAY),
            ],
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            let kind: String = row.get(4);
            let (lo, hi): (Option<String>, Option<String>) = (row.get(2), row.get(3));
            let correction: Option<i64> = row.get(7);
            let work = match (kind.as_str(), hi) {
                (KIND_CORRECTION, None) if let Some(correction_id) = correction => {
                    ChunkWork::Correction { correction_id }
                }
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
                fields: row.get(5),
                key_collations: row.get(6),
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
/// next [`claim_chunks_of`] call.
///
/// A chunk held across its definition's resume ([`STALE`]) is deleted
/// instead of freed — see [`free_or_discard_claims`].
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
/// failed chunk through [`fail_chunk`] instead, which records the failure;
/// this is the bare release the tests drive a held chunk with.
/// Scoped `claimed_by = $2` for the same reason that function is: a claim
/// already reclaimed out from under this caller (the TTL sweep, or another
/// worker) is left untouched.
///
/// A chunk held across its definition's resume ([`STALE`]) is deleted instead
/// of freed — see [`free_or_discard_claims`]. Returns how many
/// claims (zero or one) it released or discarded.
#[cfg(any(test, feature = "internals"))]
pub async fn release_chunk(
    client: &mut impl GenericClient,
    id: i64,
    claimed_by: &str,
) -> Result<u64, ChunkQueueError> {
    let txn = client.transaction().await?;
    // The definition row first, then the chunk: the order resume takes them
    // in (it locks a held chunk to wait out its write), so the
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
/// with the error. A failure is charged when it is neither transient
/// nor narrowed to a key: it says nothing about any one row, and retrying it
/// without end would leave the definition `backfilling` forever with only a
/// log line to show for it.
pub(crate) const MAX_CHARGED_ATTEMPTS: i32 = 5;

/// The backoff after a chunk's first failure, doubled per further failure.
const RETRY_BASE: Duration = Duration::from_secs(1);

/// The longest a failed Re-derive chunk or plan job waits before it is
/// claimed again. Its usual failure is its entry lock's short
/// timeout behind a drain page, which says nothing about the chunk.
pub(crate) const REDERIVE_RETRY_CAP: Duration = Duration::from_secs(5);

/// How long a chunk waits after its `attempts`th failure before it is
/// claimed again: [`RETRY_BASE`], doubled per further failure, capped at
/// [`RETRY_CAP`].
pub(crate) fn retry_delay(attempts: i32, cap: Duration) -> Duration {
    let doublings = u32::try_from(attempts.saturating_sub(1))
        .unwrap_or(0)
        .min(31);
    RETRY_BASE.saturating_mul(1 << doublings).min(cap)
}

/// What a chunk's failure says about how to retry it.
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
    /// The chunk was held across its definition's resume ([`STALE`]): it
    /// was deleted, and the resume's rebuild owns the work.
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
}

/// Gives up `chunk` after running it failed with `error` — what a drain
/// worker calls on a failure — and logs at warn what it did, with the
/// definition, the chunk, its range or key, the attempt and when the next one
/// is.
///
/// A Re-derive chunk records the failure on its row (`attempts`,
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
/// A plan job, a sweep job and a partition correction are not narrowed: their
/// data failures are charged like any other.
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
    let sqlstate = crate::staging::quarantine::sqlstate_of(error);
    let (outcome, attempts, location) = match &chunk.work {
        // A Re-derive chunk narrows a data failure to a key as a 1-1 range
        // chunk does: its range holds the same keys, and
        // it leaves a quarantined key out the same way.
        //
        // "The same keys" holds for a source that is another aggregate's
        // target too, keyed by its nullable grouping columns: the
        // build's chunk leaves a key with a `NULL` part out, as
        // `narrow_one_to_one_chunk` never counts one (`key_not_null`), and
        // the plan job stages those keys for a page to re-derive instead
        // (`staging::build`'s "Seam-fed sources"). Counting one here would
        // pin a failure on an innocent key beside it.
        //
        // A transient failure (the chunk's short entry-lock timeout, mostly)
        // backs off for at
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
                sqlstate.as_deref(),
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
                sqlstate.as_deref(),
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
                sqlstate.as_deref(),
                REDERIVE_RETRY_CAP,
            )
            .await?;
            let location = format!(
                "the re-derive build's sweep after {}",
                cursor.as_deref().unwrap_or("-infinity")
            );
            (outcome, attempts, location)
        }
        // A correction is not narrowed: a failure on one key's data fails
        // its page, whose keys are not a range a split could halve. It backs
        // off as a Re-derive chunk does, and its charged failures pause the
        // definition, whose resume rebuilds it from the source.
        ChunkWork::Correction { correction_id } => {
            let (outcome, attempts) = fail_range_chunk(
                pool,
                chunk,
                None,
                claimed_by,
                kind,
                &message,
                sqlstate.as_deref(),
                REDERIVE_RETRY_CAP,
            )
            .await?;
            (
                outcome,
                attempts,
                format!("the partition correction {correction_id}"),
            )
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
        ChunkFailure::NotHeld => crate::instance_log::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, error = %error,
            "backfill chunk failed after its claim was reclaimed; its new holder retries it"
        ),
        ChunkFailure::Discarded => crate::instance_log::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, error = %error,
            "backfill chunk failed after its definition was frozen or resumed; discarded it"
        ),
        ChunkFailure::Retrying { charged, delay } => crate::instance_log::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt,
            charged, max_charged = MAX_CHARGED_ATTEMPTS,
            next_attempt_in_secs = delay.as_secs_f64(), error = %error,
            "backfill chunk failed; retrying it after a backoff"
        ),
        ChunkFailure::Split { mid } => crate::instance_log::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt,
            split_at = %mid, next_attempt_in_secs = 0.0, error = %error,
            "backfill chunk failed on its data; split it in two to narrow the failure to a key"
        ),
        ChunkFailure::Quarantined { key, fuse_tripped } => crate::instance_log::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt,
            key = %key, fuse_tripped, next_attempt_in_secs = 0.0, error = %error,
            "backfill chunk failure narrowed to one key; quarantined the key, and the build \
             goes on without it"
        ),
        ChunkFailure::Paused => crate::instance_log::warn!(
            definition_id, chunk_id, chunk = %location, failure = %kind, attempt,
            max_charged = MAX_CHARGED_ATTEMPTS, error = %error,
            "backfill chunk kept failing without narrowing to a key; paused the definition \
             (resume it once the cause is fixed)"
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
        crate::instance_log::warn!(
            definition_id = id,
            to = %TransformStatus::Paused.as_str(),
            "transform status transition: its build kept failing; paused"
        );
    }
    Ok(())
}

/// [`fail_chunk`] for a Re-derive build's chunk, plan job, sweep job or
/// correction. Returns what it did and the chunk's failure
/// count, this one included.
///
/// `narrow` is the range a data failure is narrowed within, or `None` when
/// the chunk isn't narrowed: its data failures are charged like any other.
/// `sqlstate` is the failure's, which a key it quarantines keeps.
/// `retry_cap` caps the backoff.
#[allow(clippy::too_many_arguments)]
async fn fail_range_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    narrow: Option<(Option<&str>, &str)>,
    claimed_by: &str,
    kind: FailureKind,
    error: &str,
    sqlstate: Option<&str>,
    retry_cap: Duration,
) -> Result<(ChunkFailure, i32), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // The definition row first, as a resume locks it, then the chunk:
    // the order resume takes them in.
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&chunk.definition_id],
    )
    .await?;
    let Some(row) = txn
        .query_opt(
            &format!(
                "select {STALE}, bc.attempts + 1, bc.charged, d.source_table, d.target_table, \
                        bc.key_collations \
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
    let key_collations: Option<Vec<Option<String>>> = row.get(5);

    let narrowed = match (kind, narrow) {
        (FailureKind::Data, Some((lo, hi))) => Some(
            backfill::narrow_one_to_one_chunk(
                &*txn,
                &source_table,
                target_table
                    .split_once('.')
                    .map_or(target_table.as_str(), |(_, target)| target),
                lo,
                hi,
                key_collations.as_deref(),
            )
            .await?,
        ),
        _ => None,
    };
    let outcome = match narrowed {
        Some(backfill::ChunkNarrowing::Split { mid }) => {
            // This row keeps the lower half, and a new one takes the upper
            // half, of the same kind (a 1-1 range or a Re-derive chunk),
            // field scope and key collations. Both carry
            // the failure so far, so `status` keeps reporting it while the
            // build narrows, and the build's `fuse_rearmed_at`, so a resume
            // supersedes both alike.
            txn.execute(
                "insert into backfill_chunks \
                     (definition_id, kind, lo, hi, fuse_rearmed_at, attempts, charged, \
                      last_error, next_attempt_at, fields, key_collations) \
                 select definition_id, kind, $2, hi, fuse_rearmed_at, $3, charged, $4, now(), \
                        fields, key_collations \
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
            crate::staging::quarantine::evict_build_key(
                &txn,
                chunk.definition_id,
                &source_table,
                &key,
                error,
                sqlstate,
            )
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
            let fuse_tripped =
                crate::staging::quarantine::trip_build_fuse(pool, chunk.definition_id)
                    .await
                    .map_err(|err| ChunkQueueError::Quarantine(Box::new(err)))?;
            ChunkFailure::Quarantined { key, fuse_tripped }
        }
        outcome => outcome,
    };
    Ok((outcome, attempts))
}

/// Gives up the claims `rows` (each `(chunk id, stale)`, locked by the
/// caller along with a share lock on each chunk's definition row) name.
///
/// A current chunk is freed, claimable again by the next [`claim_chunks`]. A
/// chunk held across its definition's resume ([`STALE`]) is deleted
/// instead: resume discarded its unclaimed siblings and left
/// this one only so its worker could finish it, and running it again
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
        let task = tokio::spawn(crate::instance_log::in_current_instance(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                touch_chunk_claim(&pool, id, &claimed_by).await;
            }
        }));
        ChunkHeartbeat { task }
    }
}

impl Drop for ChunkHeartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Gives up `chunk` after its fence stopped a Re-derive build's write
/// (`staging::build`): deletes it if it was held across its
/// definition's resume ([`STALE`]), and otherwise
/// leaves it alone (its claim was reclaimed, or it is done).
pub(crate) async fn discard_if_superseded(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
) -> Result<(), ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // The definition row first, then the chunk: resume's order.
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::defs::ast::ValueType;
    use crate::integer::IntWidth;

    /// A fenced transaction left idle past the reclaim TTL loses its lock
    /// (`idle_in_transaction_session_timeout`, set by [`ClaimFence::hold`]):
    /// the stale-claim sweep skips the chunk while the session holds it, and
    /// reclaims it once the server has ended the session. The session is kept
    /// busy on an advisory lock until the first sweep has looked, so the
    /// timeout cannot fire early under load, and the test waits for the
    /// session's end with a lock request its lock blocks, not a poll.
    #[tokio::test]
    async fn a_fenced_transaction_left_idle_loses_its_lock_and_the_chunk_is_reclaimed() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let connect = || async {
            let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .batch_execute(&format!(
                    "set search_path to {}, public",
                    crate::config::DEFAULT_SCHEMA
                ))
                .await
                .expect("set search_path");
            client
        };
        let mut raw = connect().await;
        raw.batch_execute(
            "create table public.nums (id bigint primary key, x integer); \
             insert into public.nums select i, i from generate_series(1, 30) i",
        )
        .await
        .expect("seed the source");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("x".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        crate::defs::install_definition(
            &pool,
            "TRANSFORM doubles FROM nums SELECT x + x AS doubled",
            &columns,
            "public",
        )
        .await
        .expect("install_definition");
        crate::client::reconcile_pass(&mut raw, &pool, crate::config::DEFAULT_SCHEMA)
            .await
            .expect("start the build");
        let claimed = {
            let client = pool.get().await.expect("pool");
            claim_chunks_of(&**client, "holder", 1, &[KIND_PLAN])
                .await
                .expect("claim the plan job")
        };
        let id = claimed[0].id;
        raw.execute(
            "update backfill_chunks set claimed_at = now() - interval '1 hour' where id = $1",
            &[&id],
        )
        .await
        .expect("age the claim");

        // The fenced session waits on an advisory lock `gate` holds, so it is
        // busy, not idle, until the sweep has looked: the 300 ms timeout
        // starts only when the gate lets it finish that statement.
        const BUSY_LOCK: i64 = 0x1380;
        let gate = connect().await;
        gate.execute("select pg_advisory_lock($1)", &[&BUSY_LOCK])
            .await
            .expect("take the gate lock");
        let mut holder = connect().await;
        let txn = holder.transaction().await.expect("begin");
        let fence = ClaimFence::new(id, "holder", Duration::from_millis(300));
        assert!(fence.hold(&txn).await.expect("hold"), "the claim holds");

        let mut sweeper = pool.get().await.expect("pool");
        let busy = txn.execute("select pg_advisory_lock($1)", &[&BUSY_LOCK]);
        let sweep_then_idle = async {
            assert_eq!(
                reclaim_stale_chunks(&mut **sweeper, Duration::from_secs(30))
                    .await
                    .expect("reclaim"),
                0,
                "the sweep skips a chunk whose fenced transaction is open"
            );
            gate.execute("select pg_advisory_unlock($1)", &[&BUSY_LOCK])
                .await
                .expect("release the gate lock");
            // Blocks until the server ends the now idle session and its lock
            // goes.
            raw.batch_execute("set statement_timeout = '30s'")
                .await
                .expect("bound the wait");
            raw.execute(
                "select 1 from backfill_chunks where id = $1 for update",
                &[&id],
            )
            .await
            .expect("the idle session released the chunk's lock");
        };
        let (busy, ()) = tokio::join!(busy, sweep_then_idle);
        busy.expect("the fenced session got the gate lock");

        assert_eq!(
            reclaim_stale_chunks(&mut **sweeper, Duration::from_secs(30))
                .await
                .expect("reclaim"),
            1,
            "and the sweep now reclaims it"
        );
        assert!(
            raw.query_one(
                "select claimed_by is null from backfill_chunks where id = $1",
                &[&id]
            )
            .await
            .expect("read the chunk")
            .get::<_, bool>(0)
        );
        assert!(
            txn.query("select 1", &[]).await.is_err(),
            "the idle session was ended"
        );
    }
}
