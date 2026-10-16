//! The correction engine for aggregates over a partitioned source (ADR-0017,
//! P4a of epic #897).
//!
//! Partition maintenance (`ATTACH`, `DETACH`, `DROP`, a new leaf written
//! directly) changes a partitioned root's rows without a write any capture
//! trigger sees. The reconcile pass records each such change as a durable
//! correction request ([`crate::capture::partitions`], P3). This module
//! consumes the requests of an **aggregate** that reads the source, and
//! (the `to_many` module, P4b) of a definition that reads it through a **to-many
//! relationship**.
//!
//! # What a correction does
//!
//! A correction **re-derives a key set** through the ledger's normal paths
//! ([`super::build::rederive_keys`], the rebuild sweep's primitive): each key
//! takes its entry lock, the source row is read live through the root, and
//! the entry is rewritten (or tombstoned when the row is gone), so its group
//! moves by a delta, which the merger folds like any other. The code here
//! never deletes a target row, a ledger entry or a delta: deleting a group
//! row would leave its entries live, and deleting entries would drop the
//! tombstones that refuse a ring change staged before the DDL, which would
//! then resurrect the row.
//!
//! The key set of a node is two walks, each in bounded pages (one page, one
//! transaction), with the progress committed in the page's own transaction:
//!
//! 1. **The ledger phase.** The keys of the live ledger entries whose group
//!    routes to the node ([`Route`]), found through the ledger's `GROUP BY`
//!    index (a partitioned source's aggregate always has it) in
//!    `(group, key)` order, so a restart resumes after the last page.
//! 2. **The node phase**, for a node in the tree: the keys of its current
//!    rows, read from the node in primary-key order, as a build reads the
//!    source. It finds the rows a new, attached or re-attached leaf holds
//!    that no entry knows, including rows written into it directly before
//!    its capture existed. A gone node needs it only without a usable text
//!    (a detached table is then read the same way): every row of a gone node
//!    routed by its text has its live entry found by the ledger phase.
//!
//! # Routing
//!
//! A group routes to a node when the node's recorded
//! `pg_get_partition_constraintdef` text holds over the group's key columns
//! ([`route_predicate`]). The text names the source's key columns, so the
//! group's values are presented under those names, each cast to the key
//! column's recorded type, under that type's own collation: a `HASH` text
//! raises on a differently typed column, and a text names a level's
//! collation on a bound (`COLLATE`) only when it isn't the type's, so each
//! comparison is under its own level's collation, whichever levels differ.
//! The read runs under pinned settings (`search_path = pg_catalog, pg_temp`,
//! `DateStyle ISO, YMD`, `TimeZone UTC`: [`crate::capture::partitions`]'s
//! `pinned_settings`), because the text of a bound, and what it means, depends
//! on them: under the session's search path an enum in a `RANGE` bound, an ICU
//! collation shadowing a built-in name and a custom operator in `public`
//! silently misroute rows. A `DEFAULT` with no siblings has no text (every
//! group routes to it), and any other node without a usable text routes
//! nothing.
//!
//! A `complement` correction, which the every-pass sweep queues when it finds
//! groups that route to no current leaf ([`crate::capture::complement`],
//! P7), routes by the complement text it was queued with, as a gone node
//! does by its recorded one, and has no node phase.
//!
//! A `HASH` text embeds its parent's oid, so a dropped middle's leaves are
//! routed only through the middle's own text. That is why a correction names
//! the topmost gone node, and why the pass records the text while the node is
//! in the tree. A leaf that left the tree before its middle was dropped has a
//! text that embeds the middle's oid, which no longer exists: the middle's own
//! correction routes every group of its bound, so the leaf's is complete
//! without work (`parent_oid`).
//!
//! # Ordering against the ring
//!
//! A change staged before the DDL and applied after the correction must not
//! resurrect a row the correction retracted. A correction therefore starts
//! by stamping the definition's `build_seg` (the segment active when it
//! starts), as a rebuild does (`super::build::stamp_rebuild_seg`, #733): a
//! page with a batch at or below it re-derives its keys rather than applying
//! their changes. Then it waits for the transactions open at the stamp to end
//! (the `fence` phase, as a seam-fed build's plan job does), so no page that
//! read the old `build_seg` can still apply a stale change to a key with no
//! entry yet. A `gone_node` whose `pg_inherits` row is still there
//! (a detach pending) waits for `DETACH ... FINALIZE`, which waits out the
//! writers with older snapshots, before it starts: only then is the ring
//! complete for the node.
//!
//! # Scheduling
//!
//! A correction runs for each definition that reads the source (an aggregate
//! over it, or one reading it through a to-many relationship) as a
//! `backfill_chunks` row of kind `correction` (V92), one per correction and
//! definition, which the staging worker's pass enqueues ([`schedule`]) right
//! after the capture pass that queued the correction. The row gives it what a
//! build chunk has: one runner at a time (the claim), a heartbeat, the
//! stale-claim sweep for a runner that died, and a fence on every write
//! (`ClaimFence`), so a runner whose claim was reclaimed, or that held it
//! across a pause and a resume, writes nothing.
//!
//! The pass that enqueues the row moves a `live` definition to `backfilling`
//! under a Re-derive build (`build = 'rederive'`), in the same transaction,
//! so from the pass that detects the operation the definition reports
//! `backfilling`. The drain workers claim the row like a build chunk
//! ([`super::build::work_once`]), run up to [`CLAIM_PAGES`] pages, and give
//! it back for the next claim; the merger folds the correction's group
//! deltas while the definition is `backfilling`, and
//! [`super::build::try_complete`] moves it back to `live` once the row is
//! done and its deltas are folded, as it does at the end of a build. A
//! definition already under a Re-derive build stays `backfilling` until both
//! are done. A `catching_up` definition gets its row once its catch-up has
//! moved it `live`: the merger folds only a `backfilling` definition's
//! deltas, and the catch-up's discharge owns its status until then. It holds
//! the correction in the queue meanwhile.
//!
//! A correction that fails is given up through the chunk queue's
//! `fail_chunk`: its row records the error, which `Trellis::status` reports,
//! and is retried after a backoff (a transient failure is not charged), and
//! its [`chunk_queue::MAX_CHARGED_ATTEMPTS`]th charged failure pauses the
//! definition, whose resume rebuilds it from the source; the row stays, with
//! the correction, as the record `Trellis::status` reports until that resume.
//! Each row is its own claim, so a failing correction holds up no other
//! correction, definition or source. A correction leaves the queue once every
//! definition that reads its source and applies has corrected it
//! ([`schedule`], and the runner that finishes the last).
//!
//! A to-many reader's correction stages recomputes into the ring rather than
//! writing its target, and its target has no group deltas for
//! [`super::build::try_complete`] to wait on. So its row is done only once
//! the ring has drained through the segment that was active when its last
//! page committed (the `drain` phase): the definition reads `backfilling`
//! until every recompute the correction staged is applied.

use std::fmt;
use std::time::Duration;

use tokio_postgres::{GenericClient, IsolationLevel};

use crate::capture::partitions::{self, Key, PendingCorrection, Recorded};
use crate::defs::chunk_queue::{self, ChunkQueueError, ClaimFence, ClaimedChunk};
use crate::defs::model::{APPLYING_SQL, Definition, TransformStatus};
use crate::defs::{backfill, catalog, ddl};
use crate::error_code::ErrorCode;
use crate::pool::{Pool, quote_ident};

use super::apply::ApplyError;
use super::build::{self, BuildPlan, CHUNK_APPLICATION_NAME};
use super::ledger::LedgerTargetPlan;

mod to_many;

/// Keys per page by default, the size of a build chunk
/// ([`build::DEFAULT_CHUNK_ROWS`]): one page is one transaction and one entry
/// lock set, so it bounds the locks and the WAL a transaction holds.
pub const DEFAULT_PAGE_KEYS: usize = 10_000;

/// Pages a drain worker runs per claim of a correction before it gives the
/// row back: about a second of work at [`DEFAULT_PAGE_KEYS`], so the
/// worker's segments, merges and other chunks get their turn, and the ring's
/// backlog is checked again before the next claim.
pub const CLAIM_PAGES: usize = 10;

/// How long a correction waits before it looks again at a detach that is
/// still pending or a fence that has not settled.
const WAIT_RETRY: Duration = Duration::from_secs(1);

/// Why a correction stopped.
#[derive(Debug)]
pub enum CorrectionError {
    Db(tokio_postgres::Error),
    Pool(crate::error::Error),
    Apply(ApplyError),
    Capture(crate::capture::CaptureError),
    Catalog(catalog::CatalogError),
    Staging(super::StagingError),
    /// The node's group cannot be presented to its constraint text.
    Unroutable(String),
    /// The Re-derive build does not serve the definition's shape, so its keys
    /// cannot be re-derived.
    Unserved {
        target: String,
    },
}

impl CorrectionError {
    /// This error's [`ErrorCode`] category.
    pub fn code(&self) -> ErrorCode {
        match self {
            CorrectionError::Db(err) => crate::error_code::classify_pg_error(err),
            CorrectionError::Pool(err) => err.code(),
            CorrectionError::Apply(err) => err.code(),
            CorrectionError::Catalog(err) => err.code(),
            CorrectionError::Staging(err) => err.code(),
            CorrectionError::Capture(err) => {
                let mut link: Option<&(dyn std::error::Error + 'static)> = Some(err);
                while let Some(err) = link {
                    if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
                        return crate::error_code::classify_pg_error(pg);
                    }
                    link = err.source();
                }
                ErrorCode::Internal
            }
            CorrectionError::Unroutable(_) | CorrectionError::Unserved { .. } => {
                ErrorCode::Internal
            }
        }
    }
}

impl fmt::Display for CorrectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorrectionError::Db(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Pool(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Apply(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Capture(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Catalog(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Staging(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Unroutable(why) => write!(f, "partition correction: {why}"),
            CorrectionError::Unserved { target } => write!(
                f,
                "partition correction: the Re-derive build does not serve {target}'s shape, \
                 so its keys cannot be re-derived"
            ),
        }
    }
}

impl std::error::Error for CorrectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CorrectionError::Db(err) => Some(err),
            CorrectionError::Pool(err) => Some(err),
            CorrectionError::Apply(err) => Some(err),
            CorrectionError::Capture(err) => Some(err),
            CorrectionError::Catalog(err) => Some(err),
            CorrectionError::Staging(err) => Some(err),
            CorrectionError::Unroutable(_) | CorrectionError::Unserved { .. } => None,
        }
    }
}

impl From<CorrectionError> for ChunkQueueError {
    fn from(err: CorrectionError) -> Self {
        ChunkQueueError::Correction(Box::new(err))
    }
}

impl From<tokio_postgres::Error> for CorrectionError {
    fn from(err: tokio_postgres::Error) -> Self {
        CorrectionError::Db(err)
    }
}

impl From<crate::error::Error> for CorrectionError {
    fn from(err: crate::error::Error) -> Self {
        CorrectionError::Pool(err)
    }
}

impl From<ApplyError> for CorrectionError {
    fn from(err: ApplyError) -> Self {
        CorrectionError::Apply(err)
    }
}

impl From<crate::capture::CaptureError> for CorrectionError {
    fn from(err: crate::capture::CaptureError) -> Self {
        CorrectionError::Capture(err)
    }
}

impl From<super::StagingError> for CorrectionError {
    fn from(err: super::StagingError) -> Self {
        CorrectionError::Staging(err)
    }
}

impl From<catalog::CatalogError> for CorrectionError {
    fn from(err: catalog::CatalogError) -> Self {
        CorrectionError::Catalog(err)
    }
}

/// How a correction runs: a drain worker's claim ([`CLAIM_PAGES`] pages of
/// [`DEFAULT_PAGE_KEYS`]), or a test's [`run_pending`].
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Keys per page.
    pub page_keys: usize,
    /// Stops after this many pages in all, leaving the progress committed so
    /// far for the next claim: a worker's turn, or a test's deterministic
    /// stand-in for a crash or a restart in the middle of a correction.
    pub max_pages: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            page_keys: DEFAULT_PAGE_KEYS,
            max_pages: None,
        }
    }
}

/// What a run of corrections did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Outcome {
    /// Pages run (a page is one transaction).
    pub pages: usize,
    /// Keys re-derived.
    pub keys: i64,
    /// Corrections taken off the queue.
    pub completed: usize,
    /// Corrections left for a later call: stopped at `max_pages`, or waiting
    /// for a detach to finalize or for the fence to settle.
    pub remaining: usize,
}

/// Where a group of keys is routed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Every group: a `DEFAULT` with no siblings.
    Every,
    /// The groups the constraint text holds for.
    Text(String),
    /// No usable text: nothing can be routed by it.
    Unusable,
}

/// One column of the group a constraint text is evaluated over.
#[derive(Debug, Clone)]
pub struct RouteColumn {
    /// The source's key column name, which the text uses.
    pub name: String,
    /// SQL for the group's value of it.
    pub value: String,
    /// The recorded key column, to cast to; `None` leaves the value as it is.
    pub key: Option<Key>,
}

/// SQL for a boolean: whether the group `columns` describes routes to
/// `route`. The text is evaluated over a one-row subquery that presents each
/// value under its column's name, cast to the key column's recorded type and
/// collation. It must run under the pinned settings.
pub fn route_predicate(route: &Route, columns: &[RouteColumn]) -> String {
    let text = match route {
        Route::Every => return "true".to_string(),
        Route::Unusable => return "false".to_string(),
        Route::Text(text) => text,
    };
    let presented: Vec<String> = columns
        .iter()
        .map(|c| {
            let value = match &c.key {
                None => c.value.clone(),
                Some(key) => {
                    let mut cast = format!("(({})::{}", c.value, key.pg_type);
                    if !key.collation.is_empty() {
                        cast.push_str(" collate ");
                        cast.push_str(&qualified_collation(&key.collation));
                    }
                    cast.push(')');
                    cast
                }
            };
            format!("{value} as {}", quote_ident(&c.name))
        })
        .collect();
    format!(
        "exists (select 1 from (select {}) as __route where {text})",
        presented.join(", ")
    )
}

/// `schema.collation` as recorded, quoted for a `collate` clause.
fn qualified_collation(recorded: &str) -> String {
    match recorded.split_once('.') {
        Some((schema, name)) => format!("{}.{}", quote_ident(schema), quote_ident(name)),
        None => quote_ident(recorded),
    }
}

/// The route of `correction`, given the source's recorded tree.
fn route_of(correction: &PendingCorrection, recorded: Option<&Recorded>) -> Route {
    // A gone node's and a complement's text is the queued one; a node in
    // the tree routes by what the record says of it now. A `truncated`
    // correction, queued by the seal, carries neither.
    let queued = correction.kind == partitions::Kind::GoneNode.as_str()
        || correction.kind == partitions::Kind::Complement.as_str();
    let (text, is_default) = if queued {
        (correction.constraint.clone(), correction.node_default)
    } else {
        let node = recorded.and_then(|r| r.nodes.iter().find(|n| n.oid == correction.node_oid));
        (
            node.and_then(|n| n.constraint.clone()),
            correction.node_default || node.is_some_and(|n| n.is_default),
        )
    };
    match text {
        Some(text) => Route::Text(text),
        None if is_default => Route::Every,
        None => Route::Unusable,
    }
}

/// The ledger's group columns as a constraint text sees them.
fn route_columns(
    plan: &LedgerTargetPlan,
    recorded: Option<&Recorded>,
    alias: &str,
) -> Vec<RouteColumn> {
    route_columns_named(&plan.plain_group_cols(), recorded, alias)
}

/// [`route_columns`] for the group columns `names`. A column is presented
/// under the first recorded key on it: its type, and its type's own
/// collation ([`Key::collation`]), which every level's text compares under
/// unless it names its level's on the bound (#1084).
fn route_columns_named(
    names: &[&str],
    recorded: Option<&Recorded>,
    alias: &str,
) -> Vec<RouteColumn> {
    names
        .iter()
        .map(|&name| RouteColumn {
            name: name.to_string(),
            value: format!("{alias}.{}", quote_ident(name)),
            key: recorded.and_then(|r| {
                r.keys
                    .iter()
                    .find(|k| k.column.as_deref() == Some(name))
                    .cloned()
            }),
        })
        .collect()
}

/// A progress phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Fence,
    Ledger,
    /// A to-many reader's walk of the routed from-side rows (`to_many`).
    Parents,
    Node,
    /// A to-many reader's recomputes are staged; it waits for the ring to
    /// drain through `drain_seg` (`to_many`).
    Drain,
    Done,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::Fence => "fence",
            Phase::Ledger => "ledger",
            Phase::Parents => "parents",
            Phase::Node => "node",
            Phase::Drain => "drain",
            Phase::Done => "done",
        }
    }

    fn parse(text: &str) -> Phase {
        match text {
            "fence" => Phase::Fence,
            "ledger" => Phase::Ledger,
            "parents" => Phase::Parents,
            "node" => Phase::Node,
            "drain" => Phase::Drain,
            _ => Phase::Done,
        }
    }
}

/// One definition's progress on one correction
/// (`partition_correction_progress`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    phase: &'static str,
    /// The last key of the phase's walk.
    pub cursor: Option<String>,
    /// Keys re-derived so far.
    pub keys_done: i64,
    /// The keys the correction expects to re-derive, an estimate for the
    /// node phase. `None` before the ledger phase counted them.
    pub keys_total: Option<i64>,
}

impl Progress {
    /// The phase: `fence`, `ledger`, `parents`, `node`, `drain` or `done`.
    pub fn phase(&self) -> &str {
        self.phase
    }

    /// Keys still to re-derive by the estimate.
    ///
    /// A to-many reader counts a parent done once its recompute is staged,
    /// so 0 here does not yet mean its target is right: the recomputes apply
    /// as the ring drains, which the `drain` phase waits for before the
    /// definition can read `live`.
    #[cfg_attr(
        not(any(test, feature = "internals")),
        allow(dead_code, reason = "read by tests and embedders through `internals`")
    )]
    pub fn keys_remaining(&self) -> Option<i64> {
        self.keys_total.map(|t| (t - self.keys_done).max(0))
    }
}

/// The progress of `definition_id` on correction `correction_id`, `None`
/// before the correction started for it.
pub async fn progress(
    client: &impl GenericClient,
    correction_id: i64,
    definition_id: i64,
) -> Result<Option<Progress>, tokio_postgres::Error> {
    Ok(client
        .query_opt(
            "select phase, cursor, keys_done, keys_total from partition_correction_progress \
             where correction_id = $1 and definition_id = $2",
            &[&correction_id, &definition_id],
        )
        .await?
        .map(|row| Progress {
            phase: Phase::parse(row.get(0)).as_str(),
            cursor: row.get(1),
            keys_done: row.get(2),
            keys_total: row.get(3),
        }))
}

/// What [`schedule`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Scheduled {
    /// Correction rows it enqueued.
    pub enqueued: usize,
    /// Corrections it took off the queue.
    pub completed: usize,
    /// Corrections still queued.
    pub pending: usize,
}

/// The staging worker pass's step for the queued corrections (see the module
/// doc's "Scheduling"): for each correction and each definition that reads
/// its source (an aggregate over it, or one reading it through a to-many
/// relationship) and applies (`live`, `catching_up`, or `backfilling` under a
/// Re-derive build: `defs::model::APPLYING_SQL`) and has not re-derived it,
/// enqueues the correction's row unless one is there, moving a `live`
/// definition to `backfilling`. A `catching_up` one gets its row once it is
/// `live`. Takes a correction off the queue once every one of them has
/// re-derived it.
///
/// A definition that is frozen (paused, quarantined) or still waiting to
/// build gets no row: its resume or its build reads the source afresh. One
/// whose build is on the old path (`backfilling` without a Re-derive build)
/// holds every correction of its source in the queue until it applies: its
/// chunks may have read the node's rows before the operation, and nothing
/// after them reads those keys again.
pub async fn schedule(pool: &Pool) -> Result<Scheduled, CorrectionError> {
    let mut out = Scheduled::default();
    let tables: Vec<String> = {
        let client = pool.get().await?;
        client
            .query(
                "select distinct table_name from partition_corrections order by 1",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    };
    for table in tables {
        let corrections = {
            let client = pool.get().await?;
            partitions::pending_corrections(&**client, &table).await?
        };
        if corrections.is_empty() {
            continue;
        }
        let (readers, held) = all_readers(pool, &table).await?;
        for correction in &corrections {
            let mut drained = !held && !kept_by_frozen(pool, correction.id).await?;
            for &id in &readers {
                if done(pool, correction.id, id).await? {
                    continue;
                }
                drained = false;
                if enqueue(pool, id, correction.id).await? {
                    out.enqueued += 1;
                }
            }
            if drained {
                let client = pool.get().await?;
                partitions::complete_correction(&**client, correction.id).await?;
                out.completed += 1;
            } else {
                out.pending += 1;
            }
        }
    }
    Ok(out)
}

/// Whether `definition_id` has re-derived correction `correction_id`.
async fn done(
    pool: &Pool,
    correction_id: i64,
    definition_id: i64,
) -> Result<bool, CorrectionError> {
    let client = pool.get().await?;
    Ok(progress(&**client, correction_id, definition_id)
        .await?
        .is_some_and(|p| p.phase() == Phase::Done.as_str()))
}

/// Enqueues correction `correction_id`'s row for `definition_id` unless the
/// definition is not `live` or `backfilling` under a Re-derive build (one
/// `catching_up` waits for its catch-up), or a current row of it is there (one held
/// across a resume, [`chunk_queue::STALE`], is not current: its runner writes
/// nothing). Moves a `live` definition to `backfilling` under a Re-derive
/// build in the same transaction, under its row lock, which every enqueue,
/// pause and resume takes. Returns whether it enqueued.
async fn enqueue(
    pool: &Pool,
    definition_id: i64,
    correction_id: i64,
) -> Result<bool, CorrectionError> {
    let current_sql = format!(
        "select exists (select 1 from backfill_chunks bc \
                        join transform_definitions d on d.id = bc.definition_id \
                        where bc.definition_id = $1 and bc.correction_id = $2 \
                          and not bc.done and not ({}))",
        chunk_queue::STALE
    );
    let mut client = pool.get().await?;
    // Every pass asks again while the correction runs: a row already there
    // needs no lock on the definition's row, which drain pages share.
    let current: bool = client
        .query_one(&current_sql, &[&definition_id, &correction_id])
        .await?
        .get(0);
    if current {
        return Ok(false);
    }
    let txn = client.transaction().await?;
    let applies = txn
        .query_opt(
            "select status from transform_definitions \
             where id = $1 and (status = $2 or (status = $3 and build = $4)) for update",
            &[
                &definition_id,
                &TransformStatus::Live.as_str(),
                &TransformStatus::Backfilling.as_str(),
                &build::BUILD_REDERIVE,
            ],
        )
        .await?;
    let Some(row) = applies else {
        txn.rollback().await?;
        return Ok(false);
    };
    let status: String = row.get(0);
    // Again under the lock, which every enqueue takes.
    let current: bool = txn
        .query_one(&current_sql, &[&definition_id, &correction_id])
        .await?
        .get(0);
    if current {
        txn.rollback().await?;
        return Ok(false);
    }
    txn.execute(
        "insert into backfill_chunks (definition_id, kind, correction_id, fuse_rearmed_at) \
         select id, $2, $3, fuse_rearmed_at from transform_definitions where id = $1",
        &[
            &definition_id,
            &chunk_queue::KIND_CORRECTION,
            &correction_id,
        ],
    )
    .await?;
    let flipped = status == TransformStatus::Live.as_str();
    if flipped {
        txn.execute(
            "update transform_definitions set status = $2, build = $3 where id = $1",
            &[
                &definition_id,
                &TransformStatus::Backfilling.as_str(),
                &build::BUILD_REDERIVE,
            ],
        )
        .await?;
    }
    txn.commit().await?;
    if flipped {
        crate::instance_log::info!(
            definition_id,
            correction_id,
            from = %TransformStatus::Live.as_str(),
            to = %TransformStatus::Backfilling.as_str(),
            "transform status transition: a partition correction started"
        );
    }
    Ok(true)
}

/// Takes `correction` off the queue if every definition reading its source
/// that applies has corrected it, no old-path build holds it and no frozen
/// definition keeps it ([`kept_by_frozen`]): [`schedule`]'s test. Returns
/// whether it did.
async fn complete_if_drained(
    pool: &Pool,
    correction: &PendingCorrection,
) -> Result<bool, CorrectionError> {
    let (ids, held) = all_readers(pool, &correction.table).await?;
    if held || kept_by_frozen(pool, correction.id).await? {
        return Ok(false);
    }
    for id in ids {
        if !done(pool, correction.id, id).await? {
            return Ok(false);
        }
    }
    let client = pool.get().await?;
    partitions::complete_correction(&**client, correction.id).await?;
    Ok(true)
}

/// Whether a definition that no longer applies (paused, quarantined) has an
/// undone row of correction `correction_id`. Such a row is the record of why
/// its definition was paused (its charged failures, which `Trellis::status`
/// reports), or of a correction an operator's pause stopped. The correction
/// stays queued with it, and the row with the correction, until the
/// definition's resume deletes the row and rebuilds the definition from its
/// source.
async fn kept_by_frozen(pool: &Pool, correction_id: i64) -> Result<bool, CorrectionError> {
    let client = pool.get().await?;
    Ok(client
        .query_one(
            &format!(
                "select exists (select 1 from backfill_chunks bc \
                                join transform_definitions d on d.id = bc.definition_id \
                                where bc.correction_id = $1 and not bc.done \
                                  and not {})",
                crate::defs::model::applying_sql("d")
            ),
            &[&correction_id],
        )
        .await?
        .get(0))
}

/// Runs a claimed correction row (`ChunkWork::Correction`) for its
/// definition, from its committed progress, for up to `options.max_pages`
/// pages in all of `outcome`, then gives the claim back: done once the key
/// set is re-derived (then the correction leaves the queue if it was the
/// last, and [`build::try_complete`] may move the definition `live`), for a
/// later claim when it stopped at the budget, or after [`WAIT_RETRY`] while
/// it waits for a detach or its fence. A row held across a resume, or whose
/// claim was reclaimed, writes nothing. An error leaves the claim to the
/// caller's `fail_chunk`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_chunk(
    pool: &Pool,
    chunk: &ClaimedChunk,
    correction_id: i64,
    claimed_by: &str,
    heartbeat_interval: Duration,
    reclaim_ttl: Duration,
    options: &Options,
    outcome: &mut Outcome,
) -> Result<(), ChunkQueueError> {
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    let correction = {
        let client = pool.get().await?;
        // The correction's source: a to-many reader's own source is the
        // relationship's from-side, not the partitioned root.
        let table: Option<String> = client
            .query_opt(
                "select table_name from partition_corrections where id = $1",
                &[&correction_id],
            )
            .await?
            .map(|row| row.get(0));
        match table {
            Some(table) => partitions::pending_corrections(&**client, &table)
                .await?
                .into_iter()
                .find(|c| c.id == correction_id),
            None => None,
        }
    };
    let Some(correction) = correction else {
        // The correction left the queue, and its rows with it.
        return Ok(());
    };
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, reclaim_ttl);
    let step = if definition.source_table == correction.table {
        correct(pool, &definition, &correction, options, outcome, &fence).await?
    } else if let Some(reader) = to_many::reader(pool, &definition, &correction.table).await? {
        to_many::correct(pool, &reader, &correction, options, outcome, &fence).await?
    } else if mark_done(pool, &fence, &correction, &definition).await? {
        // It no longer reads the source: nothing of it to correct.
        Step::Done
    } else {
        Step::Superseded
    };
    match step {
        Step::Done => {
            if !finish_row(pool, &fence, chunk.id).await? {
                return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
            }
            if complete_if_drained(pool, &correction).await? {
                outcome.completed += 1;
            }
            build::try_complete(pool, chunk.definition_id).await?;
            Ok(())
        }
        Step::Waiting => build::defer_claim(pool, chunk, claimed_by, WAIT_RETRY).await,
        Step::Stopped => build::defer_claim(pool, chunk, claimed_by, Duration::ZERO).await,
        Step::Superseded => chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await,
    }
}

/// Marks the correction row `id` done under its fence. Returns `false`, with
/// nothing written, when the fence no longer holds.
async fn finish_row(pool: &Pool, fence: &ClaimFence<'_>, id: i64) -> Result<bool, CorrectionError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(false);
    }
    txn.execute(
        "update backfill_chunks set done = true, claimed_by = null, claimed_at = null \
         where id = $1",
        &[&id],
    )
    .await?;
    txn.commit().await?;
    Ok(true)
}

/// Runs every queued correction to its end, or to `options.max_pages` pages
/// in all, in this task: [`schedule`], then claims and runs every claimable
/// correction row as a drain worker would ([`run_chunk`]), then
/// [`schedule`] again. The synchronous stand-in for the staging worker and
/// the drain workers, for a test with neither running. `completed` and
/// `remaining` count the queue across every source.
///
/// A row that fails is given its claim back at once and the error returned;
/// a row that waits (a detach pending, a fence not settled) is left for a
/// later call, which tries it again at once.
#[cfg(any(test, feature = "internals"))]
pub async fn run_pending(pool: &Pool, options: &Options) -> Result<Outcome, ChunkQueueError> {
    const CLAIMED_BY: &str = "run_pending_corrections";
    let mut outcome = Outcome::default();
    schedule(pool).await?;
    {
        // A row that waited in an earlier call is tried again now: what it
        // waited for may have happened since, not on its backoff's clock.
        let client = pool.get().await?;
        client
            .execute(
                "update backfill_chunks set next_attempt_at = null \
                 where kind = $1 and not done and claimed_by is null and last_error is null",
                &[&chunk_queue::KIND_CORRECTION],
            )
            .await?;
    }
    while !budget_spent(options, &outcome) {
        let claimed = {
            let client = pool.get().await?;
            chunk_queue::claim_chunks_of(&**client, CLAIMED_BY, 1, &[chunk_queue::KIND_CORRECTION])
                .await?
        };
        let Some(chunk) = claimed.into_iter().next() else {
            break;
        };
        let chunk_queue::ChunkWork::Correction { correction_id } = chunk.work else {
            unreachable!("claimed only correction rows");
        };
        let ran = run_chunk(
            pool,
            &chunk,
            correction_id,
            CLAIMED_BY,
            Duration::from_secs(5),
            Duration::from_secs(60),
            options,
            &mut outcome,
        )
        .await;
        if let Err(err) = ran {
            build::defer_claim(pool, &chunk, CLAIMED_BY, Duration::ZERO).await?;
            return Err(err);
        }
    }
    let scheduled = schedule(pool).await?;
    outcome.completed += scheduled.completed;
    outcome.remaining = scheduled.pending;
    Ok(outcome)
}

fn budget_spent(options: &Options, outcome: &Outcome) -> bool {
    options.max_pages.is_some_and(|max| outcome.pages >= max)
}

/// The ids of the aggregate definitions reading `source_table` that apply,
/// and whether one is under an old-path build (`backfilling` without a
/// Re-derive build), which holds the corrections in the queue.
async fn readers(
    client: &impl GenericClient,
    source_table: &str,
) -> Result<(Vec<i64>, bool), CorrectionError> {
    // One statement, so both answers come from one snapshot: an old-path
    // build that moves to `catching_up` between two reads would be in
    // neither, and a correction completed then would never re-derive it.
    let row = client
        .query_one(
            &format!(
                "select coalesce(array_agg(id order by id) filter (where {APPLYING_SQL}), \
                                 '{{}}'::bigint[]), \
                        coalesce(bool_or(status = $2 and build is distinct from 'rederive'), \
                                 false) \
                 from transform_definitions where source_table = $1"
            ),
            &[&source_table, &TransformStatus::Backfilling.as_str()],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

/// The ids of every definition reading `source_table` that applies (the
/// aggregates over it and the definitions reading it through a to-many
/// relationship), and whether one of them is under an old-path build, which
/// holds the corrections in the queue.
async fn all_readers(pool: &Pool, source_table: &str) -> Result<(Vec<i64>, bool), CorrectionError> {
    let (mut ids, held) = {
        let client = pool.get().await?;
        readers(&**client, source_table).await?
    };
    let (to_many, to_many_held) = to_many::reader_ids(pool, source_table).await?;
    ids.extend(to_many);
    ids.sort_unstable();
    ids.dedup();
    Ok((ids, held || to_many_held))
}

/// What one call of [`correct`] left.
enum Step {
    /// The definition re-derived the correction's key set.
    Done,
    /// It waits for a detach to finalize or for the fence to settle.
    Waiting,
    /// The page budget ran out.
    Stopped,
    /// The row's fence no longer holds: its claim was reclaimed, or it was
    /// held across a resume. Nothing was written.
    Superseded,
}

/// Runs correction `correction` for `definition` page by page, resuming from
/// its progress, each write under `fence`.
async fn correct(
    pool: &Pool,
    definition: &Definition,
    correction: &PendingCorrection,
    options: &Options,
    outcome: &mut Outcome,
    fence: &ClaimFence<'_>,
) -> Result<Step, CorrectionError> {
    let Some(plan) = BuildPlan::for_definition(pool, definition).await? else {
        // A shape the Re-derive build does not serve yet (an aggregate that
        // reads a relationship): this engine cannot re-derive its keys. The
        // failure is the row's to report, rather than a correction that
        // completes uncorrected.
        return Err(CorrectionError::Unserved {
            target: definition.target_table.clone(),
        });
    };
    let ledger = plan.ledger();
    if covered_by_parent(pool, correction).await? {
        return Ok(if mark_done(pool, fence, correction, definition).await? {
            Step::Done
        } else {
            Step::Superseded
        });
    }
    if correction.kind == partitions::Kind::Complement.as_str()
        && let Some(text) = &correction.constraint
    {
        // A complement names each `HASH` node it reads remainders of, and
        // can't be evaluated once one is dropped. The dropped node's own
        // correction covers its groups, and the next pass's sweep finds any
        // others again under the tree as it is then.
        let mut oids: Vec<i64> = crate::capture::complement::hash_parents(text)
            .into_iter()
            .map(i64::from)
            .collect();
        oids.sort_unstable();
        oids.dedup();
        let client = pool.get().await?;
        let alive: i64 = client
            .query_one(
                "select count(*) from pg_catalog.pg_class \
                 where oid = any($1::bigint[]::oid[])",
                &[&oids],
            )
            .await?
            .get(0);
        if usize::try_from(alive).unwrap_or(0) < oids.len() {
            return Ok(if mark_done(pool, fence, correction, definition).await? {
                Step::Done
            } else {
                Step::Superseded
            });
        }
    }
    let recorded = {
        let client = pool.get().await?;
        partitions::recorded(&**client, &correction.table).await?
    };
    let route = route_of(correction, recorded.as_ref());
    let columns = route_columns(ledger, recorded.as_ref(), "l");
    let group_types = group_types(pool, ledger).await?;

    loop {
        if budget_spent(options, outcome) {
            return Ok(Step::Stopped);
        }
        let current = {
            let client = pool.get().await?;
            progress(&**client, correction.id, definition.id).await?
        };
        let Some(current) = current else {
            match start(pool, definition, correction, fence).await? {
                Started::Recorded => continue,
                Started::Pending => return Ok(Step::Waiting),
                Started::Superseded => return Ok(Step::Superseded),
            }
        };
        let page = match Phase::parse(current.phase) {
            Phase::Done => return Ok(Step::Done),
            Phase::Parents | Phase::Drain => {
                return Err(CorrectionError::Unroutable(format!(
                    "an aggregate's correction {} is in a to-many phase",
                    correction.id
                )));
            }
            Phase::Fence => {
                if !settle_fence(pool, definition, correction).await? {
                    return Ok(Step::Waiting);
                }
                continue;
            }
            Phase::Ledger => {
                ledger_page(
                    pool,
                    ledger,
                    &route,
                    &columns,
                    &group_types,
                    &current,
                    correction,
                    definition,
                    options.page_keys,
                    fence,
                )
                .await?
            }
            Phase::Node => {
                node_page(
                    pool,
                    ledger,
                    &current,
                    correction,
                    definition,
                    options.page_keys,
                    fence,
                )
                .await?
            }
        };
        let Some(keys) = page else {
            return Ok(Step::Superseded);
        };
        outcome.pages += 1;
        outcome.keys += keys;
    }
}

/// Whether `correction` is a gone node whose parent has been dropped since
/// it left the tree. A `HASH` text embeds the parent's oid and cannot be
/// evaluated once the parent is dropped. The parent's own correction, queued
/// by the pass that saw it go, routes every group of its bound, the node's
/// among them, so the node's is complete without work.
async fn covered_by_parent(
    pool: &Pool,
    correction: &PendingCorrection,
) -> Result<bool, CorrectionError> {
    let Some(parent) = correction.parent_oid else {
        return Ok(false);
    };
    if correction.kind != partitions::Kind::GoneNode.as_str() {
        return Ok(false);
    }
    let client = pool.get().await?;
    let alive: bool = client
        .query_one(
            "select exists (select 1 from pg_catalog.pg_class where oid = $1::bigint::oid)",
            &[&i64::from(parent)],
        )
        .await?
        .get(0);
    Ok(!alive)
}

/// Whether `correction` is a gone node whose detach is still pending.
///
/// The node's `pg_inherits` row is there, `inhdetachpending`, until the
/// detach is final. A writer with an older snapshot can still write through
/// the parent into a detach-pending leaf, and `DETACH ... FINALIZE` waits for
/// those writers, so only a node that has left the tree has all its changes
/// in the ring. A node attached again since has a row that is not pending:
/// its correction runs, and every key it corrects reads the source as it is.
async fn detach_pending(
    client: &impl GenericClient,
    correction: &PendingCorrection,
) -> Result<bool, CorrectionError> {
    if correction.kind != partitions::Kind::GoneNode.as_str() {
        return Ok(false);
    }
    Ok(client
        .query_one(
            "select exists (select 1 from pg_catalog.pg_inherits \
                            where inhrelid = $1::bigint::oid and inhdetachpending)",
            &[&u32_param(correction.node_oid)],
        )
        .await?
        .get(0))
}

/// Records `correction` done for `definition` with no work (its parent's
/// correction covers it), under `fence`. Returns `false`, with nothing
/// written, when the fence no longer holds.
async fn mark_done(
    pool: &Pool,
    fence: &ClaimFence<'_>,
    correction: &PendingCorrection,
    definition: &Definition,
) -> Result<bool, CorrectionError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    fence.arm(&*txn).await?;
    // The definition's row before the claim's (`ClaimFence`): the insert's
    // foreign key takes it.
    txn.execute(
        "select 1 from transform_definitions where id = $1 for key share",
        &[&definition.id],
    )
    .await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(false);
    }
    txn.execute(
        "insert into partition_correction_progress (correction_id, definition_id, phase) \
         values ($1, $2, 'done') \
         on conflict (correction_id, definition_id) do update set phase = 'done', cursor = null",
        &[&correction.id, &definition.id],
    )
    .await?;
    txn.commit().await?;
    Ok(true)
}

/// What [`start`] did.
enum Started {
    /// The progress row is recorded.
    Recorded,
    /// The node's detach is still pending.
    Pending,
    /// The row's fence no longer holds.
    Superseded,
}

/// Starts `correction` for `definition`: waits for a pending detach to
/// finish, then stamps the definition's `build_seg` and records the progress
/// row, in one transaction under `fence`.
async fn start(
    pool: &Pool,
    definition: &Definition,
    correction: &PendingCorrection,
    fence: &ClaimFence<'_>,
) -> Result<Started, CorrectionError> {
    let mut client = pool.get().await?;
    if detach_pending(&**client, correction).await? {
        return Ok(Started::Pending);
    }
    let txn = client.transaction().await?;
    fence.arm(&*txn).await?;
    // The definition's row first, as a rebuild's start takes it, then the
    // claim's (`ClaimFence`: resume's order).
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&definition.id],
    )
    .await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(Started::Superseded);
    }
    txn.execute(
        "insert into partition_correction_progress (correction_id, definition_id, phase) \
         values ($1, $2, 'fence') on conflict do nothing",
        &[&correction.id, &definition.id],
    )
    .await?;
    // Last: the share lock that keeps the active segment from sealing is held
    // only for the commit.
    build::stamp_rebuild_seg(&*txn, &[definition.id]).await?;
    txn.commit().await?;
    Ok(Started::Recorded)
}

/// `oid` as the `i64` a text-free bind takes.
fn u32_param(oid: u32) -> i64 {
    i64::from(oid)
}

/// Takes the fence if the progress has none (a transaction id assigned after
/// the stamp committed) and moves to the ledger phase once the oldest running
/// transaction began after it. Returns whether it moved.
async fn settle_fence(
    pool: &Pool,
    definition: &Definition,
    correction: &PendingCorrection,
) -> Result<bool, CorrectionError> {
    let client = pool.get().await?;
    client
        .execute(
            "update partition_correction_progress \
             set fence_xid = coalesce(fence_xid, pg_current_xact_id()) \
             where correction_id = $1 and definition_id = $2 and phase = 'fence'",
            &[&correction.id, &definition.id],
        )
        .await?;
    let moved = client
        .execute(
            "update partition_correction_progress set phase = 'ledger' \
             where correction_id = $1 and definition_id = $2 and phase = 'fence' \
               and pg_snapshot_xmin(pg_current_snapshot()) > fence_xid",
            &[&correction.id, &definition.id],
        )
        .await?;
    Ok(moved > 0)
}

/// The ledger's group columns' types, in `plain_group_cols` order, as the
/// cursor's binds cast to them.
async fn group_types(
    pool: &Pool,
    ledger: &LedgerTargetPlan,
) -> Result<Vec<String>, CorrectionError> {
    let client = pool.get().await?;
    let names: Vec<String> = ledger
        .plain_group_cols()
        .into_iter()
        .map(str::to_string)
        .collect();
    let rows = client
        .query(
            "select a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod) \
             from pg_catalog.pg_attribute a \
             where a.attrelid = $1::text::regclass and a.attname::text = any($2) \
               and not a.attisdropped",
            &[&ledger.ledger_ident(), &names],
        )
        .await?;
    names
        .iter()
        .map(|name| {
            rows.iter()
                .find(|r| r.get::<_, String>(0) == *name)
                .map(|r| r.get::<_, String>(1))
                .ok_or_else(|| {
                    CorrectionError::Unroutable(format!(
                        "the ledger has no group column {name} for {}",
                        ledger.ledger_ident()
                    ))
                })
        })
        .collect()
}

/// The ledger-phase cursor: the last entry's group values (text, `None` for
/// `NULL`) and key.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LedgerCursor {
    groups: Vec<Option<String>>,
    key: String,
}

/// The predicate selecting entries strictly after `cursor` in
/// `(group columns, key)` order, `NULL` groups last, binding the cursor's
/// values from `$first`, and the seeks on the first group column that the
/// walk reads in turn: a range condition the `GROUP BY` index starts its scan
/// at (`>=` the cursor's value, then `is null`, which sorts last). A single
/// `(>= or is null)` would not be an index condition, so every page would
/// scan the index from its beginning and the walk would read a quadratic
/// number of entries.
fn after_cursor(
    alias: &str,
    groups: &[&str],
    types: &[String],
    cursor: &LedgerCursor,
    first_param: usize,
) -> (String, Vec<String>, Vec<String>) {
    let key = quote_ident(crate::defs::ledger::KEY_COLUMN);
    let mut params: Vec<String> = Vec::new();
    let mut bind = |value: &str, ty: &str| -> String {
        params.push(value.to_string());
        format!("${}::text::{ty}", first_param + params.len() - 1)
    };
    let mut equal: Vec<String> = Vec::new();
    let mut any: Vec<String> = Vec::new();
    for (i, name) in groups.iter().enumerate() {
        let column = format!("{alias}.{}", quote_ident(name));
        match &cursor.groups[i] {
            Some(value) => {
                let bound = bind(value, &types[i]);
                let greater = format!("({column} > {bound} or {column} is null)");
                any.push(and_all(&equal, &greater));
                equal.push(format!("{column} = {bound}"));
            }
            None => {
                equal.push(format!("{column} is null"));
            }
        }
    }
    let bound_key = bind(&cursor.key, "text");
    any.push(and_all(&equal, &format!("{alias}.{key} > {bound_key}")));
    let predicate = format!("({})", any.join(" or "));
    let seeks = match groups.first() {
        Some(first) => {
            let column = format!("{alias}.{}", quote_ident(first));
            match &cursor.groups[0] {
                // The first group's bind is the first parameter.
                Some(_) => vec![
                    format!("{column} >= ${first_param}::text::{}", types[0]),
                    format!("{column} is null"),
                ],
                None => vec![format!("{column} is null")],
            }
        }
        None => vec!["true".to_string()],
    };
    (predicate, params, seeks)
}

fn and_all(parts: &[String], last: &str) -> String {
    if parts.is_empty() {
        return last.to_string();
    }
    format!("({} and {last})", parts.join(" and "))
}

/// Reads one page of the ledger phase's keys and re-derives them, then
/// commits the progress with them, under `fence`. Returns the keys
/// re-derived, or `None`, with nothing written, when the fence no longer
/// holds.
#[allow(clippy::too_many_arguments)]
async fn ledger_page(
    pool: &Pool,
    ledger: &LedgerTargetPlan,
    route: &Route,
    columns: &[RouteColumn],
    types: &[String],
    current: &Progress,
    correction: &PendingCorrection,
    definition: &Definition,
    page_keys: usize,
    fence: &ClaimFence<'_>,
) -> Result<Option<i64>, CorrectionError> {
    let groups = ledger.plain_group_cols();
    let key = quote_ident(crate::defs::ledger::KEY_COLUMN);
    let live = format!(
        "l.{} and not l.{}",
        quote_ident(crate::defs::ledger::MEMBER_COLUMN),
        quote_ident(crate::defs::ledger::TOMBSTONE_COLUMN)
    );
    let predicate = route_predicate(route, columns);
    let cursor: Option<LedgerCursor> = current
        .cursor
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| CorrectionError::Unroutable(format!("an unreadable cursor: {e}")))?;
    let (after, bound, seeks) = match &cursor {
        Some(cursor) => after_cursor("l", &groups, types, cursor, 2),
        None => ("true".to_string(), Vec::new(), vec!["true".to_string()]),
    };
    let group_list: Vec<String> = groups
        .iter()
        .map(|g| format!("l.{}", quote_ident(g)))
        .collect();
    let group_text: Vec<String> = group_list.iter().map(|g| format!("{g}::text")).collect();
    let order = if group_list.is_empty() {
        format!("l.{key}")
    } else {
        format!("{}, l.{key}", group_list.join(", "))
    };
    let limit = i64::try_from(page_keys.max(1)).unwrap_or(i64::MAX);
    let selects: Vec<String> = seeks
        .iter()
        .map(|seek| {
            format!(
                "select l.{key}{}{} from {ledger} l \
                 where {live} and {predicate} and {after} and {seek} \
                 order by {order} limit $1",
                if group_text.is_empty() { "" } else { ", " },
                group_text.join(", "),
                ledger = ledger.ledger_ident(),
            )
        })
        .collect();
    let count = format!(
        "select count(*) from {ledger} l where {live} and {predicate}",
        ledger = ledger.ledger_ident(),
    );

    // Read the page under the pinned settings, in a transaction of its own:
    // the settings change what the constraint text means, and the
    // re-derive below must run under the session's own.
    let mut client = pool.get().await?;
    let (rows, total) = {
        let txn = client
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await?;
        txn.batch_execute(&partitions::pinned_settings()).await?;
        txn.batch_execute(WALK_PLAN_SETTINGS).await?;
        // Each seek's entries follow the previous one's in the walk's order,
        // so the page is the first `limit` of them read in turn.
        let mut rows = Vec::new();
        for select in &selects {
            let want = limit - i64::try_from(rows.len()).unwrap_or(i64::MAX);
            if want <= 0 {
                break;
            }
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&want];
            for value in &bound {
                params.push(value);
            }
            rows.extend(txn.query(select, &params).await?);
        }
        let total: Option<i64> = if current.keys_total.is_none() {
            Some(txn.query_one(&count, &[]).await?.get(0))
        } else {
            None
        };
        txn.commit().await?;
        (rows, total)
    };
    let found: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    let last = rows.last().map(|r| LedgerCursor {
        groups: (0..groups.len()).map(|i| r.get(i + 1)).collect(),
        key: r.get(0),
    });
    let finished = rows.len() < page_keys.max(1);

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
        return Ok(None);
    }
    let keys = unpoisoned(&txn, ledger, &found).await?;
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    if !refs.is_empty() {
        build::rederive_keys(&txn, ledger, &refs).await?;
    }
    let next_cursor = match (&last, finished) {
        (Some(last), false) => Some(serde_json::to_string(last).expect("a cursor serializes")),
        _ => None,
    };
    let (phase, total_estimate) = if !finished {
        (Phase::Ledger, 0)
    } else if reads_node(correction, route) {
        (Phase::Node, node_estimate(&txn, correction).await?)
    } else {
        (Phase::Done, 0)
    };
    // The node phase's keys join the total once the ledger phase is done.
    txn.execute(
        "update partition_correction_progress set phase = $3, cursor = $4, \
             keys_done = keys_done + $5::bigint, \
             keys_total = coalesce(keys_total, 0) + coalesce($6::bigint, 0) + $7::bigint \
         where correction_id = $1 and definition_id = $2",
        &[
            &correction.id,
            &definition.id,
            &phase.as_str(),
            &next_cursor,
            &i64::try_from(found.len()).unwrap_or(i64::MAX),
            &total,
            &total_estimate,
        ],
    )
    .await?;
    pause_before_commit(&txn, definition).await?;
    txn.commit().await?;
    Ok(Some(i64::try_from(keys.len()).unwrap_or(i64::MAX)))
}

/// The test hook before a page commits ([`super::interleave`]'s
/// `BeforeCorrectionCommit`), for the definition's target.
async fn pause_before_commit(
    txn: &tokio_postgres::Transaction<'_>,
    definition: &Definition,
) -> Result<(), tokio_postgres::Error> {
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::BeforeCorrectionCommit,
        &definition.target_table,
    )
    .await?;
    #[cfg(not(any(test, feature = "test-util")))]
    let _ = (txn, definition);
    Ok(())
}

/// Whether the correction walks the node's rows after the ledger: a node in
/// the tree (new, attached, or a `DEFAULT` whose siblings changed), whose
/// rows may have no entry yet, and a gone node without a usable text, which
/// routes no entry. A gone node routed by its text needs no node phase: every
/// row it held satisfied that text while it was in the tree, so the ledger
/// phase found every live entry of its rows, and reading a detached table's
/// rows would only rewrite those tombstones again. A complement has no node.
fn reads_node(correction: &PendingCorrection, route: &Route) -> bool {
    if correction.kind == partitions::Kind::Complement.as_str() {
        return false;
    }
    correction.kind != partitions::Kind::GoneNode.as_str() || *route == Route::Unusable
}

/// The ledger walk's planner settings: an ordered scan of the `GROUP BY`
/// index from the seek, sorted by key only within a group, which stops after
/// a page of entries. On a ledger whose statistics lag (a fresh build's), the
/// planner otherwise picks a bitmap scan of every entry past the seek and a
/// sort of them all for each page, which made the walk of a 1M-entry leaf
/// read 800,000 entries a page at its start: quadratic in the node.
const WALK_PLAN_SETTINGS: &str = "set local enable_seqscan = off; \
     set local enable_bitmapscan = off; set local enable_sort = off";

/// The keys of `found` that are not quarantined for the definition, as a
/// build leaves them out (`poison`).
async fn unpoisoned(
    txn: &tokio_postgres::Transaction<'_>,
    ledger: &LedgerTargetPlan,
    found: &[String],
) -> Result<Vec<String>, CorrectionError> {
    if found.is_empty() {
        return Ok(Vec::new());
    }
    Ok(txn
        .query(
            "select k from unnest($1::text[]) as u(k) \
             where not exists (select 1 from poison p \
                               join transform_definitions d on d.id = p.transform_id \
                               where p.src_table = $2 and p.key = u.k and d.target_table = $3) \
             order by k",
            &[&found, &ledger.source_table(), &ledger.target],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// The rows the node phase expects, by the planner's estimate (`reltuples`,
/// zero for a table nothing has analyzed or a node that is gone).
async fn node_estimate(
    txn: &tokio_postgres::Transaction<'_>,
    correction: &PendingCorrection,
) -> Result<i64, CorrectionError> {
    let estimate: Option<f32> = txn
        .query_opt(
            "select c.reltuples from pg_catalog.pg_class c where c.oid = $1::bigint::oid",
            &[&u32_param(correction.node_oid)],
        )
        .await?
        .map(|row| row.get(0));
    #[allow(clippy::cast_possible_truncation)]
    Ok(estimate.map_or(0, |e| e.max(0.0) as i64))
}

/// Reads one page of the node phase's keys from the node itself and
/// re-derives them, committing the progress with them, under `fence`.
/// Returns the keys re-derived, or `None`, with nothing written, when the
/// fence no longer holds.
async fn node_page(
    pool: &Pool,
    ledger: &LedgerTargetPlan,
    current: &Progress,
    correction: &PendingCorrection,
    definition: &Definition,
    page_keys: usize,
    fence: &ClaimFence<'_>,
) -> Result<Option<i64>, CorrectionError> {
    let mut client = pool.get().await?;
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
        return Ok(None);
    }
    // The node by oid, so a rename doesn't lose it; none once it is dropped.
    // Qualified whatever the search path is.
    let node: Option<String> = txn
        .query_opt(
            "select pg_catalog.format('%I.%I', n.nspname, c.relname) \
             from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where c.oid = $1::bigint::oid and c.relkind in ('r', 'p')",
            &[&u32_param(correction.node_oid)],
        )
        .await?
        .map(|row| row.get(0));
    let Some(node) = node else {
        finish(&txn, correction, definition, 0, None, true).await?;
        txn.commit().await?;
        return Ok(Some(0));
    };
    // A gone node read here has no usable text (`reads_node`): its table,
    // once detached, is read the same way, and each key re-derived reads the
    // source, finding no row for it.
    let pk = ledger.source_pk();
    let idents: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    let cursor_parts: Option<Vec<String>> = match current.cursor.as_deref() {
        Some(cursor) => Some(
            ddl::split_pk_key(pk, ledger.source_table(), cursor)
                .map_err(|e| CorrectionError::Unroutable(format!("an unreadable cursor: {e}")))?
                .into_iter()
                .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
                .collect(),
        ),
        None => None,
    };
    let after = if cursor_parts.is_some() {
        backfill::pk_row_cmp(&idents, pk, ">", 0)
    } else {
        "true".to_string()
    };
    let order: Vec<String> = pk
        .iter()
        .zip(&idents)
        .map(|(c, ident)| c.ordered(ident))
        .collect();
    let limit = i64::try_from(page_keys.max(1)).unwrap_or(i64::MAX);
    let limit_param = cursor_parts.as_ref().map_or(0, Vec::len) + 1;
    let sql = format!(
        "select {k} from {node} s where {after} and {not_null} order by {order} limit ${limit_param}",
        k = ddl::pk_key_sql_expr(pk, Some("s")),
        not_null = backfill::key_not_null(&idents),
        order = order.join(", "),
    );
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
    if let Some(parts) = &cursor_parts {
        for part in parts {
            params.push(part);
        }
    }
    params.push(&limit);
    let found: Vec<String> = txn
        .query(&sql, &params)
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let finished = found.len() < page_keys.max(1);
    let keys = unpoisoned(&txn, ledger, &found).await?;
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    if !refs.is_empty() {
        build::rederive_keys(&txn, ledger, &refs).await?;
    }
    let next = if finished {
        None
    } else {
        found.last().cloned()
    };
    finish(
        &txn,
        correction,
        definition,
        i64::try_from(found.len()).unwrap_or(i64::MAX),
        next.as_deref(),
        finished,
    )
    .await?;
    pause_before_commit(&txn, definition).await?;
    txn.commit().await?;
    Ok(Some(i64::try_from(keys.len()).unwrap_or(i64::MAX)))
}

/// Commits a node-phase page's progress: `done` keys more, the cursor, and
/// the phase `done` when the walk is.
async fn finish(
    txn: &tokio_postgres::Transaction<'_>,
    correction: &PendingCorrection,
    definition: &Definition,
    done: i64,
    cursor: Option<&str>,
    finished: bool,
) -> Result<(), CorrectionError> {
    txn.execute(
        "update partition_correction_progress set phase = $3, cursor = $4, \
             keys_done = keys_done + $5::bigint \
         where correction_id = $1 and definition_id = $2",
        &[
            &correction.id,
            &definition.id,
            &if finished {
                Phase::Done.as_str()
            } else {
                Phase::Node.as_str()
            },
            &cursor,
            &done,
        ],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, ty: &str, collation: &str) -> RouteColumn {
        RouteColumn {
            name: name.to_string(),
            value: format!("l.{}", quote_ident(name)),
            key: Some(Key {
                node: 1,
                column: Some(name.to_string()),
                pg_type: ty.to_string(),
                collation: collation.to_string(),
                level_collation: collation.to_string(),
            }),
        }
    }

    #[test]
    fn a_text_is_evaluated_over_the_group_cast_to_the_keys_type_and_collation() {
        let sql = route_predicate(
            &Route::Text("(region = ANY (ARRAY['a'::text, 'b'::text]))".to_string()),
            &[column("region", "text", "pg_catalog.default")],
        );
        assert_eq!(
            sql,
            "exists (select 1 from (select ((l.\"region\")::text collate \
             \"pg_catalog\".\"default\") as \"region\") as __route \
             where (region = ANY (ARRAY['a'::text, 'b'::text])))"
        );
    }

    #[test]
    fn a_default_without_siblings_routes_everything_and_no_text_routes_nothing() {
        let cols = [column("a", "integer", "")];
        assert_eq!(route_predicate(&Route::Every, &cols), "true");
        assert_eq!(route_predicate(&Route::Unusable, &cols), "false");
    }

    #[test]
    fn a_column_without_a_recorded_key_is_presented_as_it_is() {
        let mut c = column("a", "integer", "");
        c.key = None;
        let sql = route_predicate(&Route::Text("(a > 1)".to_string()), &[c]);
        assert_eq!(
            sql,
            "exists (select 1 from (select l.\"a\" as \"a\") as __route where (a > 1))"
        );
    }

    #[test]
    fn a_default_with_no_text_is_every_group_and_a_node_without_one_is_none() {
        let mut correction = PendingCorrection {
            id: 1,
            table: "public.t".to_string(),
            kind: "gone_node".to_string(),
            node_oid: 7,
            node_name: "public.t_d".to_string(),
            cause: "gone".to_string(),
            constraint: None,
            node_default: true,
            parent_oid: None,
            queued_lsn: "0/1".to_string(),
        };
        assert_eq!(route_of(&correction, None), Route::Every);
        correction.node_default = false;
        assert_eq!(route_of(&correction, None), Route::Unusable);
        correction.constraint = Some("(a = 1)".to_string());
        assert_eq!(
            route_of(&correction, None),
            Route::Text("(a = 1)".to_string())
        );
    }

    #[test]
    fn each_kind_reads_its_node_or_not() {
        let correction = |kind: partitions::Kind| PendingCorrection {
            id: 1,
            table: "public.t".to_string(),
            kind: kind.as_str().to_string(),
            node_oid: 7,
            node_name: "public.t_7".to_string(),
            cause: partitions::Cause::Truncated.as_str().to_string(),
            constraint: Some("(a = 1)".to_string()),
            node_default: false,
            parent_oid: None,
            queued_lsn: "0/1".to_string(),
        };
        let text = Route::Text("(a = 1)".to_string());
        for route in [&text, &Route::Every, &Route::Unusable] {
            assert!(reads_node(&correction(partitions::Kind::NewLeaf), route));
            assert!(reads_node(&correction(partitions::Kind::Truncated), route));
            assert!(!reads_node(
                &correction(partitions::Kind::Complement),
                route
            ));
        }
        assert!(!reads_node(&correction(partitions::Kind::GoneNode), &text));
        assert!(reads_node(
            &correction(partitions::Kind::GoneNode),
            &Route::Unusable
        ));
    }

    #[test]
    fn a_truncated_node_routes_by_its_record() {
        let node = |oid: u32, constraint: Option<&str>, is_default: bool| partitions::Node {
            oid,
            parent: Some(1),
            name: format!("public.t_{oid}"),
            partitioned: false,
            is_default,
            bound: Some("DEFAULT".to_string()),
            inherits_xmin: Some("1".to_string()),
            constraint: constraint.map(str::to_string),
        };
        let recorded = Recorded {
            nodes: vec![
                partitions::Node {
                    parent: None,
                    bound: None,
                    inherits_xmin: None,
                    ..node(1, None, false)
                },
                node(7, None, true),
                node(8, Some("(a = 1)"), false),
            ],
            keys: Vec::new(),
        };
        // What the seal queues: no text and no `DEFAULT` flag of its own.
        let mut correction = PendingCorrection {
            id: 1,
            table: "public.t".to_string(),
            kind: partitions::Kind::Truncated.as_str().to_string(),
            node_oid: 7,
            node_name: "public.t_7".to_string(),
            cause: partitions::Cause::Truncated.as_str().to_string(),
            constraint: None,
            node_default: false,
            parent_oid: None,
            queued_lsn: "0/1".to_string(),
        };
        assert_eq!(route_of(&correction, Some(&recorded)), Route::Every);
        correction.node_oid = 8;
        assert_eq!(
            route_of(&correction, Some(&recorded)),
            Route::Text("(a = 1)".to_string())
        );
        correction.node_oid = 9;
        assert_eq!(route_of(&correction, Some(&recorded)), Route::Unusable);
    }

    #[test]
    fn the_cursor_predicate_orders_groups_then_key_with_nulls_last() {
        let cursor = LedgerCursor {
            groups: vec![Some("5".to_string()), None],
            key: "k9".to_string(),
        };
        let (sql, params, seeks) = after_cursor(
            "l",
            &["a", "b"],
            &["integer".to_string(), "text".to_string()],
            &cursor,
            2,
        );
        assert_eq!(params, vec!["5".to_string(), "k9".to_string()]);
        assert_eq!(
            sql,
            "((l.\"a\" > $2::text::integer or l.\"a\" is null) \
             or (l.\"a\" = $2::text::integer and l.\"b\" is null and l.\"__from_key\" > $3::text::text))"
        );
        assert_eq!(
            seeks,
            ["l.\"a\" >= $2::text::integer", "l.\"a\" is null"],
            "a seek the index can start at, then the NULLs, which sort last"
        );
        let null_first = LedgerCursor {
            groups: vec![None, Some("x".to_string())],
            key: "k1".to_string(),
        };
        let (_, _, seeks) = after_cursor(
            "l",
            &["a", "b"],
            &["integer".to_string(), "text".to_string()],
            &null_first,
            2,
        );
        assert_eq!(seeks, ["l.\"a\" is null"]);
    }

    /// Each node's route over a probe row, under [`route_columns_named`],
    /// against the leaf Postgres routes the row to, for `public.items`. The
    /// probe is presented as a value of `column` (its type, and collation),
    /// as a group column of the source's type is.
    async fn route_disagreements(
        client: &mut tokio_postgres::Client,
        ddl: &str,
        column: &str,
        probes: &[&str],
    ) -> Vec<String> {
        client
            .batch_execute(&format!("drop table if exists public.items cascade; {ddl}"))
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
        let (_, _, recorded) = partitions::read_tree_once(client, "public.items")
            .await
            .expect("read the tree")
            .expect("a partitioned root");
        let columns = route_columns_named(&["t"], Some(&recorded), "p");
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&partitions::pinned_settings())
            .await
            .expect("pinned settings");
        let nodes: Vec<(String, Option<String>)> = txn
            .query(
                "select relid::regclass::text, pg_get_partition_constraintdef(relid) \
                 from pg_partition_tree('public.items') where level > 0 order by 1",
                &[],
            )
            .await
            .expect("the nodes' texts")
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        let mut wrong = Vec::new();
        for probe in probes {
            // The node chain Postgres routes the probe through, or none.
            txn.batch_execute("savepoint probe")
                .await
                .expect("savepoint");
            let routed: Vec<String> = match txn
                .query(
                    "with ins as (insert into public.items values ($1::text) returning tableoid) \
                     select a::regclass::text from ins, pg_partition_ancestors(ins.tableoid) a",
                    &[probe],
                )
                .await
            {
                Ok(rows) => rows.iter().map(|r| r.get(0)).collect(),
                Err(e) => {
                    let message = e.as_db_error().map(|d| d.message().to_string());
                    assert!(
                        message
                            .as_deref()
                            .is_some_and(|m| m.contains("no partition")),
                        "{probe:?}: {e}"
                    );
                    Vec::new()
                }
            };
            txn.batch_execute("rollback to savepoint probe")
                .await
                .expect("rollback to savepoint");
            for (node, text) in &nodes {
                let text = text.clone().expect("a non-root node has a text");
                let predicate = route_predicate(&Route::Text(text.clone()), &columns);
                let routes: bool = txn
                    .query_one(
                        &format!(
                            "select {predicate} from (select $1::text::{column} as \"t\") as p"
                        ),
                        &[probe],
                    )
                    .await
                    .unwrap_or_else(|e| panic!("{predicate}: {e}"))
                    .get(0);
                if routes != routed.contains(node) {
                    wrong.push(format!(
                        "{probe:?} on {node}: routed {routes}, Postgres {routed:?}, by {predicate}"
                    ));
                }
            }
        }
        txn.rollback().await.expect("roll back");
        wrong
    }

    /// A column partitioned by two levels of one tree under different
    /// collations routes as Postgres does, whichever level's key
    /// [`route_columns_named`] presents it under, the column's own collation
    /// the same as either level's or as neither's, and a level under the
    /// type's own collation, whose text names none (#1084).
    #[tokio::test]
    async fn a_column_partitioned_under_two_collations_routes_as_postgres_does() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let probes = [
            "A", "B", "Z", "a", "b", "c", "m", "M", "n", "N", "z", "Z", "_", "0", "\u{e9}", "E",
            "aa", "AB", "",
        ];
        let leaves = "create table public.items_m partition of public.items \
                          for values from ('B') to ('n') \
                          partition by range ({middle}); \
                      create table public.items_m0 partition of public.items_m \
                          for values from ('B') to ('L'); \
                      create table public.items_m1 partition of public.items_m \
                          for values from ('L') to ('m'); \
                      create table public.items_md partition of public.items_m default; \
                      create table public.items_lo partition of public.items \
                          for values from (minvalue) to ('B'); \
                      create table public.items_hi partition of public.items \
                          for values from ('n') to (maxvalue);";
        client
            .batch_execute("create domain public.c_text as text collate \"C\"")
            .await
            .expect("a domain");
        let c = "t collate \"C\"";
        let icu = "t collate \"en-US-x-icu\"";
        let default = "t collate \"default\"";
        let mut wrong = Vec::new();
        for column in [
            "text",
            "text collate \"C\"",
            "text collate \"en-US-x-icu\"",
            "public.c_text",
        ] {
            // `t` alone is the column's own collation, and `default` the
            // database's: a level whose text names none when it is the type's.
            for (root, middle) in [
                (c, icu),
                (icu, c),
                (c, "t"),
                ("t", c),
                (icu, "t"),
                ("t", icu),
                (c, default),
                (default, c),
            ] {
                let ddl = format!(
                    "create table public.items (t {column}) partition by range ({root}); {}",
                    leaves.replace("{middle}", middle)
                );
                wrong.extend(route_disagreements(&mut client, &ddl, column, &probes).await);
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}
