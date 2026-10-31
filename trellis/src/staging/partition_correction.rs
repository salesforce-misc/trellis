//! The correction engine for aggregates over a partitioned source (ADR-0017).
//!
//! Partition maintenance (`ATTACH`, `DETACH`, `DROP`, a new leaf written
//! directly) changes a partitioned root's rows without a write any capture
//! trigger sees. The reconcile pass records each such change as a durable
//! correction request ([`crate::capture::partitions`], P3). This module
//! consumes the requests of an **aggregate** that reads the source, or of a
//! `JOIN` that reads it as either side.
//!
//! # What a correction does
//!
//! A correction **recomputes the groups that route to the node** (the
//! `group` module): the target's, those of the ring's undrained
//! images, and the node's current rows', in one transaction, with the
//! recompute's snapshot as each group's basis. It never zeroes a group. A
//! capture install that only adds new nodes to the tree rebuilds no
//! aggregate (`crate::intake::catch_up::park_table_catch_ups`): the nodes'
//! corrections cover them.
//!
//! A `JOIN` reading the source as either side (`super::join`) is
//! corrected the same way, over that side's partial cells (`P` or `M`)
//! instead of groups: each cell's key holds every partition-key column
//! (ADR-0019), and its recompute runs through the `J`-locked writer, so the
//! cell's change reaches the target in the same transaction.
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
//! A change staged before the operation and applied after the correction
//! must not resurrect a member the correction removed. The recompute's
//! snapshot is each group's basis, so a page that later carries such a
//! change finds it visible in the basis and skips it (the `group` module's
//! doc). A `gone_node` whose `pg_inherits` row is still there (a detach
//! pending) waits for `DETACH ... FINALIZE`, which waits out the writers
//! with older snapshots, before it starts: only then is the ring complete
//! for the node.
//!
//! # Scheduling
//!
//! A correction runs for each definition that reads the source (an aggregate
//! over it, or a `JOIN` reading it as either side) as a `backfill_chunks` row
//! of kind `correction`, one per correction and definition, which the
//! staging worker's pass enqueues ([`schedule`]) right after the capture pass
//! that queued the correction. The row gives it what a
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
//! it back for the next claim, and [`super::build::try_complete`] moves it
//! back to `live` once the row is done, as it does at the end of a build.
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

use std::fmt;
use std::time::{Duration, SystemTime};

use tokio_postgres::GenericClient;

use crate::capture::partitions::{self, Key, PendingCorrection, Recorded};
use crate::defs::catalog;
use crate::defs::chunk_queue::{self, ChunkQueueError, ClaimFence, ClaimedChunk};
use crate::defs::model::{APPLYING_SQL, Definition, StoredStatus};
use crate::pool::{Pool, quote_ident};

use super::apply::ApplyError;
use super::build::{self, CHUNK_APPLICATION_NAME};

mod group;
pub(crate) use group::lock_recorded_node;

/// Pages a drain worker runs per claim of a correction before it gives the
/// row back, so the worker's segments and other chunks get their turn, and
/// the ring's backlog is checked again before the next claim.
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
    /// Neither the group basis nor a `JOIN` serves the definition's shape,
    /// so its groups cannot be recomputed.
    Unserved {
        target: String,
    },
}

impl fmt::Display for CorrectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorrectionError::Db(err) => {
                write!(f, "partition correction: ")?;
                crate::error::write_pg_error(f, err)
            }
            CorrectionError::Pool(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Apply(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Capture(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Catalog(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Staging(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Unroutable(why) => write!(f, "partition correction: {why}"),
            CorrectionError::Unserved { target } => write!(
                f,
                "partition correction: the group basis does not serve {target}'s shape, \
                 so its groups cannot be recomputed"
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

/// How a correction runs: a drain worker's claim ([`CLAIM_PAGES`] pages), or
/// a test's [`run_pending`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    /// Stops after this many pages in all, leaving the progress committed so
    /// far for the next claim: a worker's turn, or a test's deterministic
    /// stand-in for a crash or a restart in the middle of a correction.
    pub max_pages: Option<usize>,
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

/// A leaf of a partitioned source's recorded tree and how its groups route
/// to it, for a group-basis rebuild's structures
/// ([`super::group_basis::RebuildScope`]).
#[derive(Debug, Clone)]
pub(crate) struct RebuildLeaf {
    /// The leaf's oid, which orders the structures and is the job's cursor.
    pub oid: u32,
    route: Route,
}

/// The leaves of `recorded` in oid order, each with its route by its
/// recorded constraint text. `Err` names the first leaf with no usable
/// route (its text was never fetched, and it is no `DEFAULT` alone under the
/// root, whose empty text routes every group to it), whose groups nothing
/// could be routed to.
pub(crate) fn rebuild_leaves(recorded: &Recorded) -> Result<Vec<RebuildLeaf>, String> {
    let alone = recorded.nodes.len() == 2;
    let mut leaves = Vec::new();
    for node in recorded.nodes.iter().skip(1).filter(|n| !n.partitioned) {
        let route = match &node.constraint {
            Some(text) => Route::Text(text.clone()),
            None if node.is_default && alone => Route::Every,
            None => return Err(node.name.clone()),
        };
        leaves.push(RebuildLeaf {
            oid: node.oid,
            route,
        });
    }
    leaves.sort_by_key(|leaf| leaf.oid);
    Ok(leaves)
}

/// The scope of the rebuild structure for `leaf`: the groups `names` that
/// route to it, and its own rows (`node`, locked by the caller;
/// `read_node` as [`super::group_basis::RebuildScope::read_node`]).
pub(crate) fn leaf_scope(
    names: &[&str],
    recorded: &Recorded,
    leaf: &RebuildLeaf,
    node: Option<String>,
    read_node: bool,
) -> super::group_basis::RebuildScope {
    let on = |alias: &str| {
        route_predicate(
            &leaf.route,
            &route_columns_named(names, Some(recorded), alias),
        )
    };
    super::group_basis::RebuildScope {
        hidden: on("h"),
        target: on("t"),
        images: on("s"),
        node,
        read_node,
    }
}

/// The scope of the rebuild structure after the leaves: the groups of the
/// target and the ring that route to none of `leaves`. A leaf dropped since
/// the tree was recorded, a row of the target nothing wrote, and an image
/// of a node gone are the groups here. Each is read through the root.
pub(crate) fn remainder_scope(
    names: &[&str],
    recorded: &Recorded,
    leaves: &[RebuildLeaf],
) -> super::group_basis::RebuildScope {
    let none = |alias: &str| {
        if leaves.is_empty() {
            return "true".to_string();
        }
        let any: Vec<String> = leaves
            .iter()
            .map(|leaf| {
                route_predicate(
                    &leaf.route,
                    &route_columns_named(names, Some(recorded), alias),
                )
            })
            .collect();
        format!("not ({})", any.join(" or "))
    };
    super::group_basis::RebuildScope {
        hidden: none("h"),
        target: none("t"),
        images: none("s"),
        node: None,
        read_node: false,
    }
}

/// [`route_columns`] for the group columns `names`. A column is presented
/// under the first recorded key on it: its type, and its type's own
/// collation ([`Key::collation`]), which every level's text compares under
/// unless it names its level's on the bound.
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
    Done,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::Fence => "fence",
            Phase::Done => "done",
        }
    }

    fn parse(text: &str) -> Phase {
        match text {
            "fence" => Phase::Fence,
            _ => Phase::Done,
        }
    }
}

/// One definition's progress on one correction
/// (`partition_correction_progress`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    phase: &'static str,
    /// The column's cursor; a correction commits in one transaction, so it
    /// is always `None`.
    pub cursor: Option<String>,
    /// Groups (or cells) recomputed, written when the correction commits.
    pub keys_done: i64,
    /// The groups (or cells) the correction recomputes. `None` until it
    /// commits.
    pub keys_total: Option<i64>,
}

impl Progress {
    /// The phase: `fence` or `done`.
    pub fn phase(&self) -> &str {
        self.phase
    }

    /// Keys still to re-derive by the estimate.
    #[cfg_attr(
        not(any(test, feature = "internals")),
        allow(dead_code, reason = "read by tests and embedders through `internals`")
    )]
    pub fn keys_remaining(&self) -> Option<u64> {
        keys_remaining(self.keys_total, self.keys_done)
    }
}

/// Keys still to re-derive by the estimate, whatever the phase: `None` before
/// the first page counted them, and never below zero, since the node phase's
/// total is an estimate.
fn keys_remaining(keys_total: Option<i64>, keys_done: i64) -> Option<u64> {
    keys_total.map(|total| u64::try_from(total.saturating_sub(keys_done)).unwrap_or(0))
}

/// A node a definition is being corrected for: one entry of
/// [`crate::DefinitionStatus::correcting`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CorrectingNode {
    /// The schema-qualified node the correction is for: the leaf or middle
    /// partition that was attached, detached, dropped or truncated, or, for
    /// rows that route to no current leaf, the source's root.
    pub node: String,
    /// When the staging worker's pass queued the correction.
    pub since: SystemTime,
    /// The keys still to re-derive for this definition, from its persisted
    /// progress. `None` until the correction's first page has counted them.
    /// An estimate while it walks the node's own rows.
    pub keys_remaining: Option<u64>,
}

/// The nodes `definition_id` is being corrected for, oldest first: one per
/// current, undone correction row of it (one a resume superseded,
/// [`chunk_queue::STALE`], is not current), while the definition applies. A
/// frozen definition's row is the record of why it froze, not a correction
/// in progress.
pub(crate) async fn correcting(
    client: &impl GenericClient,
    definition_id: i64,
) -> Result<Vec<CorrectingNode>, tokio_postgres::Error> {
    let rows = client
        .query(
            &format!(
                "select c.node_name, c.queued_at, p.keys_total, coalesce(p.keys_done, 0) \
                 from backfill_chunks bc \
                 join transform_definitions d on d.id = bc.definition_id \
                 join partition_corrections c on c.id = bc.correction_id \
                 left join partition_correction_progress p \
                   on p.correction_id = c.id and p.definition_id = d.id \
                 where bc.definition_id = $1 and bc.kind = $2 and not bc.done \
                   and not ({}) and {} \
                 order by c.queued_at, c.id",
                chunk_queue::STALE,
                crate::defs::model::applying_sql("d"),
            ),
            &[&definition_id, &chunk_queue::KIND_CORRECTION],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| CorrectingNode {
            node: row.get(0),
            since: row.get(1),
            keys_remaining: keys_remaining(row.get(2), row.get(3)),
        })
        .collect())
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
/// its source (an aggregate over it, or a `JOIN` reading it as either side)
/// and applies (`live`, or `backfilling` under a Re-derive build:
/// `defs::model::APPLYING_SQL`) and has not re-derived it, enqueues the
/// correction's row unless one is there, moving a `live` definition to
/// `backfilling`. Takes a correction off the queue once every one of them
/// has re-derived it.
///
/// A definition that is frozen (paused, quarantined) or still waiting to
/// build gets no row: its resume or its build reads the source afresh.
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
        let readers = all_readers(pool, &table).await?;
        for correction in &corrections {
            let mut drained = !kept_by_frozen(pool, correction.id).await?;
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
/// definition is not `live` or `backfilling` under a Re-derive build, or a
/// current row of it is there (one held
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
                &StoredStatus::Live.as_str(),
                &StoredStatus::Backfilling.as_str(),
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
    let flipped = status == StoredStatus::Live.as_str();
    if flipped {
        txn.execute(
            "update transform_definitions set status = $2, build = $3 where id = $1",
            &[
                &definition_id,
                &StoredStatus::Backfilling.as_str(),
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
            from = %StoredStatus::Live.as_str(),
            to = %StoredStatus::Backfilling.as_str(),
            "transform status transition: a partition correction started"
        );
    }
    Ok(true)
}

/// Takes `correction` off the queue if every definition reading its source
/// that applies has corrected it and no frozen definition keeps it
/// ([`kept_by_frozen`]): [`schedule`]'s test. Returns whether it did.
async fn complete_if_drained(
    pool: &Pool,
    correction: &PendingCorrection,
) -> Result<bool, CorrectionError> {
    let ids = all_readers(pool, &correction.table).await?;
    if kept_by_frozen(pool, correction.id).await? {
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
        // The correction's source: a `JOIN`'s joined table is not its
        // definition's source.
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
    let group_shape = super::group_basis::route(&definition.def, &definition.source_columns);
    // A `JOIN` reads the source as either side.
    let join_side = super::join::applies(&definition.def)
        .then(|| crate::defs::join::side_of_table(&definition.def, &correction.table))
        .flatten();
    let step = if let Some(side) = join_side {
        group::correct(
            pool,
            &definition,
            group::Cells::Join(side),
            &correction,
            options,
            outcome,
            &fence,
        )
        .await?
    } else if definition.source_table == correction.table
        && let Some(shape) = &group_shape
    {
        group::correct(
            pool,
            &definition,
            group::Cells::Groups(shape),
            &correction,
            options,
            outcome,
            &fence,
        )
        .await?
    } else if definition.source_table == correction.table {
        // A shape this engine cannot re-derive. The failure is the row's to
        // report, rather than a correction that completes uncorrected.
        return Err(CorrectionError::Unserved {
            target: definition.target_table.clone(),
        }
        .into());
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

/// The ids of the definitions reading `source_table` that apply: the
/// aggregates over it and the `JOIN`s reading it as either side.
async fn readers(
    client: &impl GenericClient,
    source_table: &str,
) -> Result<Vec<i64>, CorrectionError> {
    Ok(client
        .query(
            &format!(
                "select id from transform_definitions \
                 where (source_table = $1 or join_table = $1) and {APPLYING_SQL} order by id"
            ),
            &[&source_table],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// [`readers`] on a pooled connection.
async fn all_readers(pool: &Pool, source_table: &str) -> Result<Vec<i64>, CorrectionError> {
    let client = pool.get().await?;
    readers(&**client, source_table).await
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

/// Whether `correction` is a complement that names a `HASH` node no longer
/// there. A complement names each `HASH` node it reads remainders of, and
/// can't be evaluated once one is dropped. The dropped node's own
/// correction covers its groups, and the next pass's sweep finds any others
/// again under the tree as it is then.
async fn complement_unevaluable(
    pool: &Pool,
    correction: &PendingCorrection,
) -> Result<bool, CorrectionError> {
    if correction.kind != partitions::Kind::Complement.as_str() {
        return Ok(false);
    }
    let Some(text) = &correction.constraint else {
        return Ok(false);
    };
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
    Ok(usize::try_from(alive).unwrap_or(0) < oids.len())
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

/// What became of a new or truncated leaf's recorded ancestors.
#[derive(Debug, PartialEq, Eq)]
enum Ancestry {
    /// Every recorded ancestor is there, or the correction is of another
    /// kind, or the node is not in the recorded tree.
    Intact,
    /// An ancestor has been dropped but the node is still there: it was
    /// moved out from under it. The next pass records it where it is now,
    /// and the correction routes by that record.
    Moved,
    /// An ancestor has been dropped, and the node with it.
    Gone,
}

/// What became of `correction`'s recorded ancestors, for a new or truncated
/// leaf. The leaf's text embeds its `HASH` ancestors' oids and cannot be
/// evaluated once one is dropped. If the leaf went with it, the topmost
/// dropped ancestor's own `gone_node`, queued by the pass that sees it go,
/// routes every group of its bound, the leaf's among them, so the leaf's
/// correction is complete without work. If the leaf is still there, its rows
/// still count: the pass that sees it moved queues a `new_leaf` of it, which
/// this entry absorbs while no reader has started it, so this correction
/// waits for the pass's record rather than completing.
async fn ancestry(
    pool: &Pool,
    correction: &PendingCorrection,
    recorded: Option<&Recorded>,
) -> Result<Ancestry, CorrectionError> {
    if correction.kind != partitions::Kind::NewLeaf.as_str()
        && correction.kind != partitions::Kind::Truncated.as_str()
    {
        return Ok(Ancestry::Intact);
    }
    let Some(recorded) = recorded else {
        return Ok(Ancestry::Intact);
    };
    let mut ancestors: Vec<i64> = Vec::new();
    let mut at = correction.node_oid;
    while let Some(parent) = recorded
        .nodes
        .iter()
        .find(|n| n.oid == at)
        .and_then(|n| n.parent)
    {
        if ancestors.contains(&i64::from(parent)) {
            break;
        }
        ancestors.push(i64::from(parent));
        at = parent;
    }
    if ancestors.is_empty() {
        return Ok(Ancestry::Intact);
    }
    let client = pool.get().await?;
    let row = client
        .query_one(
            "select (select count(*) from pg_catalog.pg_class \
                     where oid = any($1::bigint[]::oid[])), \
                    exists (select 1 from pg_catalog.pg_class where oid = $2::bigint::oid)",
            &[&ancestors, &i64::from(correction.node_oid)],
        )
        .await?;
    let alive: i64 = row.get(0);
    let node_alive: bool = row.get(1);
    Ok(if usize::try_from(alive).unwrap_or(0) == ancestors.len() {
        Ancestry::Intact
    } else if node_alive {
        Ancestry::Moved
    } else {
        Ancestry::Gone
    })
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

/// `oid` as the `i64` a text-free bind takes.
fn u32_param(oid: u32) -> i64 {
    i64::from(oid)
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

/// Whether the correction reads the node's own rows for its groups: a node
/// in the tree (new, attached, or a `DEFAULT` whose siblings changed), whose
/// rows may be in groups the target doesn't have yet, and a gone node
/// without a usable text, which routes no group. A gone node routed by its
/// text needs no read of its table: every group of its rows routes to it by
/// that text. A complement has no node.
fn reads_node(correction: &PendingCorrection, route: &Route) -> bool {
    if correction.kind == partitions::Kind::Complement.as_str() {
        return false;
    }
    correction.kind != partitions::Kind::GoneNode.as_str() || *route == Route::Unusable
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
    fn keys_remaining_is_uncounted_before_the_first_page_and_never_negative() {
        assert_eq!(keys_remaining(None, 0), None);
        assert_eq!(keys_remaining(Some(40), 35), Some(5));
        assert_eq!(keys_remaining(Some(40), 40), Some(0));
        // The node phase's total is an estimate the walk can pass.
        assert_eq!(keys_remaining(Some(40), 52), Some(0));
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

    /// A rebuild's leaves: in oid order, a `DEFAULT` without a text routes
    /// every group only while it is the root's one leaf (its text is empty
    /// then); beside siblings, a missing text is a leaf nothing routes to.
    #[test]
    fn a_rebuilds_leaves_route_every_group_to_a_textless_default_only_alone() {
        let node = |oid: u32, default: bool, text: Option<&str>| partitions::Node {
            oid,
            parent: (oid != 1).then_some(1),
            name: format!("public.n{oid}"),
            partitioned: oid == 1,
            is_default: default,
            bound: None,
            inherits_xmin: None,
            constraint: text.map(str::to_string),
        };
        let alone = Recorded {
            nodes: vec![node(1, false, None), node(9, true, None)],
            keys: Vec::new(),
        };
        let leaves = rebuild_leaves(&alone).expect("a lone DEFAULT routes");
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].route, Route::Every);
        let beside = Recorded {
            nodes: vec![
                node(1, false, None),
                node(9, true, None),
                node(5, false, Some("(a < 3)")),
            ],
            keys: Vec::new(),
        };
        assert_eq!(rebuild_leaves(&beside).err().as_deref(), Some("public.n9"));
        let texts = Recorded {
            nodes: vec![
                node(1, false, None),
                node(9, true, Some("(NOT (a < 3))")),
                node(5, false, Some("(a < 3)")),
            ],
            keys: Vec::new(),
        };
        let oids: Vec<u32> = rebuild_leaves(&texts)
            .expect("every text is there")
            .iter()
            .map(|leaf| leaf.oid)
            .collect();
        assert_eq!(oids, [5, 9], "oid order");
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
    /// type's own collation, whose text names none.
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
