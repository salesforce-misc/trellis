//! The correction engine for aggregates over a partitioned source (ADR-0017,
//! P4a of epic #897).
//!
//! Partition maintenance (`ATTACH`, `DETACH`, `DROP`, a new leaf written
//! directly) changes a partitioned root's rows without a write any capture
//! trigger sees. The reconcile pass records each such change as a durable
//! correction request ([`crate::capture::partitions`], P3). This module
//! consumes the requests of an **aggregate** that reads the source.
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
//! column's recorded type and collation: a `HASH` text raises on a
//! differently typed column, and a text column compares under its collation.
//! The read runs under pinned settings (`search_path = pg_catalog, pg_temp`,
//! `DateStyle ISO, YMD`, `TimeZone UTC`: [`crate::capture::partitions`]'s
//! `pinned_settings`), because the text of a bound, and what it means, depends
//! on them: under the session's search path an enum in a `RANGE` bound, an ICU
//! collation shadowing a built-in name and a custom operator in `public`
//! silently misroute rows. A `DEFAULT` with no siblings has no text (every
//! group routes to it), and any other node without a usable text routes
//! nothing: the every-pass sweep (P5) finds those groups.
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
//! # What this module does not do
//!
//! It does not schedule itself. [`run_pending`] runs every queued correction
//! of one source to its end (or to a page limit, for a test that stops it
//! mid-way); the staging worker's scheduling, the `correcting` status and the
//! definition's `backfilling` status while it runs are later phases of
//! ADR-0017. A to-many relationship's recompute is P4b.
//!
//! Until the staging worker calls it, only tests and embedders built with the
//! `internals` feature reach it.
#![cfg_attr(not(any(test, feature = "internals")), allow(dead_code))]

use std::fmt;

use tokio_postgres::{GenericClient, IsolationLevel};

use crate::capture::partitions::{self, Key, PendingCorrection, Recorded};
use crate::defs::model::{APPLYING_SQL, Definition, TransformStatus};
use crate::defs::{backfill, catalog, ddl};
use crate::pool::{Pool, quote_ident};

use super::apply::ApplyError;
use super::build::{self, BuildPlan, CHUNK_APPLICATION_NAME};
use super::ledger::LedgerTargetPlan;

/// Keys per page by default, the size of a build chunk
/// ([`build::DEFAULT_CHUNK_ROWS`]): one page is one transaction and one entry
/// lock set, so it bounds the locks and the WAL a transaction holds.
pub const DEFAULT_PAGE_KEYS: usize = 10_000;

/// Why a correction stopped.
#[derive(Debug)]
pub enum CorrectionError {
    Db(tokio_postgres::Error),
    Pool(crate::error::Error),
    Apply(ApplyError),
    Capture(crate::capture::CaptureError),
    Catalog(catalog::CatalogError),
    /// The node's group cannot be presented to its constraint text.
    Unroutable(String),
}

impl fmt::Display for CorrectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorrectionError::Db(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Pool(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Apply(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Capture(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Catalog(err) => write!(f, "partition correction: {err}"),
            CorrectionError::Unroutable(why) => write!(f, "partition correction: {why}"),
        }
    }
}

impl std::error::Error for CorrectionError {}

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

impl From<catalog::CatalogError> for CorrectionError {
    fn from(err: catalog::CatalogError) -> Self {
        CorrectionError::Catalog(err)
    }
}

/// How [`run_pending`] runs.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Keys per page.
    pub page_keys: usize,
    /// Stops after this many pages in all, leaving the progress committed so
    /// far for the next call: a test's deterministic stand-in for a crash or
    /// a restart in the middle of a correction.
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

/// What [`run_pending`] did.
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
    // A node in the tree routes by what the record says of it now. A
    // `truncated` correction, queued by the seal, carries neither.
    let (text, is_default) = if correction.kind == partitions::Kind::GoneNode.as_str() {
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
    plan.plain_group_cols()
        .into_iter()
        .map(|name| RouteColumn {
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
    Node,
    Done,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::Fence => "fence",
            Phase::Ledger => "ledger",
            Phase::Node => "node",
            Phase::Done => "done",
        }
    }

    fn parse(text: &str) -> Phase {
        match text {
            "fence" => Phase::Fence,
            "ledger" => Phase::Ledger,
            "node" => Phase::Node,
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
    /// The phase: `fence`, `ledger`, `node` or `done`.
    pub fn phase(&self) -> &str {
        self.phase
    }

    /// Keys still to re-derive by the estimate.
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

/// Runs every queued correction of `source_table` for the aggregate
/// definitions that read it and apply (`live`, `catching_up`, or
/// `backfilling` under a Re-derive build: `defs::model::APPLYING_SQL`),
/// taking a correction off the queue once all of them have re-derived it.
/// See the module doc for the order.
///
/// A definition that is frozen (paused, quarantined) or still waiting to
/// build is skipped: its resume or its build reads the source afresh. One
/// whose build is on the old path (`backfilling` without a Re-derive build)
/// holds every correction in the queue until it applies: its chunks may have
/// read the node's rows before the operation, and nothing after them reads
/// those keys again.
pub async fn run_pending(
    pool: &Pool,
    source_table: &str,
    options: &Options,
) -> Result<Outcome, CorrectionError> {
    let mut outcome = Outcome::default();
    let corrections = {
        let client = pool.get().await?;
        partitions::pending_corrections(&**client, source_table).await?
    };
    let (definitions, held) = readers(pool, source_table).await?;
    for correction in &corrections {
        let mut finished = !held;
        for definition in &definitions {
            match correct(pool, definition, correction, options, &mut outcome).await? {
                Step::Done => {}
                Step::Waiting | Step::Stopped => finished = false,
            }
            if budget_spent(options, &outcome) {
                break;
            }
        }
        if finished {
            let client = pool.get().await?;
            partitions::complete_correction(&**client, correction.id).await?;
            outcome.completed += 1;
        } else {
            outcome.remaining += 1;
        }
        if budget_spent(options, &outcome) {
            break;
        }
    }
    Ok(outcome)
}

fn budget_spent(options: &Options, outcome: &Outcome) -> bool {
    options.max_pages.is_some_and(|max| outcome.pages >= max)
}

/// The definitions reading `source_table` that apply, and whether one is
/// under an old-path build (`backfilling` without a Re-derive build), which
/// holds the corrections in the queue.
async fn readers(
    pool: &Pool,
    source_table: &str,
) -> Result<(Vec<Definition>, bool), CorrectionError> {
    let (ids, held): (Vec<i64>, bool) = {
        let client = pool.get().await?;
        let ids = client
            .query(
                &format!(
                    "select id from transform_definitions \
                     where source_table = $1 and {APPLYING_SQL} order by id"
                ),
                &[&source_table],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        let held = client
            .query_one(
                "select exists (select 1 from transform_definitions \
                                where source_table = $1 and status = $2 \
                                  and build is distinct from 'rederive')",
                &[&source_table, &TransformStatus::Backfilling.as_str()],
            )
            .await?
            .get(0);
        (ids, held)
    };
    let mut out = Vec::new();
    for id in ids {
        if let Some(definition) = catalog::definition_by_id(pool, id).await? {
            out.push(definition);
        }
    }
    Ok((out, held))
}

/// What one call of [`correct`] left.
enum Step {
    /// The definition re-derived the correction's key set.
    Done,
    /// It waits for a detach to finalize or for the fence to settle, or the
    /// definition's shape is one the engine cannot re-derive.
    Waiting,
    /// The page budget ran out.
    Stopped,
}

/// Runs correction `correction` for `definition` page by page, resuming from
/// its progress.
async fn correct(
    pool: &Pool,
    definition: &Definition,
    correction: &PendingCorrection,
    options: &Options,
    outcome: &mut Outcome,
) -> Result<Step, CorrectionError> {
    let Some(plan) = BuildPlan::for_definition(pool, definition).await? else {
        // A shape the Re-derive build does not serve yet (an aggregate that
        // reads a relationship): this engine cannot re-derive its keys, so
        // the correction stays queued rather than completing uncorrected.
        return Ok(Step::Waiting);
    };
    let ledger = plan.ledger();
    if correction.kind == partitions::Kind::GoneNode.as_str()
        && let Some(parent) = correction.parent_oid
    {
        // A `HASH` text embeds the parent's oid and cannot be evaluated once
        // the parent is dropped. The parent's own correction, queued by the
        // pass that saw it go, routes every group of its bound, the leaf's
        // among them.
        let client = pool.get().await?;
        let alive: bool = client
            .query_one(
                "select exists (select 1 from pg_catalog.pg_class where oid = $1::bigint::oid)",
                &[&i64::from(parent)],
            )
            .await?
            .get(0);
        if !alive {
            return Ok(Step::Done);
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
            if !start(pool, definition, correction).await? {
                return Ok(Step::Waiting);
            }
            continue;
        };
        match Phase::parse(current.phase) {
            Phase::Done => return Ok(Step::Done),
            Phase::Fence => {
                if !settle_fence(pool, definition, correction).await? {
                    return Ok(Step::Waiting);
                }
            }
            Phase::Ledger => {
                let page = ledger_page(
                    pool,
                    ledger,
                    &route,
                    &columns,
                    &group_types,
                    &current,
                    correction,
                    definition,
                    options.page_keys,
                )
                .await?;
                outcome.pages += 1;
                outcome.keys += page;
            }
            Phase::Node => {
                let page = node_page(
                    pool,
                    ledger,
                    &current,
                    correction,
                    definition,
                    options.page_keys,
                )
                .await?;
                outcome.pages += 1;
                outcome.keys += page;
            }
        }
    }
}

/// Starts `correction` for `definition`: waits for a pending detach to
/// finish, then stamps the definition's `build_seg` and records the progress
/// row, in one transaction. Returns `false` while the detach is pending.
async fn start(
    pool: &Pool,
    definition: &Definition,
    correction: &PendingCorrection,
) -> Result<bool, CorrectionError> {
    let mut client = pool.get().await?;
    if correction.kind == partitions::Kind::GoneNode.as_str() {
        // The node's `pg_inherits` row is there, `inhdetachpending`, until
        // the detach is final. A writer with an older snapshot can still
        // write through the parent into a detach-pending leaf, and `DETACH
        // ... FINALIZE` waits for those writers, so only a node that has left
        // the tree has all its changes in the ring. A node attached again
        // since has a row that is not pending: its correction runs, and every
        // key it re-derives reads the source as it is.
        let pending: bool = client
            .query_one(
                "select exists (select 1 from pg_catalog.pg_inherits \
                                where inhrelid = $1::bigint::oid and inhdetachpending)",
                &[&u32_param(correction.node_oid)],
            )
            .await?
            .get(0);
        if pending {
            return Ok(false);
        }
    }
    let txn = client.transaction().await?;
    // The definition's row first, as a rebuild's start takes it.
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&definition.id],
    )
    .await?;
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
    Ok(true)
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
/// commits the progress with them. Returns the keys re-derived.
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
) -> Result<i64, CorrectionError> {
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
    txn.commit().await?;
    Ok(i64::try_from(keys.len()).unwrap_or(i64::MAX))
}

/// Whether the correction walks the node's rows after the ledger: a node in
/// the tree (new, attached, or a `DEFAULT` whose siblings changed), whose
/// rows may have no entry yet, and a gone node without a usable text, which
/// routes no entry. A gone node routed by its text needs no node phase: every
/// row it held satisfied that text while it was in the tree, so the ledger
/// phase found every live entry of its rows, and reading a detached table's
/// rows would only rewrite those tombstones again.
fn reads_node(correction: &PendingCorrection, route: &Route) -> bool {
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
/// re-derives them, committing the progress with them.
async fn node_page(
    pool: &Pool,
    ledger: &LedgerTargetPlan,
    current: &Progress,
    correction: &PendingCorrection,
    definition: &Definition,
    page_keys: usize,
) -> Result<i64, CorrectionError> {
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
        return Ok(0);
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
    txn.commit().await?;
    Ok(i64::try_from(keys.len()).unwrap_or(i64::MAX))
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
}
