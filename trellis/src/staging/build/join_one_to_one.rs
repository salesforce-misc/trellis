//! A 1-1 `JOIN` definition's build jobs (ADR-0021): the plan job's `M` pass,
//! the chunks and the rebuild sweep. What each transaction does is
//! `crate::staging::join_one_to_one::build`'s; this is the jobs around them.
//!
//! - **The plan job** runs the `M` pass first ([`build_m`]): every key of
//!   `b` ∪ `M` in batches of the chunk size, each its own transaction, a
//!   page of `b` Re-derives without the fan-out. `b`'s rows come first, then
//!   `M`'s live entries whose `b` row is gone, which become tombstones. Then
//!   it walks `a`'s primary key and enqueues `rederive` chunks as for a plain
//!   1-1 target (the parent module's `run_plan`), so no chunk runs before
//!   `M` is built, and none joins an `a` row to a `b` row that is gone. Each
//!   batch commits with the job's cursor ([`PlanStage`]), so a job that
//!   fails or dies during the pass or the walk resumes from its last batch.
//! - **A chunk** ([`run_chunk`]) runs in the transaction that marks it done,
//!   under [`CHUNK_LOCK_TIMEOUT`] for every lock it takes, so a page waits
//!   at most one chunk's length behind it, and a chunk that meets a page
//!   holding a lock gives up and retries.
//! - **The rebuild sweep** ([`sweep_batch`]) runs after the plan job and
//!   every chunk, in two passes: `R`'s stale entries, then the orphans,
//!   target rows no live `R` entry backs. Each batch re-derives what it found
//!   as a page of `a` Re-derives. A build start enqueues it whenever the
//!   target has rows (the parent module's `start`): a `RESUME` recreates `R`
//!   and `M` empty and keeps the target, so the rows of `a` rows deleted
//!   while the transform was paused are backed by nothing, and only the
//!   orphan pass removes them (#1481).

use std::sync::Arc;

use tokio_postgres::{IsolationLevel, Transaction};

use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::Side;
use crate::staging::join_one_to_one::build as steps;
use crate::staging::join_one_to_one::{JoinShape, plan_for};

use super::orphans::{self, OrphanCursor, OrphanWalk};
use super::{
    CHUNK_APPLICATION_NAME, CHUNK_LOCK_TIMEOUT, ChunkQueueError, ClaimFence, Definition, Pool,
    SweepOutcome, WorkerOptions, build_error,
};

/// What a 1-1 `JOIN`'s chunk or sweep batch needs: the definition's shape,
/// `a`'s key as the plan job walked it (with its recorded collations), and
/// the target's own key, which the orphan pass walks.
pub(super) struct JoinPlan {
    shape: Arc<JoinShape>,
    range_pk: Vec<PrimaryKeyColumn>,
    target_key: Vec<PrimaryKeyColumn>,
}

impl JoinPlan {
    /// The plan of `definition`, a 1-1 `JOIN`, its range read under
    /// `key_collations` (the chunk's recorded ones).
    pub(super) async fn load(
        pool: &Pool,
        definition: &Definition,
        key_collations: Option<&[Option<String>]>,
    ) -> Result<Self, ChunkQueueError> {
        let pk = ddl::source_primary_key(pool, &definition.source_table)
            .await
            .map_err(build_error)?;
        let mut range_pk = pk.clone();
        ddl::pin_key_collations(&mut range_pk, key_collations);
        let target_key = ddl::source_primary_key(pool, &definition.target_table)
            .await
            .map_err(build_error)?;
        let plan = plan_for(pool, definition, Side::From, pk)
            .await
            .map_err(build_error)?;
        Ok(Self {
            shape: plan.shape,
            range_pk,
            target_key,
        })
    }
}

/// Begins one `READ COMMITTED` transaction of a build step, with the chunk's
/// `application_name` and [`CHUNK_LOCK_TIMEOUT`] for every lock it takes.
async fn begin(client: &mut tokio_postgres::Client) -> Result<Transaction<'_>, ChunkQueueError> {
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&format!(
        "set local application_name = '{CHUNK_APPLICATION_NAME}'"
    ))
    .await?;
    crate::locks::set_local_lock_timeout(&txn, CHUNK_LOCK_TIMEOUT).await?;
    Ok(txn)
}

/// Where a 1-1 `JOIN`'s plan job stands (its job's `lo`): in one of the `M`
/// pass's three walks after a key, or in the walk of `a` after a boundary.
/// Each stored form is U+0001 and a letter, then `:` and the key when there
/// is one, so no stored `a` boundary reads as another stage whatever its key
/// holds. No stored cursor, or one of another form, is the start of the `M`
/// pass, which is idempotent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PlanStage {
    /// `b`'s rows in its primary-key order, under the collations the job
    /// records with the cursor (`backfill_chunks.key_collations`).
    B(Option<String>),
    /// `b`'s keys with a `NULL` part, in their text's byte order.
    BNulls(Option<String>),
    /// `M`'s live entries, in key order: those whose `b` row is gone.
    Gone(Option<String>),
    /// The walk of `a`'s primary key (the parent module's `run_plan`).
    Walk(Option<String>),
}

impl PlanStage {
    pub(super) fn parse(stored: Option<&str>) -> Self {
        let Some(stored) = stored else {
            return PlanStage::B(None);
        };
        let mut chars = stored.chars();
        let (Some('\u{1}'), Some(tag)) = (chars.next(), chars.next()) else {
            return PlanStage::B(None);
        };
        let key = chars.as_str().strip_prefix(':').map(str::to_string);
        match tag {
            'b' => PlanStage::B(key),
            'n' => PlanStage::BNulls(key),
            'g' => PlanStage::Gone(key),
            'a' => PlanStage::Walk(key),
            _ => PlanStage::B(None),
        }
    }

    pub(super) fn encode(&self) -> String {
        let (tag, key) = match self {
            PlanStage::B(key) => ('b', key),
            PlanStage::BNulls(key) => ('n', key),
            PlanStage::Gone(key) => ('g', key),
            PlanStage::Walk(key) => ('a', key),
        };
        match key {
            Some(key) => format!("\u{1}{tag}:{key}"),
            None => format!("\u{1}{tag}"),
        }
    }
}

/// The plan job's `M` pass (see the module doc) from `stage`, chunk
/// `chunk_id`'s stored cursor. Each batch is fenced by the claim and commits
/// with the cursor's advance, so a job that fails or dies resumes from the
/// last batch that committed. `recorded` is the job's
/// `backfill_chunks.key_collations`: on a resumed walk of `b`, the
/// collations its earlier batches walked under. The last batch stores
/// [`PlanStage::Walk`] with no collations, so the walk of `a` records its
/// own. Returns `false`, having rolled back, when the claim no longer holds:
/// the caller gives the job up as superseded.
pub(super) async fn build_m(
    pool: &Pool,
    definition: &Definition,
    chunk_id: i64,
    fence: &ClaimFence<'_>,
    options: &WorkerOptions,
    mut stage: PlanStage,
    recorded: Option<&[Option<String>]>,
) -> Result<bool, ChunkQueueError> {
    let plan = JoinPlan::load(pool, definition, None).await?;
    let shape = plan.shape;
    // `b`'s key once for the whole pass, under the collations its walk began
    // with, so every batch walks it in one order.
    let mut m_pk = ddl::source_primary_key(pool, shape.joined_table())
        .await
        .map_err(build_error)?;
    if matches!(stage, PlanStage::B(Some(_))) {
        ddl::pin_key_collations(&mut m_pk, recorded);
    }
    let collations = ddl::key_collations(&m_pk);
    let limit = options.chunk_rows.max(1);
    let mut client = pool.get().await?;
    while !matches!(stage, PlanStage::Walk(_)) {
        let txn = begin(&mut client).await?;
        if !fence.hold(&txn).await? {
            txn.rollback().await?;
            return Ok(false);
        }
        let next = match &stage {
            PlanStage::B(after) => {
                match steps::m_batch(&txn, &shape, &m_pk, after.as_deref(), limit)
                    .await
                    .map_err(build_error)?
                {
                    Some(key) => PlanStage::B(Some(key)),
                    None => PlanStage::BNulls(None),
                }
            }
            PlanStage::BNulls(after) => {
                match steps::m_null_batch(&txn, &shape, &m_pk, after.as_deref(), limit)
                    .await
                    .map_err(build_error)?
                {
                    Some(key) => PlanStage::BNulls(Some(key)),
                    None => PlanStage::Gone(None),
                }
            }
            // Planted bug: the `M` pass walks only `b`, not `M`'s entries
            // whose `b` row is gone. See `crate::plant`.
            #[cfg(any(test, feature = "test-util"))]
            PlanStage::Gone(_)
                if crate::plant::fires(crate::plant::Plant::Join1MPassBOnly, true) =>
            {
                PlanStage::Walk(None)
            }
            PlanStage::Gone(after) => {
                match steps::m_gone_batch(&txn, &shape, &m_pk, after.as_deref(), limit)
                    .await
                    .map_err(build_error)?
                {
                    Some(key) => PlanStage::Gone(Some(key)),
                    None => PlanStage::Walk(None),
                }
            }
            PlanStage::Walk(_) => unreachable!("the pass ends at the walk of `a`"),
        };
        let walking_b = matches!(next, PlanStage::B(_));
        txn.execute(
            "update backfill_chunks set lo = $2, key_collations = $3 where id = $1",
            &[&chunk_id, &next.encode(), &walking_b.then_some(&collations)],
        )
        .await?;
        txn.commit().await?;
        stage = next;
    }
    crate::instance_log::debug!(
        definition_id = definition.id,
        "1-1 JOIN build re-derived every key of its joined table and of M"
    );
    Ok(true)
}

/// One chunk, `(lo, hi]` of `a`'s primary key, in `txn`, the chunk's own
/// transaction (`super::run_rederive`), which marks it done and commits.
/// Returns the keys it re-derived.
pub(super) async fn run_chunk(
    txn: &Transaction<'_>,
    plan: &JoinPlan,
    lo: Option<&str>,
    hi: &str,
) -> Result<usize, ChunkQueueError> {
    crate::locks::set_local_lock_timeout(txn, CHUNK_LOCK_TIMEOUT).await?;
    let outcome = steps::run_chunk(txn, &plan.shape, &plan.range_pk, lo, hi)
        .await
        .map_err(build_error)?;
    // Test-only pause point: the chunk has written everything and holds its
    // locks. See `super::super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::super::interleave::pause_at(
        txn,
        super::super::interleave::PausePoint::BeforeRebuildCommit,
        &plan.shape.target,
    )
    .await?;
    crate::instance_log::debug!(
        target_table = %plan.shape.target,
        keys = outcome.keys,
        written = outcome.written,
        deleted = outcome.deleted,
        "1-1 JOIN build chunk re-derived its range"
    );
    Ok(outcome.keys)
}

/// Where a 1-1 `JOIN`'s rebuild sweep stands (its job's `lo`): in `R`'s
/// stale pass after an entry key, or in the orphan walk. Each stored form is
/// U+0001 and a letter, then `:` and the key when there is one; no stored
/// cursor, or one of another form, is the start of `R`'s pass.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Stage {
    Stale(Option<String>),
    Orphans(OrphanCursor),
}

impl Stage {
    fn parse(stored: Option<&str>) -> Self {
        let Some(stored) = stored else {
            return Stage::Stale(None);
        };
        let mut chars = stored.chars();
        let (Some('\u{1}'), Some(tag)) = (chars.next(), chars.next()) else {
            return Stage::Stale(None);
        };
        let key = chars.as_str().strip_prefix(':').map(str::to_string);
        match tag {
            'r' => Stage::Stale(key),
            'o' => Stage::Orphans(OrphanCursor::Keyed(key)),
            'n' => Stage::Orphans(OrphanCursor::Nulls(key)),
            _ => Stage::Stale(None),
        }
    }

    fn encode(&self) -> String {
        let (tag, key) = match self {
            Stage::Stale(key) => ('r', key),
            Stage::Orphans(OrphanCursor::Keyed(key)) => ('o', key),
            Stage::Orphans(OrphanCursor::Nulls(key)) => ('n', key),
        };
        match key {
            Some(key) => format!("\u{1}{tag}:{key}"),
            None => format!("\u{1}{tag}"),
        }
    }
}

/// One batch of a 1-1 `JOIN`'s rebuild sweep (see the module doc), from
/// `cursor`, in `txn`, which the caller commits with the cursor's advance.
/// `R`'s stale pass reads up to `scan` entries; the orphan pass, `scan` target
/// rows. The sweep is finished when the orphan walk is.
pub(super) async fn sweep_batch(
    txn: &Transaction<'_>,
    plan: &JoinPlan,
    start_xid: &str,
    cursor: Option<&str>,
    scan: i64,
) -> Result<SweepOutcome, ChunkQueueError> {
    crate::locks::set_local_lock_timeout(txn, CHUNK_LOCK_TIMEOUT).await?;
    let shape = &plan.shape;
    let stage = Stage::parse(cursor);
    let (scanned, rederived, next) = match stage {
        Stage::Stale(after) => {
            let (scanned, last, stale) =
                steps::stale_entries(txn, shape, start_xid, after.as_deref(), scan)
                    .await
                    .map_err(build_error)?;
            steps::rederive(txn, shape, Side::From, &stale, true)
                .await
                .map_err(build_error)?;
            let next = if scanned < scan {
                Stage::Orphans(OrphanCursor::Keyed(None))
            } else {
                Stage::Stale(last)
            };
            (scanned, stale.len(), Some(next))
        }
        Stage::Orphans(cursor) => {
            // Planted bug: the sweep has no orphan pass. See `crate::plant`.
            #[cfg(any(test, feature = "test-util"))]
            if crate::plant::fires(crate::plant::Plant::Join1NoOrphanPass, true) {
                return Ok(SweepOutcome {
                    scanned: 0,
                    rederived: 0,
                    next: None,
                    finished: true,
                });
            }
            let walk = OrphanWalk {
                target: &shape.target,
                key: &plan.target_key,
                ledger: shape.r_ident(),
                source_table: shape.source_table(),
            };
            let batch = orphans::orphan_batch(txn, &walk, &cursor, scan)
                .await
                .map_err(build_error)?;
            // Test-only pause point: the orphans are read, none is locked.
            // See `super::super::interleave`.
            #[cfg(any(test, feature = "test-util"))]
            super::super::interleave::pause_at(
                txn,
                super::super::interleave::PausePoint::AfterJoin1OrphanRead,
                &shape.target,
            )
            .await?;
            // Planted bug: the orphans' target rows are deleted by
            // statement, with no entry lock and no re-read of `a`. See
            // `crate::plant`.
            #[cfg(any(test, feature = "test-util"))]
            let deleted = crate::plant::fires(
                crate::plant::Plant::Join1OrphanDelete,
                !batch.orphans.is_empty(),
            );
            #[cfg(not(any(test, feature = "test-util")))]
            let deleted = false;
            if deleted {
                txn.execute(
                    &format!(
                        "delete from {} t where ({}) = any($1::text[])",
                        ddl::qualified_target_table_ident(&shape.target),
                        ddl::pk_key_sql_expr(&plan.target_key, Some("t")),
                    ),
                    &[&batch.orphans],
                )
                .await?;
            } else {
                steps::rederive(txn, shape, Side::From, &batch.orphans, true)
                    .await
                    .map_err(build_error)?;
            }
            (
                batch.scanned,
                batch.orphans.len(),
                batch.next.map(Stage::Orphans),
            )
        }
    };
    crate::instance_log::debug!(
        target_table = %shape.target,
        scanned,
        rederived,
        next = ?next,
        "1-1 JOIN rebuild sweep batch re-derived what it found"
    );
    Ok(SweepOutcome {
        scanned,
        rederived,
        finished: next.is_none(),
        next: next.map(|stage| stage.encode()),
    })
}

#[cfg(test)]
mod tests {
    use super::{OrphanCursor, PlanStage, Stage};

    /// Every stage's stored cursor parses back to itself, whatever its key
    /// holds, and no cursor is the start of `R`'s pass.
    #[test]
    fn a_sweep_cursor_round_trips() {
        for key in [None, Some(""), Some("42"), Some("\u{1}o:7"), Some("a:b")] {
            let key = key.map(str::to_string);
            for stage in [
                Stage::Stale(key.clone()),
                Stage::Orphans(OrphanCursor::Keyed(key.clone())),
                Stage::Orphans(OrphanCursor::Nulls(key.clone())),
            ] {
                assert_eq!(Stage::parse(Some(&stage.encode())), stage, "{key:?}");
            }
        }
        assert_eq!(Stage::parse(None), Stage::Stale(None));
    }

    /// Every stage of the plan job's stored cursor parses back to itself,
    /// whatever its key holds (an `a` key may begin with U+0001), and no
    /// cursor, or a bare key, is the start of the `M` pass.
    #[test]
    fn a_plan_cursor_round_trips() {
        for key in [None, Some(""), Some("42"), Some("\u{1}a:7"), Some("a:b")] {
            let key = key.map(str::to_string);
            for stage in [
                PlanStage::B(key.clone()),
                PlanStage::BNulls(key.clone()),
                PlanStage::Gone(key.clone()),
                PlanStage::Walk(key.clone()),
            ] {
                assert_eq!(PlanStage::parse(Some(&stage.encode())), stage, "{key:?}");
            }
        }
        assert_eq!(PlanStage::parse(None), PlanStage::B(None));
        assert_eq!(PlanStage::parse(Some("a07")), PlanStage::B(None));
    }
}
