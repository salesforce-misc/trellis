//! Test-only pause points inside a drain page's apply transaction.
//! **Compiled only under `cfg(any(test, feature =
//! "test-util"))`**, like [`crate::plant`] and the pre-commit pause in
//! [`super::apply`].
//!
//! The interleaving tests (`tests/one_to_one_ledger_interleavings.rs`) freeze one drain
//! worker between two steps of its page transaction, do something else (commit
//! a source write, drain another batch on another worker), then let it go. No
//! test sleeps or polls for convergence.
//!
//! # How a test uses it
//!
//! 1. Build a [`PauseScope`] and [`PauseScope::arm`] a [`PausePoint`] for one
//!    target, naming an advisory lock key the test already holds on a
//!    connection of its own (a *session* lock, `pg_advisory_lock`).
//! 2. Run the drain inside [`with_scope`]. The scope is a tokio task-local,
//!    so an armed point only fires in that drain's own task: two drains
//!    running at once, or other tests in the same binary, never see it.
//! 3. When the page reaches the point for that target, the hook sends a
//!    [`Reached`] naming its backend and then blocks on
//!    `pg_advisory_xact_lock_shared(key)` inside the page transaction, with
//!    the session's `lock_timeout` lifted for that one wait. The test learns
//!    the worker is frozen from the channel, not by polling.
//! 4. The test releases its session lock. The page carries on.
//!
//! Each arming fires once. A page that never reaches its point (the step
//! doesn't run for this page) leaves the arming unfired; the test's driver
//! reports that as a failure when the drain returns.
//!
//! The blocking lock is an advisory lock rather than an in-process channel so
//! that the frozen worker is visible to Postgres as an ordinary lock wait in
//! `pg_locks`, with its page transaction open and every lock it took so far
//! still held.
//!
//! # Where the points sit
//!
//! The names are the ledger design's steps (ADR-0002, "Apply" and
//! "Re-derive"). A 1-1 target is on its ledger (`super::one_to_one_ledger`):
//!
//! | Point | 1-1 target |
//! |---|---|
//! | [`PausePoint::AfterPlaceholders`] | after the placeholder insert, before the entry lock |
//! | [`PausePoint::AfterEntryLock`] | after the sorted entry lock |
//! | [`PausePoint::AfterRederiveRead`] | directly after the one read-and-snapshot statement, when the page re-derives any key |
//! | [`PausePoint::BeforeCommit`] | after the page's last write |
//!
//! An aggregate (`super::group_basis`) fires `AfterEntryLock` after
//! its sorted group lock, before the statement that reads the groups' bases,
//! `AfterGroupUpsert` after that statement, and `BeforeCommit`.
//!
//! A `JOIN` target (`super::join`) fires, in order:
//! `BeforeJoinLock` after the pair's lock row, `AfterJoinKeyInsert` after
//! its `J` insert, `AfterJoinLock` after its sorted `J` lock,
//! `AfterEntryLock` after its `P` and `M` cell locks, `BeforeJoinTargetLock`
//! after its product, `AfterJoinTargetLock` after its `T` group lock,
//! `AfterGroupUpsert` after its `T` write, and `BeforeCommit`. A build's
//! cell write of a side fires [`PausePoint::AfterJoinCellMatch`] between
//! its match statement and its write.
//!
//! A 1-1 build chunk (`super::build::one_to_one::run_chunk`) fires
//! `AfterPlaceholders` and `AfterEntryLock` around its entry lock, and
//! `AfterRederiveRead` directly after its read-and-write statement
//! (uncommitted). [`PausePoint::BeforeChunkTransaction`] is a chunk's alone:
//! after a claimed chunk is planned and before its transaction begins (any
//! Re-derive chunk `super::build::run_rederive` runs, a field build's
//! included). A test runs either inside [`with_scope`] as it would a drain.
//!
//! A key release (`super::quarantine::release_key`) fires
//! [`PausePoint::BeforeReleaseCommit`] for the key's table after its last
//! write. A test runs it inside [`with_scope`] as it would a drain.
//!
//! Each pair of a column pause's cascade (`super::quarantine::cascade_pause`)
//! fires [`PausePoint::AfterCascadeFenceBump`] for the reader's transform
//! after its fence bump, before it checks its upstream column is still
//! paused. A test runs the pause inside [`with_scope`].
//!
//! The walk also fires [`PausePoint::AfterCascadeDependentsRead`] for an
//! upstream's transform after it reads that column's dependents, holding
//! nothing, before the first of their pairs.
//!
//! A define or `ALTER TRANSFORM` fires [`PausePoint::AfterUpstreamPausesRead`]
//! for its own target after it reads the paused columns its definition
//! reads. A test runs the define inside [`with_scope`].
//!
//! Each pair of a column resume (`super::quarantine::resume_column`) fires
//! [`PausePoint::AfterResumedColumnDeleted`] for its target after it deletes
//! the resumed column's row, before it releases that column's readers.
//! A resume of a column that stays paused with a sibling it reads
//! fires [`PausePoint::BeforeSiblingHeldResume`] for its target, holding the
//! column-pause lock, before it clears the column's `local_fuse`. The
//! walk fires [`PausePoint::AfterResumePairDefinitionRead`] for a pair's
//! target after it reads the pair's definition, holding nothing.
//! A test runs the resume inside [`with_scope`].
//!
//! A drain's eviction (`super::quarantine::isolate_and_evict`) fires
//! [`PausePoint::BeforeEvictionLocks`] for each definition's bare target
//! after its transaction begins, before it takes the definition's fuse gate
//! and row. A test runs the isolation inside [`with_scope`].
//!
//! A Re-derive plan job's batch (`super::build::run_plan`) fires
//! [`PausePoint::AfterPlanHold`] for the definition's target after it holds
//! the definition row and its claim, before it inserts its chunks. A test
//! runs the job inside [`with_scope`].
//!
//! Each page of a partition correction (`super::partition_correction`) fires
//! [`PausePoint::BeforeCorrectionCommit`] for the definition's target after
//! it has re-derived its keys and recorded its progress, before it commits.
//! A test runs the correction inside [`with_scope`]. A correction on the
//! group basis fires it after its recompute, and fires
//! [`PausePoint::BeforeCorrectionRecompute`] before it, once it has
//! enumerated the groups to recompute.
//!
//! A partitioned source's every-pass sweep (`crate::capture::complement`)
//! fires [`PausePoint::AfterSweepCatalogRead`] for the source after its
//! catalog reads, before its search. A test runs the reconcile pass
//! inside [`with_scope`].
//!
//! A partitioned source's deep check (`crate::capture::partitions`) fires
//! [`PausePoint::BeforeConstraintFetch`] for the source after it reads and
//! classifies the tree, before it fetches the constraint texts, and
//! [`PausePoint::BeforeFingerprintRead`] after the pass's check that the
//! source's name still holds the recorded root, before it reads the
//! fingerprints. A test runs the reconcile pass inside
//! [`with_scope`].
//!
//! A group-basis build or rebuild (`super::group_basis::rebuild`, run by a
//! fresh build's plan job or a rebuild's sweep job in `super::build`) fires
//! [`PausePoint::AfterRebuildEnumerate`] for the definition's target once it
//! has enumerated its groups, before it locks them, [`PausePoint::AfterRebuildLock`] for
//! the definition's target once it has locked its groups, before the
//! statement that takes its snapshot, and [`PausePoint::BeforeRebuildCommit`]
//! after its last write, before it commits. Over a partitioned
//! source the job is a transaction per leaf, and each fires
//! [`PausePoint::BeforeRebuildCommit`] (an armed point fires once, at the
//! first); between two of them it fires [`PausePoint::BetweenRebuildStructures`].
//! A test runs the job inside [`with_scope`].
//!
//! A later part that replaces a step moves its hook with it.
//!
//! # Stalls
//!
//! The generative concurrent tier can't freeze a worker by hand: its drains
//! run on the engine's own worker tasks, outside any [`PauseScope`]. What it
//! can do is make the entry-lock step slow now and then, the way a busy or
//! descheduled worker is ([`set_stall`]). While a stall is
//! set, every page or build chunk that reaches [`PausePoint::AfterPlaceholders`]
//! or [`PausePoint::AfterEntryLock`] sleeps a random time up to it, mostly
//! short (the cube of a uniform draw, so about one in five sleeps is over
//! half the maximum). Those two points bound the windows two races need:
//! between a page's placeholder insert and its entry lock, a tombstone it
//! found can be collected (the `early_tombstone_gc` plant) while a later
//! segment drains past it; between its entry lock and its write, a build
//! chunk that skipped the lock reads the entry stale (the
//! `chunk_without_entry_lock` plant). A stall changes when things happen,
//! never what any step does, so a correct engine converges under it.
//!
//! It applies wherever the table above puts those two points, on purpose: a
//! 1-1 page, an aggregate target's page (at its group lock) and a build
//! chunk alike. A busy worker is slow at whatever page it is running, and a
//! chunk racing a 1-1 page needs that page held as much as one on its own
//! path does.
//!
//! A group-basis build or rebuild stalls the same way between its
//! enumeration and its lock statement ([`PausePoint::AfterRebuildEnumerate`],
//! a busy build worker), where a page that commits is seen by a snapshot
//! taken after the locks and missed by one taken before them (the
//! `gb_build_snapshot_before_locks` plant).
//!
//! A commit stall ([`set_commit_stall`]) does the same at
//! [`PausePoint::BeforeCommit`], between a page's last write and its
//! commit: the window in which another page that skipped a lock reads what
//! the first wrote but hasn't committed (the `JOIN` tier's `join_no_j_lock`
//! and `join_t_skip_lock` plants).
//!
//! A stall is process-wide, like a plant, and an armed [`PauseScope`] point
//! ignores it. Every page of every engine in the process takes it, so a
//! harness that sets one runs nothing else in that process meanwhile (the
//! generative tier runs its steady-load cases only in its planted-bug
//! sweep's own processes) and clears it when the run ends, however it ends.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_postgres::GenericClient;

/// A step of a drain page's transaction a test can freeze a worker after.
/// See the module doc for where each sits today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PausePoint {
    /// After a 1-1 target's placeholder insert, before its entry lock:
    /// where a tombstone the insert found can be collected.
    AfterPlaceholders,
    /// After the page has locked the entries (on an aggregate, the group
    /// rows; on a `JOIN`, its `P` and `M` cells) it will write.
    AfterEntryLock,
    /// On a `JOIN` target (`super::join`), at the start of a build's write
    /// (`super::join::build`): the definition row and the job row are
    /// held, and no hidden table has been read yet.
    BeforeJoinBuild,
    /// On a `JOIN` target (`super::join`), after the page took the pair's
    /// lock row, before it creates and locks its `J` rows.
    BeforeJoinLock,
    /// On a `JOIN` target, after the page's insert of the `J` rows it
    /// names, before its sorted lock of them.
    AfterJoinKeyInsert,
    /// On a `JOIN` target, after the page locked its `J` rows, before it
    /// locks its `P` and `M` cells.
    AfterJoinLock,
    /// On a `JOIN` target, after each side's cells are written (the `FROM`
    /// side's, then the joined side's), before the next side or the product:
    /// a build's cross term (`super::join::build`).
    BetweenJoinSideWrites,
    /// On a `JOIN` target, in a build's cell write of one side
    /// (`super::group_basis::write_join_build_cells`), after the statement
    /// that matches each cell to its hidden row's `ctid` and before the
    /// write from that match. Waits as [`pause_between_transactions`] does,
    /// holding no snapshot, as nothing does between the two statements.
    AfterJoinCellMatch,
    /// On a `JOIN` target, after the page's product of its `P` and `M`
    /// changes (and a build's enumeration of `T`'s groups), before its lock
    /// of the `T` groups.
    BeforeJoinTargetLock,
    /// On a `JOIN` target, after the page applied its `P` and `M` changes and
    /// locked the `T` groups their product changes, before it writes them.
    AfterJoinTargetLock,
    /// After a Re-derive's live read of the source (on a 1-1 ledger, a
    /// forced group recompute).
    AfterRederiveRead,
    /// After a claimed build chunk is planned, before its transaction
    /// begins (`super::build::run_rederive`): the plan read the
    /// definition, its fields and the range's keys, and holds no lock and
    /// no transaction id, so the ring seals and drains around it.
    BeforeChunkTransaction,
    /// After the page's group increments (aggregate targets only).
    AfterGroupUpsert,
    /// After every write the page makes, just before it commits.
    BeforeCommit,
    /// After every write a key release makes (`super::quarantine::release_key`),
    /// just before it commits, for the key's table (its canonical name): the
    /// release holds the table's fence and the definition's row.
    BeforeReleaseCommit,
    /// After a column pause cascade's pair bumps its reader's fence, before
    /// it takes the column-pause lock and reads its upstream column's
    /// `column_status` row, for the reader's transform
    /// (`super::quarantine::cascade_pause`).
    AfterCascadeFenceBump,
    /// After a column pause cascade's walk reads the dependents of an
    /// upstream column, before it goes to the first of their pairs, for the
    /// upstream's transform (`super::quarantine::cascade_pause`). The
    /// walk holds no lock and no transaction, so an `ALTER TRANSFORM` of a
    /// reader it just listed can commit here.
    AfterCascadeDependentsRead,
    /// After a define or `ALTER TRANSFORM` reads the paused columns of the
    /// targets its definition reads, holding the column-pause lock (shared
    /// for a define, exclusive for an edit), before it pauses its
    /// readers of them, for its own (bare) target
    /// (`defs::catalog`'s `pause_readers_of_paused_columns`).
    AfterUpstreamPausesRead,
    /// After a drain's eviction transaction begins, before it takes a
    /// definition's fuse gate and row lock and writes the poison for its
    /// charged keys, for the definition's bare target
    /// (`super::quarantine::isolate_and_evict`). The isolation has
    /// charged the keys and committed; the eviction holds no lock of this
    /// definition's yet (it may hold a lower-id definition's).
    BeforeEvictionLocks,
    /// After a column resume's walk reads the definition of a pair's target,
    /// before it opens the pair's transaction, for the pair's (bare) target
    /// (`super::quarantine::resume_pairs`). The walk holds no lock and
    /// no transaction, so an `ALTER TRANSFORM` of that definition can commit
    /// here.
    AfterResumePairDefinitionRead,
    /// After a column resume deletes the `column_status` row of the column
    /// it resumes, before it releases that column's readers, for the
    /// column's (bare) target (`super::quarantine::resume_column`).
    AfterResumedColumnDeleted,
    /// After a column resume finds its column reads a sibling field still
    /// paused, holding the column-pause lock, before it clears the column's
    /// own reason (`local_fuse`) and leaves it paused with that sibling, for
    /// the column's (bare) target (`super::quarantine::resume_column`).
    BeforeSiblingHeldResume,
    /// After a Re-derive plan job's batch transaction holds the job's claim
    /// and before it inserts the batch's chunks, for the definition's
    /// target (`super::build::run_plan`). The batch holds the definition
    /// row `for key share` and the plan row, and has written nothing.
    AfterPlanHold,
    /// After a worker claims a `self_check` job and before the page it runs
    /// (`super::self_check_job::work_once`), for the audited target's
    /// bare name. The page holds no lock and no transaction, so the target
    /// can be dropped, written or checked again here.
    BeforeSelfCheckPage,
    /// After a partition correction's page has re-derived its keys and
    /// recorded its progress, just before it commits, for the definition's
    /// target (`super::partition_correction`, ADR-0017 P4a). The page holds
    /// its keys' entry locks and its row's claim fence.
    BeforeCorrectionCommit,
    /// After a group-basis partition correction has enumerated the groups
    /// that route to its node, in its own committed statement, and before
    /// it locks and recomputes them, for the definition's target
    /// (`super::partition_correction`). Its transaction holds only
    /// its row's claim fence, so the source can be written and drained here.
    BeforeCorrectionRecompute,
    /// After the every-pass sweep of a partitioned source has read the
    /// lifecycle fingerprint, the bounds and the queued corrections, before
    /// it searches the targets, for the source's unquoted `schema.table`
    /// (`capture::complement`). The sweep's `READ COMMITTED`
    /// transaction holds no lock, so a leaf can be created, written and
    /// drained here.
    AfterSweepCatalogRead,
    /// After a deep check of a partitioned source has read and classified
    /// the tree, before it fetches the new or changed nodes' constraint
    /// texts, for the source's unquoted `schema.table`
    /// (`capture::partitions`). The pass holds no transaction, so a
    /// node it read can be detached or dropped here.
    BeforeConstraintFetch,
    /// After the capture pass's check that a partitioned source's name still
    /// holds the recorded root, before the deep check reads the fingerprints,
    /// for the source's unquoted `schema.table` (`capture::partitions`).
    /// The pass holds no transaction, so the root can be swapped by
    /// renames here.
    BeforeFingerprintRead,
    /// After a group-basis build or rebuild has enumerated the groups it
    /// covers, before the statement that locks them, for the definition's
    /// target (`super::group_basis::rebuild`). No group is locked
    /// here: a page of any of them applies now, and a group a page creates
    /// is not enumerated.
    AfterRebuildEnumerate,
    /// After a group-basis build or rebuild has locked every group it covers, before
    /// the statement that takes its snapshot and replaces them, for the
    /// definition's target (`super::group_basis::rebuild`). Pages of
    /// those groups queue on their locks here; a group a page creates now
    /// isn't locked.
    AfterRebuildLock,
    /// After a group-basis rebuild has replaced every group and marked its
    /// job done, just before it commits, for the definition's target
    /// (`super::build`'s rebuild job). Readers still see the old
    /// values here.
    BeforeRebuildCommit,
    /// Between two transactions of a rebuild of a partitioned source's
    /// leaves, after one leaf committed and before the next begins, for the
    /// definition's target (`super::build`'s rebuild job). Waits as
    /// [`pause_between_transactions`] does, holding no snapshot.
    BetweenRebuildStructures,
}

/// What a frozen worker reports when it reaches its armed point.
#[derive(Debug, Clone, Copy)]
pub struct Reached {
    /// The point it reached.
    pub point: PausePoint,
    /// The Postgres backend running the frozen page transaction. A test
    /// waits on `pg_blocking_pids` naming it to know a second worker is
    /// queued behind the first.
    pub backend_pid: i32,
}

struct Arming {
    point: PausePoint,
    target: String,
    lock_key: i64,
    reached: oneshot::Sender<Reached>,
}

/// The points armed for one drain task. See the module doc.
#[derive(Default)]
pub struct PauseScope {
    armings: Mutex<Vec<Arming>>,
}

impl PauseScope {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Arms `point` for `target` (its qualified identity, `schema.table`).
    /// The worker blocks on `lock_key`, which the caller must already hold
    /// as a session advisory lock on a connection that waits on nothing
    /// else, or Postgres's deadlock detector can end the test.
    pub fn arm(
        &self,
        point: PausePoint,
        target: &str,
        lock_key: i64,
    ) -> oneshot::Receiver<Reached> {
        let (reached, rx) = oneshot::channel();
        self.armings.lock().expect("pause scope lock").push(Arming {
            point,
            target: target.to_string(),
            lock_key,
            reached,
        });
        rx
    }

    fn take(&self, point: PausePoint, target: &str) -> Option<Arming> {
        let mut armings = self.armings.lock().expect("pause scope lock");
        let at = armings
            .iter()
            .position(|a| a.point == point && a.target == target)?;
        Some(armings.swap_remove(at))
    }
}

/// The longest stall, in microseconds; 0 for none. See the module doc's
/// "Stalls".
static STALL_MAX_MICROS: AtomicU64 = AtomicU64::new(0);

/// Sets the longest stall a page or a build chunk takes at its entry-lock
/// step, or clears it with `None`. See the module doc's "Stalls".
pub fn set_stall(max: Option<Duration>) {
    let micros = max.map_or(0, |max| u64::try_from(max.as_micros()).unwrap_or(u64::MAX));
    STALL_MAX_MICROS.store(micros, Ordering::Relaxed);
}

/// The longest commit stall, in microseconds; 0 for none. See the module
/// doc's "Stalls".
static COMMIT_STALL_MAX_MICROS: AtomicU64 = AtomicU64::new(0);

/// Sets the longest stall a page takes between its last write and its
/// commit ([`PausePoint::BeforeCommit`]), or clears it with `None`. See the
/// module doc's "Stalls".
pub fn set_commit_stall(max: Option<Duration>) {
    let micros = max.map_or(0, |max| u64::try_from(max.as_micros()).unwrap_or(u64::MAX));
    COMMIT_STALL_MAX_MICROS.store(micros, Ordering::Relaxed);
}

/// Sleeps a random time up to the set stall, at the points it applies to.
async fn stall(point: PausePoint) {
    let max = match point {
        PausePoint::AfterPlaceholders
        | PausePoint::AfterEntryLock
        | PausePoint::AfterRebuildEnumerate => STALL_MAX_MICROS.load(Ordering::Relaxed),
        PausePoint::BeforeCommit => COMMIT_STALL_MAX_MICROS.load(Ordering::Relaxed),
        _ => return,
    };
    if max == 0 {
        return;
    }
    // A splitmix64 step over a shared counter: no RNG dependency for a
    // test-only jitter that needs no quality beyond "spread out".
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE
        .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    let unit = (x >> 11) as f64 / (1u64 << 53) as f64;
    let micros = (unit * unit * unit * max as f64) as u64;
    tokio::time::sleep(Duration::from_micros(micros)).await;
}

tokio::task_local! {
    static SCOPE: Arc<PauseScope>;
}

/// Runs `drain` with `scope`'s points armed for it alone.
pub async fn with_scope<F: Future>(scope: Arc<PauseScope>, drain: F) -> F::Output {
    SCOPE.scope(scope, drain).await
}

/// The hook: unless the running task's [`PauseScope`] armed `point` for
/// `target`, a no-op, or a stall when one is set (see the module doc's
/// "Stalls"). When it is armed, it reports [`Reached`] and blocks, inside
/// the page transaction `client` is running, until the test releases the
/// lock.
pub(crate) async fn pause_at<C: GenericClient>(
    client: &C,
    point: PausePoint,
    target: &str,
) -> Result<(), tokio_postgres::Error> {
    let Some(arming) = SCOPE
        .try_with(|scope| scope.take(point, target))
        .ok()
        .flatten()
    else {
        stall(point).await;
        return Ok(());
    };
    // The page's session caps every lock wait at `locks::LOCK_TIMEOUT` (I7),
    // and the pause is a lock wait. Capped, a pause the test held past it
    // would roll the page back with `55P03`, the drain would retry the page
    // with the arming already spent, and the test would carry on against an
    // interleaving it never forced, and could pass. So the pause's own wait
    // is uncapped, and the page gets its setting back for everything after.
    let row = client
        .query_one(
            "select pg_backend_pid(), current_setting('lock_timeout')",
            &[],
        )
        .await?;
    let (backend_pid, lock_timeout): (i32, String) = (row.get(0), row.get(1));
    client
        .execute("select set_config('lock_timeout', '0', true)", &[])
        .await?;
    let _ = arming.reached.send(Reached { point, backend_pid });
    client
        .execute(
            "select pg_advisory_xact_lock_shared($1)",
            &[&arming.lock_key],
        )
        .await?;
    client
        .execute(
            "select set_config('lock_timeout', $1, true)",
            &[&lock_timeout],
        )
        .await?;
    Ok(())
}

/// [`pause_at`] for a point between transactions: it waits by polling the
/// lock in short statements, so the waiting session holds no snapshot. A
/// `DETACH ... CONCURRENTLY` or `FINALIZE` waits for every older snapshot,
/// so a wait inside one statement would block the very DDL the window is
/// for, which production's gap between two transactions does not.
pub(crate) async fn pause_between_transactions<C: GenericClient>(
    client: &C,
    point: PausePoint,
    target: &str,
) -> Result<(), tokio_postgres::Error> {
    let Some(arming) = SCOPE
        .try_with(|scope| scope.take(point, target))
        .ok()
        .flatten()
    else {
        return Ok(());
    };
    let backend_pid: i32 = client
        .query_one("select pg_backend_pid()", &[])
        .await?
        .get(0);
    let _ = arming.reached.send(Reached { point, backend_pid });
    loop {
        let free: bool = client
            .query_one(
                "select pg_try_advisory_xact_lock_shared($1)",
                &[&arming.lock_key],
            )
            .await?
            .get(0);
        if free {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A pause outlasts the page's `lock_timeout`, and the page gets that
    /// timeout back once released.
    #[tokio::test]
    async fn a_pause_is_not_cut_short_by_the_page_lock_timeout() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let gate = db.pool.get().await.expect("gate connection");
        gate.execute("select pg_advisory_lock(623)", &[])
            .await
            .expect("take the pause lock");

        let scope = PauseScope::new();
        let reached = scope.arm(PausePoint::BeforeCommit, "public.t", 623);
        let pool = db.pool.clone();
        let page = tokio::spawn(with_scope(scope, async move {
            let mut client = pool.get().await.expect("page connection");
            let txn = client.transaction().await.expect("begin");
            txn.batch_execute("set local lock_timeout = 50")
                .await
                .expect("a short page lock_timeout");
            pause_at(&*txn, PausePoint::BeforeCommit, "public.t").await?;
            let after: String = txn
                .query_one("select current_setting('lock_timeout')", &[])
                .await?
                .get(0);
            txn.commit().await?;
            Ok::<_, tokio_postgres::Error>(after)
        }));

        reached.await.expect("the page reached its pause");
        // Well past the page's 50 ms: capped, the pause would have failed
        // with `55P03` and the page would have finished by now.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!page.is_finished(), "the page is still paused");
        gate.execute("select pg_advisory_unlock(623)", &[])
            .await
            .expect("release the pause lock");
        let after = page
            .await
            .expect("page task")
            .expect("the pause ended by release, not by lock_timeout");
        assert_eq!(after, "50ms", "the page's lock_timeout is back");
    }
}
