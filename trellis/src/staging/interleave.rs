//! Test-only pause points inside a drain page's apply transaction (epic
//! #556, #623 D1). **Compiled only under `cfg(any(test, feature =
//! "test-util"))`**, like [`crate::plant`] and the pre-commit pause in
//! [`super::apply`].
//!
//! The interleaving tests (`tests/ledger_interleavings.rs`) freeze one drain
//! worker between two steps of its page transaction, do something else (commit
//! a source write, drain another batch on another worker), then let it go. No
//! test sleeps or polls for convergence (#297).
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
//! "Re-derive"). Every aggregate target is on the ledger since #623 D5
//! (`super::ledger`) and a 1-1 target since D6 (`super::one_to_one_ledger`):
//!
//! | Point | 1-1 target | Aggregate target |
//! |---|---|---|
//! | [`PausePoint::AfterPlaceholders`] | after the placeholder insert, before the entry lock | after the placeholder insert, before the entry lock (each time it is taken) |
//! | [`PausePoint::AfterEntryLock`] | after the sorted entry lock | after the sorted entry lock |
//! | [`PausePoint::AfterRederiveRead`] | directly after the one read-and-snapshot statement, when the page re-derives any key | directly after the one read-and-snapshot statement, when the page re-derives any key |
//! | [`PausePoint::AfterGroupUpsert`] | never reached | after the entries-and-groups statement |
//! | [`PausePoint::AfterReverseGuards`] | after a relationship reverse record's guards, named by the projection | the same |
//! | [`PausePoint::BeforeCommit`] | after the page's last write | after the page's last write |
//!
//! A build chunk (`super::build::run_chunk`, #625 F1) fires the ledger
//! column's first three points for its target too: `AfterPlaceholders` and
//! `AfterEntryLock` around the entry lock of the keys that had an entry
//! (after its insert of the others', #723), and `AfterRederiveRead`
//! directly after its last read-and-write statement (the entries written
//! and the deltas appended, uncommitted). Three points are a chunk's alone:
//! [`PausePoint::BeforeChunkTransaction`], after a claimed chunk is planned
//! and before its transaction begins (any Re-derive chunk
//! `super::build::run_rederive` runs, a field build's included),
//! [`PausePoint::BeforeChunkInsert`], after its read of the keys and before
//! its insert, and [`PausePoint::AfterChunkSnapshot`], inside the insert,
//! after its snapshot ([`pause_in_statement`]). The merger
//! (`super::build::merge_deltas`) fires `AfterGroupUpsert` after its one
//! statement, holding its claimed delta rows and its groups. A test runs
//! either inside [`with_scope`] as it would a drain.
//!
//! A key release (`super::quarantine::release_key`) fires
//! [`PausePoint::BeforeReleaseCommit`] for the key's table after its last
//! write. A test runs it inside [`with_scope`] as it would a drain.
//!
//! Each pair of a column pause's cascade (`super::quarantine::cascade_pause`)
//! fires [`PausePoint::AfterCascadeFenceBump`] for the reader's transform
//! after its fence bump, before it checks its upstream column is still
//! paused (#912). A test runs the pause inside [`with_scope`].
//!
//! The walk also fires [`PausePoint::AfterCascadeDependentsRead`] for an
//! upstream's transform after it reads that column's dependents, holding
//! nothing, before the first of their pairs (#955).
//!
//! A define or `ALTER TRANSFORM` fires [`PausePoint::AfterUpstreamPausesRead`]
//! for its own target after it reads the paused columns its definition
//! reads (#914). A test runs the define inside [`with_scope`].
//!
//! Each pair of a column resume (`super::quarantine::resume_column`) fires
//! [`PausePoint::AfterResumedColumnDeleted`] for its target after it deletes
//! the resumed column's row, before it releases that column's readers
//! (#917). A resume of a column that stays paused with a sibling it reads
//! fires [`PausePoint::BeforeSiblingHeldResume`] for its target, holding the
//! column-pause lock, before it clears the column's `local_fuse` (#922). A
//! test runs the resume inside [`with_scope`].
//!
//! A drain's eviction (`super::quarantine::isolate_and_evict`) fires
//! [`PausePoint::BeforeEvictionLocks`] for each definition's bare target
//! after its transaction begins, before it takes the definition's fuse gate
//! and row (#880). A test runs the isolation inside [`with_scope`].
//!
//! A later part that replaces a step moves its hook with it.
//!
//! # Stalls
//!
//! The generative concurrent tier can't freeze a worker by hand: its drains
//! run on the engine's own worker tasks, outside any [`PauseScope`]. What it
//! can do is make the entry-lock step slow now and then, the way a busy or
//! descheduled worker is ([`set_stall`], #720 and #725). While a stall is
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
//! 1-1 page, a ledger page, another aggregate target's page (at its group
//! pre-lock) and a build chunk alike. A busy worker is slow at whatever page
//! it is running, and a chunk racing a 1-1 page, or a ledger page racing an
//! old-path page of the same burst, needs that page held as much as one on
//! its own path does.
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
    /// After a ledger target's placeholder insert, before its entry lock
    /// (#623 D7): where a tombstone the insert found can be collected.
    AfterPlaceholders,
    /// After the page has locked the entries (off the ledger, the group
    /// rows) it will write.
    AfterEntryLock,
    /// After a Re-derive's live read of the source (off the ledger, a
    /// forced group recompute).
    AfterRederiveRead,
    /// After a claimed build chunk is planned, before its transaction
    /// begins (`super::build::run_rederive`, #832): the plan read the
    /// definition, its fields and the range's keys, and holds no lock and
    /// no transaction id, so the ring seals and drains around it.
    BeforeChunkTransaction,
    /// After a build chunk's read of its keys, before its insert of the
    /// entries of those that have none (#723).
    BeforeChunkInsert,
    /// Inside a build chunk's insert of its new keys' entries, after the
    /// statement's snapshot and before its first insert (#723).
    AfterChunkSnapshot,
    /// After the page's group increments (aggregate targets only).
    AfterGroupUpsert,
    /// After a page's guards for a relationship reverse record, for the
    /// relationship's projection (its quoted, qualified name), with the
    /// projection rows the page locks for its reverses held (#848).
    AfterReverseGuards,
    /// After every write the page makes, just before it commits.
    BeforeCommit,
    /// After every write a key release makes (`super::quarantine::release_key`),
    /// just before it commits, for the key's table (its canonical name): the
    /// release holds the table's fence, the definition's row, and each to-one
    /// relationship's refresh stamp and projection rows it wrote (#831).
    BeforeReleaseCommit,
    /// After a column pause cascade's pair bumps its reader's fence, before
    /// it takes the column-pause lock and reads its upstream column's
    /// `column_status` row, for the reader's transform
    /// (`super::quarantine::cascade_pause`, #912).
    AfterCascadeFenceBump,
    /// After a column pause cascade's walk reads the dependents of an
    /// upstream column, before it goes to the first of their pairs, for the
    /// upstream's transform (`super::quarantine::cascade_pause`, #955). The
    /// walk holds no lock and no transaction, so an `ALTER TRANSFORM` of a
    /// reader it just listed can commit here.
    AfterCascadeDependentsRead,
    /// After a define or `ALTER TRANSFORM` reads the paused columns of the
    /// targets its definition reads, holding the column-pause lock (shared
    /// for a define, exclusive for an edit, #922), before it pauses its
    /// readers of them, for its own (bare) target
    /// (`defs::catalog`'s `pause_readers_of_paused_columns`, #914).
    AfterUpstreamPausesRead,
    /// After a drain's eviction transaction begins, before it takes a
    /// definition's fuse gate and row lock and writes the poison for its
    /// charged keys, for the definition's bare target
    /// (`super::quarantine::isolate_and_evict`, #880). The isolation has
    /// charged the keys and committed; the eviction holds no lock of this
    /// definition's yet (it may hold a lower-id definition's).
    BeforeEvictionLocks,
    /// After a column resume deletes the `column_status` row of the column
    /// it resumes, before it releases that column's readers, for the
    /// column's (bare) target (`super::quarantine::resume_column`, #917).
    AfterResumedColumnDeleted,
    /// After a column resume finds its column reads a sibling field still
    /// paused, holding the column-pause lock, before it clears the column's
    /// own reason (`local_fuse`) and leaves it paused with that sibling, for
    /// the column's (bare) target (`super::quarantine::resume_column`, #922).
    BeforeSiblingHeldResume,
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

/// Sleeps a random time up to the set stall, at the two points it applies
/// to.
async fn stall(point: PausePoint) {
    if !matches!(
        point,
        PausePoint::AfterPlaceholders | PausePoint::AfterEntryLock
    ) {
        return;
    }
    let max = STALL_MAX_MICROS.load(Ordering::Relaxed);
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

/// The in-statement hook, for a point inside one statement
/// ([`PausePoint::AfterChunkSnapshot`]): unless the running task's
/// [`PauseScope`] armed `point` for `target`, `None`. When it is armed, it
/// lifts `client`'s `lock_timeout` for the rest of the transaction, reports
/// [`Reached`] and returns the lock key, which the caller's statement then
/// waits on (`pg_advisory_xact_lock_shared`). The caller sets the timeout it
/// wants back after the statement.
///
/// [`Reached`] is sent before the statement starts, so a test that needs the
/// statement's snapshot taken waits until the backend is queued on the lock
/// (`pg_blocking_pids`).
pub(crate) async fn pause_in_statement<C: GenericClient>(
    client: &C,
    point: PausePoint,
    target: &str,
) -> Result<Option<i64>, tokio_postgres::Error> {
    let Some(arming) = SCOPE
        .try_with(|scope| scope.take(point, target))
        .ok()
        .flatten()
    else {
        return Ok(None);
    };
    let backend_pid: i32 = client
        .query_one("select pg_backend_pid()", &[])
        .await?
        .get(0);
    client
        .execute("select set_config('lock_timeout', '0', true)", &[])
        .await?;
    let _ = arming.reached.send(Reached { point, backend_pid });
    Ok(Some(arming.lock_key))
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
