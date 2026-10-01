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
//! "Re-derive"). A plain `SUM`/`AVG`/`COUNT` aggregate is on the ledger since #623
//! D3 (`super::ledger`); every other target has no entry lock and no ledger
//! Re-derive yet, so each point sits at the step that plays that role now:
//!
//! | Point | 1-1 target | Aggregate target on the ledger | Other aggregate target |
//! |---|---|---|---|
//! | [`PausePoint::AfterPlaceholders`] | never reached | after the placeholder insert, before the entry lock (each time it is taken) | never reached |
//! | [`PausePoint::AfterEntryLock`] | after the per-key stripe locks (`lock_one_to_one_keys`, #344) | after the sorted entry lock | after the group pre-lock (`prelock_sql`) |
//! | [`PausePoint::AfterRederiveRead`] | after the live re-read (`reconcile_with_source`) | directly after the one read-and-snapshot statement, when the page re-derives any key | after the forced groups' live re-read (`apply_forced_groups_bulk`), when the page forces any |
//! | [`PausePoint::AfterGroupUpsert`] | never reached | after the entries-and-groups statement | after the delta groups' upsert |
//! | [`PausePoint::BeforeCommit`] | after the page's last write | after the page's last write | after the page's last write |
//!
//! A build chunk (`super::build::run_chunk`, #625 F1) fires the ledger
//! column's first three points for its target too: `AfterPlaceholders` and
//! `AfterEntryLock` around the same entry lock, and `AfterRederiveRead`
//! directly after its one read-and-write statement (the entries rewritten
//! and the deltas appended, uncommitted). The merger
//! (`super::build::merge_deltas`) fires `AfterGroupUpsert` after its one
//! statement, holding its claimed delta rows and its groups. A test runs
//! either inside [`with_scope`] as it would a drain.
//!
//! A later part that replaces a step moves its hook with it.

use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;
use tokio_postgres::GenericClient;

/// A step of a drain page's transaction a test can freeze a worker after.
/// See the module doc for where each sits today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PausePoint {
    /// After a ledger target's placeholder insert, before its entry lock
    /// (#623 D7): where a tombstone the insert found can be collected.
    AfterPlaceholders,
    /// After the page has locked the entries (today: stripes or group rows)
    /// it will write.
    AfterEntryLock,
    /// After a Re-derive's live read of the source (today: the 1-1 re-read
    /// or a forced group recompute).
    AfterRederiveRead,
    /// After the page's group increments (aggregate targets only).
    AfterGroupUpsert,
    /// After every write the page makes, just before it commits.
    BeforeCommit,
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

tokio::task_local! {
    static SCOPE: Arc<PauseScope>;
}

/// Runs `drain` with `scope`'s points armed for it alone.
pub async fn with_scope<F: Future>(scope: Arc<PauseScope>, drain: F) -> F::Output {
    SCOPE.scope(scope, drain).await
}

/// The hook: a no-op unless the running task's [`PauseScope`] armed `point`
/// for `target`. Then it reports [`Reached`] and blocks, inside the page
/// transaction `client` is running, until the test releases the lock.
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
