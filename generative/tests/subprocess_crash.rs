//! Issue #166: the settled-state model's Phase 3 (apply ∪ mark-drained)
//! atomicity claim, proven across a **real** crash — a genuine `SIGKILL` of
//! a real OS process, not `ManualBackend::restart`'s in-process simulation
//! (see that method's own doc comment for exactly why it can't stand in for
//! this). This is the last unmet piece of issue #138's original scope ("plus
//! a SIGKILL during Phase 3, so the re-drain must redo the moves and the
//! projection advance together"), deliberately deferred out of #138 rather
//! than faked, and filed as its own issue for exactly this reason.
//!
//! Uses `generative::backend::SubprocessBackend` (issue #166) against the
//! relationship-interleaving scenario `generative::generate::
//! build_relationship_interleaving_scenario` builds for #138 — a to-one
//! relationship whose parent-side change and a from-side re-point land
//! back-to-back, no intervening `quiesce()`, so whichever Phase 3 pass
//! drains them exercises both the ordinary target write *and* the
//! settled-parent projection gen bump/relationship reverse-delta machinery
//! (epic #127) in the same batch — the "moves and the projection advance
//! together" #138 called out. `SubprocessBackend::arm_pause_before_commit`
//! (driving `staging::apply`'s test-only pre-commit pause hook)
//! lands the `SIGKILL` deterministically inside that batch's still-open,
//! uncommitted Phase 3 transaction, rather than hoping timing luck hits the
//! window — see `SubprocessBackend`'s own module doc comment for the full
//! mechanism.

use std::time::{Duration, Instant};

use generative::backend::{Backend, SubprocessBackend};
use generative::generate::{RelInterleavingVariant, build_relationship_interleaving_scenario};
use generative::run::check_program;
use testkit::TestCluster;
use tokio_postgres::NoTls;
use trellis::dev::staging::producer_singleton_lock_key;
use trellis::{Config, Pool};

/// How long to wait for the primary engine subprocess to reach the
/// pre-commit pause point after the critical op pair is applied. Generous
/// (this suite's programs are tiny; a real hang here means something is
/// genuinely wrong, not that the window is just narrow) — mirrors
/// `ManualBackend`'s `QUIESCE_TIMEOUT`.
const PAUSE_TIMEOUT: Duration = Duration::from_secs(15);

/// The regression test issue #166 exists for: `SIGKILL` a real engine
/// subprocess while it is sitting inside Phase 3's still-open transaction —
/// every write already issued against it, nothing yet committed — then
/// confirm a freshly spawned subprocess redrives the same batch and the
/// system converges exactly as if the crash had never happened. Proves
/// Phase 3 is atomic across a genuine crash (not just this suite's
/// in-process `ManualBackend::restart` simulation): either both the target
/// write and the relationship projection advance land, or (as here, since
/// the kill always lands pre-commit) neither does and the redrive redoes
/// both together.
#[tokio::test(flavor = "multi_thread")]
async fn a_sigkill_mid_phase_3_drain_still_converges_on_redrive() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // The #138 relationship-interleaving scenario (epic #127's to-one
    // relationship delta path — see this file's own doc comment): a parent
    // row's own field changes while, in the very next op, a from-side row
    // re-points onto it. `parent_op`/`from_side_op` mark the two ops meant
    // to be applied back-to-back with no intervening `quiesce()`; every op
    // before `parent_op` is ordinary seeding.
    let scenario =
        build_relationship_interleaving_scenario(RelInterleavingVariant::ParentFieldUpdate);
    let program = scenario.program;

    let engine_bin = env!("CARGO_BIN_EXE_engine_subprocess");
    let mut backend = SubprocessBackend::connect(db.dsn(), engine_bin)
        .await
        .expect("connect subprocess backend");

    backend
        .install(&program)
        .await
        .expect("install program (spawns the primary engine subprocess)");

    // Seed every op strictly before the critical pair, quiescing after each
    // — safe, ordinary traffic establishing the parent/from-side rows the
    // critical pair then interleaves on.
    for op in &program.ops[..scenario.parent_op] {
        backend.apply(op).await.expect("apply seed op");
        backend.quiesce().await.expect("quiesce after seed op");
    }

    // Arm the pre-commit pause hook, then apply the two critical ops
    // back-to-back with no intervening quiesce: whichever Phase 3 pass
    // first attempts to commit — draining one or both, coalesced or not —
    // is the one the hook parks, mid-transaction, before it can persist.
    backend
        .arm_pause_before_commit()
        .expect("arm the Phase 3 pre-commit pause hook");
    backend
        .apply(&program.ops[scenario.parent_op])
        .await
        .expect("apply the parent-side op");
    backend
        .apply(&program.ops[scenario.from_side_op])
        .await
        .expect("apply the from-side op");

    let paused = backend.wait_for_pause(PAUSE_TIMEOUT).await;
    assert!(
        paused,
        "the primary engine subprocess must reach the Phase 3 pre-commit pause within {PAUSE_TIMEOUT:?} \
         — if this fires, the pause hook (staging::apply::pause_before_commit_for_tests) \
         never engaged, so nothing below would actually be testing a mid-transaction kill"
    );

    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // Anti-vacuity guard. Everything below only *means* something if the
    // crash actually destroys work the system then has to redo: if the
    // critical pair had somehow already landed before the pause engaged,
    // the final convergence check would pass no matter how badly broken
    // Phase 3's atomicity was. So pin the precondition explicitly — while
    // the engine is parked mid-transaction, the oracle must still see the
    // target side as diverged from the settled-state model. (A plain
    // `SELECT` here can't block on the paused transaction: it only ever
    // wrote rows, and this reads through a separate connection under MVCC.)
    let mid_crash = backend
        .snapshot()
        .await
        .expect("snapshot while the engine is paused mid-Phase-3");
    let mid_crash_diverged = check_program(&pool, &program, &mid_crash)
        .await
        .expect("oracle check must run mid-pause");
    assert!(
        !mid_crash_diverged.is_empty(),
        "the paused Phase 3 batch must still have real, uncommitted work in it — if the system \
         already matches the model here, the SIGKILL below destroys nothing and the final \
         convergence assertion passes vacuously"
    );

    // Disarm before restarting: the respawned subprocess inherits the same
    // trigger-file path and must not immediately re-pause while redraining
    // the very batch the killed process never got to commit.
    backend.disarm_pause_before_commit();

    // The claims the paused primary holds, read before the restart: the
    // fresh subprocess is already draining when `restart` returns, so its
    // own claims must not be aged below (issue #756). A plain `SELECT`
    // doesn't block on the paused transaction, as above.
    let killed_claimants: Vec<String> = pool
        .get()
        .await
        .expect("pool connection")
        .query(
            &format!(
                "select distinct claimed_by from {}.seg_claims",
                trellis::config::DEFAULT_SCHEMA
            ),
            &[],
        )
        .await
        .expect("read the paused primary's claimants")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert!(
        !killed_claimants.is_empty(),
        "the primary, paused mid-Phase-3, must hold the claims of the batch it is draining"
    );

    backend
        .restart()
        .await
        .expect("restart (SIGKILL the paused primary, then spawn a fresh one)");

    // Confirm the kill really was a SIGKILL, not a clean exit that happened
    // to race `restart` — otherwise this test would silently degrade into
    // exercising an ordinary shutdown instead of a real crash.
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        let status = backend
            .last_kill_status()
            .expect("restart must record the killed child's exit status");
        assert_eq!(
            status.signal(),
            Some(9),
            "the primary engine subprocess must have been terminated by SIGKILL (signal 9), not \
             exited on its own: {status:?}"
        );
    }

    // The killed process's claims on the batch outlive it: the fresh
    // subprocess has an id of its own (issue #756), so it takes them back
    // only once the reclaim sweep finds them past `reclaim_ttl` (30 s, as
    // long as `quiesce` waits). Age them past it instead of waiting it out;
    // the fresh subprocess's next maintenance tick reclaims them. Only the
    // killed process's: any claim the fresh one has taken since stays live.
    pool.get()
        .await
        .expect("pool connection")
        .execute(
            &format!(
                "update {}.seg_claims set claimed_at = now() - interval '1 hour' \
                 where claimed_by = any($1)",
                trellis::config::DEFAULT_SCHEMA
            ),
            &[&killed_claimants],
        )
        .await
        .expect("age the killed process's claims");

    // The scenario's critical pair is always its program's last two ops
    // (`RelInterleavingScenario`'s own doc comment), so there is nothing
    // left to apply — just wait for the fresh subprocess to finish
    // redraining what the killed one left mid-flight.
    backend.quiesce().await.expect(
        "quiesce must converge after the redrive — a stuck ring here would mean the \
                 crashed batch was left unrecoverable",
    );

    let snapshot = backend.snapshot().await.expect("snapshot after redrive");
    let diverged = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");
    assert!(
        diverged.is_empty(),
        "post-SIGKILL redrive must converge onto exactly the same state an uninterrupted drain \
         would have reached — both the target write and the relationship projection advance \
         redone together, or the batch wasn't atomic: {diverged:?}"
    );
}

/// `restart` must not spawn the fresh engine while the killed one's
/// staging-worker singleton is still held. A killed engine's backend frees it
/// only once Postgres schedules that backend to read the EOF, which under CPU
/// contention can trail the kill by milliseconds; a fresh engine that asked
/// first failed its start with `ProducerAlreadyRunning`, and the restart above
/// timed out waiting for a readiness marker that never came (PR #859's CI).
///
/// The stand-in for that slow backend is a session of this test's own,
/// queued on the singleton behind the running primary: Postgres hands it the
/// lock the instant the killed primary's backend frees it, and it holds it
/// on for a while. The fresh engine can only start once it lets go.
#[tokio::test(flavor = "multi_thread")]
async fn restart_waits_for_the_killed_engines_singleton_to_free() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let program =
        build_relationship_interleaving_scenario(RelInterleavingVariant::ParentFieldUpdate).program;

    let engine_bin = env!("CARGO_BIN_EXE_engine_subprocess");
    let mut backend = SubprocessBackend::connect(db.dsn(), engine_bin)
        .await
        .expect("connect subprocess backend");
    backend
        .install(&program)
        .await
        .expect("install program (spawns the primary engine subprocess)");

    let key = producer_singleton_lock_key(trellis::config::DEFAULT_SCHEMA);
    let (stand_in, connection) = tokio_postgres::connect(db.dsn(), NoTls)
        .await
        .expect("connect the stand-in session");
    tokio::spawn(connection);
    let stand_in_pid: i32 = stand_in
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("read the stand-in's backend pid")
        .get(0);

    let hold = Duration::from_millis(1500);
    let stand_in_task = tokio::spawn(async move {
        stand_in
            .execute("select pg_advisory_lock($1)", &[&key])
            .await
            .expect("queue on the singleton, then take it when the primary dies");
        let taken_at = Instant::now();
        tokio::time::sleep(hold).await;
        stand_in
            .execute("select pg_advisory_unlock($1)", &[&key])
            .await
            .expect("let the singleton go");
        taken_at
    });

    // Kill only once the stand-in is queued, so it, not the fresh engine, is
    // next in line for the singleton.
    let (observer, connection) = tokio_postgres::connect(db.dsn(), NoTls)
        .await
        .expect("connect the observer");
    tokio::spawn(connection);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let queued: bool = observer
            .query_one(
                "select exists(select 1 from pg_locks \
                 where pid = $1 and locktype = 'advisory' and not granted)",
                &[&stand_in_pid],
            )
            .await
            .expect("read the stand-in's lock wait")
            .get(0);
        if queued {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the stand-in never queued on the singleton behind the running primary"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    backend
        .restart()
        .await
        .expect("restart must wait for the singleton to free, then start the fresh engine on it");
    let restarted_at = Instant::now();
    let taken_at = stand_in_task.await.expect("stand-in task");
    assert!(
        restarted_at >= taken_at + hold,
        "restart returned {:?} after the stand-in took the singleton, before it let go after \
         {hold:?}: the fresh engine can't have started on it",
        restarted_at - taken_at
    );
}
