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
//! (driving `trellis::staging::apply`'s test-only pre-commit pause hook)
//! lands the `SIGKILL` deterministically inside that batch's still-open,
//! uncommitted Phase 3 transaction, rather than hoping timing luck hits the
//! window — see `SubprocessBackend`'s own module doc comment for the full
//! mechanism.

use std::time::Duration;

use generative::backend::{Backend, SubprocessBackend};
use generative::generate::{RelInterleavingVariant, build_relationship_interleaving_scenario};
use generative::run::check_program;
use testkit::TestCluster;
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
    let scenario = build_relationship_interleaving_scenario(RelInterleavingVariant::ParentFieldUpdate);
    let program = scenario.program;

    let engine_bin = env!("CARGO_BIN_EXE_engine_subprocess");
    let mut backend = SubprocessBackend::connect(db.dsn(), engine_bin)
        .await
        .expect("connect subprocess backend");
    // Issue #188: a unique slot/publication per isolated database, same
    // rationale as every `ManualBackend`-driven test in this suite —
    // `TestCluster`'s underlying Postgres cluster is shared across
    // concurrently-running tests, and a replication slot name is
    // cluster-wide, not database-scoped.
    let unique = db.name().replace('-', "_");
    backend.set_slot_and_publication(format!("{unique}_slot"), format!("{unique}_pub"));

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
         — if this fires, the pause hook (trellis::staging::apply::pause_before_commit_for_tests) \
         never engaged, so nothing below would actually be testing a mid-transaction kill"
    );

    // Disarm before restarting: the respawned subprocess inherits the same
    // trigger-file path and must not immediately re-pause while redraining
    // the very batch the killed process never got to commit.
    backend.disarm_pause_before_commit();

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

    // The scenario's critical pair is always its program's last two ops
    // (`RelInterleavingScenario`'s own doc comment), so there is nothing
    // left to apply — just wait for the fresh subprocess to finish
    // redraining what the killed one left mid-flight.
    backend
        .quiesce()
        .await
        .expect("quiesce must converge after the redrive — a stuck ring here would mean the \
                 crashed batch was left unrecoverable");

    let snapshot = backend.snapshot().await.expect("snapshot after redrive");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    let diverged = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");
    assert!(
        diverged.is_none(),
        "post-SIGKILL redrive must converge onto exactly the same state an uninterrupted drain \
         would have reached — both the target write and the relationship projection advance \
         redone together, or the batch wasn't atomic: {diverged:?}"
    );
}
