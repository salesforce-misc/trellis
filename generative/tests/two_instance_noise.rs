//! Issue #234 (epic #238, layer-3 bucket 1 remainder; design doc §8/§9):
//! **two Trellis instances side by side, each converging against its own
//! oracle** — the sub-requirement #11 called out and the one shape of bug a
//! single-instance suite structurally cannot see.
//!
//! Everything about *why* this is an interaction test and not just two
//! unrelated runs (both engines live for the whole run, op streams
//! interleaved, each instance checked against its own oracle independently
//! so neither can mask the other) lives on
//! [`generative::run::run_two_instance_convergence`]'s own module doc
//! comment. This file is the harness: it decides *how close together* the
//! two instances are put, and stands them up.
//!
//! # The design decision: two schemas in one database, not two clusters
//!
//! Issue #234 leaves the topology open ("the same or independent clusters").
//! This file puts both instances in the **same database of the same
//! cluster**, separated only by `docs/instance-identity.md`'s named-schema
//! mechanism (`Config::schema()`/`TRELLIS_SCHEMA`, `search_path` pinning,
//! the `trellis_instance` marker). Three reasons, in order of weight:
//!
//! 1. **It is the strongest test, because it shares the most.** The whole
//!    point is to catch a resource one instance believes it owns privately
//!    but actually shares. Two separate clusters share almost nothing —
//!    separate `postgres` processes, separate WAL, separate catalogs,
//!    separate lock managers — so a hardcoded, unscoped resource name is
//!    *invisible* there, which is the opposite of what this property is
//!    for. Two databases in one cluster share a little more (the
//!    cluster-wide replication-slot namespace — already covered by issue
//!    #188's per-case slot naming). Two schemas in one database share
//!    everything a Postgres database has: the slot *and* publication
//!    namespaces, the advisory-lock keyspace (Postgres advisory locks are
//!    scoped to a database, **not** to a schema), one WAL and one logical
//!    decoding stream feeding both instances' CDC, one catalog, one set of
//!    background workers. That is the maximum-shared-surface configuration,
//!    and therefore the one that can actually fail.
//! 2. **It is the production topology the docs promise.**
//!    `docs/instance-identity.md` says in as many words that "several
//!    Trellis instances can coexist in one cluster — even one database —
//!    each isolated within its own schema." A property testing anything
//!    weaker would be testing a claim nobody made. (It found a real bug in
//!    exactly that claim — see below.)
//! 3. **It is by far the cheapest.** No second `postgres` process per case,
//!    not even a second database: two `CREATE SCHEMA`s and two
//!    `trellis::migrate` runs on the isolated database this suite already
//!    creates per case. That matters when every case now stands up two
//!    engines instead of one.
//!
//! The one thing the two instances are *not* allowed to share is their
//! transform **target** schema. Two independently-generated programs name
//! their targets from the same small vocabulary, so pointing both instances
//! at `public` would make them collide on a table name — a plain
//! misconfiguration, not an isolation failure, and a guaranteed false
//! positive that would drown out the real signal. Giving each instance its
//! own target schema is the correct configuration for coexisting instances
//! (`Config::with_target_schema`/`TRELLIS_TARGET_SCHEMA` exist for exactly
//! this), not a weakening of the test: the instance schema, the thing
//! `docs/instance-identity.md` is actually about, stays maximally shared.
//!
//! # The bugs this found (both fixed in this same change)
//!
//! Neither was a divergence — both were *stand-up* failures, which is what a
//! two-instance property looks for first: a second instance that cannot even
//! start is the loudest possible isolation failure.
//!
//! 1. **The producer singleton was scoped per database, not per instance.**
//!    `trellis::staging::session::ProducerSession` took
//!    `pg_try_advisory_lock` under a single hardcoded global constant, and
//!    Postgres advisory locks are keyed by `(database, key)` — a schema does
//!    not enter into the lock tag. So "exactly one CDC intake producer at a
//!    time" was really enforced per *database*: two correctly-configured
//!    instances sharing one database could never both run, the second one
//!    failing with `StagingError::ProducerAlreadyRunning` over a producer
//!    that was not its own. The key is now derived from the instance schema
//!    (`trellis/src/staging/session.rs`).
//! 2. **`Client::start` threw away its caller's configured schema.** It took
//!    only a DSN and re-resolved a `Config` from the *process environment*,
//!    so the instance schema was a property of the process rather than of
//!    the client. `trellis::Trellis` — which is built from an explicit
//!    `Config` and hands `config.dsn()` to `Client::start` — therefore ran
//!    its background client in `TRELLIS_SCHEMA`/`DEFAULT_SCHEMA` no matter
//!    what schema it had been configured with, against a different
//!    instance's staging ring. `Client::start_with_config`
//!    (`trellis/src/client.rs`) now carries the resolved `Config` through,
//!    and `Trellis` uses it.
//!
//! # A cost that used to set this file's shape
//!
//! Every `quiesce()` in a two-instance run used to cost ~10 seconds, against
//! ~0.1s for the identical single-instance run. `quiesce()`'s token is
//! `pg_current_wal_lsn()` — a **cluster-wide** LSN — so an instance asking
//! "have I converged?" is really asking whether it has confirmed through
//! WAL its co-tenant wrote and its own publication filters out. That used
//! to advance only on a keepalive, whose persist is throttled to once per
//! 10s (`intake::KEEPALIVE_PERSIST_INTERVAL`). Since issue #452 the waiter
//! writes a `trellis.converge` logical message that both instances' intakes
//! decode and confirm through at once, so the co-tenant costs nothing
//! extra. [`MAX_CHECKS_PER_RUN`] was sized for the old cost.
//!
//! # Running this property alone (design doc §9)
//!
//! ```text
//! cargo test -p generative --test two_instance_noise -- --nocapture
//! PROPTEST_CASES=64 cargo test -p generative --test two_instance_noise -- --nocapture
//! ```
//!
//! Every case bootstraps its own database, schemas, and migrations, so this
//! never depends on run order or on any other property having run.

use generative::backend::ManualBackend;
use generative::generate::{Mutate, build_program, trivial_program};
use generative::model::Program;
use generative::run::{InstanceLabel, InstanceRun, RunError, run_two_instance_convergence};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::{TestCluster, TestDatabase};
use trellis::{Config, Pool};

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: TestCluster::start(),
    };
}

/// Design doc §9: 12 rather than the 16 every single-instance property in
/// this crate uses, because every case here stands up *two* engines, runs
/// two programs, and runs the three-way oracle twice per checkpoint against
/// two separate oracles. Override with `PROPTEST_CASES` for a deep run.
fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    ProptestConfig {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// How many convergence checkpoints a single run spends, spread across its
/// interleaved steps. Chosen when every `quiesce()` in a two-instance run
/// cost ~10 seconds (see `generative::run::run_two_instance_convergence`'s
/// doc comment, and for the coarser divergence localization this implies).
/// Issue #452 removed that cost, so more checkpoints, up to one per step,
/// are now affordable; four still gives a failure several distinct windows
/// to be localized to rather than one all-or-nothing check at the end.
const MAX_CHECKS_PER_RUN: usize = 4;

/// One stood-up Trellis instance: its backend and the oracle's read handle
/// into its own schema, kept together so neither can accidentally be paired
/// with the *other* instance's counterpart.
struct Instance {
    backend: ManualBackend,
    pool: Pool,
    label: InstanceLabel,
}

/// Stands up one Trellis instance inside `db`, in its own named schema.
///
/// `tag` distinguishes the two instances everywhere a name has to differ:
/// the instance schema, the transform target schema, and the replication
/// slot/publication (issue #188 — a slot name is unique cluster-wide, so it
/// has to carry both the database's unique suffix *and* the instance tag).
/// Nothing else is made distinct: both instances share the database, its
/// WAL, its catalog, and its advisory-lock keyspace, which is the whole
/// point (see the module doc comment).
///
/// `trellis::migrate` creates the schema if absent
/// (`docs/instance-identity.md`), so the instance schema needs no explicit
/// `CREATE SCHEMA`; the *target* schema does, since nothing in the engine
/// creates a target schema on a caller's behalf.
async fn stand_up(db: &TestDatabase, label: InstanceLabel, tag: &str) -> Instance {
    let schema = format!("trellis_{tag}");
    let target_schema = format!("targets_{tag}");

    let config = Config::with_schema(db.dsn().to_string(), schema.clone())
        .expect("valid instance schema")
        .with_target_schema(target_schema.clone())
        .expect("valid target schema");
    let pool = Pool::new(&config).expect("build oracle pool for instance");

    // The target schema: nothing in the engine creates a target schema on a
    // caller's behalf, so the harness does it, through the database's own
    // default-schema pool (`testkit` migrated that one already) rather than
    // through `pool` — whose `search_path` bootstrap names `target_schema`
    // itself, which would be needlessly circular. Both `tag`s used here are
    // plain lowercase ASCII, so the interpolation below needs no quoting;
    // `Config::with_target_schema` above has already validated the name.
    db.pool
        .get()
        .await
        .expect("checkout for create schema")
        .batch_execute(&format!("create schema if not exists {target_schema}"))
        .await
        .expect("create target schema");

    trellis::migrate(&pool, &config)
        .await
        .expect("migrate instance schema");

    let mut backend =
        ManualBackend::connect_with_instance(db.dsn(), &schema, &target_schema, 1, None)
            .await
            .expect("connect instance backend");
    // Issue #188: unique per-case *and* per-instance slot/publication names.
    let unique = format!("{}_{tag}", db.name().replace('-', "_"));
    backend.set_slot_and_publication(format!("{unique}_slot"), format!("{unique}_pub"));

    Instance {
        backend,
        pool,
        label,
    }
}

/// Renders a two-instance run's failure, naming the instance it happened on
/// so a divergence on A is never reported as if it could have been B's.
fn describe(err: generative::run::TwoInstanceError) -> TestCaseError {
    let instance = err.instance;
    match err.error {
        RunError::Diverged(d) => TestCaseError::fail(format!(
            "instance {instance} diverged against its OWN oracle after step {} (target {}) \
             while the other instance was running side by side in the same database:\n{}",
            d.op_index, d.def_target, d.report
        )),
        other => TestCaseError::fail(format!("instance {instance} run error: {other:?}")),
    }
}

/// Runs `program_a` and `program_b` as two side-by-side instances in one
/// freshly-created isolated database.
fn run_one(program_a: &Program, program_b: &Program) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut a = stand_up(&db, InstanceLabel::A, "a").await;
            let mut b = stand_up(&db, InstanceLabel::B, "b").await;

            let outcome = run_two_instance_convergence(
                InstanceRun {
                    label: a.label,
                    backend: &mut a.backend,
                    pool: &a.pool,
                    program: program_a,
                },
                InstanceRun {
                    label: b.label,
                    backend: &mut b.backend,
                    pool: &b.pool,
                    program: program_b,
                },
                MAX_CHECKS_PER_RUN,
            )
            .await
            .map_err(describe)?;

            if outcome.as_pass() {
                Ok(())
            } else {
                Err(TestCaseError::fail(format!("run did not pass: {outcome}")))
            }
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// Two independently-generated programs, run by two independent Trellis
    /// instances sharing one database and separated only by
    /// `docs/instance-identity.md`'s named-schema isolation, must each
    /// converge against their own oracle at every checkpoint — neither
    /// perturbing the other, and neither's correctness masking the other's.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_two_side_by_side_instances_each_converge_against_their_own_oracle(
        program_a in trivial_program(),
        program_b in trivial_program(),
    ) {
        run_one(&program_a, &program_b)?;
    }
}

/// This file's named "first end-to-end green": two hand-built, deliberately
/// *differently-shaped* programs run by two instances in one database.
///
/// The shapes are chosen so that a leak in either direction would be visible
/// rather than coincidentally identical: A seeds three rows and then mutates
/// two of them, B seeds two rows with disjoint primary keys and values and
/// deletes one. If either instance's CDC stream, ring, or target maintenance
/// saw the other's changes, neither instance's own oracle recompute (which
/// reads only that instance's own source tables, through a pool pinned to
/// that instance's own schema) would match its own materialized target.
///
/// This pin is also the direct, legible regression test for both bugs
/// described in the module doc comment (the per-database producer singleton
/// and `Client::start`'s dropped schema): before those fixes, this test could
/// not even reach its first op — instance
/// B's `install` failed outright with `ProducerAlreadyRunning`.
#[tokio::test(flavor = "multi_thread")]
async fn two_hand_built_instances_in_one_database_each_converge_independently() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program_a = build_program(
        &[
            (Some(10), Some(1)),
            (Some(20), Some(2)),
            (Some(30), Some(3)),
        ],
        &[
            Mutate::Update {
                pk: 1,
                c1: Some(100),
                c2: Some(5),
            },
            Mutate::Delete { pk: 2 },
        ],
    );
    let program_b = build_program(
        &[(Some(7000), Some(700)), (Some(8000), Some(800))],
        &[Mutate::Delete { pk: 1 }],
    );

    let mut a = stand_up(&db, InstanceLabel::A, "a").await;
    let mut b = stand_up(&db, InstanceLabel::B, "b").await;

    let outcome = run_two_instance_convergence(
        InstanceRun {
            label: a.label,
            backend: &mut a.backend,
            pool: &a.pool,
            program: &program_a,
        },
        InstanceRun {
            label: b.label,
            backend: &mut b.backend,
            pool: &b.pool,
            program: &program_b,
        },
        MAX_CHECKS_PER_RUN,
    )
    .await
    .unwrap_or_else(|err| {
        panic!(
            "two Trellis instances in one database, isolated by schema per \
             docs/instance-identity.md, must each converge independently: {err}"
        )
    });
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}
