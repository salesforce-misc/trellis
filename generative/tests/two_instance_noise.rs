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
//!    for. Two databases in one cluster share a little more (cluster-wide
//!    objects such as roles). Two schemas in one database share everything
//!    a Postgres database has: the source tables and the capture triggers
//!    on them, the advisory-lock keyspace (Postgres advisory locks are
//!    scoped to a database, **not** to a schema), one WAL, one catalog, one
//!    set of background workers. That is the maximum-shared-surface configuration,
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
//! ~0.1s for the identical single-instance run: the replication intake Trellis
//! ran before #622 confirmed WAL its co-tenant wrote only on a throttled
//! keepalive. Trigger capture has no such wait. [`MAX_CHECKS_PER_RUN`] was
//! sized for the old cost.
//!
//! # The shapes planned for production use (issue #879, epic #806)
//!
//! Everything above runs two instances that share a database but nothing
//! else: each has its own source tables, in its own catalog schema. Epic #806
//! plans three shapes with more in common than that, and each has a property
//! and a hand-built pin below:
//!
//! 1. **One source table, two instances** ([`share_source`]). The source lives
//!    in `public`. Instance A creates and writes it and defines the first half
//!    of a program's transforms over it; instance B defines the second half.
//!    Both capture the table, so both fire triggers on every write, and each
//!    is checked against the oracle over the one table.
//! 2. **A chain across instances** ([`chain_off`]). Instance A keeps its
//!    targets in `public`, and instance B defines a transform over each of A's
//!    one-to-one targets, which B captures like any source. B's targets are
//!    checked against the oracle over A's target tables as they are in the
//!    database. A runner step A has checked before B quiesces is what makes
//!    B's wait cover A's writes to those tables. Then A drops a transform
//!    whose target B reads, and B must take its dropped-source path.
//! 3. **Two databases in one process.** The instances use the *same* schema
//!    names, as a multi-tenant host with one database per tenant would, so
//!    anything the engine keeps per process under a schema name alone is
//!    shared between them.
//!
//! Every shape goes through [`run_two_instance_convergence`], which drives
//! both instances to quiescence at each checkpoint and compares; none polls
//! for convergence itself.
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

use generative::backend::Backend;
use generative::backend::{ManualBackend, SourceTables};
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec, build_program, build_program_multi,
    build_program_multi_with_shapes, trivial_program,
};
use generative::model::{OpOutcome, Program};
use generative::run::{
    InstanceLabel, InstanceRun, RunError, chain_off, check_program, run_two_instance_convergence,
    share_source,
};
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
/// the instance schema and the transform target schema.
/// Nothing else is made distinct: both instances share the database, its
/// WAL, its catalog, and its advisory-lock keyspace, which is the whole
/// point (see the module doc comment).
///
/// `trellis::migrate` creates the schema if absent
/// (`docs/instance-identity.md`), so the instance schema needs no explicit
/// `CREATE SCHEMA`; the *target* schema does, since nothing in the engine
/// creates a target schema on a caller's behalf.
async fn stand_up(db: &TestDatabase, label: InstanceLabel, tag: &str) -> Instance {
    stand_up_in(
        db,
        label,
        &format!("trellis_{tag}"),
        &format!("targets_{tag}"),
        SourceTables::Own,
    )
    .await
}

/// [`stand_up`] with the instance schema, the target schema and the place
/// the instance's source tables live all chosen by the caller (issue #879).
async fn stand_up_in(
    db: &TestDatabase,
    label: InstanceLabel,
    schema: &str,
    target_schema: &str,
    sources: SourceTables,
) -> Instance {
    let config = Config::with_schema(db.dsn().to_string(), schema.to_string())
        .expect("valid instance schema")
        .with_target_schema(target_schema.to_string())
        .expect("valid target schema");
    let pool = Pool::new(&config).expect("build oracle pool for instance");

    // The target schema: nothing in the engine creates a target schema on a
    // caller's behalf, so the harness does it, through the database's own
    // default-schema pool (`testkit` migrated that one already) rather than
    // through `pool` — whose `search_path` bootstrap names `target_schema`
    // itself, which would be needlessly circular. Callers pass plain
    // lowercase ASCII names, so the interpolation below needs no quoting;
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
        ManualBackend::connect_with_instance(db.dsn(), schema, target_schema, 1, None)
            .await
            .expect("connect instance backend");
    backend.set_source_tables(sources);

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

/// Runs `program_a` on instance `a` and `program_b` on instance `b` through
/// [`run_two_instance_convergence`], each checked against its own oracle.
async fn run_pair(
    a: &mut Instance,
    b: &mut Instance,
    program_a: &Program,
    program_b: &Program,
) -> Result<(), TestCaseError> {
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
}

/// Runs `program_a` and `program_b` as two side-by-side instances in one
/// freshly-created isolated database.
fn run_one(program_a: &Program, program_b: &Program) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut a = stand_up(&db, InstanceLabel::A, "a").await;
            let mut b = stand_up(&db, InstanceLabel::B, "b").await;
            run_pair(&mut a, &mut b, program_a, program_b).await
        })
    })
}

/// Instances over one source table (shape 1): `writer` creates and writes it
/// in `public`, `reader` only reads it. Each instance keeps its own targets
/// in its own schema, so both can name a target `d0`.
async fn stand_up_sharing_a_source(db: &TestDatabase) -> (Instance, Instance) {
    let a = stand_up_in(
        db,
        InstanceLabel::A,
        "trellis_a",
        "targets_a",
        SourceTables::SharedCreate,
    )
    .await;
    let b = stand_up_in(
        db,
        InstanceLabel::B,
        "trellis_b",
        "targets_b",
        SourceTables::SharedExisting,
    )
    .await;
    (a, b)
}

/// Instances in a chain (shape 2): A keeps its targets in `public`, where B's
/// `search_path` finds them by bare name, and B reads them as its source
/// tables.
async fn stand_up_chain(db: &TestDatabase) -> (Instance, Instance) {
    let a = stand_up_in(
        db,
        InstanceLabel::A,
        "trellis_a",
        "public",
        SourceTables::Own,
    )
    .await;
    let b = stand_up_in(
        db,
        InstanceLabel::B,
        "trellis_b",
        "targets_b",
        SourceTables::SharedExisting,
    )
    .await;
    (a, b)
}

fn run_sharing_a_source(program: &Program) -> Result<(), TestCaseError> {
    let (writer, reader) = share_source(program);
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let (mut a, mut b) = stand_up_sharing_a_source(&db).await;
            run_pair(&mut a, &mut b, &writer, &reader).await
        })
    })
}

fn run_chain(program: &Program, chained: &Program) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let (mut a, mut b) = stand_up_chain(&db).await;
            run_pair(&mut a, &mut b, program, chained).await
        })
    })
}

/// Two instances in two databases of one cluster, both in the default
/// `trellis` catalog schema and the default `public` target schema, which is
/// the shape of one process serving a database per tenant (shape 3).
fn run_in_two_databases(program_a: &Program, program_b: &Program) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db_a = h.cluster.create_isolated_database().await;
            let db_b = h.cluster.create_isolated_database().await;
            let mut a = stand_up_in(
                &db_a,
                InstanceLabel::A,
                "trellis",
                "public",
                SourceTables::Own,
            )
            .await;
            let mut b = stand_up_in(
                &db_b,
                InstanceLabel::B,
                "trellis",
                "public",
                SourceTables::Own,
            )
            .await;
            run_pair(&mut a, &mut b, program_a, program_b).await
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

    /// Shape 1 (issue #879): two instances define transforms over the same
    /// source tables, and each matches the oracle over them while one program
    /// churns them. The instances are separated by their schemas alone, as
    /// above, but now they capture the same tables as well.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_two_instances_over_one_source_each_match_the_oracle(
        program in trivial_program(),
    ) {
        run_sharing_a_source(&program)?;
    }

    /// Shape 2 (issue #879): instance B defines transforms over instance A's
    /// one-to-one targets, and matches the oracle over A's targets while A's
    /// program churns its own sources. A program with no one-to-one
    /// definition has nothing for B to read and is discarded.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_an_instance_reading_another_instances_target_matches_the_oracle(
        program in trivial_program(),
    ) {
        let chained = chain_off(&program);
        prop_assume!(chained.is_some());
        run_chain(&program, &chained.expect("assumed above"))?;
    }

    /// Shape 3 (issue #879): two instances in two databases of one cluster,
    /// in the same catalog schema, each match their own oracle.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_two_instances_in_two_databases_each_match_their_own_oracle(
        program_a in trivial_program(),
        program_b in trivial_program(),
    ) {
        run_in_two_databases(&program_a, &program_b)?;
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

/// Asserts that each instance schema in `schemas` has its capture trigger
/// installed on `schema.table`, so a run over it is known to have captured
/// it rather than merely not noticed it.
async fn assert_captured_by(db: &TestDatabase, schema: &str, table: &str, instances: &[&str]) {
    let rows = db
        .pool
        .get()
        .await
        .expect("checkout")
        .query(
            "select tgname from pg_trigger \
             where tgrelid = format('%I.%I', $1::text, $2::text)::regclass and not tgisinternal",
            &[&schema, &table],
        )
        .await
        .expect("read triggers");
    let triggers: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    for instance in instances {
        let expected = format!("{instance}_capture_insert");
        assert!(
            triggers.contains(&expected),
            "{schema}.{table} should be captured by {instance}: {triggers:?}"
        );
    }
}

/// Shape 1, hand-built: one table, three transforms (two one-to-one and an
/// aggregate), churned by inserts, updates, group moves, deletes, a rejected
/// duplicate insert and a truncate. A's instance defines the first two
/// transforms and writes; B's defines the last two, so the second
/// transform is defined in both instances.
#[tokio::test(flavor = "multi_thread")]
async fn two_instances_over_one_source_table_each_match_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let seeds = vec![
        (Some(10), Some(1)),
        (Some(20), Some(2)),
        (Some(30), None),
        (None, Some(4)),
    ];
    let mutates = vec![
        Mutate::Update {
            pk: 1,
            c1: Some(100),
            c2: Some(5),
        },
        Mutate::MoveGroup {
            pk: 2,
            grain: Some(3),
        },
        Mutate::Delete { pk: 3 },
        Mutate::Truncate,
        Mutate::DuplicateInsert {
            pk: 4,
            c1: Some(1),
            c2: Some(1),
        },
        Mutate::DuplicateInsert {
            pk: 1,
            c1: Some(6),
            c2: None,
        },
        Mutate::Update {
            pk: 1,
            c1: None,
            c2: Some(9),
        },
    ];
    let program = build_program_multi_with_shapes(
        &[TableSpec::numeric_only(seeds, mutates)],
        &[
            (0, DefShape::OneToOne),
            (
                0,
                DefShape::Aggregate {
                    functions: vec![AggregateFn::Count, AggregateFn::Sum(AggregateColumn::C1)],
                },
            ),
            (0, DefShape::OneToOne),
        ],
    );
    let (writer, reader) = share_source(&program);
    assert_eq!(writer.defs.len(), 2);
    assert_eq!(reader.defs.len(), 2);

    let (mut a, mut b) = stand_up_sharing_a_source(&db).await;
    run_pair(&mut a, &mut b, &writer, &reader)
        .await
        .unwrap_or_else(|err| panic!("two instances over one source table: {err}"));

    // The run only means something if both instances really captured the one
    // table, each with triggers of its own.
    assert_captured_by(
        &db,
        "public",
        &program.tables[0].name,
        &["trellis_a", "trellis_b"],
    )
    .await;
}

/// Shape 2, hand-built: A maintains a one-to-one target in `public` from its
/// own source, and B maintains a target from A's, through every kind of
/// write A's source takes.
#[tokio::test(flavor = "multi_thread")]
async fn an_instance_reading_another_instances_target_matches_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = chain_program();
    let chained = chain_off(&program).expect("a one-to-one definition to chain off");

    let (mut a, mut b) = stand_up_chain(&db).await;
    run_pair(&mut a, &mut b, &program, &chained)
        .await
        .unwrap_or_else(|err| panic!("B over A's target: {err}"));

    // A's target is B's source, so B captured it.
    assert_captured_by(&db, "public", &program.defs[0].target, &["trellis_b"]).await;
}

/// The first instance's program for the chain pins: a one-to-one transform
/// whose target takes inserts, updates, a null, a delete and a revival.
fn chain_program() -> Program {
    build_program(
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
            Mutate::Update {
                pk: 3,
                c1: None,
                c2: Some(7),
            },
            Mutate::DuplicateInsert {
                pk: 2,
                c1: Some(8),
                c2: Some(8),
            },
        ],
    )
}

/// Shape 3, hand-built: the same two programs as the same-database pin, in
/// two databases that use the same schema names.
#[tokio::test(flavor = "multi_thread")]
async fn two_instances_in_two_databases_each_converge_independently() {
    let program_a = chain_program();
    let program_b = build_program(
        &[(Some(7000), Some(700)), (Some(8000), Some(800))],
        &[Mutate::Delete { pk: 1 }],
    );
    let cluster = TestCluster::start();
    let db_a = cluster.create_isolated_database().await;
    let db_b = cluster.create_isolated_database().await;
    let mut a = stand_up_in(
        &db_a,
        InstanceLabel::A,
        "trellis",
        "public",
        SourceTables::Own,
    )
    .await;
    let mut b = stand_up_in(
        &db_b,
        InstanceLabel::B,
        "trellis",
        "public",
        SourceTables::Own,
    )
    .await;
    run_pair(&mut a, &mut b, &program_a, &program_b)
        .await
        .unwrap_or_else(|err| panic!("two instances in two databases: {err}"));
}

/// Applies `op` to `instance` and checks its outcome is the one the program
/// expects, as the runner does.
async fn apply_expecting(instance: &mut Instance, op: &generative::model::Op) {
    let actual = match instance.backend.apply(op).await {
        Err(_) => OpOutcome::Fails,
        Ok(0) => OpOutcome::AffectsNoRows,
        Ok(_) => OpOutcome::Succeeds,
    };
    assert!(op.expect().matches(&actual), "{op:?} gave {actual:?}");
}

/// Quiesces `instance` and checks every definition of `program` against the
/// oracle over the database as it is.
async fn assert_converged(instance: &mut Instance, program: &Program, when: &str) {
    instance.backend.quiesce().await.expect("quiesce");
    let snapshot = instance.backend.snapshot().await.expect("snapshot");
    let diverged = check_program(&instance.pool, program, &snapshot)
        .await
        .expect("the oracle reads the database");
    assert!(
        diverged.is_empty(),
        "instance {} {when}: {}",
        instance.label,
        diverged
            .iter()
            .map(|(target, report)| format!("{target}: {report}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// `program` without its first definition: what is left of an instance after
/// the operator drops that transform.
fn without_first_definition(program: &Program) -> Program {
    let mut rest = program.clone();
    rest.defs.remove(0);
    rest.def_install_after_op.remove(0);
    rest
}

/// Shape 2's other half: instance A drops a transform whose target instance B
/// reads, while B has changes to that target staged and not yet applied.
///
/// B's staging worker is stopped while A writes, so A's writes to both of its
/// targets land in B's ring through B's capture triggers and wait there. A then
/// drops the first transform, which drops its target table and B's triggers on
/// it, and B's worker starts again. Its first batch holds rows naming a table
/// that no longer exists, which is the dropped-source path
/// (`ApplyError::SourceTableDropped`): the apply purges them and goes on.
///
/// What must hold: B's ring drains rather than wedging, B's other transform
/// (over A's second target) is still maintained and matches the oracle for
/// the writes staged before the drop and the ones after it, and B's operator
/// can drop the transform that lost its source.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_transform_in_one_instance_does_not_wedge_the_instance_reading_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let seeds = vec![
        (Some(10), Some(1)),
        (Some(20), Some(2)),
        (Some(30), Some(3)),
    ];
    let update = |pk, c1, c2| Mutate::Update { pk, c1, c2 };
    let mutates = vec![
        update(1, Some(100), Some(5)),
        update(2, Some(7), None),
        Mutate::Delete { pk: 3 },
        // Written while B's worker is stopped.
        update(1, Some(11), Some(12)),
        Mutate::DuplicateInsert {
            pk: 3,
            c1: Some(8),
            c2: Some(8),
        },
        update(2, None, Some(2)),
        // Written after A has dropped the first transform.
        update(1, Some(1), Some(1)),
        Mutate::Delete { pk: 2 },
    ];
    let program = build_program_multi(&[TableSpec::numeric_only(seeds, mutates)], &[0, 0]);
    let chained = chain_off(&program).expect("two one-to-one definitions to chain off");
    assert_eq!(chained.defs.len(), 2);
    let ops = &program.ops;
    let (staged_from, dropped_at) = (ops.len() - 5, ops.len() - 2);

    let (mut a, mut b) = stand_up_chain(&db).await;
    a.backend.install(&program).await.expect("install A");
    b.backend.install(&chained).await.expect("install B");

    for op in &ops[..staged_from] {
        apply_expecting(&mut a, op).await;
    }
    assert_converged(&mut a, &program, "before the drop").await;
    assert_converged(&mut b, &chained, "before the drop").await;

    // B's worker stops; A's writes go on and reach B's ring only.
    b.backend.stop_engine().await.expect("stop B's worker");
    for op in &ops[staged_from..dropped_at] {
        apply_expecting(&mut a, op).await;
    }
    a.backend.quiesce().await.expect("A applies its writes");
    let dropped = program.defs[0].target.clone();
    assert!(
        b.backend.has_pending().await.expect("read B's ring"),
        "A's writes to its targets should be staged in B's ring"
    );
    // The ring is four fixed tables (`staging::append::RING_SIZE`).
    let staged_for_it = (0..4)
        .map(|slot| format!("select 1 from seg_{slot} where src_table = 'public.{dropped}'"))
        .collect::<Vec<_>>()
        .join(" union all ");
    assert!(
        b.backend
            .execute_raw(&staged_for_it)
            .await
            .expect("read B's ring")
            > 0,
        "rows naming {dropped} should be staged in B's ring"
    );

    // A drops the first transform; B finds out only from the rows it staged.
    a.backend
        .drop_transform(&dropped)
        .await
        .expect("A drops it");
    b.backend.forget_table(&dropped);
    b.backend.start_engine().await.expect("start B's worker");
    let program_a = without_first_definition(&program);
    let program_b = without_first_definition(&chained);
    assert_converged(&mut a, &program_a, "after the drop").await;
    b.backend
        .quiesce()
        .await
        .expect("B's ring drains past the dropped table");
    // The drain purged every staged row naming it, not only the ones in the
    // batch it happened to fold (`quarantine::purge_dropped_table`). A drained
    // segment keeps its rows until it is recycled, so without the purge some
    // would still be here. This holds while B's transform over the dropped
    // table stays `live` (#999), so the drain reads its rows rather than
    // skipping them as a paused reader's.
    assert_eq!(
        b.backend
            .execute_raw(&staged_for_it)
            .await
            .expect("read B's ring"),
        0,
        "B's ring should hold no rows naming {dropped} once it has drained"
    );
    // B's transform over it is still `live`, with nothing to say its source is
    // gone (#999), so the operator drops it.
    let lost_source = chained.defs[0].target.clone();
    b.backend
        .drop_transform(&lost_source)
        .await
        .expect("B drops it");
    assert_converged(&mut b, &program_b, "after reading a dropped source").await;

    // The writes that come after are still maintained in both.
    for op in &ops[dropped_at..] {
        apply_expecting(&mut a, op).await;
    }
    assert_converged(&mut a, &program_a, "after the drop's writes").await;
    assert_converged(&mut b, &program_b, "after the drop's writes").await;
}
