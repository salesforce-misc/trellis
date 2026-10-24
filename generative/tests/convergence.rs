//! The convergence property, first end-to-end green (issue #6, design doc
//! §4). Generates trivial 1-1 numeric-`+` programs, drives each through the
//! real [`ManualBackend`] one op at a time, and after every op asserts the
//! materialized target equals the [`generative::oracle`] recompute.
//!
//! # Operational shape (design doc §9)
//!
//! `TestCluster::start()` runs its own `initdb` and is by far the most
//! expensive step, so the property shares **one** cluster across all its
//! proptest cases via a `thread_local` harness: proptest runs a test's cases
//! on the calling thread, so the harness is built once (first case) and its
//! `Drop` — the clean `pg_ctl stop` — runs when the test thread exits. Each
//! case still provisions a fresh *isolated database* (its own schema)
//! inside that shared cluster.
//!
//! Case count defaults to 16 (design doc §9's 12–24 band) and is overridable
//! for deep sweeps with `PROPTEST_CASES` (e.g. `PROPTEST_CASES=500 cargo test
//! -p generative --test convergence`). Failing seeds are persisted to a local,
//! git-ignored `convergence.proptest-regressions` and replayed first; a failure
//! worth keeping is transcribed into a hand-built pin, since a seed replays into
//! a different program once the strategy changes shape (#505).

use generative::backend::{Backend, ManualBackend};
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, DerivedShape, Mutate, TableSpec, build_program,
    build_program_multi, build_program_multi_with_derived, build_program_multi_with_shapes,
    partition_table, partitioned_program, trivial_program,
};
use generative::model::group_key;
use generative::run::{RunError, check_program, run_convergence};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::dev::defs::ast::Expr;
use trellis::dev::defs::qualified_target_table;
use trellis::{Config, Pool};

/// One tokio runtime and one shared, initdb-once cluster for a test thread's
/// proptest cases. See the module doc comment for why this is thread-local.
///
/// `coverage` (improvement-plan task A2, `docs/generative-test-suite.md` §3)
/// accumulates every program `run_one` drives, pass or fail, so the property
/// can report what it actually exercised instead of a single green/red bit —
/// see [`Harness`]'s `Drop` impl below.
struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    coverage: std::cell::RefCell<generative::run::Coverage>,
}

impl Drop for Harness {
    /// Prints the accumulated [`generative::run::Coverage`] when the test
    /// thread exits. Print-only, deliberately: this runs during the shared
    /// thread-local's teardown, which can itself be *during* an unwind if the
    /// macro-generated proptest test is re-panicking with a shrunk failure —
    /// asserting or panicking here risks a double panic, which aborts the
    /// whole test process instead of cleanly failing one test. This is
    /// human/CI-visible reporting only, never a pass/fail gate (the floor
    /// assertions for that live in `tests/coverage.rs`, which never touches a
    /// database).
    fn drop(&mut self) {
        eprintln!(
            "generative: convergence run coverage:\n{}",
            self.coverage.borrow()
        );
    }
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: {
            // Improvement-plan workstream C, task C2: opt-in per-call timing
            // around cluster startup (`initdb` + `pg_ctl start`), the most
            // expensive one-time cost the shared thread-local harness pays.
            // Silent and free unless `GENERATIVE_COST_TIMING` is set, same
            // convention as C1's `GENERATIVE_QUIESCE_TIMING` in
            // `generative/src/backend/manual.rs` (improvement-plan task C2).
            let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();
            let start = timing_enabled.then(std::time::Instant::now);
            let cluster = TestCluster::start().long_lived();
            if let Some(start) = start {
                eprintln!("COST_TIMING cluster_startup {}", start.elapsed().as_millis());
            }
            cluster
        },
        coverage: std::cell::RefCell::new(generative::run::Coverage::new()),
    };
}

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    ProptestConfig {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// Runs one generated program against a fresh isolated database in the shared
/// cluster, returning a proptest-friendly result: a divergence (or any run
/// error) becomes a `TestCaseError::fail` carrying the full report, so proptest
/// shrinks toward the smallest program and prints it.
fn run_one(program: &generative::model::Program) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        // Recorded unconditionally, before the run: even a case that goes on
        // to fail (and shrinks) is real evidence of what the generator drew.
        h.coverage.borrow_mut().record_program(program);

        h.runtime.block_on(async {
            // C2: per-case isolated-database provisioning inside the shared
            // cluster. See the cluster-startup
            // timing above for the env-var convention.
            let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();
            let start = timing_enabled.then(std::time::Instant::now);
            let db = h.cluster.create_isolated_database().await;
            if let Some(start) = start {
                eprintln!("COST_TIMING db_provision {}", start.elapsed().as_millis());
            }
            let mut backend = ManualBackend::connect(db.dsn())
                .await
                .expect("connect manual backend");
            if program.tables.iter().any(|t| t.partitioning.is_some()) {
                backend.set_reconcile_interval(generative::generate::partition::RECONCILE_INTERVAL);
            }
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

            match run_convergence(&mut backend, &pool, program).await {
                Ok(outcome) => {
                    if outcome.as_pass() {
                        Ok(())
                    } else {
                        // Unreachable today: run_convergence's only `Ok` is
                        // `Outcome::Ran` (design doc §6). Asserted anyway so
                        // this stays the single decision point rather than
                        // a second, parallel pass/fail path.
                        Err(TestCaseError::fail(format!("run did not pass: {outcome}")))
                    }
                }
                Err(RunError::Diverged(d)) => Err(TestCaseError::fail(format!(
                    "convergence diverged after op {} (target {}):\n{}",
                    d.op_index, d.def_target, d.report
                ))),
                Err(other) => Err(TestCaseError::fail(format!("run error: {other:?}"))),
            }
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_convergence_holds_for_trivial_programs(program in trivial_program()) {
        run_one(&program)?;
    }

    /// ADR-0017: a program whose first table is a partitioned source, read
    /// by `GROUP BY` definitions over its partition column. See
    /// `partitioned_program`.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_convergence_holds_over_a_partitioned_source(program in partitioned_program()) {
        run_one(&program)?;
    }
}

/// ADR-0017: a hand-built program over a partitioned source, one per
/// partitioning strategy, converges end to end: seeds and writes through the
/// root land in each leaf, a `NULL` grain is written as `0` (the partition
/// key is part of the primary key), and the aggregate grouped by the
/// partition column follows deletes and a truncate of the root.
#[tokio::test(flavor = "multi_thread")]
async fn a_program_over_a_partitioned_source_converges_end_to_end() {
    use generative::model::PartitionStrategy;
    let cluster = TestCluster::start();
    for strategy in [
        PartitionStrategy::Hash { modulus: 3 },
        PartitionStrategy::List {
            values: 3,
            default: false,
        },
        PartitionStrategy::List {
            values: 1,
            default: true,
        },
        PartitionStrategy::Range { bounds: 3 },
    ] {
        let db = cluster.create_isolated_database().await;
        let program = partition_table(
            build_program_multi_with_shapes(
                &[TableSpec {
                    seed_values: vec![
                        (Some(10), Some(1)),
                        (Some(20), Some(2)),
                        (Some(30), Some(3)),
                        (Some(40), None),
                    ],
                    text_values: vec![None; 4],
                    bool_values: vec![None; 4],
                    uuid_values: vec![None; 4],
                    grain_values: vec![
                        Some("0".to_string()),
                        Some("1".to_string()),
                        Some("2".to_string()),
                        None,
                    ],
                    mutates: vec![
                        Mutate::Update {
                            pk: 2,
                            c1: Some(7),
                            c2: None,
                        },
                        Mutate::Delete { pk: 3 },
                        Mutate::DuplicateInsert {
                            pk: 1,
                            c1: Some(5),
                            c2: Some(5),
                        },
                        Mutate::DuplicateInsert {
                            pk: 3,
                            c1: Some(9),
                            c2: Some(9),
                        },
                        Mutate::Truncate,
                        Mutate::DuplicateInsert {
                            pk: 4,
                            c1: Some(1),
                            c2: Some(1),
                        },
                    ],
                }],
                &[(
                    0,
                    DefShape::Aggregate {
                        functions: vec![
                            AggregateFn::Sum(AggregateColumn::C1),
                            AggregateFn::Count,
                            AggregateFn::CountColumn(AggregateColumn::C2),
                        ],
                    },
                )],
            ),
            0,
            strategy,
        );
        let mut backend = ManualBackend::connect(db.dsn())
            .await
            .expect("connect manual backend");
        let pool =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
        let outcome = run_convergence(&mut backend, &pool, &program)
            .await
            .unwrap_or_else(|err| panic!("{strategy:?}: {err:?}"));
        assert!(outcome.as_pass(), "{strategy:?}: {outcome}");
    }
}

/// Issue #1320: partition maintenance, partition-key updates and writes
/// named on a leaf or a detached table converge end to end, one program per
/// partitioning strategy. Each check after a maintenance op waits for the
/// reconcile pass that queues its correction, and for the correction.
#[tokio::test(flavor = "multi_thread")]
async fn partition_maintenance_converges_end_to_end() {
    use generative::generate::partition::{
        MaintenanceKind, PartitionDraw, RECONCILE_INTERVAL, add_partition_maintenance,
    };
    use generative::model::{Op, PartitionStrategy};
    let cluster = TestCluster::start();
    for strategy in [
        PartitionStrategy::Hash { modulus: 2 },
        PartitionStrategy::List {
            values: 2,
            default: true,
        },
        PartitionStrategy::Range { bounds: 3 },
    ] {
        let db = cluster.create_isolated_database().await;
        let mut spec = TableSpec::numeric_only(
            vec![
                (Some(10), Some(1)),
                (Some(20), Some(2)),
                (Some(30), Some(3)),
            ],
            vec![
                Mutate::Update {
                    pk: 2,
                    c1: Some(7),
                    c2: None,
                },
                Mutate::DuplicateInsert {
                    pk: 1,
                    c1: Some(5),
                    c2: Some(5),
                },
            ],
        );
        spec.grain_values = vec![Some("0".into()), Some("1".into()), Some("2".into())];
        let base = partition_table(
            build_program_multi_with_shapes(
                &[spec],
                &[(
                    0,
                    DefShape::Aggregate {
                        functions: vec![AggregateFn::Sum(AggregateColumn::C1), AggregateFn::Count],
                    },
                )],
            ),
            0,
            strategy,
        );
        let maintenance = |at, pick, kind| PartitionDraw::Maintenance { at, pick, kind };
        let program = add_partition_maintenance(
            base,
            0,
            &[
                PartitionDraw::MoveRow {
                    at: 3,
                    pk: 1,
                    grain: 2,
                    via_leaf: false,
                },
                PartitionDraw::ViaLeaf {
                    at: 3,
                    detached: false,
                },
                maintenance(4, 0, MaintenanceKind::TruncateLeaf),
                maintenance(4, 1, MaintenanceKind::Detach { concurrently: true }),
                PartitionDraw::ViaLeaf {
                    at: 4,
                    detached: true,
                },
                maintenance(5, 0, MaintenanceKind::Attach),
                maintenance(5, 0, MaintenanceKind::Drop),
                maintenance(
                    5,
                    0,
                    MaintenanceKind::Create {
                        rows: 2,
                        standalone: false,
                    },
                ),
                maintenance(
                    5,
                    1,
                    MaintenanceKind::Detach {
                        concurrently: false,
                    },
                ),
                maintenance(
                    5,
                    0,
                    MaintenanceKind::Create {
                        rows: 1,
                        standalone: true,
                    },
                ),
                PartitionDraw::MoveRow {
                    at: 5,
                    pk: 3,
                    grain: 3,
                    via_leaf: true,
                },
            ],
        );
        assert!(
            program
                .ops
                .iter()
                .filter(|op| matches!(op, Op::Partition { .. }))
                .count()
                >= 4
                && program.ops.iter().any(|op| matches!(op, Op::Leaf { .. })),
            "{strategy:?}: {:#?}",
            program.ops
        );
        let mut backend = ManualBackend::connect(db.dsn())
            .await
            .expect("connect manual backend");
        backend.set_reconcile_interval(RECONCILE_INTERVAL);
        let pool =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
        let outcome = run_convergence(&mut backend, &pool, &program)
            .await
            .unwrap_or_else(|err| panic!("{strategy:?}: {err:?}\n{:#?}", program.ops));
        assert!(outcome.as_pass(), "{strategy:?}: {outcome}");
    }
}

/// The named "first end-to-end green": a fully-worked, hand-built convergent
/// program driven through the real convergence property (not the generator),
/// so a reader has one concrete, legible case that must pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_hand_built_program_converges_end_to_end() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Seed three rows, then update one and delete one — every mutate hits a
    // real row.
    let program = build_program(
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

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("hand-built program must converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// Control (design doc §7): a program whose mutate stream includes ops that
/// name a never-seeded primary key. Postgres updates/deletes zero rows — a
/// source no-op, not an error — and the property must still converge, proving
/// "operation errors are checked, not swallowed" (design doc §4) does not
/// spuriously fail a run.
#[tokio::test(flavor = "multi_thread")]
async fn no_op_mutations_on_missing_rows_still_converge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(Some(7), Some(8))],
        &[
            // pk 42 was never seeded: an update and a delete that hit nothing.
            Mutate::Update {
                pk: 42,
                c1: Some(1),
                c2: Some(1),
            },
            Mutate::Delete { pk: 42 },
            // A real update on the one seeded row, to prove the run still makes
            // progress after the no-ops.
            Mutate::Update {
                pk: 1,
                c1: Some(9),
                c2: Some(9),
            },
        ],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("no-op mutations must not break convergence");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// The red half of the control (design doc §6/§7): prove the harness can tell
/// green from red. Settle a program, then corrupt the persisted target
/// directly — not via a source change, so the pipeline won't repair it — and
/// confirm the exact comparison the property runs per op ([`check_program`])
/// reports the divergence, localized to `target != SQL`. Without this, a
/// convergence run that always reported "converged" would look identical to a
/// working one.
#[tokio::test(flavor = "multi_thread")]
async fn the_harness_detects_a_corrupted_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(&[(Some(10), Some(1)), (Some(20), Some(2))], &[]);
    let def = program.defs[0].clone();
    let pk_col = program.tables[0].pk_col.clone();

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // First converge cleanly through the real property.
    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("program must converge before we corrupt it");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    // Corrupt one target cell directly; the source is untouched, so both the
    // SQL oracle and the evaluator still compute the right value.
    let qualified = qualified_target_table("public", &def);
    let client = pool.get().await.expect("pool connection");
    client
        .execute(
            &format!("update {qualified} set \"total\" = 999 where \"{pk_col}\" = 1"),
            &[],
        )
        .await
        .expect("corrupt target");

    let snapshot = backend.snapshot().await.expect("snapshot after corruption");
    let found = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");

    let [(target, report)]: [_; 1] = found
        .try_into()
        .expect("the harness must detect the corrupted target, and only it");
    assert_eq!(target, def.target);
    assert_eq!(
        report.target_vs_sql.len(),
        1,
        "corruption localizes to target != SQL: {report}"
    );
    assert!(
        report.evaluator_vs_sql.is_empty(),
        "only the persisted target is wrong; the evaluator agrees with the SQL oracle: {report}"
    );
}

/// [`check_program`] reports every diverging target, in definition order, not
/// just the first (#671): a plant sweep attributes a failure by the targets it
/// diverged in, so a divergence earlier in definition order must not hide a
/// later one. Three definitions share one source; the first and last targets
/// are corrupted and the middle one is left alone.
#[tokio::test(flavor = "multi_thread")]
async fn the_harness_reports_every_corrupted_target_in_definition_order() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi(
        &[TableSpec::numeric_only(
            vec![(Some(10), Some(1)), (Some(20), Some(2))],
            Vec::new(),
        )],
        &[0, 0, 0],
    );
    assert_eq!(program.defs.len(), 3);
    let pk_col = program.tables[0].pk_col.clone();

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("program must converge before we corrupt it");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let client = pool.get().await.expect("pool connection");
    for def in [&program.defs[0], &program.defs[2]] {
        let qualified = qualified_target_table("public", def);
        client
            .execute(
                &format!("update {qualified} set \"total\" = 999 where \"{pk_col}\" = 1"),
                &[],
            )
            .await
            .expect("corrupt target");
    }

    let snapshot = backend.snapshot().await.expect("snapshot after corruption");
    let found = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");
    let targets: Vec<&str> = found.iter().map(|(target, _)| target.as_str()).collect();
    assert_eq!(
        targets,
        [
            program.defs[0].target.as_str(),
            program.defs[2].target.as_str()
        ],
        "both corrupted targets, in definition order, and not the untouched one"
    );
}

/// Closes the issue #6 gap: a program whose mutate stream includes a
/// [`Mutate::DuplicateInsert`] — a second `INSERT` at an already-seeded pk,
/// rejected by the source table's real primary-key constraint. Unlike the
/// missing-pk update/delete case (`no_op_mutations_on_missing_rows_still_converge`),
/// this is a genuine `apply()` `Err`, not a silent zero-row success. The
/// insert is atomic, so the source is unaffected, and the property must still
/// converge — "an op that errors changed nothing" (design doc §4) exercised
/// against a real rejection, not just a no-op.
#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_pk_insert_error_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(Some(1), Some(2)), (Some(3), Some(4))],
        &[
            // pk 1 is already seeded: this insert must be rejected by the
            // primary-key constraint, a real `apply()` error.
            Mutate::DuplicateInsert {
                pk: 1,
                c1: Some(999),
                c2: Some(999),
            },
            // A real update afterward, to prove the run still makes progress.
            Mutate::Update {
                pk: 1,
                c1: Some(10),
                c2: Some(20),
            },
        ],
    );

    // No separate pin re-applying the duplicate-pk insert against a
    // throwaway connection is needed here: `build_program` tags this op
    // `OpOutcome::Fails`, and `run_convergence` itself now classifies every
    // op's actual outcome and asserts it against that expectation (design
    // doc §4 "operation errors are checked, not swallowed"). If
    // `DuplicateInsert` ever stopped actually erroring (or started
    // affecting rows), `run_convergence` below would return
    // `Err(RunError::UnexpectedOpOutcome { .. })` and the `.expect` below
    // would fail with that mismatch spelled out — no separate pin required.
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("a rejected duplicate-pk insert must not break convergence");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// A seeded row with `NULL` in a nullable column (design doc §3's awkward
/// values) converges: `NULL + n = NULL` on both sides (Postgres's `numeric`
/// arithmetic and `defs::eval`'s `Operator::Add` arm agree), so the
/// SQL oracle and the maintained target must agree too.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_value_in_a_nullable_column_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(None, Some(4)), (Some(5), None)],
        &[Mutate::Update {
            pk: 1,
            c1: None,
            c2: None,
        }],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("NULL values in nullable columns must not break convergence");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// Improvement-plan task B3: a hand-built, fully-worked 2-table/2-def
/// program (one definition per table) driven through the real convergence
/// property end-to-end. `trivial_program`'s proptest strategy now draws
/// multi-table/multi-def shapes too (see `tests/coverage.rs`'s
/// `trivial_program_sometimes_draws_more_than_one_table`/`_definition`), but
/// per this suite's own "coverage that silently drops out" principle
/// (`docs/generative-test-suite.md`), that shape also needs its own pin, not
/// just a property that happens to draw it sometimes.
///
/// Table A gets seeded, updated, and deleted from; table B gets seeded and
/// updated independently — proving both tables' pk-liveness simulations, ops,
/// and definitions are actually independent against a real cluster, not just
/// in `build_program_multi`'s own DB-free unit tests
/// (`generate::tests::pk_liveness_multi_table`).
#[tokio::test(flavor = "multi_thread")]
async fn a_two_table_two_def_program_converges_end_to_end() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi(
        &[
            TableSpec::numeric_only(
                vec![(Some(1), Some(2)), (Some(3), Some(4))],
                vec![
                    Mutate::Update {
                        pk: 1,
                        c1: Some(10),
                        c2: Some(20),
                    },
                    Mutate::Delete { pk: 2 },
                ],
            ),
            TableSpec::numeric_only(
                vec![(Some(5), Some(6))],
                vec![Mutate::Update {
                    pk: 1,
                    c1: Some(50),
                    c2: Some(60),
                }],
            ),
        ],
        // One definition per table (not fan-out) — the other B3 shape,
        // two defs sharing one source, is covered by
        // `generate::tests::pk_liveness_multi_table::two_definitions_can_share_one_source_table`
        // (DB-free) and by the property itself
        // (`trivial_program_sometimes_draws_defs_sharing_a_source_and_sometimes_draws_defs_on_different_sources`).
        &[0, 1],
    );
    assert_eq!(program.tables.len(), 2);
    assert_eq!(program.defs.len(), 2);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("a 2-table/2-def program must converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// Improvement-plan task B1: a hand-built program exercising the new
/// `Text`/`Boolean`/`Uuid` columns (and their identity-passthrough fields)
/// end-to-end against a real cluster — not just `trivial_program`'s property
/// happening to draw them, per this suite's own "coverage that silently
/// drops out" principle (same rationale as the B3 pin above).
///
/// Row 1 carries plain, unremarkable values for all three new columns. Row 2
/// carries the U+001F ("unit separator") awkward text literal specifically
/// (design doc §3 / the module doc comment's B1 scope-cut note): it's the
/// same byte `defs::ddl::join_pk_key`'s composite-key encoding treats
/// specially, so proving *this* value round-trips correctly through a plain
/// `Text` column — SQL binding, the `::text` cast on both insert and every
/// read-back, this harness's own snapshot diffing — is the most direct way
/// to confirm the "it doesn't yet attack key encoding, but it's still real
/// text-round-tripping coverage" claim actually holds against a live engine,
/// not just in the DB-free generator. Row 2 also draws `NULL` for the
/// `Boolean` column, so a `NULL` in one of the new columns (not just
/// `c1`/`c2`) gets real end-to-end coverage too.
#[tokio::test(flavor = "multi_thread")]
async fn a_text_boolean_uuid_program_converges_end_to_end() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi(
        &[TableSpec {
            seed_values: vec![(Some(1), Some(2)), (Some(3), Some(4))],
            text_values: vec![
                Some("hello".to_string()),
                Some("has\u{1f}unit-separator".to_string()),
            ],
            bool_values: vec![Some("true".to_string()), None],
            uuid_values: vec![
                Some("123e4567-e89b-42d3-a456-426614174000".to_string()),
                Some("00000000-0000-4000-8000-000000000000".to_string()),
            ],
            grain_values: vec![None, None],
            mutates: vec![],
        }],
        &[0],
    );
    assert_eq!(program.tables.len(), 1);
    assert_eq!(program.defs.len(), 1);
    assert_eq!(
        program.defs[0].fields.len(),
        4,
        "expected `total` plus one passthrough field per new column"
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("a Text/Boolean/Uuid program must converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// Improvement-plan task B2: a hand-built program whose "derived" field is
/// `STRPOS(text_col, 'wor') > 0` — a nested, *mixed* operator-and-function
/// expression (`Operator::GreaterThan` wrapping a `FunctionCall`) — driven
/// through the real convergence property end-to-end. Not just the property
/// happening to draw this `DerivedShape` sometimes (per this suite's own
/// "coverage that silently drops out" principle, same rationale as the B1/B3
/// pins above): this is the specific shape the render_expr parenthesization
/// fix (`generative::backend::manual::render_expr`) and the oracle's new
/// `>`/function rendering (`generative::oracle::render_expr`) both exist for.
///
/// Row 1's `text_col` is `"hello world"`, so `STRPOS(text_col, 'wor')` finds
/// a match (`> 0` is `true`); row 2's is `"goodbye"`, so it doesn't (`> 0` is
/// `false`) — both branches of the nested comparison get real, live coverage,
/// not just the "always true" or "always false" case a less deliberate
/// choice of values could have left untested.
#[tokio::test(flavor = "multi_thread")]
async fn a_nested_mixed_operator_and_function_expression_converges_end_to_end() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_derived(
        &[TableSpec {
            seed_values: vec![(Some(1), Some(2)), (Some(3), Some(4))],
            text_values: vec![Some("hello world".to_string()), Some("goodbye".to_string())],
            bool_values: vec![Some("true".to_string()), Some("false".to_string())],
            uuid_values: vec![
                Some("123e4567-e89b-42d3-a456-426614174000".to_string()),
                Some("00000000-0000-4000-8000-000000000000".to_string()),
            ],
            grain_values: vec![None, None],
            mutates: vec![],
        }],
        &[0],
        &[DerivedShape::StrposGreaterThan {
            needle: "wor".to_string(),
            threshold: 0,
        }],
    );
    assert_eq!(program.tables.len(), 1);
    assert_eq!(program.defs.len(), 1);
    assert_eq!(
        program.defs[0].fields.len(),
        5,
        "expected total + 3 passthroughs + 1 derived"
    );
    let derived = &program.defs[0].fields.last().expect("derived field").expr;
    assert!(
        matches!(
            derived,
            Expr::BinaryOp {
                op: trellis::dev::defs::ast::Operator::GreaterThan,
                lhs,
                ..
            } if matches!(lhs.as_ref(), Expr::FunctionCall { name, .. } if name == "STRPOS")
        ),
        "expected the derived field to be STRPOS(...) > <literal>, got {derived:?}"
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("a nested mixed operator-and-function expression must converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// Improvement-plan task B4, the non-negotiable pin: a hand-built
/// `KeySpace::Aggregate` program driven through the real convergence property
/// end-to-end, specifically driving one group to empty via deletes — the
/// single hardest edge the task names (the only non-idempotent maintenance
/// path in the engine, per `docs/generative-test-suite.md` §4). Table `t0`'s
/// grain column puts pks 1 and 2 in group `"0"` and pk 3 in group `"1"`; the
/// mutate stream deletes *both* of group `"0"`'s members.
///
/// `run_convergence` already checks the persisted target against the SQL
/// oracle after every op — Postgres's own `GROUP BY` naturally produces zero
/// rows for an empty group, so if the engine ever left group `"0"`'s row
/// behind (stale or zeroed) instead of deleting it outright, that per-op
/// check would already fail — but the explicit snapshot assertions below
/// additionally make the "genuinely gone, not left behind as zeroes" claim
/// directly visible, per the task's own wording, rather than only inferred
/// from a green run.
#[tokio::test(flavor = "multi_thread")]
async fn an_aggregate_group_emptied_by_deletes_converges_and_the_row_disappears() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_shapes(
        &[TableSpec {
            seed_values: vec![
                (Some(10), Some(1)),
                (Some(20), Some(2)),
                (Some(99), Some(3)),
            ],
            text_values: vec![None, None, None],
            bool_values: vec![None, None, None],
            uuid_values: vec![None, None, None],
            grain_values: vec![
                Some("0".to_string()),
                Some("0".to_string()),
                Some("1".to_string()),
            ],
            mutates: vec![Mutate::Delete { pk: 1 }, Mutate::Delete { pk: 2 }],
        }],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![
                    AggregateFn::Sum(AggregateColumn::C1),
                    AggregateFn::Count,
                    AggregateFn::Avg(AggregateColumn::C1),
                    AggregateFn::CountColumn(AggregateColumn::C1),
                ],
            },
        )],
    );
    assert_eq!(program.tables.len(), 1);
    assert_eq!(program.defs.len(), 1);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("an aggregate group emptied by deletes must still converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    let target = &snapshot[&program.defs[0].target];
    let emptied_group = group_key(&[Some("0.00".to_string())]);
    let surviving_group = group_key(&[Some("1.00".to_string())]);
    assert!(
        !target.contains_key(&emptied_group),
        "group \"0\" was emptied by deleting both its members and must have no target row at \
         all — not a stale/zeroed row: {target:?}"
    );
    assert!(
        target.contains_key(&surviving_group),
        "group \"1\" (pk 3, untouched) must still have its target row: {target:?}"
    );
    let g1 = &target[&surviving_group];
    assert_eq!(g1["sum_c1"], Some("99.00".to_string()), "group 1's sum");
    assert_eq!(g1["cnt"], Some("1".to_string()), "group 1's count");
    assert_eq!(
        g1["count_c1"],
        Some("1".to_string()),
        "group 1's column count"
    );
}

/// Improvement-plan task B4: `AVG`'s hidden sum/count partials, and — since
/// this def's `SUM` and `AVG` both aggregate the exact same column — the one
/// partial the group basis keeps for them (`trellis::staging::group_basis`,
/// one per distinct argument). An
/// insert, an update that changes the aggregated value, and a delete all
/// land in the same group, so every fold path (insert delta, update delta,
/// delete delta) exercises both fields' partials in one run.
#[tokio::test(flavor = "multi_thread")]
async fn avg_and_sum_over_the_same_column_share_a_partial_and_stay_correct_through_mutation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_shapes(
        &[TableSpec {
            seed_values: vec![(Some(10), Some(0)), (Some(30), Some(0))],
            text_values: vec![None, None],
            bool_values: vec![None, None],
            uuid_values: vec![None, None],
            grain_values: vec![Some("0".to_string()), Some("0".to_string())],
            mutates: vec![
                // Group "0" starts as {10, 30} (sum 40, avg 20). Update pk 1
                // to 50 (sum 80, avg 40), then delete pk 2 (sum 50, avg 50).
                Mutate::Update {
                    pk: 1,
                    c1: Some(50),
                    c2: Some(0),
                },
                Mutate::Delete { pk: 2 },
            ],
        }],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![
                    AggregateFn::Sum(AggregateColumn::C1),
                    AggregateFn::Avg(AggregateColumn::C1),
                ],
            },
        )],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("AVG/SUM sharing a partial must still converge through insert/update/delete");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    let target = &snapshot[&program.defs[0].target];
    let group = group_key(&[Some("0.00".to_string())]);
    let row = &target[&group];
    assert_eq!(row["sum_c1"], Some("50.00".to_string()), "final sum");
    // Postgres's numeric division picks its own display scale (e.g.
    // `50.0000000000000000`), which this grammar's `AVG` inherits rather
    // than reformats — compare by parsed value, not exact text.
    let avg: f64 = row["avg_c1"]
        .as_deref()
        .expect("avg_c1 must not be NULL")
        .parse()
        .expect("avg_c1 must parse as a number");
    assert!(
        (avg - 50.0).abs() < 1e-9,
        "final avg: expected 50, got {avg}"
    );
}
