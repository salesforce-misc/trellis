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
//! case still provisions a fresh *isolated database* (its own schema, slot,
//! and publication) inside that shared cluster.
//!
//! **Issue #188: each case's slot/publication names must be unique, not just
//! its database.** A Postgres logical replication slot name is unique
//! *cluster-wide*, not scoped per database, even though the slot itself is
//! tied to the database it was created against. Every case used to install
//! through `ClientOptions::default()`'s literal `"trellis_slot"`/
//! `"trellis_pub"` (the `ManualBackend` seam, issue #3, starts a client with
//! a fixed slot per `install`), which was survivable only because each
//! case's prior database was normally fully dropped before the next case's
//! install ran. But `trellis::Client::drop` is a documented best-effort
//! shutdown signal, not a synchronous join, so occasionally a case's
//! `EngineClient` (and the replication connection actually holding its
//! slot) hadn't finished disconnecting by the time that case's
//! `TestDatabase::drop` ran `dropdb --force` — and Postgres refuses to drop
//! a database with an *active* logical slot outright, an error
//! `TestDatabase::drop` swallows silently. The next case's fresh database
//! then tried to install against the same slot name, found it already
//! existed (owned by the still-live prior database) with no
//! `replication_progress` row in *its own* fresh schema, and
//! `initial_snapshot_handshake` correctly-by-its-own-lights but
//! incorrectly-overall called that an orphaned slot
//! (`Client(Intake(OrphanedSlot { .. }))`) — the failure this issue's nightly
//! `deep` run hit after 43 clean cases. See
//! `trellis/src/intake/publication.rs`'s `slot_is_orphaned` for a related
//! production hardening fix: that check now also scopes its
//! `pg_replication_slots` lookup to `database = current_database()`, so a
//! same-named slot genuinely owned by a *different* database is no longer
//! misreported as this database's own orphan.
//!
//! The fix here is simpler than synchronizing the shutdown race: every case
//! gets its own slot/publication name, derived from its already-unique
//! isolated database name via [`ManualBackend::set_slot_and_publication`].
//! Two cases can then never collide on a slot no matter how their
//! `EngineClient` teardowns overlap in time — the same "give every case its
//! own everything" isolation `create_isolated_database` already gives
//! schema/tables, extended to the two names that used to be the one
//! shared-per-run exception. (A used-but-not-yet-dropped slot from a slow
//! prior case can still make a **later** attempt to reuse *that exact same*
//! name collide in principle, but with process-id-plus-monotonic-counter
//! uniqueness per `testkit::TestCluster::create_empty_database`, that's not
//! a real risk within one run.)
//!
//! Case count defaults to 16 (design doc §9's 12–24 band) and is overridable
//! for deep sweeps with `PROPTEST_CASES` (e.g. `PROPTEST_CASES=500 cargo test
//! -p generative --test convergence`). Failing seeds are persisted to the
//! checked-in `tests/proptest-regressions/convergence.txt` and replayed first.

use generative::backend::{Backend, ManualBackend};
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, DerivedShape, Mutate, RelAggregateFn, RelFieldKind,
    RelFieldSpec, TableSpec, build_program, build_program_multi, build_program_multi_with_derived,
    build_program_multi_with_relationships, build_program_multi_with_shapes, trivial_program,
};
use generative::model::group_key;
use generative::run::{RunError, check_program, run_convergence};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::defs::ast::Expr;
use trellis::defs::qualified_target_table;
use trellis::{Config, Pool};

/// One tokio runtime and one shared, initdb-once cluster for a test thread's
/// proptest cases. See the module doc comment for why this is thread-local.
///
/// `coverage` (A2, `local_docs/generative-suite-improvement-plan.md`)
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
            // `generative/src/backend/manual.rs` (see
            // `local_docs/generative-suite-improvement-plan.md` "C2").
            let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();
            let start = timing_enabled.then(std::time::Instant::now);
            let cluster = TestCluster::start();
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
            // C2: per-case isolated-database provisioning (schema, slot,
            // publication) inside the shared cluster. See the cluster-startup
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
            // Issue #188: give this case its own slot/publication names,
            // not the shared `ClientOptions::default()` literals — see the
            // module doc comment. `db.name()` is already unique per case
            // (`testkit::TestCluster::create_empty_database`'s
            // process-id-plus-counter suffix); replication slot names only
            // allow `[a-z0-9_]`, so hyphens are sanitized to underscores.
            let unique = db.name().replace('-', "_");
            backend.set_slot_and_publication(format!("{unique}_slot"), format!("{unique}_pub"));
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
    fn property_convergence_holds_for_trivial_programs(program in trivial_program()) {
        run_one(&program)?;
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

    let (target, report) = found.expect("the harness must detect the corrupted target");
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
/// arithmetic and `trellis::defs::eval`'s `Operator::Add` arm agree), so the
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
/// same byte `trellis::intake::extract_key`'s composite-key encoding treats
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
            rel_fk_values: vec![None, None],
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
            rel_fk_values: vec![None, None],
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
                op: trellis::defs::ast::Operator::GreaterThan,
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
            rel_fk_values: vec![None, None, None],
            mutates: vec![Mutate::Delete { pk: 1 }, Mutate::Delete { pk: 2 }],
        }],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![
                    AggregateFn::Sum(AggregateColumn::C1),
                    AggregateFn::Count,
                    AggregateFn::Avg(AggregateColumn::C1),
                    AggregateFn::Min(AggregateColumn::C1),
                    AggregateFn::Max(AggregateColumn::C1),
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
    let emptied_group = group_key(&[Some("0".to_string())]);
    let surviving_group = group_key(&[Some("1".to_string())]);
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
    assert_eq!(g1["sum_c1"], Some("99".to_string()), "group 1's sum");
    assert_eq!(g1["cnt"], Some("1".to_string()), "group 1's count");
    assert_eq!(g1["min_c1"], Some("99".to_string()), "group 1's min");
    assert_eq!(g1["max_c1"], Some("99".to_string()), "group 1's max");
}

/// Improvement-plan task B4: `MIN`/`MAX` are always
/// `trellis::defs::invertibility::Invertibility::RecomputeOnly` (never
/// delta-maintained) — deleting a group's *current* extreme member must
/// recompute the new extreme from the group's remaining rows, not fall back
/// to a stale cached value. `trellis/tests/apply_aggregate.rs` already covers
/// this shape by hand at the engine layer; this pin drives the identical
/// edge through the generative harness's own convergence property, so a
/// regression here is caught by the same three-way (target/evaluator/SQL)
/// machinery every other case in this suite is.
#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_groups_current_min_and_max_forces_a_real_recompute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_shapes(
        &[TableSpec {
            seed_values: vec![(Some(5), Some(0)), (Some(20), Some(0)), (Some(1), Some(0))],
            text_values: vec![None, None, None],
            bool_values: vec![None, None, None],
            uuid_values: vec![None, None, None],
            grain_values: vec![Some("0".to_string()); 3],
            rel_fk_values: vec![None; 3],
            // Delete the current max (pk 2, c1=20) and the current min
            // (pk 3, c1=1); only pk 1 (c1=5) survives.
            mutates: vec![Mutate::Delete { pk: 2 }, Mutate::Delete { pk: 3 }],
        }],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![
                    AggregateFn::Min(AggregateColumn::C1),
                    AggregateFn::Max(AggregateColumn::C1),
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
        .expect("deleting the current min/max must still converge via recompute");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    let target = &snapshot[&program.defs[0].target];
    let group = group_key(&[Some("0".to_string())]);
    let row = &target[&group];
    assert_eq!(
        row["min_c1"],
        Some("5".to_string()),
        "min must recompute to the one surviving row (5), not stay stale at 1"
    );
    assert_eq!(
        row["max_c1"],
        Some("5".to_string()),
        "max must recompute to the one surviving row (5), not stay stale at 20"
    );
}

/// Improvement-plan task B4: `AVG`'s hidden sum/count partials
/// (`trellis::defs::invertibility::PartialField`), and — since this def's
/// `SUM` and `AVG` both aggregate the exact same column — the shared hidden
/// running-count column `trellis::defs::ddl::count_column_names` gives them
/// (`trellis/tests/apply_aggregate.rs` covers the shared-count-column shape by
/// hand; this drives the same shape through the generative harness). An
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
            rel_fk_values: vec![None, None],
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
    let group = group_key(&[Some("0".to_string())]);
    let row = &target[&group];
    assert_eq!(row["sum_c1"], Some("50".to_string()), "final sum");
    // Postgres's numeric division picks its own display scale (e.g.
    // `50.0000000000000000`), which this grammar's `AVG` inherits rather
    // than reformats — compare by parsed value, not exact text, matching
    // `trellis/tests/apply_aggregate.rs`'s own `assert_count_and_avg` helper.
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

// ---------------------------------------------------------------------
// Issue #34: relationship edges (ADR-0006), one hand-built pin per
// engine-supported shape. Each drives the same per-op convergence property
// the property test does, so a reader has a legible, minimal case for each
// of the three shapes and a regression names the shape it broke.
// ---------------------------------------------------------------------

/// A [`TableSpec`] with `rel_fk_values` and `grain_values` spelled out and
/// every other non-numeric column `NULL` — the relationship pins below only
/// vary along those two axes, so spelling the other three out five times
/// apiece would bury what actually differs.
fn rel_spec(
    seed_values: Vec<(Option<i64>, Option<i64>)>,
    grain_values: Vec<Option<String>>,
    rel_fk_values: Vec<Option<String>>,
    mutates: Vec<Mutate>,
) -> TableSpec {
    let n = seed_values.len();
    TableSpec {
        seed_values,
        text_values: vec![None; n],
        bool_values: vec![None; n],
        uuid_values: vec![None; n],
        grain_values,
        rel_fk_values,
        mutates,
    }
}

/// A bare to-one enrichment (`rel_enrich = <rel>.<c1>`) on a row-grain
/// definition, exercising all three join outcomes ADR-0006 distinguishes in
/// one program: `t0`'s three rows carry a foreign key that **matches** a
/// related row (`k1`), one that **matches nothing** (`k9`), and one that is
/// **`NULL`**. Every one of the three source rows must survive with its own
/// enrichment (`NULL` for the latter two) — a `LEFT JOIN`, never an inner
/// one.
///
/// It also exercises **reverse propagation**, for free and unavoidably: ops
/// are emitted one table at a time, so every `t0` row is inserted and
/// converged *before* `t1` exists at all. The matching row's enrichment
/// therefore starts `NULL` and must be re-derived when the related row
/// arrives, and re-derived again when that related row is updated and then
/// deleted.
#[tokio::test(flavor = "multi_thread")]
async fn a_to_one_relationship_enrichment_converges_end_to_end() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            rel_spec(
                vec![(Some(1), Some(2)), (Some(3), Some(4)), (Some(5), Some(6))],
                vec![None, None, None],
                vec![Some("k1".to_string()), Some("k9".to_string()), None],
                Vec::new(),
            ),
            rel_spec(
                vec![(Some(100), Some(0)), (Some(200), Some(0))],
                vec![None, None],
                vec![None, None],
                vec![
                    // The related row `t0`'s first row joins to changes,
                    // then vanishes: both must propagate back.
                    Mutate::Update {
                        pk: 1,
                        c1: Some(111),
                        c2: Some(0),
                    },
                    Mutate::Delete { pk: 1 },
                ],
            ),
        ],
        &[(0, DefShape::OneToOne)],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneBare,
        })],
    );
    assert_eq!(program.relationships.len(), 1);
    assert_eq!(
        program.relationships[0].cardinality,
        generative::model::Cardinality::ToOne
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("a to-one relationship enrichment must converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    // The related row was deleted by the last op, so every enrichment is
    // back to NULL — and, critically, all three source rows are still there.
    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    let target = &snapshot[&program.defs[0].target];
    assert_eq!(
        target.len(),
        3,
        "a LEFT JOIN keeps every source row alive regardless of whether its FK resolves: \
         {target:?}"
    );
    for pk in ["1", "2", "3"] {
        assert_eq!(
            target[pk]["rel_enrich"], None,
            "pk {pk}'s enrichment must be NULL once the related row is gone: {target:?}"
        );
    }
}

/// A to-many aggregate (`rel_agg = COUNT(<rel>.<c1>)`) on a row-grain
/// definition: `t0`'s unique key column is the *from* side, `t1`'s
/// non-unique FK column the *to* side, so one source row fans out to many
/// related rows. `COUNT` specifically, because it is the one aggregate whose
/// empty-related-set answer is `0` rather than `NULL` — the to-many half of
/// ADR-0006's nullability rule, and the half a naive `NULL`-propagating
/// implementation gets wrong.
#[tokio::test(flavor = "multi_thread")]
async fn a_to_many_relationship_aggregate_converges_end_to_end() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            // `t0`'s key column is `k1`/`k2` (derived from the pk), so row 1
            // has two related rows below, row 2 has one, and row 3 has none.
            rel_spec(
                vec![(Some(1), Some(2)), (Some(3), Some(4)), (Some(5), Some(6))],
                vec![None, None, None],
                vec![None, None, None],
                Vec::new(),
            ),
            rel_spec(
                vec![
                    (Some(10), Some(0)),
                    (Some(20), Some(0)),
                    (Some(30), Some(0)),
                ],
                vec![None, None, None],
                vec![
                    Some("k1".to_string()),
                    Some("k1".to_string()),
                    Some("k2".to_string()),
                ],
                vec![Mutate::Delete { pk: 1 }],
            ),
        ],
        &[(0, DefShape::OneToOne)],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToManyAggregate(RelAggregateFn::Count),
        })],
    );
    assert_eq!(
        program.relationships[0].cardinality,
        generative::model::Cardinality::ToMany
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("a to-many relationship aggregate must converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    let target = &snapshot[&program.defs[0].target];
    assert_eq!(
        target["1"]["rel_agg"],
        Some("1".to_string()),
        "pk 1 had two related rows, one of which was deleted: {target:?}"
    );
    assert_eq!(
        target["2"]["rel_agg"],
        Some("1".to_string()),
        "pk 2 has exactly one related row: {target:?}"
    );
    assert_eq!(
        target["3"]["rel_agg"],
        Some("0".to_string()),
        "pk 3 has no related rows at all, and COUNT over the empty set is 0 (not NULL): \
         {target:?}"
    );
}

/// A to-one relationship path aggregated **inside a `GROUP BY`** — the shape
/// this branch's engine work (issue #94) made legal, and the one the
/// generator would previously have had to steer around. `SUM(<rel>.<c1>)`
/// folds one related value per source row across each group, so it is an
/// ordinary aggregate over a `LEFT JOIN`, not nested aggregation.
///
/// Both grain groups contain a row whose foreign key resolves and one whose
/// does not, so the test pins the interaction the two features have with
/// each other: an unmatched row must still *belong* to its group (it counts
/// toward `COUNT(*)`) while contributing nothing to `SUM`'s value set.
#[tokio::test(flavor = "multi_thread")]
async fn a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            rel_spec(
                vec![
                    (Some(1), Some(0)),
                    (Some(2), Some(0)),
                    (Some(3), Some(0)),
                    (Some(4), Some(0)),
                ],
                vec![
                    Some("0".to_string()),
                    Some("0".to_string()),
                    Some("1".to_string()),
                    Some("1".to_string()),
                ],
                vec![
                    Some("k1".to_string()),
                    Some("k9".to_string()),
                    Some("k2".to_string()),
                    None,
                ],
                Vec::new(),
            ),
            rel_spec(
                vec![(Some(10), Some(0)), (Some(20), Some(0))],
                vec![None, None],
                vec![None, None],
                Vec::new(),
            ),
        ],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![AggregateFn::Count],
            },
        )],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneAggregate(RelAggregateFn::Sum),
        })],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("a to-one relationship aggregated inside a GROUP BY must converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    let target = &snapshot[&program.defs[0].target];
    for (grain, expected_sum) in [("0", "10"), ("1", "20")] {
        let row = &target[&group_key(&[Some(grain.to_string())])];
        assert_eq!(
            row["cnt"],
            Some("2".to_string()),
            "grain {grain}: an unmatched FK must still count toward COUNT(*): {target:?}"
        );
        assert_eq!(
            row["rel_agg"],
            Some(expected_sum.to_string()),
            "grain {grain}: only the matched row contributes to SUM: {target:?}"
        );
    }
}

/// **Issue #98 coverage: the aggregate case.** The pinned finding above is a
/// `OneToOne` enrichment reading a to-one relationship; this is the same
/// root cause on a `GROUP BY` definition whose aggregated field reads a
/// to-one relationship path (issue #94's `SUM(post.word_count)` shape,
/// mirrored here by `a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end`'s
/// `rel_agg = SUM(<rel>.c1)`).
///
/// Byte-for-byte that test's two-row-per-group shape, restricted to one
/// grain group to keep the finding minimal, with `t1` (the to-side)
/// `TRUNCATE`d instead of left alone. `t0`'s two rows stay in their group
/// either way (`COUNT(*)` must hold at `2`), but the `SUM` must fall back to
/// `NULL` once the to-side has no rows left for it to match.
///
/// Whether this passes tells us whether the `apply.rs` fix above already
/// generalizes to the aggregate path: the reverse-recompute it stages is an
/// ordinary image-less `Recompute` on the from-side (`t0`) row, and
/// `apply_aggregate::accumulate_changes` always forces *any* image-less
/// change's group onto the full-recompute path (a `force_full_recompute`
/// group is re-derived by `apply_forced_groups_bulk`'s `LEFT JOIN`
/// unconditionally) regardless of whether the touching change carries a
/// real image — so no aggregate-specific code needed to change for this to
/// converge. (Before issue #136, epic #127, an *ordinary* image-bearing
/// change to a relationship-reading aggregate was also forced onto this
/// same path, via `accumulate_changes`'s own now-removed
/// `force_every_group`; #136 replaced that half with a real per-row delta,
/// but left the image-less half this test exercises untouched.)
#[tokio::test(flavor = "multi_thread")]
async fn truncating_a_relationship_to_side_table_leaves_a_stale_aggregate_enrichment() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            rel_spec(
                vec![(Some(1), Some(0)), (Some(2), Some(0))],
                vec![Some("0".to_string()), Some("0".to_string())],
                vec![Some("k1".to_string()), Some("k9".to_string())],
                Vec::new(),
            ),
            rel_spec(
                vec![(Some(10), Some(0))],
                vec![None],
                vec![None],
                vec![Mutate::Truncate],
            ),
        ],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![AggregateFn::Count],
            },
        )],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneAggregate(RelAggregateFn::Sum),
        })],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("truncating a relationship's to-side table must clear the aggregate it fed");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    let target = &snapshot[&program.defs[0].target];
    let row = &target[&group_key(&[Some("0".to_string())])];
    assert_eq!(
        row["cnt"],
        Some("2".to_string()),
        "both source rows must still count toward COUNT(*) after the truncate: {target:?}"
    );
    assert_eq!(
        row["rel_agg"], None,
        "the to-side table is empty, so the SUM must fall back to NULL: {target:?}"
    );
}

/// **Issue #98 regression pin.** `TRUNCATE` on a table some definition reads
/// *through a relationship* used to leave that definition's enrichment
/// permanently stale.
///
/// Mechanism (`trellis::staging::apply`): the truncate-clear path resolves
/// affected targets with `catalog::transforms_for_source` — definitions whose
/// **source** is the truncated table — while reverse propagation into
/// definitions that merely *read* the table lived in the separate keyed
/// by-source loop, driven by per-row change images and
/// `catalog::relationships_to_table`. A `TRUNCATE` stages one key-less
/// sentinel row (`append::TRUNCATE_SENTINEL_KEY`), not per-row images, so it
/// never reached that loop at all. Exactly the "its own logical-decoding
/// message, not a bulk delete" hazard the design doc §7 flags.
///
/// The program: `t0`'s single row has a foreign key resolving to `t1`'s
/// single row, so `rel_enrich` is `22`. Then `t1` is truncated. The oracle's
/// `LEFT JOIN` finds nothing and says `NULL`; the maintained target used to
/// still say `22`.
///
/// Fixed by having the truncate-clear path (`trellis/src/staging/apply.rs`)
/// also resolve `catalog::relationships_to_table` for the truncated table and
/// stage every from-side row with a non-`NULL` join column as a reverse
/// recompute, through the same `reverse_recomputes` accumulator the
/// row-driven path (issue #30) already feeds. Kept as a permanent regression
/// pin (design doc §6) rather than deleted now that it passes.
///
/// **Was failing again** (issue #168): epic #127's settled-parent-projection
/// reverse mechanism for to-one relationships did not handle TRUNCATE's
/// key-less sentinel — the projection was never cleared on a to-side
/// truncate, so the from-side recompute this path stages kept re-deriving
/// against the stale pre-truncate projection row. Fixed by also clearing the
/// relationship's settled parent projection in full
/// (`ApplyPlan::relationship_projection_clears`) whenever a `TRUNCATE`
/// empties a to-one relationship's to-side table. Re-enabled as a permanent
/// regression pin.
#[tokio::test(flavor = "multi_thread")]
async fn truncating_a_relationship_to_side_table_leaves_a_stale_enrichment() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            rel_spec(
                vec![(Some(1), Some(2))],
                vec![None],
                vec![Some("k1".to_string())],
                Vec::new(),
            ),
            rel_spec(
                vec![(Some(22), Some(0))],
                vec![None],
                vec![None],
                vec![Mutate::Truncate],
            ),
        ],
        &[(0, DefShape::OneToOne)],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneBare,
        })],
    );
    // `t1`'s `mutates: vec![Mutate::Truncate]` above is enough to generate
    // the triggering truncate directly now — the generator no longer steers
    // around this shape (issue #98 removed
    // `without_truncates_on_relationship_to_sides`, the workaround that used
    // to require manually re-chaining a `Truncate` op here to reproduce the
    // finding).

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    run_convergence(&mut backend, &pool, &program)
        .await
        .expect("truncating a relationship's to-side table must clear the enrichment it fed");
}

/// The control beside the finding above (design doc §6: "a control test
/// beside each finding — a case that must *converge* — proving the harness
/// can tell green from red").
///
/// Byte-for-byte the same program except the last op `DELETE`s `t1`'s only
/// row instead of `TRUNCATE`ing the table. The end state of the source is
/// identical — `t1` is empty either way — so any oracle that judged the two
/// differently would be wrong. This one converges, which localizes the
/// defect precisely: not reverse propagation in general, but the
/// `TRUNCATE`-shaped change specifically.
#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_relationship_to_side_row_does_clear_the_enrichment() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            rel_spec(
                vec![(Some(1), Some(2))],
                vec![None],
                vec![Some("k1".to_string())],
                Vec::new(),
            ),
            rel_spec(
                vec![(Some(22), Some(0))],
                vec![None],
                vec![None],
                vec![Mutate::Delete { pk: 1 }],
            ),
        ],
        &[(0, DefShape::OneToOne)],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneBare,
        })],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("deleting the related row must clear the enrichment it fed");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    assert_eq!(
        snapshot[&program.defs[0].target]["1"]["rel_enrich"], None,
        "the related row is gone, so the enrichment must be NULL: {snapshot:#?}"
    );
}
