//! The epoch tier (#2187): many generated programs run back to back on one
//! instance and one database, which is never reset between them.
//!
//! Every other tier starts each case from a fresh database. This one draws
//! a chain ([`generative::generate::epoch`]): a first program as
//! `trivial_program` draws it, then one to `GENERATIVE_MAX_EPOCHS` (default
//! 12) later epochs. Each later epoch starts at a quiet point with up to
//! three changes to what the earlier epochs left: a drop, a drop and
//! redefinition, a definition dropped in an earlier epoch brought back, a
//! second definition over a defined one's source, or an engine restart.
//! Then it writes each table's rows, carrying on from the rows the earlier
//! epochs left.
//!
//! [`run_epochs`] quiesces after every epoch and checks every definition
//! defined then against the oracle, and by a `Strict` `self_check`, so a
//! failure names the epoch that broke the instance.
//!
//! # Shrinking
//!
//! A failing chain shrinks whole epochs first, then within the epochs left:
//! the epochs are a proptest `Vec`, and the chain is re-rendered from what
//! remains (each table's ops keep their expected outcomes). The engine runs
//! three drain workers and each epoch's ops are issued without a quiesce
//! between them, so treat a shrunk chain as `tests/concurrent_convergence.rs`'s
//! shrink-trust convention says.

use generative::backend::ManualBackend;
use generative::generate::epoch::{EpochCase, chain, chain_shapes, epoch_case};
use generative::run::{RunError, run_epochs};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::Config;
use trellis::dev::pool::Pool;

/// The engine's drain workers: more than one, so an epoch's ops drain side
/// by side.
const WORKERS: usize = 3;

/// The engine's reconcile interval, where a definition's build starts. At
/// the engine's 5s default, waiting for an epoch's definitions to build
/// would be most of the run.
const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    cases: std::cell::Cell<usize>,
    shapes: std::cell::RefCell<std::collections::BTreeMap<&'static str, usize>>,
}

impl Drop for Harness {
    /// Print-only, as `tests/convergence.rs`'s.
    fn drop(&mut self) {
        if self.cases.get() > 0 {
            eprintln!(
                "generative: epoch_chain coverage over {} cases: {:?}",
                self.cases.get(),
                self.shapes.borrow()
            );
        }
    }
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: TestCluster::start().long_lived(),
        cases: std::cell::Cell::new(0),
        shapes: std::cell::RefCell::new(Default::default()),
    };
}

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    ProptestConfig {
        cases,
        max_shrink_time: generative::shrink::max_shrink_time_ms(),
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// Runs `case`'s chain against a fresh isolated database.
fn run_case(case: &EpochCase) -> Result<(), TestCaseError> {
    let chain = chain(case);
    HARNESS.with(|h| {
        h.cases.set(h.cases.get() + 1);
        for (name, n) in chain_shapes(&chain) {
            *h.shapes.borrow_mut().entry(name).or_insert(0) += n;
        }
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect_with_workers(db.dsn(), WORKERS)
                .await
                .expect("connect backend");
            backend.set_strict_audit(true);
            backend.set_reconcile_interval(RECONCILE_INTERVAL);
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
            match run_epochs(&mut backend, &pool, &chain).await {
                Ok(outcome) if outcome.as_pass() => Ok(()),
                Ok(outcome) => Err(TestCaseError::fail(format!("run did not pass: {outcome}"))),
                Err(RunError::Epoch { epoch, error }) => Err(TestCaseError::fail(format!(
                    "epoch {epoch} of {} failed: {}\nsteps: {:?}",
                    chain.epochs.len(),
                    match *error {
                        RunError::Diverged(d) => format!(
                            "diverged (targets {}):\n{}",
                            d.targets().collect::<Vec<_>>().join(", "),
                            d.all()
                                .map(|(t, r)| format!("target {t}:\n{r}"))
                                .collect::<Vec<_>>()
                                .join("\n")
                        ),
                        other => format!("{other:?}"),
                    },
                    chain.epochs[epoch].steps
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
    fn property_epoch_chains_converge_on_one_instance(case in epoch_case()) {
        run_case(&case)?;
    }
}

/// Every step an epoch's start takes, in one chain on one instance: a drop
/// and a restart, writes to a table nothing reads, the dropped definition
/// back under its name, a renamed and a changed redefinition, a second
/// definition, an epoch with nothing defined, and a renamed, changed
/// definition dropped an epoch earlier. The chain converges after each.
#[test]
fn every_epoch_step_runs_on_one_instance() {
    use generative::generate::epoch::{EpochChangeDraw, EpochChangeKind, EpochDraw};
    use generative::generate::{AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec};

    let change = |kind, target| EpochChangeDraw { kind, target };
    let table = || {
        TableSpec::numeric_only(
            vec![(Some(1), Some(2)), (Some(3), None), (None, Some(5))],
            vec![Mutate::Delete { pk: 2 }],
        )
    };
    let writes = |pk| {
        vec![
            vec![Mutate::Update {
                pk,
                c1: Some(10 + pk),
                c2: None,
            }],
            vec![
                Mutate::Update {
                    pk,
                    c1: Some(20 + pk),
                    c2: Some(1),
                },
                Mutate::DuplicateInsert {
                    pk: 2,
                    c1: Some(7),
                    c2: Some(8),
                },
            ],
        ]
    };
    let case = EpochCase {
        tables: vec![table(), table()],
        defs: vec![
            (0, DefShape::OneToOne, None),
            (
                1,
                DefShape::Aggregate {
                    functions: vec![AggregateFn::Sum(AggregateColumn::C1)],
                },
                None,
            ),
        ],
        epochs: vec![
            EpochDraw {
                changes: vec![
                    change(EpochChangeKind::Drop, 0),
                    change(EpochChangeKind::Restart, 0),
                ],
                mutates: writes(1),
            },
            EpochDraw {
                changes: vec![
                    change(EpochChangeKind::Restore, 0),
                    change(EpochChangeKind::Redefine, 2),
                ],
                mutates: writes(3),
            },
            EpochDraw {
                changes: vec![
                    change(EpochChangeKind::Redefine, 4),
                    change(EpochChangeKind::Second, 1),
                ],
                mutates: writes(2),
            },
            EpochDraw {
                changes: vec![
                    change(EpochChangeKind::Drop, 0),
                    change(EpochChangeKind::Drop, 0),
                    change(EpochChangeKind::Drop, 0),
                ],
                mutates: writes(1),
            },
            EpochDraw {
                changes: vec![change(EpochChangeKind::Restore, 9)],
                mutates: writes(3),
            },
        ],
    };
    let shapes = chain_shapes(&chain(&case));
    for name in [
        "drop",
        "restart",
        "define_same",
        "define_changed",
        "define_renamed",
        "define_renamed_changed",
        "define_second",
        "restore",
        "define_unread_table",
        "none_defined",
    ] {
        assert!(shapes.contains_key(name), "{name} not reached: {shapes:?}");
    }
    run_case(&case).unwrap_or_else(|e| panic!("{e}"));
}

/// Residue (#2187, sub-step 8b): the chain runs as the case's roles, with
/// the ring lapped and each Trellis table vacuumed and measured after every
/// epoch ([`generative::run::run_epochs_lapping`]).
mod residue {
    use super::*;
    use generative::backend::CaseDatabase;
    use generative::run::{LAP_SEAL_INTERVAL, ResidueReport, check_residue, run_epochs_lapping};

    /// The engine's housekeeping interval, where the ring's tail retires and
    /// the tombstones and zero groups are collected. At the engine's 5s
    /// default, settling at each epoch's quiet point would be most of the run.
    const HOUSEKEEPING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

    /// Runs `case`'s chain, lapping the ring, against a provisioned database.
    pub(super) fn run_lapping_case(case: &EpochCase) -> Result<ResidueReport, TestCaseError> {
        let chain = chain(case);
        HARNESS.with(|h| {
            h.cases.set(h.cases.get() + 1);
            h.runtime.block_on(async {
                let db = CaseDatabase::provision(&h.cluster).await;
                let mut backend = ManualBackend::connect_as_with_options(
                    db.roles(),
                    WORKERS,
                    Some(LAP_SEAL_INTERVAL),
                )
                .await
                .expect("connect backend");
                backend.set_strict_audit(true);
                backend.set_reconcile_interval(RECONCILE_INTERVAL);
                backend.set_housekeeping_interval(HOUSEKEEPING_INTERVAL);
                let config = Config::from_dsn(db.dsn().to_string()).expect("config");
                let pool = Pool::new(&config).expect("pool");
                match run_epochs_lapping(&mut backend, &pool, &config, db.roles(), &chain).await {
                    Ok((outcome, report)) if outcome.as_pass() => Ok(report),
                    Ok((outcome, _)) => {
                        Err(TestCaseError::fail(format!("run did not pass: {outcome}")))
                    }
                    Err(RunError::Epoch { epoch, error }) => Err(TestCaseError::fail(format!(
                        "epoch {epoch} of {} failed: {error:?}\nsteps: {:?}",
                        chain.epochs.len(),
                        chain.epochs[epoch].steps
                    ))),
                    Err(other) => Err(TestCaseError::fail(format!("run error: {other:?}"))),
                }
            })
        })
    }

    proptest! {
        #![proptest_config(proptest_config())]

        /// A chain on one instance laps the ring, and leaves no dead tuples
        /// and no growth its peak live rows don't explain, in any Trellis
        /// table after a `VACUUM`.
        #[test]
        #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
        fn property_epoch_chains_leave_no_residue(case in epoch_case()) {
            run_lapping_case(&case)?;
        }
    }

    /// Updates of one row through many epochs: the ring goes round more than
    /// ten times, and the tables stay inside the residue bounds throughout.
    #[test]
    fn a_chain_laps_the_ring_within_the_residue_bounds() {
        use generative::generate::epoch::{EpochChangeDraw, EpochChangeKind, EpochDraw};
        use generative::generate::{AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec};

        let updates = |from: i64| {
            (0..12)
                .map(|i| Mutate::Update {
                    pk: 1,
                    c1: Some(from + i),
                    c2: Some(i),
                })
                .collect::<Vec<_>>()
        };
        let case = EpochCase {
            tables: vec![TableSpec::numeric_only(
                vec![(Some(1), Some(2)), (Some(3), None), (None, Some(5))],
                Vec::new(),
            )],
            defs: vec![
                (0, DefShape::OneToOne, None),
                (
                    0,
                    DefShape::Aggregate {
                        functions: vec![AggregateFn::Sum(AggregateColumn::C1)],
                    },
                    None,
                ),
            ],
            epochs: (0..6)
                .map(|e| EpochDraw {
                    changes: if e % 3 == 2 {
                        vec![EpochChangeDraw {
                            kind: EpochChangeKind::Redefine,
                            target: e,
                        }]
                    } else {
                        Vec::new()
                    },
                    mutates: vec![updates(100 * e as i64)],
                })
                .collect(),
        };
        let report = run_lapping_case(&case).unwrap_or_else(|e| panic!("{e}"));
        assert!(
            report.laps() >= 10,
            "only {} laps: {report:?}",
            report.laps()
        );
        assert!(!report.last.is_empty(), "no table was checked: {report:?}");
        assert!(
            report.hidden.hidden.keys().any(|t| t.ends_with("__ledger"))
                && report
                    .hidden
                    .hidden
                    .keys()
                    .any(|t| t.starts_with("trellis.agg_")),
            "no hidden table was counted: {report:?}"
        );
        assert!(!report.hidden.captured.is_empty(), "no capture: {report:?}");
        assert!(
            report.last.iter().any(|t| t.table.ends_with("__ledger\"")),
            "no 1-1 ledger was checked: {report:?}"
        );
    }

    /// Opens a session as the operator, on `db`.
    async fn operator(db: &CaseDatabase) -> tokio_postgres::Client {
        let (client, connection) =
            tokio_postgres::connect(&db.roles().admin, tokio_postgres::NoTls)
                .await
                .expect("connect as the operator");
        tokio::spawn(connection);
        client
    }

    /// Plant: a transaction holding the vacuum horizon back leaves dead
    /// tuples `VACUUM` cannot reclaim. [`check_residue`] must name them, not
    /// the oracle.
    #[test]
    fn plant_dead_tuples_are_caught() {
        HARNESS.with(|h| {
            h.runtime.block_on(async {
                let db = CaseDatabase::provision(&h.cluster).await;
                let config = Config::from_dsn(db.dsn().to_string()).expect("config");
                let mut widths = Default::default();
                let peaks = Default::default();
                check_residue(&config, db.roles(), &[], &[], &peaks, &mut widths)
                    .await
                    .expect("a fresh instance has no residue");

                let holder = operator(&db).await;
                holder
                    .batch_execute(
                        "begin isolation level repeatable read; \
                         select count(*) from trellis.catalog_floor",
                    )
                    .await
                    .expect("hold a snapshot");
                let writer = operator(&db).await;
                for _ in 0..3 {
                    writer
                        .batch_execute("update trellis.catalog_floor set version = version")
                        .await
                        .expect("update");
                }
                let failed =
                    check_residue(&config, db.roles(), &[], &[], &peaks, &mut widths).await;
                match failed {
                    Err(RunError::Residue { failures }) => assert!(
                        failures
                            .iter()
                            .any(|f| f.contains("catalog_floor") && f.contains("n_dead_tup")),
                        "{failures:?}"
                    ),
                    other => panic!("dead tuples went unseen: {other:?}"),
                }
            })
        });
    }

    /// Plant: a table that grew far past its peak and kept its high-water
    /// mark is over the pages backstop.
    #[test]
    fn plant_growth_past_the_peak_is_caught() {
        HARNESS.with(|h| {
            h.runtime.block_on(async {
                let db = CaseDatabase::provision(&h.cluster).await;
                let config = Config::from_dsn(db.dsn().to_string()).expect("config");
                let operator = operator(&db).await;
                operator
                    .batch_execute(
                        "create table trellis.plant (id int primary key, pad text); \
                         insert into trellis.plant select i, repeat('x', 200) from generate_series(1, 20000) i; \
                         delete from trellis.plant where id < 19990",
                    )
                    .await
                    .expect("grow and empty a table");
                let mut widths = Default::default();
                let peaks = std::collections::BTreeMap::from([("t0".to_string(), 10)]);
                match check_residue(&config, db.roles(), &[], &[], &peaks, &mut widths).await {
                    Err(RunError::Residue { failures }) => assert!(
                        failures.iter().any(|f| f.contains("plant") && f.contains("pages")),
                        "{failures:?}"
                    ),
                    other => panic!("growth went unseen: {other:?}"),
                }
            })
        });
    }
    /// What the planted tests below keep of a chain that ran.
    struct Kept {
        db: CaseDatabase,
        backend: ManualBackend,
        config: Config,
        pool: Pool,
        program: generative::model::Program,
        defined: Vec<trellis::dev::defs::ast::TransformDef>,
    }

    impl Kept {
        /// [`check_hidden`] over the instance, given `settle` to settle in.
        async fn hidden(
            &self,
            settle: std::time::Duration,
        ) -> Result<generative::run::HiddenReport, RunError> {
            generative::run::check_hidden(
                &self.config,
                self.db.roles(),
                &self.pool,
                &self.program,
                &self.defined,
                &self.defined,
                settle,
            )
            .await
        }
    }

    /// Runs `case`'s chain, lapping the ring, and keeps the instance.
    async fn run_keeping(h: &Harness, case: &EpochCase) -> Kept {
        let chain = chain(case);
        let db = CaseDatabase::provision(&h.cluster).await;
        let mut backend =
            ManualBackend::connect_as_with_options(db.roles(), WORKERS, Some(LAP_SEAL_INTERVAL))
                .await
                .expect("connect backend");
        backend.set_strict_audit(true);
        backend.set_reconcile_interval(RECONCILE_INTERVAL);
        backend.set_housekeeping_interval(HOUSEKEEPING_INTERVAL);
        let config = Config::from_dsn(db.dsn().to_string()).expect("config");
        let pool = Pool::new(&config).expect("pool");
        run_epochs_lapping(&mut backend, &pool, &config, db.roles(), &chain)
            .await
            .unwrap_or_else(|e| panic!("the chain should leave no residue: {e:?}"));
        // The install drew the targets' schema; the checks name them there.
        let config = config
            .with_target_schema(backend.target_schema().to_string())
            .expect("the drawn target schema");
        let mut defined = chain.program.defs.clone();
        for epoch in &chain.epochs {
            for step in &epoch.steps {
                generative::generate::epoch::apply_step(&mut defined, step);
            }
        }
        Kept {
            db,
            backend,
            config,
            pool,
            program: chain.program.clone(),
            defined,
        }
    }

    /// Plants: each leaves a row, table or trigger behind that no current
    /// definition owns, as a drop that forgot it would. The oracle passes
    /// throughout, so only [`check_hidden`] can name them.
    #[test]
    fn plant_hidden_rows_left_by_a_drop_are_caught() {
        use generative::backend::Backend;
        use generative::generate::epoch::{EpochChangeDraw, EpochChangeKind, EpochDraw};
        use generative::generate::{AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec};
        use generative::run::check_defs;

        let case = EpochCase {
            tables: vec![TableSpec::numeric_only(
                vec![(Some(1), Some(2)), (Some(3), None), (None, Some(5))],
                vec![Mutate::Delete { pk: 2 }],
            )],
            defs: vec![
                (0, DefShape::OneToOne, None),
                (
                    0,
                    DefShape::Aggregate {
                        functions: vec![AggregateFn::Sum(AggregateColumn::C1)],
                    },
                    None,
                ),
            ],
            epochs: vec![
                EpochDraw {
                    changes: vec![EpochChangeDraw {
                        kind: EpochChangeKind::Redefine,
                        target: 0,
                    }],
                    mutates: vec![vec![Mutate::Update {
                        pk: 1,
                        c1: Some(8),
                        c2: Some(2),
                    }]],
                },
                EpochDraw {
                    changes: Vec::new(),
                    mutates: vec![vec![Mutate::Update {
                        pk: 3,
                        c1: Some(4),
                        c2: Some(6),
                    }]],
                },
            ],
        };
        HARNESS.with(|h| {
            h.runtime.block_on(async {
                let mut kept = run_keeping(h, &case).await;
                let operator = operator(&kept.db).await;
                let one_to_one = kept
                    .defined
                    .iter()
                    .find(|d| matches!(d.key_space, trellis::dev::defs::ast::KeySpace::OneToOne))
                    .expect("a 1-1 definition")
                    .target
                    .clone();
                let ts = kept.config.target_schema().to_string();
                kept.hidden(generative::run::SETTLE).await.expect("no residue before any plant");

                // Each plant, with what the failure must name, and its undo.
                let plants: Vec<(String, &str, String, &str)> = vec![
                    (
                        // A dropped key's entry, kept in a live ledger.
                        format!(
                            "insert into \"{ts}\".\"{one_to_one}__ledger\" (__from_key) values ('999')"
                        ),
                        "ledger",
                        format!("delete from \"{ts}\".\"{one_to_one}__ledger\" where __from_key = '999'"),
                        "hidden tables",
                    ),
                    (
                        // A dropped aggregate's group table.
                        "create table trellis.agg_9999 as select 1 as g".into(),
                        "agg_9999",
                        "drop table trellis.agg_9999".into(),
                        "hidden tables",
                    ),
                    (
                        // A dropped 1-1's ledger.
                        "create table public.gone__ledger (__from_key text primary key)".into(),
                        "gone__ledger",
                        "drop table public.gone__ledger".into(),
                        "hidden tables",
                    ),
                    (
                        // A dropped definition's catalog row.
                        "insert into trellis.schema_nodes (table_name, is_source, is_target) \
                         values ('public.gone', false, true)"
                            .into(),
                        "public.gone",
                        "delete from trellis.schema_nodes where table_name = 'public.gone'".into(),
                        "catalog",
                    ),
                    (
                        // A row a drained ring slot kept.
                        "insert into trellis.seg_1 (src_table, key, op) values ('public.t0', '1', 'recompute')"
                            .into(),
                        "seg_1",
                        "truncate trellis.seg_1".into(),
                        "ring",
                    ),
                    (
                        // A capture trigger on a table nothing reads.
                        "create table public.unread (id int primary key); \
                         do $$ declare f text; begin \
                           select p.oid::regproc::text into f from pg_proc p \
                           join pg_namespace n on n.oid = p.pronamespace \
                           where n.nspname = 'trellis' and p.proname like 'cap\\_insert\\_%' limit 1; \
                           execute format('create trigger trellis_capture_insert after insert on public.unread \
                                           for each row execute function %s()', f); end $$"
                            .into(),
                        "public.unread",
                        "drop table public.unread".into(),
                        "capture",
                    ),
                ];
                for (plant, names, undo, class) in &plants {
                    operator.batch_execute(plant).await.expect("plant");
                    match kept.hidden(std::time::Duration::ZERO).await {
                        Err(RunError::Residue { failures }) => assert!(
                            failures
                                .iter()
                                .any(|f| f.starts_with(class) && f.contains(names)),
                            "{plant}: {failures:?}"
                        ),
                        other => panic!("{plant} went unseen: {other:?}"),
                    }
                    // The oracle sees nothing wrong.
                    kept.backend.quiesce().await.expect("quiesce");
                    let snapshot = kept.backend.snapshot().await.expect("snapshot");
                    let diverged =
                        check_defs(&kept.pool, &kept.program, &kept.defined, &snapshot)
                            .await
                            .expect("oracle");
                    assert!(diverged.is_empty(), "{plant}: the oracle saw it");
                    operator.batch_execute(undo).await.expect("undo");
                    kept.hidden(generative::run::SETTLE)
                        .await
                        .expect("clean again after the undo");
                }
            })
        });
    }
    /// A chain whose second epoch drops the only definition, over a table
    /// that is then read by nothing.
    fn last_reader_dropped() -> EpochCase {
        use generative::generate::epoch::{EpochChangeDraw, EpochChangeKind, EpochDraw};
        use generative::generate::{DefShape, Mutate, TableSpec};
        EpochCase {
            tables: vec![TableSpec::numeric_only(
                vec![(Some(1), Some(2)), (Some(3), None)],
                vec![Mutate::Delete { pk: 2 }],
            )],
            defs: vec![(0, DefShape::OneToOne, None)],
            epochs: vec![EpochDraw {
                changes: vec![EpochChangeDraw {
                    kind: EpochChangeKind::Drop,
                    target: 0,
                }],
                mutates: vec![Vec::new()],
            }],
        }
    }

    /// Dropping the last definition over a table leaves the table's
    /// `schema_nodes` row behind (#2238). A version-fence row stays (#2239).
    #[test]
    fn a_dropped_last_reader_leaves_no_schema_node() {
        let report = run_lapping_case(&last_reader_dropped());
        assert!(report.is_ok(), "{report:?}");
    }
    /// The allowances for #2239 and #2240 take their own shapes and no
    /// nearby leak: a version-fence row for a table never defined, a
    /// non-zero stray group, and a zero group not stamped at the active
    /// segment are all still named.
    #[test]
    fn the_allowances_accept_only_their_shape() {
        use generative::generate::epoch::{EpochChangeDraw, EpochChangeKind, EpochDraw};
        use generative::generate::{AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec};

        let case = EpochCase {
            tables: vec![TableSpec::numeric_only(
                vec![(Some(1), Some(2)), (Some(3), None)],
                Vec::new(),
            )],
            defs: vec![
                (
                    0,
                    DefShape::Aggregate {
                        functions: vec![AggregateFn::Sum(AggregateColumn::C1)],
                    },
                    None,
                ),
                (0, DefShape::OneToOne, None),
            ],
            epochs: vec![EpochDraw {
                changes: vec![EpochChangeDraw {
                    kind: EpochChangeKind::Restart,
                    target: 0,
                }],
                mutates: vec![vec![Mutate::Update {
                    pk: 1,
                    c1: Some(8),
                    c2: Some(2),
                }]],
            }],
        };
        HARNESS.with(|h| {
            h.runtime.block_on(async {
                let kept = run_keeping(h, &case).await;
                let operator = operator(&kept.db).await;
                kept.hidden(generative::run::SETTLE)
                    .await
                    .expect("clean before any plant");
                let agg: String = operator
                    .query_one(
                        "select relname from pg_class where relnamespace = 'trellis'::regnamespace \
                         and relname ~ '^agg_[0-9]+$'",
                        &[],
                    )
                    .await
                    .expect("an aggregate's table")
                    .get(0);
                let group: String = operator
                    .query_one(
                        "select attname from pg_attribute where attrelid = ('trellis.' || $1)::regclass \
                         and attnum > 0 order by attnum limit 1",
                        &[&agg],
                    )
                    .await
                    .expect("its group column")
                    .get(0);
                let active: i64 = operator
                    .query_one("select active_seq from trellis.segment_pointer", &[])
                    .await
                    .expect("active")
                    .get(0);
                let insert = |n: i32, zero: bool, stamp: i64| {
                    format!(
                        "insert into trellis.{agg} ({group}, __n, __stamp) \
                         values ({n}, {}, {stamp})",
                        if zero { 0 } else { 3 }
                    )
                };
                // Allowed: #2240's shape.
                operator
                    .batch_execute(&insert(777, true, active))
                    .await
                    .expect("plant a zero group at the active segment");
                kept.hidden(std::time::Duration::ZERO)
                    .await
                    .expect("a zero group at the active segment is allowed (#2240)");
                operator
                    .batch_execute(&format!("delete from trellis.{agg} where {group} = 777"))
                    .await
                    .unwrap();
                // Allowed: the same shape in a 1-1 ledger (#2240), and its
                // nearby leaks.
                let ts = kept.config.target_schema().to_string();
                let ledger: String = operator
                    .query_one(
                        "select relname from pg_class where relname like '%__ledger' \
                         and relnamespace = $1::text::regnamespace",
                        &[&kept.config.target_schema()],
                    )
                    .await
                    .map(|r| r.get(0))
                    .unwrap_or_default();
                assert!(!ledger.is_empty(), "the case has a 1-1 ledger");
                {
                    let put = |key: &str, tomb: bool, seg: i64| {
                        format!(
                            "insert into \"{ts}\".\"{ledger}\" (__from_key, __tombstone, __applied_seg) \
                             values ('{key}', {tomb}, {seg})"
                        )
                    };
                    operator.batch_execute(&put("k1", true, active)).await.unwrap();
                    kept.hidden(std::time::Duration::ZERO)
                        .await
                        .expect("a tombstone at the active segment is allowed (#2240)");
                    for (plant, what) in [
                        (put("k2", true, active - 1), "a tombstone stamped elsewhere"),
                        (put("k3", false, active), "a live stray entry"),
                    ] {
                        operator.batch_execute(&plant).await.unwrap();
                        match kept.hidden(std::time::Duration::ZERO).await {
                            Err(RunError::Residue { .. }) => {}
                            other => panic!("{what} went unseen: {other:?}"),
                        }
                        operator
                            .batch_execute(&format!(
                                "delete from \"{ts}\".\"{ledger}\" where __from_key in ('k2', 'k3')"
                            ))
                            .await
                            .unwrap();
                    }
                    operator
                        .batch_execute(&format!(
                            "delete from \"{ts}\".\"{ledger}\" where __from_key = 'k1'"
                        ))
                        .await
                        .unwrap();
                }
                // Not allowed: the nearby leaks.
                for (plant, what) in [
                    (insert(778, true, active - 1), "a zero group stamped elsewhere"),
                    (insert(779, false, active), "a non-zero stray group"),
                    (
                        "insert into trellis.source_table_versions values ('public.never_defined', 1)"
                            .to_string(),
                        "a version row for a table never defined (#2239)",
                    ),
                ] {
                    operator.batch_execute(&plant).await.expect("plant");
                    match kept.hidden(std::time::Duration::ZERO).await {
                        Err(RunError::Residue { .. }) => {}
                        other => panic!("{what} went unseen: {other:?}"),
                    }
                    operator
                        .batch_execute(&format!(
                            "delete from trellis.{agg} where {group} in (778, 779); \
                             delete from trellis.source_table_versions \
                             where source_table = 'public.never_defined'"
                        ))
                        .await
                        .unwrap();
                }
                kept.hidden(generative::run::SETTLE).await.expect("clean again");
            })
        });
    }
}
