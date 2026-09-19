//! ADR-0013's continuous independence check (issue #174): "the generative
//! suite runs both renderers over the same definitions and asserts they
//! agree, on every run." `trellis::staging::self_check`'s leaf renderer
//! (`render_leaf`, `trellis/src/staging/self_check.rs`) and
//! `generative::oracle`'s own SQL-rendering oracle (`render_expr`,
//! `generative/src/oracle/mod.rs`) are independently authored — neither
//! imports the other, and neither shares code with
//! `trellis::defs::oracle`'s renderer either (see all three modules' own
//! doc comments on that). This test is what turns "independently authored"
//! into a load-bearing, continuously-checked guarantee rather than an
//! assertion nobody re-verifies.
//!
//! Reaches `self_check` only through the public `Trellis` facade — never by
//! naming `trellis::staging::self_check` directly — per ADR-0012 ("sibling
//! crates verify through the public API and through Postgres, not the
//! engine"); its independently-rendered leaf SQL is observed only
//! indirectly, through [`trellis::Divergence::Cell`]'s `recomputed` field.

use std::time::Duration;

use generative::backend::{Backend, ManualBackend};
use generative::model::{NamePool, Op, OpOutcome, Program, Table};
use generative::oracle::sql_oracle;
use testkit::TestCluster;
use trellis::dev::defs::ast::{
    Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType,
};
use trellis::dev::defs::qualified_target_table;
use trellis::{
    Config, Divergence, Pool, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, Trellis,
    TrellisOptions,
};

/// A 1-1 numeric-`+` program — reproduced from
/// `generative/tests/oracle.rs::numeric_add_program` rather than shared
/// across a test-binary boundary (that file's own `connect_raw` helper is
/// reproduced the same way elsewhere in this crate's tests).
fn numeric_add_program() -> (Program, TransformDef, Table, String) {
    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric, ValueType::Numeric]);
    let a = source.columns[1].name.clone();
    let b = source.columns[2].name.clone();
    let target_name = pool.next_table_name();

    let def = TransformDef {
        target: target_name.clone(),
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(a.clone())),
                rhs: Box::new(Expr::Column(b.clone())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    let program = Program {
        tables: vec![source.clone()],
        relationships: Vec::new(),
        defs: vec![def.clone()],
        def_install_after_op: vec![0],
        ops: vec![
            Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some("1".to_string())),
                    (a.clone(), Some("10.00".to_string())),
                    (b.clone(), Some("1.50".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            },
            Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some("2".to_string())),
                    (a.clone(), Some("20.00".to_string())),
                    (b.clone(), Some("2.00".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            },
        ],
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };

    (program, def, source, target_name)
}

async fn settle(backend: &mut ManualBackend, program: &Program) {
    backend.install(program).await.expect("install program");
    for op in &program.ops {
        backend.apply(op).await.expect("apply op");
    }
    backend.quiesce().await.expect("quiesce");
}

/// The steady-state half: with nothing corrupted, `generative::oracle`'s
/// independent recompute must equal the persisted target — and
/// `Trellis::self_check` (its own, differently-authored recompute) must
/// independently agree the target is correct too. Two renderers, run over
/// the same live data, reaching the same "this target is right" verdict.
#[tokio::test]
async fn self_check_and_the_generative_sql_oracle_agree_the_target_is_correct() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (program, def, source, target_name) = numeric_add_program();
    let pk_column = source.pk_col.clone();

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    settle(&mut backend, &program).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let pool = Pool::new(&config).expect("pool");

    // generative::oracle's own, independently-rendered recompute.
    let sql = sql_oracle(&pool, &program, &def, &pk_column)
        .await
        .expect("sql oracle");
    assert_eq!(sql["1"]["total"], Some("11.50".to_string()));
    assert_eq!(sql["2"]["total"], Some("22.00".to_string()));

    // A read-only Trellis handle (no staging/drain of its own — self_check
    // only ever reads) against the same database, purely to reach the
    // public self_check facade.
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect read-only trellis");

    let report = trellis
        .self_check(
            &target_name,
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Strict,
            Duration::from_secs(30),
        )
        .await
        .expect("self_check");

    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "self_check's own independent recompute must also agree the target is correct, got {:?}",
        report.outcome
    );
}

/// The load-bearing half: corrupt the persisted target directly (bypassing
/// the engine entirely, matching `generative/tests/oracle.rs`'s own
/// `an_injected_target_corruption_reads_as_target_not_sql`), then confirm
/// `Trellis::self_check`'s independently-rendered recompute
/// ([`Divergence::Cell::recomputed`]) is byte-identical to
/// `generative::oracle`'s own independently-rendered recompute for the same
/// key/column. Two renderers, written by different code, over the same
/// definition and the same live source data, computing the same answer —
/// exactly the guarantee ADR-0013 requires be checked on every run.
#[tokio::test]
async fn self_checks_recompute_matches_the_generative_sql_oracles_recompute_for_a_corrupted_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (program, def, source, target_name) = numeric_add_program();
    let pk_column = source.pk_col.clone();

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    settle(&mut backend, &program).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let pool = Pool::new(&config).expect("pool");

    // Corrupt row 1's persisted total directly — not via a source change, so
    // neither renderer's own recompute is affected, only the persisted
    // value being compared against.
    let qualified = qualified_target_table("public", &def);
    let client = pool.get().await.expect("pool connection");
    client
        .execute(
            &format!("update {qualified} set \"total\" = 999 where \"{pk_column}\" = 1"),
            &[],
        )
        .await
        .expect("corrupt target");
    drop(client);

    // generative::oracle's own independent recompute — the value ADR-0013
    // says self_check's *own* independent recompute must also land on.
    let sql = sql_oracle(&pool, &program, &def, &pk_column)
        .await
        .expect("sql oracle");
    let expected_recompute = sql["1"]["total"].clone();
    assert_eq!(
        expected_recompute,
        Some("11.50".to_string()),
        "sanity: the source data is untouched, so the SQL oracle's own recompute is still 11.50"
    );

    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect read-only trellis");

    let report = trellis
        .self_check(
            &target_name,
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Strict,
            Duration::from_secs(30),
        )
        .await
        .expect("self_check");

    let divergences = match report.outcome {
        SelfCheckOutcome::Diverged(divergences) => divergences,
        other => panic!("expected the corruption to be caught as Diverged, got {other:?}"),
    };

    let cell = divergences
        .iter()
        .find(|d| matches!(d, Divergence::Cell { key, column, .. } if key == "1" && column == "total"))
        .unwrap_or_else(|| panic!("expected a Cell divergence for (1, total), got {divergences:?}"));

    match cell {
        Divergence::Cell {
            persisted,
            recomputed,
            ..
        } => {
            assert_eq!(persisted, &Some("999".to_string()));
            assert_eq!(
                recomputed, &expected_recompute,
                "self_check's independently-rendered recompute must agree with \
                 generative::oracle's own independently-rendered recompute — a disagreement \
                 here means the two renderers have silently drifted apart"
            );
        }
        other => unreachable!("filtered to Divergence::Cell above, got {other:?}"),
    }
}
