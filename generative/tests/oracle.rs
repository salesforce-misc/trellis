//! Proves the Postgres-SQL oracle and the three-way comparison
//! (`generative::oracle`, issue #5): render a 1-1 numeric-`+` definition back
//! to a `SELECT`, run it against the same `testkit` cluster the program ran
//! in, and confirm the comparison localizes a divergence to the right layer
//! (`target ≠ SQL` vs `evaluator ≠ SQL`). Comparator/report unit tests live
//! beside the code in `src/oracle/mod.rs`; these are the ones that need a real
//! database.

use std::collections::HashMap;

use generative::backend::{Backend, ManualBackend};
use generative::model::{NamePool, Op, OpOutcome, Program, Table};
use generative::oracle::{self, evaluator_oracle, sql_oracle, three_way};
use testkit::TestCluster;
use trellis::dev::defs::ast::{
    Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType,
};
use trellis::dev::defs::qualified_target_table;
use trellis::{Config, Pool};

/// The same 1-1 numeric-`+` program the backend seam test uses, so the oracle
/// is exercised against a shape the backend already proves it can maintain.
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

    (program, def, source.clone(), target_name)
}

fn source_columns(table: &Table) -> HashMap<String, ValueType> {
    table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type))
        .collect()
}

async fn settle(backend: &mut ManualBackend, program: &Program) {
    backend.install(program).await.expect("install program");
    for op in &program.ops {
        backend.apply(op).await.expect("apply op");
    }
    backend.quiesce().await.expect("quiesce");
}

#[tokio::test]
async fn sql_oracle_matches_the_persisted_target_and_a_hand_computed_expected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (program, def, source, target_name) = numeric_add_program();
    let pk_column = source.pk_col.clone();
    let columns = source_columns(&source);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    settle(&mut backend, &program).await;
    let snapshot = backend.snapshot().await.expect("snapshot");
    let target = snapshot[&target_name].clone();

    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // The SQL oracle equals a hand-computed expected: 10.00+1.50, 20.00+2.00.
    let sql = sql_oracle(&pool, &program, &def, &pk_column)
        .await
        .expect("sql oracle");
    assert_eq!(sql["1"]["total"], Some("11.50".to_string()));
    assert_eq!(sql["2"]["total"], Some("22.00".to_string()));

    // And the full three-way check reports no divergence at all.
    let report = oracle::check(&pool, &program, &def, &pk_column, &columns, &target)
        .await
        .expect("three-way check");
    assert!(
        !report.diverged(),
        "healthy run must not diverge:\n{report}"
    );
}

#[tokio::test]
async fn an_injected_target_corruption_reads_as_target_not_sql() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (program, def, source, target_name) = numeric_add_program();
    let pk_column = source.pk_col.clone();
    let columns = source_columns(&source);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    settle(&mut backend, &program).await;

    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // Corrupt the persisted target directly — not via a source change, so the
    // maintenance pipeline won't correct it. Source is untouched, so the SQL
    // oracle and the evaluator both still compute the right value.
    let qualified = qualified_target_table("public", &def);
    let client = pool.get().await.expect("pool connection");
    client
        .execute(
            &format!("update {qualified} set \"total\" = 999 where \"{pk_column}\" = 1"),
            &[],
        )
        .await
        .expect("corrupt target");

    let snapshot = backend.snapshot().await.expect("snapshot");
    let target = snapshot[&target_name].clone();

    let report = oracle::check(&pool, &program, &def, &pk_column, &columns, &target)
        .await
        .expect("three-way check");

    assert!(report.diverged(), "corruption must be caught");
    assert_eq!(
        report.target_vs_sql.len(),
        1,
        "corruption localizes to target != SQL: {report}"
    );
    assert!(
        report.evaluator_vs_sql.is_empty(),
        "the evaluator agrees with the SQL oracle; only the persisted target is wrong: {report}"
    );
}

#[tokio::test]
async fn an_evaluator_disagreement_reads_as_evaluator_not_sql() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (program, def, source, target_name) = numeric_add_program();
    let pk_column = source.pk_col.clone();
    let columns = source_columns(&source);
    let a = source.columns[1].name.clone();

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    settle(&mut backend, &program).await;
    let snapshot = backend.snapshot().await.expect("snapshot");
    let target = oracle::target_fields(&snapshot[&target_name], &pk_column);

    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // The SQL oracle and the persisted target both reflect the real def
    // (c1 + c2). Simulate an eval-layer drift by running the evaluator over a
    // *different* expression (c1 + c1) — standing in for an evaluator that
    // computes the wrong thing while Postgres and the pipeline agree.
    let drifted = TransformDef {
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(a.clone())),
                rhs: Box::new(Expr::Column(a.clone())),
            },
        }],
        ..def.clone()
    };

    let sql = sql_oracle(&pool, &program, &def, &pk_column)
        .await
        .expect("sql oracle");
    let evaluator = evaluator_oracle(&pool, &drifted, &pk_column, &columns)
        .await
        .expect("evaluator oracle");

    let report = three_way(&program, &def, &columns, &target, &evaluator, &sql);

    assert!(report.diverged(), "drift must be caught");
    assert!(
        report.target_vs_sql.is_empty(),
        "the persisted target agrees with the SQL oracle; only the evaluator drifted: {report}"
    );
    assert_eq!(
        report.evaluator_vs_sql.len(),
        2,
        "both rows' evaluator values drift from the SQL oracle: {report}"
    );
}
