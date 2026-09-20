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
//!
//! # Issue #237: the fixed scenario above, promoted to a swept property
//!
//! `self_checks_recompute_matches_the_generative_sql_oracles_recompute_for_a_corrupted_row`
//! above is layer-3 bucket 6's (`docs/generative-test-suite.md` §8/§9,
//! `docs/decisions/0013-self-check-production-recompute-audit.md`)
//! detection-focused scenario, but as one hand-picked 1-1 numeric-add
//! program with one hand-picked corrupted cell. The
//! `property_self_check_catches_out_of_band_tampering_across_generated_programs`
//! property below (bottom of this file) generalizes it: an arbitrary
//! `trivial_one_to_one_program_with`-drawn program (arbitrary transform
//! definition(s), arbitrary source data, arbitrary post-seed mutates),
//! settled to convergence, with an arbitrary persisted target cell
//! corrupted out-of-band afterward — same load-bearing assertion (a
//! [`Divergence::Cell`] whose `recomputed` matches
//! [`generative::oracle::sql_oracle`]'s own independent recompute), now
//! swept across the generated space instead of pinned to one case. The
//! fixed scenario stays: it's a fast, single-round-trip, readable
//! golden-path regression that pins the exact numeric-add shape by name,
//! which a shrunk-but-still-generated failure from the property below is a
//! poor substitute for — see this file's own doc comment on
//! `self_check_and_the_generative_sql_oracles_agree_the_target_is_correct`-style
//! pins for why a hand-built pin is kept alongside a property that
//! subsumes its *coverage* rather than replacing it.

use std::time::Duration;

use generative::backend::{Backend, ManualBackend};
use generative::generate::trivial_one_to_one_program_with;
use generative::model::{NamePool, Op, OpOutcome, Program, Table};
use generative::oracle::sql_oracle;
use generative::run::run_convergence;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
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

// ---------------------------------------------------------------------
// Issue #237 (layer-3 bucket 6): the swept, generated version of the fixed
// corruption-detection scenario above.
// ---------------------------------------------------------------------

/// Mirrors `generative/tests/noise.rs`'s own `Harness`/`HARNESS`
/// thread-local: one shared `testkit` cluster and single-threaded `tokio`
/// runtime reused across every proptest case in this file's process, so a
/// 16-case sweep pays cluster start-up once rather than once per case.
struct TamperingHarness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
}

thread_local! {
    static TAMPERING_HARNESS: TamperingHarness = TamperingHarness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: TestCluster::start(),
    };
}

/// This crate's usual `PROPTEST_CASES`-overridable case count (see
/// `generative/tests/noise.rs`'s identical helper and its own doc comment —
/// this file duplicates rather than shares it, matching how every other
/// `generative/tests/*.rs` property file already keeps its own copy).
fn tampering_proptest_config() -> ProptestConfig {
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

/// Pairs a drawn 1-1-only [`Program`] with two unbounded selector draws used,
/// *after* the program has been settled against a real database, to pick
/// which persisted target cell to corrupt.
///
/// **Design choice: restricted to [`trivial_one_to_one_program_with`], not
/// the full [`generative::generate::trivial_program_with`] mix.**
/// `self_check` (`trellis/src/staging/self_check.rs`) only audits a
/// `KeySpace::OneToOne` target this issue
/// (`SelfCheckError::UnsupportedKeySpace`) and only renders a
/// relationship-free leaf expression (`SelfCheckError::UnsupportedExpr` on a
/// `RelationshipPath`) — so drawing an `Aggregate` def or a
/// relationship-reading field would make this property fail on
/// `self_check`'s own documented, deliberate scope boundary instead of on a
/// real tampering-detection question. `trivial_one_to_one_program_with`
/// already exists, restricted to exactly that same boundary, for the
/// unrelated E3 client-restart property — reused here rather than
/// duplicated.
///
/// **Design choice: the corruption target is selected post-settle, by index
/// modulo the real candidate count, not drawn against a range fixed up
/// front.** Which (definition, row) pairs actually have a *live* persisted
/// target row depends on the program's own generated mutates (a `Delete`
/// can empty out a def's target entirely) and isn't knowable until the
/// program has actually run against Postgres. Proptest strategies build
/// values with no database access, so the two `usize` draws here are
/// resolved against the real, post-settle candidate lists inside
/// [`run_swept_tampering_case`] — shrinking still works: proptest shrinks a
/// `usize` toward `0`, and index `0` of a non-empty candidate list is always
/// valid.
fn program_with_corruption_selectors() -> impl Strategy<Value = (Program, usize, usize)> {
    trivial_one_to_one_program_with(true)
        .prop_flat_map(|program| (Just(program), any::<usize>(), any::<usize>()))
}

/// The [`ValueType`] one of [`trivial_one_to_one_program_with`]'s `OneToOne`
/// field expressions produces — resolved independently of
/// `generative::oracle`'s own (private) `field_value_type`, over the small,
/// closed set of top-level field shapes that generator actually builds for a
/// `OneToOne` def (see `crate::generate::DerivedShape`'s doc comment, and
/// `build_program_multi_with_shapes`'s `OneToOne` arm): a bare source-column
/// passthrough (the `Text`/`Boolean`/`Uuid` columns, task B1), the fixed
/// `total = c1 + c2` field, or one more `BinaryOp`/`FunctionCall` "derived"
/// field. Every `FunctionCall` this generator ever attaches to a `OneToOne`
/// def (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/`REGEXP_COUNT`/`COALESCE`)
/// returns `Numeric` — `COUNT` is aggregate-only, and this program space
/// never draws `DefShape::Aggregate` — so no case here needs a registry
/// lookup the way the oracle's own general-purpose version does. A shape
/// outside this closed set (a `RelationshipPath`, or a bare literal at field
/// top level — neither of which `trivial_one_to_one_program_with` ever
/// builds) is a generator-scope bug for this property and panics loudly
/// rather than guessing (design doc §2 "refuse to guess").
fn corrupted_field_value_type(expr: &Expr, source: &Table) -> ValueType {
    match expr {
        Expr::Column(name) => source.column_type(name).unwrap_or_else(|| {
            panic!(
                "corrupted_field_value_type: field references column {name:?} absent from its \
                 own source table {:?} — a generator bug",
                source.name
            )
        }),
        Expr::BinaryOp {
            op: Operator::Add, ..
        } => ValueType::Numeric,
        Expr::BinaryOp {
            op: Operator::GreaterThan,
            ..
        } => ValueType::Boolean,
        Expr::FunctionCall { .. } => ValueType::Numeric,
        other => panic!(
            "corrupted_field_value_type: {other:?} is outside the closed set of top-level field \
             shapes trivial_one_to_one_program_with draws for a OneToOne def — extend this \
             helper if that generator ever widens"
        ),
    }
}

/// A `(SQL literal expression, its exact ::text rendering)` pair to write
/// into a `value_type`-typed cell whose current `::text` reading is
/// `current` — guaranteed to actually change the cell (never a no-op
/// corruption) by picking between two fixed literals per type and returning
/// whichever one's rendering doesn't already equal `current`.
///
/// **Design choice: corruption is scoped to `Numeric`/`Boolean`/`Text`/`Uuid`
/// fields** — the full set of [`ValueType`] variants
/// `trivial_one_to_one_program_with` ever attaches to a `OneToOne` field
/// (see [`corrupted_field_value_type`]) — **not every possible corruption
/// shape** (e.g. corrupting the primary key column itself, or a schema-level
/// change). Bucket 6 is "detection-focused": the property's job is proving
/// `self_check` catches an arbitrary *value* tamper on an arbitrary
/// calculated field, which this covers across all four field types this
/// generator produces; corrupting the key column would change *which row*
/// is being audited rather than what value it holds, a different (also
/// legitimate) bucket-6 question left for a follow-up.
fn corruption_literal(
    value_type: ValueType,
    current: &Option<String>,
) -> (&'static str, &'static str) {
    let (a_sql, a_text, b_sql, b_text) = match value_type {
        ValueType::Numeric => ("12345::numeric", "12345", "-98765::numeric", "-98765"),
        ValueType::Boolean => ("true::boolean", "true", "false::boolean", "false"),
        ValueType::Text => (
            "'zzz_tampered_a'::text",
            "zzz_tampered_a",
            "'zzz_tampered_b'::text",
            "zzz_tampered_b",
        ),
        ValueType::Uuid => (
            "'00000000-0000-0000-0000-0000000000a1'::uuid",
            "00000000-0000-0000-0000-0000000000a1",
            "'00000000-0000-0000-0000-0000000000b2'::uuid",
            "00000000-0000-0000-0000-0000000000b2",
        ),
        ValueType::Other(ref pg_type) => panic!(
            "corruption_literal: trivial_one_to_one_program_with never attaches an \
             Other({pg_type}) field to a OneToOne def — extend this helper if that changes"
        ),
        // #111/#112 split Integer/Float out of Numeric after this property was
        // written; trivial_one_to_one_program_with still only ever attaches
        // Numeric/Boolean/Text/Uuid to a OneToOne field (see this function's
        // and corrupted_field_value_type's doc comments), so these two stay
        // unreached in practice — extend this helper if that generator ever
        // widens to emit an exact-integer or floating-point column.
        ValueType::Integer(width) => panic!(
            "corruption_literal: trivial_one_to_one_program_with never attaches an \
             Integer({width:?}) field to a OneToOne def — extend this helper if that changes"
        ),
        ValueType::Float(width) => panic!(
            "corruption_literal: trivial_one_to_one_program_with never attaches a \
             Float({width:?}) field to a OneToOne def — extend this helper if that changes"
        ),
    };
    if current.as_deref() == Some(a_text) {
        (b_sql, b_text)
    } else {
        (a_sql, a_text)
    }
}

/// Settles `program` to convergence, corrupts one persisted target cell
/// out-of-band (direct SQL, bypassing the engine — matching the fixed
/// scenario above), and asserts `Trellis::self_check` catches it as a
/// [`Divergence::Cell`] whose `recomputed` value matches
/// [`generative::oracle::sql_oracle`]'s own independent recompute for the
/// same (target, key, column) — see this file's module doc comment and
/// [`program_with_corruption_selectors`]'s doc comment for the design
/// choices this makes.
fn run_swept_tampering_case(
    program: &Program,
    cell_selector: usize,
    field_selector: usize,
) -> Result<(), TestCaseError> {
    TAMPERING_HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect(db.dsn())
                .await
                .expect("connect manual backend");
            // Issue #188: unique per-case slot/publication names on a shared
            // cluster — see `generative/tests/noise.rs`'s `run_one` for the
            // identical reasoning.
            let unique = db.name().replace('-', "_");
            backend.set_slot_and_publication(format!("{unique}_slot"), format!("{unique}_pub"));
            let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
            let pool = Pool::new(&config).expect("pool");

            match run_convergence(&mut backend, &pool, program).await {
                Ok(outcome) if outcome.as_pass() => {}
                Ok(other) => {
                    return Err(TestCaseError::fail(format!(
                        "settling the generated program before corruption did not pass: {other}"
                    )));
                }
                Err(err) => {
                    return Err(TestCaseError::fail(format!(
                        "settling the generated program failed before any corruption was \
                         injected — a real convergence bug, not a self_check finding: {err:?}"
                    )));
                }
            }

            // Every (def_index, pk) pair with a live persisted target row
            // after the program's own mutates have run — see
            // `program_with_corruption_selectors`'s doc comment for why this
            // can only be known post-settle.
            let mut candidates: Vec<(usize, String)> = Vec::new();
            {
                let client = pool.get().await.expect("pool connection");
                for (def_index, def) in program.defs.iter().enumerate() {
                    let source = program
                        .tables
                        .iter()
                        .find(|t| t.name == def.source)
                        .expect("every def's source names a table in the same program");
                    let qualified = qualified_target_table("public", def);
                    let pk_ident = format!("\"{}\"", source.pk_col);
                    let rows = client
                        .query(
                            format!("select {pk_ident}::text from {qualified} order by {pk_ident}")
                                .as_str(),
                            &[],
                        )
                        .await
                        .expect("read target keys");
                    candidates.extend(rows.into_iter().map(|row| {
                        let pk: String = row.get(0);
                        (def_index, pk)
                    }));
                }
            }

            if candidates.is_empty() {
                return Err(TestCaseError::reject(
                    "generated program left no live target row on any definition to corrupt",
                ));
            }

            let (def_index, pk_text) = candidates[cell_selector % candidates.len()].clone();
            let def = &program.defs[def_index];
            let source = program
                .tables
                .iter()
                .find(|t| t.name == def.source)
                .expect("every def's source names a table in the same program");
            let field = &def.fields[field_selector % def.fields.len()];
            let value_type = corrupted_field_value_type(&field.expr, source);

            let qualified = qualified_target_table("public", def);
            let pk_ident = format!("\"{}\"", source.pk_col);
            let field_ident = format!("\"{}\"", field.name);

            let client = pool.get().await.expect("pool connection");
            let current: Option<String> = client
                .query_one(
                    format!(
                        "select {field_ident}::text from {qualified} where {pk_ident}::text = $1"
                    )
                    .as_str(),
                    &[&pk_text],
                )
                .await
                .expect("read the cell about to be corrupted")
                .get(0);

            let (corruption_sql, corruption_text) = corruption_literal(value_type, &current);
            client
                .execute(
                    format!(
                        "update {qualified} set {field_ident} = {corruption_sql} where \
                         {pk_ident}::text = $1"
                    )
                    .as_str(),
                    &[&pk_text],
                )
                .await
                .expect("corrupt target cell out-of-band");
            drop(client);

            // generative::oracle's own independent recompute — the value
            // ADR-0013 says self_check's own independent recompute must also
            // land on. The source data is untouched, so this is exactly the
            // pre-corruption correct answer.
            let sql = sql_oracle(&pool, program, def, &source.pk_col)
                .await
                .expect("sql oracle");
            let expected_recompute: Option<String> = sql
                .get(&pk_text)
                .unwrap_or_else(|| {
                    panic!(
                        "sql_oracle produced no row for pk {pk_text:?} on target {:?}, but a \
                         live persisted row was just read there",
                        def.target
                    )
                })
                .get(&field.name)
                .cloned()
                .unwrap_or_else(|| {
                    panic!(
                        "sql_oracle's row for pk {pk_text:?} on target {:?} has no column {:?}",
                        def.target, field.name
                    )
                });

            let trellis = Trellis::connect(config, TrellisOptions::default())
                .await
                .expect("connect read-only trellis");
            let report = trellis
                .self_check(
                    &def.target,
                    SelfCheckScope {
                        after: None,
                        // Comfortably above trivial_one_to_one_program_with's
                        // largest possible target (MAX_TABLES * MAX_SEED_ROWS
                        // rows) — one page always covers the whole target.
                        limit: 1000,
                    },
                    SelfCheckMode::Strict,
                    Duration::from_secs(30),
                )
                .await
                .expect("self_check");

            let divergences = match report.outcome {
                SelfCheckOutcome::Diverged(divergences) => divergences,
                other => {
                    return Err(TestCaseError::fail(format!(
                        "corrupted target {:?} key {pk_text:?} column {:?} but self_check \
                         reported {other:?} instead of Diverged — a real detection-side bug \
                         (self_check failed to catch a genuine out-of-band tamper), not a \
                         test-authoring problem",
                        def.target, field.name
                    )));
                }
            };

            let cell = divergences.iter().find(|d| {
                matches!(d, Divergence::Cell { key, column, .. } if key == &pk_text && column == &field.name)
            });
            let cell = match cell {
                Some(cell) => cell,
                None => {
                    return Err(TestCaseError::fail(format!(
                        "expected a Cell divergence for target {:?} key {pk_text:?} column {:?}, \
                         got {divergences:?}",
                        def.target, field.name
                    )));
                }
            };

            match cell {
                Divergence::Cell {
                    persisted,
                    recomputed,
                    ..
                } => {
                    if persisted.as_deref() != Some(corruption_text) {
                        return Err(TestCaseError::fail(format!(
                            "self_check's persisted reading {persisted:?} does not match the \
                             corruption actually written ({corruption_text:?})"
                        )));
                    }
                    if recomputed != &expected_recompute {
                        return Err(TestCaseError::fail(format!(
                            "self_check's independently-rendered recompute ({recomputed:?}) \
                             disagrees with generative::oracle's own independently-rendered \
                             recompute ({expected_recompute:?}) for the same (target, key, \
                             column) — the two renderers have silently drifted apart"
                        )));
                    }
                }
                other => unreachable!("filtered to Divergence::Cell above, got {other:?}"),
            }

            Ok(())
        })
    })
}

proptest! {
    #![proptest_config(tampering_proptest_config())]

    /// Issue #237, layer-3 bucket 6: the swept, generated counterpart of
    /// `self_checks_recompute_matches_the_generative_sql_oracles_recompute_for_a_corrupted_row`
    /// above — see this file's module doc comment. An arbitrary
    /// `trivial_one_to_one_program_with`-drawn program is settled to
    /// convergence, an arbitrary persisted target cell is corrupted
    /// out-of-band, and `Trellis::self_check` must catch it as a
    /// `Divergence::Cell` whose `recomputed` value agrees with
    /// `generative::oracle::sql_oracle`'s own independent recompute — across
    /// the generated space, not just one fixed numeric-add case.
    #[test]
    fn property_self_check_catches_out_of_band_tampering_across_generated_programs(
        (program, cell_selector, field_selector) in program_with_corruption_selectors()
    ) {
        run_swept_tampering_case(&program, cell_selector, field_selector)?;
    }
}
