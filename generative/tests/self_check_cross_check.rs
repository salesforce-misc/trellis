//! ADR-0013's continuous independence check: "the generative
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
//! # The fixed scenario above, promoted to a swept property
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
//!
//! # `MIN`/`MAX`: tampering with the hidden extremes
//!
//! `property_self_check_catches_tampered_extremes` runs a
//! `generative::generate::min_max` program serially to its end (its
//! definitions admitted with the engine's test-only switch), checks it
//! against the oracle, and then tampers with one definition's hidden table
//! ([`ExtremeTamper`]): one hidden extreme set wrong alone or with its visible
//! field, a live group's hidden row deleted, or a zero group's hidden row
//! deleted (#1117's F6 shape, settled, which must report nothing). `self_check`
//! must report exactly what the tamper broke, each hidden extreme's recompute
//! equal by value to the oracle's. Its planted-bug sweep,
//! `extremes_tamper_planted_bugs_are_caught` (tier `extremes_tamper`), gates
//! `sc_extremes_uncompared`. The other two `self_check` extremes plants
//! misreport only while a change is undrained, which a settled target never
//! has (`EXTREMES_TAMPER_NOT_GATED`).

use std::collections::HashMap;
use std::time::Duration;

use generative::backend::{Backend, ManualBackend};
use generative::baseline_quarantine;
use generative::generate::min_max::{
    Arg, Grouping, MinMaxDefDraw, MinMaxDraws, PK, build_min_max_program, min_max_def,
};
use generative::generate::trivial_one_to_one_program_with;
use generative::model::{NamePool, Op, OpOutcome, Program, Table, group_key};
use generative::oracle::sql_oracle;
use generative::plant_sweep::{self, PlantPath, SweepTier, not_on_path, plant_path};
use generative::run::{check_program, run_convergence};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::dev::config::DEFAULT_SCHEMA;
use trellis::dev::defs::PgType;
use trellis::dev::defs::ast::{
    Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType,
};
use trellis::dev::defs::qualified_target_table;
use trellis::dev::pool::Pool;
use trellis::dev::staging::{SelfCheckWorkerOptions, work_self_check_once};
use trellis::{
    Config, Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckReport, Trellis, TrellisOptions,
};

/// The most pages [`self_check_to_end`] runs, and the longest it runs them,
/// before failing the case: a job here is one page.
const SELF_CHECK_MAX_PAGES: usize = 100;
const SELF_CHECK_LIMIT: Duration = Duration::from_secs(120);

/// `Trellis::self_check` run to its end. The call starts a background job and
/// returns. The caller stops the backend's engine first, and with it
/// the worker task that would otherwise race this one for the job, so this
/// runs every page of it by hand, the way `ManualBackend` drains, and reads
/// the report back through the facade. The page holds a whole generated target (the largest is
/// `MAX_TABLES * MAX_SEED_ROWS` rows), so a job is one page.
async fn self_check_to_end(trellis: &Trellis, pool: &Pool, target: &str) -> SelfCheckReport {
    let job = trellis
        .self_check(target, SelfCheckMode::Strict, Duration::from_secs(30))
        .await
        .expect("start self_check");
    let worker = SelfCheckWorkerOptions::new(Duration::from_secs(30));
    // A job here is one page, and a page is bounded by the worker's 30 s
    // budget, but a page that kept claiming work would loop forever. Fail
    // the case instead, naming the job's state (#1641).
    let started = std::time::Instant::now();
    let mut pages = 0;
    while work_self_check_once(pool, "generative", &worker)
        .await
        .expect("a self_check page")
    {
        pages += 1;
        if pages >= SELF_CHECK_MAX_PAGES || started.elapsed() >= SELF_CHECK_LIMIT {
            let state = trellis.self_check_job(job.id).await;
            let sessions = match pool.get().await {
                Ok(conn) => generative::backend::activity_summary_on(&conn).await,
                Err(err) => format!("(no connection to read pg_stat_activity: {err})"),
            };
            panic!(
                "self_check job {} still claimed work after {pages} page(s) in {:?} \
                 (limits: {SELF_CHECK_MAX_PAGES} pages, {SELF_CHECK_LIMIT:?}); job: {state:?}\n\
                 sessions:\n{}",
                job.id,
                started.elapsed(),
                sessions,
            );
        }
    }
    let job = trellis
        .self_check_job(job.id)
        .await
        .expect("poll the job")
        .expect("the job exists");
    job.report.unwrap_or_else(|| {
        panic!(
            "the job ended {:?} with no report: {:?}",
            job.state, job.error
        )
    })
}

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
        without_source_index: false,
        adopt_target: false,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    let program = Program {
        tables: vec![source.clone()],
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
    let sql = sql_oracle(&pool, &def, &pk_column)
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

    backend.stop_engine().await.expect("stop the engine");
    let report = self_check_to_end(&trellis, &pool, &target_name).await;

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
    let sql = sql_oracle(&pool, &def, &pk_column)
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

    backend.stop_engine().await.expect("stop the engine");
    let report = self_check_to_end(&trellis, &pool, &target_name).await;

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
// Layer-3 bucket 6: the swept, generated version of the fixed
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
        cluster: TestCluster::start().long_lived(),
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
        max_shrink_time: generative::shrink::max_shrink_time_ms(),
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
/// (`SelfCheckError::UnsupportedKeySpace`) and only renders a leaf
/// expression over the source's own columns (`SelfCheckError::UnsupportedExpr`
/// on a path into another table) — so drawing an `Aggregate` def would make
/// this property fail on
/// `self_check`'s own documented, deliberate scope boundary instead of on a
/// real tampering-detection question. `trivial_one_to_one_program_with`
/// already exists, restricted to exactly that same boundary, for the
/// unrelated client-restart property — reused here rather than
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

/// The [`ValueType`] of field `field_index` of `program.defs[def_index]`,
/// from the oracle's own inference over the source table's columns: a
/// derived field is a generated expression of any admitted type.
fn corrupted_field_value_type(def: &TransformDef, field_index: usize, source: &Table) -> ValueType {
    let source_columns: HashMap<String, ValueType> = source
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type))
        .collect();
    generative::oracle::field_types(def, &source_columns)[field_index].1
}

/// A `(SQL literal expression, its exact ::text rendering)` pair to write
/// into a `value_type`-typed cell whose current `::text` reading is
/// `current` — guaranteed to actually change the cell (never a no-op
/// corruption) by picking between two fixed literals per type and returning
/// whichever one's rendering doesn't already equal `current`.
///
/// **Design choice: corruption covers every [`ValueType`] the expression
/// generator can type a field as** ([`generative::expr_gen::universe`] and
/// every rule's output type; the
/// `every_type_the_generator_can_produce_has_a_tamper_value` test below
/// pins that, so a type added to the generator without a tamper value here
/// fails that test instead of panicking mid-property). Each type has two
/// fixed literals; the one whose `::text` rendering differs from the cell's
/// current reading is used, so the corruption is never a no-op. It is **not**
/// every possible corruption shape (e.g. corrupting the primary key column
/// itself, or a schema-level change). Bucket 6 is "detection-focused": the
/// property's job is proving `self_check` catches an arbitrary *value*
/// tamper on an arbitrary calculated field; corrupting the key column
/// would change *which row* is being audited rather than what value it
/// holds, a different (also legitimate) bucket-6 question left for a
/// follow-up.
fn corruption_literal(value_type: ValueType, current: &Option<String>) -> (String, String) {
    use generative::expr_gen::{Magnitude, typed_literal_samples};
    use trellis::dev::defs::typed_literal::TYPED_LITERALS;
    let (a_sql, a_text, b_sql, b_text) = match value_type {
        // One pair for every numeric type: 123 and -45 fit all of them.
        ValueType::Numeric | ValueType::Integer(_) | ValueType::Float(_) => (
            format!("123::{value_type}"),
            "123".to_string(),
            format!("-45::{value_type}"),
            "-45".to_string(),
        ),
        ValueType::Boolean => (
            "true::boolean".to_string(),
            "true".to_string(),
            "false::boolean".to_string(),
            "false".to_string(),
        ),
        ValueType::Text => (
            "'zzz_tampered_a'::text".to_string(),
            "zzz_tampered_a".to_string(),
            "'zzz_tampered_b'::text".to_string(),
            "zzz_tampered_b".to_string(),
        ),
        ValueType::Uuid => (
            "'00000000-0000-0000-0000-0000000000a1'::uuid".to_string(),
            "00000000-0000-0000-0000-0000000000a1".to_string(),
            "'00000000-0000-0000-0000-0000000000b2'::uuid".to_string(),
            "00000000-0000-0000-0000-0000000000b2".to_string(),
        ),
        // The one generated type with no typed literal (its canonical form is
        // not yet checkable, see `TYPED_LITERALS`), so spelled here. The
        // connection pins `TimeZone = 'UTC'`, so `::text` renders as
        // `YYYY-MM-DD HH:MM:SS[.f]+00`; the two instants are far from any
        // value the generator draws.
        ValueType::Other(PgType::TimestampTz) => (
            "'1999-12-31 23:59:59+00'::timestamptz".to_string(),
            "1999-12-31 23:59:59+00".to_string(),
            "'2001-02-03 04:05:06.5+00'::timestamptz".to_string(),
            "2001-02-03 04:05:06.5+00".to_string(),
        ),
        // A type only a typed literal produces: two of its canonical
        // spellings, whose text is the spelling itself.
        ValueType::Other(_) => {
            let spec = TYPED_LITERALS
                .iter()
                .find(|spec| spec.value_type == value_type)
                .unwrap_or_else(|| {
                    panic!(
                        "corruption_literal: no tamper value for {value_type}; add one to \
                         corruption_literal (or exclude the type from generation, saying why)"
                    )
                });
            let samples = typed_literal_samples(spec.keyword, Magnitude::Small);
            let sql_type = spec.keyword.to_lowercase();
            (
                format!("'{}'::{sql_type}", samples[0]),
                samples[0].to_string(),
                format!("'{}'::{sql_type}", samples[1]),
                samples[1].to_string(),
            )
        }
    };
    if current.as_deref() == Some(a_text.as_str()) {
        (b_sql, b_text)
    } else {
        (a_sql, a_text)
    }
}

/// Every type the expression generator can type a field as has a tamper
/// value, and that value differs from the cell's current reading whichever
/// of the two fixed literals that reading equals. A type the generator
/// gains without a tamper value here fails this test, rather than
/// panicking inside the (ignored, slow) tampering property.
#[test]
fn every_type_the_generator_can_produce_has_a_tamper_value() {
    use generative::expr_gen::{rules, universe};
    let mut types = universe();
    for rule in rules() {
        let out = rule.out();
        if !types.contains(&out) {
            types.push(out);
        }
    }
    assert!(
        types.contains(&ValueType::Other(PgType::TimestampTz)),
        "the generator no longer emits timestamptz; the regression this test pins is gone"
    );
    for ty in types {
        let (first_sql, first_text) = corruption_literal(ty, &None);
        assert!(!first_sql.is_empty(), "{ty}: empty tamper literal");
        // A cell already holding the first literal gets the second.
        let (second_sql, second_text) = corruption_literal(ty, &Some(first_text.clone()));
        assert_ne!(first_text, second_text, "{ty}: tamper is a no-op");
        assert_ne!(first_sql, second_sql, "{ty}: tamper literal repeats");
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
            let value_type = corrupted_field_value_type(
                def,
                field_selector % def.fields.len(),
                source,
            );

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
            let (corruption_sql, corruption_text) = (corruption_sql.as_str(), corruption_text.as_str());
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
            let sql = sql_oracle(&pool, def, &source.pk_col)
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
            backend.stop_engine().await.expect("stop the engine");
            let report = self_check_to_end(&trellis, &pool, &def.target).await;

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

    /// Layer-3 bucket 6: the swept, generated counterpart of
    /// `self_checks_recompute_matches_the_generative_sql_oracles_recompute_for_a_corrupted_row`
    /// above — see this file's module doc comment. An arbitrary
    /// `trivial_one_to_one_program_with`-drawn program is settled to
    /// convergence, an arbitrary persisted target cell is corrupted
    /// out-of-band, and `Trellis::self_check` must catch it as a
    /// `Divergence::Cell` whose `recomputed` value agrees with
    /// `generative::oracle::sql_oracle`'s own independent recompute — across
    /// the generated space, not just one fixed numeric-add case.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_self_check_catches_out_of_band_tampering_across_generated_programs(
        (program, cell_selector, field_selector) in program_with_corruption_selectors()
    ) {
        run_swept_tampering_case(&program, cell_selector, field_selector)?;
    }
}

// ---------------------------------------------------------------------
// MIN/MAX: tampering with a group-basis aggregate's hidden extremes.
// ---------------------------------------------------------------------

/// How [`run_extreme_tamper_case`] tampers with a `MIN`/`MAX` definition's
/// hidden table (`trellis::staging::group_basis`'s "Extremes"), after the
/// program has settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtremeTamper {
    /// One hidden extreme of a live group and its visible field, set alike
    /// to a wrong value: the visible row agrees with the hidden one, so only
    /// `self_check`'s comparison with the source (its side 7) shows it.
    Alike,
    /// One hidden extreme alone: the visible row disagrees with the hidden
    /// one, and the hidden one with the source.
    HiddenOnly,
    /// A live group's hidden row deleted: every partial and extreme of it is
    /// missing.
    LostRow,
    /// A zero group's hidden row deleted (#1117's F6 shape, settled): a zero
    /// group is the same as no row, so `self_check` must report nothing.
    /// Falls back to [`ExtremeTamper::Alike`] when no zero group is left.
    LostZeroRow,
}

/// A `MIN`/`MAX` program ([`generative::generate::min_max`]'s definitions and
/// op stream, run serially to its end) and what to tamper with once it has
/// settled. The selectors are resolved against the hidden table post-settle,
/// as [`program_with_corruption_selectors`]'s are.
#[derive(Debug, Clone)]
struct ExtremeTamperCase {
    draws: MinMaxDraws,
    tamper: ExtremeTamper,
    def: usize,
    group: usize,
    column: usize,
    /// Set the extreme to `NULL` rather than to a value no row holds, when
    /// it isn't `NULL` already.
    to_null: bool,
}

impl ExtremeTamperCase {
    fn program(&self) -> Program {
        build_min_max_program(&self.draws)
    }
}

fn extreme_tamper_case() -> impl Strategy<Value = ExtremeTamperCase> {
    (
        any::<u64>(),
        prop::collection::vec(min_max_def(), 1..=3),
        20..=60usize,
        100..=400usize,
        prop_oneof![
            3 => Just(ExtremeTamper::Alike),
            2 => Just(ExtremeTamper::HiddenOnly),
            2 => Just(ExtremeTamper::LostRow),
            1 => Just(ExtremeTamper::LostZeroRow),
        ],
        (any::<usize>(), any::<usize>(), any::<usize>()),
        prop::bool::weighted(0.25),
    )
        .prop_map(
            |(seed, defs, seed_rows, ops, tamper, (def, group, column), to_null)| {
                ExtremeTamperCase {
                    draws: serial_min_max_draws(seed, defs, seed_rows, ops),
                    tamper,
                    def,
                    group,
                    column,
                    to_null,
                }
            },
        )
}

/// The draws of a `MIN`/`MAX` program run serially: no `TRUNCATE` (the
/// floor of live rows then keeps a group to tamper with) and no deferred
/// install. The concurrency draws are the concurrent tier's and unused here.
fn serial_min_max_draws(
    seed: u64,
    defs: Vec<MinMaxDefDraw>,
    seed_rows: usize,
    ops: usize,
) -> MinMaxDraws {
    MinMaxDraws {
        seed,
        defs,
        deferred: None,
        seed_rows,
        ops,
        truncates: 0,
        burst_size: 1,
        lanes: 1,
        workers: 1,
        seal_interval_ms: 10,
        housekeeping_interval_ms: 100,
        actions: Vec::new(),
        pace_micros: 0,
        entry_stall_seals: None,
        commit_stall_ms: None,
    }
}

/// The SQL type of `arg`'s column.
fn arg_sql_type(arg: Arg) -> &'static str {
    match arg {
        Arg::V => "bigint",
        Arg::N => "numeric",
        Arg::F => "double precision",
        Arg::Ts => "timestamp with time zone",
        Arg::D => "date",
        Arg::Iv => "interval",
    }
}

/// A value of `arg` no drawn row holds: far past every domain.
fn arg_tamper_value(arg: Arg) -> &'static str {
    match arg {
        Arg::V => "999999",
        Arg::N => "12345.5",
        Arg::F => "1e+10",
        Arg::Ts => "2001-02-03 04:05:06+00",
        Arg::D => "1990-01-01",
        Arg::Iv => "1000 days",
    }
}

/// One hidden extreme column: its name, the argument it is an extreme of
/// (each argument of the table has a type of its own), and the visible field
/// that reads it.
#[derive(Debug, Clone)]
struct HiddenExtreme {
    column: String,
    arg: Arg,
    field: String,
}

/// `def`'s hidden table, qualified, and its bare name (how `self_check`
/// names its columns in a report: `<name>.<column>`).
async fn hidden_table(client: &tokio_postgres::Client, def: &TransformDef) -> (String, String) {
    let id: i64 = client
        .query_one(
            format!(
                "select id from {DEFAULT_SCHEMA}.transform_definitions where target_table = $1"
            )
            .as_str(),
            &[&format!("public.{}", def.target)],
        )
        .await
        .expect("read the definition's id")
        .get(0);
    let name = format!("agg_{id}");
    (format!("{DEFAULT_SCHEMA}.{name}"), name)
}

/// The hidden table's extreme columns, each matched to its argument by type
/// and to the visible field of the same direction over that argument.
async fn hidden_extremes(
    client: &tokio_postgres::Client,
    hidden: &str,
    def: &TransformDef,
) -> Vec<HiddenExtreme> {
    let rows = client
        .query(
            "select attname::text, format_type(atttypid, atttypmod) from pg_attribute \
             where attrelid = $1::text::regclass and attnum > 0 and not attisdropped \
               and (attname like '\\_\\_min\\_%' or attname like '\\_\\_max\\_%') \
             order by attnum",
            &[&hidden],
        )
        .await
        .expect("read the hidden table's columns");
    rows.iter()
        .map(|row| {
            let column: String = row.get(0);
            let ty: String = row.get(1);
            let arg = *Arg::ALL
                .iter()
                .find(|a| arg_sql_type(**a) == ty)
                .unwrap_or_else(|| panic!("{hidden}.{column} is {ty}, no argument's type"));
            let direction = &column[2..5];
            let field = format!("{direction}_{}", arg.column());
            assert!(
                def.fields.iter().any(|f| f.name == field),
                "{hidden}.{column} has no visible field {field} in {}",
                def.target
            );
            HiddenExtreme { column, arg, field }
        })
        .collect()
}

/// Whether `a` and `b` are one value of `ty` (`NULL` matching `NULL`), by
/// Postgres's `=`: an extreme's equal spellings (`1.0`, `1.00`; `26:00:00`,
/// `1 day 02:00:00`) are one value (ADR-0020).
async fn same_value(
    client: &tokio_postgres::Client,
    ty: &str,
    a: &Option<String>,
    b: &Option<String>,
) -> bool {
    client
        .query_one(
            format!("select $1::text::{ty} is not distinct from $2::text::{ty}").as_str(),
            &[a, b],
        )
        .await
        .expect("compare two values")
        .get(0)
}

/// Settles `case`'s program, checks the targets against the oracle, tampers
/// with one definition's hidden table as `case.tamper` says, and checks that
/// `Trellis::self_check` reports exactly what the tamper broke, each hidden
/// extreme's recompute agreeing by value with `generative::oracle`'s.
/// Returns the tamper it ran (see [`ExtremeTamper::LostZeroRow`]); `Err`
/// names what went wrong.
fn run_extreme_tamper_case(case: &ExtremeTamperCase) -> Result<ExtremeTamper, String> {
    let program = case.program();
    TAMPERING_HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            trellis::dev::staging::admit_min_max_for_tests();
            let mut backend = ManualBackend::connect(db.dsn())
                .await
                .expect("connect manual backend");
            let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
            let pool = Pool::new(&config).expect("pool");
            backend.install(&program).await.expect("install program");
            for op in &program.ops {
                backend.apply(op).await.expect("apply op");
            }
            backend.quiesce().await.expect("quiesce");
            let snapshot = backend.snapshot().await.expect("snapshot");
            let diverged = check_program(&pool, &program, &snapshot).await?;
            if !diverged.is_empty() {
                return Err(format!(
                    "the settled program diverged from the oracle before any tamper — a \
                     convergence bug, not a self_check finding: {diverged:#?}"
                ));
            }
            // Stopped before the tamper: the maintenance tick's GC collects
            // zero groups (Rx7), and could take the one a `LostZeroRow` reads
            // before its delete lands.
            let trellis = Trellis::connect(config, TrellisOptions::default())
                .await
                .expect("connect read-only trellis");
            backend.stop_engine().await.expect("stop the engine");

            let def = &program.defs[case.def % program.defs.len()];
            let KeySpace::Aggregate { group_by } = &def.key_space else {
                unreachable!("a min_max definition groups");
            };
            let client = pool.get().await.expect("pool connection");
            let (hidden, hidden_name) = hidden_table(&client, def).await;
            let extremes = hidden_extremes(&client, &hidden, def).await;
            let extreme = extremes[case.column % extremes.len()].clone();
            let group_text: Vec<String> =
                group_by.iter().map(|c| format!("\"{c}\"::text")).collect();
            let groups: Vec<(Vec<Option<String>>, i64)> = client
                .query(
                    format!(
                        "select array[{}]::text[], __n::bigint from {hidden} order by 1",
                        group_text.join(", ")
                    )
                    .as_str(),
                    &[],
                )
                .await
                .expect("read the hidden groups")
                .iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect();
            let live: Vec<&Vec<Option<String>>> = groups
                .iter()
                .filter(|(_, n)| *n > 0)
                .map(|(g, _)| g)
                .collect();
            let zero: Vec<&Vec<Option<String>>> = groups
                .iter()
                .filter(|(_, n)| *n == 0)
                .map(|(g, _)| g)
                .collect();
            let tamper = match case.tamper {
                ExtremeTamper::LostZeroRow if zero.is_empty() => ExtremeTamper::Alike,
                other => other,
            };
            let candidates = if tamper == ExtremeTamper::LostZeroRow {
                &zero
            } else {
                &live
            };
            if candidates.is_empty() {
                // The floor of live rows keeps a group; a serial program
                // never empties the table.
                return Err(format!("no live group in {} to tamper with", def.target));
            }
            let group = candidates[case.group % candidates.len()].clone();
            // The group, by its columns' text: `$1`… bind each, `NULL` too.
            let pred: Vec<String> = group_by
                .iter()
                .enumerate()
                .map(|(i, c)| format!("\"{c}\"::text is not distinct from ${}", i + 1))
                .collect();
            let pred = pred.join(" and ");
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = group
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            let visible = qualified_target_table("public", def);
            let ty = arg_sql_type(extreme.arg);
            let current: Option<String> = client
                .query_one(
                    format!("select {}::text from {hidden} where {pred}", extreme.column).as_str(),
                    &params,
                )
                .await
                .expect("read the hidden extreme")
                .get(0);
            let value: Option<String> = if case.to_null && current.is_some() {
                None
            } else {
                Some(arg_tamper_value(extreme.arg).to_string())
            };
            let literal = match &value {
                Some(v) => format!("'{v}'::{ty}"),
                None => "null".to_string(),
            };
            let set_hidden = format!(
                "update {hidden} set {} = {literal} where {pred}",
                extreme.column
            );
            let set_visible = format!(
                "update {visible} set \"{}\" = {literal} where {pred}",
                extreme.field
            );
            let delete_row = format!("delete from {hidden} where {pred}");
            let statements: Vec<&String> = match tamper {
                ExtremeTamper::Alike => vec![&set_hidden, &set_visible],
                ExtremeTamper::HiddenOnly => vec![&set_hidden],
                ExtremeTamper::LostRow | ExtremeTamper::LostZeroRow => vec![&delete_row],
            };
            for sql in statements {
                let n = client
                    .execute(sql.as_str(), &params)
                    .await
                    .unwrap_or_else(|e| panic!("tamper {sql}: {e}"));
                assert_eq!(n, 1, "{sql} wrote {n} rows");
            }

            // The oracle's own recompute of the group, the value
            // `self_check`'s side 7 must land on.
            let oracle = sql_oracle(&pool, def, PK).await.expect("sql oracle");
            let oracle_row = oracle.get(&group_key(&group));

            let report = self_check_to_end(&trellis, &pool, &def.target).await;
            let what = format!(
                "{tamper:?} of {} group {group:?} column {} ({}) to {value:?}",
                def.target, extreme.column, extreme.field
            );
            let divergences = match (report.outcome, tamper) {
                (SelfCheckOutcome::Converged, ExtremeTamper::LostZeroRow) => return Ok(tamper),
                (other, ExtremeTamper::LostZeroRow) => {
                    return Err(format!(
                        "{what}: a zero group's row is the same as none, but self_check \
                         reported {other:?}"
                    ));
                }
                (SelfCheckOutcome::Diverged(divergences), _) => divergences,
                (other, _) => {
                    return Err(format!(
                        "{what}: self_check reported {other:?}, not Diverged"
                    ));
                }
            };
            let key = match &divergences[0] {
                Divergence::Cell { key, .. }
                | Divergence::MissingRow { key }
                | Divergence::ExtraRow { key } => key.clone(),
                other => return Err(format!("{what}: unexpected divergence {other:?}")),
            };
            if divergences.iter().any(|d| {
                !matches!(d, Divergence::Cell { key: k, .. }
                    | Divergence::MissingRow { key: k }
                    | Divergence::ExtraRow { key: k } if *k == key)
            }) {
                return Err(format!(
                    "{what}: the divergences name more than the tampered group: {divergences:?}"
                ));
            }
            let cell = |column: &str| {
                divergences.iter().find_map(|d| match d {
                    Divergence::Cell {
                        column: c,
                        persisted,
                        recomputed,
                        ..
                    } if c == column => Some((persisted.clone(), recomputed.clone())),
                    _ => None,
                })
            };
            let expected_of = |field: &str| -> Option<String> {
                oracle_row.and_then(|row| row.get(field).cloned()).flatten()
            };
            // Each hidden extreme that must be reported: (column, persisted,
            // recomputed).
            let mut want: Vec<(String, Option<String>, Option<String>, &'static str)> = Vec::new();
            let mut cells = 0;
            match tamper {
                ExtremeTamper::Alike | ExtremeTamper::HiddenOnly => {
                    want.push((
                        format!("{hidden_name}.{}", extreme.column),
                        value.clone(),
                        expected_of(&extreme.field),
                        ty,
                    ));
                    cells += 1;
                    if tamper == ExtremeTamper::HiddenOnly {
                        // The visible row against the one the hidden partials
                        // and extremes imply.
                        want.push((
                            extreme.field.clone(),
                            expected_of(&extreme.field),
                            value.clone(),
                            ty,
                        ));
                        cells += 1;
                    }
                    if divergences.len() != cells {
                        return Err(format!(
                            "{what}: expected {cells} divergence(s), got {divergences:?}"
                        ));
                    }
                }
                ExtremeTamper::LostRow => {
                    for x in &extremes {
                        let expected = expected_of(&x.field);
                        if expected.is_some() {
                            want.push((
                                format!("{hidden_name}.{}", x.column),
                                None,
                                expected,
                                arg_sql_type(x.arg),
                            ));
                        }
                    }
                }
                ExtremeTamper::LostZeroRow => unreachable!("judged above"),
            }
            for (column, persisted, recomputed, ty) in want {
                let Some((p, r)) = cell(&column) else {
                    return Err(format!(
                        "{what}: no divergence on {column}, got {divergences:?}"
                    ));
                };
                if !same_value(&client, ty, &p, &persisted).await {
                    return Err(format!(
                        "{what}: {column} read as {p:?}, but {persisted:?} is what it holds"
                    ));
                }
                if !same_value(&client, ty, &r, &recomputed).await {
                    return Err(format!(
                        "{what}: self_check recomputed {column} as {r:?}, and \
                         generative::oracle as {recomputed:?} — the two renderers disagree"
                    ));
                }
            }
            Ok(tamper)
        })
    })
}

/// A hand-picked case of each tamper: a fast, readable pin of the property
/// below.
#[test]
fn self_check_catches_each_tamper_of_the_hidden_extremes() {
    let defs = vec![
        MinMaxDefDraw {
            grouping: Grouping::G1,
            extremes: vec![(false, Arg::Iv), (true, Arg::N), (true, Arg::Ts)],
            partials: true,
        },
        MinMaxDefDraw {
            grouping: Grouping::G1G2,
            extremes: vec![(true, Arg::F), (false, Arg::D), (false, Arg::V)],
            partials: false,
        },
    ];
    for (i, tamper) in [
        ExtremeTamper::Alike,
        ExtremeTamper::HiddenOnly,
        ExtremeTamper::LostRow,
        ExtremeTamper::LostZeroRow,
    ]
    .into_iter()
    .enumerate()
    {
        let case = ExtremeTamperCase {
            draws: serial_min_max_draws(11, defs.clone(), 30, 120),
            tamper,
            def: i,
            group: 2 * i + 1,
            column: i,
            to_null: i == 1,
        };
        match run_extreme_tamper_case(&case) {
            Ok(ran) => assert_eq!(ran, tamper, "the case has no zero group"),
            Err(message) => panic!("{message}"),
        }
    }
}

proptest! {
    #![proptest_config(tampering_proptest_config())]

    /// The `MIN`/`MAX` counterpart of the tampering property above: a
    /// `generate::min_max` program run to its end, one definition's hidden
    /// extremes tampered with ([`ExtremeTamper`]), and `self_check` must
    /// report exactly that, each hidden extreme's recompute agreeing by
    /// value with `generative::oracle`'s.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_self_check_catches_tampered_extremes(case in extreme_tamper_case()) {
        run_extreme_tamper_case(&case).map_err(TestCaseError::fail)?;
    }
}

/// The `self_check` extremes plants (`PlantPath::SelfCheckExtremes`) this
/// tier doesn't gate, each with why.
const EXTREMES_TAMPER_NOT_GATED: &[(&str, &str)] = &[
    (
        "sc_extremes_under_basis",
        "self_check reads a probed extreme wrong only while a change the probe saw is still \
         undrained, and this tier tampers with a settled target; \
         self_check_reads_the_extremes_under_their_own_basis pins it",
    ),
    (
        "sc_extreme_groups_uncompared",
        "a group only the source's extremes have is one whose partials a probe got ahead of, \
         which needs a change still undrained; in a settled target the source's partials and \
         extremes have the same groups, so no tamper reaches it; \
         self_check_compares_the_groups_only_the_extremes_have pins it",
    ),
];

/// The tampering tier's sweep: [`EXTREMES_TAMPER_NOT_GATED`], and every
/// plant of another path, which the tier whose programs take it gates.
fn extremes_tamper_sweep_tier() -> SweepTier {
    let mut not_gated: Vec<(&'static str, &'static str)> = EXTREMES_TAMPER_NOT_GATED.to_vec();
    not_gated.extend(not_on_path(
        &[
            PlantPath::OneToOne,
            PlantPath::GroupBasis,
            PlantPath::GroupBasisWriter,
            PlantPath::Join,
            PlantPath::OneToOneJoin,
            PlantPath::Chain,
            PlantPath::Extremes,
        ],
        "the tampering tier runs a settled program and gates only self_check's comparison of \
         the extremes; the plant's own path is gated by the tier its comment names",
    ));
    SweepTier {
        name: baseline_quarantine::EXTREMES_TAMPER_TIER,
        known_misses: &[],
        not_gated,
    }
}

/// The planted-bug sweep of [`run_extreme_tamper_case`]: the `self_check`
/// extremes plants make `self_check` miss a wrong extreme, which this tier
/// asks it to find. Returns at once without `GENERATIVE_PLANTS`. Run it with:
///
/// ```text
/// GENERATIVE_PLANTS=all cargo test -p generative \
///     --test self_check_cross_check extremes_tamper_planted_bugs_are_caught \
///     -- --ignored --nocapture
/// ```
#[test]
#[ignore = "planted-bug sweep: run with GENERATIVE_PLANTS=all and `--ignored`"]
fn extremes_tamper_planted_bugs_are_caught() {
    let strategy = extreme_tamper_case().boxed();
    plant_sweep::sweep(
        "extremes_tamper_planted_bugs_are_caught",
        &extremes_tamper_sweep_tier(),
        |seed, cases| plant_sweep::seeded_cases(&strategy, seed, cases),
        |case: &ExtremeTamperCase| {
            run_extreme_tamper_case(case)
                .map(drop)
                .map_err(|message| plant_sweep::CaseFailure {
                    key_spaces: case
                        .program()
                        .defs
                        .iter()
                        .map(|def| Some(def.key_space.clone()))
                        .collect(),
                    message,
                })
        },
    );
}

/// The tier gates exactly the `self_check` extremes plants it can reach.
#[test]
fn the_extremes_tamper_tier_runs_the_self_check_extremes_plants() {
    use trellis::dev::plant::Plant;
    let tier = extremes_tamper_sweep_tier();
    for plant in Plant::ALL {
        let name = plant.name();
        let gated = !tier.not_gated.iter().any(|(not, _)| *not == name);
        let reachable = plant_path(*plant) == PlantPath::SelfCheckExtremes
            && !EXTREMES_TAMPER_NOT_GATED
                .iter()
                .any(|(not, _)| *not == name);
        assert_eq!(gated, reachable, "{name}");
    }
}
