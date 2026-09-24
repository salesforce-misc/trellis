//! End-to-end tests for issue #109's typed-literal grammar, against a real,
//! ephemeral Postgres via `testkit::TestCluster`.
//!
//! The point of this issue is that a calculated field can *produce* a value
//! of a type it has no column of — `docs/type-support.md`'s **computed 1-1
//! target** role, as distinct from passthrough. Proving that needs the whole
//! pipeline, not a parser unit test, because the claim spans five layers that
//! each have to agree:
//!
//! 1. **parse** — `DATE '2024-01-01'` / `CAST('2024-01-01' AS date)` both
//!    reach the same `Expr::TypedLiteral`.
//! 2. **validate** — the literal is in canonical form, and the field's
//!    inferred `ValueType` is `Other(Date)`, not `Text`.
//! 3. **DDL** — the target column is declared `date`, not `text`.
//! 4. **apply / backfill** — the value survives the `$n::text::date` write.
//! 5. **both renderers** — the Rust evaluator's text and the SQL oracle's
//!    `(expr)::text` are byte-identical, which is ADR-0013's continuous
//!    cross-check and the reason `super::typed_literal` demands canonical
//!    literals in the first place.
//!
//! Layer 5 is the one a parser test could never catch and the one the
//! generative suite would otherwise catch only by luck, so it gets its own
//! direct assertion here (`evaluator_and_sql_oracle_agree_on_every_literal`)
//! rather than being left to a fuzz run.
//!
//! Harness conventions (hand-built source table, `create_definition` +
//! `create_target_table`, CDC rows staged straight into the ring, seal, then
//! `drain_once`) are copied from `apply.rs`; the backfill leg's
//! chunk-draining helper is copied from `defs_install_definition.rs`.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::Pool;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ast::{Expr, ValueType};
use trellis::defs::{
    PgType, chunk_queue, create_definition, create_target_table, install_definition, parse,
    recompute, render_expr_sql, source_primary_key, validate,
};
use trellis::staging::StagedWatermark;
use trellis::staging::apply;

// ---------------------------------------------------------------------
// The families that exercise this shared mechanism, each with a canonical
// literal and the Postgres text it must round-trip to. `date` and
// `timestamp` are the temporal cases (#113) issue #109 itself proved the
// mechanism against, `bytea` the binary one (#114), `bit varying` the
// bit-string one (#118), and `jsonb` (#115) the first case whose canonical
// form needs a real recursive checker rather than a fixed-shape one;
// fixed-length `bit` is deliberately absent — see
// `defs::typed_literal::TYPED_LITERALS`.
// ---------------------------------------------------------------------

/// `(field name, type keyword, literal text, expected pg column type)`.
const CASES: &[(&str, &str, &str, &str)] = &[
    ("d", "DATE", "2024-01-01", "date"),
    (
        "ts",
        "TIMESTAMP",
        "2024-03-05 12:34:56.5",
        "timestamp without time zone",
    ),
    ("b", "BYTEA", "\\x0102ff", "bytea"),
    // Issue #118: `bit varying` joins the allowlist; fixed-length `bit` does
    // not (see `defs::typed_literal::TYPED_LITERALS`'s doc comment for why —
    // its bare default typmod is `bit(1)`, a truncation trap `bit varying`'s
    // unconstrained default doesn't have).
    ("v", "VARBIT", "101", "bit varying"),
    // Issue #115: exercises a nested object/array, sorted keys and a
    // decimal-scale-preserving number all in one literal — see
    // `crate::jsonb::canonical_jsonb`.
    (
        "j",
        "JSONB",
        "{\"a\": 1.50, \"c\": [1, 2], \"bb\": null}",
        "jsonb",
    ),
];

/// `SELECT id AS id, DATE '...' AS d, TIMESTAMP '...' AS ts, BYTEA '...' AS b`,
/// using the `<type> '<text>'` spelling.
fn select_list_typed_literal_spelling() -> String {
    let mut parts = Vec::new();
    for (name, keyword, text, _) in CASES {
        parts.push(format!("{keyword} '{text}' AS {name}"));
    }
    parts.join(", ")
}

/// The same fields written with standard SQL's `CAST('<text>' AS <type>)`.
fn select_list_cast_spelling() -> String {
    let mut parts = Vec::new();
    for (name, keyword, text, _) in CASES {
        parts.push(format!(
            "CAST('{text}' AS {}) AS {name}",
            keyword.to_lowercase()
        ));
    }
    parts.join(", ")
}

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([("id".to_string(), ValueType::Numeric)])
}

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; \
             set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"
        ))
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn insert_cdc_row(client: &Client, key: &str, new_image: &str) {
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            "insert into seg_0 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
             values ($1, $2, 'insert', $3, null, $4::text::jsonb, 0)",
            &[
                &format!("{DEFAULT_SCHEMA}.s"),
                &key,
                &lsn,
                &new_image.to_string(),
            ],
        )
        .await
        .expect("stage cdc row");
}

/// Drives the durable backfill chunk queue to completion, standing in for a
/// running `application_threads` drain worker (copied from
/// `defs_install_definition.rs`).
async fn drain_backfill_chunks(pool: &trellis::Pool) {
    // ADR-0016 (#418): registration only records a definition; the backfill
    // discharge dispatches its chunks.
    trellis::intake::publication::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    const CLAIMED_BY: &str = "typed_literal_test_backfill_worker";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(pool, chunk, CLAIMED_BY, Duration::from_secs(5))
                .await
                .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

/// The declared Postgres type of `table`.`column`, straight from
/// `information_schema`.
async fn column_pg_type(client: &Client, table: &str, column: &str) -> String {
    client
        .query_one(
            "select data_type from information_schema.columns \
             where table_name = $1 and column_name = $2",
            &[&table, &column],
        )
        .await
        .unwrap_or_else(|e| panic!("introspect {table}.{column}: {e}"))
        .get(0)
}

// ---------------------------------------------------------------------
// Parse + validate
// ---------------------------------------------------------------------

/// Both accepted spellings must produce the *same* AST, not merely
/// equivalent ones — that identity is what lets one evaluator arm and one
/// renderer arm serve both, and it mirrors Postgres, which folds
/// `CAST('2024-01-01' AS date)` and `DATE '2024-01-01'` to the same
/// `'2024-01-01'::date` constant.
#[test]
fn the_typed_literal_and_cast_spellings_parse_to_one_ast() {
    let typed = parse(&format!(
        "TRANSFORM t FROM s SELECT {}",
        select_list_typed_literal_spelling()
    ))
    .expect("typed-literal spelling parses");
    let cast = parse(&format!(
        "TRANSFORM t FROM s SELECT {}",
        select_list_cast_spelling()
    ))
    .expect("CAST spelling parses");

    assert_eq!(typed, cast);
    for ((field, (_, _, text, _)), expected_type) in typed.fields.iter().zip(CASES).zip(
        [
            PgType::Date,
            PgType::Timestamp,
            PgType::Bytea,
            PgType::VarBit,
            PgType::Jsonb,
        ]
        .map(ValueType::Other),
    ) {
        assert_eq!(
            field.expr,
            Expr::TypedLiteral {
                value_type: expected_type,
                text: text.to_string(),
            }
        );
    }
}

/// A typed literal's inferred type is its own family, not `Text` — the whole
/// point of the issue. Checked through `validate`, the gate every install
/// path goes through.
#[test]
fn a_typed_literal_field_validates_and_is_not_text() {
    let def = parse(&format!(
        "TRANSFORM t FROM s SELECT {}",
        select_list_typed_literal_spelling()
    ))
    .expect("parses");
    validate(&def, &source_columns(), &HashMap::new()).expect("validates");
}

/// The type keyword is case-insensitive like every other keyword in this
/// grammar, and a column that happens to be *named* like a type keyword is
/// still a column — the lookahead only fires on a following string literal.
#[test]
fn type_keywords_are_case_insensitive_and_do_not_shadow_a_column_named_date() {
    let def = parse("TRANSFORM t FROM s SELECT date AS d0, date '2024-01-01' AS d1")
        .expect("both a `date` column and a `date '...'` literal parse");
    assert_eq!(def.fields[0].expr, Expr::Column("date".to_string()));
    assert_eq!(
        def.fields[1].expr,
        Expr::TypedLiteral {
            value_type: ValueType::Other(PgType::Date),
            text: "2024-01-01".to_string(),
        }
    );
}

/// Non-canonical and non-immutable literals are rejected at *definition*
/// time, with a message that names the field. `'today'` is the immutability
/// case (it is why `date_in` is `STABLE`); `'2024-1-5'` is the round-trip
/// case (Postgres parses it, then renders it back differently).
#[test]
fn non_canonical_literals_are_rejected_by_the_validator() {
    for bad in [
        "DATE 'today'",
        "DATE '2024-1-5'",
        "DATE '2024-02-30'",
        "TIMESTAMP 'now'",
        "TIMESTAMP '2024-01-01'",
        "TIMESTAMP '2024-01-01 12:00:00.10'",
        "BYTEA '\\xAB'",
        "BYTEA '\\x0'",
        "CAST('today' AS date)",
        // Issue #115: exponent notation (`jsonb_out` never emits one) and
        // missing canonical whitespace (`jsonb_out` always separates with
        // `": "`, never a bare `":"`) are both non-canonical.
        "JSONB '1e2'",
        "JSONB '{\"a\":1}'",
    ] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {bad} AS x"))
            .unwrap_or_else(|e| panic!("{bad} should parse (it's a validate-time error): {e}"));
        let err = validate(&def, &source_columns(), &HashMap::new())
            .expect_err(&format!("{bad} must be rejected"));
        assert!(
            err.to_string().contains('x'),
            "{bad}: error must name the field: {err}"
        );
    }
}

/// A type outside the allowlist is rejected by name, so the message can say
/// *which* families are spellable rather than reporting a generic parse
/// failure. `timestamptz` is the interesting one: a real Postgres type the
/// OID registry already knows, held back for the reasons
/// `defs::typed_literal::TYPED_LITERALS` documents. `jsonb` used to be a
/// case here too, before issue #115 gave it a canonical-form checker
/// (`crate::jsonb::canonical_jsonb`) and moved it into the allowlist — its
/// coverage now lives in `CASES` above (every `CASES`-driven test in this
/// file) and in `non_canonical_literals_are_rejected_by_the_validator`'s
/// `JSONB` cases.
#[test]
fn types_outside_the_allowlist_are_rejected_at_parse_time() {
    for bad in [
        "CAST('2024-01-01 00:00:00+00' AS timestamptz)",
        "CAST('1 day' AS interval)",
        "CAST('1' AS money)",
        "CAST('x' AS frobnicate)",
    ] {
        let err = parse(&format!("TRANSFORM t FROM s SELECT {bad} AS x"))
            .expect_err(&format!("{bad} must be rejected"));
        assert!(
            err.to_string()
                .contains("is not a type a literal can be spelled as"),
            "{bad} must get the purpose-built allowlist error, got: {err}"
        );
    }
}

/// A **general** cast is recognized and refused by name, pointing at the
/// per-family issues that own the coercion lattice — the same
/// recognize-and-explain treatment this grammar already gives `JOIN` and
/// `COUNT(<column>)`.
#[test]
fn a_general_cast_is_rejected_with_a_pointed_error() {
    for bad in ["CAST(id AS date)", "CAST(id + 1 AS date)"] {
        let err = parse(&format!("TRANSFORM t FROM s SELECT {bad} AS x"))
            .expect_err(&format!("{bad} must be rejected"));
        let message = err.to_string();
        assert!(
            message.contains("CAST is only supported over a single-quoted literal"),
            "{bad}: {message}"
        );
    }
}

/// `::` is refused at the character the user typed, with a message naming
/// the two spellings that do work.
#[test]
fn the_cast_operator_is_rejected_with_a_pointed_error() {
    let err = parse("TRANSFORM t FROM s SELECT '2024-01-01'::date AS x")
        .expect_err("`::` must be rejected");
    let message = err.to_string();
    assert!(
        message.contains("'::' cast operator is not supported"),
        "{message}"
    );
    assert!(message.contains("CAST('<literal>' AS <type>)"), "{message}");
}

// ---------------------------------------------------------------------
// The renderer cross-check (ADR-0013)
// ---------------------------------------------------------------------

/// The load-bearing test. `render_expr_sql` is the SQL oracle's rendering of
/// a typed literal; the evaluator's is the literal text carried verbatim in
/// `Value::Other`. The generative suite compares those two as **byte-exact
/// strings** for a `ValueType::Other` field (`Comparison::Exact`), so any
/// literal whose Postgres rendering differs from its source text is a
/// guaranteed false divergence. This asserts directly that it cannot happen
/// for anything the grammar accepts.
#[tokio::test]
async fn evaluator_and_sql_oracle_agree_on_every_literal() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for (_, keyword, text, _) in CASES {
        let def = parse(&format!(
            "TRANSFORM t FROM s SELECT {keyword} '{text}' AS x"
        ))
        .expect("parses");
        validate(&def, &source_columns(), &HashMap::new()).expect("validates");

        let sql = render_expr_sql(&def.fields[0].expr);
        let rendered: String = client
            .query_one(&format!("select ({sql})::text"), &[])
            .await
            .unwrap_or_else(|e| panic!("postgres rejected the rendered literal {sql}: {e}"))
            .get(0);

        assert_eq!(
            &rendered, text,
            "postgres renders {keyword} '{text}' back as {rendered:?}; the evaluator carries the \
             source text verbatim, so the two renderers would disagree"
        );
    }
}

// ---------------------------------------------------------------------
// The full pipeline
// ---------------------------------------------------------------------

/// parse -> install -> **backfill/direct build** -> read. `install_definition`
/// is the real front door, so this covers the SQL the direct build actually
/// emits for a typed literal (`defs::backfill`'s renderer), plus the DDL that
/// decides the target column's type.
#[tokio::test]
async fn install_and_backfill_produce_real_typed_columns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key); \
             insert into s (id) select g from generate_series(1, 50) g",
        )
        .await
        .expect("seed source");

    install_definition(
        &db.pool,
        &format!(
            "TRANSFORM t FROM s SELECT {}",
            select_list_typed_literal_spelling()
        ),
        &source_columns(),
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool).await;

    // The columns are genuinely their own Postgres types — this is the
    // "computed 1-1 target" claim. Before #109 the only way to get a `date`
    // column here was to pass one through from the source.
    for (name, _, _, expected_pg_type) in CASES {
        assert_eq!(
            &column_pg_type(&client, "t", name).await,
            expected_pg_type,
            "target column {name} must be declared {expected_pg_type}"
        );
    }

    // Every backfilled row carries the literal, compared against Postgres's
    // own evaluation of the same literal rather than a hardcoded string.
    let row_count: i64 = client
        .query_one("select count(*) from t", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(row_count, 50, "every source row must be backfilled");

    for (name, keyword, text, _) in CASES {
        let mismatches: i64 = client
            .query_one(
                &format!("select count(*) from t where {name} is distinct from {keyword} '{text}'"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("compare {name}: {e}"))
            .get(0);
        assert_eq!(
            mismatches, 0,
            "{name} must equal {keyword} '{text}' on every row"
        );
    }
}

/// parse -> create -> stage CDC -> **apply** -> read, the incremental path
/// (`$n::text::<type>`), cross-checked against `defs::oracle::recompute` —
/// the independently-authored Rust evaluator — so the assertion is
/// evaluator-vs-Postgres, not evaluator-vs-itself.
#[tokio::test]
async fn a_cdc_apply_writes_typed_literal_values_matching_the_evaluator() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Empty at definition time, exactly as `apply.rs`'s own drain test does,
    // so the initial backfill doesn't race the hand-staged CDC rows.
    client
        .batch_execute("create table s (id bigint primary key)")
        .await
        .expect("create source");

    let text = format!("TRANSFORM t FROM s SELECT {}", select_list_cast_spelling());
    let cols = source_columns();
    let def = parse(&text).expect("parses");
    create_definition(&db.pool, &text, &cols)
        .await
        .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect pk");
    create_target_table(&db.pool, &def, "public", &pk, &cols, &def.source)
        .await
        .expect("create target table");

    client
        .execute("insert into s (id) values (1), (2)", &[])
        .await
        .expect("seed source rows after the definition exists");
    insert_cdc_row(&client, "1", r#"{"id":"1"}"#).await;
    insert_cdc_row(&client, "2", r#"{"id":"2"}"#).await;

    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "typed_literal_worker",
        1,
        "trellis_typed_literal_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something");

    // Both columns' declared types survive the apply path, and the persisted
    // values match what the Rust evaluator computed for the same rows.
    for (name, _, _, expected_pg_type) in CASES {
        assert_eq!(&column_pg_type(&client, "t", name).await, expected_pg_type);
    }

    let oracle = recompute(&db.pool, &def, &pk[0].name, &cols)
        .await
        .expect("oracle recompute");
    let select_list = CASES
        .iter()
        .map(|(name, _, _, _)| format!("{name}::text"))
        .collect::<Vec<_>>()
        .join(", ");
    let rows = client
        .query(&format!("select id::text, {select_list} from t"), &[])
        .await
        .expect("read target");
    assert_eq!(rows.len(), 2, "both staged rows must have been applied");

    for row in rows {
        let id: String = row.get(0);
        let expected = &oracle[&id];
        for (i, (name, _, text, _)) in CASES.iter().enumerate() {
            let persisted: Option<String> = row.get(i + 1);
            assert_eq!(
                persisted.as_deref(),
                Some(*text),
                "{name} for id {id}: Postgres's own rendering must equal the literal source text"
            );
            assert_eq!(
                persisted,
                expected[*name].as_ref().map(|v| v.to_string()),
                "{name} for id {id}: persisted value must match the Rust evaluator"
            );
        }
    }
}

/// The reviewer's scenario, reproduced and then defended against.
///
/// Issue #109's canonical-form rule has two halves. The first — an ISO
/// literal *parses* to the same value under every `DateStyle` — is a
/// property of the literal itself and needs no help. The second — the Rust
/// evaluator's text and Postgres's rendering are byte-identical — is only
/// true if Postgres renders in ISO, and Trellis pinned no output GUC
/// anywhere before this test's fix: `pool::session_bootstrap` and the
/// non-pooled connect sites all set `search_path` and nothing else.
///
/// So a server, database or role carrying `ALTER ... SET datestyle` would
/// have made the generative suite's byte-exact cross-check
/// (`Comparison::Exact` for a `ValueType::Other` field) report divergences
/// on a target that is in fact perfectly converged. Under
/// `DateStyle = 'SQL, MDY'`, `('2024-01-01'::date)::text` is `01/01/2024`.
///
/// This sets exactly that hostile configuration at the **database** level,
/// where it applies to every session that connects afterwards — including
/// ones the engine opens itself — and then asserts the engine still reads
/// canonical text. It fails without `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`.
///
/// Every type family in epic #123 hits the same wall — `interval` under
/// `IntervalStyle`, floats under `extra_float_digits`, and (issue #246)
/// `timestamptz` under `TimeZone`, all now pinned in
/// `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` — and a future family will hit it
/// too; extending that one constant and this one test's hostile-GUC list is
/// the intended way to cover them.
#[tokio::test]
async fn a_hostile_database_level_output_guc_does_not_change_what_the_engine_reads() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    {
        // Applied on its own connection and *not* reverted: `ALTER DATABASE`
        // only affects sessions started after it, which is exactly the
        // production hazard being modelled.
        let client = connect_raw(db.dsn()).await;
        client
            .batch_execute(&format!(
                "alter database \"{}\" set datestyle to 'SQL, MDY'; \
                 alter database \"{}\" set bytea_output to 'escape'; \
                 alter database \"{}\" set extra_float_digits to 0",
                db.name(),
                db.name(),
                db.name()
            ))
            .await
            .expect("apply hostile database-level GUCs");
    }

    // A connection that does *not* pin anything sees the hostile setting —
    // proof the scenario is real and this test isn't vacuous.
    {
        let (unpinned, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let rendered: String = unpinned
            .query_one("select ('2024-01-01'::date)::text", &[])
            .await
            .expect("render a date on an unpinned session")
            .get(0);
        assert_eq!(
            rendered, "01/01/2024",
            "the hostile GUC must actually be in effect, or this test proves nothing"
        );
    }

    // A pool built *after* the ALTER — deadpool recycles connections with
    // `RecyclingMethod::Fast`, so `db.pool`'s own connections were
    // established before it and would keep the old session state no matter
    // what `session_bootstrap` does. Building a fresh one is what makes this
    // test bite: without `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` its
    // connections inherit the database's hostile defaults.
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let pool = Pool::new(&config).expect("build a pool against the hostile database");

    let pooled = pool.get().await.expect("pooled connection");
    let rendered: String = pooled
        .query_one("select ('2024-01-01'::date)::text", &[])
        .await
        .expect("render a date on a pooled session")
        .get(0);
    assert_eq!(
        rendered, "2024-01-01",
        "a pooled connection must render ISO regardless of the database's DateStyle"
    );
    let rendered_bytea: String = pooled
        .query_one("select ('\\x0102ff'::bytea)::text", &[])
        .await
        .expect("render a bytea on a pooled session")
        .get(0);
    assert_eq!(
        rendered_bytea, "\\x0102ff",
        "a pooled connection must render hex regardless of the database's bytea_output"
    );
    // Issue #112's half of the same hazard. `extra_float_digits = 0` is the
    // pre-Postgres-12 default and rounds to `DBL_DIG` significant digits,
    // which is *lossy*: `0.1::float8 + 0.2::float8` renders `0.3` under `0`
    // and `0.30000000000000004` under `1`. `float::render` reproduces the
    // `>= 1` spelling, so a connection that inherited `0` would make the
    // evaluator and the server disagree byte-for-byte on a converged value.
    let rendered_float: String = pooled
        .query_one("select (0.1::float8 + 0.2::float8)::text", &[])
        .await
        .expect("render a float8 on a pooled session")
        .get(0);
    assert_eq!(
        rendered_float, "0.30000000000000004",
        "a pooled connection must render shortest-round-trip floats regardless of the \
         database's extra_float_digits"
    );
    assert_eq!(
        rendered_float,
        trellis::float::render(0.1 + 0.2, trellis::FloatWidth::Float8),
        "and `float::render` must agree with it"
    );
    drop(pooled);

    // And the whole pipeline still agrees end-to-end under that database.
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table s (id bigint primary key); \
             insert into s (id) select g from generate_series(1, 5) g",
        )
        .await
        .expect("seed source");

    install_definition(
        &pool,
        &format!(
            "TRANSFORM t FROM s SELECT {}",
            select_list_typed_literal_spelling()
        ),
        &source_columns(),
        "public",
    )
    .await
    .expect("install_definition under a hostile DateStyle");
    drain_backfill_chunks(&pool).await;

    for (name, _, text, _) in CASES {
        let persisted: Option<String> = client
            .query_one(&format!("select {name}::text from t limit 1"), &[])
            .await
            .unwrap_or_else(|e| panic!("read {name}: {e}"))
            .get(0);
        assert_eq!(
            persisted.as_deref(),
            Some(*text),
            "{name} must still render as the literal's own source text"
        );
    }
}
