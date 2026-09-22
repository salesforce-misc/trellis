//! End-to-end tests for issue #111's exact integer types
//! (`smallint`/`integer`/`bigint`, plus `oid`) against a real, ephemeral
//! Postgres via `testkit::TestCluster`.
//!
//! # Why these run against a live server
//!
//! ADR-0004 makes Postgres the correctness oracle for this grammar, and the
//! two claims #111 actually makes are *exactly* claims about agreeing with
//! Postgres:
//!
//! 1. **Result types.** `int4 + int4` is `integer` in Postgres, `int2 + int8`
//!    is `bigint`, `int4 + numeric` is `numeric`, `sum(int4)` is `bigint`,
//!    `sum(int8)` is `numeric`, `avg(int4)` is `numeric`, `max(int2)` is
//!    `smallint`. Every one of those is checked here against Postgres's own
//!    `pg_typeof` on the equivalent expression, not against a table of
//!    expectations copied out of the documentation.
//! 2. **Overflow.** `int4 + int4` raises `22003 numeric_value_out_of_range`
//!    once the sum leaves `int4`; `int4 + numeric` never does. Checked by
//!    running the same operands through Postgres and through the evaluator
//!    and asserting the two agree about *whether* it fails, row by row.
//!
//! Neither is provable by a unit test: a unit test asserting `int4 + int4 ->
//! integer` is just the implementation restating itself. Per ADR-0013 the
//! comparisons below are against independently-authored SQL — plain
//! `pg_typeof`, plain `select ... group by ...` — never against
//! `defs::oracle::recompute`, which would be the engine's own renderer
//! grading the engine's own evaluator.
//!
//! Harness conventions (`install_definition` + drain the chunk queue, then
//! introspect `information_schema`) follow `defs_typed_literals.rs`.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};
use trellis::IntWidth;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::eval::{RegexCache, Row, Value, evaluate};
use trellis::defs::{chunk_queue, create_relationship, install_definition, parse, validate};
use trellis::{Config, Trellis, TrellisOptions};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

/// The source table every test below installs against: one column per exact
/// integer width, one `numeric` (so the mixed-type coercion has something to
/// mix with), one `text` (for the `integer`-returning scalar functions) and
/// one `oid`.
const SOURCE_DDL: &str = "create table s ( \
     id bigint primary key, \
     a2 smallint, \
     a4 integer, \
     a8 bigint, \
     n numeric, \
     txt text, \
     o oid \
   ); \
   alter table s replica identity full";

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("a2".to_string(), ValueType::Integer(IntWidth::Int2)),
        ("a4".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("a8".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("n".to_string(), ValueType::Numeric),
        ("txt".to_string(), ValueType::Text),
        (
            "o".to_string(),
            ValueType::Other(trellis::defs::PgType::Oid),
        ),
    ])
}

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

/// Drives the durable backfill chunk queue to completion, standing in for a
/// running drain worker (copied from `defs_typed_literals.rs`).
async fn drain_backfill_chunks(pool: &trellis::Pool, target_schema: &str) {
    const CLAIMED_BY: &str = "exact_integer_test_backfill_worker";
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
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                target_schema,
                CLAIMED_BY,
                Duration::from_secs(5),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

/// The declared Postgres type of `table`.`column`, straight from
/// `information_schema` — i.e. what Trellis's DDL actually emitted.
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

/// Postgres's own type for `sql_expr` evaluated over the source table — the
/// independent oracle every result-type assertion below compares against.
///
/// `pg_typeof` reports the *internal* name for the integer widths (`int2`,
/// `int4`, `int8`) while `information_schema` reports the SQL standard one
/// (`smallint`, `integer`, `bigint`), so this normalizes through
/// `format_type`, which emits the standard spelling both sides can be read
/// in.
async fn postgres_type_of(client: &Client, sql_expr: &str) -> String {
    client
        .query_one(
            &format!("select format_type(pg_typeof({sql_expr}), null) from s limit 1"),
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("pg_typeof({sql_expr}): {e}"))
        .get(0)
}

/// Seeds one row whose every column is non-`NULL` and small, so `pg_typeof`
/// has something to evaluate over and the arithmetic below can't overflow by
/// accident.
async fn seed_one_row(client: &Client) {
    client
        .execute(
            "insert into s (id, a2, a4, a8, n, txt, o) \
             values (1, 3, 4, 5, 6.25, 'hello', 12345)",
            &[],
        )
        .await
        .expect("seed row");
}

// ---------------------------------------------------------------------
// 1. Result types
// ---------------------------------------------------------------------

/// `(field name, Trellis expression, the equivalent Postgres expression)`.
///
/// The two spellings are identical here on purpose — the grammar is a subset
/// of Postgres's own — which is what makes the comparison meaningful: the
/// *same* text is handed to Trellis's type inference and to `pg_typeof`, and
/// the two must reach the same type.
const SCALAR_CASES: &[(&str, &str, &str)] = &[
    ("f0", "a2 + a2", "a2 + a2"),
    ("f1", "a2 + a4", "a2 + a4"),
    ("f2", "a4 + a4", "a4 + a4"),
    ("f3", "a4 + a8", "a4 + a8"),
    ("f4", "a8 + a8", "a8 + a8"),
    ("f5", "a8 + a2", "a8 + a2"),
    // Mixed with arbitrary-precision `numeric`: Postgres promotes the
    // integer operand through its implicit cast, so the result is `numeric`
    // and cannot overflow.
    ("f6", "a4 + n", "a4 + n"),
    ("f7", "n + a8", "n + a8"),
    // Unadorned literals: `1` is `integer`, `3000000000` is `bigint`, and
    // `1.5` is `numeric` — Postgres's own literal-typing rule.
    ("f8", "a4 + 1", "a4 + 1"),
    ("f9", "a2 + 3000000000", "a2 + 3000000000"),
    ("f10", "a4 + 1.5", "a4 + 1.5"),
    // The four scalar functions are `integer`-returning in Postgres.
    ("f11", "char_length(txt)", "char_length(txt)"),
    ("f12", "octet_length(txt)", "octet_length(txt)"),
    ("f13", "strpos(txt, 'l')", "strpos(txt, 'l')"),
    ("f14", "char_length(txt) + 1", "char_length(txt) + 1"),
];

/// The type of every derived column Trellis declares equals the type
/// Postgres gives the same expression.
///
/// This is the whole "type honesty" half of #111 in one assertion. Before
/// this issue every one of these columns came out `numeric`, including the
/// six that Postgres types as one of the three integer widths.
#[tokio::test]
async fn derived_column_types_match_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client.batch_execute(SOURCE_DDL).await.expect("source");
    seed_one_row(&client).await;

    let select_list = SCALAR_CASES
        .iter()
        .map(|(name, expr, _)| format!("{expr} AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    install_definition(
        &db.pool,
        &format!("TRANSFORM t FROM s SELECT {select_list}"),
        &source_columns(),
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool, "public").await;

    for (name, expr, sql) in SCALAR_CASES {
        let declared = column_pg_type(&client, "t", name).await;
        let expected = postgres_type_of(&client, sql).await;
        assert_eq!(
            declared, expected,
            "Trellis declared `{expr}` as {declared}, Postgres types it as {expected}"
        );
    }
}

/// `(field name, aggregate expression, the equivalent Postgres aggregate)`.
const AGGREGATE_CASES: &[(&str, &str, &str)] = &[
    // Postgres widens a narrow integer sum to `bigint`...
    ("g0", "SUM(a2)", "sum(a2)"),
    ("g1", "SUM(a4)", "sum(a4)"),
    // ...and a `bigint` sum all the way to `numeric`.
    ("g2", "SUM(a8)", "sum(a8)"),
    ("g3", "SUM(n)", "sum(n)"),
    // The average of integers is never an integer.
    ("g4", "AVG(a4)", "avg(a4)"),
    ("g5", "AVG(a8)", "avg(a8)"),
    // `min`/`max` keep their argument's own type exactly.
    ("g6", "MIN(a2)", "min(a2)"),
    ("g7", "MAX(a4)", "max(a4)"),
    ("g8", "MAX(a8)", "max(a8)"),
];

/// Every aggregate's derived column type equals Postgres's own, including
/// the two widenings (`sum(int4) -> bigint`, `sum(int8) -> numeric`) that
/// are easy to get wrong by assuming an aggregate returns its argument's
/// type.
#[tokio::test]
async fn aggregate_column_types_match_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client.batch_execute(SOURCE_DDL).await.expect("source");
    seed_one_row(&client).await;

    let select_list = AGGREGATE_CASES
        .iter()
        .map(|(name, expr, _)| format!("{expr} AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    install_definition(
        &db.pool,
        &format!("TRANSFORM t FROM s GROUP BY a4 SELECT a4 AS a4, {select_list}"),
        &source_columns(),
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool, "public").await;

    // The `GROUP BY` key itself is a derived column too, and it must keep
    // the source column's exact width — that is the "exact key round-trip"
    // the issue asks for.
    assert_eq!(column_pg_type(&client, "t", "a4").await, "integer");

    for (name, expr, sql) in AGGREGATE_CASES {
        let declared = column_pg_type(&client, "t", name).await;
        let expected = postgres_type_of(&client, sql).await;
        assert_eq!(
            declared, expected,
            "Trellis declared `{expr}` as {declared}, Postgres types it as {expected}"
        );
    }
}

// ---------------------------------------------------------------------
// 2. Overflow
// ---------------------------------------------------------------------

/// `(width, lhs, rhs)` pairs that sit on, or just over, each width's edge.
const BOUNDARY_CASES: &[(IntWidth, i64, i64)] = &[
    (IntWidth::Int2, 32766, 1),
    (IntWidth::Int2, 32767, 1),
    (IntWidth::Int2, -32768, -1),
    (IntWidth::Int4, 2147483646, 1),
    (IntWidth::Int4, 2147483647, 1),
    (IntWidth::Int4, -2147483648, -1),
    (IntWidth::Int8, i64::MAX - 1, 1),
    (IntWidth::Int8, i64::MAX, 1),
    (IntWidth::Int8, i64::MIN, -1),
];

fn add_def(lhs: &str, rhs: &str) -> TransformDef {
    TransformDef {
        target: "t".to_string(),
        explicit_target_schema: None,
        source: "s".to_string(),
        explicit_source_schema: None,
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(lhs.to_string())),
                rhs: Box::new(Expr::Column(rhs.to_string())),
            },
        }],
        predicate: Predicate::True,
    }
}

fn row2(a: i64, b: i64) -> Row {
    HashMap::from([
        ("a".to_string(), Some(a.to_string())),
        ("b".to_string(), Some(b.to_string())),
    ])
}

/// Trellis raises exactly when Postgres raises, and produces exactly what
/// Postgres produces otherwise.
///
/// This is the behavioural half of #111. The evaluator used to fold every
/// one of these through arbitrary-precision `numeric`, so it silently
/// produced `32768` where a server-side `select a2 + b2` raised
/// `22003 numeric_value_out_of_range` — meaning an incremental apply and a
/// backfill of the *same definition* could disagree about whether it works
/// at all.
///
/// The Postgres side is written independently (`select $1::<w> + $2::<w>`),
/// with the operands bound as text and cast server-side, so neither side
/// sees the other's answer.
#[tokio::test]
async fn integer_addition_raises_exactly_where_postgres_raises() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let def = add_def("a", "b");

    for (width, lhs, rhs) in BOUNDARY_CASES {
        let pg_name = width.pg_name();
        let columns = HashMap::from([
            ("a".to_string(), ValueType::Integer(*width)),
            ("b".to_string(), ValueType::Integer(*width)),
        ]);

        let postgres = client
            .query_one(
                &format!("select ($1::text::{pg_name} + $2::text::{pg_name})::text"),
                &[&lhs.to_string(), &rhs.to_string()],
            )
            .await;
        let trellis = evaluate(&def, &row2(*lhs, *rhs), &columns, &mut RegexCache::new());

        match (postgres, trellis) {
            (Ok(pg_row), Ok(values)) => {
                let expected: String = pg_row.get(0);
                let got = values["out"].as_ref().expect("a non-NULL sum").to_string();
                assert_eq!(
                    got, expected,
                    "{lhs} + {rhs} on {pg_name}: Postgres says {expected}, Trellis says {got}"
                );
            }
            (Err(pg_err), Err(eval_err)) => {
                let db_err = pg_err
                    .as_db_error()
                    .unwrap_or_else(|| panic!("{lhs} + {rhs} on {pg_name}: not a server error"));
                assert_eq!(
                    db_err.code(),
                    &SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
                    "{lhs} + {rhs} on {pg_name}: unexpected SQLSTATE"
                );
                // Postgres's own wording, reproduced verbatim, so an
                // operator reading a quarantine record sees the message
                // they would have seen from the server.
                assert!(
                    eval_err
                        .to_string()
                        .contains(&format!("{pg_name} out of range")),
                    "{lhs} + {rhs} on {pg_name}: expected Postgres's wording, got: {eval_err}"
                );
            }
            (Ok(pg_row), Err(eval_err)) => {
                let value: String = pg_row.get(0);
                panic!(
                    "{lhs} + {rhs} on {pg_name}: Postgres computed {value}, Trellis raised \
                     {eval_err}"
                );
            }
            (Err(pg_err), Ok(values)) => panic!(
                "{lhs} + {rhs} on {pg_name}: Postgres raised {pg_err}, Trellis computed {:?}",
                values["out"]
            ),
        }
    }
}

/// Mixing an integer with `numeric` promotes through Postgres's implicit
/// cast to the *unbounded* `numeric_add`, so the sum that overflows above
/// does not overflow here — on either side.
#[tokio::test]
async fn mixing_with_numeric_does_not_overflow_on_either_side() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let def = add_def("a", "b");
    let columns = HashMap::from([
        ("a".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("b".to_string(), ValueType::Numeric),
    ]);

    let expected: String = client
        .query_one(
            "select ($1::text::integer + $2::text::numeric)::text",
            &[&"2147483647".to_string(), &"1".to_string()],
        )
        .await
        .expect("postgres must not raise for int + numeric")
        .get(0);

    let values = evaluate(&def, &row2(2147483647, 1), &columns, &mut RegexCache::new())
        .expect("trellis must not raise for int + numeric either");
    assert_eq!(values["out"].as_ref().unwrap().to_string(), expected);
    assert_eq!(expected, "2147483648");
}

/// A value that can't have come out of a column of its declared width is a
/// named error, not a silently-too-wide integer. Defense-in-depth against a
/// corrupted or hand-written staged image.
#[test]
fn a_value_outside_its_columns_width_is_rejected_at_decode_time() {
    let def = add_def("a", "b");
    let columns = HashMap::from([
        ("a".to_string(), ValueType::Integer(IntWidth::Int2)),
        ("b".to_string(), ValueType::Integer(IntWidth::Int2)),
    ]);
    let err = evaluate(&def, &row2(40000, 1), &columns, &mut RegexCache::new())
        .expect_err("40000 is not a smallint");
    assert!(
        err.to_string().contains("smallint out of range"),
        "got: {err}"
    );
}

// ---------------------------------------------------------------------
// 3. Values, against an independently-written SQL query
// ---------------------------------------------------------------------

/// The backfilled target equals what a hand-written `GROUP BY` query
/// computes over the same rows — values, not just types.
///
/// Written as a **symmetric difference** (`EXCEPT` both ways, counted)
/// rather than by pulling both sides into Rust and comparing, so a missing
/// group, an extra group and a wrong value all surface the same way.
/// `EXCEPT` is also the operator that gets `NULL` right here: it compares
/// rows with `NOT DISTINCT` semantics, so a `NULL`-keyed group matches a
/// `NULL`-keyed group instead of vanishing. The right-hand side names no
/// Trellis machinery at all (ADR-0013: the oracle must not be the engine's
/// own renderer).
#[tokio::test]
async fn an_integer_group_by_target_matches_a_hand_written_sql_query() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client.batch_execute(SOURCE_DDL).await.expect("source");
    client
        .batch_execute(
            "insert into s (id, a2, a4, a8, n, txt, o) values \
               (1,  1,  10, 100, 1.5, 'a', 1), \
               (2,  1,  10, 200, 2.5, 'b', 2), \
               (3,  2,  20, 300, 3.5, 'c', 3), \
               (4,  2,  20, 400, 4.5, 'd', 4), \
               (5,  3,  30, 500, 5.5, 'e', 5), \
               (6,  3, -30, -32768, -6.5, 'f', 6)",
        )
        .await
        .expect("seed rows");

    install_definition(
        &db.pool,
        "TRANSFORM t FROM s GROUP BY a2 \
         SELECT a2 AS a2, SUM(a4) AS total, MAX(a8) AS biggest, MIN(a4) AS smallest, \
                AVG(a8) AS mean",
        &source_columns(),
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool, "public").await;

    let differences: i64 = client
        .query_one(
            "with expected as ( \
                 select a2, sum(a4) as total, max(a8) as biggest, min(a4) as smallest, \
                        avg(a8) as mean \
                 from s group by a2 \
             ), \
             actual as (select a2, total, biggest, smallest, mean from t) \
             select (select count(*) from (table expected except table actual) missing) \
                  + (select count(*) from (table actual except table expected) extra)",
            &[],
        )
        .await
        .expect("difference query")
        .get(0);
    assert_eq!(differences, 0, "the target must equal the SQL oracle");

    // And the key column really is an exact integer, not a `numeric` that
    // happens to render the same.
    assert_eq!(column_pg_type(&client, "t", "a2").await, "smallint");
    assert_eq!(column_pg_type(&client, "t", "total").await, "bigint");
    assert_eq!(column_pg_type(&client, "t", "biggest").await, "bigint");
    assert_eq!(column_pg_type(&client, "t", "smallest").await, "integer");
    assert_eq!(column_pg_type(&client, "t", "mean").await, "numeric");
}

// ---------------------------------------------------------------------
// 4. `oid`
// ---------------------------------------------------------------------

/// `oid` is admitted as a `GROUP BY` key and passes through as a real `oid`
/// column, and the grouped target matches a hand-written SQL query.
///
/// It is *not* an arithmetic type: Postgres has no `oid + oid`, so neither
/// does Trellis (checked below). This split is the whole design decision
/// recorded on `pg_type::PgType::Oid`.
#[tokio::test]
async fn an_oid_column_is_a_valid_group_by_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client.batch_execute(SOURCE_DDL).await.expect("source");
    client
        .batch_execute(
            "insert into s (id, a4, o) values \
               (1, 10, 7), (2, 20, 7), (3, 30, 9), (4, 40, 4294967295)",
        )
        .await
        .expect("seed rows");

    install_definition(
        &db.pool,
        "TRANSFORM t FROM s GROUP BY o SELECT o AS o, SUM(a4) AS total",
        &source_columns(),
        "public",
    )
    .await
    .expect("an oid GROUP BY key must be accepted");
    drain_backfill_chunks(&db.pool, "public").await;

    assert_eq!(column_pg_type(&client, "t", "o").await, "oid");

    let differences: i64 = client
        .query_one(
            "with expected as (select o, sum(a4) as total from s group by o), \
             actual as (select o, total from t) \
             select (select count(*) from (table expected except table actual) missing) \
                  + (select count(*) from (table actual except table expected) extra)",
            &[],
        )
        .await
        .expect("difference query")
        .get(0);
    assert_eq!(differences, 0, "the oid-keyed target must equal the oracle");
}

/// `oid` has no arithmetic in Postgres (`select 1::oid + 1::oid` is
/// `operator does not exist: oid + oid`), so Trellis must reject it too
/// rather than quietly treating it as a 32-bit integer — the reason it is
/// deliberately not a `ValueType::Integer`.
#[tokio::test]
async fn oid_has_no_arithmetic_in_trellis_because_it_has_none_in_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let pg_err = client
        .query_one("select 1::oid + 1::oid", &[])
        .await
        .expect_err("postgres has no oid + oid");
    assert_eq!(
        pg_err.as_db_error().expect("a server error").code(),
        &SqlState::UNDEFINED_FUNCTION
    );

    let def = parse("TRANSFORM t FROM s SELECT o + o AS x").expect("parses");
    let err = validate(&def, &source_columns(), &HashMap::new())
        .expect_err("Trellis must reject oid + oid too");
    assert!(err.to_string().contains('x'), "must name the field: {err}");
}

/// An `oid` literal is spellable (`OID '12345'`), and only in Postgres's own
/// canonical rendering — the typed-literal allowlist row #111 adds.
#[test]
fn an_oid_literal_must_be_in_canonical_form() {
    for good in ["OID '0'", "OID '12345'", "CAST('4294967295' AS oid)"] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {good} AS x"))
            .unwrap_or_else(|e| panic!("{good} should parse: {e}"));
        validate(&def, &source_columns(), &HashMap::new())
            .unwrap_or_else(|e| panic!("{good} should validate: {e}"));
    }
    for bad in ["OID '-1'", "OID '007'", "OID '4294967296'", "OID '1.0'"] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {bad} AS x"))
            .unwrap_or_else(|e| panic!("{bad} should parse (validate-time error): {e}"));
        let err = validate(&def, &source_columns(), &HashMap::new())
            .expect_err("a non-canonical oid literal must be rejected");
        assert!(
            err.to_string().contains("not in canonical form"),
            "{bad}: {err}"
        );
    }
}

// ---------------------------------------------------------------------
// 5. Values survive a round trip through a text-keyed path
// ---------------------------------------------------------------------

/// Every boundary value of every width renders to text and parses back to
/// itself — byte-identically to Postgres's own rendering.
///
/// This is the "exact key round-trip" claim stated directly. Integer keys
/// worked before #111 by luck of sharing `Numeric`'s decimal rendering; now
/// they work because `Value::Integer`'s `Display` *is* Postgres's
/// `int4out`, which is what lets these types stay on
/// `catalog::TEXT_STABLE_JOIN_KEY_TYPES`.
#[tokio::test]
async fn integer_text_rendering_is_byte_identical_to_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // Identity: `SELECT a AS out`, so the evaluated value is exactly the
    // decoded column value.
    let def = TransformDef {
        target: "t".to_string(),
        explicit_target_schema: None,
        source: "s".to_string(),
        explicit_source_schema: None,
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::Column("a".to_string()),
        }],
        predicate: Predicate::True,
    };

    for width in IntWidth::ALL {
        let (min, max) = width.range();
        for value in [min, -1, 0, 1, max] {
            let columns = HashMap::from([("a".to_string(), ValueType::Integer(width))]);
            let row: Row = HashMap::from([("a".to_string(), Some(value.to_string()))]);

            let expected: String = client
                .query_one(
                    &format!("select ($1::text::{})::text", width.pg_name()),
                    &[&value.to_string()],
                )
                .await
                .unwrap_or_else(|e| panic!("{value} as {width}: {e}"))
                .get(0);

            let values = evaluate(&def, &row, &columns, &mut RegexCache::new())
                .unwrap_or_else(|e| panic!("{value} as {width}: {e}"));
            let got = values["out"].as_ref().expect("non-NULL");
            assert!(
                matches!(got, Value::Integer(w, v) if *w == width && *v == value),
                "{value} as {width} decoded as {got:?}"
            );
            assert_eq!(
                got.to_string(),
                expected,
                "{value} as {width}: Trellis renders {got}, Postgres renders {expected}"
            );
        }
    }
}

// ---------------------------------------------------------------------
// 6. The incremental path, live
// ---------------------------------------------------------------------

/// The **incremental** aggregate path keeps a `bigint` `SUM` column correct
/// across inserts, updates and deletes.
///
/// This is the one claim the backfill test above cannot make, and it is
/// where a `SUM(integer) -> bigint` column is genuinely load-bearing:
/// `staging::apply_aggregate` maintains a group's running sum as a hidden
/// `numeric` partial and folds it into the visible column, so the visible
/// column's type change from `numeric` to `bigint` puts a Postgres
/// assignment cast on the hot path of every delta. Nothing else in the test
/// suite exercises that — the generative suite's own value columns are
/// `numeric`, so its `SUM` targets stay `numeric` too.
///
/// Run through the public `Trellis` facade against the full live pipeline
/// (real logical-replication intake, real ring, real drain workers), with
/// convergence awaited via `watermark_token`/`await_converged` rather than
/// slept for. The comparison at the end is a symmetric difference against a
/// hand-written `GROUP BY`, per ADR-0013.
#[tokio::test]
async fn an_incremental_bigint_sum_stays_equal_to_a_hand_written_group_by() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE_DDL).await.expect("source");

    let definer = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect definer");
    definer
        .apply(
            "TRANSFORM t FROM s GROUP BY a2 \
             SELECT a2 AS a2, SUM(a4) AS total, MAX(a8) AS biggest, AVG(a4) AS mean",
        )
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown definer");

    // The `SUM` column really is a `bigint` — otherwise the rest of this
    // test would be exercising the same `numeric` path everything else does.
    assert_eq!(column_pg_type(&raw, "t", "total").await, "bigint");
    assert_eq!(column_pg_type(&raw, "t", "biggest").await, "bigint");
    assert_eq!(column_pg_type(&raw, "t", "a2").await, "smallint");

    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect running trellis");

    // A workload that moves rows *between* groups, empties a group, and
    // pushes a running sum well past `int4` — so a delta that silently
    // stayed in `int4`, or one that lost precision through the `numeric`
    // partial, shows up as a difference below.
    for sql in [
        "insert into s (id, a2, a4, a8) values \
           (1, 1, 2000000000, 10), (2, 1, 2000000000, 20), \
           (3, 2, 5, 30), (4, 2, 7, 40), (5, 3, 9, 50)",
        "update s set a4 = 2000000001 where id = 1",
        "update s set a2 = 2 where id = 5",
        "delete from s where id = 3",
        "insert into s (id, a2, a4, a8) values (6, 1, 2000000000, 60)",
        "delete from s where id = 4",
        "update s set a2 = null where id = 2",
    ] {
        raw.execute(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let token = trellis.watermark_token().await.expect("watermark_token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("the pipeline must converge");

    let differences: i64 = raw
        .query_one(
            "with expected as ( \
                 select a2, sum(a4) as total, max(a8) as biggest, avg(a4) as mean \
                 from s group by a2 \
             ), \
             actual as (select a2, total, biggest, mean from t) \
             select (select count(*) from (table expected except table actual) missing) \
                  + (select count(*) from (table actual except table expected) extra)",
            &[],
        )
        .await
        .expect("difference query")
        .get(0);
    assert_eq!(
        differences, 0,
        "the incrementally-maintained target must equal the SQL oracle"
    );

    // And the sum really did leave `int4`, so the assertion above had teeth.
    let biggest_total: i64 = raw
        .query_one("select max(total) from t", &[])
        .await
        .expect("read back the largest group total")
        .get(0);
    assert!(
        biggest_total > i64::from(i32::MAX),
        "the workload must push a group's sum past int4 for this test to mean anything, \
         got {biggest_total}"
    );

    trellis.shutdown().await.expect("shutdown");
}

/// An `oid` column is accepted as a **relationship join key**, and a
/// `smallint`/`integer`/`bigint` one keeps being accepted — the
/// `catalog::TEXT_STABLE_JOIN_KEY_TYPES` allowlist #111 widens.
///
/// The `numeric` case is the control: it must still be *rejected*, because
/// `1.0::text != 1.00::text` though the two are equal to Postgres. That
/// contrast is the whole reason the allowlist is a per-type positive list
/// rather than "anything numeric-ish", and it is what makes admitting `oid`
/// a claim about `oid_out`'s canonical rendering rather than a guess.
#[tokio::test]
async fn oid_and_the_integer_widths_are_accepted_as_relationship_join_keys() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent2 (k smallint primary key); \
             create table parent4 (k integer primary key); \
             create table parent8 (k bigint primary key); \
             create table parent_oid (k oid primary key); \
             create table parent_num (k numeric primary key); \
             create table child ( \
                 id bigint primary key, \
                 f2 smallint, f4 integer, f8 bigint, f_oid oid, f_num numeric \
             ); \
             alter table child replica identity full; \
             alter table parent2 replica identity full; \
             alter table parent4 replica identity full; \
             alter table parent8 replica identity full; \
             alter table parent_oid replica identity full; \
             alter table parent_num replica identity full",
        )
        .await
        .expect("create relationship tables");

    for (name, from_col, to_table) in [
        ("r2", "f2", "parent2"),
        ("r4", "f4", "parent4"),
        ("r8", "f8", "parent8"),
        ("r_oid", "f_oid", "parent_oid"),
    ] {
        create_relationship(
            &db.pool,
            &format!("RELATIONSHIP {name} FROM child.{from_col} TO {to_table}.k"),
        )
        .await
        .unwrap_or_else(|e| panic!("{name} ({to_table}.k) must be a valid join key: {e}"));
    }

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP r_num FROM child.f_num TO parent_num.k",
    )
    .await
    .expect_err("a numeric join key must still be rejected");
    assert!(
        err.to_string().contains("numeric"),
        "the rejection must name the offending type: {err}"
    );
}
