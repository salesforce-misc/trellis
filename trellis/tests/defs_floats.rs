//! End-to-end tests for issue #112's binary float types (`real`/`double
//! precision`) against a real, ephemeral Postgres via
//! `testkit::TestCluster`.
//!
//! # Why these run against a live server
//!
//! ADR-0004 makes Postgres the correctness oracle for this grammar, and
//! `docs/type-support.md` parked this family on "IEEE edge cases need a
//! decision". The decision #112 makes is *not* to invent semantics: it is to
//! reproduce what Postgres already does, which in two places is famously not
//! IEEE 754. Every claim below is therefore a claim about agreeing with a
//! real server, and is checked against one:
//!
//! 1. **Comparison order.** `'NaN'::float8 = 'NaN'::float8` is **true**, and
//!    `NaN` sorts above `Infinity`. `-0 = 0` is true. None of that is
//!    assumed here — it is read back out of Postgres and compared against
//!    `trellis::float::compare`.
//! 2. **Result types.** `real + real` is `real`, `real + integer` is
//!    `double precision`, `sum(real)` is `real`, `avg(real)` is `double
//!    precision`. Checked against `pg_typeof` on the equivalent expression.
//! 3. **Overflow.** `3.4e38::float4 + 3.4e38::float4` raises `22003`;
//!    `'Infinity'::float4 + 1e38::float4` does not. Checked by running the
//!    same operands through both and asserting they agree about *whether* it
//!    fails.
//! 4. **Text rendering.** `trellis::float::render` must be byte-identical to
//!    `float4out`/`float8out`, over a grid spanning both notations, both
//!    signs, the subnormal floor and the finite ceiling.
//! 5. **Key roles.** `real`/`double precision` are rejected as relationship
//!    join keys, primary keys and `GROUP BY` keys — and the reason is
//!    demonstrated from the server, not asserted: `-0` and `0` are `=` but
//!    render differently, so the engine's `::text` key matching would split
//!    one Postgres group in two.
//!
//! Per ADR-0013, every comparison is against independently-authored SQL —
//! plain `pg_typeof`, plain `select ... group by ...`, plain `::text` —
//! never against `defs::oracle::recompute`, which would be the engine's own
//! renderer grading the engine's own evaluator.
//!
//! Harness conventions follow `defs_exact_integers.rs`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};
use trellis::FloatWidth;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{
    Expr, FieldDef, GroupByKey, KeySpace, Operator, Predicate, TransformDef, ValueType,
};
use trellis::defs::eval::{RegexCache, Row, Value, evaluate, evaluate_aggregate};
use trellis::defs::{chunk_queue, create_relationship, install_definition, parse, validate};
use trellis::integer::IntWidth;
use trellis::{Config, Trellis, TrellisOptions};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

/// The source table every test below installs against: one column per float
/// width, one `numeric` and one `integer` (so the mixed-type resolutions
/// have something to mix with), and a `bigint` key.
const SOURCE_DDL: &str = "create table s ( \
     id bigint primary key, \
     f4 real, \
     f8 double precision, \
     n numeric, \
     i integer \
   ); \
   alter table s replica identity full";

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("f4".to_string(), ValueType::Float(FloatWidth::Float4)),
        ("f8".to_string(), ValueType::Float(FloatWidth::Float8)),
        ("n".to_string(), ValueType::Numeric),
        ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
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
/// running drain worker (copied from `defs_exact_integers.rs`).
async fn drain_backfill_chunks(pool: &trellis::Pool) {
    // ADR-0016 (#418): registration only records a definition; the backfill
    // discharge dispatches its chunks.
    trellis::intake::publication::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    const CLAIMED_BY: &str = "float_test_backfill_worker";
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

/// Postgres's own type for `sql_expr` evaluated over the source table,
/// normalized through `format_type` so it reads in the same SQL-standard
/// spelling `information_schema` reports (`real`, `double precision`) rather
/// than `pg_typeof`'s internal `float4`/`float8`.
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

async fn seed_one_row(client: &Client) {
    client
        .execute(
            "insert into s (id, f4, f8, n, i) values (1, 1.5, 2.5, 6.25, 4)",
            &[],
        )
        .await
        .expect("seed row");
}

/// `SELECT <expr on column `a`> AS out` — the minimal definition shape the
/// evaluator-level tests below drive.
fn identity_def() -> TransformDef {
    TransformDef {
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
    }
}

fn add_def() -> TransformDef {
    TransformDef {
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            },
        }],
        ..identity_def()
    }
}

// ---------------------------------------------------------------------
// 1. Postgres's comparison order — the decision the issue asks for
// ---------------------------------------------------------------------

/// The `NaN`/±0 decision, read out of the oracle and compared against
/// [`trellis::float::compare`].
///
/// This is #112's central question and it is **not** a free design choice.
/// Postgres deliberately departs from IEEE 754 so floats have a total order
/// and can be used in `ORDER BY`/`GROUP BY`/`DISTINCT`/btree at all:
/// `'NaN' = 'NaN'` is true, and `NaN` sorts above every other value
/// including `Infinity`. `-0 = 0` keeps IEEE's answer. Trellis's job is to
/// match, so this test asks the server for each verdict and asserts
/// `float::compare` agrees — nothing here is hardcoded from documentation.
#[tokio::test]
async fn nan_and_signed_zero_follow_postgres_not_ieee() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // Every interesting pair, spelled the way Postgres spells them.
    const VALUES: &[(&str, f64)] = &[
        ("'-Infinity'", f64::NEG_INFINITY),
        ("-1", -1.0),
        ("'-0'", -0.0),
        ("0", 0.0),
        ("1", 1.0),
        ("'Infinity'", f64::INFINITY),
        ("'NaN'", f64::NAN),
    ];

    for width in FloatWidth::ALL {
        let pg = width.pg_name();
        for (a_sql, a) in VALUES {
            for (b_sql, b) in VALUES {
                let row = client
                    .query_one(
                        &format!(
                            "select {a_sql}::{pg} = {b_sql}::{pg}, \
                                    {a_sql}::{pg} < {b_sql}::{pg}, \
                                    {a_sql}::{pg} > {b_sql}::{pg}"
                        ),
                        &[],
                    )
                    .await
                    .unwrap_or_else(|e| panic!("{a_sql} vs {b_sql} as {pg}: {e}"));
                let (eq, lt, gt): (bool, bool, bool) = (row.get(0), row.get(1), row.get(2));
                let expected = match (eq, lt, gt) {
                    (true, false, false) => Ordering::Equal,
                    (false, true, false) => Ordering::Less,
                    (false, false, true) => Ordering::Greater,
                    other => panic!(
                        "{a_sql} vs {b_sql} as {pg}: Postgres's own comparison is not a total \
                         order? got {other:?}"
                    ),
                };
                assert_eq!(
                    trellis::float::compare(*a, *b),
                    expected,
                    "{a_sql} vs {b_sql} as {pg}"
                );
            }
        }
    }

    // The two headline departures from IEEE, asserted explicitly so a
    // reader of this test sees them rather than inferring them from the
    // matrix above.
    let (nan_eq, zero_eq, nan_above_inf): (bool, bool, bool) = {
        let row = client
            .query_one(
                "select 'NaN'::float8 = 'NaN'::float8, \
                        (-0.0::float8) = (0.0::float8), \
                        'NaN'::float8 > 'Infinity'::float8",
                &[],
            )
            .await
            .expect("the three headline comparisons");
        (row.get(0), row.get(1), row.get(2))
    };
    assert!(
        nan_eq,
        "Postgres must say NaN = NaN (IEEE says it does not)"
    );
    assert!(zero_eq, "Postgres must say -0 = 0");
    assert!(nan_above_inf, "Postgres must sort NaN above Infinity");
    assert!(trellis::float::equal(f64::NAN, f64::NAN));
    assert!(trellis::float::equal(-0.0, 0.0));
}

/// `ORDER BY` over a float column puts `NaN` last, and Trellis's sort key
/// ([`trellis::float::compare`]) reproduces that ordering exactly.
#[tokio::test]
async fn ordering_places_nan_last_on_both_sides() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let rendered: Vec<String> = client
        .query(
            // Aliased: a bare `order by v` would bind to the *output*
            // column (`v::text`) and sort by collated text, not by the
            // float order under test.
            "select v::text as rendered from (values \
                ('NaN'::float8), (1), ('Infinity'), ('-Infinity'), (0), (-1) \
             ) t(v) order by t.v",
            &[],
        )
        .await
        .expect("order by")
        .into_iter()
        .map(|row| row.get(0))
        .collect();

    let mut ours = vec![f64::NAN, 1.0, f64::INFINITY, f64::NEG_INFINITY, 0.0, -1.0];
    ours.sort_by(|a, b| trellis::float::compare(*a, *b));
    let ours: Vec<String> = ours
        .into_iter()
        .map(|v| trellis::float::render(v, FloatWidth::Float8))
        .collect();

    assert_eq!(ours, rendered);
    assert_eq!(rendered.last().map(String::as_str), Some("NaN"));
}

// ---------------------------------------------------------------------
// 2. Result types
// ---------------------------------------------------------------------

/// `(field name, Trellis expression, the equivalent Postgres expression)`.
/// The two spellings are identical on purpose — the grammar is a subset of
/// Postgres's own — so the *same* text is typed by Trellis and by
/// `pg_typeof`.
const SCALAR_CASES: &[(&str, &str)] = &[
    ("h0", "f4 + f4"),
    ("h1", "f8 + f8"),
    // A float mixed with anything else resolves to `float8pl`, so these are
    // all `double precision` — including `real + real`'s near neighbour
    // `real + double precision`.
    ("h2", "f4 + f8"),
    ("h3", "f8 + f4"),
    ("h4", "f4 + i"),
    ("h5", "i + f4"),
    ("h6", "f4 + n"),
    ("h7", "n + f8"),
    // An unadorned `1.5` is `numeric` in Postgres, not a float — so this is
    // the mixed case too, not `real + real`.
    ("h8", "f4 + 1.5"),
    ("h9", "f4 + 1"),
    // Typed float literals (issue #109's grammar, issue #112's new rows).
    ("h10", "REAL '1.5' + f4"),
    ("h11", "DOUBLE PRECISION '1.5' + f8"),
];

/// The type of every derived float column Trellis declares equals the type
/// Postgres gives the same expression.
///
/// Before #112 every one of these came out `numeric`, including the two
/// Postgres types as `real`.
#[tokio::test]
async fn derived_float_column_types_match_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client.batch_execute(SOURCE_DDL).await.expect("source");
    seed_one_row(&client).await;

    let select_list = SCALAR_CASES
        .iter()
        .map(|(name, expr)| format!("{expr} AS {name}"))
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
    drain_backfill_chunks(&db.pool).await;

    for (name, expr) in SCALAR_CASES {
        let declared = column_pg_type(&client, "t", name).await;
        // The `REAL '1.5'` spelling is Trellis's; Postgres accepts it too,
        // so the same text goes to `pg_typeof`.
        let expected = postgres_type_of(&client, expr).await;
        assert_eq!(
            declared, expected,
            "Trellis declared `{expr}` as {declared}, Postgres types it as {expected}"
        );
    }
}

/// `(field name, aggregate expression)`.
const AGGREGATE_CASES: &[(&str, &str)] = &[
    // A float sum keeps its argument's width — unlike `sum(int4) -> bigint`.
    ("k0", "SUM(f4)"),
    ("k1", "SUM(f8)"),
    // ...but `avg(real)` is `double precision`, not `real`.
    ("k2", "AVG(f4)"),
    ("k3", "AVG(f8)"),
    ("k4", "MIN(f4)"),
    ("k5", "MAX(f4)"),
    ("k6", "MIN(f8)"),
    ("k7", "MAX(f8)"),
];

/// Every float aggregate's derived column type equals Postgres's own,
/// including `avg(real) -> double precision`, which is easy to get wrong by
/// assuming `MIN`/`MAX`'s "keeps its argument's type" rule generalizes.
#[tokio::test]
async fn float_aggregate_column_types_match_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client.batch_execute(SOURCE_DDL).await.expect("source");
    seed_one_row(&client).await;

    let select_list = AGGREGATE_CASES
        .iter()
        .map(|(name, expr)| format!("{expr} AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    install_definition(
        &db.pool,
        &format!("TRANSFORM t FROM s GROUP BY i SELECT i AS i, {select_list}"),
        &source_columns(),
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool).await;

    for (name, expr) in AGGREGATE_CASES {
        let declared = column_pg_type(&client, "t", name).await;
        let expected = postgres_type_of(&client, &expr.to_lowercase()).await;
        assert_eq!(
            declared, expected,
            "Trellis declared `{expr}` as {declared}, Postgres types it as {expected}"
        );
    }
}

// ---------------------------------------------------------------------
// 3. Overflow, and the special values that are *not* overflow
// ---------------------------------------------------------------------

/// `(width, lhs, rhs)` pairs on and over each width's finite edge, plus the
/// `Infinity`/`NaN` operands that must *not* raise.
const BOUNDARY_CASES: &[(FloatWidth, &str, &str)] = &[
    (FloatWidth::Float4, "1e38", "1e38"),
    (FloatWidth::Float4, "3.4e38", "3.4e38"),
    (FloatWidth::Float4, "-3.4e38", "-3.4e38"),
    (FloatWidth::Float4, "Infinity", "1e38"),
    (FloatWidth::Float4, "NaN", "1"),
    (FloatWidth::Float8, "1e307", "1e307"),
    (FloatWidth::Float8, "1e308", "1e308"),
    (FloatWidth::Float8, "-1e308", "-1e308"),
    (FloatWidth::Float8, "Infinity", "1e308"),
    (FloatWidth::Float8, "Infinity", "-Infinity"),
    (FloatWidth::Float8, "NaN", "Infinity"),
];

/// Trellis raises exactly when Postgres raises, and produces exactly what
/// Postgres produces otherwise.
///
/// The interesting half is the *non*-raising cases: `'Infinity' + 1e38` is
/// `Infinity` (Postgres only errors when a finite pair overflows) and
/// `'Infinity' + '-Infinity'` is `NaN`, a value, not an error. An
/// implementation that treated every infinite result as `22003` would fail
/// four of the rows below.
///
/// The Postgres side is written independently (`select $1::<w> + $2::<w>`),
/// with the operands bound as text and cast server-side, so neither side
/// sees the other's answer.
#[tokio::test]
async fn float_addition_raises_exactly_where_postgres_raises() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let def = add_def();

    for (width, lhs, rhs) in BOUNDARY_CASES {
        let pg_name = width.pg_name();
        let columns = HashMap::from([
            ("a".to_string(), ValueType::Float(*width)),
            ("b".to_string(), ValueType::Float(*width)),
        ]);

        // The row image carries Postgres's *canonical* rendering of each
        // operand, which is what a real CDC decode would deliver — obtained
        // from the server rather than hand-spelled, so the decode step is
        // exercised against genuine `float4out`/`float8out` text.
        let (lhs_text, rhs_text): (String, String) = {
            let row = client
                .query_one(
                    &format!("select ($1::text::{pg_name})::text, ($2::text::{pg_name})::text"),
                    &[&lhs.to_string(), &rhs.to_string()],
                )
                .await
                .unwrap_or_else(|e| panic!("canonicalize {lhs}/{rhs} as {pg_name}: {e}"));
            (row.get(0), row.get(1))
        };
        let row: Row = HashMap::from([
            ("a".to_string(), Some(lhs_text)),
            ("b".to_string(), Some(rhs_text)),
        ]);

        let postgres = client
            .query_one(
                &format!("select ($1::text::{pg_name} + $2::text::{pg_name})::text"),
                &[&lhs.to_string(), &rhs.to_string()],
            )
            .await;
        let trellis = evaluate(&def, &row, &columns, &mut RegexCache::new());

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
                // Postgres's own wording, reproduced verbatim.
                assert_eq!(
                    db_err.message(),
                    "value out of range: overflow",
                    "{lhs} + {rhs} on {pg_name}: the oracle's wording changed"
                );
                assert!(
                    eval_err
                        .to_string()
                        .contains("value out of range: overflow"),
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

// ---------------------------------------------------------------------
// 4. Text rendering
// ---------------------------------------------------------------------

/// A grid chosen to straddle every boundary in `float4out`/`float8out`'s
/// notation rule: the fixed/scientific switchover on both sides and at both
/// widths (which differ — `FLT_DIG` is 6, `DBL_DIG` is 15), the
/// `0.0001`/`1e-05` lower switchover, subnormals, the finite ceiling,
/// signed zero, and a value whose shortest round-trip needs all 17 digits.
const RENDER_GRID: &[&str] = &[
    "0",
    "-0",
    "1",
    "-1",
    "0.1",
    "0.5",
    "0.0001",
    "0.00001",
    "1e-7",
    "100000",
    "999999",
    "1000000",
    "1e7",
    "1e14",
    "1e15",
    "1e16",
    "123456789012345",
    "1234567890123456",
    "3.14159",
    "-1.5e-10",
    "16777216",
    "16777217",
    "NaN",
    "Infinity",
    "-Infinity",
];

/// [`trellis::float::render`] is byte-identical to `float4out`/`float8out`.
///
/// The engine's own text is what travels through the staging ring and what a
/// `::text`-comparing self-check sees, so "close enough to re-parse" is not
/// the bar — byte equality is. Note the grid deliberately includes values
/// that render *differently at the two widths* (`16777217` is `1.6777216e+07`
/// as a `real` and `16777217` as a `double precision`), so a renderer that
/// ignored its width argument would fail here rather than pass by accident.
#[tokio::test]
async fn rendering_matches_postgres_over_a_value_grid() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for width in FloatWidth::ALL {
        let pg_name = width.pg_name();
        for spelling in RENDER_GRID {
            // The value, and Postgres's rendering of it, both straight from
            // the server. Some grid entries overflow `real`; those are
            // skipped for that width rather than asserted about here (the
            // out-of-range path is covered above).
            let Ok(row) = client
                .query_one(
                    &format!("select ($1::text::{pg_name})::text"),
                    &[&spelling.to_string()],
                )
                .await
            else {
                continue;
            };
            let expected: String = row.get(0);

            let value = trellis::float::parse(&expected, width).unwrap_or_else(|e| {
                panic!(
                    "{spelling} as {pg_name}: Postgres rendered {expected:?}, which Trellis \
                        refuses to parse: {e}"
                )
            });
            assert_eq!(
                trellis::float::render(value, width),
                expected,
                "{spelling} as {pg_name}"
            );
        }
    }
}

/// A float column's value decodes, renders and round-trips byte-identically
/// through the evaluator, exactly as the integer widths do — the claim that
/// lets a computed float survive the text-carried staging ring.
#[tokio::test]
async fn float_text_rendering_is_byte_identical_to_postgres_through_the_evaluator() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let def = identity_def();

    for width in FloatWidth::ALL {
        let pg_name = width.pg_name();
        for spelling in ["0", "-0", "0.1", "1e15", "NaN", "Infinity", "-Infinity"] {
            let expected: String = client
                .query_one(
                    &format!("select ($1::text::{pg_name})::text"),
                    &[&spelling.to_string()],
                )
                .await
                .unwrap_or_else(|e| panic!("{spelling} as {pg_name}: {e}"))
                .get(0);

            let columns = HashMap::from([("a".to_string(), ValueType::Float(width))]);
            let row: Row = HashMap::from([("a".to_string(), Some(expected.clone()))]);
            let values = evaluate(&def, &row, &columns, &mut RegexCache::new())
                .unwrap_or_else(|e| panic!("{spelling} as {pg_name}: {e}"));
            let got = values["out"].as_ref().expect("non-NULL");
            assert!(
                matches!(got, Value::Float(w, _) if *w == width),
                "{spelling} as {pg_name} decoded as {got:?}, not a Float of that width"
            );
            assert_eq!(
                got.to_string(),
                expected,
                "{spelling} as {pg_name}: Trellis renders {got}, Postgres renders {expected}"
            );
        }
    }
}

// ---------------------------------------------------------------------
// 5. Values, against an independently-written SQL query
// ---------------------------------------------------------------------

/// The backfilled float target equals what a hand-written `GROUP BY` query
/// computes over the same rows — values, not just types — including groups
/// containing `NaN` and `Infinity`.
///
/// Written as a symmetric difference (`EXCEPT` both ways, counted) so a
/// missing group, an extra group and a wrong value all surface the same way.
/// `EXCEPT` uses `NOT DISTINCT` semantics, which for floats means Postgres's
/// own equality — so `NaN` on both sides matches, which is exactly the
/// behaviour under test.
#[tokio::test]
async fn a_float_aggregate_target_matches_a_hand_written_sql_query() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client.batch_execute(SOURCE_DDL).await.expect("source");
    client
        .batch_execute(
            "insert into s (id, f4, f8, n, i) values \
               (1,  1.5,  1.5,  1, 1), \
               (2,  2.5,  2.5,  2, 1), \
               (3, -1.5, 'Infinity', 3, 2), \
               (4,  0.25, 4.5,  4, 2), \
               (5, 'NaN', 5.5,  5, 3), \
               (6,  1.0,  6.5,  6, 3), \
               (7,  null, null, 7, 4)",
        )
        .await
        .expect("seed rows");

    install_definition(
        &db.pool,
        "TRANSFORM t FROM s GROUP BY i \
         SELECT i AS i, SUM(f4) AS total4, SUM(f8) AS total8, MIN(f4) AS smallest, \
                MAX(f8) AS biggest, AVG(f4) AS mean",
        &source_columns(),
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool).await;

    let differences: i64 = client
        .query_one(
            "with expected as ( \
                 select i, sum(f4) as total4, sum(f8) as total8, min(f4) as smallest, \
                        max(f8) as biggest, avg(f4) as mean \
                 from s group by i \
             ), \
             actual as (select i, total4, total8, smallest, biggest, mean from t) \
             select (select count(*) from (table expected except table actual) missing) \
                  + (select count(*) from (table actual except table expected) extra)",
            &[],
        )
        .await
        .expect("difference query")
        .get(0);
    assert_eq!(differences, 0, "the target must equal the SQL oracle");

    // The columns really are floats, not `numeric` values that happen to
    // render the same — and `AVG` really did widen where Postgres widens.
    assert_eq!(column_pg_type(&client, "t", "total4").await, "real");
    assert_eq!(
        column_pg_type(&client, "t", "total8").await,
        "double precision"
    );
    assert_eq!(column_pg_type(&client, "t", "smallest").await, "real");
    assert_eq!(
        column_pg_type(&client, "t", "biggest").await,
        "double precision"
    );
    assert_eq!(
        column_pg_type(&client, "t", "mean").await,
        "double precision"
    );

    // The `NaN` group really is `NaN` on both sides — the assertion above
    // would also pass if the group had vanished from *both*.
    let nan_group: String = client
        .query_one("select total4::text from t where i = 3", &[])
        .await
        .expect("the NaN-containing group must exist")
        .get(0);
    assert_eq!(
        nan_group, "NaN",
        "a group containing NaN must sum to NaN, as Postgres does"
    );
}

/// `MIN`/`MAX` over a group containing `NaN` follow Postgres's order, not
/// IEEE's: the max is `NaN` and the min is the smallest ordinary value.
///
/// Checked against the server's own `min()`/`max()` on the same rows rather
/// than against a hardcoded expectation.
#[tokio::test]
async fn min_and_max_over_nan_follow_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client.batch_execute(SOURCE_DDL).await.expect("source");
    client
        .batch_execute(
            "insert into s (id, f8, i) values \
               (1, 'NaN', 1), (2, 1, 1), (3, 'Infinity', 1), (4, '-Infinity', 1)",
        )
        .await
        .expect("seed rows");

    install_definition(
        &db.pool,
        "TRANSFORM t FROM s GROUP BY i SELECT i AS i, MIN(f8) AS lo, MAX(f8) AS hi",
        &source_columns(),
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool).await;

    let row = client
        .query_one(
            "select t.lo::text, t.hi::text, \
                    (select min(f8) from s)::text, (select max(f8) from s)::text \
             from t",
            &[],
        )
        .await
        .expect("read back");
    let (lo, hi, pg_lo, pg_hi): (String, String, String, String) =
        (row.get(0), row.get(1), row.get(2), row.get(3));
    assert_eq!(lo, pg_lo);
    assert_eq!(hi, pg_hi);
    // And Postgres's answer really is the non-IEEE one.
    assert_eq!(pg_hi, "NaN", "NaN sorts highest, so it is the max");
    assert_eq!(pg_lo, "-Infinity");
}

/// The **Rust evaluator's own** float aggregate fold agrees with Postgres,
/// value for value and byte for byte.
///
/// Found in review of #112: `eval::fold_aggregate` and
/// `eval::eval_to_many_aggregate` both filtered their collected values with
/// `ValueType::is_exact_numeric_family()`, which by construction excludes
/// `ValueType::Float`. Every float row was therefore silently dropped, the
/// fold saw an empty group, and `SUM`/`MIN`/`MAX`/`AVG` over a float column
/// returned `NULL` — with the entire float branch of
/// `reduce_numeric_aggregate` unreachable. The live-pipeline aggregate tests
/// could not catch it: a float aggregate is `RecomputeOnly`, so the apply
/// path probes a server-side `sum(...)` and never folds in Rust. This test
/// drives the fold directly, which is what `defs::oracle::recompute_aggregate`
/// (the ADR-0013 cross-check oracle) and the to-many relationship path do.
///
/// The `-0`-only group is the second half of the regression: Postgres's
/// `sum(float8)` has a `NULL` initial condition, so `sum` over a group of
/// `-0`s is `-0`, not the `+0` a zero-seeded accumulator produces. Both
/// render, and `-0` is the one Postgres writes.
#[tokio::test]
async fn the_evaluator_fold_agrees_with_postgres_on_float_aggregates() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // `(group label, the f8 values in that group)` — chosen to cover an
    // ordinary group, a `NaN` group (absorbing), an `Infinity` group, and
    // the signed-zero group whose Postgres `sum` is `-0`.
    const GROUPS: &[(&str, &[&str])] = &[
        ("plain", &["1.5", "2.5", "3.5"]),
        ("nan", &["1", "NaN", "2"]),
        ("inf", &["Infinity", "1"]),
        ("negzero", &["-0", "-0"]),
        ("mixedzero", &["0", "-0"]),
        // 16 orders of magnitude apart, so reassociation is visible in the
        // last bits. Spelled in canonical `float8out` form, which is what a
        // column's text always is.
        ("spread", &["1e+16", "1", "1", "1"]),
    ];

    for (label, values) in GROUPS {
        for agg in ["SUM", "MIN", "MAX", "AVG"] {
            // The SQL oracle: the same values, the same aggregate, rendered
            // by Postgres itself.
            let rows_sql = values
                .iter()
                .map(|v| format!("('{v}'::float8)"))
                .collect::<Vec<_>>()
                .join(", ");
            let expected: Option<String> = client
                .query_one(
                    &format!(
                        "select {}(v)::text from (values {rows_sql}) t(v)",
                        agg.to_lowercase()
                    ),
                    &[],
                )
                .await
                .unwrap_or_else(|e| panic!("{label}/{agg}: {e}"))
                .get(0);

            // Trellis's evaluator fold over the same values.
            let def = TransformDef {
                target: "t".to_string(),
                explicit_target_schema: None,
                source: "s".to_string(),
                explicit_source_schema: None,
                key_space: KeySpace::Aggregate {
                    group_by: vec![GroupByKey::Column("i".to_string())],
                },
                fields: vec![FieldDef {
                    name: "out".to_string(),
                    expr: Expr::FunctionCall {
                        name: agg.to_string(),
                        args: vec![Expr::Column("f8".to_string())],
                    },
                }],
                predicate: Predicate::True,
            };
            let columns = HashMap::from([
                ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
                ("f8".to_string(), ValueType::Float(FloatWidth::Float8)),
            ]);
            let rows: Vec<Row> = values
                .iter()
                .map(|v| {
                    HashMap::from([
                        ("i".to_string(), Some("1".to_string())),
                        ("f8".to_string(), Some(v.to_string())),
                    ])
                })
                .collect();
            let got = evaluate_aggregate(&def, &rows, &columns, &mut RegexCache::new())
                .unwrap_or_else(|e| panic!("{label}/{agg}: {e}"));
            let got = got["out"].as_ref().map(|v| v.to_string());

            assert_eq!(
                got, expected,
                "{label}/{agg}: Trellis's fold and Postgres must agree byte for byte"
            );
            assert!(
                got.is_some(),
                "{label}/{agg}: a non-empty float group must not fold to NULL"
            );
        }
    }
}

// ---------------------------------------------------------------------
// 6. Key roles: deliberately refused, and why
// ---------------------------------------------------------------------

/// `-0` and `0` are one value with two text renderings — demonstrated from
/// the server, since it is the entire reason floats are refused as keys.
///
/// Postgres groups them together (one group, count 2), but their `::text`
/// renderings differ, so the engine's raw-`::text` key matching would split
/// that one group into two target rows. This is the same text-instability
/// bar `numeric` already fails (`1.0` vs `1.00`).
#[tokio::test]
async fn signed_zero_is_why_float_text_keys_are_unsafe() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let row = client
        .query_one(
            "select (select count(*) from ( \
                        select v from (values (-0.0::float8), (0.0::float8)) t(v) group by v \
                     ) g), \
                    (-0.0::float8)::text, (0.0::float8)::text",
            &[],
        )
        .await
        .expect("the signed-zero demonstration");
    let (groups, neg_zero_text, zero_text): (i64, String, String) =
        (row.get(0), row.get(1), row.get(2));

    assert_eq!(groups, 1, "Postgres groups -0 and 0 together");
    assert_ne!(
        neg_zero_text, zero_text,
        "...but renders them differently, which is what breaks a ::text-keyed group"
    );
}

/// `real`/`double precision` are refused as relationship join keys, through
/// the same `catalog::TEXT_STABLE_JOIN_KEY_TYPES` allowlist that gates a 1-1
/// transform's **primary key** (issue #107's "close the gap" fix) — so this
/// is one assertion about both roles.
///
/// The `bigint` case is the control: it must still be accepted, so this is a
/// claim about float text specifically and not about the gate being broken.
///
/// **This is #112's primary-key decision.** Postgres itself would be fine
/// with a float PK — its float btree opclass is a total order, `NaN` and
/// `±0` included — so the refusal is not a statement about the type. It is
/// about *this* engine's current key scheme, which matches keys by raw
/// `::text` and therefore cannot represent `-0 = 0` (see
/// `signed_zero_is_why_float_text_keys_are_unsafe` just above). Admitting
/// floats as keys is #110's typed key index to grant, by comparing decoded
/// values through `float::compare`.
#[tokio::test]
async fn a_float_relationship_join_key_is_rejected_by_name() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent4 (k real primary key); \
             create table parent8 (k double precision primary key); \
             create table parent_ok (k bigint primary key); \
             create table child (id bigint primary key, f4 real, f8 double precision, ok bigint); \
             alter table child replica identity full; \
             alter table parent4 replica identity full; \
             alter table parent8 replica identity full; \
             alter table parent_ok replica identity full",
        )
        .await
        .expect("create relationship tables");

    for (name, from_col, to_table, pg_name) in [
        ("r4", "f4", "parent4", "real"),
        ("r8", "f8", "parent8", "double precision"),
    ] {
        let err = create_relationship(
            &db.pool,
            &format!("RELATIONSHIP {name} FROM child.{from_col} TO {to_table}.k"),
        )
        .await
        .expect_err("a float join key must be rejected");
        assert!(
            err.to_string().contains(pg_name),
            "the rejection must name the offending type ({pg_name}): {err}"
        );
    }

    // The control: the gate isn't simply rejecting everything.
    create_relationship(&db.pool, "RELATIONSHIP r_ok FROM child.ok TO parent_ok.k")
        .await
        .expect("a bigint join key must still be accepted");
}

/// A float source column is refused as a **`GROUP BY` key**, with an error
/// that names the column.
///
/// This is a deliberate tightening relative to before #112: `real` used to
/// reach this gate classified as `ValueType::Numeric` and was waved through.
/// A `::text`-matched float group key is not correct (see the signed-zero
/// test above), so refusing is the honest answer until #110.
#[test]
fn a_float_group_by_key_is_rejected() {
    for column in ["f4", "f8"] {
        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY {column} SELECT {column} AS k, SUM(n) AS total"
        ))
        .expect("parses");
        let err = validate(&def, &source_columns(), &HashMap::new())
            .expect_err("a float GROUP BY key must be rejected");
        assert!(
            err.to_string().contains(column),
            "the rejection must name the column: {err}"
        );
    }

    // The control: a `numeric` key is still accepted (knowingly weaker, and
    // #110's to tighten), so this is a claim about floats specifically.
    let def =
        parse("TRANSFORM t FROM s GROUP BY n SELECT n AS k, SUM(n) AS total").expect("parses");
    validate(&def, &source_columns(), &HashMap::new()).expect("a numeric GROUP BY key still works");
}

// ---------------------------------------------------------------------
// 7. Literals
// ---------------------------------------------------------------------

/// A float constant is spellable only through the typed-literal grammar,
/// because Postgres gives floats no bare literal syntax at all — and the
/// literal text must be in canonical `float4out`/`float8out` form.
///
/// The bare-literal half is verified against the server: `pg_typeof(1.5)` is
/// `numeric`, which is why `REAL '1.5'` has to exist rather than being a
/// second spelling of something already spellable.
#[tokio::test]
async fn a_float_literal_needs_the_typed_literal_grammar_and_must_be_canonical() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let bare: String = client
        .query_one("select format_type(pg_typeof(1.5), null)", &[])
        .await
        .expect("pg_typeof(1.5)")
        .get(0);
    assert_eq!(
        bare, "numeric",
        "an unadorned decimal literal is `numeric` in Postgres, which is the whole reason \
         floats need a typed-literal row"
    );

    for good in [
        "REAL '1.5'",
        "REAL 'NaN'",
        "REAL 'Infinity'",
        "REAL '-Infinity'",
        "DOUBLE PRECISION '1.5'",
        "DOUBLE PRECISION '1e+30'",
        "CAST('1.5' AS real)",
        "CAST('0.30000000000000004' AS double precision)",
    ] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {good} AS x"))
            .unwrap_or_else(|e| panic!("{good} should parse: {e}"));
        validate(&def, &source_columns(), &HashMap::new())
            .unwrap_or_else(|e| panic!("{good} should validate: {e}"));

        // Postgres parses the same spelling to the same type.
        let pg: String = client
            .query_one(&format!("select format_type(pg_typeof({good}), null)"), &[])
            .await
            .unwrap_or_else(|e| panic!("{good} must be valid Postgres too: {e}"))
            .get(0);
        assert!(
            pg == "real" || pg == "double precision",
            "{good} types as {pg} in Postgres"
        );
    }

    for bad in [
        // Spellings `float8in` accepts but `float8out` never emits, so they
        // would not round-trip through the text-carried value path.
        "REAL '+1.5'",
        "REAL '1.50'",
        "REAL 'inf'",
        "REAL 'nan'",
        "DOUBLE PRECISION '1E+30'",
        "DOUBLE PRECISION '1.5e1'",
        "DOUBLE PRECISION 'abc'",
        // Out of `real`'s range entirely.
        "REAL '1e+300'",
    ] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {bad} AS x"))
            .unwrap_or_else(|e| panic!("{bad} should parse (validate-time error): {e}"));
        let err = validate(&def, &source_columns(), &HashMap::new())
            .expect_err("a non-canonical float literal must be rejected");
        assert!(
            err.to_string().contains("not in canonical form"),
            "{bad}: {err}"
        );
    }
}

/// `DOUBLE PRECISION` is the grammar's only two-word type keyword, and both
/// spellings of a typed literal must handle it — including case-insensitively
/// and without a column named `double` being shadowed.
#[test]
fn the_two_word_double_precision_keyword_parses_in_both_spellings() {
    let typed = parse("TRANSFORM t FROM s SELECT double precision '1.5' AS x").expect("parses");
    let cast =
        parse("TRANSFORM t FROM s SELECT CAST('1.5' AS DOUBLE PRECISION) AS x").expect("parses");
    assert_eq!(typed.fields[0].expr, cast.fields[0].expr);
    assert_eq!(
        typed.fields[0].expr,
        Expr::TypedLiteral {
            value_type: ValueType::Float(FloatWidth::Float8),
            text: "1.5".to_string(),
        }
    );

    // A column named `double` is still a column — the typed-literal
    // lookahead only fires when a string literal follows.
    let def = parse("TRANSFORM t FROM s SELECT double AS d").expect("parses");
    assert_eq!(def.fields[0].expr, Expr::Column("double".to_string()));
}

// ---------------------------------------------------------------------
// 8. The incremental path, live
// ---------------------------------------------------------------------

/// The **incremental** aggregate path keeps a float `SUM`/`AVG` column equal
/// to a hand-written `GROUP BY` across inserts, updates and deletes.
///
/// This is the claim the backfill test above cannot make, and it is the one
/// the recompute-vs-delta decision is *about*: `defs::invertibility`
/// classifies a float `SUM`/`AVG` as recompute-only, so both
/// `defs::backfill::classify_field` and
/// `staging::apply_aggregate::classify_fields` must route these fields to
/// the probe-assisted recompute path rather than the delta path. If either
/// still passed a hardcoded `ValueType::Numeric` to the gate — as both did
/// before #112 — the workload below would leave a group whose running sum
/// had drifted, or one poisoned by a deleted `NaN` row that subtraction
/// cannot undo.
///
/// Run through the public `Trellis` facade against the full live pipeline,
/// with convergence awaited via `watermark_token`/`await_converged`. The
/// comparison is a symmetric difference against independently-written SQL,
/// per ADR-0013.
#[tokio::test]
async fn an_incremental_float_aggregate_stays_equal_to_a_hand_written_group_by() {
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
            "TRANSFORM t FROM s GROUP BY i \
             SELECT i AS i, SUM(f8) AS total, AVG(f8) AS mean, MAX(f4) AS biggest",
        )
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown definer");

    // The columns really are floats — otherwise this test would be
    // exercising the same `numeric` path everything else does.
    assert_eq!(column_pg_type(&raw, "t", "total").await, "double precision");
    assert_eq!(column_pg_type(&raw, "t", "mean").await, "double precision");
    assert_eq!(column_pg_type(&raw, "t", "biggest").await, "real");

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

    // A workload built specifically to break a delta-maintained float sum:
    // a group whose magnitudes differ by 16 orders of magnitude (so
    // reassociation is visible in the last bits), a `NaN` row that is later
    // *deleted* (irrecoverable by subtraction), rows moving between groups,
    // and a group emptied entirely.
    for sql in [
        "insert into s (id, f4, f8, i) values \
           (1, 1.5, 1e16, 1), (2, 2.5, 1, 1), (3, 3.5, 1, 1), \
           (4, 1.0, 2.5, 2), (5, 2.0, 'NaN', 2), \
           (6, 9.0, 4.5, 3)",
        "update s set f8 = 1 where id = 1",
        "delete from s where id = 5",
        "update s set i = 3 where id = 4",
        "insert into s (id, f4, f8, i) values (7, 0.5, 1e16, 1)",
        "delete from s where id = 6",
        "update s set i = null where id = 2",
        // Group 4: a `NaN` row deleted from a group that *survives* the
        // delete. This is the case a delta path cannot undo — subtracting
        // `NaN` leaves `NaN` — and it has to be a surviving group, because a
        // group emptied entirely is deleted outright and agrees with the
        // oracle either way.
        "insert into s (id, f4, f8, i) values (8, 1.0, 'NaN', 4), (9, 2.0, 7.5, 4)",
        "delete from s where id = 8",
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
                 select i, sum(f8) as total, avg(f8) as mean, max(f4) as biggest \
                 from s group by i \
             ), \
             actual as (select i, total, mean, biggest from t) \
             select (select count(*) from (table expected except table actual) missing) \
                  + (select count(*) from (table actual except table expected) extra)",
            &[],
        )
        .await
        .expect("difference query")
        .get(0);
    assert_eq!(
        differences, 0,
        "the incrementally-maintained float target must equal the SQL oracle"
    );

    // And the workload really did delete the only NaN row, so no group is
    // left holding one — the assertion above would pass trivially if a
    // delta path had poisoned every group with NaN, since it would then
    // differ from the oracle, but this pins that the *oracle* has none.
    let nan_groups: i64 = raw
        .query_one("select count(*) from t where total = 'NaN'::float8", &[])
        .await
        .expect("count NaN groups")
        .get(0);
    assert_eq!(
        nan_groups, 0,
        "the NaN row was deleted, so no group may still sum to NaN — a delta path \
         subtracting NaN would leave one here"
    );

    trellis.shutdown().await.expect("shutdown");
}
