//! End-to-end tests for the binary float types (`real`/`double
//! precision`) against a real, ephemeral Postgres via
//! `testkit::TestCluster`.
//!
//! # Why these run against a live server
//!
//! ADR-0004 makes Postgres the correctness oracle for this grammar, and
//! `docs/type-support.md` parked this family on "IEEE edge cases need a
//! decision". The decision is *not* to invent semantics: it is to
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
//! 5. **Key roles.** `real`/`double precision` are rejected as `JOIN` `ON`
//!    keys, primary keys and `GROUP BY` keys — and the reason is
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

use testkit::TestCluster;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::eval::{RegexCache, Row, Value, evaluate};
use trellis::defs::{CatalogError, ValidationError, install_definition, parse, validate};
use trellis::float::FloatWidth;
use trellis::integer::IntWidth;

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
   )";

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

/// Defines a `JOIN` whose one `ON` term pairs `left.lcol` with
/// `right.rcol`. `JOIN` key admission reads the same allowlist
/// (`join_key_types::is_text_stable_join_key_type`) a 1-1 primary key does.
async fn define_join_on(
    pool: &trellis::pool::Pool,
    target: &str,
    (left, lcol): (&str, &str),
    (right, rcol): (&str, &str),
) -> Result<(), trellis::defs::CatalogError> {
    let text = format!(
        "TRANSFORM {target} FROM {left} JOIN {right} ON {left}.{lcol} = {right}.{rcol} \
         GROUP BY {left}.id WITHOUT SOURCE INDEX SELECT COUNT(*) AS n"
    );
    install_definition(pool, &text, &HashMap::new(), "public")
        .await
        .map(|_| ())
}

/// Starts every registered definition's build and runs it to completion,
/// standing in for the staging worker and a running drain worker (copied
/// from `defs_exact_integers.rs`).
async fn drain_backfill_chunks(pool: &trellis::Pool) {
    // ADR-0002: registration only records a definition; the staging
    // worker starts its build.
    trellis::intake::harness::start_registered_builds(pool)
        .await
        .expect("dispatch registered definitions' builds");
    trellis::staging::build::settle_builds(pool).await;
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
        without_source_index: false,
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
/// This is the central question for floats and it is **not** a free design choice.
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
    // Typed float literals.
    ("h10", "REAL '1.5' + f4"),
    ("h11", "DOUBLE PRECISION '1.5' + f8"),
];

/// The type of every derived float column Trellis declares equals the type
/// Postgres gives the same expression.
///
/// Every one of these is a float type, never `numeric`, including the two
/// that Postgres types as `real`.
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

/// A float against a *bare* number literal compares as Postgres does,
/// in `double precision`: the literal is cast to `float8` and a `real`
/// operand widens to it, so `0.1::real > 0.1` is true (the widened real is
/// 0.10000000149...). Every verdict below is read from the server, over rows
/// that include `NULL`, `NaN`, `-0`, a subnormal and a `real` that rounded,
/// and the evaluator must agree; narrowing the literal to `f32` flips the
/// `r` rows. A literal is refused at define exactly where the server raises
/// `22003` for it.
#[tokio::test]
async fn a_float_against_a_bare_literal_compares_in_double_precision_like_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let columns = HashMap::from([
        ("r".to_string(), ValueType::Float(FloatWidth::Float4)),
        ("d".to_string(), ValueType::Float(FloatWidth::Float8)),
    ]);
    let f64_max = format!("{:.0}", f64::MAX);
    let subnormal = |digits: &str| format!("0.{}{digits}", "0".repeat(323));
    let mut exprs: Vec<String> = [
        "r > 0.1",
        "0.1 > r",
        "d > 0.1",
        "0.1 > d",
        "r > (0.1)",
        "r > 0.1000000014",
        "r > 0.1000000015",
        "0.1000000015 > r",
        "r > 0",
        "0 > r",
        "r > 0.0000",
        "r > 1",
        "r > 16777216",
        "16777216.5 > r",
        "d > 0.09999999999999999",
        "d > 0.10000000000000001",
        "d > 0.3",
        "0.30000000000000004 > d",
        "d > 3000000000",
        "3000000000 > d",
        "r > 9223372036854775808",
        "COALESCE(r, 0) > 0.1",
        "COALESCE(d, 0.1) > 0.1",
        "r + 1 > 1.1",
        "r > r",
        "r > d",
        "d > r",
    ]
    .iter()
    .map(|e| e.to_string())
    .collect();
    exprs.push(format!("{f64_max} > d"));
    exprs.push(format!("d > {}", subnormal("5")));
    exprs.push(format!("r > {}", subnormal("3")));

    for (r, d) in [
        ("0.1", "0.1"),
        ("16777217", "0.30000000000000004"),
        ("'NaN'", "'Infinity'"),
        ("'-0'", "'4.9406564584124654e-324'"),
        ("null", "null"),
    ] {
        let source = format!("select {r}::real as r, {d}::float8 as d");
        let texts = client
            .query_one(&format!("select r::text, d::text from ({source}) t"), &[])
            .await
            .expect("row text");
        let row: Row = HashMap::from([
            ("r".to_string(), texts.get(0)),
            ("d".to_string(), texts.get(1)),
        ]);
        for expr in &exprs {
            let def = parse(&format!("TRANSFORM t FROM s SELECT {expr} AS out")).expect("parse");
            assert_eq!(validate(&def, &columns), Ok(()), "{expr}");
            let oracle: Option<bool> = client
                .query_one(&format!("select {expr} from ({source}) t"), &[])
                .await
                .unwrap_or_else(|e| panic!("{expr}: {e}"))
                .get(0);
            let values = evaluate(&def, &row, &columns, &mut RegexCache::new())
                .unwrap_or_else(|e| panic!("{expr}: {e}"));
            assert_eq!(
                values["out"],
                oracle.map(Value::Boolean),
                "{expr} over r = {r}, d = {d}: Postgres says {oracle:?}"
            );
        }
    }

    // Range: refused at define exactly where the server raises 22003 when
    // it casts the literal to `double precision` (past the finite ceiling,
    // or a nonzero value that rounds to zero; a subnormal is in range).
    for literal in [
        f64_max.clone(),
        format!("{f64_max}.5"),
        format!("17976931348623158{}", "0".repeat(292)),
        "9".repeat(309),
        subnormal("24703282292062328"),
        subnormal("24703282292062327"),
        subnormal("5"),
        format!("0.{}1", "0".repeat(400)),
    ] {
        let expr = format!("d > {literal}");
        let def = parse(&format!("TRANSFORM t FROM s SELECT {expr} AS out")).expect("parse");
        let refused = matches!(
            validate(&def, &columns),
            Err(ValidationError::FloatComparisonLiteralOutOfRange { .. })
        );
        let server = client
            .query_one(
                &format!("select {expr} from (select 1::float8 as d) t"),
                &[],
            )
            .await;
        if let Err(e) = &server {
            assert_eq!(e.code(), Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE), "{e}");
        }
        assert_eq!(refused, server.is_err(), "{}...", &literal[..40]);
    }

    // The headline pair, stated outright.
    let headline = client
        .query_one("select 0.1::real > 0.1, 0.1::real > 0.1::real", &[])
        .await
        .expect("headline");
    assert!(headline.get::<_, bool>(0));
    assert!(!headline.get::<_, bool>(1));
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

/// `real`/`double precision` are refused as `JOIN` `ON` keys, through
/// the same `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` allowlist that gates a 1-1
/// transform's **primary key** — so this
/// is one assertion about both roles.
///
/// The `bigint` case is the control: it must still be accepted, so this is a
/// claim about float text specifically and not about the gate being broken.
///
/// **This is the primary-key decision for floats.** Postgres itself would be fine
/// with a float PK — its float btree opclass is a total order, `NaN` and
/// `±0` included — so the refusal is not a statement about the type. It is
/// about *this* engine's current key scheme, which matches keys by raw
/// `::text` and therefore cannot represent `-0 = 0` (see
/// `signed_zero_is_why_float_text_keys_are_unsafe` just above). Admitting
/// floats as keys would need a typed key index that compares decoded
/// values through `float::compare`.
#[tokio::test]
async fn a_float_join_key_is_rejected_by_name() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent4 (k real primary key); \
             create table parent8 (k double precision primary key); \
             create table parent_ok (k bigint primary key); \
             create table child (id bigint primary key, f4 real, f8 double precision, ok bigint)",
        )
        .await
        .expect("create join tables");

    for (name, from_col, to_table, pg_name) in [
        ("j4", "f4", "parent4", "real"),
        ("j8", "f8", "parent8", "double precision"),
    ] {
        let err = define_join_on(&db.pool, name, ("child", from_col), (to_table, "k"))
            .await
            .expect_err("a float join key must be rejected");
        assert!(
            matches!(
                &err,
                CatalogError::Validate(ValidationError::JoinUnsupportedKeyType { pg_type, .. })
                    if pg_type == pg_name
            ),
            "the rejection must name the offending type ({pg_name}): {err:?}"
        );
    }

    // The control: the gate isn't simply rejecting everything.
    define_join_on(&db.pool, "j_ok", ("child", "ok"), ("parent_ok", "k"))
        .await
        .expect("a bigint join key must still be accepted");
}

/// A float source column is refused as a **`GROUP BY` key**, with an error
/// that names the column.
///
/// `real` is classified as a float, not `ValueType::Numeric`, so it does not
/// get waved through. A `::text`-matched float group key is not correct (see
/// the signed-zero test above), so refusing is the honest answer.
#[test]
fn a_float_group_by_key_is_rejected() {
    for column in ["f4", "f8"] {
        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY {column} WITHOUT SOURCE INDEX SELECT {column} AS k, SUM(n) AS total"
        ))
        .expect("parses");
        let err =
            validate(&def, &source_columns()).expect_err("a float GROUP BY key must be rejected");
        assert!(
            err.to_string().contains(column),
            "the rejection must name the column: {err}"
        );
    }

    // The control: a `numeric` key is still accepted (knowingly weaker),
    // so this is a claim about floats specifically.
    let def =
        parse("TRANSFORM t FROM s GROUP BY n WITHOUT SOURCE INDEX SELECT n AS k, SUM(n) AS total")
            .expect("parses");
    validate(&def, &source_columns()).expect("a numeric GROUP BY key still works");
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
        validate(&def, &source_columns()).unwrap_or_else(|e| panic!("{good} should validate: {e}"));

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
        let err = validate(&def, &source_columns())
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
