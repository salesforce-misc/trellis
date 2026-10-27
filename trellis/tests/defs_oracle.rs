//! Integration tests for the SQL renderers the tests compare targets with
//! ([`render_one_to_one_select_sql`], [`render_aggregate_select_sql`],
//! [`render_expr_sql`]), run against a real, ephemeral Postgres instance via
//! `testkit::TestCluster`. Each rendering is compared with an independently
//! written query over the same rows, so a renderer bug (a lost cross-field
//! alias, a wrong cast) is caught here rather than hiding in a comparison
//! that uses the renderer on both sides.

#[path = "support/render_lab.rs"]
mod render_lab;

use render_lab::{Lab, Outcome};
use testkit::TestCluster;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{render_aggregate_select_sql, render_expr_sql, render_one_to_one_select_sql};
use trellis::integer::IntWidth;

fn order_totals_def() -> TransformDef {
    TransformDef {
        target: "order_totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "double_price".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("price".to_string())),
                    rhs: Box::new(Expr::Column("price".to_string())),
                },
            },
            FieldDef {
                name: "total".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("double_price".to_string())),
                    rhs: Box::new(Expr::Column("tax".to_string())),
                },
            },
        ],
        predicate: Predicate::True,
        without_source_index: false,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

fn order_totals_aggregate_def() -> TransformDef {
    TransformDef {
        target: "order_totals".to_string(),
        source: "order_line_items".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec!["order_id".to_string()],
        },
        fields: vec![
            FieldDef {
                name: "order_id".to_string(),
                expr: Expr::Column("order_id".to_string()),
            },
            FieldDef {
                name: "total_amount".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
            FieldDef {
                name: "avg_amount".to_string(),
                expr: Expr::FunctionCall {
                    name: "AVG".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
            FieldDef {
                name: "count_amount".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
            FieldDef {
                name: "count_rows".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: Vec::new(),
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
        without_source_index: false,
    }
}

/// Counts the rows of `left EXCEPT right` and `right EXCEPT left`: a missing
/// row, an extra row and a wrong value all show up as a nonzero count.
async fn symmetric_difference(client: &tokio_postgres::Client, left: &str, right: &str) -> i64 {
    let sql = format!(
        "select (select count(*) from (({left}) except ({right})) a) \
              + (select count(*) from (({right}) except ({left})) b)"
    );
    client
        .query_one(sql.as_str(), &[])
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
        .get(0)
}

/// The 1-1 rendering, including `total`'s read of the sibling field
/// `double_price`, equals a hand-written `SELECT` over the same rows. The
/// alias must be substituted: Postgres cannot read a same-`SELECT`-list name.
#[tokio::test]
async fn the_one_to_one_rendering_equals_a_hand_written_select() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric);
             insert into orders (id, price, tax) values
                 (1, 10.00, 1.50),
                 (2, 0.00, 0.00),
                 (3, -5.25, 2.00),
                 (4, null, 1.00)",
        )
        .await
        .expect("seed source table");

    let rendered = render_one_to_one_select_sql(&order_totals_def(), "id");
    let hand = "select id::text, (price + price)::text, (price + price + tax)::text from orders";
    assert_eq!(symmetric_difference(&client, &rendered, hand).await, 0);
    let rows: i64 = client
        .query_one(&format!("select count(*) from ({rendered}) t"), &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(rows, 4, "every source row is rendered");
}

/// Cross-checks `strpos(name, 'foo') > 0` (a function call composed with the
/// `>` comparison operator): the real apply path's value for each name equals
/// Postgres running the rendered expression, and the rendering is valid SQL.
#[tokio::test]
async fn function_call_composed_with_greater_than_matches_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut lab = Lab::new(
        &db,
        "name text",
        "TRANSFORM t FROM s SELECT STRPOS(name, 'foo') > 0 AS has_foo",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("name", ValueType::Text),
        ],
    )
    .await;

    let expr = Expr::BinaryOp {
        op: Operator::GreaterThan,
        lhs: Box::new(Expr::FunctionCall {
            name: "STRPOS".to_string(),
            args: vec![
                Expr::Column("name".to_string()),
                Expr::StringLiteral("foo".to_string()),
            ],
        }),
        rhs: Box::new(Expr::NumberLiteral("0".to_string())),
    };
    let sql = render_expr_sql(&expr);

    for name in ["has foo in it", "no match here", ""] {
        let expected: bool = lab
            .client()
            .query_one(
                &format!("select ({sql})::boolean from (select $1::text as name) t"),
                &[&name],
            )
            .await
            .expect("query postgres")
            .get(0);
        assert_eq!(
            lab.apply(&[("name", Some(name))]).await,
            Outcome::Values(vec![Some(expected.to_string())]),
            "mismatch for name = {name:?}"
        );
    }
}

async fn seed_line_items(client: &tokio_postgres::Client, rows: &str) {
    client
        .batch_execute(&format!(
            "create table order_line_items (
                 id integer primary key, order_id integer, amount numeric
             );
             insert into order_line_items (id, order_id, amount) values {rows}"
        ))
        .await
        .expect("seed source table");
}

/// The aggregate rendering equals a hand-written `GROUP BY`, covering a
/// single-row group, a group with a negative value, and a group whose `AVG`
/// is a non-terminating decimal (`55 / 3`).
#[tokio::test]
async fn the_aggregate_rendering_equals_a_hand_written_group_by() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");
    seed_line_items(
        &client,
        "(1, 1, 10.00), (2, 2, -5.25), (3, 2, 10.00), (4, 3, 10.00), (5, 3, 20.00), (6, 3, 25.00)",
    )
    .await;

    let base_sql = render_aggregate_select_sql(&order_totals_aggregate_def());
    let rendered = format!(
        "select order_id::text, total_amount::text, avg_amount::text, \
         count_amount::text, count_rows::text from ({base_sql}) t"
    );
    let hand = "select order_id::text, sum(amount)::text, avg(amount)::text, \
                count(amount)::text, count(*)::text from order_line_items group by order_id";
    assert_eq!(symmetric_difference(&client, &rendered, hand).await, 0);
    let groups: i64 = client
        .query_one(&format!("select count(*) from ({rendered}) t"), &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(groups, 3);
}

/// A `GROUP BY` field referencing another calculated field by name
/// (`double_total = total + total`, where `total = SUM(amount)`) is
/// substituted: `render_aggregate_select_sql` must not render
/// `double_total`'s raw `Expr::Column("total")` as a bare SQL identifier,
/// which Postgres rejects since `total` names neither a source column nor a
/// same-SELECT-list-visible name.
#[tokio::test]
async fn the_aggregate_rendering_substitutes_a_cross_field_alias() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");
    seed_line_items(&client, "(1, 1, 10.00), (2, 2, -5.25), (3, 2, 10.00)").await;

    let def = TransformDef {
        target: "order_alias_totals".to_string(),
        source: "order_line_items".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec!["order_id".to_string()],
        },
        fields: vec![
            FieldDef {
                name: "order_id".to_string(),
                expr: Expr::Column("order_id".to_string()),
            },
            FieldDef {
                name: "total".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
            FieldDef {
                name: "double_total".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("total".to_string())),
                    rhs: Box::new(Expr::Column("total".to_string())),
                },
            },
        ],
        predicate: Predicate::True,
        without_source_index: false,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    let base_sql = render_aggregate_select_sql(&def);
    let rendered =
        format!("select order_id::text, total::text, double_total::text from ({base_sql}) t");
    let hand = "select order_id::text, sum(amount)::text, (sum(amount) + sum(amount))::text \
                from order_line_items group by order_id";
    assert_eq!(symmetric_difference(&client, &rendered, hand).await, 0);
}
