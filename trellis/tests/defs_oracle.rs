//! Integration tests for the from-scratch correctness oracle (issue #25),
//! run against a real, ephemeral Postgres instance via `testkit::TestCluster`.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::eval::{RegexCache, Row, Value, evaluate, evaluate_aggregate};
use trellis::defs::{
    create_target_table, recompute, recompute_aggregate, render_aggregate_select_sql,
    render_expr_sql, source_primary_key,
};

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

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

/// The oracle's recompute over the whole source table must equal evaluating
/// each source row directly with #24's evaluator — the self-consistency
/// property that follows from the oracle being evaluator-driven rather than
/// a second, independently-written SQL expression.
#[tokio::test]
async fn oracle_recompute_equals_per_row_evaluation_over_the_source() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric);
             insert into orders (id, price, tax) values
                 (1, 10.00, 1.50),
                 (2, 0.00, 0.00),
                 (3, -5.25, 2.00)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();

    let oracle_result = recompute(&db.pool, &def, "id", &numeric_columns(&["price", "tax"]))
        .await
        .expect("oracle recompute");

    let rows = client
        .query("select id::text, price::text, tax::text from orders", &[])
        .await
        .expect("read source rows directly");

    let mut expected: HashMap<String, HashMap<String, Option<trellis::defs::Value>>> =
        HashMap::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        let price: String = row.get(1);
        let tax: String = row.get(2);

        let mut image: Row = HashMap::new();
        image.insert("price".to_string(), Some(price));
        image.insert("tax".to_string(), Some(tax));

        let evaluated = evaluate(
            &def,
            &image,
            &numeric_columns(&["price", "tax"]),
            &mut RegexCache::new(),
        )
        .expect("direct evaluation");
        expected.insert(id, evaluated);
    }

    assert_eq!(oracle_result, expected);
}

/// Cross-checks `strpos(name, 'foo') > 0` (a function call composed with the
/// `>` comparison operator, issue #65) against real Postgres, by rendering
/// the expression back to SQL via [`render_expr_sql`] and comparing its
/// result to the evaluator's — the same "our grammar is a subset of Postgres
/// semantics" check ADR-0004 calls for, exercised for the composed
/// function-call-plus-comparison shape the issue's report calls out.
#[tokio::test]
async fn function_call_composed_with_greater_than_matches_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

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
        let expected: bool = client
            .query_one(
                &format!("select ({sql})::boolean from (select $1::text as name) t"),
                &[&name],
            )
            .await
            .expect("query postgres")
            .get(0);

        let mut image: Row = HashMap::new();
        image.insert("name".to_string(), Some(name.to_string()));
        let def = TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "has_foo".to_string(),
                expr: expr.clone(),
            }],
            predicate: Predicate::True,
            without_source_index: false,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        let source_columns = HashMap::from([("name".to_string(), ValueType::Text)]);
        let evaluated =
            evaluate(&def, &image, &source_columns, &mut RegexCache::new()).expect("evaluate");
        let actual = match evaluated["has_foo"].as_ref().unwrap() {
            Value::Boolean(b) => *b,
            other => panic!("expected Boolean, got {other:?}"),
        };

        assert_eq!(actual, expected, "mismatch for name = {name:?}");
    }
}

/// A hand-staged apply of a 1-1 change converges to the oracle's recompute:
/// this simulates (by hand, in the test) the step the real apply stage
/// (#11, not yet built) will one day perform automatically — pre-populating
/// the target directly stands in for backfill-on-create (#6), which is
/// explicitly out of scope for this issue.
#[tokio::test]
async fn hand_staged_apply_of_a_source_change_converges_to_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric);
             insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 20.00, 2.00)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &numeric_columns(&["price", "tax"]),
        &def.source,
    )
    .await
    .expect("create target table");

    // Pre-populate the target directly, standing in for backfill-on-create
    // (#6), which this issue defers.
    for id in [1i32, 2] {
        let row = client
            .query_one(
                "select price::text, tax::text from orders where id = $1",
                &[&id],
            )
            .await
            .expect("read seed row");
        let price: String = row.get(0);
        let tax: String = row.get(1);
        let mut image: Row = HashMap::new();
        image.insert("price".to_string(), Some(price));
        image.insert("tax".to_string(), Some(tax));
        let evaluated = evaluate(
            &def,
            &image,
            &numeric_columns(&["price", "tax"]),
            &mut RegexCache::new(),
        )
        .expect("evaluate seed row");

        client
            .execute(
                "insert into order_totals (id, double_price, total)
                 values ($1, $2::text::numeric, $3::text::numeric)",
                &[
                    &id,
                    &evaluated["double_price"].as_ref().map(|n| n.to_string()),
                    &evaluated["total"].as_ref().map(|n| n.to_string()),
                ],
            )
            .await
            .expect("insert pre-populated target row");
    }

    // A source change: order 1's price changes. In the real (not-yet-built)
    // data flow, this would land in the staging ring and get applied by
    // stage 05; here it's applied to the target by hand, the same way that
    // stage will, to bootstrap the correctness bar ahead of it existing.
    client
        .execute(
            "update orders set price = $1::text::numeric where id = $2",
            &[&"15.00", &1i32],
        )
        .await
        .expect("mutate source row");

    let updated_row = client
        .query_one(
            "select price::text, tax::text from orders where id = 1",
            &[],
        )
        .await
        .expect("read updated row");
    let price: String = updated_row.get(0);
    let tax: String = updated_row.get(1);
    let mut image: Row = HashMap::new();
    image.insert("price".to_string(), Some(price));
    image.insert("tax".to_string(), Some(tax));
    let evaluated = evaluate(
        &def,
        &image,
        &numeric_columns(&["price", "tax"]),
        &mut RegexCache::new(),
    )
    .expect("evaluate updated row");

    client
        .execute(
            "update order_totals
             set double_price = $1::text::numeric, total = $2::text::numeric
             where id = $3",
            &[
                &evaluated["double_price"].as_ref().map(|n| n.to_string()),
                &evaluated["total"].as_ref().map(|n| n.to_string()),
                &1i32,
            ],
        )
        .await
        .expect("hand-apply the change to the target");

    // The hand-applied target must now equal a fresh from-scratch recompute.
    let oracle_result = recompute(
        &db.pool,
        &def,
        &pk[0].name,
        &numeric_columns(&["price", "tax"]),
    )
    .await
    .expect("oracle recompute");

    let target_rows = client
        .query(
            "select id::text, double_price::text, total::text from order_totals",
            &[],
        )
        .await
        .expect("read target table");

    assert_eq!(target_rows.len(), oracle_result.len());
    for row in target_rows {
        let id: String = row.get(0);
        let double_price: Option<String> = row.get(1);
        let total: Option<String> = row.get(2);

        let expected = &oracle_result[&id];
        assert_eq!(
            double_price,
            expected["double_price"].as_ref().map(|n| n.to_string()),
            "double_price mismatch for id {id}"
        );
        assert_eq!(
            total,
            expected["total"].as_ref().map(|n| n.to_string()),
            "total mismatch for id {id}"
        );
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
                name: "min_amount".to_string(),
                expr: Expr::FunctionCall {
                    name: "MIN".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
            FieldDef {
                name: "max_amount".to_string(),
                expr: Expr::FunctionCall {
                    name: "MAX".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
        without_source_index: false,
    }
}

/// The evaluator-driven [`recompute_aggregate`] must equal grouping the
/// source rows and running [`evaluate_aggregate`] directly over each group
/// — the same self-consistency property [`oracle_recompute_equals_per_row_evaluation_over_the_source`]
/// checks for the 1-1 case, exercised here for `SUM`/`AVG`/`MIN`/`MAX`.
#[tokio::test]
async fn oracle_recompute_aggregate_equals_per_group_evaluation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table order_line_items (
                 id integer primary key, order_id integer, amount numeric
             );
             insert into order_line_items (id, order_id, amount) values
                 (1, 1, 10.00),
                 (2, 2, -5.25),
                 (3, 2, 10.00),
                 (4, 3, 10.00),
                 (5, 3, 20.00),
                 (6, 3, 25.00)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_aggregate_def();
    let source_columns = HashMap::from([
        ("order_id".to_string(), ValueType::Numeric),
        ("amount".to_string(), ValueType::Numeric),
    ]);

    let oracle_result = recompute_aggregate(&db.pool, &def, &source_columns)
        .await
        .expect("oracle recompute_aggregate");

    let rows = client
        .query(
            "select order_id::text, amount::text from order_line_items",
            &[],
        )
        .await
        .expect("read source rows directly");

    let mut groups: HashMap<String, Vec<Row>> = HashMap::new();
    for row in rows {
        let order_id: String = row.get(0);
        let amount: String = row.get(1);
        let mut image: Row = HashMap::new();
        image.insert("order_id".to_string(), Some(order_id.clone()));
        image.insert("amount".to_string(), Some(amount));
        groups.entry(order_id).or_default().push(image);
    }

    let mut expected: HashMap<String, HashMap<String, Option<Value>>> =
        HashMap::with_capacity(groups.len());
    for (order_id, rows) in groups {
        let evaluated = evaluate_aggregate(&def, &rows, &source_columns, &mut RegexCache::new())
            .expect("direct aggregate evaluation");
        // `recompute_aggregate` keys `Recomputed` by its internal
        // length-prefixed grouping-key encoding (`oracle::group_key`), not
        // the bare grouping-column text, so this must match that encoding
        // to compare against `oracle_result` below.
        expected.insert(format!("{}:{order_id}", order_id.len()), evaluated);
    }

    assert_eq!(oracle_result, expected);
}

/// Cross-checks [`recompute_aggregate`] against real Postgres's own `GROUP
/// BY` (via [`render_aggregate_select_sql`]) byte-for-byte, covering a
/// single-row group, a group with a negative value, and a group whose `AVG`
/// is a non-terminating decimal (`55 / 3`) — the primary correctness oracle
/// this grammar addition is checked against, not just the secondary
/// evaluator self-consistency check above.
#[tokio::test]
async fn aggregate_recompute_matches_postgres_group_by_exactly() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table order_line_items (
                 id integer primary key, order_id integer, amount numeric
             );
             insert into order_line_items (id, order_id, amount) values
                 -- a single-row group
                 (1, 1, 10.00),
                 -- a group containing a negative value
                 (2, 2, -5.25),
                 (3, 2, 10.00),
                 -- a group whose AVG (55 / 3) is a non-terminating decimal
                 (4, 3, 10.00),
                 (5, 3, 20.00),
                 (6, 3, 25.00)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_aggregate_def();
    let source_columns = HashMap::from([
        ("order_id".to_string(), ValueType::Numeric),
        ("amount".to_string(), ValueType::Numeric),
    ]);

    let oracle_result = recompute_aggregate(&db.pool, &def, &source_columns)
        .await
        .expect("oracle recompute_aggregate");

    let base_sql = render_aggregate_select_sql(&def);
    let sql = format!(
        "select order_id::text, total_amount::text, avg_amount::text, \
         min_amount::text, max_amount::text from ({base_sql}) t"
    );
    let postgres_rows = client
        .query(sql.as_str(), &[])
        .await
        .expect("query postgres");

    assert_eq!(postgres_rows.len(), 3);
    for row in postgres_rows {
        let order_id: String = row.get(0);
        let total_amount: Option<String> = row.get(1);
        let avg_amount: Option<String> = row.get(2);
        let min_amount: Option<String> = row.get(3);
        let max_amount: Option<String> = row.get(4);

        // See the matching comment in
        // `oracle_recompute_aggregate_equals_per_group_evaluation` — the
        // oracle's `Recomputed` map is keyed by its internal length-prefixed
        // grouping-key encoding, not the bare grouping-column text.
        let expected = &oracle_result[&format!("{}:{order_id}", order_id.len())];
        assert_eq!(
            total_amount,
            expected["total_amount"].as_ref().map(|v| v.to_string()),
            "total_amount mismatch for order_id {order_id}"
        );
        assert_eq!(
            avg_amount,
            expected["avg_amount"].as_ref().map(|v| v.to_string()),
            "avg_amount mismatch for order_id {order_id}"
        );
        assert_eq!(
            min_amount,
            expected["min_amount"].as_ref().map(|v| v.to_string()),
            "min_amount mismatch for order_id {order_id}"
        );
        assert_eq!(
            max_amount,
            expected["max_amount"].as_ref().map(|v| v.to_string()),
            "max_amount mismatch for order_id {order_id}"
        );
    }
}

/// A `GROUP BY` field referencing another calculated field by name
/// (`double_total = total + total`, where `total = SUM(amount)`) — the
/// Aggregate-key-space counterpart of issue #83's 1-1 cross-field-alias
/// substitution fix, which was never extended to this oracle's rendering
/// (`render_aggregate_select_sql` used to render `double_total`'s raw
/// `Expr::Column("total")` as a bare SQL identifier, which Postgres rejects
/// since `total` names neither a source column nor a same-SELECT-list-visible
/// name — so this oracle never actually ran as working ground truth for this
/// shape; both the real target and the oracle would have hit the identical
/// "column does not exist" error). Confirms the oracle now renders valid SQL
/// that agrees with the evaluator-driven [`recompute_aggregate`].
#[tokio::test]
async fn aggregate_recompute_matches_postgres_group_by_for_a_cross_field_alias() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table order_line_items (
                 id integer primary key, order_id integer, amount numeric
             );
             insert into order_line_items (id, order_id, amount) values
                 (1, 1, 10.00),
                 (2, 2, -5.25),
                 (3, 2, 10.00)",
        )
        .await
        .expect("seed source table");

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
    let source_columns = HashMap::from([
        ("order_id".to_string(), ValueType::Numeric),
        ("amount".to_string(), ValueType::Numeric),
    ]);

    let oracle_result = recompute_aggregate(&db.pool, &def, &source_columns)
        .await
        .expect("oracle recompute_aggregate");

    let base_sql = render_aggregate_select_sql(&def);
    let sql = format!("select order_id::text, total::text, double_total::text from ({base_sql}) t");
    let postgres_rows = client
        .query(sql.as_str(), &[])
        .await
        .expect("query postgres");

    assert_eq!(postgres_rows.len(), 2);
    for row in postgres_rows {
        let order_id: String = row.get(0);
        let total: Option<String> = row.get(1);
        let double_total: Option<String> = row.get(2);

        // See the matching comment in
        // `oracle_recompute_aggregate_equals_per_group_evaluation` — the
        // oracle's `Recomputed` map is keyed by its internal length-prefixed
        // grouping-key encoding, not the bare grouping-column text.
        let expected = &oracle_result[&format!("{}:{order_id}", order_id.len())];
        assert_eq!(
            total,
            expected["total"].as_ref().map(|v| v.to_string()),
            "total mismatch for order_id {order_id}"
        );
        assert_eq!(
            double_total,
            expected["double_total"].as_ref().map(|v| v.to_string()),
            "double_total mismatch for order_id {order_id}"
        );
    }
}
