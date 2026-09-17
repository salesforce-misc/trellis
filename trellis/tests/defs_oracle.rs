//! Integration tests for the from-scratch correctness oracle (issue #25),
//! run against a real, ephemeral Postgres instance via `testkit::TestCluster`.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::ast::{
    Expr, FieldDef, GroupByKey, KeySpace, Operator, Predicate, RelationshipDef, TransformDef,
    ValueType,
};
use trellis::defs::eval::{
    RegexCache, RelationshipContext, Row, ToManyRelationship, ToOneRelationship, Value, evaluate,
    evaluate_aggregate, evaluate_with_relationships,
};
use trellis::defs::{
    RelationshipCardinality, create_target_table, recompute, recompute_aggregate,
    render_aggregate_select_sql, render_expr_sql, render_relationship_select_sql,
    require_single_column_pk, source_primary_key,
};
use trellis::numeric::Numeric;

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
    let pk = require_single_column_pk(
        source_primary_key(&db.pool, &def.source)
            .await
            .expect("introspect source primary key"),
        &def.source,
    )
    .expect("single-column pk");
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
        &pk.name,
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
            group_by: vec![GroupByKey::Column("order_id".to_string())],
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
            group_by: vec![GroupByKey::Column("order_id".to_string())],
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

/// A to-one enrichment (`category.name`, issue #28) rendered as a `LEFT JOIN`
/// (issue #32) must agree with the evaluator's resolution row-for-row over the
/// same seeded data — a genuine oracle: the rendered SQL runs in Postgres and
/// its `category_name` is compared to `evaluate_with_relationships`' output,
/// not to a hand-written string. Covers a matching join, a from-row whose FK
/// has no matching to-row, a `NULL` FK, and a matched to-row whose referenced
/// column is itself `NULL` — the four LEFT-JOIN / NULL cases #28 calls out.
#[tokio::test]
async fn to_one_left_join_render_matches_evaluator() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table categories (id integer primary key, name text);
             create table products (id integer primary key, category_id integer);
             insert into categories (id, name) values
                 (10, 'Books'),
                 (20, null);
             insert into products (id, category_id) values
                 (1, 10),   -- matches category 'Books'
                 (2, 99),   -- FK with no matching category => NULL
                 (3, null), -- NULL FK => NULL
                 (4, 20)    -- matches a category whose name is NULL => NULL",
        )
        .await
        .expect("seed source + related tables");

    let def = TransformDef {
        target: "product_view".to_string(),
        source: "products".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                expr: Expr::Column("id".to_string()),
            },
            FieldDef {
                name: "category_name".to_string(),
                expr: Expr::RelationshipPath {
                    rel: "category".to_string(),
                    column: "name".to_string(),
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let relationships = HashMap::from([(
        "category".to_string(),
        RelationshipDef {
            name: "category".to_string(),
            from_table: "products".to_string(),
            from_col: "category_id".to_string(),
            to_table: "categories".to_string(),
            to_col: "id".to_string(),
        },
    )]);

    // Postgres side: run the rendered SELECT, collect category_name by product id.
    let base_sql = render_relationship_select_sql(&def, &relationships);
    let sql = format!("select id::text, category_name::text from ({base_sql}) t");
    let postgres: HashMap<String, Option<String>> = client
        .query(sql.as_str(), &[])
        .await
        .expect("query rendered to-one SQL")
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
        .collect();

    // Evaluator side: build the to-one context from the same categories rows,
    // then evaluate each product row and compare category_name.
    let source_columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("category_id".to_string(), ValueType::Numeric),
    ]);
    let to_columns = HashMap::from([("name".to_string(), ValueType::Text)]);
    let category_rows = client
        .query("select id::text, name from categories", &[])
        .await
        .expect("read categories");
    let mut to_rows_by_key: HashMap<String, Row> = HashMap::new();
    for row in category_rows {
        let id: String = row.get(0);
        let name: Option<String> = row.get(1);
        to_rows_by_key.insert(id, HashMap::from([("name".to_string(), name)]));
    }
    let ctx = RelationshipContext::new(HashMap::from([(
        "category".to_string(),
        ToOneRelationship {
            from_col: "category_id".to_string(),
            cardinality: RelationshipCardinality::ToOne,
            to_columns,
            to_rows_by_key,
        },
    )]));

    let product_rows = client
        .query("select id::text, category_id::text from products", &[])
        .await
        .expect("read products");
    assert_eq!(product_rows.len(), 4);
    for row in product_rows {
        let id: String = row.get(0);
        let category_id: Option<String> = row.get(1);
        let image: Row = HashMap::from([
            ("id".to_string(), Some(id.clone())),
            ("category_id".to_string(), category_id),
        ]);
        let evaluated = evaluate_with_relationships(
            &def,
            &image,
            &source_columns,
            &ctx,
            &mut RegexCache::new(),
        )
        .expect("evaluate with relationships");
        let expected = evaluated["category_name"].as_ref().map(|v| v.to_string());
        assert_eq!(
            postgres[&id], expected,
            "category_name mismatch for product {id}"
        );
    }
}

/// A to-many aggregate enrichment (`sum(comments.word_count)` etc., issue #29)
/// rendered as a correlated aggregate subquery (issue #32) must agree with the
/// evaluator's fold row-for-row — a genuine oracle over seeded data. Covers a
/// post with several comments (one with a `NULL` word_count), a single-comment
/// post, and a post with none (the empty set: `COUNT → 0`, the rest `NULL`).
/// `SUM`/`MIN`/`MAX`/`COUNT` are compared by text; `AVG` by numeric value
/// (Postgres `numeric` avg carries a fractional scale, as #29's own test notes).
#[tokio::test]
async fn to_many_correlated_aggregate_render_matches_evaluator() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table posts (id integer primary key);
             create table comments (
                 id integer primary key, post_id integer, word_count numeric
             );
             insert into posts (id) values (1), (2), (3);
             insert into comments (id, post_id, word_count) values
                 (1, 1, 10),
                 (2, 1, 20),
                 (3, 1, null), -- skipped by SUM/AVG, not counted by COUNT(col)
                 (4, 2, 5)
                 -- post 3 has no comments: the empty set",
        )
        .await
        .expect("seed source + related tables");

    let aggs = [
        ("total_words", "SUM"),
        ("num_comments", "COUNT"),
        ("min_words", "MIN"),
        ("max_words", "MAX"),
        ("avg_words", "AVG"),
    ];
    let def = TransformDef {
        target: "post_stats".to_string(),
        source: "posts".to_string(),
        key_space: KeySpace::OneToOne,
        fields: std::iter::once(FieldDef {
            name: "id".to_string(),
            expr: Expr::Column("id".to_string()),
        })
        .chain(aggs.iter().map(|(field, func)| FieldDef {
            name: (*field).to_string(),
            expr: Expr::FunctionCall {
                name: (*func).to_string(),
                args: vec![Expr::RelationshipPath {
                    rel: "comments".to_string(),
                    column: "word_count".to_string(),
                }],
            },
        }))
        .collect(),
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let relationships = HashMap::from([(
        "comments".to_string(),
        RelationshipDef {
            name: "comments".to_string(),
            from_table: "posts".to_string(),
            from_col: "id".to_string(),
            to_table: "comments".to_string(),
            to_col: "post_id".to_string(),
        },
    )]);

    // Postgres side: run the rendered SELECT (each aggregate cast to text).
    let base_sql = render_relationship_select_sql(&def, &relationships);
    let select_cols: Vec<String> = std::iter::once("id::text".to_string())
        .chain(aggs.iter().map(|(field, _)| format!("{field}::text")))
        .collect();
    let sql = format!("select {} from ({base_sql}) t", select_cols.join(", "));
    let postgres_rows = client
        .query(sql.as_str(), &[])
        .await
        .expect("query rendered to-many SQL");

    // Evaluator side: build the to-many context from the same comment rows.
    let source_columns = HashMap::from([("id".to_string(), ValueType::Numeric)]);
    let to_columns = HashMap::from([("word_count".to_string(), ValueType::Numeric)]);
    let comment_rows = client
        .query("select post_id::text, word_count::text from comments", &[])
        .await
        .expect("read comments");
    let mut to_rows_by_key: HashMap<String, Vec<Row>> = HashMap::new();
    for row in comment_rows {
        let post_id: String = row.get(0);
        let word_count: Option<String> = row.get(1);
        to_rows_by_key
            .entry(post_id)
            .or_default()
            .push(HashMap::from([("word_count".to_string(), word_count)]));
    }
    let ctx = RelationshipContext::default().with_to_many(HashMap::from([(
        "comments".to_string(),
        ToManyRelationship {
            from_col: "id".to_string(),
            to_columns,
            to_rows_by_key,
        },
    )]));

    assert_eq!(postgres_rows.len(), 3);
    for row in postgres_rows {
        let id: String = row.get(0);
        let image: Row = HashMap::from([("id".to_string(), Some(id.clone()))]);
        let evaluated = evaluate_with_relationships(
            &def,
            &image,
            &source_columns,
            &ctx,
            &mut RegexCache::new(),
        )
        .expect("evaluate with relationships");
        for (idx, (field, func)) in aggs.iter().enumerate() {
            // column 0 is id; the aggregates follow in order.
            let pg: Option<String> = row.get(idx + 1);
            let eval_val = evaluated[*field].as_ref();
            if *func == "AVG" {
                // Compare AVG by numeric value: Postgres numeric avg carries a
                // fractional scale the engine's Numeric need not match textually.
                match (pg, eval_val) {
                    (None, None) => {}
                    (Some(pg_text), Some(Value::Numeric(n))) => assert_eq!(
                        Numeric::parse(&pg_text).unwrap().compare(n),
                        std::cmp::Ordering::Equal,
                        "avg mismatch for post {id}: pg={pg_text} eval={n}"
                    ),
                    (pg, eval_val) => {
                        panic!("avg shape mismatch for post {id}: pg={pg:?} eval={eval_val:?}")
                    }
                }
            } else {
                assert_eq!(
                    pg,
                    eval_val.map(|v| v.to_string()),
                    "{func} ({field}) mismatch for post {id}"
                );
            }
        }
    }
}
