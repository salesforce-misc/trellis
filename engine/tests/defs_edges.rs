//! Integration tests for the persisted cross-table dependency graph and
//! edge-typed resolver (issue #21), run against a real, ephemeral Postgres
//! instance via `testkit::TestCluster`.

use std::collections::HashMap;

use engine::defs::{
    CatalogError, EdgeKind, NodeKind, ValidationError, ValueType, create_definition,
    create_target_table, dependents_of, node_for_source_oid, parse, persist_edge, resolve_node,
    source_primary_key, transforms_for_source,
};
use engine::{Config, Pool};
use testkit::TestCluster;

/// Creates a minimal backing relation for a definition's source table
/// (issue #23's backfill enumerates it for real, via a live `regclass`/
/// catalog lookup) — a bare PK column is enough, since `validate()` checks
/// column references against the passed-in `source_columns` map, not the
/// live schema. Left unqualified so it lands via the pool's ambient
/// `search_path` (Trellis schema first), matching the schema
/// `create_definition` assumes for `def.source` today.
async fn create_bare_source_table(pool: &engine::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (id serial primary key, price numeric)"
        ))
        .await
        .expect("create bare source table");
}

/// Physically materializes `dsl`'s target table under `public` (mirroring
/// what a real chained definition always does before anything downstream can
/// name it as a `FROM` source). A handful of tests below chain a second
/// definition off of a target that was only ever registered in the catalog
/// via `create_definition`, never backfilled with a real table — issue #23
/// made that chained definition's own backfill actually enumerate its
/// source, so the source now has to be a real, queryable relation in
/// whichever schema `create_definition`'s `is_target` check resolves it to
/// (`public`, same as [`create_target_table`]'s own hardcoded schema), not
/// just a name in `schema_nodes`.
async fn materialize_chained_target(
    pool: &engine::pool::Pool,
    dsl: &str,
    source_columns: &HashMap<String, ValueType>,
) {
    let def = parse(dsl).expect("parse dsl for target materialization");
    let pk = source_primary_key(pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(pool, &def, "public", &pk, source_columns)
        .await
        .expect("materialize chained target table");
}

#[tokio::test]
async fn creating_a_definition_persists_a_source_edge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("valid definition should be stored");

    let dependents = dependents_of(&db.pool, "orders", EdgeKind::Source)
        .await
        .expect("query dependents");
    assert_eq!(dependents.len(), 1);
    assert_eq!(dependents[0].def.target, "order_totals");
}

#[tokio::test]
async fn a_node_with_no_dependents_of_a_kind_returns_empty() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("valid definition should be stored");

    // No relationship or join edges are ever persisted today (issue #21's
    // explicit out-of-scope) — querying for them must come back empty, not
    // error or fall back to source edges.
    let joins = dependents_of(&db.pool, "orders", EdgeKind::Join)
        .await
        .expect("query join dependents");
    assert!(joins.is_empty());

    let relationships = dependents_of(&db.pool, "orders", EdgeKind::Relationship)
        .await
        .expect("query relationship dependents");
    assert!(relationships.is_empty());

    let unrelated = dependents_of(&db.pool, "nonexistent_table", EdgeKind::Source)
        .await
        .expect("query dependents of an unknown node");
    assert!(unrelated.is_empty());
}

/// The graph is real and walkable across more than one hop: A -> B -> C,
/// resolving A's dependents finds B via a `Source` edge, and separately
/// resolving B's dependents finds C — each hop is an independent edge
/// lookup, not a single query that already knows the whole chain.
#[tokio::test]
async fn the_dependency_graph_is_walkable_across_multiple_hops() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "a").await;

    let b_source_columns = HashMap::from([("price".to_string(), ValueType::Numeric)]);
    create_definition(
        &db.pool,
        "TRANSFORM b FROM a SELECT price AS total",
        &b_source_columns,
    )
    .await
    .expect("a -> b definition should be stored");
    materialize_chained_target(
        &db.pool,
        "TRANSFORM b FROM a SELECT price AS total",
        &b_source_columns,
    )
    .await;

    create_definition(
        &db.pool,
        "TRANSFORM c FROM b SELECT total AS total_again",
        &HashMap::from([("total".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("b -> c definition should be stored");

    let a_dependents = dependents_of(&db.pool, "a", EdgeKind::Source)
        .await
        .expect("query a's dependents");
    assert_eq!(a_dependents.len(), 1);
    assert_eq!(a_dependents[0].def.target, "b");

    let b_dependents = dependents_of(&db.pool, "b", EdgeKind::Source)
        .await
        .expect("query b's dependents");
    assert_eq!(b_dependents.len(), 1);
    assert_eq!(b_dependents[0].def.target, "c");

    // "a" has no direct edge to "c" — a two-hop chain is two edges, not one
    // that skips the intermediate node.
    let a_to_c = dependents_of(&db.pool, "a", EdgeKind::Source)
        .await
        .expect("query a's dependents again");
    assert!(a_to_c.iter().all(|def| def.def.target != "c"));
}

/// A configured target schema must bind its physical target node before a
/// chained definition resolves that table as a source. A same-named Trellis
/// relation would be an incorrect node and must not affect the analytics one.
#[tokio::test]
async fn an_analytics_target_promotes_to_its_chained_source_node_by_oid() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric);
             create schema analytics;
             create table trellis.order_totals (id integer primary key, total numeric)",
        )
        .await
        .expect("seed sources and colliding public table");
    let analytics_pool = Pool::new(
        &Config::from_dsn(db.dsn())
            .expect("analytics config")
            .with_target_schema("analytics")
            .expect("valid analytics schema"),
    )
    .expect("analytics pool");

    let source_columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("price".to_string(), ValueType::Numeric),
    ]);
    let first = create_definition(
        &analytics_pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &source_columns,
    )
    .await
    .expect("create first definition");
    let pk = source_primary_key(&db.pool, "orders")
        .await
        .expect("source primary key");
    create_target_table(
        &analytics_pool,
        &first.def,
        "analytics",
        &pk,
        &source_columns,
    )
    .await
    .expect("create and bind analytics target");

    let analytics_oid: u32 = client
        .query_one("select 'analytics.order_totals'::regclass::oid", &[])
        .await
        .expect("read analytics target oid")
        .get(0);
    let totals_columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("total".to_string(), ValueType::Numeric),
    ]);
    create_definition(
        &analytics_pool,
        "TRANSFORM order_summary FROM order_totals SELECT total AS grand_total",
        &totals_columns,
    )
    .await
    .expect("chain from analytics target through target schema search path");

    let node = node_for_source_oid(&db.pool, analytics_oid)
        .await
        .expect("query bound analytics node")
        .expect("analytics target has one node");
    assert!(node.is_source && node.is_target);
    assert_eq!(node.schema_name.as_deref(), Some("analytics"));
    let dependents = transforms_for_source(&analytics_pool, "order_totals")
        .await
        .expect("find chain by bound target relation");
    assert_eq!(dependents.len(), 1);
    assert_eq!(dependents[0].def.target, "order_summary");
}

/// `transforms_for_source` is a thin wrapper over `dependents_of` filtered
/// to `EdgeKind::Source` — both must agree.
#[tokio::test]
async fn transforms_for_source_agrees_with_dependents_of_source_edges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("valid definition should be stored");

    let via_wrapper = transforms_for_source(&db.pool, "orders")
        .await
        .expect("transforms_for_source");
    let via_resolver = dependents_of(&db.pool, "orders", EdgeKind::Source)
        .await
        .expect("dependents_of");

    assert_eq!(via_wrapper, via_resolver);
}

/// Two distinct definitions from the same source (different targets) each
/// get their own `Source` edge — `dependents_of` fans out to all of them,
/// not just the first. (This is a fan-out test, not a dedup test: see
/// `persisting_the_same_edge_twice_does_not_duplicate_the_row` below for
/// coverage of the `on conflict do nothing` path.)
#[tokio::test]
async fn dependents_of_returns_all_distinct_source_edges_for_fan_out() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("first definition establishes the orders -> order_totals edge");

    // A second, distinct definition from the same source to a different
    // target must not affect the first edge's count.
    create_definition(
        &db.pool,
        "TRANSFORM order_flags FROM orders SELECT price AS flag",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("second definition establishes a distinct orders -> order_flags edge");

    let dependents = dependents_of(&db.pool, "orders", EdgeKind::Source)
        .await
        .expect("query dependents");
    assert_eq!(dependents.len(), 2);
}

/// `create_definition` can never exercise `schema_edges`'s
/// `on conflict (from_node_id, to_node_id, kind) do nothing` dedup path
/// itself: `transform_definitions.target_table` is unique, so no two
/// definitions can ever resolve to the same `(from_node_id, to_node_id)`
/// pair. This test drives the conflict path directly through
/// `persist_edge`, calling it twice with the identical triple, and asserts
/// exactly one row lands in `schema_edges`.
#[tokio::test]
async fn persisting_the_same_edge_twice_does_not_duplicate_the_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let from_node = resolve_node(&db.pool, "orders", NodeKind::Source)
        .await
        .expect("resolve source node");
    let to_node = resolve_node(&db.pool, "order_totals", NodeKind::Target)
        .await
        .expect("resolve target node");

    persist_edge(&db.pool, from_node.id, to_node.id, EdgeKind::Source)
        .await
        .expect("first persist_edge call establishes the edge");
    persist_edge(&db.pool, from_node.id, to_node.id, EdgeKind::Source)
        .await
        .expect("second persist_edge call with the same triple hits on conflict do nothing");

    let client = db.pool.get().await.expect("get connection");
    let rows = client
        .query(
            "select count(*) from schema_edges where from_node_id = $1 and to_node_id = $2 and kind = $3",
            &[&from_node.id, &to_node.id, &EdgeKind::Source.as_str()],
        )
        .await
        .expect("count schema_edges rows");
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 1);
}

/// A direct table-level cycle — A -> B already exists, then B -> A is
/// declared — is rejected at definition time (issue #22, generalized from
/// column-only to the two-level graph), not just persisted as a second edge.
#[tokio::test]
async fn a_direct_table_cycle_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "a").await;

    create_definition(
        &db.pool,
        "TRANSFORM b FROM a SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("a -> b definition should be stored");
    materialize_chained_target(
        &db.pool,
        "TRANSFORM b FROM a SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM a FROM b SELECT total AS price",
        &HashMap::from([("total".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect_err("b -> a would close a direct cycle with a -> b");

    match err {
        CatalogError::Validate(ValidationError::TableCycle { cycle }) => {
            assert_eq!(
                cycle,
                vec!["a".to_string(), "b".to_string(), "a".to_string()]
            );
        }
        other => panic!("expected Validate(TableCycle), got {other:?}"),
    }

    // The rejected definition must not have persisted anything: "a" still
    // has exactly one dependent.
    let a_dependents = dependents_of(&db.pool, "a", EdgeKind::Source)
        .await
        .expect("query a's dependents");
    assert_eq!(a_dependents.len(), 1);
}

/// A transitive table-level cycle — A -> B -> C already exists, then C -> A
/// is declared — is rejected, not just a direct A <-> B cycle.
#[tokio::test]
async fn a_transitive_table_cycle_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "a").await;

    let b_source_columns = HashMap::from([("price".to_string(), ValueType::Numeric)]);
    create_definition(
        &db.pool,
        "TRANSFORM b FROM a SELECT price AS total",
        &b_source_columns,
    )
    .await
    .expect("a -> b definition should be stored");
    materialize_chained_target(
        &db.pool,
        "TRANSFORM b FROM a SELECT price AS total",
        &b_source_columns,
    )
    .await;

    create_definition(
        &db.pool,
        "TRANSFORM c FROM b SELECT total AS total_again",
        &HashMap::from([("total".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("b -> c definition should be stored");
    materialize_chained_target(
        &db.pool,
        "TRANSFORM c FROM b SELECT total AS total_again",
        &HashMap::from([("total".to_string(), ValueType::Numeric)]),
    )
    .await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM a FROM c SELECT total_again AS price",
        &HashMap::from([("total_again".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect_err("c -> a would close a transitive cycle with a -> b -> c");

    match err {
        CatalogError::Validate(ValidationError::TableCycle { cycle }) => {
            assert_eq!(
                cycle,
                vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "a".to_string(),
                ]
            );
        }
        other => panic!("expected Validate(TableCycle), got {other:?}"),
    }
}

/// Two definitions from an unrelated, disjoint part of the graph must not be
/// mistaken for a cycle — this pins down that the check is a real
/// reachability search, not a blanket "any second edge is suspicious" rule.
#[tokio::test]
async fn unrelated_definitions_are_not_flagged_as_a_cycle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "a").await;
    create_bare_source_table(&db.pool, "x").await;

    create_definition(
        &db.pool,
        "TRANSFORM b FROM a SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("a -> b definition should be stored");

    create_definition(
        &db.pool,
        "TRANSFORM y FROM x SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("x -> y definition, disjoint from a -> b, should be stored");
}

/// A "shortcut" edge across nodes that already share a path — given existing
/// edges a -> b -> c, declaring a -> c directly — is not a cycle and must be
/// allowed. This exercises the case where source and target share a graph
/// neighbor without an actual cycle, which the fully-disjoint case above
/// doesn't cover: the reachability search must find that "b" (an
/// intermediate, not the source) is not actually the destination and keep
/// walking to "a", not falsely conclude proximity implies a cycle.
///
/// The a -> b and b -> c edges are seeded directly via `resolve_node`/
/// `persist_edge` (as `persisting_the_same_edge_twice_does_not_duplicate_the_row`
/// does above) rather than through two `create_definition` calls: a real
/// `TRANSFORM c FROM b ...` definition would already occupy target "c" in
/// `transform_definitions` (unique per table), which would make the
/// shortcut's own `TRANSFORM c FROM a ...` fail on that unrelated unique
/// constraint before the cycle check is even reached.
#[tokio::test]
async fn a_shortcut_edge_across_an_existing_path_is_not_a_cycle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "a").await;

    let a_node = resolve_node(&db.pool, "a", NodeKind::Source)
        .await
        .expect("resolve a as source");
    let b_source_node = resolve_node(&db.pool, "b", NodeKind::Source)
        .await
        .expect("resolve b as source");
    let b_target_node = resolve_node(&db.pool, "b", NodeKind::Target)
        .await
        .expect("resolve b as target");
    let c_node = resolve_node(&db.pool, "c", NodeKind::Target)
        .await
        .expect("resolve c as target");

    persist_edge(&db.pool, a_node.id, b_target_node.id, EdgeKind::Source)
        .await
        .expect("seed a -> b edge");
    persist_edge(&db.pool, b_source_node.id, c_node.id, EdgeKind::Source)
        .await
        .expect("seed b -> c edge");

    create_definition(
        &db.pool,
        "TRANSFORM c FROM a SELECT price AS total_again",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("a -> c is a shortcut across an existing path, not a cycle");
}
