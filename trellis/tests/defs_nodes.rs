//! Integration tests for the first-class schema-node model (issue #20), run
//! against a real, ephemeral Postgres instance via `testkit::TestCluster`.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{NodeKind, ValueType, create_definition, node_for_table, resolve_node};

/// Creates a minimal backing relation for a definition's source table
/// (issue #23's backfill enumerates it for real, via a live `regclass`/
/// catalog lookup) — a bare PK column is enough, since `validate()` checks
/// column references against the passed-in `source_columns` map, not the
/// live schema.
///
/// Explicitly under `public` (issue #74, ADR-0011), not wherever a bare
/// `CREATE TABLE` would land via the pool's ambient `search_path` —
/// `a_definitions_target_matching_an_existing_source_node_merges_roles`
/// below reuses "orders" both as a real source table here and as a later
/// definition's bare `TRANSFORM orders ...` target, which always resolves
/// against `Config::target_schema` (`public` by default); the two must
/// agree on a schema or they resolve to two different qualified nodes
/// instead of merging into one dual-role node.
async fn create_bare_source_table(pool: &trellis::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table public.{name} (id serial primary key)"
        ))
        .await
        .expect("create bare source table");
}

#[tokio::test]
async fn creating_a_definition_resolves_its_source_and_target_as_nodes() {
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

    let source = node_for_table(&db.pool, "public.orders")
        .await
        .expect("query source node")
        .expect("source node must exist after create_definition");
    assert_eq!(source.table_name, "public.orders");
    assert!(source.is_source);
    assert!(!source.is_target);

    let target = node_for_table(&db.pool, "public.order_totals")
        .await
        .expect("query target node")
        .expect("target node must exist after create_definition");
    assert_eq!(target.table_name, "public.order_totals");
    assert!(target.is_target);
    assert!(!target.is_source);
}

#[tokio::test]
async fn a_table_with_no_definitions_has_no_node() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let node = node_for_table(&db.pool, "public.nonexistent")
        .await
        .expect("query mapping");
    assert!(node.is_none());
}

#[tokio::test]
async fn resolving_the_same_table_and_kind_twice_is_idempotent() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let first = resolve_node(&db.pool, "public.orders", NodeKind::Source)
        .await
        .expect("first resolution");
    let second = resolve_node(&db.pool, "public.orders", NodeKind::Source)
        .await
        .expect("second resolution is idempotent");

    assert_eq!(first.id, second.id);
    assert!(second.is_source);
    assert!(!second.is_target);
}

/// Resolving a table under the *other* kind merges into the same node
/// rather than erroring — a transform's target is a completely ordinary
/// table a later transform can subscribe to as its source
/// (chained/multi-hop transforms), so the same physical table legitimately
/// ends up resolved under both roles. See
/// [`trellis::defs::NodeKind`]'s doc comment.
#[tokio::test]
async fn resolving_a_table_under_the_other_kind_merges_into_one_dual_role_node() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let first = resolve_node(&db.pool, "public.orders", NodeKind::Source)
        .await
        .expect("first resolution as a source");

    let second = resolve_node(&db.pool, "public.orders", NodeKind::Target)
        .await
        .expect("resolving the other role merges rather than erroring");

    assert_eq!(first.id, second.id);
    assert!(second.is_source);
    assert!(second.is_target);
}

/// A definition's target table naming an already-known *source* table
/// (from an unrelated transform) merges "orders" into a dual-role node
/// rather than being rejected — see [`trellis::defs::NodeKind`]'s doc
/// comment on chained/multi-hop transforms.
#[tokio::test]
async fn a_definitions_target_matching_an_existing_source_node_merges_roles() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;
    create_bare_source_table(&db.pool, "upstream").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("first definition establishes 'orders' as a source node");

    create_definition(
        &db.pool,
        "TRANSFORM orders FROM upstream SELECT 1 AS out",
        &HashMap::new(),
    )
    .await
    .expect("second definition merges 'orders' into a dual-role node");

    let node = node_for_table(&db.pool, "public.orders")
        .await
        .expect("query orders node")
        .expect("orders node must exist");
    assert!(node.is_source);
    assert!(node.is_target);
}
