//! Integration tests for the persisted cross-table dependency graph and
//! edge-typed resolver (issue #21), run against a real, ephemeral Postgres
//! instance via `testkit::TestCluster`.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{
    CatalogError, EdgeKind, NodeKind, ValidationError, ValueType, create_definition,
    create_target_table, dependents_of, node_for_table, parse, persist_edge, resolve_node,
    source_primary_key, transforms_for_source,
};

/// Creates a minimal backing relation for a definition's source table
/// (issue #23's backfill enumerates it for real, via a live `regclass`/
/// catalog lookup) — a bare PK column is enough, since `validate()` checks
/// column references against the passed-in `source_columns` map, not the
/// live schema.
///
/// Explicitly under `public` (issue #74, ADR-0007) — not left to land
/// wherever the pool's ambient `search_path` happens to put a bare `CREATE
/// TABLE` (`trellis`, per `pool::session_bootstrap`), as before #74. Several
/// tests below reuse the same bare name both as a real source table here and
/// later as a chained definition's bare `TRANSFORM <name> ...` target; a
/// target always resolves against `Config::target_schema` (`public` by
/// default — a config-time decision, never a `search_path` walk, per
/// ADR-0007 decision (1)), so this table needs to actually live there too or
/// the two roles now resolve to two different qualified nodes instead of one
/// — before #74's qualified `schema_nodes` keying, the bare graph couldn't
/// tell the difference either way.
async fn create_bare_source_table(pool: &trellis::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table public.{name} (id serial primary key)"
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
    pool: &trellis::pool::Pool,
    dsl: &str,
    source_columns: &HashMap<String, ValueType>,
) {
    let def = parse(dsl).expect("parse dsl for target materialization");
    let pk = source_primary_key(pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(pool, &def, "public", &pk, source_columns, &def.source)
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

    let dependents = dependents_of(&db.pool, "public.orders", EdgeKind::Source)
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
    let joins = dependents_of(&db.pool, "public.orders", EdgeKind::Join)
        .await
        .expect("query join dependents");
    assert!(joins.is_empty());

    let relationships = dependents_of(&db.pool, "public.orders", EdgeKind::Relationship)
        .await
        .expect("query relationship dependents");
    assert!(relationships.is_empty());

    let unrelated = dependents_of(&db.pool, "public.nonexistent_table", EdgeKind::Source)
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

    let a_dependents = dependents_of(&db.pool, "public.a", EdgeKind::Source)
        .await
        .expect("query a's dependents");
    assert_eq!(a_dependents.len(), 1);
    assert_eq!(a_dependents[0].def.target, "b");

    let b_dependents = dependents_of(&db.pool, "public.b", EdgeKind::Source)
        .await
        .expect("query b's dependents");
    assert_eq!(b_dependents.len(), 1);
    assert_eq!(b_dependents[0].def.target, "c");

    // "a" has no direct edge to "c" — a two-hop chain is two edges, not one
    // that skips the intermediate node.
    let a_to_c = dependents_of(&db.pool, "public.a", EdgeKind::Source)
        .await
        .expect("query a's dependents again");
    assert!(a_to_c.iter().all(|def| def.def.target != "c"));
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

    let via_wrapper = transforms_for_source(&db.pool, "public.orders")
        .await
        .expect("transforms_for_source");
    let via_resolver = dependents_of(&db.pool, "public.orders", EdgeKind::Source)
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

    let dependents = dependents_of(&db.pool, "public.orders", EdgeKind::Source)
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

    let from_node = resolve_node(&db.pool, "public.orders", NodeKind::Source)
        .await
        .expect("resolve source node");
    let to_node = resolve_node(&db.pool, "public.order_totals", NodeKind::Target)
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
                vec![
                    "public.a".to_string(),
                    "public.b".to_string(),
                    "public.a".to_string()
                ]
            );
        }
        other => panic!("expected Validate(TableCycle), got {other:?}"),
    }

    // The rejected definition must not have persisted anything: "a" still
    // has exactly one dependent.
    let a_dependents = dependents_of(&db.pool, "public.a", EdgeKind::Source)
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
                    "public.a".to_string(),
                    "public.b".to_string(),
                    "public.c".to_string(),
                    "public.a".to_string(),
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

    let a_node = resolve_node(&db.pool, "public.a", NodeKind::Source)
        .await
        .expect("resolve a as source");
    let b_source_node = resolve_node(&db.pool, "public.b", NodeKind::Source)
        .await
        .expect("resolve b as source");
    let b_target_node = resolve_node(&db.pool, "public.b", NodeKind::Target)
        .await
        .expect("resolve b as target");
    let c_node = resolve_node(&db.pool, "public.c", NodeKind::Target)
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

/// The actual point of issue #74 (ADR-0007): `public.posts` and
/// `archive.posts` — the same bare table name, in two different schemas —
/// must resolve to two independent `schema_nodes` rows with two
/// independent sets of dependent edges, not collide into one node the way
/// the bare-keyed graph did before this issue. Both sources are named via
/// the explicit `schema.table` grammar (issue #76) so this test doesn't
/// depend on `search_path` ordering to pick one or the other.
#[tokio::test]
async fn same_named_tables_in_different_schemas_are_distinct_nodes_with_independent_edges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table public.posts (id serial primary key, views integer); \
             create schema archive; \
             create table archive.posts (id serial primary key, views integer)",
        )
        .await
        .expect("create public.posts and archive.posts");
    drop(client);

    create_definition(
        &db.pool,
        "TRANSFORM public_post_totals FROM public.posts SELECT views AS total_views",
        &HashMap::from([("views".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("public.posts definition should be stored");

    create_definition(
        &db.pool,
        "TRANSFORM archive_post_totals FROM archive.posts SELECT views AS total_views",
        &HashMap::from([("views".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("archive.posts definition should be stored");

    // Distinct nodes, each correctly reporting its own qualified identity —
    // before issue #74 both `FROM` clauses would have resolved to the same
    // bare "posts" node.
    let public_node = node_for_table(&db.pool, "public.posts")
        .await
        .expect("query public.posts node")
        .expect("public.posts node must exist");
    let archive_node = node_for_table(&db.pool, "archive.posts")
        .await
        .expect("query archive.posts node")
        .expect("archive.posts node must exist");
    assert_ne!(
        public_node.id, archive_node.id,
        "public.posts and archive.posts must resolve to distinct schema_nodes rows"
    );
    assert_eq!(public_node.table_name, "public.posts");
    assert_eq!(archive_node.table_name, "archive.posts");

    // Independent edges: each node's own `Source` dependent is its own
    // definition, not the other schema's.
    let public_dependents = dependents_of(&db.pool, "public.posts", EdgeKind::Source)
        .await
        .expect("query public.posts dependents");
    assert_eq!(public_dependents.len(), 1);
    assert_eq!(public_dependents[0].def.target, "public_post_totals");

    let archive_dependents = dependents_of(&db.pool, "archive.posts", EdgeKind::Source)
        .await
        .expect("query archive.posts dependents");
    assert_eq!(archive_dependents.len(), 1);
    assert_eq!(archive_dependents[0].def.target, "archive_post_totals");
}
