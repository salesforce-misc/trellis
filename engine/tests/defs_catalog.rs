//! Integration tests for the transform-definition catalog (issue #23),
//! run against a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).

use std::collections::HashMap;

use engine::defs::{
    CatalogError, ValidationError, ValueType, all_source_relations, create_definition,
    resolve_source_relation, source_relation_by_oid, source_table_version_by_oid,
    transforms_for_source, transforms_for_source_oid,
};
use testkit::TestCluster;

fn columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|s| (s.to_string(), ValueType::Numeric))
        .collect()
}

/// Creates a minimal backing relation for a definition's source table
/// (issue #23's backfill enumerates it for real, via a live `regclass`/
/// catalog lookup) — a bare PK column is enough, since `validate()` checks
/// column references against both the passed-in `source_columns` map and the
/// live schema. Left unqualified so it lands via the pool's ambient
/// `search_path` (Trellis schema first), matching the schema
/// `create_definition` assumes for `def.source` today.
async fn create_bare_source_table(pool: &engine::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (
                id serial primary key,
                price numeric,
                tax numeric,
                label text,
                active boolean,
                author uuid,
                name numeric,
                a numeric
            )"
        ))
        .await
        .expect("create bare source table");
}

#[tokio::test]
async fn valid_definition_is_stored_and_retrievable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    let def = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &columns(&["price", "tax"]),
    )
    .await
    .expect("valid definition should be stored");

    assert_eq!(def.source_version, 1);
    assert_eq!(def.def.target, "order_totals");
    assert_eq!(def.def.source, "orders");

    let subscribers = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 1);
    assert_eq!(subscribers[0].id, def.id);
    assert_eq!(subscribers[0].def.target, "order_totals");
}

/// Issue #63's write-path gap: the source-column type map a definition was
/// validated against must be persisted, not just returned transiently from
/// `create_definition` — `transforms_for_source` (what the physical apply
/// path loads) must read the exact same map back, mixed value types
/// included.
#[tokio::test]
async fn transforms_for_source_returns_the_persisted_source_column_types() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    let source_columns: HashMap<String, ValueType> = HashMap::from([
        ("price".to_string(), ValueType::Numeric),
        ("label".to_string(), ValueType::Text),
        ("active".to_string(), ValueType::Boolean),
        ("author".to_string(), ValueType::Uuid),
    ]);

    let created = create_definition(
        &db.pool,
        "TRANSFORM order_labels FROM orders SELECT label AS out",
        &source_columns,
    )
    .await
    .expect("valid definition should be stored");
    assert_eq!(created.source_columns, source_columns);

    let subscribers = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 1);
    assert_eq!(
        subscribers[0].source_columns, source_columns,
        "the persisted type map must survive a fresh read, not just the in-memory return value"
    );
}

/// Issue #69's batched read path: `transforms_for_source` now decodes every
/// subscriber's `source_columns` via a single `left join lateral
/// jsonb_each_text(...)` query rather than one query per row. A `left`
/// (not inner) join matters specifically for a definition whose
/// `source_columns` is empty — a literal-only field references no source
/// column at all — since an inner join would drop such a definition from
/// the result entirely instead of returning it with zero entries.
#[tokio::test]
async fn a_definition_with_no_source_columns_survives_the_left_join_read() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "widgets").await;

    let def = create_definition(
        &db.pool,
        "TRANSFORM constants FROM widgets SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("a literal-only definition needs no source columns");
    assert!(def.source_columns.is_empty());

    let subscribers = transforms_for_source(&db.pool, "widgets")
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 1);
    assert_eq!(subscribers[0].id, def.id);
    assert!(
        subscribers[0].source_columns.is_empty(),
        "a definition with no source columns must still come back, with an empty map, \
         not disappear from the result"
    );
}

/// Issue #69's by-id grouping: with more than one definition subscribed to
/// the same source table, the single lateral-joined query's rows (one per
/// definition per source-column entry) must regroup correctly per
/// definition rather than smearing one definition's `source_columns` into
/// another's.
#[tokio::test]
async fn transforms_for_source_groups_multiple_subscribers_by_id() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    let totals_columns = HashMap::from([
        ("price".to_string(), ValueType::Numeric),
        ("tax".to_string(), ValueType::Numeric),
    ]);
    let first = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &totals_columns,
    )
    .await
    .expect("first definition");

    let labels_columns = HashMap::from([("label".to_string(), ValueType::Text)]);
    let second = create_definition(
        &db.pool,
        "TRANSFORM order_labels FROM orders SELECT label AS out",
        &labels_columns,
    )
    .await
    .expect("second definition against the same source table");

    assert!(first.id < second.id);

    let subscribers = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 2);

    // `order by t.id` in the query must be reflected in the grouped result.
    assert_eq!(subscribers[0].id, first.id);
    assert_eq!(subscribers[0].def.target, "order_totals");
    assert_eq!(subscribers[0].source_columns, totals_columns);

    assert_eq!(subscribers[1].id, second.id);
    assert_eq!(subscribers[1].def.target, "order_labels");
    assert_eq!(subscribers[1].source_columns, labels_columns);
}

#[tokio::test]
async fn creating_a_new_definition_bumps_the_source_tables_version() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;
    create_bare_source_table(&db.pool, "customers").await;

    let first = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("first definition");
    assert_eq!(first.source_version, 1);

    let second = create_definition(
        &db.pool,
        "TRANSFORM order_discounts FROM orders SELECT price AS discount",
        &columns(&["price"]),
    )
    .await
    .expect("second definition against the same source table");
    assert_eq!(second.source_version, 2);

    // A definition against an unrelated source table starts its own,
    // independent version counter.
    let unrelated = create_definition(
        &db.pool,
        "TRANSFORM customer_names FROM customers SELECT name AS full_name",
        &columns(&["name"]),
    )
    .await
    .expect("definition against a different source table");
    assert_eq!(unrelated.source_version, 1);
}

#[tokio::test]
async fn a_column_cycle_within_a_target_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT y AS x, x AS y",
        &HashMap::new(),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::Cycle { .. }) => {}
        other => panic!("expected a Cycle validation error, got {other:?}"),
    }
}

/// Issue #63's end-to-end passthrough bar: a `Text`-typed source column
/// parses, validates, and is stored as a calculated field with no operator
/// applied to it.
#[tokio::test]
async fn a_text_column_passthrough_is_stored_and_retrievable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "widgets").await;

    let source_columns = HashMap::from([("label".to_string(), ValueType::Text)]);
    let def = create_definition(
        &db.pool,
        "TRANSFORM labels FROM widgets SELECT label AS out",
        &source_columns,
    )
    .await
    .expect("text passthrough should be a valid definition");

    assert_eq!(def.def.target, "labels");
}

#[tokio::test]
async fn a_provided_source_column_map_cannot_invent_a_live_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute("create table source_column_missing (id integer primary key)")
        .await
        .expect("create source table");
    drop(client);

    let err = create_definition(
        &db.pool,
        "TRANSFORM source_column_missing_target FROM source_column_missing SELECT imaginary AS total",
        &columns(&["imaginary"]),
    )
    .await
    .expect_err("a caller-provided map cannot claim a missing PostgreSQL column");

    assert!(matches!(
        err,
        CatalogError::SourceColumnNotFound { column, .. } if column == "imaginary"
    ));
}

/// Issue #63's type-mismatch bar: `text_col + 1` must be rejected at
/// validation time with a clear [`ValidationError::TypeMismatch`], not a
/// panic.
#[tokio::test]
async fn adding_a_text_column_to_a_number_is_rejected_at_validation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let source_columns = HashMap::from([("label".to_string(), ValueType::Text)]);
    let err = create_definition(
        &db.pool,
        "TRANSFORM labels FROM widgets SELECT label + 1 AS out",
        &source_columns,
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::TypeMismatch {
            field,
            expected,
            found,
        }) => {
            assert_eq!(field, "out");
            assert_eq!(expected, ValueType::Numeric);
            assert_eq!(found, ValueType::Text);
        }
        other => panic!("expected a TypeMismatch validation error, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unresolved_column_reference_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT mystery_column AS x",
        &columns(&["price"]),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::UnresolvedColumn { column, .. }) => {
            assert_eq!(column, "mystery_column");
        }
        other => panic!("expected an UnresolvedColumn validation error, got {other:?}"),
    }
}

#[tokio::test]
async fn cross_join_and_partial_data_definitions_are_rejected_cleanly() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let cross_join = create_definition(
        &db.pool,
        "TRANSFORM t FROM s JOIN other ON s.id = other.id SELECT a AS x",
        &HashMap::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(cross_join, CatalogError::Parse(_)));

    let partial_data = create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a AS x WHERE a = b",
        &columns(&["a"]),
    )
    .await
    .unwrap_err();
    assert!(matches!(partial_data, CatalogError::Parse(_)));

    // None of the rejected attempts should have left a row behind.
    let subscribers = transforms_for_source(&db.pool, "s")
        .await
        .expect("query mapping");
    assert!(subscribers.is_empty());
}

/// Unlike `JOIN`, a `<rel>.<column>` relationship path is now real grammar
/// (issue #25) rather than a parse-time rejection, so it parses successfully
/// and instead fails at validation (the validator has no relationship
/// resolution yet — that's a separate, later issue) — exercising that the
/// catalog's parse-then-validate pipeline routes it through validation
/// rather than short-circuiting at parse time the way `JOIN` still does.
#[tokio::test]
async fn a_relationship_path_definition_is_rejected_at_validation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT product.category_name AS x",
        &HashMap::new(),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::UnsupportedRelationshipPath {
            field,
            rel,
            column,
        }) => {
            assert_eq!(field, "x");
            assert_eq!(rel, "product");
            assert_eq!(column, "category_name");
        }
        other => panic!("expected an UnsupportedRelationshipPath validation error, got {other:?}"),
    }

    // The rejected attempt should not have left a row behind.
    let subscribers = transforms_for_source(&db.pool, "s")
        .await
        .expect("query mapping");
    assert!(subscribers.is_empty());
}

/// Unlike `JOIN`/relationship paths, `GROUP BY` is a real, now-supported
/// construct, so an aggregate definition parses successfully and instead
/// fails at validation if its grouping column isn't a real source column —
/// exercising that the catalog's parse-then-validate pipeline routes an
/// aggregate definition through validation rather than short-circuiting at
/// parse time the way the still-unsupported constructs above do.
#[tokio::test]
async fn an_aggregate_definition_with_an_unresolvable_group_by_column_is_rejected_at_validation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM t FROM s GROUP BY a SELECT a AS x",
        &HashMap::new(),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::UnresolvedGroupByColumn { column }) => {
            assert_eq!(column, "a");
        }
        other => panic!("expected an UnresolvedGroupByColumn validation error, got {other:?}"),
    }

    let subscribers = transforms_for_source(&db.pool, "s")
        .await
        .expect("query mapping");
    assert!(subscribers.is_empty());
}

#[tokio::test]
async fn source_to_transform_mapping_reflects_a_newly_created_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    let before = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping before creation");
    assert!(before.is_empty());

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("create definition");

    let after = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping after creation");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].def.target, "order_totals");

    // A different source table's mapping is unaffected.
    let unrelated = transforms_for_source(&db.pool, "customers")
        .await
        .expect("query unrelated mapping");
    assert!(unrelated.is_empty());
}

/// Issue #80: `CatalogError::Db`'s `Display` must surface the real Postgres
/// error text (e.g. the duplicate-key detail), not just the bare "db error"
/// `tokio_postgres::Error`'s own `Display` prints on its own.
#[tokio::test]
async fn a_duplicate_target_table_surfaces_the_underlying_postgres_detail() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;
    create_bare_source_table(&db.pool, "customers").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("first definition should be stored");

    let err = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM customers SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .unwrap_err();

    assert!(matches!(err, CatalogError::Db(_)));
    let message = err.to_string();
    assert!(
        message.contains("duplicate key value violates unique constraint"),
        "expected the underlying Postgres detail in the error message, got: {message}"
    );
    assert!(
        message.contains("transform_definitions_target_table_key"),
        "expected the violated constraint's name in the error message, got: {message}"
    );
}

/// ADR-0007: an unqualified source follows the definition connection's
/// search path, while same-named relations in two schemas remain distinct
/// OID-backed sources with independent catalog versions.
#[tokio::test]
async fn source_bindings_distinguish_same_named_relations_across_schemas() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create schema catalog_oid_other;
             create table public.catalog_oid_foo (id serial primary key, price numeric);
             create table catalog_oid_other.catalog_oid_foo (id serial primary key, price numeric)",
        )
        .await
        .expect("create fully-qualified source tables");
    drop(client);

    let public = create_definition(
        &db.pool,
        "TRANSFORM catalog_oid_public_target FROM catalog_oid_foo SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("configured search_path resolves public source");
    assert_eq!(public.source.schema, "public");
    assert_eq!(public.source.name, "catalog_oid_foo");
    assert_eq!(public.source.qualified(), "public.catalog_oid_foo");

    // This test uses one pool connection. Changing the connection setting
    // exercises PostgreSQL's ordinary unqualified-name resolution again.
    let client = db.pool.get().await.expect("reuse pool connection");
    client
        .batch_execute("set search_path to catalog_oid_other, trellis, public")
        .await
        .expect("switch definition search path");
    drop(client);

    let other = create_definition(
        &db.pool,
        "TRANSFORM catalog_oid_other_target FROM catalog_oid_foo SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("alternate search_path resolves other source");
    assert_eq!(other.source.schema, "catalog_oid_other");
    assert_ne!(public.source.oid, other.source.oid);

    let public_binding = resolve_source_relation(&db.pool, "public.catalog_oid_foo")
        .await
        .expect("resolve public source by qualified name");
    let other_binding = resolve_source_relation(&db.pool, "catalog_oid_other.catalog_oid_foo")
        .await
        .expect("resolve other source by qualified name");
    assert_eq!(public_binding.oid, public.source.oid);
    assert_eq!(other_binding.oid, other.source.oid);
    assert_ne!(public_binding.oid, other_binding.oid);

    assert_eq!(
        source_table_version_by_oid(&db.pool, public_binding.oid)
            .await
            .expect("public source version"),
        Some(1)
    );
    assert_eq!(
        source_table_version_by_oid(&db.pool, other_binding.oid)
            .await
            .expect("other source version"),
        Some(1)
    );
    assert_eq!(
        transforms_for_source_oid(&db.pool, public_binding.oid)
            .await
            .expect("public OID lookup")
            .len(),
        1
    );
    assert_eq!(
        transforms_for_source_oid(&db.pool, other_binding.oid)
            .await
            .expect("other OID lookup")
            .len(),
        1
    );
}

/// A source OID continues to name the same object after its presentation name
/// changes. Both direct OID resolution and catalog enumeration refresh the
/// schema/name metadata from PostgreSQL rather than re-resolving DSL text.
#[tokio::test]
async fn source_relation_metadata_refreshes_after_rename_and_schema_move() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create schema catalog_oid_moved;
             create table public.catalog_oid_original (id serial primary key, price numeric)",
        )
        .await
        .expect("create source relation");
    drop(client);

    let created = create_definition(
        &db.pool,
        "TRANSFORM catalog_oid_move_target FROM catalog_oid_original SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("create definition");

    let client = db.pool.get().await.expect("reuse pool connection");
    client
        .batch_execute(
            "alter table public.catalog_oid_original rename to catalog_oid_renamed;
             alter table public.catalog_oid_renamed set schema catalog_oid_moved",
        )
        .await
        .expect("rename and move bound source");
    drop(client);

    let resolved = source_relation_by_oid(&db.pool, created.source.oid)
        .await
        .expect("resolve bound OID")
        .expect("bound source still exists");
    assert_eq!(resolved.oid, created.source.oid);
    assert_eq!(resolved.schema, "catalog_oid_moved");
    assert_eq!(resolved.name, "catalog_oid_renamed");
    assert_eq!(
        resolved.qualified(),
        "catalog_oid_moved.catalog_oid_renamed"
    );

    assert_eq!(
        all_source_relations(&db.pool)
            .await
            .expect("enumerate current source bindings"),
        vec![resolved.clone()]
    );

    let loaded = transforms_for_source_oid(&db.pool, created.source.oid)
        .await
        .expect("load definition by stable OID");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].source, resolved);
}

/// A dropped OID-bound source remains a catalog error, not an absent
/// subscriber that could be mistaken for a valid empty mapping.
#[tokio::test]
async fn dropped_bound_source_is_loud_on_oid_definition_lookup() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table public.catalog_oid_dropped (id serial primary key, price numeric)",
        )
        .await
        .expect("create source relation");
    drop(client);

    let created = create_definition(
        &db.pool,
        "TRANSFORM catalog_oid_dropped_target FROM catalog_oid_dropped SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("create definition");

    let client = db.pool.get().await.expect("reuse pool connection");
    client
        .batch_execute("drop table public.catalog_oid_dropped")
        .await
        .expect("drop bound source relation");
    drop(client);

    assert!(matches!(
        transforms_for_source_oid(&db.pool, created.source.oid).await,
        Err(CatalogError::BoundSourceRelationMissing { oid }) if oid == created.source.oid
    ));
    assert!(
        all_source_relations(&db.pool)
            .await
            .expect("list current source relations")
            .is_empty(),
        "dropped OID bindings are omitted rather than rebound by source text"
    );
}
