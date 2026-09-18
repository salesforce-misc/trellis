//! Integration tests for the transform-definition catalog (issue #23),
//! run against a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{
    CatalogError, DdlError, ValidationError, ValueType, all_source_tables, create_definition,
    create_relationship, install_definition, transforms_for_source,
};

fn columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|s| (s.to_string(), ValueType::Numeric))
        .collect()
}

/// Bare `name` qualified under [`DEFAULT_SCHEMA`] — where
/// `create_bare_source_table`/`create_authors_and_posts` actually land a
/// bare `create table` (the pool's ambient `search_path`, Trellis schema
/// first). `transforms_for_source` now requires its argument already
/// fully-qualified (issue #74, ADR-0007); a bare name silently matches
/// nothing rather than erroring.
fn qualified(name: &str) -> String {
    format!("{DEFAULT_SCHEMA}.{name}")
}

/// Creates a minimal backing relation for a definition's source table
/// (issue #23's backfill enumerates it for real, via a live `regclass`/
/// catalog lookup) — a bare PK column is enough, since `validate()` checks
/// column references against the passed-in `source_columns` map, not the
/// live schema. Left unqualified so it lands via the pool's ambient
/// `search_path` (Trellis schema first), matching the schema
/// `create_definition` assumes for `def.source` today.
async fn create_bare_source_table(pool: &trellis::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!("create table {name} (id serial primary key)"))
        .await
        .expect("create bare source table");
}

/// A table usable as either endpoint of a relationship declared by
/// [`create_relationship`] (issue #65's `all_source_tables` tests): `id` is
/// a serial primary key, suitable as a relationship's unique `to_col`
/// (cardinality `ToOne`, avoiding the to-many replica-identity requirement
/// these tests don't care about); `fk_col` is a plain, non-unique integer
/// column of the same type family, suitable as a relationship's `from_col`.
/// `REPLICA IDENTITY FULL` unconditionally (issue #129, epic #127): several
/// of this file's tests chain this table as the to-side of a to-one
/// relationship, which now requires it regardless of whether the table is
/// used as a from-side or to-side in any given test — harmless either way.
async fn create_bare_relationship_table(pool: &trellis::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (id serial primary key, fk_col integer); \
             alter table {name} replica identity full"
        ))
        .await
        .expect("create bare relationship table");
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

    let subscribers = transforms_for_source(&db.pool, &qualified("orders"))
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 1);
    assert_eq!(subscribers[0].id, def.id);
    assert_eq!(subscribers[0].def.target, "order_totals");
}

/// Issue #72 / ADR-0007: `transform_definitions.source_table` (and its
/// `source_table_versions` counterpart) must persist the *fully-qualified*
/// `schema.table` identity a bare `FROM` clause resolves to — not the bare
/// spelling the definition text names. `posts` is created explicitly in
/// `public` here, a schema that isn't first on the pool's own `search_path`
/// (`create_bare_source_table` lands a table in the Trellis schema instead —
/// see its own doc comment), so a definition resolving it correctly to
/// `public.posts` exercises real search-path resolution rather than just
/// echoing back whatever schema an unqualified `CREATE TABLE` would have
/// landed in by default.
///
/// `def.def.source` (the in-memory [`trellis::Definition`] returned by
/// [`create_definition`]) stays bare — it's re-parsed straight from
/// `definition_text`, and this definition's own `FROM` clause is bare too
/// (see [`trellis::defs::TransformDef`]'s own doc comment for why `source`
/// never carries a dotted spelling even for a definition that *did* qualify
/// it explicitly, issue #76) — so this test reads
/// `transform_definitions.source_table` back directly to observe the
/// persisted identity, rather than trusting the returned `Definition`.
#[tokio::test]
async fn source_table_is_persisted_fully_qualified() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute("create table public.posts (id serial primary key, title text)")
        .await
        .expect("create public.posts");

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("title".to_string(), ValueType::Text)]);
    let def = create_definition(
        &db.pool,
        "TRANSFORM post_titles FROM posts SELECT title AS out",
        &source_columns,
    )
    .await
    .expect("valid definition should be stored");
    // The returned `Definition` re-parses `definition_text`; this bare `FROM
    // posts` never carries a dotted spelling regardless of what's persisted
    // (see this test's own doc comment).
    assert_eq!(def.def.source, "posts");

    let source_table: String = client
        .query_one(
            "select source_table from transform_definitions where id = $1",
            &[&def.id],
        )
        .await
        .expect("read back the persisted definition")
        .get(0);
    assert_eq!(source_table, "public.posts");

    let version: i64 = client
        .query_one(
            "select version from source_table_versions where source_table = $1",
            &[&source_table],
        )
        .await
        .expect("source_table_versions is keyed by the same qualified identity")
        .get(0);
    assert_eq!(version, 1);
}

/// Issue #73 / ADR-0007's mirror of `source_table_is_persisted_fully_qualified`
/// above, for the target side: `transform_definitions.target_table` must
/// persist `def.target` resolved to `{Config::target_schema}.{def.target}`,
/// not the bare spelling the definition text names. Unlike the source side,
/// the target schema is never search-path-resolved — it's `Config::target_schema`,
/// `"public"` by default (`DEFAULT_TARGET_SCHEMA`, what `testkit`'s pools use
/// unless overridden) — so this doesn't need a non-default-schema table to
/// prove real resolution happened; it just needs to prove the schema prefix
/// is there at all.
#[tokio::test]
async fn target_table_is_persisted_fully_qualified() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "posts").await;

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("title".to_string(), ValueType::Text)]);
    let def = create_definition(
        &db.pool,
        "TRANSFORM post_titles FROM posts SELECT title AS out",
        &source_columns,
    )
    .await
    .expect("valid definition should be stored");
    // The returned `Definition` re-parses `definition_text`; this bare
    // `TRANSFORM post_titles` never carries a dotted spelling regardless of
    // what's persisted, exactly like `def.source` above.
    assert_eq!(def.def.target, "post_titles");

    let client = db.pool.get().await.expect("get connection");
    let target_table: String = client
        .query_one(
            "select target_table from transform_definitions where id = $1",
            &[&def.id],
        )
        .await
        .expect("read back the persisted definition")
        .get(0);
    assert_eq!(target_table, "public.post_titles");
}

/// Issue #73's chained/multi-hop coverage: once both the source and target
/// sides are qualified (issues #72 and #73 together), a second definition
/// chained off a first definition's *target* must still resolve correctly —
/// the second definition's `resolve_source_schema_in_txn` walk over its bare
/// `FROM` clause finds the first definition's physical target table via
/// `search_path` (`pool::session_bootstrap` pins the configured target
/// schema onto it) exactly as it would any other source, and the two
/// definitions' persisted `target_table`/`source_table` columns land on the
/// identical qualified string — proving the chain's identity actually
/// matches end to end, not just that each definition independently
/// qualified its own name the same way.
#[tokio::test]
async fn a_definitions_target_table_matches_a_chained_definitions_source_table() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "a").await;

    let a_def = create_definition(
        &db.pool,
        "TRANSFORM b FROM a SELECT id AS total",
        &HashMap::from([("id".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("a -> b definition should be stored");

    // `create_definition` never issues target-table DDL itself (`install_definition`'s
    // job) — materialize `b` for real before chaining `c` off of it, mirroring
    // `defs_edges.rs`'s `materialize_chained_target` helper.
    let pk = trellis::defs::require_single_column_pk(
        trellis::defs::source_primary_key(&db.pool, "a")
            .await
            .expect("introspect a's primary key"),
        "a",
    )
    .expect("single-column pk");
    let b_def = trellis::defs::parse("TRANSFORM b FROM a SELECT id AS total")
        .expect("parse b's definition");
    trellis::defs::create_target_table(
        &db.pool,
        &b_def,
        "public",
        &pk,
        &HashMap::from([("id".to_string(), ValueType::Numeric)]),
        &b_def.source,
    )
    .await
    .expect("materialize b's target table");

    let c_def = create_definition(
        &db.pool,
        "TRANSFORM c FROM b SELECT total AS total_again",
        &HashMap::from([("total".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("b -> c definition should be stored");

    let client = db.pool.get().await.expect("get connection");
    let a_target_table: String = client
        .query_one(
            "select target_table from transform_definitions where id = $1",
            &[&a_def.id],
        )
        .await
        .expect("read back a's persisted definition")
        .get(0);
    let c_source_table: String = client
        .query_one(
            "select source_table from transform_definitions where id = $1",
            &[&c_def.id],
        )
        .await
        .expect("read back c's persisted definition")
        .get(0);

    assert_eq!(a_target_table, "public.b");
    assert_eq!(
        c_source_table, a_target_table,
        "c's source and a's target must resolve to the identical qualified \
         identity for the chain to actually connect"
    );
}

/// Issue #76 / ADR-0007 grammar clause 4: an explicit `FROM <schema>.<source>`
/// resolves to *that exact relation*, never a `search_path` walk. Proven
/// here by deliberately creating two same-named `orders` tables — one that
/// bare resolution would win (landed in the Trellis-pinned schema, first on
/// `search_path` — see `create_bare_source_table`'s own doc comment) and one
/// in a schema nowhere near the front of the path — and confirming the
/// explicitly-qualified spelling still resolves to the *non-default* one.
/// If explicit qualification silently fell back to a `search_path` walk
/// (the #72 bare-name behavior), this would instead persist the
/// Trellis-schema `orders`, not `custom.orders`.
#[tokio::test]
async fn an_explicitly_qualified_source_resolves_to_that_exact_relation_not_search_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    // Lands in the Trellis-pinned schema (first on `search_path`) — the
    // table bare resolution would pick.
    create_bare_source_table(&db.pool, "orders").await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create schema custom; \
             create table custom.orders (id serial primary key, price numeric)",
        )
        .await
        .expect("create custom.orders");

    let def = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM custom.orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("explicitly-qualified source should resolve to custom.orders");
    // The returned `Definition` re-parses `definition_text` — `def.source`
    // itself always stays bare (see `TransformDef`'s own doc comment), but
    // `explicit_source_schema` carries the qualification through.
    assert_eq!(def.def.source, "orders");
    assert_eq!(def.def.explicit_source_schema, Some("custom".to_string()));

    let source_table: String = client
        .query_one(
            "select source_table from transform_definitions where id = $1",
            &[&def.id],
        )
        .await
        .expect("read back the persisted definition")
        .get(0);
    assert_eq!(
        source_table, "custom.orders",
        "must resolve to the explicitly-named schema, not whichever schema \
         search_path would have picked for the bare name"
    );
}

/// The rejection twin of the test above: an explicit `FROM <schema>.<source>`
/// whose named schema doesn't actually contain a table by that name is
/// rejected with [`ValidationError::QualifiedSourceTableNotFound`], not
/// silently falling back to a `search_path` walk that might resolve some
/// *other* `orders` table instead.
#[tokio::test]
async fn an_explicitly_qualified_source_naming_a_schema_without_that_table_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    // A same-named bare `orders` exists (and would win any search_path
    // walk), but `custom` itself has no `orders` table at all.
    create_bare_source_table(&db.pool, "orders").await;
    {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("create schema custom")
            .await
            .expect("create the custom schema");
    }

    let err = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM custom.orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::QualifiedSourceTableNotFound { schema, table }) => {
            assert_eq!(schema, "custom");
            assert_eq!(table, "orders");
        }
        other => panic!("expected QualifiedSourceTableNotFound, got: {other:?}"),
    }

    let client = db.pool.get().await.expect("get connection");
    let count: i64 = client
        .query_one("select count(*) from transform_definitions", &[])
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(count, 0, "the rejected definition must not persist");
}

/// Issue #76's own follow-up to
/// `a_target_table_colliding_on_bare_suffix_under_a_different_schema_is_rejected`
/// (issue #73's guard): an explicitly-qualified `TRANSFORM <schema>.<target>`
/// must trip the very same [`CatalogError::TargetTableSuffixCollision`] guard
/// a resolved-bare target under a different `Config::target_schema` does —
/// the check operates on the final qualified string and the bare suffix
/// alone, with no branch on *how* the qualification was produced, so this
/// proves that in practice rather than just by reading the code. Unlike the
/// bare-resolution version of this test, `create_definition` never runs
/// target-table DDL itself, so `custom.foo` is materialized by hand first
/// (mirroring `a_definitions_target_table_matches_a_chained_definitions_source_table`'s
/// `create_target_table` call) — otherwise the new
/// `QualifiedTargetTableNotFound` existence check (also issue #76) would
/// reject it before the collision guard is ever reached.
#[tokio::test]
async fn an_explicitly_qualified_target_still_triggers_the_suffix_collision_guard() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    create_definition(
        &db.pool,
        "TRANSFORM foo FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("first definition (public.foo) should be stored");

    {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("create schema custom")
            .await
            .expect("create the second target schema");
    }

    // Materialize `custom.foo` by hand — `create_definition` (the ring-path
    // entry point) assumes its caller already created the physical target
    // table, exactly like the chained-definition test above.
    let pk = trellis::defs::require_single_column_pk(
        trellis::defs::source_primary_key(&db.pool, "orders")
            .await
            .expect("introspect orders' primary key"),
        "orders",
    )
    .expect("single-column pk");
    let custom_foo_def =
        trellis::defs::parse("TRANSFORM custom.foo FROM orders SELECT price AS total")
            .expect("parse the explicitly-qualified definition");
    trellis::defs::create_target_table(
        &db.pool,
        &custom_foo_def,
        "custom",
        &pk,
        &columns(&["price"]),
        &custom_foo_def.source,
    )
    .await
    .expect("materialize custom.foo");

    let err = create_definition(
        &db.pool,
        "TRANSFORM custom.foo FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::TargetTableSuffixCollision {
            target,
            requested,
            existing,
        } => {
            assert_eq!(target, "foo");
            assert_eq!(requested, "custom.foo");
            assert_eq!(existing, Some("public.foo".to_string()));
        }
        other => panic!("expected TargetTableSuffixCollision, got: {other:?}"),
    }

    let client = db.pool.get().await.expect("get connection");
    let count: i64 = client
        .query_one(
            "select count(*) from transform_definitions where split_part(target_table, '.', 2) = 'foo'",
            &[],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(count, 1, "the rejected second definition must not persist");
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

    let subscribers = transforms_for_source(&db.pool, &qualified("orders"))
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

    let subscribers = transforms_for_source(&db.pool, &qualified("widgets"))
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

    let subscribers = transforms_for_source(&db.pool, &qualified("orders"))
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

/// A `<rel>.<column>` relationship path is real grammar (issue #25) that
/// parses successfully and is now validated against catalog-resolved
/// relationship metadata (issue #40). With no relationship named `product`
/// declared, the head is unknown, so validation rejects it with
/// `UnknownRelationship` — naming the offending field — rather than the
/// old blanket "relationship paths are unsupported" rejection. This exercises
/// that the catalog's parse-then-validate pipeline resolves relationships and
/// routes an unknown one through validation, not a parse-time short-circuit
/// the way `JOIN` still does.
#[tokio::test]
async fn a_relationship_path_to_an_unknown_relationship_is_rejected_at_validation() {
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
        CatalogError::Validate(ValidationError::UnknownRelationship { field, rel }) => {
            assert_eq!(field, "x");
            assert_eq!(rel, "product");
        }
        other => panic!("expected an UnknownRelationship validation error, got {other:?}"),
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

    let before = transforms_for_source(&db.pool, &qualified("orders"))
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

    let after = transforms_for_source(&db.pool, &qualified("orders"))
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

/// Reviewer follow-up to issue #73 (see
/// `CatalogError::TargetTableSuffixCollision`'s own doc comment):
/// qualifying `target_table` narrowed its `unique` constraint to the
/// *qualified* spelling only, so two live definitions could otherwise
/// coexist as `public.foo` and `custom.foo` — most plausibly from
/// `Config::target_schema` changing between deploys and an operator
/// redeclaring a same-named `TRANSFORM ... TARGET foo` under it. Every
/// `split_part(target_table, '.', 2)`-keyed read site downstream
/// (`definition_by_target`, `dependents_of`, `app.rs`'s status/quarantine
/// polls, `generative`'s `unsettled_definitions`) assumes that suffix is
/// globally unique, so `create_definition_inner` must reject the second
/// definition outright rather than let the collision persist. Reproduces
/// the "target schema changed between deploys" scenario directly, via a
/// second pool built with a different `target_schema` against the same
/// database, rather than mutating search_path mid-test.
#[tokio::test]
async fn a_target_table_colliding_on_bare_suffix_under_a_different_schema_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    create_definition(
        &db.pool,
        "TRANSFORM foo FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("first definition (public.foo) should be stored");

    {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("create schema custom")
            .await
            .expect("create the second target schema");
    }

    let custom_config = trellis::Config::from_dsn(db.dsn().to_string())
        .expect("valid dsn")
        .with_target_schema("custom")
        .expect("valid target schema");
    let custom_pool =
        trellis::Pool::new(&custom_config).expect("build pool with target_schema=custom");

    let err = create_definition(
        &custom_pool,
        "TRANSFORM foo FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::TargetTableSuffixCollision {
            target,
            requested,
            existing,
        } => {
            assert_eq!(target, "foo");
            assert_eq!(requested, "custom.foo");
            assert_eq!(existing, Some("public.foo".to_string()));
        }
        other => panic!("expected TargetTableSuffixCollision, got: {other:?}"),
    }

    // The rejected second definition must not have been persisted at all —
    // this is a write-path rejection, not a read-side workaround.
    let client = db.pool.get().await.expect("get connection");
    let count: i64 = client
        .query_one(
            "select count(*) from transform_definitions where split_part(target_table, '.', 2) = 'foo'",
            &[],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(count, 1, "the rejected second definition must not persist");
}

/// Reviewer follow-up to issue #73's own follow-up
/// (`transform_definitions_target_suffix_idx`,
/// `V23__transform_definitions_target_suffix_idx.sql`): the TOCTOU race the
/// index exists to close, reproduced for real rather than merely asserted by
/// inspection. `create_definition_inner`'s pre-check
/// (`a_target_table_colliding_on_bare_suffix_under_a_different_schema_is_rejected`
/// above) only ever sees its own transaction's snapshot, so it cannot see a
/// concurrent transaction's still-uncommitted insert of a colliding
/// definition — this test holds exactly such an uncommitted insert open on a
/// raw connection while a real `create_definition` call runs concurrently,
/// so its pre-check provably passes (the colliding row isn't visible yet)
/// and only the final `insert` — which must then block on Postgres's own
/// unique-index conflict resolution until the raw connection's transaction
/// resolves — discovers the collision. Committing the raw connection's
/// transaction (rather than rolling it back) makes that final insert lose
/// the race deterministically, exercising exactly the "the check passed but
/// the insert now races the index" path
/// `is_target_suffix_index_violation`/`CatalogError::TargetTableSuffixCollision`
/// (`existing: None`) exist for.
///
/// Bypasses `create_definition_inner`'s own pre-check entirely for the raw
/// insert (a direct `insert into transform_definitions`, not a second
/// `create_definition` call) specifically so nothing about *this* test
/// relies on the pre-check's behavior — only the DB-level index is under
/// test here.
#[tokio::test]
async fn a_concurrent_insert_racing_the_target_suffix_index_is_translated_to_a_collision_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    // Establishes the `source_table_versions` row the raw insert below needs
    // to satisfy `transform_definitions.source_table`'s FK — a real
    // definition already exists for `orders`'s qualified identity, whatever
    // its own target happens to be. Read back the qualified spelling rather
    // than assuming `public.orders`: `create_bare_source_table` deliberately
    // leaves the table unqualified so it lands via the pool's ambient
    // search_path (`trellis` schema first — see that helper's own doc
    // comment), not necessarily `public`.
    let seed_def = create_definition(
        &db.pool,
        "TRANSFORM bar FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("seed definition should be stored");
    let qualified_orders: String = db
        .pool
        .get()
        .await
        .expect("get connection")
        .query_one(
            "select source_table from transform_definitions where id = $1",
            &[&seed_def.id],
        )
        .await
        .expect("read back the seed definition's qualified source")
        .get(0);

    {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("create schema custom")
            .await
            .expect("create the second target schema");
    }

    // A raw connection, outside the pool, holding open a transaction that
    // has inserted `public.foo` but not yet committed — standing in for
    // "another session's concurrent `create_definition` call", except here
    // the timing is under this test's own control rather than left to
    // chance.
    let (raw_client, raw_connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("raw connect");
    tokio::spawn(async move {
        let _ = raw_connection.await;
    });
    raw_client
        .batch_execute(&format!(
            "set search_path to {}, public",
            trellis::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("set search_path on raw connection");
    raw_client
        .batch_execute("begin")
        .await
        .expect("begin raw transaction");
    raw_client
        .execute(
            "insert into transform_definitions
                (target_table, source_table, source_version, definition_text)
             values ('public.foo', $1, 1, 'TRANSFORM foo FROM orders SELECT price AS total')",
            &[&qualified_orders],
        )
        .await
        .expect("raw, uncommitted insert of the colliding row");

    let custom_config = trellis::Config::from_dsn(db.dsn().to_string())
        .expect("valid dsn")
        .with_target_schema("custom")
        .expect("valid target schema");
    let custom_pool =
        trellis::Pool::new(&custom_config).expect("build pool with target_schema=custom");

    let racing_call = tokio::spawn(async move {
        create_definition(
            &custom_pool,
            "TRANSFORM foo FROM orders SELECT price AS total",
            &columns(&["price"]),
        )
        .await
    });

    // Wait until the spawned `create_definition` call's own insert is
    // actually blocked on the raw connection's uncommitted conflicting index
    // entry (`pg_locks.granted = false`) before committing — committing too
    // early (before the racing call even reaches its insert) would let the
    // pre-check see the row instead and take the ordinary, non-racing path
    // this test isn't exercising; there's nothing to wait on if the race
    // never actually engages.
    let observer = db.pool.get().await.expect("get connection");
    let mut waited = std::time::Duration::ZERO;
    let poll_interval = std::time::Duration::from_millis(20);
    let timeout = std::time::Duration::from_secs(10);
    loop {
        let blocked: i64 = observer
            .query_one(
                "select count(*) from pg_locks l
                 join pg_stat_activity a on l.pid = a.pid
                 where not l.granted and a.datname = current_database()",
                &[],
            )
            .await
            .expect("poll pg_locks")
            .get(0);
        if blocked > 0 {
            break;
        }
        if waited >= timeout {
            panic!(
                "timed out waiting for the racing create_definition call's insert \
                 to block on the raw connection's uncommitted row"
            );
        }
        tokio::time::sleep(poll_interval).await;
        waited += poll_interval;
    }
    drop(observer);

    raw_client
        .batch_execute("commit")
        .await
        .expect("commit the raw connection's transaction, unblocking the racing insert");

    let err = racing_call
        .await
        .expect("racing create_definition task should not panic")
        .unwrap_err();

    match err {
        CatalogError::TargetTableSuffixCollision {
            target,
            requested,
            existing,
        } => {
            assert_eq!(target, "foo");
            assert_eq!(requested, "custom.foo");
            assert_eq!(
                existing, None,
                "the insert-time race path has no live transaction left to \
                 look the colliding row's identity up in"
            );
        }
        other => panic!("expected TargetTableSuffixCollision, got: {other:?}"),
    }

    // The racing call must not have persisted anything either.
    let client = db.pool.get().await.expect("get connection");
    let count: i64 = client
        .query_one(
            "select count(*) from transform_definitions where split_part(target_table, '.', 2) = 'foo'",
            &[],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(
        count, 1,
        "only the raw connection's committed row should persist under this suffix"
    );
}

/// Issue #65, case 1: with no relationships declared at all, `all_source_tables`
/// must still behave exactly as it did pre-#65 — just the anchor
/// `source_table` of each registered transform. Issue #75, ADR-0007:
/// fully-qualified, not the bare suffix this returned before.
#[tokio::test]
async fn all_source_tables_with_no_relationships_returns_only_anchor_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("valid definition should be stored");

    let mut tables = all_source_tables(&db.pool).await.expect("query mapping");
    tables.sort();
    assert_eq!(tables, vec![qualified("orders")]);
}

/// Issue #65, case 2: a transform anchored on `a`, plus a relationship
/// `a -> b` (`a` is `from_table`, `b` is `to_table`), must pull `b` into the
/// result too — the CDC gap the issue reports, where a calculated field on
/// `a` reading a relationship path into `b` needs `b`'s writes captured.
#[tokio::test]
async fn all_source_tables_follows_a_single_relationship_hop() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_relationship_table(&db.pool, "a").await;
    create_bare_relationship_table(&db.pool, "b").await;

    create_definition(
        &db.pool,
        "TRANSFORM a_calc FROM a SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("valid definition should be stored");

    create_relationship(&db.pool, "RELATIONSHIP r1 FROM a.fk_col TO b.id")
        .await
        .expect("valid relationship should be stored");

    let mut tables = all_source_tables(&db.pool).await.expect("query mapping");
    tables.sort();
    assert_eq!(tables, vec![qualified("a"), qualified("b")]);
}

/// Issue #65, case 3: a multi-hop chain — relationship `a -> b` and
/// `b -> c` — with the transform anchored only on `a`, must resolve the
/// transitive closure, pulling in both `b` and `c`, not just the direct
/// hop.
#[tokio::test]
async fn all_source_tables_follows_a_multi_hop_relationship_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_relationship_table(&db.pool, "a").await;
    create_bare_relationship_table(&db.pool, "b").await;
    create_bare_relationship_table(&db.pool, "c").await;

    create_definition(
        &db.pool,
        "TRANSFORM a_calc FROM a SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("valid definition should be stored");

    create_relationship(&db.pool, "RELATIONSHIP r1 FROM a.fk_col TO b.id")
        .await
        .expect("a -> b relationship should be stored");
    create_relationship(&db.pool, "RELATIONSHIP r2 FROM b.fk_col TO c.id")
        .await
        .expect("b -> c relationship should be stored");

    let mut tables = all_source_tables(&db.pool).await.expect("query mapping");
    tables.sort();
    assert_eq!(tables, vec![qualified("a"), qualified("b"), qualified("c")]);
}

/// Issue #65, case 4: a relationship declared on a table that is not any
/// transform's `source_table` must not leak its `to_table` into the
/// result — only tables reachable from an actual registered transform's
/// anchor should appear. `x -> y` here is never seeded (neither `x` nor `y`
/// anchors a transform), so both must be absent even though the
/// relationship itself is validly stored; only `z`, the real transform's
/// anchor, should come back.
#[tokio::test]
async fn all_source_tables_does_not_leak_relationships_unreachable_from_any_transform() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_relationship_table(&db.pool, "x").await;
    create_bare_relationship_table(&db.pool, "y").await;
    create_bare_source_table(&db.pool, "z").await;

    create_relationship(&db.pool, "RELATIONSHIP r1 FROM x.fk_col TO y.id")
        .await
        .expect("valid relationship should be stored");

    create_definition(
        &db.pool,
        "TRANSFORM z_calc FROM z SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("valid definition should be stored");

    let tables = all_source_tables(&db.pool).await.expect("query mapping");
    assert_eq!(tables, vec![qualified("z")]);
}

/// Issue #75, ADR-0007: `all_source_tables` must return each source's own
/// *actual* persisted qualified name, not a bare suffix a caller then has to
/// re-guess against one assumed schema. Before this fix it returned the bare
/// `split_part(source_table, '.', 2)` suffix, and its one production caller
/// (`client::reconcile_source_tables`) re-qualified every result against
/// `Config::target_schema` (`"public"` by default) — wrong for a source
/// living anywhere else, including via issue #76's explicit-schema grammar.
///
/// Proven the same way issue #76's own
/// `an_explicitly_qualified_source_resolves_to_that_exact_relation_not_search_path`
/// proves explicit qualification itself: a same-named decoy table sits in
/// the schema the old bug would have guessed (`public`, `reconcile_source_tables`'s
/// assumed `target_schema`), while the real, registered source lives in
/// `custom`, named explicitly via `FROM custom.orders`. If the bare-suffix
/// bug were still present, this would return `public.orders` (the decoy) —
/// exactly the wrong table a publication reconcile would then add.
#[tokio::test]
async fn all_source_tables_returns_the_actual_schema_not_a_target_schema_guess() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");

    // The decoy: same bare name, sitting in `public` — the schema
    // `reconcile_source_tables` used to assume every source lived under.
    client
        .batch_execute("create table public.orders (id serial primary key, price numeric)")
        .await
        .expect("create decoy public.orders");
    // The real source: explicitly qualified into a schema that is neither
    // `public` nor the Trellis-pinned schema bare resolution would pick.
    client
        .batch_execute(
            "create schema custom; \
             create table custom.orders (id serial primary key, price numeric)",
        )
        .await
        .expect("create custom.orders");

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM custom.orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("explicitly-qualified source should resolve to custom.orders");

    let tables = all_source_tables(&db.pool).await.expect("query mapping");
    assert_eq!(
        tables,
        vec!["custom.orders".to_string()],
        "must return the actually-registered custom.orders, not a bare suffix a caller \
         would re-guess into the public.orders decoy"
    );
}

/// Issue #36's exact repro: a 1-1 transform with a pass-through field named
/// the same as the source column it reads (`author AS author`) must not be
/// rejected as a self-referencing cycle, even alongside other calculated
/// fields on the same target. `is_self_passthrough` in `validate.rs`
/// already exempts `column == field.name` when `column` is a source
/// column — this pins that exemption against the issue's literal schema
/// and DSL so a regression here fails loudly.
#[tokio::test]
async fn a_passthrough_field_sharing_its_source_columns_name_is_not_a_cycle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table authors (
                 id integer not null,
                 created_at timestamp with time zone default now() not null,
                 name character varying not null,
                 constraint authors_pkey primary key (id)
             );
             create table posts (
                 id integer not null,
                 created_at timestamp with time zone default now() not null,
                 title text,
                 body text,
                 author integer not null,
                 constraint posts_pkey primary key (id),
                 constraint posts_author_fkey foreign key (author)
                     references authors(id) on update cascade on delete cascade
             );",
        )
        .await
        .expect("seed authors and posts");
    drop(client);

    let source_columns: HashMap<String, ValueType> = HashMap::from([
        ("author".to_string(), ValueType::Numeric),
        ("body".to_string(), ValueType::Text),
    ]);

    create_definition(
        &db.pool,
        "TRANSFORM posts_calc FROM posts SELECT author AS author, \
         regexp_count(body, '(^|[^A-Za-z0-9_])') as word_count, \
         octet_length(body) as byte_size",
        &source_columns,
    )
    .await
    .expect("author AS author passthrough must not be rejected as a self-reference cycle");
}

/// Seeds the `authors`/`posts` schema from `poc/schema_dump.sql` (the same
/// DDL issue #36's test above uses) — the exact repro schema issue #47's
/// report is filed against.
async fn create_authors_and_posts(pool: &trellis::pool::Pool) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table authors (
                 id integer not null,
                 created_at timestamp with time zone default now() not null,
                 name character varying not null,
                 constraint authors_pkey primary key (id)
             );
             create table posts (
                 id integer not null,
                 created_at timestamp with time zone default now() not null,
                 title text,
                 body text,
                 author integer not null,
                 constraint posts_pkey primary key (id),
                 constraint posts_author_fkey foreign key (author)
                     references authors(id) on update cascade on delete cascade
             );",
        )
        .await
        .expect("seed authors and posts");
}

/// Issue #47: a 1-1 transform (`posts_calc`) against `posts` must succeed
/// regardless of `posts`'s replica identity — [`KeySpace::OneToOne`]
/// derivations are a pure function of the *current* row and never need an
/// old image.
#[tokio::test]
async fn a_one_to_one_transform_succeeds_regardless_of_replica_identity() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_authors_and_posts(&db.pool).await;

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("author".to_string(), ValueType::Numeric)]);

    create_definition(
        &db.pool,
        "TRANSFORM posts_calc FROM posts SELECT author AS author",
        &source_columns,
    )
    .await
    .expect("a 1-1 transform needs no old image, so it must succeed at default replica identity");
}

/// Issue #47's exact repro: an aggregate (`GROUP BY`) transform's
/// delta-maintenance path (`apply_aggregate.rs`) needs the source row's old
/// image on delete/update/re-parent to find which group to decrement — a
/// requirement `defs::catalog::create_definition` never checked, so defining
/// `posts_totals` (`GROUP BY author`) against `posts` at its default (PK-only)
/// replica identity must be rejected at define time, naming the exact `ALTER
/// TABLE posts REPLICA IDENTITY FULL;` fix — not silently accepted only to
/// corrupt totals later on a delete or non-key update.
#[tokio::test]
async fn an_aggregate_transform_against_default_replica_identity_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_authors_and_posts(&db.pool).await;

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("author".to_string(), ValueType::Numeric)]);

    // The 1-1 transform above is unaffected by `posts`'s replica identity
    // and should still succeed even though the aggregate attempt below will
    // be rejected.
    create_definition(
        &db.pool,
        "TRANSFORM posts_calc FROM posts SELECT author AS author",
        &source_columns,
    )
    .await
    .expect("1-1 transform should succeed regardless of replica identity");

    let err = create_definition(
        &db.pool,
        "TRANSFORM posts_totals FROM posts GROUP BY author SELECT author AS author, \
         COUNT(*) AS post_count",
        &source_columns,
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::ReplicaIdentityRequired(_) => {}
        other => panic!("expected ReplicaIdentityRequired, got {other:?}"),
    }
    let message = err.to_string();
    assert!(
        message.contains("ALTER TABLE posts REPLICA IDENTITY FULL;"),
        "expected the exact ALTER TABLE fix in the error message, got: {message}"
    );

    // The rejected attempt must not have left a row behind.
    let subscribers = transforms_for_source(&db.pool, &qualified("posts"))
        .await
        .expect("query mapping");
    assert_eq!(
        subscribers.len(),
        1,
        "only posts_calc should be registered; posts_totals must not have been persisted"
    );
    assert_eq!(subscribers[0].def.target, "posts_calc");
}

/// Issue #47: the same aggregate definition succeeds once `posts` has
/// `REPLICA IDENTITY FULL`, which puts every column (including `author`) into
/// delete/update pre-images.
#[tokio::test]
async fn an_aggregate_transform_against_replica_identity_full_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_authors_and_posts(&db.pool).await;
    {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("alter table posts replica identity full")
            .await
            .expect("set replica identity full");
    }

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("author".to_string(), ValueType::Numeric)]);

    create_definition(
        &db.pool,
        "TRANSFORM posts_totals FROM posts GROUP BY author SELECT author AS author, \
         COUNT(*) AS post_count",
        &source_columns,
    )
    .await
    .expect("aggregate transform with REPLICA IDENTITY FULL should be accepted");
}

/// Reviewer follow-up to issue #76: `assert_replica_identity_supports_aggregate`
/// (issue #47's guard, in `create_definition_inner`) used to check
/// `def.source` *bare* against `pg_class` via `to_regclass`'s own
/// `search_path` walk — running before `qualified_source` resolution a few
/// lines later, and never consulting `def.explicit_source_schema` at all.
/// For an aggregate definition with an explicit `FROM <schema>.<source>`,
/// that meant the check could silently examine the wrong relation whenever a
/// same-named table also existed earlier on `search_path`.
///
/// This is the dangerous direction: a decoy `orders` (landed in the
/// Trellis-pinned schema, first on `search_path` — see
/// `create_bare_source_table`'s own doc comment) has `REPLICA IDENTITY
/// FULL`, but the *real*, explicitly-qualified source `custom.orders` is
/// left at the default (PK-only) identity. A bare `to_regclass` lookup would
/// resolve to the decoy and wrongly *accept* this aggregate, silently
/// reintroducing issue #47's aggregate-corruption bug (delta-maintenance
/// can't recover the old row image on delete/non-key update) through this
/// issue's own new grammar. Checking the real, qualified source must instead
/// reject it.
#[tokio::test]
async fn an_aggregate_against_an_explicitly_qualified_source_is_rejected_despite_a_full_identity_decoy()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table orders (id serial primary key, customer integer not null); \
             alter table orders replica identity full; \
             create schema custom; \
             create table custom.orders (id serial primary key, customer integer not null)",
        )
        .await
        .expect("seed decoy orders (FULL) and real custom.orders (default)");

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("customer".to_string(), ValueType::Numeric)]);

    let err = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM custom.orders GROUP BY customer SELECT customer AS customer, \
         COUNT(*) AS order_count",
        &source_columns,
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::ReplicaIdentityRequired(_) => {}
        other => panic!("expected ReplicaIdentityRequired, got {other:?}"),
    }

    let count: i64 = client
        .query_one("select count(*) from transform_definitions", &[])
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(
        count, 0,
        "the wrongly-would-be-accepted definition must not persist"
    );
}

/// The mirror of the test above: the real, explicitly-qualified source
/// `custom.orders` has `REPLICA IDENTITY FULL`, while a same-named decoy
/// `orders` (Trellis-pinned schema, first on `search_path`) is left at the
/// default identity. A bare `to_regclass` lookup would resolve to the decoy
/// and wrongly *reject* this otherwise-legitimate aggregate. Checking the
/// real, qualified source must instead accept it.
#[tokio::test]
async fn an_aggregate_against_an_explicitly_qualified_source_is_accepted_despite_a_default_identity_decoy()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table orders (id serial primary key, customer integer not null); \
             create schema custom; \
             create table custom.orders (id serial primary key, customer integer not null); \
             alter table custom.orders replica identity full",
        )
        .await
        .expect("seed decoy orders (default) and real custom.orders (FULL)");

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("customer".to_string(), ValueType::Numeric)]);

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM custom.orders GROUP BY customer SELECT customer AS customer, \
         COUNT(*) AS order_count",
        &source_columns,
    )
    .await
    .expect(
        "aggregate against the explicitly-qualified custom.orders (REPLICA IDENTITY FULL) \
         must be accepted even though a same-named decoy lacks it",
    );
}

/// Reviewer follow-up to issue #74 (epic #78's own whole-branch review):
/// unlike the two tests above (an aggregate whose *own* `FROM` is explicitly
/// schema-qualified), this covers a bare aggregate `FROM t` chained off a
/// *different* definition's target that was explicitly qualified into a
/// non-default schema. `assert_replica_identity_supports_aggregate` runs
/// before `create_definition_inner` resolves `qualified_source`, so it used
/// to pass the bare `def.source` straight to `to_regclass` in the `None`
/// (bare) branch — no `search_path` fallback at all, unlike the explicit
/// branch just above. `custom` is nowhere on this pool's pinned
/// `search_path` (`Config::schema`/`Config::target_schema`/`public`), so
/// `to_regclass("t")` returned `NULL` and the identity query then matched
/// zero `pg_class` rows — an opaque `Db(Error{kind: RowCount})`, not the
/// `custom.t` lookup this test expects.
#[tokio::test]
async fn an_aggregate_against_a_bare_source_chained_off_an_explicitly_qualified_target_is_accepted_when_full()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create schema custom; \
             create table s (id bigint primary key, a numeric); \
             insert into s (id, a) values (1, 10), (2, 20)",
        )
        .await
        .expect("seed source and create the custom schema");

    // Def A: installs with an explicit non-default target schema, exactly
    // like the two tests above's own decoy setup — `custom` is nowhere on
    // this pool's pinned `search_path`.
    install_definition(
        &db.pool,
        "TRANSFORM custom.t FROM s SELECT a AS x",
        &columns(&["a"]),
        "public",
    )
    .await
    .expect("def A installs with an explicit non-default target schema");

    client
        .batch_execute("alter table custom.t replica identity full")
        .await
        .expect("grant custom.t full replica identity");

    // Def B: a bare aggregate `FROM t` must still resolve to def A's
    // `custom.t` — a plain `search_path` walk alone (what `to_regclass` did
    // here before this fix) would find nothing and fail with an opaque
    // RowCount error, never reaching the identity check at all.
    create_definition(
        &db.pool,
        "TRANSFORM totals FROM t GROUP BY x SELECT x AS x, COUNT(*) AS n",
        &columns(&["x"]),
    )
    .await
    .expect(
        "aggregate's bare FROM must resolve to def A's explicitly-qualified custom.t \
         target (REPLICA IDENTITY FULL), not fail with a RowCount resolution miss",
    );
}

/// The mirror of the test above: `custom.t` genuinely lacks `REPLICA
/// IDENTITY FULL`, so the bare-chained aggregate must still be rejected —
/// but with the real [`CatalogError::ReplicaIdentityRequired`], not the
/// opaque `RowCount` resolution-miss error the gap produced before this fix
/// (a totally unresolvable bare source hit the same `RowCount` failure,
/// masking what should have been a clean identity rejection).
#[tokio::test]
async fn an_aggregate_against_a_bare_source_chained_off_an_explicitly_qualified_target_is_rejected_cleanly_when_not_full()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create schema custom; \
             create table s (id bigint primary key, a numeric); \
             insert into s (id, a) values (1, 10), (2, 20)",
        )
        .await
        .expect("seed source and create the custom schema");

    install_definition(
        &db.pool,
        "TRANSFORM custom.t FROM s SELECT a AS x",
        &columns(&["a"]),
        "public",
    )
    .await
    .expect("def A installs with an explicit non-default target schema");
    // `custom.t` is left at the default replica identity deliberately.

    let err = create_definition(
        &db.pool,
        "TRANSFORM totals FROM t GROUP BY x SELECT x AS x, COUNT(*) AS n",
        &columns(&["x"]),
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::ReplicaIdentityRequired(_) => {}
        other => panic!(
            "expected a clean ReplicaIdentityRequired rejection, got {other:?} \
             (a RowCount error here would mean resolution never even reached \
             custom.t)"
        ),
    }

    let count: i64 = client
        .query_one(
            "select count(*) from transform_definitions \
             where split_part(target_table, '.', 2) = 'totals'",
            &[],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(
        count, 0,
        "the wrongly-would-be-accepted aggregate definition must not persist"
    );
}

/// Issue #177: a `OneToOne` target's primary key is always narrowed down to
/// a single column, mirroring the source's own (see
/// `ddl::require_single_column_pk`'s doc comment, issue #126) — so a source
/// with a genuinely composite primary key can never be represented and must
/// be rejected outright.
///
/// [`create_definition`] is the ring-path entry point this test calls
/// directly, deliberately bypassing [`install_definition`] entirely: before
/// this issue's fix, only `install_definition`'s own `KeySpace::OneToOne` arm
/// ran this arity check, so a composite-PK source reaching Postgres only via
/// `create_definition`/`create_definition_without_backfill` skipped it
/// entirely and only surfaced the failure much later, deep in
/// `staging::apply`'s own machinery, manifesting as a whole-instance halt
/// (see `quarantine.rs`'s
/// `a_composite_primary_key_source_is_rejected_at_create_time_not_quarantined_or_halted`,
/// which pins that this scenario can no longer even reach that machinery).
/// This test pins the fix itself: `create_definition_inner` now runs the
/// exact same check up front, so the rejection is a clean, typed
/// `CatalogError` returned synchronously from `create_definition`, before any
/// row is persisted, any DDL runs, or any CDC/apply machinery is ever
/// touched.
#[tokio::test]
async fn a_one_to_one_transform_against_a_composite_primary_key_source_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table order_lines (order_id integer, line_no integer, price numeric, \
             primary key (order_id, line_no))",
        )
        .await
        .expect("create source table with a composite primary key");

    let err = create_definition(
        &db.pool,
        "TRANSFORM line_totals FROM order_lines SELECT price AS total",
        &columns(&["order_id", "line_no", "price"]),
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::Ddl(DdlError::CompositePrimaryKeyUnsupported { source_table }) => {
            assert!(
                source_table.ends_with("order_lines"),
                "expected the composite source to be named in the error, got {source_table}"
            );
        }
        other => panic!("expected a clean CompositePrimaryKeyUnsupported rejection, got {other:?}"),
    }

    let count: i64 = client
        .query_one(
            "select count(*) from transform_definitions \
             where split_part(target_table, '.', 2) = 'line_totals'",
            &[],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(
        count, 0,
        "the rejected definition must not have been persisted"
    );
}
