//! Integration tests for the relationship catalog (issue #26 storage, issue
//! #27 validation), run against a real, ephemeral Postgres instance via the
//! shared harness (`testkit::TestCluster`).

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{
    CatalogError, EdgeKind, RelationshipCardinality, RelationshipDefinition,
    RelationshipTypeMismatch, RelationshipWarning, ValidationError, ValueType, create_definition,
    create_relationship, create_target_table, edges_from, install_definition, parse,
    relationship_by_name, source_primary_key,
};

/// The schema this file's bare `CREATE TABLE`s land in, and so the schema a
/// bare relationship endpoint resolves to: the Trellis schema, first on every
/// pooled connection's `search_path`. A relationship is looked up by its
/// qualified from-table (issue #288).
const SCHEMA: &str = "trellis";

/// A bare table with an integer primary key named `pk_col` — good enough to
/// stand in as a relationship's to-side when the test wants a `UNIQUE`/`PK`
/// column present (cardinality `ToOne`). Carries `REPLICA IDENTITY FULL`
/// unconditionally (issue #129, epic #127): every to-one relationship's
/// to-side now needs it for `create_relationship` to accept the
/// relationship at all (its settled parent projection reads old images —
/// see `assert_replica_identity_supports_projection`), and this helper is
/// overwhelmingly used to build a to-one relationship's to-side across this
/// file's tests. Harmless for the handful of from-side uses too — `REPLICA
/// IDENTITY FULL` only ever widens what a pre-image carries, never rejects
/// anything on the from-side.
async fn create_table_with_pk(pool: &trellis::pool::Pool, name: &str, pk_col: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} ({pk_col} serial primary key); \
             alter table {name} replica identity full"
        ))
        .await
        .expect("create table with pk");
}

/// A table with an ordinary (non-unique) integer column — a relationship's
/// from-side, or a to-side deliberately left without a uniqueness guarantee
/// (cardinality `ToMany`).
async fn create_table_with_plain_column(pool: &trellis::pool::Pool, name: &str, col: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (row_id serial primary key, {col} integer)"
        ))
        .await
        .expect("create table with plain column");
}

/// A table with a plain-`UNIQUE` (not primary-key) integer column — the
/// other route to cardinality `ToOne` per ADR-0006. `REPLICA IDENTITY FULL`
/// for the same reason [`create_table_with_pk`] carries it now (issue #129).
async fn create_table_with_unique_column(pool: &trellis::pool::Pool, name: &str, col: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (row_id serial primary key, {col} integer unique, other_col integer); \
             alter table {name} replica identity full"
        ))
        .await
        .expect("create table with unique column");
}

/// A table with a `text`-typed column, for type-mismatch tests.
async fn create_table_with_text_column(pool: &trellis::pool::Pool, name: &str, col: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (row_id serial primary key, {col} text)"
        ))
        .await
        .expect("create table with text column");
}

/// A table with a `numeric`-typed column, for the numeric-join-key
/// rejection test.
async fn create_table_with_numeric_column(pool: &trellis::pool::Pool, name: &str, col: &str) {
    create_table_with_typed_column(pool, name, col, "numeric").await;
}

async fn create_table_with_typed_column(
    pool: &trellis::pool::Pool,
    name: &str,
    col: &str,
    sql_type: &str,
) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (row_id serial primary key, {col} {sql_type})"
        ))
        .await
        .expect("create table with typed column");
}

/// Sets `REPLICA IDENTITY FULL` on `name` — the to-side prerequisite (#41)
/// for a to-many relationship, whose non-PK join key must appear in
/// delete/re-parent pre-images for reverse recompute; also the from-side
/// prerequisite (issue #158) for a to-one relationship, whose `from_col` is
/// itself an ordinary non-PK column and so needs the same guarantee for a
/// re-pointing `UPDATE` to carry an old image at all.
async fn set_replica_identity_full(pool: &trellis::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!("alter table {name} replica identity full"))
        .await
        .expect("set replica identity full");
}

#[tokio::test]
async fn a_relationship_round_trips_through_the_catalog() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    // Issue #158: a to-one relationship's from-side needs `REPLICA IDENTITY
    // FULL` too — see `assert_replica_identity_supports_projection`'s doc
    // comment.
    set_replica_identity_full(&db.pool, "order_line_items").await;
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(created.def.name, "product");
    assert_eq!(created.def.from_table, "order_line_items");
    assert_eq!(created.def.from_col, "product_id");
    assert_eq!(created.def.to_table, "products");
    assert_eq!(created.def.to_col, "id");
    assert_eq!(created.cardinality, RelationshipCardinality::ToOne);

    let read_back = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query")
        .expect("relationship should be found");

    // `warnings` (issue #31) is creation-time guidance, not a persisted
    // fact — see `RelationshipDefinition::warnings`'s doc comment — so it's
    // compared separately rather than folded into the full-struct equality
    // below.
    assert_eq!(read_back.warnings, Vec::new());
    assert_eq!(
        read_back,
        RelationshipDefinition {
            warnings: Vec::new(),
            ..created.clone()
        },
        "re-parsed read-back must match the created value"
    );
}

#[tokio::test]
async fn relationship_by_name_returns_none_for_an_unknown_pair() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let missing = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Acceptance criterion: the declared relationship appears as a typed edge
/// in the resolver — a `Relationship`-kind `schema_edges` row from the
/// to-table's node to the from-table's node (matching `Source`'s "to_node
/// depends on from_node" convention: the FK-holding from-table is the
/// dependent side), distinct from a `Source` edge.
#[tokio::test]
async fn a_relationship_appears_as_a_typed_edge_in_the_resolver() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "posts", "author_id").await;
    set_replica_identity_full(&db.pool, "posts").await;
    create_table_with_pk(&db.pool, "users", "id").await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts.author_id TO users.id",
    )
    .await
    .expect("valid relationship should be stored");

    // "trellis" (`Config::schema`, the default `TRELLIS_SCHEMA`), not
    // "public" — issue #74, ADR-0007: `edges_from`/`node_for_table` now
    // require a fully-qualified name, and `posts`/`users` above land under
    // whatever schema is first in the pool's ambient `search_path` for a
    // bare `CREATE TABLE`, which `pool::session_bootstrap` pins to the
    // Trellis schema first.
    let relationship_edges = edges_from(&db.pool, "trellis.users", EdgeKind::Relationship)
        .await
        .expect("query edges");
    assert_eq!(relationship_edges.len(), 1);
    assert_eq!(relationship_edges[0].kind, EdgeKind::Relationship);

    let from_node = trellis::defs::node_for_table(&db.pool, "trellis.posts")
        .await
        .expect("query node")
        .expect("posts node should exist");
    let to_node = trellis::defs::node_for_table(&db.pool, "trellis.users")
        .await
        .expect("query node")
        .expect("users node should exist");
    assert_eq!(relationship_edges[0].from_node_id, to_node.id);
    assert_eq!(relationship_edges[0].to_node_id, from_node.id);

    // No `Source` edge was created by declaring a relationship — the two
    // edge kinds stay distinct in the graph.
    let source_edges = edges_from(&db.pool, "trellis.users", EdgeKind::Source)
        .await
        .expect("query edges");
    assert!(source_edges.is_empty());
}

/// ADR-0006's "Naming and scope": a relationship name is unique **per
/// from-table**, not global — declaring the same name twice on the same
/// from-table is rejected, with an actionable message (issue #27), but the
/// same name on two different from-tables is fine.
#[tokio::test]
async fn a_duplicate_name_on_the_same_from_table_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table posts (id serial primary key, author_id integer, editor_id integer); \
             alter table posts replica identity full",
        )
        .await
        .expect("create posts");
    drop(client);
    create_table_with_pk(&db.pool, "users", "id").await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts.author_id TO users.id",
    )
    .await
    .expect("first relationship should be stored");

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts.editor_id TO users.id",
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::Validate(ValidationError::DuplicateRelationshipName { from_table, name }) => {
            // The qualified from-table: a name is unique per qualified
            // from-table (issue #288), so that's what it collided on.
            assert_eq!(from_table, "trellis.posts");
            assert_eq!(name, "author");
        }
        other => panic!("expected DuplicateRelationshipName, got {other:?}"),
    }
    let message = err.to_string();
    assert!(message.contains("author"));
    assert!(message.contains("posts"));
}

#[tokio::test]
async fn the_same_relationship_name_is_allowed_on_different_from_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "posts", "owner_id").await;
    set_replica_identity_full(&db.pool, "posts").await;
    create_table_with_plain_column(&db.pool, "comments", "owner_id").await;
    set_replica_identity_full(&db.pool, "comments").await;
    create_table_with_pk(&db.pool, "users", "id").await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP owner FROM posts.owner_id TO users.id",
    )
    .await
    .expect("first relationship should be stored");

    create_relationship(
        &db.pool,
        "RELATIONSHIP owner FROM comments.owner_id TO users.id",
    )
    .await
    .expect("same name on a different from-table should be allowed");

    let posts_owner = relationship_by_name(&db.pool, SCHEMA, "posts", "owner")
        .await
        .expect("read query")
        .expect("posts.owner should exist");
    let comments_owner = relationship_by_name(&db.pool, SCHEMA, "comments", "owner")
        .await
        .expect("read query")
        .expect("comments.owner should exist");
    assert_ne!(posts_owner.id, comments_owner.id);
}

/// No DDL is ever issued against `from_table`/`to_table` (ADR-0005): storing
/// a relationship never creates, alters, or indexes those tables — only the
/// test setup's own `create table` calls (not `create_relationship`) put
/// these two relations there.
#[tokio::test]
async fn creating_a_relationship_issues_no_ddl_against_the_source_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    create_table_with_pk(&db.pool, "products", "id").await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    let client = db.pool.get().await.expect("get connection");
    let index_count: i64 = client
        .query_one(
            "select count(*) from pg_index \
             where indrelid = 'order_line_items'::regclass and not indisprimary",
            &[],
        )
        .await
        .expect("query")
        .get(0);
    assert_eq!(
        index_count, 0,
        "no index should have been added to the from-table"
    );
}

/// Invalid source text fails to parse and leaves no row behind.
#[tokio::test]
async fn an_unparseable_relationship_is_rejected_and_leaves_no_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_relationship(&db.pool, "RELATIONSHIP product FROM order_line_items TO")
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::Parse(_)));

    let missing = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// ADR-0006's "type-check the join": a from-side column and to-side column
/// with incompatible types (here `integer` vs `text`) is rejected at
/// `create_relationship` time with an actionable message, not silently
/// stored.
#[tokio::test]
async fn a_type_mismatch_between_endpoints_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    create_table_with_text_column(&db.pool, "products", "id").await;

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::Validate(ValidationError::RelationshipTypeMismatch(mismatch)) => {
            let RelationshipTypeMismatch {
                name,
                from_table,
                to_table,
                ..
            } = &**mismatch;
            assert_eq!(name, "product");
            assert_eq!(from_table, "order_line_items");
            assert_eq!(to_table, "products");
        }
        other => panic!("expected RelationshipTypeMismatch, got {other:?}"),
    }
    let message = err.to_string();
    assert!(message.contains("not comparable"));

    let missing = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Issue #28 review, ADR-0006: a join key resolving to a fractional or
/// arbitrary-precision numeric type (`numeric`, `real`, `double precision`)
/// is rejected at `create_relationship` time. The engine compares join keys
/// as raw `::text`, which is exact for integer/uuid/text keys
/// (`a_relationship_round_trips_through_the_catalog` covers the integer
/// case) but not for this family — Postgres considers `1.0::numeric =
/// 1.00::numeric` but their `::text` renderings differ, which would produce
/// a false-miss NULL in the engine where a real LEFT JOIN matches.
#[tokio::test]
async fn a_numeric_join_key_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_numeric_column(&db.pool, "order_line_items", "product_id").await;
    create_table_with_numeric_column(&db.pool, "products", "id").await;

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::Validate(ValidationError::RelationshipUnsupportedJoinKeyType {
            name,
            table,
            column,
            ..
        }) => {
            assert_eq!(name, "product");
            assert_eq!(table, "order_line_items");
            assert_eq!(column, "product_id");
        }
        other => panic!("expected RelationshipUnsupportedJoinKeyType, got {other:?}"),
    }
    let message = err.to_string();
    assert!(message.contains("numeric"));

    let missing = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Issue #28 review (epic #19 cross-cutting): the join-key guard is a
/// positive allowlist of text-stable types, not just a numeric blocklist.
/// `character(n)` shares a `type_family` with `text`/`varchar` but its native
/// `=` is blank-padding-insensitive while its `::text` form is blank-padded,
/// so the engine's text-equality join would diverge from the oracle's typed
/// join. Rejected at `create_relationship` time.
#[tokio::test]
async fn a_character_n_join_key_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_typed_column(&db.pool, "order_line_items", "product_id", "character(8)")
        .await;
    create_table_with_typed_column(&db.pool, "products", "id", "character(8)").await;

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::Validate(ValidationError::RelationshipUnsupportedJoinKeyType {
            column,
            ..
        }) => assert_eq!(column, "product_id"),
        other => panic!("expected RelationshipUnsupportedJoinKeyType, got {other:?}"),
    }

    let missing = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// `timestamptz` used to be this test's rejection example, a companion to
/// `a_character_n_join_key_is_rejected`: `timestamptz_out` renders under the
/// session's `TimeZone`, and until issue #246 pinned `TimeZone` identically
/// on every connection Trellis opens (including the walsender) its `::text`
/// was not text-stable. It is a text-stable join key now
/// (`catalog::TEXT_STABLE_JOIN_KEY_TYPES`), so this asserts the opposite —
/// acceptance, mirroring `compatible_integer_widths_are_accepted`'s setup
/// (a real primary key plus `REPLICA IDENTITY FULL` on both sides).
#[tokio::test]
async fn a_timestamptz_join_key_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_typed_column(
        &db.pool,
        "order_line_items",
        "product_id",
        "timestamp with time zone",
    )
    .await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table products (id timestamptz primary key); \
             alter table products replica identity full",
        )
        .await
        .expect("create products with a timestamptz pk");
    drop(client);

    create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("timestamptz is a text-stable join key as of issue #246");

    let found = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(found.is_some());
}

/// Issue #41: a to-many relationship whose to-side has only the default (PK)
/// replica identity is rejected at define time — the non-PK join key would be
/// absent from delete/re-parent pre-images, so reverse recompute would
/// silently under-recompute. (`products.id` is a plain non-unique column, so
/// cardinality is `ToMany`, and the table's default replica identity omits it
/// from pre-images.)
#[tokio::test]
async fn a_to_many_to_side_with_default_replica_identity_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    create_table_with_plain_column(&db.pool, "products", "id").await;

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        &err,
        CatalogError::Validate(ValidationError::RelationshipToManyRequiresReplicaIdentity { .. })
    ));

    let missing = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Issue #41: `REPLICA IDENTITY NOTHING` (`relreplident = 'n'`) is rejected too
/// — it omits *every* column from pre-images, so the non-PK join key is just as
/// absent as under the default identity. Pinned separately so a future SQL
/// change can't silently start accepting `'n'`. Since issue #375 the endpoint
/// keying guard catches it first: intake can't key a table with no replica
/// identity at all, whatever the relationship's cardinality.
#[tokio::test]
async fn a_to_many_to_side_with_replica_identity_nothing_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    create_table_with_plain_column(&db.pool, "products", "id").await;
    {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("alter table products replica identity nothing")
            .await
            .expect("set replica identity nothing");
    }

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            &err,
            CatalogError::RelationshipEndpointNotChangeKeyed { endpoint }
                if endpoint == "trellis.products"
        ),
        "{err:?}"
    );

    let missing = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Issue #41: the same to-many relationship is accepted once the to-side has
/// `REPLICA IDENTITY FULL`, which puts the non-PK join key into pre-images.
#[tokio::test]
async fn a_to_many_to_side_with_replica_identity_full_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    create_table_with_plain_column(&db.pool, "products", "id").await;
    set_replica_identity_full(&db.pool, "products").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("to-many with REPLICA IDENTITY FULL should be accepted");

    assert_eq!(created.cardinality, RelationshipCardinality::ToMany);
}

/// Issue #41: `REPLICA IDENTITY USING INDEX` is accepted iff the replica-
/// identity index covers the join column. A unique index on the join column
/// itself both covers it (accepted) — but note that also makes the column
/// unique, so cardinality is `ToOne` and the gate doesn't even apply; to
/// exercise the to-many index-coverage path we use a non-unique... which
/// can't back a replica identity. So the meaningful to-many index case is a
/// *multi-column* unique index that includes the join column: cardinality
/// stays `ToMany` (the column isn't unique alone) yet the index covers it.
#[tokio::test]
async fn a_to_many_to_side_with_covering_replica_identity_index_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    let client = db.pool.get().await.expect("get connection");
    // Multi-column unique index over (id, region): `id` alone isn't unique
    // (cardinality ToMany) but the index — set as the replica identity —
    // covers `id`.
    client
        .batch_execute(
            "create table products (id integer not null, region text not null);              create unique index products_id_region_uk on products (id, region);              alter table products replica identity using index products_id_region_uk",
        )
        .await
        .expect("create products with covering replica-identity index");
    drop(client);

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("covering replica-identity index should be accepted");

    assert_eq!(created.cardinality, RelationshipCardinality::ToMany);
}

/// Issue #41: `REPLICA IDENTITY USING INDEX` is rejected when the replica-
/// identity index does NOT cover the join column — the pre-image would carry
/// the index's columns but not the join key.
#[tokio::test]
async fn a_to_many_to_side_with_non_covering_replica_identity_index_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    let client = db.pool.get().await.expect("get connection");
    // Replica-identity index is on `other_id`, not the join column `id`.
    client
        .batch_execute(
            "create table products (id integer not null, other_id integer not null);              create unique index products_other_uk on products (other_id);              alter table products replica identity using index products_other_uk",
        )
        .await
        .expect("create products with non-covering replica-identity index");
    drop(client);

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .unwrap_err();

    assert!(matches!(
        &err,
        CatalogError::Validate(ValidationError::RelationshipToManyRequiresReplicaIdentity { .. })
    ));

    let missing = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Slightly different integer widths (`integer` FK to `bigint`-identity-style
/// PK) are still comparable — the common case a strict exact-type-match rule
/// would wrongly reject.
#[tokio::test]
async fn compatible_integer_widths_are_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table products (id bigint primary key); \
             alter table products replica identity full",
        )
        .await
        .expect("create products with bigint pk");
    drop(client);

    create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("integer-to-bigint join should be accepted as comparable");
}

/// `text` and `character varying(n)` are the same [`type_family`] bucket,
/// but `format_type` renders the latter with its length modifier
/// (`character varying(255)`) — a regression check that bucket matching
/// strips the modifier rather than comparing the two renderings verbatim
/// (which would wrongly reject this as a mismatch).
#[tokio::test]
async fn text_and_varchar_are_accepted_as_comparable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_text_column(&db.pool, "order_line_items", "sku").await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table products (sku character varying(255) primary key); \
             alter table products replica identity full",
        )
        .await
        .expect("create products with varchar pk");
    drop(client);

    create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.sku TO products.sku",
    )
    .await
    .expect("text-to-varchar join should be accepted as comparable");
}

/// Two `character varying` columns with different length modifiers
/// (`varchar(50)` vs `varchar(255)`) are comparable — same regression as
/// [`text_and_varchar_are_accepted_as_comparable`], but for two modifier
/// renderings that differ from each other rather than one lacking a
/// modifier at all.
#[tokio::test]
async fn varchar_columns_with_different_lengths_are_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table order_line_items (id serial primary key, sku character varying(50));
             alter table order_line_items replica identity full;
             create table products (sku character varying(255) primary key);
             alter table products replica identity full",
        )
        .await
        .expect("create tables with differing varchar lengths");
    drop(client);

    create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.sku TO products.sku",
    )
    .await
    .expect("varchar(50)-to-varchar(255) join should be accepted as comparable");
}

/// ADR-0006's endpoint-resolution requirement: a relationship whose
/// `from_col`/`to_col` doesn't exist on the named table (including the table
/// itself not existing) is rejected with an actionable message.
#[tokio::test]
async fn an_unknown_from_column_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_pk(&db.pool, "products", "id").await;
    // `order_line_items` never created at all.

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::Validate(ValidationError::UnknownRelationshipColumn { table, column }) => {
            assert_eq!(table, "order_line_items");
            assert_eq!(column, "product_id");
        }
        other => panic!("expected UnknownRelationshipColumn, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unknown_to_column_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    create_table_with_pk(&db.pool, "products", "id").await;

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.missing_col",
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::Validate(ValidationError::UnknownRelationshipColumn { table, column }) => {
            assert_eq!(table, "products");
            assert_eq!(column, "missing_col");
        }
        other => panic!("expected UnknownRelationshipColumn, got {other:?}"),
    }
}

/// ADR-0006's cardinality rule: `to_col` being the table's `PRIMARY KEY`
/// determines `ToOne`.
#[tokio::test]
async fn a_primary_key_to_column_determines_to_one_cardinality() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(created.cardinality, RelationshipCardinality::ToOne);
    let read_back = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query")
        .expect("relationship should be found");
    assert_eq!(read_back.cardinality, RelationshipCardinality::ToOne);
}

/// ADR-0006's cardinality rule: a plain `UNIQUE` (non-PK) `to_col` also
/// determines `ToOne`.
#[tokio::test]
async fn a_plain_unique_to_column_determines_to_one_cardinality() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "accounts", "profile_code").await;
    set_replica_identity_full(&db.pool, "accounts").await;
    create_table_with_unique_column(&db.pool, "profiles", "code").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP profile FROM accounts.profile_code TO profiles.code",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(created.cardinality, RelationshipCardinality::ToOne);
}

/// ADR-0006's cardinality rule: a `to_col` with no uniqueness guarantee at
/// all determines `ToMany`. This issue (#27) stores that cardinality but
/// does not itself reject anything based on it — rejecting a bare-path
/// reference to a `ToMany` relationship is reference-time behavior
/// (validating how the relationship is *used* in a calculated field),
/// deferred past this issue since no such reference resolves yet (see
/// `trellis::defs::Expr::RelationshipPath`).
#[tokio::test]
async fn a_non_unique_to_column_determines_to_many_cardinality() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    create_table_with_plain_column(&db.pool, "products", "id").await;
    // To-many requires a replica identity carrying the non-PK join key (#41).
    set_replica_identity_full(&db.pool, "products").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored, even though its to-side isn't unique");

    assert_eq!(created.cardinality, RelationshipCardinality::ToMany);
    let read_back = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "product")
        .await
        .expect("read query")
        .expect("relationship should be found");
    assert_eq!(read_back.cardinality, RelationshipCardinality::ToMany);
}

/// A `to_col` that merely participates in a multi-column `UNIQUE`/`PRIMARY
/// KEY` index doesn't make it, alone, a unique key — cardinality must still
/// be `ToMany`.
#[tokio::test]
async fn a_to_column_in_a_composite_unique_index_is_still_to_many() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute("create table products (id integer, region text, primary key (id, region))")
        .await
        .expect("create products with composite pk");
    drop(client);
    // To-many requires a replica identity carrying the non-PK join key (#41).
    set_replica_identity_full(&db.pool, "products").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(created.cardinality, RelationshipCardinality::ToMany);
}

/// Issue #31 / ADR-0005: a from-side join column with no usable index still
/// makes for a *correct* relationship — reverse propagation is a plain
/// `from_col = $1` lookup — but it's a full scan on every update to the
/// to-side, so `create_relationship` surfaces it as a performance warning
/// naming the exact `CREATE INDEX` the user may run, rather than rejecting
/// the definition or creating the index itself.
#[tokio::test]
async fn a_missing_from_side_index_surfaces_a_performance_warning() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should still be stored");

    assert_eq!(
        created.warnings,
        vec![RelationshipWarning::MissingFkIndex {
            from_table: "order_line_items".to_string(),
            from_col: "product_id".to_string(),
        }]
    );
    let message = created.warnings[0].to_string();
    assert!(message.contains("order_line_items"));
    assert!(message.contains("product_id"));
    assert!(message.contains("CREATE INDEX ON order_line_items (product_id);"));

    let client = db.pool.get().await.expect("get connection");
    let index_count: i64 = client
        .query_one(
            "select count(*) from pg_index \
             where indrelid = 'order_line_items'::regclass and not indisprimary",
            &[],
        )
        .await
        .expect("query")
        .get(0);
    assert_eq!(
        index_count, 0,
        "the warning must not have caused Trellis to create the index itself"
    );
}

/// A plain `btree` index whose leading column is the from-side join column
/// is usable for the reverse lookup, so no warning is surfaced.
#[tokio::test]
async fn a_usable_from_side_index_suppresses_the_performance_warning() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    create_table_with_pk(&db.pool, "products", "id").await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute("create index on order_line_items (product_id)")
        .await
        .expect("create index on from-side join column");
    drop(client);

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(created.warnings, Vec::new());
}

/// A from-side primary key also backs a usable `btree` index (Postgres
/// creates one implicitly), so it likewise suppresses the warning.
#[tokio::test]
async fn a_from_side_primary_key_suppresses_the_performance_warning() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_pk(&db.pool, "order_line_items", "product_id").await;
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(created.warnings, Vec::new());
}

/// An index that merely *contains* the from-side join column, but not as
/// its leading column, can't be used for a `from_col = $1` lookup — the
/// warning still fires.
#[tokio::test]
async fn an_index_where_the_join_column_is_not_leading_still_warns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table order_line_items (id serial primary key, other_col integer, product_id integer); \
             alter table order_line_items replica identity full",
        )
        .await
        .expect("create from-table");
    client
        .batch_execute("create index on order_line_items (other_col, product_id)")
        .await
        .expect("create index with product_id trailing, not leading");
    drop(client);
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(
        created.warnings,
        vec![RelationshipWarning::MissingFkIndex {
            from_table: "order_line_items".to_string(),
            from_col: "product_id".to_string(),
        }]
    );
}

/// A composite index *led by* the join column is usable even though it
/// isn't the index's only column — leading-column position is what matters
/// for an equality lookup, not exclusivity (contrast the to-side's
/// uniqueness check, which does require exclusivity).
#[tokio::test]
async fn a_composite_index_led_by_the_join_column_suppresses_the_warning() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table order_line_items (id serial primary key, product_id integer, other_col integer); \
             alter table order_line_items replica identity full",
        )
        .await
        .expect("create from-table");
    client
        .batch_execute("create index on order_line_items (product_id, other_col)")
        .await
        .expect("create index led by product_id");
    drop(client);
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(created.warnings, Vec::new());
}

/// A partial index led by the join column doesn't cover every row, so it
/// can't be relied on for a generic reverse lookup — the warning still
/// fires, mirroring the to-side cardinality check's exclusion of partial
/// indexes for the same reason.
#[tokio::test]
async fn a_partial_index_on_the_join_column_still_warns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table order_line_items (id serial primary key, product_id integer, active boolean); \
             alter table order_line_items replica identity full",
        )
        .await
        .expect("create from-table");
    client
        .batch_execute("create index on order_line_items (product_id) where active")
        .await
        .expect("create partial index");
    drop(client);
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(
        created.warnings,
        vec![RelationshipWarning::MissingFkIndex {
            from_table: "order_line_items".to_string(),
            from_col: "product_id".to_string(),
        }]
    );
}

/// An expression index's leading "column" isn't a plain column reference
/// (`indkey[0]` is `0`, which never matches a real `attnum`), so it isn't
/// usable for a `from_col = $1` lookup — the warning still fires.
#[tokio::test]
async fn an_expression_index_on_the_join_column_still_warns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute("create index on order_line_items ((product_id + 0))")
        .await
        .expect("create expression index on join column");
    drop(client);
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(
        created.warnings,
        vec![RelationshipWarning::MissingFkIndex {
            from_table: "order_line_items".to_string(),
            from_col: "product_id".to_string(),
        }]
    );
}

/// A `hash` index doesn't support the leading-column equality lookup the
/// way `btree` does, and isn't worth special-casing for what's only a
/// performance hint — the warning still fires.
#[tokio::test]
async fn a_hash_index_on_the_join_column_still_warns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_plain_column(&db.pool, "order_line_items", "product_id").await;
    set_replica_identity_full(&db.pool, "order_line_items").await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute("create index on order_line_items using hash (product_id)")
        .await
        .expect("create hash index on join column");
    drop(client);
    create_table_with_pk(&db.pool, "products", "id").await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(
        created.warnings,
        vec![RelationshipWarning::MissingFkIndex {
            from_table: "order_line_items".to_string(),
            from_col: "product_id".to_string(),
        }]
    );
}

/// ADR-0006: relationship edges join the same cross-table dependency graph
/// transform edges do, and a new edge that would close a cycle is rejected —
/// generalizing the existing `Source`-edge cycle detector (issue #21) rather
/// than introducing a relationship-specific one.
#[tokio::test]
async fn a_relationship_edge_that_would_close_a_cycle_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table a (id serial primary key, b_id integer);
             create table b (id serial primary key, a_id integer);
             alter table a replica identity full;
             alter table b replica identity full",
        )
        .await
        .expect("create a and b");
    drop(client);

    create_relationship(&db.pool, "RELATIONSHIP b FROM a.b_id TO b.id")
        .await
        .expect("first relationship should be stored");

    // b -> a would close a 2-cycle: a -> b -> a.
    let err = create_relationship(&db.pool, "RELATIONSHIP a FROM b.a_id TO a.id")
        .await
        .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::TableCycle { cycle }) => {
            // "trellis.a"/"trellis.b" (issue #74, ADR-0007): `a`/`b` above
            // land under the Trellis schema, the first entry in a bare
            // `CREATE TABLE`'s ambient `search_path` — see
            // `a_relationship_appears_as_a_typed_edge_in_the_resolver`'s
            // identical note.
            assert!(cycle.contains(&"trellis.a".to_string()));
            assert!(cycle.contains(&"trellis.b".to_string()));
        }
        other => panic!("expected TableCycle, got {other:?}"),
    }

    let missing = relationship_by_name(&db.pool, SCHEMA, "b", "a")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Regression for the edge-direction fix: a transform target declaring a
/// relationship back to its own source table must be accepted, not rejected
/// as a false 2-cycle. Both the `Source` edge (`orders -> order_totals`, from
/// `create_definition`) and the `Relationship` edge this test adds mean the
/// same thing — "`order_totals` depends on `orders`" — so persisting the
/// relationship edge in the direction consistent with `Source`'s "to_node
/// depends on from_node" convention (`orders -> order_totals`, i.e.
/// `to_table -> from_table`) must not trip the cycle detector. Persisting it
/// naively as `from_table -> to_table` (`order_totals -> orders`) would have
/// made `order_totals` and `orders` mutually reachable, and rejected this as
/// closing a cycle even though there is no real cycle.
#[tokio::test]
async fn a_relationship_from_a_transform_target_back_to_its_own_source_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_pk(&db.pool, "orders", "id").await;

    let dsl = "TRANSFORM order_totals FROM orders SELECT id AS total";
    let source_columns = HashMap::from([("id".to_string(), ValueType::Numeric)]);
    create_definition(&db.pool, dsl, &source_columns)
        .await
        .expect("valid definition should be stored");

    let def = parse(dsl).expect("parse dsl for target materialization");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("materialize chained target table");

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "alter table order_totals add column order_ref integer; \
             alter table order_totals replica identity full",
        )
        .await
        .expect("add fk-shaped column to target table");
    drop(client);

    create_relationship(
        &db.pool,
        "RELATIONSHIP origin FROM order_totals.order_ref TO orders.id",
    )
    .await
    .expect("relationship back to a target's own source should not be a false cycle");
}

/// Issue #45 end-to-end: a calculated target table that passes an `integer` FK
/// column through (`SELECT author AS author`) must be usable as a relationship
/// endpoint. Before the fix the passthrough column was DDL'd as `numeric`
/// (`ValueType::Numeric` collapses every integer family to `numeric`), and
/// `numeric` is excluded from the join-key allowlist — so this relationship
/// was rejected with `RelationshipUnsupportedJoinKeyType`, even though the raw
/// `posts.author` column joins fine. With the passthrough now DDL'd as
/// `integer`, the relationship is accepted. Mirrors the issue's POC repro
/// (`authors`/`posts` from `poc/schema_dump.sql`), against the calculated
/// table rather than the raw one.
#[tokio::test]
async fn an_integer_passthrough_on_a_calculated_table_is_a_valid_join_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             create table posts (id integer primary key, author integer, body text)",
        )
        .await
        .expect("seed authors and posts");
    drop(client);

    // A calculated 1-1 transform over `posts` that passes the `author`
    // integer FK through alongside a derived field.
    let dsl =
        "TRANSFORM posts_calc FROM posts SELECT author AS author, octet_length(body) AS byte_size";
    let source_columns = HashMap::from([
        ("author".to_string(), ValueType::Numeric),
        ("body".to_string(), ValueType::Text),
    ]);
    create_definition(&db.pool, dsl, &source_columns)
        .await
        .expect("valid calculated definition should be stored");

    let def = parse(dsl).expect("parse dsl for target materialization");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("materialize calculated target table");

    // `posts_calc.author` is the to-many join key (a non-unique column), so
    // the to-side needs a replica identity carrying it (#41) — orthogonal to
    // the #45 join-key-type fix under test.
    set_replica_identity_full(&db.pool, "posts_calc").await;

    // The exact repro from the issue: relate `authors.id` to the calculated
    // table's passthrough column. This is what previously failed.
    create_relationship(
        &db.pool,
        "RELATIONSHIP posts_calc FROM authors.id TO posts_calc.author",
    )
    .await
    .expect("integer passthrough on a calculated table must be a valid join key");
}

/// Negative counterpart to
/// `an_integer_passthrough_on_a_calculated_table_is_a_valid_join_key`: the
/// #45 fix only narrows a bare passthrough to the source column's *concrete*
/// type when that type is itself text-stable (e.g. `integer`). A passthrough
/// of a genuinely `numeric`/`real`/`double precision` source column must
/// still be DDL'd with that (non-text-stable) concrete type and therefore
/// still fail the join-key allowlist exactly as before the fix — confirming
/// #45 didn't accidentally make true-numeric-family columns eligible as join
/// keys.
#[tokio::test]
async fn a_numeric_passthrough_on_a_calculated_table_is_still_rejected_as_a_join_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table authors (id real primary key, name text); \
             create table posts (id integer primary key, author real, body text)",
        )
        .await
        .expect("seed authors and posts");
    drop(client);

    // A calculated 1-1 transform over `posts` that passes the `author`
    // `real` column through alongside a derived field.
    let dsl =
        "TRANSFORM posts_calc FROM posts SELECT author AS author, octet_length(body) AS byte_size";
    let source_columns = HashMap::from([
        ("author".to_string(), ValueType::Numeric),
        ("body".to_string(), ValueType::Text),
    ]);
    create_definition(&db.pool, dsl, &source_columns)
        .await
        .expect("valid calculated definition should be stored");

    let def = parse(dsl).expect("parse dsl for target materialization");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("materialize calculated target table");

    // Relate the calculated table's `real` passthrough (checked first, as
    // the from-side) to `authors.id` (also `real`, so the two endpoints are
    // comparable and the join-key-type check — not the type-mismatch check
    // — is what rejects this).
    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts_calc.author TO authors.id",
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::Validate(ValidationError::RelationshipUnsupportedJoinKeyType {
            table,
            column,
            ..
        }) => {
            assert_eq!(table, "posts_calc");
            assert_eq!(column, "author");
        }
        other => panic!("expected RelationshipUnsupportedJoinKeyType, got {other:?}"),
    }

    let missing = relationship_by_name(&db.pool, SCHEMA, "posts_calc", "author")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Reviewer follow-up to issue #74 (epic #78's own whole-branch review):
/// `create_relationship`'s own pg_catalog introspection
/// (`column_type_in_txn`/`to_col_cardinality_in_txn`/`has_usable_fk_index_in_txn`/
/// `assert_replica_identity_supports_to_many`) resolved `def.from_table`/
/// `def.to_table` bare via `pg_catalog.to_regclass`'s own `search_path` walk,
/// with no fallback — unlike this same function's later
/// `resolve_graph_identity_in_txn` calls, which already had issue #74's
/// bare-target-suffix fallback. So a relationship whose `to_table` bare-names
/// a definition's target explicitly qualified into a non-default schema
/// (issue #76) failed here first, in `column_type_in_txn`, as a false
/// "does not exist" — never reaching the later, fallback-equipped
/// resolution at all.
#[tokio::test]
async fn a_relationship_to_table_resolves_a_bare_name_chained_off_a_non_default_schema_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create schema custom; \
             create table s (id bigint primary key, a numeric); \
             create table order_line_items (id integer primary key, t2_id integer); \
             alter table order_line_items replica identity full",
        )
        .await
        .expect("seed tables and the custom schema");
    drop(client);

    // Def A: installs with an explicit non-default target schema — `custom`
    // is nowhere on this pool's pinned `search_path`
    // (`Config::schema`/`Config::target_schema`/`public`).
    let cols = HashMap::from([("a".to_string(), ValueType::Numeric)]);
    install_definition(
        &db.pool,
        "TRANSFORM custom.t2 FROM s SELECT a AS total",
        &cols,
        "public",
    )
    .await
    .expect("def A installs with an explicit non-default target schema");
    // `s` starts empty, so def A's discharge takes it `live` at once: a
    // target not yet `live` can't be made a relationship endpoint (issue
    // #403). No replica identity is needed on it either, since an endpoint
    // that is this instance's own target is never published.
    trellis::intake::publication::settle_registrations(&db.pool).await;

    // The relationship's bare `TO t2.id` must still resolve to def A's
    // `custom.t2` — a plain `search_path` walk alone (what `to_regclass`
    // does inside `column_type_in_txn`/`to_col_cardinality_in_txn`) would
    // report "does not exist", even though `custom.t2` is live.
    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP t2 FROM order_line_items.t2_id TO t2.id",
    )
    .await
    .expect(
        "the relationship's bare TO must resolve to def A's explicitly-qualified \
         custom.t2 target, not be rejected as 'does not exist'",
    );
    assert_eq!(created.def.to_table, "t2");
    assert_eq!(created.cardinality, RelationshipCardinality::ToOne);

    let read_back = relationship_by_name(&db.pool, SCHEMA, "order_line_items", "t2")
        .await
        .expect("read query")
        .expect("relationship should be found");
    assert_eq!(read_back.def.to_table, "t2");
}

/// The 4th gap in this same reviewer follow-up round (epic #78's own
/// whole-branch review, issue #74/#40): unlike the test above (which covers
/// `create_relationship`'s own pg_catalog checks), this covers a *separate*
/// definition's calculated-field relationship-path enrichment
/// (`catalog::resolve_relationships`, issue #40) reading a to-side column
/// through a relationship whose `to_table` bare-names a chained,
/// non-default-schema-qualified target. `resolve_relationships`' own
/// `column_type` (pooled, not txn-scoped — this resolver runs entirely
/// outside a transaction) queried `pg_attribute`/`to_regclass` directly
/// against the relationship's bare `to_table`, with no fallback — even
/// though `create_relationship` itself (the test above) had already been
/// fixed to resolve the identical bare `to_table` before its own pg_catalog
/// checks. So a relationship that resolves fine at `create_relationship`
/// time could still make a *later* definition's calculated field referencing
/// it fail here, at enrichment resolution, as a false
/// `ValidationError::UnknownRelationshipColumn` — even though the relevant
/// column plainly exists on `custom.t2`.
#[tokio::test]
async fn a_calculated_field_relationship_path_resolves_a_to_table_chained_off_a_non_default_schema_target()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create schema custom; \
             create table s (id bigint primary key, a numeric); \
             create table order_line_items (id integer primary key, t2_id integer); \
             alter table order_line_items replica identity full; \
             insert into order_line_items (id, t2_id) values (1, 1), (2, 2)",
        )
        .await
        .expect("seed tables and the custom schema");
    drop(client);

    // Def A: installs with an explicit non-default target schema — `custom`
    // is nowhere on this pool's pinned `search_path`
    // (`Config::schema`/`Config::target_schema`/`public`).
    let cols = HashMap::from([("a".to_string(), ValueType::Numeric)]);
    install_definition(
        &db.pool,
        "TRANSFORM custom.t2 FROM s SELECT a AS total",
        &cols,
        "public",
    )
    .await
    .expect("def A installs with an explicit non-default target schema");
    // `s` starts empty, so def A's discharge takes it `live` at once: a
    // target not yet `live` can't be made a relationship endpoint (issue
    // #403). No replica identity is needed on it either, since an endpoint
    // that is this instance's own target is never published.
    trellis::intake::publication::settle_registrations(&db.pool).await;

    // The relationship's bare `TO t2.id` must resolve to def A's `custom.t2`
    // (already covered by the test above; needed here as this test's own
    // setup).
    create_relationship(
        &db.pool,
        "RELATIONSHIP t2 FROM order_line_items.t2_id TO t2.id",
    )
    .await
    .expect("the relationship's bare TO must resolve to def A's custom.t2 target");

    // Def B: a bare, relationship-free source with a calculated field
    // enriched via `t2.total` — this is what exercises
    // `resolve_relationships`/`column_type`'s own resolution of the
    // relationship's `to_table`. Before this fix, `column_type`'s bare
    // `to_regclass("t2")` lookup would miss `custom.t2` entirely and report
    // `UnknownRelationshipColumn { table: "t2", column: "total" }`, even
    // though `create_relationship` above already proved `t2` resolves to a
    // live, real relationship.
    let def = install_definition(
        &db.pool,
        "TRANSFORM enriched FROM order_line_items SELECT t2.total AS total_copy",
        &HashMap::new(),
        "public",
    )
    .await
    .expect(
        "the calculated field's t2.total enrichment must resolve t2's to_table to \
         def A's explicitly-qualified custom.t2 target, not be rejected as an \
         unknown relationship column",
    );

    let client = db.pool.get().await.expect("get connection");
    let mismatches: i64 = client
        .query_one(
            "select count(*) from enriched e \
             join order_line_items o on o.id = e.id \
             join custom.t2 on custom.t2.id = o.t2_id \
             where e.total_copy is distinct from custom.t2.total",
            &[],
        )
        .await
        .expect("compare enriched against custom.t2")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "enriched.total_copy must reflect custom.t2.total via the resolved relationship"
    );
    assert_eq!(def.def.target, "enriched");
}

/// Issue #288: a relationship name is unique per *schema-qualified* from-table,
/// not per bare from-table name. `blog.posts` and `shop.posts` are unrelated
/// tables that merely share a name, so each may declare its own `author`
/// relationship — and each must then resolve, and drop, independently of the
/// other.
///
/// The `RELATIONSHIP` grammar has no qualified endpoint spelling, so a bare
/// `posts` resolves through the declaring connection's `search_path` — the
/// same way it does everywhere else. Two pools whose `target_schema` (and so
/// whose `search_path`) differ are how the same statement text reaches two
/// different `posts` tables here, against one shared catalog.
#[tokio::test]
async fn same_named_relationships_on_same_named_tables_in_different_schemas_coexist() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute(
                "create schema blog; create schema shop; \
                 create table blog.users (id integer primary key); \
                 create table shop.users (id integer primary key); \
                 create table blog.posts (id integer primary key, author_id integer); \
                 create table shop.posts (id integer primary key, author_id integer); \
                 alter table blog.users replica identity full; \
                 alter table shop.users replica identity full; \
                 alter table blog.posts replica identity full; \
                 alter table shop.posts replica identity full;",
            )
            .await
            .expect("seed blog and shop");
    }
    let schema_pool = |target_schema: &str| {
        let config = trellis::Config::from_dsn(db.dsn().to_string())
            .expect("valid dsn")
            .with_target_schema(target_schema)
            .expect("valid target schema");
        trellis::pool::Pool::new(&config).expect("build pool")
    };
    let blog_pool = schema_pool("blog");
    let shop_pool = schema_pool("shop");

    let statement = "RELATIONSHIP author FROM posts.author_id TO users.id";
    let blog_rel = create_relationship(&blog_pool, statement)
        .await
        .expect("blog.posts may declare `author`");
    let shop_rel = create_relationship(&shop_pool, statement)
        .await
        .expect("shop.posts may declare its own `author` — a different table entirely");
    assert_ne!(blog_rel.id, shop_rel.id);

    let count = |schema: &'static str| {
        let pool = &db.pool;
        async move {
            let client = pool.get().await.expect("get connection");
            client
                .query_one(
                    "select count(*) from relationship_definitions \
                     where from_schema = $1 and from_table = 'posts' and name = 'author'",
                    &[&schema],
                )
                .await
                .expect("count")
                .get::<_, i64>(0)
        }
    };
    assert_eq!(count("blog").await, 1);
    assert_eq!(count("shop").await, 1);

    // Each resolves on its own qualified from-table, to its own row.
    let blog_read = relationship_by_name(&db.pool, "blog", "posts", "author")
        .await
        .expect("read query")
        .expect("blog.posts.author resolves");
    let shop_read = relationship_by_name(&db.pool, "shop", "posts", "author")
        .await
        .expect("read query")
        .expect("shop.posts.author resolves");
    assert_eq!(blog_read.id, blog_rel.id);
    assert_eq!(blog_read.from_schema, "blog");
    assert_eq!(blog_read.qualified_from_table(), "blog.posts");
    assert_eq!(shop_read.id, shop_rel.id);
    assert_eq!(shop_read.from_schema, "shop");
    assert_eq!(shop_read.qualified_from_table(), "shop.posts");

    // A bare `DROP` address now matches two relationships, so it is refused
    // rather than resolved through whichever `search_path` the caller has.
    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis");
    let err = trellis
        .apply("DROP RELATIONSHIP posts.author")
        .await
        .expect_err("a bare address naming two relationships is ambiguous");
    assert!(
        matches!(
            &err,
            trellis::TrellisError::Catalog(CatalogError::AmbiguousRelationshipAddress {
                schemas, ..
            }) if schemas == &["blog".to_string(), "shop".to_string()]
        ),
        "expected AmbiguousRelationshipAddress naming both schemas, got {err:?}"
    );
    assert_eq!(count("blog").await, 1, "a refused drop drops nothing");
    assert_eq!(count("shop").await, 1, "a refused drop drops nothing");

    // Each qualified address drops exactly its own relationship.
    trellis
        .apply("DROP RELATIONSHIP shop.posts.author")
        .await
        .expect("drop shop.posts.author");
    assert_eq!(count("shop").await, 0);
    assert_eq!(count("blog").await, 1, "blog.posts.author is untouched");

    // With only one left, the bare address is no longer ambiguous.
    trellis
        .apply("DROP RELATIONSHIP posts.author")
        .await
        .expect("a bare address naming one relationship drops it");
    assert_eq!(count("blog").await, 0);
}
