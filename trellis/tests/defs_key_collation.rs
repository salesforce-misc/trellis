//! Issue #638: every column the engine matches as a key by its exact text
//! needs a deterministic collation. A nondeterministic one's `=` (and so its
//! `GROUP BY` and unique indexes) treats `'A'` and `'a'` as equal, so Postgres
//! would fold rows the engine keeps apart. Refused at define time, one test
//! per key class, and a deterministic non-default collation (`"C"`) still
//! passes in every one of them.
//!
//! The join-column twin of this rule (#590) is pinned in
//! `defs_relationship_catalog.rs`; the pure check is covered by
//! `defs::catalog`'s `join_column_tests`.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{
    CatalogError, KeyColumnRole, NondeterministicKeyCollation, RelationshipSide, ValidationError,
    ValueType, create_relationship, install_definition,
};

/// Only ICU provides nondeterministic collations. A server built without ICU,
/// or a cluster whose encoding ICU doesn't support (`SQL_ASCII`, which
/// `initdb` picks under `LANG=C`), refuses the `CREATE COLLATION` with
/// `0A000`, and this returns `false` so the caller skips (the same pattern as
/// `defs_relationship_catalog.rs`'s nondeterministic join-column test).
async fn create_case_insensitive_collation(pool: &trellis::pool::Pool) -> bool {
    let client = pool.get().await.expect("get connection");
    match client
        .batch_execute(
            "create collation case_insensitive \
             (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        )
        .await
    {
        Ok(()) => true,
        Err(err) if err.code() == Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED) => {
            let reason = err.as_db_error().map(|db| db.message().to_string());
            eprintln!("skipping: this server can't create an ICU collation: {reason:?}");
            false
        }
        Err(err) => panic!("create a nondeterministic ICU collation: {err:?}"),
    }
}

async fn batch(pool: &trellis::pool::Pool, sql: &str) {
    pool.get()
        .await
        .expect("get connection")
        .batch_execute(sql)
        .await
        .expect("set up tables");
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, value_type)| (name.to_string(), *value_type))
        .collect()
}

/// Unwraps `err` as [`ValidationError::NondeterministicKeyCollation`] and
/// checks it names `key`, `table.column` and the `case_insensitive`
/// collation.
fn assert_refused(err: CatalogError, key: KeyColumnRole, table: &str, column: &str) {
    let message = err.to_string();
    match err {
        CatalogError::Validate(ValidationError::NondeterministicKeyCollation(refused)) => {
            assert_eq!(
                *refused,
                NondeterministicKeyCollation {
                    key,
                    table: table.to_string(),
                    column: column.to_string(),
                    collation: "case_insensitive".to_string(),
                }
            );
        }
        other => panic!("expected NondeterministicKeyCollation, got {other:?}"),
    }
    assert!(
        message.contains(&format!("{table}.{column} is "))
            && message.contains(r#"collation "case_insensitive" is nondeterministic"#),
        "{message}"
    );
}

/// Refused before any DDL: registration creates the target in the same
/// transaction, after every check.
async fn assert_no_target(pool: &trellis::pool::Pool, target: &str) {
    let exists: bool = pool
        .get()
        .await
        .expect("get connection")
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&format!("public.{target}")],
        )
        .await
        .expect("probe target")
        .get(0);
    assert!(!exists, "a refused definition must not leave its target");
}

/// A source whose primary key has a nondeterministic collation is refused:
/// apply keys every change by it, and a 1-1 target mirrors it as its key.
#[tokio::test]
async fn a_source_key_with_a_nondeterministic_collation_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    if !create_case_insensitive_collation(&db.pool).await {
        return;
    }
    batch(
        &db.pool,
        "create table codes (code text collate case_insensitive primary key, n integer)",
    )
    .await;
    let err = install_definition(
        &db.pool,
        "TRANSFORM code_copies FROM codes SELECT n AS n",
        &columns(&[("code", ValueType::Text), ("n", ValueType::Numeric)]),
        "public",
    )
    .await
    .expect_err("a nondeterministic source key must be refused");
    assert_refused(err, KeyColumnRole::SourceKey, "trellis.codes", "code");
    assert_no_target(&db.pool, "code_copies").await;
}

/// A `GROUP BY` over a source column with a nondeterministic collation is
/// refused: the target's group key is the value's exact text.
#[tokio::test]
async fn a_group_by_column_with_a_nondeterministic_collation_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    if !create_case_insensitive_collation(&db.pool).await {
        return;
    }
    batch(
        &db.pool,
        "create table sales (id bigint primary key, region text collate case_insensitive, \
                             amount numeric); \
         alter table sales replica identity full",
    )
    .await;
    let err = install_definition(
        &db.pool,
        "TRANSFORM region_totals FROM sales GROUP BY region SELECT sum(amount) AS total",
        &columns(&[
            ("id", ValueType::Numeric),
            ("region", ValueType::Text),
            ("amount", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect_err("a nondeterministic GROUP BY column must be refused");
    assert_refused(err, KeyColumnRole::GroupBy, "trellis.sales", "region");
    assert_no_target(&db.pool, "region_totals").await;
}

/// A `GROUP BY` over a to-one relationship path is refused when the to-side
/// column has a nondeterministic collation; the error names the to-side
/// column, since that's the column to alter.
#[tokio::test]
async fn a_group_by_relationship_path_with_a_nondeterministic_collation_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    if !create_case_insensitive_collation(&db.pool).await {
        return;
    }
    batch(
        &db.pool,
        "create table posts (id bigint primary key, author text collate case_insensitive); \
         create table post_tags (id bigint primary key, post bigint, tag text); \
         alter table posts replica identity full; \
         alter table post_tags replica identity full",
    )
    .await;
    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("the join columns are bigint, so the relationship is fine");
    let err = install_definition(
        &db.pool,
        "TRANSFORM author_counts FROM post_tags GROUP BY post.author SELECT count(*) AS n",
        &columns(&[
            ("id", ValueType::Numeric),
            ("post", ValueType::Numeric),
            ("tag", ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect_err("a nondeterministic GROUP BY relationship path must be refused");
    assert_refused(err, KeyColumnRole::GroupBy, "trellis.posts", "author");
    assert_no_target(&db.pool, "author_counts").await;
}

/// A relationship whose endpoint's key has a nondeterministic collation is
/// refused even though its join columns are fine: apply keys every change
/// the relationship propagates by that key.
#[tokio::test]
async fn a_relationship_endpoint_key_with_a_nondeterministic_collation_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    if !create_case_insensitive_collation(&db.pool).await {
        return;
    }
    batch(
        &db.pool,
        "create table order_line_items \
             (code text collate case_insensitive primary key, product_id bigint); \
         create table products (id bigint primary key); \
         alter table products replica identity full",
    )
    .await;
    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect_err("a nondeterministic endpoint key must be refused");
    assert_refused(
        err,
        KeyColumnRole::RelationshipEndpointKey {
            name: "product".to_string(),
            side: RelationshipSide::From,
        },
        "trellis.order_line_items",
        "code",
    );
}

/// Every key class above passes under a deterministic non-default collation
/// (`"C"`): a `"C"` source key and relationship endpoint key, a `"C"`
/// `GROUP BY` column and a `"C"` relationship-path `GROUP BY` key. Needs no
/// ICU, so it runs on every cluster.
#[tokio::test]
async fn deterministic_non_default_key_collations_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        r#"create table posts (id bigint primary key, author text collate "C");
           create table post_tags
               (id text collate "C" primary key, post bigint, tag text collate "C");
           alter table posts replica identity full;
           alter table post_tags replica identity full"#,
    )
    .await;
    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("a \"C\" endpoint key is deterministic");
    install_definition(
        &db.pool,
        "TRANSFORM author_tag_counts FROM post_tags GROUP BY tag, post.author \
         SELECT count(*) AS n",
        &columns(&[
            ("id", ValueType::Text),
            ("post", ValueType::Numeric),
            ("tag", ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect("\"C\" source key and GROUP BY keys are deterministic");
}
