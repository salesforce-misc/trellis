//! Integration tests for the settled parent projection's catalog, DDL,
//! backfill, and replica-identity gate (issue #129, epic #127's "relationship
//! delta" — see `trellis/tests/spikes/issue-102-PLAN-DRAFT.md` §2 for the full
//! mechanism). This is foundation-only: nothing here exercises a forward read
//! from the projection (#130) or a reverse-applied advance (#131) — those
//! don't exist yet. These tests only check that the projection table itself
//! is correctly shaped, created, kept in step with its to-side table by
//! backfill, and gated by replica identity.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{
    CatalogError, RelationshipCardinality, ValueType, create_definition, create_relationship,
    relationship_projection,
};

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// The projection's own bookkeeping + data columns, as introspected off
/// `information_schema.columns` in the catalog schema (issue #435) — `(name, data_type, is_nullable)` sorted by
/// name so assertions don't depend on physical column order.
async fn projection_columns(
    pool: &trellis::pool::Pool,
    projection_table: &str,
) -> Vec<(String, String, String)> {
    let client = pool.get().await.expect("get connection");
    let rows = client
        .query(
            "select column_name, data_type, is_nullable from information_schema.columns \
             where table_schema = $1 and table_name = $2 \
             order by column_name",
            &[&trellis::config::DEFAULT_SCHEMA, &projection_table],
        )
        .await
        .expect("introspect projection columns");
    rows.into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

async fn projection_row_count(pool: &trellis::pool::Pool, projection_table: &str) -> i64 {
    let client = pool.get().await.expect("get connection");
    client
        .query_one(&format!("select count(*) from {projection_table}"), &[])
        .await
        .expect("count projection rows")
        .get(0)
}

/// Creating a to-one relationship creates its settled parent projection
/// unconditionally (before any consumer exists), keyed by the to-side's own
/// primary key column, carrying exactly the two bookkeeping columns and no
/// data columns yet.
#[tokio::test]
async fn projection_table_is_created_for_a_to_one_relationship_with_bookkeeping_columns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full",
        )
        .await
        .expect("create tables");
    drop(client);

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");
    assert_eq!(created.cardinality, RelationshipCardinality::ToOne);

    let projection = relationship_projection(&db.pool, created.id)
        .await
        .expect("read projection catalog row")
        .expect("a to-one relationship must have a projection");
    assert_eq!(projection.relationship_id, created.id);
    assert_eq!(
        projection.projection_table,
        format!("_trellis_rel_projection_{}", created.id)
    );

    let cols = projection_columns(&db.pool, &projection.projection_table).await;
    assert_eq!(
        cols,
        vec![
            (
                "__trellis_gen".to_string(),
                "bigint".to_string(),
                "NO".to_string()
            ),
            (
                "__trellis_lsn".to_string(),
                "pg_lsn".to_string(),
                "NO".to_string()
            ),
            ("id".to_string(), "integer".to_string(), "NO".to_string()),
        ],
        "the projection should carry only its key + the two bookkeeping columns \
         until a consumer widens it"
    );

    // `gen` bumps by forward applies (#131/#132's job) but starts at 0; `lsn`
    // is seeded from a real captured LSN, never left `NULL` — see
    // `ddl::PROJECTION_GEN_COLUMN`/`PROJECTION_LSN_COLUMN`'s doc comments.
    let seeded: Vec<(i32, i64, String)> = db
        .pool
        .get()
        .await
        .expect("get connection")
        .query(
            &format!(
                "select id, __trellis_gen, __trellis_lsn::text from {}",
                projection.projection_table
            ),
            &[],
        )
        .await
        .expect("read seeded projection rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert!(seeded.is_empty(), "no categories were inserted yet");
}

/// The projection widens (`ALTER TABLE ... ADD COLUMN` + backfill) to cover a
/// newly-created consumer's read columns — and the union grows across
/// several consumers rather than each one clobbering what an earlier
/// consumer already added (issue #129's own scope line: "one projection can
/// serve several transforms").
#[tokio::test]
async fn projection_column_set_widens_to_cover_each_consumers_reads() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table categories (id integer primary key, name text, rank integer); \
             alter table categories replica identity full; \
             insert into categories (id, name, rank) values (10, 'Tech', 1), (20, 'News', 2); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table articles replica identity full; \
             insert into articles (id, category_id, title) values (1, 10, 'a1'), (2, 20, 'a2')",
        )
        .await
        .expect("create + seed tables");
    drop(client);

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");
    let projection_table = relationship_projection(&db.pool, created.id)
        .await
        .expect("read projection catalog row")
        .expect("a to-one relationship must have a projection")
        .projection_table;

    // First consumer reads only `category.name`.
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &numeric_columns(&["id", "category_id"]),
    )
    .await
    .expect("first consumer definition");

    let cols_after_first = projection_columns(&db.pool, &projection_table).await;
    assert!(
        cols_after_first.iter().any(|(name, _, _)| name == "name"),
        "widening should have added the `name` column: {cols_after_first:?}"
    );
    assert!(
        !cols_after_first.iter().any(|(name, _, _)| name == "rank"),
        "a column no consumer has read yet should not appear: {cols_after_first:?}"
    );

    // Second, independent consumer reads a *different* to-side column
    // (`rank`) through the same relationship.
    create_definition(
        &db.pool,
        "TRANSFORM article_rank FROM articles SELECT category.rank AS category_rank",
        &numeric_columns(&["id", "category_id"]),
    )
    .await
    .expect("second consumer definition");

    let cols_after_second = projection_columns(&db.pool, &projection_table).await;
    assert!(
        cols_after_second.iter().any(|(name, _, _)| name == "name"),
        "the first consumer's column must survive the second consumer's widen: \
         {cols_after_second:?}"
    );
    assert!(
        cols_after_second.iter().any(|(name, _, _)| name == "rank"),
        "the second consumer's column should now be present: {cols_after_second:?}"
    );

    // Both widened columns are backfilled for every existing to-side row —
    // not left NULL because they were added after the projection's own
    // initial backfill.
    let client = db.pool.get().await.expect("get connection");
    let mut rows: Vec<(i32, String, i32)> = client
        .query(
            &format!("select id, name, rank from {projection_table} order by id"),
            &[],
        )
        .await
        .expect("read widened projection data")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    rows.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        rows,
        vec![(10, "Tech".to_string(), 1), (20, "News".to_string(), 2)]
    );
}

/// Regression test for a review follow-up to #129: a to-side row inserted
/// *directly* (an ordinary replicated write, landing after the relationship
/// was declared and its projection backfilled, but before any consumer
/// exists to trigger a widen) must still show up in the projection once a
/// widen finally does run. Nothing keeps the projection continuously synced
/// yet — that's #130/#131 — so a widen that only `ALTER TABLE`s and
/// `UPDATE`s rows *already in the projection* would silently and
/// permanently strand this row, with no later mechanism to ever catch it up.
#[tokio::test]
async fn projection_widen_catches_up_a_to_side_row_inserted_before_the_widen() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             insert into categories (id, name) values (10, 'Tech'); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table articles replica identity full; \
             insert into articles (id, category_id, title) values (1, 10, 'a1')",
        )
        .await
        .expect("create + seed tables");
    drop(client);

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");
    let projection_table = relationship_projection(&db.pool, created.id)
        .await
        .expect("read projection catalog row")
        .expect("a to-one relationship must have a projection")
        .projection_table;

    assert_eq!(
        projection_row_count(&db.pool, &projection_table).await,
        1,
        "the projection's initial backfill should cover the one pre-existing category"
    );

    // A second category row lands as an ordinary write, with no consumer
    // and therefore no widen anywhere near it yet — exactly the window
    // #130/#131 don't cover.
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute("insert into categories (id, name) values (20, 'News')")
        .await
        .expect("insert a second category row directly");
    drop(client);

    // The first consumer's widen is the only thing that runs against this
    // relationship's projection from here on.
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &numeric_columns(&["id", "category_id"]),
    )
    .await
    .expect("first consumer definition");

    let client = db.pool.get().await.expect("get connection");
    let mut rows: Vec<(i32, String)> = client
        .query(
            &format!("select id, name from {projection_table} order by id"),
            &[],
        )
        .await
        .expect("read projection data")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![(10, "Tech".to_string()), (20, "News".to_string())],
        "the directly-inserted row must be caught up by the widen, not stranded"
    );

    // Bookkeeping is seeded for the caught-up row too, not left however
    // `insert ... default` would have left it (there is no `default` on
    // these columns, so a bug here would surface as a NOT NULL violation
    // rather than a silently-wrong value — this asserts the intended values
    // directly).
    let bookkeeping: Vec<(i32, i64, bool)> = client
        .query(
            &format!(
                "select id, __trellis_gen, __trellis_lsn is not null from {projection_table} \
                 order by id"
            ),
            &[],
        )
        .await
        .expect("read projection bookkeeping")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(bookkeeping, vec![(10, 0, true), (20, 0, true)]);
}

/// A to-side row whose (non-primary-key, plain-`UNIQUE`) key is `NULL` is
/// excluded from the projection's backfill outright: the projection's key
/// column is `NOT NULL` (it's the physical primary key of the projection
/// table too), and a `NULL` join key can never match a from-side row anyway
/// (`NULL <> NULL`), so there is no row for such a key to seed. This is a
/// different situation from #128's nullable *grouping*-key fix — a NULL
/// group there is a real bucket that needs storage, whereas a NULL to-side
/// key here is simply never a projection row at all.
#[tokio::test]
async fn projection_backfill_excludes_null_to_side_keys() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table categories (row_id integer primary key, code integer unique, \
             name text); \
             alter table categories replica identity full; \
             insert into categories (row_id, code, name) values \
             (1, 10, 'Tech'), (2, null, 'Orphaned'), (3, 30, 'News'); \
             create table articles (id integer primary key, category_code integer); \
             alter table articles replica identity full",
        )
        .await
        .expect("create + seed tables");
    drop(client);

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_code TO categories.code",
    )
    .await
    .expect("create to-one relationship over a nullable unique column");
    assert_eq!(created.cardinality, RelationshipCardinality::ToOne);

    let projection_table = relationship_projection(&db.pool, created.id)
        .await
        .expect("read projection catalog row")
        .expect("a to-one relationship must have a projection")
        .projection_table;

    assert_eq!(
        projection_row_count(&db.pool, &projection_table).await,
        2,
        "the NULL-keyed category row must be excluded from the backfill"
    );

    let client = db.pool.get().await.expect("get connection");
    let mut codes: Vec<i32> = client
        .query(&format!("select code from {projection_table}"), &[])
        .await
        .expect("read projection keys")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    codes.sort_unstable();
    assert_eq!(codes, vec![10, 30]);
}

/// A to-one relationship's to-side without `REPLICA IDENTITY FULL` is
/// rejected at `create_relationship` time, with the exact `ALTER TABLE ...
/// REPLICA IDENTITY FULL;` text an operator can copy verbatim — and no
/// relationship row (and therefore no projection) is left behind.
#[tokio::test]
async fn creating_a_to_one_relationship_without_replica_identity_full_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer)",
        )
        .await
        .expect("create tables");
    drop(client);

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "table categories needs its old row image for a derivation that requires it; run \
         this against the source database first: ALTER TABLE categories REPLICA IDENTITY \
         FULL;"
    );
    assert!(matches!(err, CatalogError::ReplicaIdentityRequired(_)));

    let count: i64 = db
        .pool
        .get()
        .await
        .expect("get connection")
        .query_one("select count(*) from relationship_definitions", &[])
        .await
        .expect("count relationship rows")
        .get(0);
    assert_eq!(count, 0, "the rejected relationship must leave no row");
}

/// Issue #158: a to-one relationship's *from*-side (child) table also needs
/// `REPLICA IDENTITY FULL`, not just its to-side — `from_col` is an ordinary
/// non-PK column, so under the from-side's default (PK-only) replica
/// identity a re-pointing `UPDATE` ships no old image at all (`old_image =
/// None` from `pgoutput`), which breaks anything (e.g. the `group_key` union
/// mechanism, issue #133) that needs to recover a row's true prior parent
/// from the replication message. The to-side here already carries `REPLICA
/// IDENTITY FULL` (satisfying the check `creating_a_to_one_relationship_without_replica_identity_full_is_rejected`
/// covers), isolating this rejection to the from-side gate alone; the exact
/// `ALTER TABLE ... REPLICA IDENTITY FULL;` text still names the from-side
/// table specifically, so an operator can copy it verbatim.
#[tokio::test]
async fn creating_a_to_one_relationship_without_from_side_replica_identity_full_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             create table articles (id integer primary key, category_id integer)",
        )
        .await
        .expect("create tables");
    drop(client);

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "table articles needs its old row image for a derivation that requires it; run \
         this against the source database first: ALTER TABLE articles REPLICA IDENTITY \
         FULL;"
    );
    assert!(matches!(err, CatalogError::ReplicaIdentityRequired(_)));

    let count: i64 = db
        .pool
        .get()
        .await
        .expect("get connection")
        .query_one("select count(*) from relationship_definitions", &[])
        .await
        .expect("count relationship rows")
        .get(0);
    assert_eq!(count, 0, "the rejected relationship must leave no row");
}

/// Issue #158's positive counterpart: once *both* endpoints of a to-one
/// relationship carry `REPLICA IDENTITY FULL` — the to-side (issue #129) and
/// the from-side (issue #158) — `create_relationship` accepts it.
#[tokio::test]
async fn creating_a_to_one_relationship_with_both_sides_replica_identity_full_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full",
        )
        .await
        .expect("create tables");
    drop(client);

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("both endpoints carry REPLICA IDENTITY FULL, so this must be accepted");
    assert_eq!(created.cardinality, RelationshipCardinality::ToOne);
}

/// A to-many relationship never gets a projection — Phase 1 of this epic
/// (`issue-102-PLAN-DRAFT.md` §7) is to-one relationship *values* only. A
/// to-many relationship's aggregate reads keep going through the existing
/// `rel_joins`/ring-based delta path this epic doesn't touch.
#[tokio::test]
async fn a_to_many_relationship_gets_no_projection() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table posts (id integer primary key, word_count integer); \
             alter table posts replica identity full; \
             create table comments (id integer primary key, post_id integer); \
             alter table comments replica identity full",
        )
        .await
        .expect("create tables");
    drop(client);

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM posts.id TO comments.post_id",
    )
    .await
    .expect("create to-many relationship");
    assert_eq!(created.cardinality, RelationshipCardinality::ToMany);

    let projection = relationship_projection(&db.pool, created.id)
        .await
        .expect("read projection catalog row");
    assert!(
        projection.is_none(),
        "a to-many relationship must not get a settled parent projection"
    );
}

/// Issue #435: a projection's name (`_trellis_rel_projection_<id>`) is only
/// unique within one catalog, since every instance numbers its relationships
/// from 1. Two instances sharing a target schema (both on the default
/// `public`) that each declare a to-one relationship get the same generated
/// name, so a projection placed in the target schema was shared: the second
/// instance adopted the first's table and caught its own to-side keys up into
/// it. Projections live in each instance's own catalog schema instead, where
/// the name can't collide.
#[tokio::test]
async fn two_instances_sharing_a_target_schema_keep_independent_projections() {
    use trellis::config::DEFAULT_SCHEMA;
    use trellis::{Config, Pool, migrate};

    const INSTANCE_B: &str = "instance_b";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table public.categories (id integer primary key, name text); \
             alter table public.categories replica identity full; \
             create table public.articles (id integer primary key, category_id integer); \
             alter table public.articles replica identity full; \
             insert into public.categories (id, name) values (1, 'news'), (2, 'sport'); \
             create table public.brands (id integer primary key, name text); \
             alter table public.brands replica identity full; \
             create table public.products (id integer primary key, brand_id integer); \
             alter table public.products replica identity full; \
             insert into public.brands (id, name) values (10, 'acme'), (20, 'globex')",
        )
        .await
        .expect("create both instances' tables in the shared target schema");
    drop(client);

    let config_b = Config::with_schema(db.dsn(), INSTANCE_B).expect("valid schema");
    assert_eq!(config_b.target_schema(), "public");
    let pool_b = Pool::new(&config_b).expect("build instance B's pool");
    migrate(&pool_b, &config_b)
        .await
        .expect("migrate instance B");

    let rel_a = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("instance A declares a to-one relationship");
    let rel_b = create_relationship(
        &pool_b,
        "RELATIONSHIP brand FROM products.brand_id TO brands.id",
    )
    .await
    .expect("instance B declares a to-one relationship");
    assert_eq!(
        rel_a.id, rel_b.id,
        "each catalog numbers its relationships from 1, so the generated names collide"
    );

    let projection_a = relationship_projection(&db.pool, rel_a.id)
        .await
        .expect("read A's projection")
        .expect("A's to-one relationship has a projection");
    let projection_b = relationship_projection(&pool_b, rel_b.id)
        .await
        .expect("read B's projection")
        .expect("B's to-one relationship has a projection");
    let client = db.pool.get().await.expect("get connection");
    let keys = |qualified: String| {
        let client = &client;
        async move {
            client
                .query(&format!("select id from {qualified} order by id"), &[])
                .await
                .unwrap_or_else(|e| panic!("read {qualified}: {e}"))
                .into_iter()
                .map(|row| row.get::<_, i32>(0))
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        keys(projection_a.qualified_table()).await,
        vec![1, 2],
        "A's projection holds only A's to-side keys"
    );
    assert_eq!(
        keys(projection_b.qualified_table()).await,
        vec![10, 20],
        "B's projection holds only B's to-side keys"
    );

    assert_eq!(projection_a.projection_schema, DEFAULT_SCHEMA);
    assert_eq!(projection_b.projection_schema, INSTANCE_B);
    assert_ne!(
        projection_a.qualified_table(),
        projection_b.qualified_table()
    );

    let in_target_schema: i64 = client
        .query_one(
            "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = 'public' and c.relname like '\\_trellis\\_rel\\_projection\\_%'",
            &[],
        )
        .await
        .expect("look for projections in the target schema")
        .get(0);
    assert_eq!(
        in_target_schema, 0,
        "no projection lives in the target schema"
    );
}
