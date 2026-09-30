//! Issue #376: a second Trellis instance must not consume another instance's
//! aggregate target over logical replication.
//!
//! An aggregate target has no primary key (its identity is the `UNIQUE NULLS
//! NOT DISTINCT` grouping constraint), so CDC can't key its changes. The
//! owning instance reaches its own readers of that target through the
//! in-transaction downstream `Recompute` (`staging::apply` step 4), never
//! through CDC. A *different* instance has no such path: before this fix it
//! accepted the definition and added the target to its own publication (now
//! its capture).
//! Postgres then refused every one of the owning instance's updates to its
//! own target (`cannot update table ... because it does not have a replica
//! identity and publishes updates`), or, under `REPLICA IDENTITY FULL`, the
//! reading instance's intake died on the first change with
//! `MissingKeyValue`.
//!
//! These tests drive both instances' catalogs directly and never wait for a
//! live pipeline to converge.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{
    CatalogError, RelationshipSide, ValueType, create_relationship, install_definition,
    publication_tables,
};
use trellis::integer::IntWidth;
use trellis::{Config, Pool, migrate};

const INSTANCE_B: &str = "instance_b";

async fn connect_raw(dsn: &str, schema: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {schema}, public"))
        .await
        .expect("set search_path");
    client
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

fn sales_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("sku", ValueType::Text),
        ("amount", ValueType::Numeric),
    ])
}

fn sku_totals_columns() -> HashMap<String, ValueType> {
    columns(&[("sku", ValueType::Text), ("total", ValueType::Numeric)])
}

/// Instance A (the isolated database's default instance) owns `sales` and the
/// aggregate target `public.sku_totals` built from it. Instance B is a second
/// instance in the same database, with its own catalog schema and its own
/// publication, as its `Client` would create at startup.
async fn two_instances(db: &testkit::TestDatabase) -> (Pool, Client) {
    let a = connect_raw(db.dsn(), DEFAULT_SCHEMA).await;
    a.batch_execute(
        "create table public.sales (id integer primary key, sku text, amount integer); \
         alter table public.sales replica identity full; \
         insert into public.sales (id, sku, amount) values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
    )
    .await
    .expect("create instance A's source");
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
        &sales_columns(),
        "public",
    )
    .await
    .expect("instance A installs its aggregate");
    // The aggregate builds in the background (#419); settle it so A's target
    // is live and populated, as a chain off it requires.
    trellis::intake::publication::settle_registrations(&db.pool).await;

    let config_b = Config::with_schema(db.dsn(), INSTANCE_B).expect("valid schema");
    let pool_b = Pool::new(&config_b).expect("build instance B's pool");
    migrate(&pool_b, &config_b)
        .await
        .expect("migrate instance B");
    let b = connect_raw(db.dsn(), INSTANCE_B).await;
    (pool_b, b)
}

/// Brings instance B's capture to the tables its catalog says to capture,
/// exactly as B's maintenance loop would (`client::reconcile_source_tables`,
/// via `defs::publication_tables`).
async fn reconcile_b(pool_b: &Pool, b: &mut Client) -> Vec<String> {
    let desired = publication_tables(pool_b)
        .await
        .expect("instance B's source tables");
    let outcome = trellis::capture::reconcile::reconcile(
        b,
        INSTANCE_B,
        &desired,
        std::time::Instant::now() + std::time::Duration::from_secs(5),
    )
    .await
    .expect("reconcile instance B's capture");
    assert!(
        outcome.failed.is_empty() && outcome.waiting.is_empty(),
        "{outcome:?}"
    );
    desired
}

/// Whether instance B has capture installed on `public.<table>`.
async fn b_captures(b: &Client, table: &str) -> bool {
    trellis::capture::reconcile::installed_tables(b, INSTANCE_B)
        .await
        .expect("read instance B's capture")
        .contains(&format!("public.{table}"))
}

async fn table_exists(client: &Client, qualified: &str) -> bool {
    client
        .query_one("select to_regclass($1) is not null", &[&qualified])
        .await
        .expect("to_regclass")
        .get(0)
}

fn assert_rejected_as_unkeyed(result: Result<trellis::defs::Definition, CatalogError>) {
    match result {
        Err(CatalogError::SourceNotChangeKeyed { source_table }) => {
            assert_eq!(source_table, "public.sku_totals");
        }
        Err(other) => panic!("expected SourceNotChangeKeyed, got {other:?}"),
        Ok(def) => panic!(
            "instance B accepted a definition over instance A's aggregate target: {}",
            def.target_table
        ),
    }
}

/// A 1-1 transform in instance B over instance A's aggregate target. The
/// target keeps its default replica identity (a 1-1 reader asks for nothing
/// more), so before the fix the damage lands on instance A: once B publishes
/// the target, Postgres refuses A's own updates to it.
#[tokio::test]
async fn a_one_to_one_over_another_instances_aggregate_target_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (pool_b, mut b) = two_instances(&db).await;

    let result = install_definition(
        &pool_b,
        "TRANSFORM sku_copy FROM public.sku_totals SELECT total AS total_copy",
        &sku_totals_columns(),
        "public",
    )
    .await;

    let desired = reconcile_b(&pool_b, &mut b).await;

    // What instance A's apply does to its own target on every batch.
    let a = connect_raw(db.dsn(), DEFAULT_SCHEMA).await;
    a.execute(
        "update public.sku_totals set total = total + 1 where sku = 'a'",
        &[],
    )
    .await
    .expect("instance A must still be able to update its own aggregate target");

    assert!(
        !desired.contains(&"public.sku_totals".to_string()),
        "instance B must not count instance A's aggregate target as a source: {desired:?}"
    );
    assert!(!b_captures(&b, "sku_totals").await);
    assert_rejected_as_unkeyed(result);
    assert!(
        !table_exists(&b, "public.sku_copy").await,
        "the rejection must come before instance B builds a target table"
    );
}

/// An aggregate in instance B over instance A's aggregate target. B's own
/// replica-identity check makes the operator put `REPLICA IDENTITY FULL` on
/// the target, so A's writes go through, but CDC still has no key for the
/// target's changes and B's intake would stop on the first one.
#[tokio::test]
async fn an_aggregate_over_another_instances_aggregate_target_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (pool_b, mut b) = two_instances(&db).await;
    b.batch_execute("alter table public.sku_totals replica identity full")
        .await
        .expect("widen sku_totals's replica identity");

    let result = install_definition(
        &pool_b,
        "TRANSFORM sku_rollup FROM public.sku_totals GROUP BY sku SELECT sum(total) AS total2",
        &sku_totals_columns(),
        "public",
    )
    .await;
    assert_rejected_as_unkeyed(result);

    reconcile_b(&pool_b, &mut b).await;
    assert!(!b_captures(&b, "sku_totals").await);
    assert!(!table_exists(&b, "public.sku_rollup").await);
}

/// The exemption: instance A's own chain off its aggregate target is still
/// accepted, since A propagates writes to that target in the same
/// transaction rather than over CDC.
#[tokio::test]
async fn the_owning_instance_can_still_chain_off_its_own_aggregate_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let a = connect_raw(db.dsn(), DEFAULT_SCHEMA).await;
    let (_pool_b, _b) = two_instances(&db).await;
    a.batch_execute("alter table public.sku_totals replica identity full")
        .await
        .expect("widen sku_totals's replica identity");

    install_definition(
        &db.pool,
        "TRANSFORM sku_rollup FROM sku_totals GROUP BY sku SELECT sum(total) AS total2",
        &sku_totals_columns(),
        "public",
    )
    .await
    .expect("instance A chains an aggregate off its own aggregate target");
    install_definition(
        &db.pool,
        "TRANSFORM sku_copy FROM sku_totals SELECT total AS total_copy",
        &sku_totals_columns(),
        "public",
    )
    .await
    .expect("instance A chains a 1-1 off its own aggregate target");
}

/// A 1-1 target has a real primary key, so another instance can still read it
/// over CDC (issue #312's cross-instance hop), and that instance still
/// captures it.
#[tokio::test]
async fn another_instances_one_to_one_target_is_still_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (pool_b, mut b) = two_instances(&db).await;
    install_definition(
        &db.pool,
        "TRANSFORM sales_copy FROM sales SELECT amount AS amount_copy",
        &sales_columns(),
        "public",
    )
    .await
    .expect("instance A installs a 1-1");

    install_definition(
        &pool_b,
        "TRANSFORM sales_copy_b FROM public.sales_copy SELECT amount_copy AS amount_b",
        &columns(&[
            ("id", ValueType::Numeric),
            ("amount_copy", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("instance B reads instance A's 1-1 target");

    reconcile_b(&pool_b, &mut b).await;
    assert!(b_captures(&b, "sales_copy").await);
}

/// Issue #375: a relationship endpoint reaches instance B's capture through
/// `all_source_tables`' relationship walk, not as a definition's source, so it
/// must pass the same keying rule. Instance A's aggregate target fails it
/// whichever side of the relationship it is on; a plain table without a
/// primary key fails it too. A 1-1 target of A's has a key and is accepted.
#[tokio::test]
async fn a_relationship_endpoint_intake_cant_key_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (pool_b, mut b) = two_instances(&db).await;
    let a = connect_raw(db.dsn(), DEFAULT_SCHEMA).await;
    a.batch_execute(
        "create table public.visits (id integer primary key, region integer); \
         alter table public.visits replica identity full; \
         create table public.stores (id integer primary key, region integer); \
         alter table public.stores replica identity full; \
         create table public.keyless (region integer not null unique, name text); \
         alter table public.keyless replica identity full",
    )
    .await
    .expect("create instance A's tables");
    let int_columns = columns(&[
        ("id", ValueType::Integer(IntWidth::Int4)),
        ("region", ValueType::Integer(IntWidth::Int4)),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM region_visits FROM visits GROUP BY region SELECT COUNT(*) AS n",
        &int_columns,
        "public",
    )
    .await
    .expect("instance A installs an aggregate grouped on an integer");
    a.batch_execute("alter table public.region_visits replica identity full")
        .await
        .expect("widen region_visits's replica identity");
    install_definition(
        &db.pool,
        "TRANSFORM stores_copy FROM stores SELECT region AS region",
        &int_columns,
        "public",
    )
    .await
    .expect("instance A installs a 1-1");

    for (text, side, endpoint) in [
        (
            "RELATIONSHIP visits FROM stores.region TO region_visits.region",
            RelationshipSide::To,
            "public.region_visits",
        ),
        (
            "RELATIONSHIP store FROM region_visits.region TO stores.region",
            RelationshipSide::From,
            "public.region_visits",
        ),
        (
            "RELATIONSHIP named FROM stores.region TO keyless.region",
            RelationshipSide::To,
            "public.keyless",
        ),
    ] {
        match create_relationship(&pool_b, text).await {
            Err(CatalogError::RelationshipEndpointNotChangeKeyed {
                side: got_side,
                endpoint: got,
            }) => {
                assert_eq!(got_side, side, "{text}");
                assert_eq!(got, endpoint, "{text}");
            }
            other => {
                panic!("expected RelationshipEndpointNotChangeKeyed for {text}, got {other:?}")
            }
        }
    }

    // A's 1-1 target is keyed, so B may relate to it. It is a plain table to
    // B, though, so B only checks its replica identity and never sets it:
    // `REPLICA IDENTITY FULL` on an endpoint is set only on the instance's own
    // targets.
    const COPY: &str = "RELATIONSHIP copy FROM stores.id TO stores_copy.id";
    match create_relationship(&pool_b, COPY).await {
        Err(CatalogError::ReplicaIdentityRequired(_)) => {}
        other => panic!("expected ReplicaIdentityRequired for A's target, got {other:?}"),
    }
    let identity: String = b
        .query_one(
            "select relreplident::text from pg_class where oid = 'public.stores_copy'::regclass",
            &[],
        )
        .await
        .expect("read relreplident")
        .get(0);
    assert_eq!(identity, "d");
    a.batch_execute("alter table public.stores_copy replica identity full")
        .await
        .expect("widen stores_copy's replica identity");
    create_relationship(&pool_b, COPY)
        .await
        .expect("instance B relates to A's 1-1 target");
    install_definition(
        &pool_b,
        "TRANSFORM store_view FROM public.stores SELECT copy.region AS copied_region",
        &int_columns,
        "public",
    )
    .await
    .expect("instance B reads through the relationship");
    let desired = reconcile_b(&pool_b, &mut b).await;
    assert!(b_captures(&b, "stores_copy").await, "{desired:?}");
}

/// The rule is about the table, not its owner, so a plain source table the
/// capture triggers can't key is held to it too (issue #622 C5): every ring
/// row is keyed by the primary key, so a table without one is refused whatever
/// its replica identity (a unique index under `FULL`, or `REPLICA IDENTITY
/// USING INDEX`), and so is a partitioned table, which statement triggers on
/// its parent would capture only partially. A plain table with a primary key
/// is keyed whatever its replica identity, `NOTHING` included.
#[tokio::test]
async fn a_plain_source_is_held_to_the_same_keying_rule() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let a = connect_raw(db.dsn(), DEFAULT_SCHEMA).await;
    a.batch_execute(
        "create table public.full_no_pk (code text not null unique, amount integer); \
         alter table public.full_no_pk replica identity full; \
         create table public.pk_nothing (id integer primary key, amount integer); \
         alter table public.pk_nothing replica identity nothing; \
         create table public.using_index (code text not null, amount integer); \
         create unique index using_index_code on public.using_index (code); \
         alter table public.using_index replica identity using index using_index_code; \
         create table public.parted (code text not null, amount integer, primary key (code)) \
             partition by list (code); \
         create table public.parted_a partition of public.parted for values in ('a')",
    )
    .await
    .expect("create plain sources");
    let coded = columns(&[("code", ValueType::Text), ("amount", ValueType::Numeric)]);

    for source in ["full_no_pk", "using_index", "parted"] {
        let result = install_definition(
            &db.pool,
            &format!("TRANSFORM {source}_copy FROM {source} SELECT amount AS amount_copy"),
            &coded,
            "public",
        )
        .await;
        match result {
            Err(CatalogError::SourceNotChangeKeyed { source_table }) => {
                assert_eq!(source_table, format!("public.{source}"));
            }
            other => panic!("expected SourceNotChangeKeyed for {source}, got {other:?}"),
        }
    }

    install_definition(
        &db.pool,
        "TRANSFORM pk_nothing_copy FROM pk_nothing SELECT amount AS amount_copy",
        &columns(&[("id", ValueType::Numeric), ("amount", ValueType::Numeric)]),
        "public",
    )
    .await
    .expect("a source with a primary key is keyed under any replica identity");
}
