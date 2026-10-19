//! `Trellis::status` of a `JOIN` transform (#1112 J7b): the hidden tables'
//! sizes, the `ON` columns with no usable index, and the warning when `P`
//! approaches the fact table. Warnings only: the definition's status is
//! unaffected. Nothing here waits for anything (#297): each report is a
//! catalog read, and the sizes are made current by an explicit `ANALYZE`.

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, JoinStatus, Trellis, TrellisOptions};

const BY_REGION: &str = "TRANSFORM by_region FROM orders LEFT JOIN items \
     ON orders.id = items.order_id GROUP BY orders.region \
     SELECT orders.region AS region, COUNT(*) AS n, SUM(items.qty) AS qty";

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn join_status(trellis: &Trellis) -> JoinStatus {
    trellis
        .status("by_region")
        .await
        .expect("status")
        .expect("the definition exists")
        .join
        .expect("a JOIN reports its join status")
}

#[tokio::test]
async fn status_reports_hidden_tables_unindexed_on_columns_and_a_large_p() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id integer primary key, region text); \
         create table public.items (id integer primary key, order_id integer, qty integer)",
    )
    .await
    .expect("create tables");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis.apply(BY_REGION).await.expect("define");

    // A fresh definition: four empty hidden tables, the joined side's key
    // unindexed (the `FROM` side's is its primary key), no warning.
    let fresh = join_status(&trellis).await;
    let names: Vec<_> = fresh.hidden_tables.iter().map(|t| t.table).collect();
    assert_eq!(names, ["P", "M", "J", "T"]);
    assert!(fresh.hidden_tables.iter().all(|t| t.estimated_rows == 0));
    assert!(fresh.hidden_tables.iter().all(|t| t.bytes > 0));
    assert_eq!(fresh.unindexed_on.len(), 1, "{:?}", fresh.unindexed_on);
    let on = &fresh.unindexed_on[0];
    assert_eq!(
        (on.table.as_str(), on.column.as_str()),
        ("public.items", "order_id")
    );
    assert_eq!(on.fix(), "create an index on public.items (order_id)");
    assert_eq!(fresh.partials_warning, None);

    // An index clears the entry on the next call.
    raw.batch_execute("create index on public.items (order_id)")
        .await
        .expect("create index");
    assert_eq!(join_status(&trellis).await.unindexed_on, []);

    // A P about as big as the fact table warns; one that compresses it
    // does not.
    let id: i64 = raw
        .query_one(
            "select id from transform_definitions where target_table = 'public.by_region'",
            &[],
        )
        .await
        .expect("id")
        .get(0);
    raw.batch_execute(&format!(
        "insert into public.orders select g, 'r' || g from generate_series(1, 2000) g; \
         insert into {DEFAULT_SCHEMA}.join_{id}_p (region, id, __n) \
             select 'r' || g, g, 1 from generate_series(1, 400) g; \
         analyze public.orders; analyze {DEFAULT_SCHEMA}.join_{id}_p"
    ))
    .await
    .expect("seed");
    assert_eq!(join_status(&trellis).await.partials_warning, None);
    raw.batch_execute(&format!(
        "insert into {DEFAULT_SCHEMA}.join_{id}_p (region, id, __n) \
             select 'r' || g, g, 1 from generate_series(401, 1500) g; \
         analyze {DEFAULT_SCHEMA}.join_{id}_p"
    ))
    .await
    .expect("grow P");
    let status = join_status(&trellis).await;
    let warning = status.partials_warning.expect("P is 75% of the fact table");
    assert_eq!((warning.partials_rows, warning.fact_rows), (1500, 2000));
    let p = &status.hidden_tables[0];
    assert_eq!((p.table, p.estimated_rows), ("P", 1500));
}

#[tokio::test]
async fn a_plain_transform_has_no_join_status() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute("create table public.orders (id integer primary key, region text)")
        .await
        .expect("create table");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply("TRANSFORM plain FROM orders SELECT region AS r")
        .await
        .expect("define");
    let status = trellis
        .status("plain")
        .await
        .expect("status")
        .expect("exists");
    assert_eq!(status.join, None);
}

/// A composite `ON` key is indexed when any of its columns leads a usable
/// index (a lookup by the whole key seeks on it), a partial index doesn't
/// count, and a side whose `ON` column was dropped is left out: there is no
/// index to create on a column that doesn't exist.
#[tokio::test]
async fn a_composite_key_counts_any_leading_column_and_a_dropped_one_is_left_out() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id integer primary key, region text); \
         create table public.items (id integer primary key, order_id integer, \
                                    region text, qty integer)",
    )
    .await
    .expect("create tables");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply(
            "TRANSFORM by_region FROM orders LEFT JOIN items \
             ON orders.id = items.order_id AND orders.region = items.region \
             GROUP BY orders.region \
             SELECT orders.region AS region, COUNT(*) AS n, SUM(items.qty) AS qty",
        )
        .await
        .expect("define");

    let unindexed = join_status(&trellis).await.unindexed_on;
    let tables: Vec<_> = unindexed
        .iter()
        .map(|on| (on.table.as_str(), on.column.as_str()))
        .collect();
    assert_eq!(tables, [("public.items", "order_id, region")]);

    raw.batch_execute("create index on public.items (region) where qty > 0")
        .await
        .expect("partial index");
    assert_eq!(
        join_status(&trellis).await.unindexed_on.len(),
        1,
        "a partial index doesn't count"
    );
    raw.batch_execute("create index on public.items (region, id)")
        .await
        .expect("index");
    assert_eq!(join_status(&trellis).await.unindexed_on, []);

    raw.batch_execute(
        "drop index public.items_region_id_idx; \
         alter table public.items drop column order_id",
    )
    .await
    .expect("drop the key column");
    assert_eq!(join_status(&trellis).await.unindexed_on, []);
}
