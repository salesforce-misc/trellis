//! Issue #1322: a definition registered over a table another definition
//! already reads `live` must not have its marker's discharge enumerate the
//! whole table for that reader. The live reader's build read every row and
//! capture has staged every change since, so the `Recompute` rows would do
//! nothing for it, and on the group basis each page of them recomputes
//! nearly every group from the source.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::intake::markers;
use trellis::integer::IntWidth;
use trellis::staging::StagedWatermark;

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

fn types() -> std::collections::HashMap<String, ValueType> {
    ["id", "g", "h", "v"]
        .iter()
        .map(|c| (c.to_string(), ValueType::Integer(IntWidth::Int4)))
        .collect()
}

async fn capture(client: &mut Client) {
    let outcome = trellis::capture::reconcile::reconcile(
        client,
        DEFAULT_SCHEMA,
        &["public.src".to_string()],
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("capture pass");
    assert!(
        outcome.failed.is_empty() && outcome.waiting.is_empty(),
        "{outcome:?}"
    );
}

/// `Recompute` rows for `public.src` in every ring segment.
async fn recomputes(client: &Client) -> i64 {
    let segments: Vec<String> = client
        .query(
            "select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname ~ '^seg_[0-9]+$' and c.relkind = 'r'",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("list segments")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    let union: Vec<String> = segments
        .iter()
        .map(|s| format!("select 1 from {s} where op = 'recompute' and src_table = 'public.src'"))
        .collect();
    client
        .query_one(
            &format!("select count(*) from ({}) r", union.join(" union all ")),
            &[],
        )
        .await
        .expect("count recomputes")
        .get(0)
}

async fn discharge(client: &mut Client) {
    markers::run_pending_backfills(
        client,
        "w",
        &StagedWatermark::saturated(),
        Duration::from_secs(5),
    )
    .await
    .expect("discharge");
}

async fn status(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions where target_table = $1",
            &[&format!("public.{target}")],
        )
        .await
        .expect("read status")
        .get(0)
}

/// `first` built and `live` over 1,000 rows of `src`, then `second`
/// registered over it and its marker parked as the staging worker parks it.
/// Returns the `Recompute` rows the second marker's discharge staged.
async fn second_definition(first: &str, second: &str) -> i64 {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.src (id integer primary key, g integer, h integer, v integer); \
             create index on public.src (g, h); \
             insert into public.src select i, i % 7, i % 3, i from generate_series(1, 1000) i",
        )
        .await
        .expect("seed");
    install_definition(&db.pool, first, &types(), "public")
        .await
        .expect("define the first");
    capture(&mut client).await;
    markers::settle_registrations(&db.pool).await;
    discharge(&mut client).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    discharge(&mut client).await;
    let first_target = first.split_whitespace().nth(1).expect("name");
    assert_eq!(
        status(&client, first_target).await,
        "live",
        "the first is live"
    );
    let before = recomputes(&client).await;

    install_definition(&db.pool, second, &types(), "public")
        .await
        .expect("define the second");
    capture(&mut client).await;
    let ids: Vec<i64> = client
        .query(
            "select id from transform_definitions where status = 'waiting_to_backfill'",
            &[],
        )
        .await
        .expect("waiting ids")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    markers::park_ready_registration_markers(&client, &ids)
        .await
        .expect("park the registration marker");
    discharge(&mut client).await;
    recomputes(&client).await - before
}

#[tokio::test]
async fn a_second_aggregate_reading_a_new_column_enumerates_nothing_for_a_live_aggregate() {
    let staged = second_definition(
        "TRANSFORM agg1 FROM src GROUP BY g WITHOUT SOURCE INDEX SELECT sum(v) AS total",
        "TRANSFORM agg2 FROM src GROUP BY g, h WITHOUT SOURCE INDEX SELECT sum(v) AS total",
    )
    .await;
    assert_eq!(
        staged, 0,
        "the registration's discharge enumerated for the live reader"
    );
}

#[tokio::test]
async fn a_second_aggregate_over_the_same_columns_enumerates_nothing_for_a_live_aggregate() {
    let staged = second_definition(
        "TRANSFORM agg1 FROM src GROUP BY g WITHOUT SOURCE INDEX SELECT sum(v) AS total",
        "TRANSFORM agg2 FROM src GROUP BY g WITHOUT SOURCE INDEX SELECT count(*) AS n",
    )
    .await;
    assert_eq!(
        staged, 0,
        "the registration's discharge enumerated for the live reader"
    );
}

#[tokio::test]
async fn a_second_definition_enumerates_nothing_for_a_live_one_to_one_reader() {
    let staged = second_definition(
        "TRANSFORM copy1 FROM src SELECT g AS g, v AS v",
        "TRANSFORM agg2 FROM src GROUP BY g, h WITHOUT SOURCE INDEX SELECT sum(v) AS total",
    )
    .await;
    assert_eq!(
        staged, 0,
        "the registration's discharge enumerated for the live reader"
    );
}
