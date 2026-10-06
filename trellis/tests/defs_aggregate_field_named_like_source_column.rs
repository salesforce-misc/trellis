//! Issue #695: a field named after a source column (`SUM(val) AS val`) used to
//! be substituted into a later field's aggregate argument, so
//! `MIN(val) AS lo` read as `MIN(SUM(val))`. Define accepted it and every
//! build attempt failed. An aggregate's argument now reads the source column,
//! as in SQL, and a name that is no source column and names another aggregate
//! field is refused at define.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, install_definition};
use trellis::intake::markers;
use trellis::integer::IntWidth;
use trellis::staging::{has_pending, retire_drained_segments};

const TEST_NAME: &str = "defs_aggregate_field_named_like_source_column_test";

/// The definition from the issue.
const AGG_TOTALS: &str = "TRANSFORM agg_totals FROM public.agg_src GROUP BY grp \
    SELECT grp AS grp, SUM(val) AS val, COUNT(*) AS row_count, MIN(val) AS lo, MAX(val) AS hi";

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Text),
        ("val".to_string(), ValueType::Integer(IntWidth::Int4)),
    ])
}

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

async fn drain_backfill_chunks(pool: &trellis::Pool) {
    markers::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, TEST_NAME, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                TEST_NAME,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("the build must not fail");
            chunk_queue::finish_chunk(pool, chunk, TEST_NAME)
                .await
                .expect("finish_chunk");
        }
    }
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, seal};
    let watermark = trellis::staging::StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            TEST_NAME,
            1,
            "trellis_defs_aggregate_field_named_like_source_column_test",
            &watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

/// `grp -> (val, row_count, lo, hi)` as text.
async fn target_rows(client: &Client) -> Vec<(String, String, String, String, String)> {
    client
        .query(
            "select grp, val::text, row_count::text, lo::text, hi::text \
             from agg_totals order by grp",
            &[],
        )
        .await
        .expect("read agg_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect()
}

fn row(
    grp: &str,
    val: &str,
    n: &str,
    lo: &str,
    hi: &str,
) -> (String, String, String, String, String) {
    (
        grp.to_string(),
        val.to_string(),
        n.to_string(),
        lo.to_string(),
        hi.to_string(),
    )
}

/// The issue's example builds the values SQL gives.
#[tokio::test]
async fn an_aggregate_field_named_like_a_source_column_reads_the_source_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.agg_src (id integer primary key, grp text, val integer); \
             insert into public.agg_src (id, grp, val) values \
               (1, 'a', 5), (2, 'a', 7), (3, 'a', 2), (4, 'b', 9)",
        )
        .await
        .expect("create + seed agg_src");

    install_definition(&db.pool, AGG_TOTALS, &source_columns(), "public")
        .await
        .expect("define accepts the issue's definition");
    drain_backfill_chunks(&db.pool).await;
    markers::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_rows(&client).await,
        vec![row("a", "14", "3", "2", "7"), row("b", "9", "1", "9", "9"),],
        "val is SUM(val), lo and hi are the MIN and MAX of the source column"
    );
}

/// A name inside an aggregate's argument that is no source column and names
/// another aggregate field would nest one aggregate in another; define refuses
/// it instead of accepting a definition that can never build.
#[tokio::test]
async fn an_aggregate_of_another_aggregate_field_is_refused_at_define() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.agg_src (id integer primary key, grp text, val integer)",
        )
        .await
        .expect("create agg_src");

    let err = install_definition(
        &db.pool,
        "TRANSFORM nested FROM public.agg_src GROUP BY grp \
         SELECT grp AS grp, SUM(val) AS total, MAX(total) AS biggest",
        &source_columns(),
        "public",
    )
    .await
    .expect_err("MAX(total) would nest SUM inside MAX");
    let message = err.to_string();
    assert!(
        message.contains("biggest") && message.contains("total"),
        "the refusal names the field and what it aggregates: {message}"
    );
}
