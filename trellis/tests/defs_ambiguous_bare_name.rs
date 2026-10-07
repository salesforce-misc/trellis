//! Issue #830: a bare name that is both a source column and a field whose
//! expression is something else is refused at define, naming both meanings
//! and both qualified spellings. A qualified `<table>.<column>` reads the
//! source column.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, install_definition};
use trellis::intake::markers;
use trellis::integer::IntWidth;
use trellis::staging::{has_pending, retire_drained_segments};

const TEST_NAME: &str = "defs_ambiguous_bare_name_test";

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Integer(IntWidth::Int4)),
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
            "trellis_defs_ambiguous_bare_name_test",
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

/// The issue's example: `val` is a source column and a field, and `SUM(val)`
/// used to read the field. Define refuses it and creates nothing; the message
/// spells the schema the bare `FROM` resolved to.
#[tokio::test]
async fn an_ambiguous_bare_name_is_refused_at_define() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.amb_src (id integer primary key, grp integer, val integer)",
        )
        .await
        .expect("create amb_src");

    let err = install_definition(
        &db.pool,
        "TRANSFORM amb_totals FROM amb_src GROUP BY grp \
         SELECT grp AS grp, (grp + 1) AS val, SUM(val) AS total",
        &source_columns(),
        "public",
    )
    .await
    .expect_err("SUM(val) could mean the source column or the field");
    assert_eq!(
        err.to_string(),
        "definition failed validation: calculated field 'total' reads 'val', which is ambiguous: it names both the source \
         column 'val' of 'amb_src' and the calculated field 'val' (`(grp + 1)`). Write \
         `amb_src.val` or `public.amb_src.val` to read the source column, or rename the field \
         'val' and read it by its new name"
    );
    let target_exists: bool = client
        .query_one("select to_regclass('public.amb_totals') is not null", &[])
        .await
        .expect("look up the target")
        .get(0);
    assert!(
        !target_exists,
        "a refused definition leaves no target behind"
    );
}

/// The qualified spelling defines and builds `SUM` and `MAX` over the source
/// column, beside the field that shadows it.
#[tokio::test]
async fn a_qualified_name_aggregates_the_source_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.amb_src (id integer primary key, grp integer, val integer); \
             insert into public.amb_src (id, grp, val) values (1, 1, 5), (2, 1, 7), (3, 2, 9)",
        )
        .await
        .expect("create + seed amb_src");

    install_definition(
        &db.pool,
        "TRANSFORM amb_totals FROM amb_src GROUP BY grp \
         SELECT grp AS grp, (grp + 1) AS val, SUM(amb_src.val) AS total, \
         MAX(public.amb_src.val) AS biggest",
        &source_columns(),
        "public",
    )
    .await
    .expect("the qualified names are not ambiguous");
    drain_backfill_chunks(&db.pool).await;
    markers::settle_registrations(&db.pool).await;
    markers::run_pending_backfills(
        &mut client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the go-live catch-up");
    drain_to_quiescence(&db.pool, &mut client).await;

    let rows: Vec<(i32, String, String, String)> = client
        .query(
            "select grp, val::text, total::text, biggest::text from amb_totals order by grp",
            &[],
        )
        .await
        .expect("read amb_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    let row = |grp: i32, val: &str, total: &str, biggest: &str| {
        (grp, val.to_string(), total.to_string(), biggest.to_string())
    };
    assert_eq!(rows, vec![row(1, "2", "12", "7"), row(2, "3", "9", "9")]);
}
