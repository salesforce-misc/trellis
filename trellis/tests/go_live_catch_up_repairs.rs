//! Issues #468 and #485: a build's go-live catch-up repairs whatever the
//! definition missed while it was being built, including deletes.
//!
//! A definition doesn't apply CDC until its build finishes, so a change that
//! drains while it is `backfilling` is skipped, and only the go-live
//! catch-up can fold it in. Under #476 a finished build is `catching_up`
//! until the discharge of its last catch-up flips it `live`, in the
//! transaction that re-reads its tables. That discharge now always
//! re-reads (the coverage skip that let it vouch for an unchanged table is
//! gone, #468), and it deletes every target row no source row backs any more
//! (#485), since a per-key re-read never visits a key that is gone.
//!
//! - #468: a row inserted after the build's start, read by the build and
//!   deleted again leaves the source looking exactly as it did before the
//!   build: same row count, no row with a fresh `xmin`. The coverage skip
//!   took that for "unchanged" and kept the deleted row counted.
//! - #485: a delete that drained while the definition was `backfilling`
//!   leaves a 1-1 row that the re-read can't reach.
//!
//! The test ends with the definition `live` after the discharge, and the
//! target equal to the source. It drives `claim_chunks` / `run_claimed_chunk`
//! / `finish_chunk` by hand, over a seam-fed source: over a captured one a
//! plain 1-1 is the Re-derive build's since #625 F8a.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::intake::markers;
use trellis::staging::{CdcOp, StagedChange, StagedWatermark, apply, seal};
use trellis::staging::{has_pending, retire_drained_segments};

const TEST_NAME: &str = "go_live_catch_up_repairs_test";
const WAKE: &str = "go_live_catch_up_repairs_wake";

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

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        while apply::drain_once(pool, outcome.sealed_seg_seq, TEST_NAME, 1, WAKE)
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

async fn discharge_markers(pool: &trellis::Pool, client: &mut Client) {
    markers::run_pending_backfills(client, WAKE, &StagedWatermark::saturated(), Duration::ZERO)
        .await
        .expect("run_pending_backfills");
    drain_to_quiescence(pool, client).await;
}

/// Commits `sql` and stages `change` (built from the write's LSN) in one
/// transaction, as capture would stage it.
async fn commit_and_stage(client: &mut Client, sql: &str, change: impl Fn(PgLsn) -> StagedChange) {
    let txn = client.transaction().await.expect("begin source write");
    txn.batch_execute(sql).await.expect("source write");
    let lsn: PgLsn = txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("read the write's lsn")
        .get(0);
    trellis::staging::append(&txn, &[change(lsn)])
        .await
        .expect("stage the change's CDC row");
    txn.commit().await.expect("commit source write");
}

fn cdc(op: CdcOp, lsn: PgLsn, old: Option<&str>, new: Option<&str>) -> StagedChange {
    cdc_for("4", op, lsn, old, new)
}

fn cdc_for(key: &str, op: CdcOp, lsn: PgLsn, old: Option<&str>, new: Option<&str>) -> StagedChange {
    StagedChange::Cdc {
        src_table: "public.sales".to_string(),
        key: key.to_string(),
        op,
        lsn: Some(lsn),
        old_image: old.map(str::to_string),
        new_image: new.map(str::to_string),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
    }
}

async fn status_of(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("read the definition's status")
        .get(0)
}

const ROW_4: &str = r#"{"id":"4","sku":"SKU","amount":"1000"}"#;

fn row_4(sku: &str) -> String {
    ROW_4.replace("SKU", sku)
}

/// #485 on a plain (chunked) 1-1: a row the chunk read is deleted before the
/// chunk finishes, and its delete drains while the definition is
/// `backfilling`, so apply skips it. The go-live re-read only visits keys
/// the source still has, so only the orphan sweep removes key `4`.
///
/// `sales` is made to read as another definition's target
/// ([`markers::feed_from_a_test_definition`]; no staging worker runs here):
/// #625 F8a gives a plain 1-1 over a captured table to the Re-derive build,
/// which applies from its start and has no catch-up, and the chunked build
/// and go-live catch-up this pins survive for a seam-fed source.
#[tokio::test]
async fn a_one_to_one_row_deleted_during_the_build_is_gone_at_live() {
    use trellis::defs::chunk_queue;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), \
                                             (4, 'a', 1000)",
        )
        .await
        .expect("create + seed sales");
    markers::feed_from_a_test_definition(&client, "public.sales")
        .await
        .expect("make sales another definition's target");
    install_definition(
        &db.pool,
        "TRANSFORM sales_copy FROM sales SELECT amount AS amt",
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("sku".to_string(), ValueType::Text),
            ("amount".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the 1-1");
    markers::discharge_registrations(&db.pool)
        .await
        .expect("dispatch");
    let chunks = chunk_queue::claim_chunks(&client, "probe", 1000)
        .await
        .expect("claim");
    assert_eq!(chunks.len(), 1);
    chunk_queue::run_claimed_chunk(
        &db.pool,
        &chunks[0],
        "probe",
        Duration::from_secs(5),
        Duration::from_secs(60),
    )
    .await
    .expect("run chunk");

    let image = row_4("a");
    commit_and_stage(
        &mut client,
        "delete from public.sales where id = 4",
        |lsn| cdc(CdcOp::Delete, lsn, Some(&image), None),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    chunk_queue::finish_chunk(&db.pool, &chunks[0], "probe")
        .await
        .expect("finish chunk");
    assert_eq!(status_of(&client, "public.sales_copy").await, "catching_up");
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sales_copy").await, "live");
    let ids: Vec<i32> = client
        .query("select id from sales_copy order by id", &[])
        .await
        .expect("read sales_copy")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);
}
