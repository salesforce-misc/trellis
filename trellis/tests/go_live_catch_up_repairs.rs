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
//!   leaves a 1-1 row, or an aggregate group with no rows left, that the
//!   re-read can't reach.
//!
//! Each test ends with the definition `live` after the discharge, and the
//! target equal to the source.
//!
//! The aggregate build is held with event triggers on advisory locks the
//! test holds: one at the build's first `DROP TABLE` (before the source
//! read) and one at its `ALTER TABLE` (after the read, before the target
//! write). Neither statement has an xid when the trigger fires, so the held
//! build doesn't hold back the seal gate. The chunked 1-1 test drives
//! `claim_chunks` / `run_claimed_chunk` / `finish_chunk` by hand.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::intake::publication;
use trellis::staging::{CdcOp, StagedChange, StagedWatermark, apply, seal};
use trellis::staging::{has_pending, retire_drained_segments};

const TEST_NAME: &str = "go_live_catch_up_repairs_test";
const WAKE: &str = "go_live_catch_up_repairs_wake";
/// Held by the test while the build waits before its read.
const BEFORE_READ: i64 = 4681;
/// Held by the test while the build waits between its read and its write.
const AFTER_READ: i64 = 4682;

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
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        while apply::drain_once(pool, outcome.sealed_seg_seq, TEST_NAME, 1, WAKE, &watermark)
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
    publication::run_pending_backfills(client, WAKE, &StagedWatermark::saturated(), Duration::ZERO)
        .await
        .expect("run_pending_backfills");
    drain_to_quiescence(pool, client).await;
}

/// Parks the build's `DROP TABLE` on [`BEFORE_READ`] and its `ALTER TABLE`
/// on [`AFTER_READ`].
async fn install_build_holds(client: &Client) {
    client
        .batch_execute(&format!(
            "create function hold_direct_build() returns event_trigger \
             language plpgsql as $$ \
             declare held bigint := case tg_tag when 'DROP TABLE' then {BEFORE_READ} \
                                                  else {AFTER_READ} end; \
             begin \
               perform pg_advisory_lock(held); \
               perform pg_advisory_unlock(held); \
             end $$; \
             create event trigger hold_direct_build on ddl_command_start \
               when tag in ('DROP TABLE', 'ALTER TABLE') execute function hold_direct_build()"
        ))
        .await
        .expect("install the build-hold event trigger");
}

async fn wait_for_hold(client: &Client, lock: i64) {
    for _ in 0..500 {
        let held: bool = client
            .query_one(
                "select exists (select 1 from pg_locks \
                 where locktype = 'advisory' and objid = $1 and not granted)",
                &[&(lock as u32)],
            )
            .await
            .expect("read pg_locks")
            .get(0);
        if held {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the direct build never reached hold {lock}");
}

/// Commits `sql` and stages `change` (built from the write's LSN) in one
/// transaction, as intake would stage it.
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
    StagedChange::Cdc {
        src_table: "public.sales".to_string(),
        key: "4".to_string(),
        op,
        lsn: Some(lsn),
        old_image: old.map(str::to_string),
        new_image: new.map(str::to_string),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// Registers `sku_totals` over `public.sales` (`sku = 'a'` totalling `12`)
/// and starts its build, returning once the build is parked before its read.
/// The build task runs the build to completion and leaves the definition
/// `catching_up`, with its go-live catch-up parked for [`discharge_markers`].
async fn start_held_build(pool: &trellis::Pool, client: &Client) -> tokio::task::JoinHandle<()> {
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             alter table public.sales replica identity full; \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
        )
        .await
        .expect("create + seed sales");
    install_build_holds(client).await;
    for lock in [BEFORE_READ, AFTER_READ] {
        client
            .query_one("select pg_advisory_lock($1)", &[&lock])
            .await
            .expect("take a hold lock");
    }
    install_definition(
        pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("sku".to_string(), ValueType::Text),
            ("amount".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the aggregate");
    let pool = pool.clone();
    let build = tokio::spawn(async move { publication::settle_builds(&pool).await });
    wait_for_hold(client, BEFORE_READ).await;
    build
}

async fn release(client: &Client, lock: i64) {
    client
        .query_one("select pg_advisory_unlock($1)", &[&lock])
        .await
        .expect("release a hold");
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

async fn totals(client: &Client) -> Vec<(String, String)> {
    client
        .query("select sku, total::text from sku_totals order by sku", &[])
        .await
        .expect("read sku_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

fn expected(rows: &[(&str, &str)]) -> Vec<(String, String)> {
    rows.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

const ROW_4: &str = r#"{"id":"4","sku":"SKU","amount":"1000"}"#;

fn row_4(sku: &str) -> String {
    ROW_4.replace("SKU", sku)
}

/// #468's probe: the row is inserted after the build started and before its
/// read, the build counts it, and it is deleted once the build has finished.
/// Both CDC changes drain after that, in one batch, where they cancel out,
/// so only the go-live catch-up's re-read of `a` can take the row back out.
/// The source then has the row count and `xmin`s it had before the build,
/// which the coverage skip took for "unchanged".
#[tokio::test]
async fn a_row_inserted_during_the_build_and_deleted_after_it_is_not_left_counted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let build = start_held_build(&db.pool, &client).await;

    let image = row_4("a");
    commit_and_stage(
        &mut client,
        "insert into public.sales values (4, 'a', 1000)",
        |lsn| cdc(CdcOp::Insert, lsn, None, Some(&image)),
    )
    .await;
    release(&client, BEFORE_READ).await;
    release(&client, AFTER_READ).await;
    build.await.expect("build task");
    assert_eq!(
        totals(&client).await,
        expected(&[("a", "1012"), ("b", "2")]),
        "the build read the inserted row"
    );
    assert_eq!(status_of(&client, "public.sku_totals").await, "catching_up");

    commit_and_stage(
        &mut client,
        "delete from public.sales where id = 4",
        |lsn| cdc(CdcOp::Delete, lsn, Some(&image), None),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    assert_eq!(totals(&client).await, expected(&[("a", "12"), ("b", "2")]));
}

/// As above, but the delete lands between the build's read and its write,
/// and both changes drain while the definition is still `backfilling`, so
/// apply skips them.
#[tokio::test]
async fn a_row_inserted_and_deleted_during_the_build_is_not_left_counted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let build = start_held_build(&db.pool, &client).await;

    let image = row_4("a");
    commit_and_stage(
        &mut client,
        "insert into public.sales values (4, 'a', 1000)",
        |lsn| cdc(CdcOp::Insert, lsn, None, Some(&image)),
    )
    .await;
    release(&client, BEFORE_READ).await;
    wait_for_hold(&client, AFTER_READ).await;
    commit_and_stage(
        &mut client,
        "delete from public.sales where id = 4",
        |lsn| cdc(CdcOp::Delete, lsn, Some(&image), None),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    release(&client, AFTER_READ).await;
    build.await.expect("build task");
    assert_eq!(status_of(&client, "public.sku_totals").await, "catching_up");

    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    assert_eq!(totals(&client).await, expected(&[("a", "12"), ("b", "2")]));
}

/// #485: as above, but the row was its group's only one. The go-live
/// re-read has no key left in group `z` to re-derive it from, so only the
/// orphan sweep at the flip removes it.
#[tokio::test]
async fn a_group_emptied_during_the_build_is_gone_at_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let build = start_held_build(&db.pool, &client).await;

    let image = row_4("z");
    commit_and_stage(
        &mut client,
        "insert into public.sales values (4, 'z', 1000)",
        |lsn| cdc(CdcOp::Insert, lsn, None, Some(&image)),
    )
    .await;
    release(&client, BEFORE_READ).await;
    wait_for_hold(&client, AFTER_READ).await;
    commit_and_stage(
        &mut client,
        "delete from public.sales where id = 4",
        |lsn| cdc(CdcOp::Delete, lsn, Some(&image), None),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    release(&client, AFTER_READ).await;
    build.await.expect("build task");
    assert_eq!(
        totals(&client).await,
        expected(&[("a", "12"), ("b", "2"), ("z", "1000")]),
        "the build wrote the group it read"
    );
    assert_eq!(status_of(&client, "public.sku_totals").await, "catching_up");

    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    assert_eq!(totals(&client).await, expected(&[("a", "12"), ("b", "2")]));
}

/// #485 on a plain (chunked) 1-1: a row the chunk read is deleted before the
/// chunk finishes, and its delete drains while the definition is
/// `backfilling`, so apply skips it. The go-live re-read only visits keys
/// the source still has, so only the orphan sweep removes key `4`.
#[tokio::test]
async fn a_one_to_one_row_deleted_during_the_build_is_gone_at_live() {
    use trellis::defs::chunk_queue;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             alter table public.sales replica identity full; \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), \
                                             (4, 'a', 1000)",
        )
        .await
        .expect("create + seed sales");
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
    publication::discharge_registrations(&db.pool)
        .await
        .expect("dispatch");
    let chunks = chunk_queue::claim_chunks(&client, "probe", 1000)
        .await
        .expect("claim");
    assert_eq!(chunks.len(), 1);
    chunk_queue::run_claimed_chunk(&db.pool, &chunks[0], "probe", Duration::from_secs(5))
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
