//! Issue #486: an insert at or below a recompute horizon and a delete above
//! it, folded into one batch, left a change with neither image. It named no
//! group, so the horizon check never ran and a group the delete emptied kept
//! the value a forced recompute had counted.
//!
//! Here the horizon is a direct aggregate build's: the row is inserted while
//! the build is held just before its source read, so the build's own read
//! picks it up, then deleted after go-live, and both CDC changes drain live
//! in one batch. The go-live catch-up always re-reads its source now
//! (`backfill_coverage` is retired, issues #468/#485), which on its own can't
//! repair the group: enumeration only visits keys the source still has, and
//! by the time it runs the row is gone. (Found while investigating #468; the
//! reproduction comes from that investigation's WIP commit 41d260b.)
//! `apply_aggregate.rs` covers the same fold deterministically, without a
//! build.
//!
//! The build is held with event triggers on advisory locks the test holds:
//! one at the aggregate build's first `DROP TABLE` (before the source read)
//! and one at its `ALTER TABLE` (after the read, before the target write).
//! Neither statement has an xid when the trigger fires, so the held build
//! doesn't hold back the seal gate.

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

const TEST_NAME: &str = "coverage_insert_delete_test";
const WAKE: &str = "coverage_insert_delete_wake";
/// Held by the test while the build waits between its fence and its read.
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
/// and starts its build, returning once the build is parked between its
/// coverage fence and its read.
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
    let build = tokio::spawn(async move { publication::settle_registrations(&pool).await });
    wait_for_hold(client, BEFORE_READ).await;
    build
}

async fn release(client: &Client, lock: i64) {
    client
        .query_one("select pg_advisory_unlock($1)", &[&lock])
        .await
        .expect("release a hold");
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

/// The issue's probe (deleted after go-live) with a group of one row, and the
/// catch-up's always-on enumeration (`backfill_coverage` retired, #468/#485).
/// The insert (below the build's recompute horizon) and the delete (above it)
/// drain live in one batch and fold to a change with neither image, which
/// names no group, so the horizon check never runs and group `z` keeps the
/// build's `1000`.
#[tokio::test]
async fn a_group_emptied_across_the_build_horizon_is_removed_even_by_an_enumerating_catch_up() {
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
    release(&client, AFTER_READ).await;
    build.await.expect("build task");

    commit_and_stage(
        &mut client,
        "delete from public.sales where id = 4",
        |lsn| cdc(CdcOp::Delete, lsn, Some(&image), None),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(totals(&client).await, expected(&[("a", "12"), ("b", "2")]));
}
