//! Issue #330: `RESUME TRANSFORM` rebuilds a frozen target by re-enumerating
//! the source's *current* keys (`pending_backfill`'s discharge,
//! `publication::run_pending_backfills`). That enumeration never visits a
//! target row whose source rows all went away while the transform was frozen,
//! so the discharge also deletes every target row no source row backs any
//! more (`intake::resume_orphans`), reporting each deletion through the
//! target-mutation seam.
//!
//! The deletes here are written straight to the source with no intake running,
//! which is exactly the frozen-transform gap: a paused definition's share of
//! the change stream is never folded into its target (and after a slot loss,
//! #310, the gap's changes never reach the ring at all). Every test drives the
//! discharge and the drain by hand, so they wait on nothing. The ordering
//! of the delete against the enumeration's snapshot is pinned by
//! `intake::publication`'s own unit tests, which can hold a discharge
//! mid-flight.
//!
//! The issue as filed says a 1-1 transform already drops such rows. It does
//! not: the 1-1 path has the same gap, for the same reason, so it is pinned
//! here too.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::{ValueType, chunk_queue, install_definition};
use trellis::intake::{publication, slot_loss};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
use trellis::{Config, Trellis, TrellisOptions};

const TEST_NAME: &str = "resume_drops_deleted_keys_test";

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'"
        ))
        .await
        .expect("set search_path");
    client
}

/// A define-only facade connection (no staging worker, no drain threads), for
/// the `PAUSE`/`RESUME` statements an operator would run.
async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

async fn drain_backfill_chunks(pool: &trellis::Pool) {
    // ADR-0016 (#418): registration only records a definition; the backfill
    // discharge dispatches its chunks.
    trellis::intake::publication::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, TEST_NAME, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            // The staging worker's next pass: the builds' go-live catch-ups
            // take them `live` (issue #476).
            trellis::intake::publication::discharge_registrations(pool)
                .await
                .expect("discharge the go-live catch-ups");
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
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, TEST_NAME)
                .await
                .expect("finish_chunk");
        }
    }
}

/// Seals and drains until nothing is pending anywhere in the ring. A bounded
/// deterministic loop, not a wait-for-convergence poll.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = StagedWatermark::saturated();
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
            "trellis_resume_drops_deleted_keys_test",
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

/// Discharges the marker `RESUME` parked, then drains what it staged.
async fn run_resume_rebuild(pool: &trellis::Pool, client: &mut Client) {
    // Consume an xid so the marker's fence, captured inside the resume's own
    // transaction, has settled.
    client
        .batch_execute("select txid_current()")
        .await
        .expect("consume an xid");
    publication::run_pending_backfills(
        client,
        "wake",
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the resume's backfill marker");
    let pending: i64 = client
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("count markers")
        .get(0);
    assert_eq!(pending, 0, "precondition: the resume's marker discharged");
    drain_backfill_chunks(pool).await;
    drain_to_quiescence(pool, client).await;
}

async fn status(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0)
}

fn orders_columns() -> HashMap<String, ValueType> {
    [
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("a", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

/// `orders`: group `g = 0` holds ids 2, 4, 6; group `g = 1` holds 1, 3, 5.
async fn create_orders(client: &Client) {
    client
        .batch_execute(
            "create table orders (id bigint primary key, g bigint, a numeric); \
             alter table orders replica identity full; \
             insert into orders (id, g, a) select s, s % 2, s from generate_series(1, 6) s;",
        )
        .await
        .expect("create + seed orders");
}

/// The issue's repro: pause an aggregate, delete every source row of one
/// group, resume. The rebuilt target must not hold that group's row.
#[tokio::test]
async fn resume_drops_an_aggregate_group_whose_rows_were_all_deleted_while_paused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;

    install_definition(
        &db.pool,
        "TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total",
        &orders_columns(),
        "public",
    )
    .await
    .expect("install the aggregate");
    // Registration only records the aggregate (#419): run its direct-build
    // job, then discharge the catch-up marker going live parks, so the only
    // marker the rebuild below sees is the resume's.
    drain_backfill_chunks(&db.pool).await;
    publication::run_pending_backfills(
        &mut client,
        "wake",
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the build's catch-up marker");
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(status(&client, "order_rollup").await, "live");
    let built: Vec<(i64, String)> = client
        .query(
            "select g::bigint, total::text from order_rollup order by g",
            &[],
        )
        .await
        .expect("read order_rollup")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        built,
        vec![(0, "12".to_string()), (1, "9".to_string())],
        "precondition: group 0 is built before the pause"
    );

    let operator = define_only(db.dsn()).await;
    operator
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause");
    // The gap: every row of group 0 goes away, and group 1 changes too, so the
    // rebuild has something observable to do for the surviving group.
    client
        .batch_execute(
            "delete from orders where g = 0; insert into orders (id, g, a) values (7, 1, 7);",
        )
        .await
        .expect("write to the source while paused");
    operator
        .apply("RESUME TRANSFORM order_rollup")
        .await
        .expect("resume");

    run_resume_rebuild(&db.pool, &mut client).await;

    assert_eq!(status(&client, "order_rollup").await, "live");
    let rows: Vec<(i64, String)> = client
        .query(
            "select g::bigint, total::text from order_rollup order by g",
            &[],
        )
        .await
        .expect("read order_rollup")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rows,
        vec![(1, "16".to_string())],
        "the rebuild re-derived group 1 (1 + 3 + 5 + 7) and dropped group 0, whose \
         every source row was deleted while the transform was paused"
    );
    operator.shutdown().await.expect("shut down");
}

/// The 1-1 twin of the test above. A deleted source row's target row must be
/// gone after the rebuild, just as a live 1-1 transform would delete it.
#[tokio::test]
async fn resume_drops_a_one_to_one_row_whose_source_row_was_deleted_while_paused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;

    install_definition(
        &db.pool,
        "TRANSFORM order_doubles FROM orders SELECT a + a AS x",
        &orders_columns(),
        "public",
    )
    .await
    .expect("install the 1-1");
    drain_backfill_chunks(&db.pool).await;
    // The chunked build's completion parks its own catch-up marker; discharge
    // it now so the only marker the rebuild below sees is the resume's.
    publication::run_pending_backfills(
        &mut client,
        "wake",
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the build's catch-up marker");
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(status(&client, "order_doubles").await, "live");

    let operator = define_only(db.dsn()).await;
    operator
        .apply("PAUSE TRANSFORM order_doubles")
        .await
        .expect("pause");
    client
        .batch_execute(
            "delete from orders where g = 0; insert into orders (id, g, a) values (7, 1, 7);",
        )
        .await
        .expect("write to the source while paused");
    operator
        .apply("RESUME TRANSFORM order_doubles")
        .await
        .expect("resume");

    run_resume_rebuild(&db.pool, &mut client).await;

    assert_eq!(status(&client, "order_doubles").await, "live");
    let rows: Vec<(i64, String)> = client
        .query(
            "select id::bigint, x::text from order_doubles order by id",
            &[],
        )
        .await
        .expect("read order_doubles")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "2".to_string()),
            (3, "6".to_string()),
            (5, "10".to_string()),
            (7, "14".to_string()),
        ],
        "the rebuild picked up id 7 and dropped ids 2, 4 and 6, whose source rows were \
         deleted while the transform was paused"
    );
    operator.shutdown().await.expect("shut down");
}

/// Runs every settled marker, the chunks and the ring until nothing is left
/// to do: a rebuild's own marker, plus the catch-ups a definition going
/// `live` parks for its readers. A bounded loop, not a convergence wait.
async fn settle(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..8 {
        drain_backfill_chunks(pool).await;
        client
            .batch_execute("select txid_current()")
            .await
            .expect("consume an xid");
        publication::run_pending_backfills(
            client,
            "wake",
            &StagedWatermark::saturated(),
            Duration::ZERO,
        )
        .await
        .expect("discharge settled markers");
        drain_to_quiescence(pool, client).await;
        let outstanding: i64 = client
            .query_one(
                "select (select count(*) from pending_backfill) \
                      + (select count(*) from backfill_chunks where not done)",
                &[],
            )
            .await
            .expect("count outstanding work")
            .get(0);
        if outstanding == 0 {
            return;
        }
    }
    panic!("markers and chunks did not settle within 8 rounds");
}

/// Every row of `sql`, each column rendered as text (`None` for SQL `NULL`).
async fn text_rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

fn text(values: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
    values
        .iter()
        .map(|row| row.iter().map(|v| v.map(str::to_string)).collect())
        .collect()
}

async fn apply_all(operator: &Trellis, statements: &[&str]) {
    for statement in statements {
        operator.apply(statement).await.expect(statement);
    }
}

/// Pause, change the source, resume, rebuild: the scenario every test below
/// shares.
async fn pause_write_resume(
    db: &testkit::TestDatabase,
    client: &mut Client,
    operator: &Trellis,
    transforms: &[&str],
    gap: &str,
) {
    for t in transforms {
        operator
            .apply(&format!("PAUSE TRANSFORM {t}"))
            .await
            .expect("pause");
    }
    client.batch_execute(gap).await.expect("write while paused");
    for t in transforms {
        operator
            .apply(&format!("RESUME TRANSFORM {t}"))
            .await
            .expect("resume");
    }
    settle(&db.pool, client).await;
    for t in transforms {
        assert_eq!(status(client, t).await, "live");
    }
}

/// A 1-1 rebuild after every shape of delete a pause can hide: a deleted
/// key, a key deleted and then re-inserted with a new value, and a brand-new
/// key.
#[tokio::test]
async fn resume_rebuilds_a_one_to_one_through_deletes_and_reinserted_keys() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &["TRANSFORM order_doubles FROM orders SELECT a + a AS x"],
    )
    .await;
    settle(&db.pool, &mut client).await;

    pause_write_resume(
        &db,
        &mut client,
        &operator,
        &["order_doubles"],
        "delete from orders where id in (2, 3, 4, 6); \
         insert into orders (id, g, a) values (3, 1, 30), (8, 0, 8);",
    )
    .await;

    assert_eq!(
        text_rows(
            &client,
            "select id::text, x::text from order_doubles order by id"
        )
        .await,
        text(&[
            &[Some("1"), Some("2")],
            &[Some("3"), Some("60")],
            &[Some("5"), Some("10")],
            &[Some("8"), Some("16")],
        ]),
    );
    operator.shutdown().await.expect("shut down");
}

/// The aggregate twin: group 0 goes extinct and is then repopulated under
/// one of its old keys, group 1 loses one row, and a deleted key comes back
/// in a brand-new group 2.
#[tokio::test]
async fn resume_rebuilds_an_aggregate_through_whole_partial_and_reinserted_deletes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &["TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total"],
    )
    .await;
    settle(&db.pool, &mut client).await;

    pause_write_resume(
        &db,
        &mut client,
        &operator,
        &["order_rollup"],
        "delete from orders where g = 0 or id in (3, 5); \
         insert into orders (id, g, a) values (4, 0, 40), (3, 2, 30);",
    )
    .await;

    assert_eq!(
        text_rows(
            &client,
            "select g::text, total::text from order_rollup order by g"
        )
        .await,
        text(&[
            &[Some("0"), Some("40")],
            &[Some("1"), Some("1")],
            &[Some("2"), Some("30")],
        ]),
    );
    operator.shutdown().await.expect("shut down");
}

/// A composite, nullable `GROUP BY`: two groups go extinct, one of them the
/// `(1, NULL)` group, while the `(0, NULL)` group survives with one row fewer.
/// The anti-join handles a `NULL` grouping column in its own statement (see
/// `intake::resume_orphans`' "Keeping the anti-join hashable"), so this covers
/// both the `NULL` and the non-`NULL` pattern.
#[tokio::test]
async fn resume_drops_extinct_composite_and_null_groups() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table lines (id bigint primary key, g bigint, h text, a numeric); \
             alter table lines replica identity full; \
             insert into lines values \
                 (1, 0, null, 1), (2, 0, null, 2), (3, 1, null, 3), \
                 (4, 1, 'x', 4), (5, 2, 'y', 5), (6, 2, 'y', 6);",
        )
        .await
        .expect("seed lines");
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &["TRANSFORM line_rollup FROM lines GROUP BY g, h SELECT sum(a) AS total"],
    )
    .await;
    settle(&db.pool, &mut client).await;

    pause_write_resume(
        &db,
        &mut client,
        &operator,
        &["line_rollup"],
        "delete from lines where id in (1, 3, 5, 6)",
    )
    .await;

    assert_eq!(
        text_rows(
            &client,
            "select g::text, h, total::text from line_rollup order by g, h"
        )
        .await,
        text(&[
            &[Some("0"), None, Some("2")],
            &[Some("1"), Some("x"), Some("4")],
        ]),
        "(1, NULL) and (2, 'y') went extinct while paused; (0, NULL) lost a row"
    );
    operator.shutdown().await.expect("shut down");
}

/// A relationship-path `GROUP BY` key. Three groups go extinct three ways
/// while paused: `eu` loses every order, `apac` loses its only customer to
/// `us` (a to-side change, no order touched), and the `NULL` group (an order
/// whose customer doesn't exist) loses its only order. Only an anti-join
/// that joins the to-side table, as the build does, can see the `apac` case.
#[tokio::test]
async fn resume_drops_groups_a_relationship_key_no_longer_reaches() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table customers (id bigint primary key, region text); \
             alter table customers replica identity full; \
             create table orders (id bigint primary key, customer_id bigint, a numeric); \
             alter table orders replica identity full; \
             insert into customers values (1, 'eu'), (2, 'us'), (3, 'apac'); \
             insert into orders values (1, 1, 1), (2, 1, 2), (3, 2, 3), (4, 3, 4), (5, 99, 5);",
        )
        .await
        .expect("seed customers and orders");
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &[
            "RELATIONSHIP customer FROM orders.customer_id TO customers.id",
            "TRANSFORM region_totals FROM orders GROUP BY customer.region SELECT sum(a) AS total",
        ],
    )
    .await;
    settle(&db.pool, &mut client).await;
    assert_eq!(
        text_rows(
            &client,
            "select region, total::text from region_totals order by region"
        )
        .await,
        text(&[
            &[Some("apac"), Some("4")],
            &[Some("eu"), Some("3")],
            &[Some("us"), Some("3")],
            &[None, Some("5")],
        ]),
        "precondition: the build grouped every order through its customer"
    );

    pause_write_resume(
        &db,
        &mut client,
        &operator,
        &["region_totals"],
        "delete from orders where id in (1, 2, 5); \
         update customers set region = 'us' where id = 3;",
    )
    .await;

    assert_eq!(
        text_rows(
            &client,
            "select region, total::text from region_totals order by region"
        )
        .await,
        text(&[&[Some("us"), Some("7")]]),
    );
    operator.shutdown().await.expect("shut down");
}

/// Two hops: `rollup_echo` aggregates `order_rollup`'s target and stays
/// `live` while `order_rollup` is paused. When the rebuild deletes
/// `order_rollup`'s extinct group 0, `rollup_echo` must drop its group 0 too.
/// Its catch-up enumerates `order_rollup`'s *current* keys, which no longer
/// include 0, and the seam's `Recompute` for the deleted key finds nothing
/// when it re-reads it. Only the prior image that `Recompute` carries says
/// which group the row left.
#[tokio::test]
async fn resume_drops_an_extinct_group_one_hop_down_a_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &["TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total"],
    )
    .await;
    settle(&db.pool, &mut client).await;
    apply_all(
        &operator,
        &[&format!(
            "TRANSFORM rollup_echo FROM {DEFAULT_TARGET_SCHEMA}.order_rollup \
             GROUP BY g SELECT sum(total) AS total"
        )],
    )
    .await;
    settle(&db.pool, &mut client).await;
    let echo = "select g::text, total::text from rollup_echo order by g";
    assert_eq!(
        text_rows(&client, echo).await,
        text(&[&[Some("0"), Some("12")], &[Some("1"), Some("9")]]),
        "precondition: the chain is built"
    );

    pause_write_resume(
        &db,
        &mut client,
        &operator,
        &["order_rollup"],
        "delete from orders where g = 0",
    )
    .await;

    assert_eq!(status(&client, "rollup_echo").await, "live");
    assert_eq!(
        text_rows(&client, echo).await,
        text(&[&[Some("1"), Some("9")]]),
        "the group order_rollup dropped is dropped one hop down too"
    );
    operator.shutdown().await.expect("shut down");
}

/// A sibling on the same source that stayed `live` through the pause is not
/// rebuilt, so the discharge must not touch its target. With no intake
/// running, the sibling never saw the gap's deletes, so its stale rows are
/// still there afterwards: the orphan sweep left them alone. (The
/// rebuild's enumeration still re-derives the sibling's surviving keys.)
#[tokio::test]
async fn resume_leaves_a_live_sibling_on_the_same_source_alone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &[
            "TRANSFORM order_doubles FROM orders SELECT a + a AS x",
            "TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total",
            "TRANSFORM order_copies FROM orders SELECT a AS a",
        ],
    )
    .await;
    settle(&db.pool, &mut client).await;

    pause_write_resume(
        &db,
        &mut client,
        &operator,
        &["order_doubles"],
        "delete from orders where g = 0",
    )
    .await;

    assert_eq!(
        text_rows(&client, "select id::text from order_doubles order by id").await,
        text(&[&[Some("1")], &[Some("3")], &[Some("5")]]),
        "the resumed transform dropped its deleted rows"
    );
    assert_eq!(
        text_rows(&client, "select id::text from order_copies order by id").await,
        text(&[
            &[Some("1")],
            &[Some("2")],
            &[Some("3")],
            &[Some("4")],
            &[Some("5")],
            &[Some("6")],
        ]),
        "the live 1-1 sibling's target is untouched"
    );
    assert_eq!(
        text_rows(
            &client,
            "select g::text, total::text from order_rollup order by g"
        )
        .await,
        text(&[&[Some("0"), Some("12")], &[Some("1"), Some("9")]]),
        "the live aggregate sibling's target is untouched"
    );
    operator.shutdown().await.expect("shut down");
}

/// The discharge gives up when intake hasn't staged through its snapshot
/// (issue #312) and rolls back. The orphan delete is in that transaction,
/// so the target must come out exactly as the pause left it: no rows gone,
/// and nothing staged for a chained reader. The retry then drops them.
///
/// The resumed aggregate's rebuild is a direct-build job, which doesn't wait
/// for intake (#419), so a live 1-1 sibling on the same source is what makes
/// this discharge enumerate (its catch-up) and wait.
#[tokio::test]
async fn a_discharge_that_times_out_waiting_for_intake_leaves_the_target_as_it_was() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &[
            "TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total",
            "TRANSFORM order_doubles FROM orders SELECT a + a AS x",
        ],
    )
    .await;
    settle(&db.pool, &mut client).await;
    apply_all(
        &operator,
        &[&format!(
            "TRANSFORM rollup_echo FROM {DEFAULT_TARGET_SCHEMA}.order_rollup \
             GROUP BY g SELECT sum(total) AS total"
        )],
    )
    .await;
    settle(&db.pool, &mut client).await;
    apply_all(&operator, &["PAUSE TRANSFORM order_rollup"]).await;
    client
        .batch_execute("delete from orders where g = 0")
        .await
        .expect("write while paused");
    apply_all(&operator, &["RESUME TRANSFORM order_rollup"]).await;
    client
        .batch_execute("select txid_current()")
        .await
        .expect("consume an xid");

    // A watermark that never moves, and no time to wait for it.
    publication::run_pending_backfills(
        &mut client,
        "wake",
        &StagedWatermark::new(),
        Duration::ZERO,
    )
    .await
    .expect("a deferred discharge is not an error");

    let rollup = "select g::text, total::text from order_rollup order by g";
    assert_eq!(
        text_rows(&client, rollup).await,
        text(&[&[Some("0"), Some("12")], &[Some("1"), Some("9")]]),
        "the rolled-back discharge deleted nothing"
    );
    assert!(
        !has_pending(&client).await.expect("has_pending"),
        "the rolled-back discharge staged nothing for rollup_echo"
    );
    assert_eq!(status(&client, "order_rollup").await, "waiting_to_backfill");

    settle(&db.pool, &mut client).await;
    assert_eq!(
        text_rows(&client, rollup).await,
        text(&[&[Some("1"), Some("9")]]),
        "the retry drops group 0"
    );
    assert_eq!(
        text_rows(
            &client,
            "select g::text, total::text from rollup_echo order by g"
        )
        .await,
        text(&[&[Some("1"), Some("9")]]),
    );
    operator.shutdown().await.expect("shut down");
}

/// Issue #310's path, the one that motivated #330: a lost replication slot
/// pauses every transform it fed, so every source change in the gap is lost
/// to streaming. The deletes in that gap must still come out of the targets
/// once the operator resumes them.
#[tokio::test]
async fn resume_after_a_slot_loss_drops_rows_deleted_in_the_gap() {
    const LOST: &str = "resume_drops_deleted_keys_lost_slot";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &[
            "TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total",
            "TRANSFORM order_doubles FROM orders SELECT a + a AS x",
        ],
    )
    .await;
    settle(&db.pool, &mut client).await;

    client
        .batch_execute(&format!(
            "create publication lost_pub for table orders; \
             insert into replication_progress (slot_name, confirmed_lsn) values ('{LOST}', '0/10');"
        ))
        .await
        .expect("stage a lost slot");
    let mut session = trellis::staging::session::ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("open a producer session");
    let recovery = slot_loss::pause_if_slot_lost(&mut session, &db.pool, LOST, "lost_pub")
        .await
        .expect("recover from the lost slot")
        .expect("a slot that doesn't exist is lost");
    assert_eq!(recovery.paused, vec!["order_rollup", "order_doubles"]);

    client
        .batch_execute("delete from orders where g = 0 or id = 5")
        .await
        .expect("write in the gap");
    apply_all(
        &operator,
        &[
            "RESUME TRANSFORM order_rollup",
            "RESUME TRANSFORM order_doubles",
        ],
    )
    .await;
    settle(&db.pool, &mut client).await;

    assert_eq!(
        text_rows(
            &client,
            "select g::text, total::text from order_rollup order by g"
        )
        .await,
        text(&[&[Some("1"), Some("4")]]),
    );
    assert_eq!(
        text_rows(
            &client,
            "select id::text, x::text from order_doubles order by id"
        )
        .await,
        text(&[&[Some("1"), Some("2")], &[Some("3"), Some("6")]]),
    );
    drop(session);
    client
        .batch_execute(&format!("select pg_drop_replication_slot('{LOST}')"))
        .await
        .expect("drop the recreated slot");
    operator.shutdown().await.expect("shut down");
}

/// Mixed-case target names (the lexer keeps an identifier's case, and the
/// target is created quoted). The orphan delete must find their keys, not
/// case-fold the name and skip the target as if it were gone.
#[tokio::test]
async fn resume_drops_orphans_from_mixed_case_targets() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &[
            "TRANSFORM OrderDoubles FROM orders SELECT a + a AS x",
            "TRANSFORM OrderRollup FROM orders GROUP BY g SELECT sum(a) AS total",
        ],
    )
    .await;
    settle(&db.pool, &mut client).await;

    pause_write_resume(
        &db,
        &mut client,
        &operator,
        &["OrderDoubles", "OrderRollup"],
        "delete from orders where g = 0",
    )
    .await;

    assert_eq!(
        text_rows(
            &client,
            r#"select id::text from "OrderDoubles" order by id"#
        )
        .await,
        text(&[&[Some("1")], &[Some("3")], &[Some("5")]]),
    );
    assert_eq!(
        text_rows(
            &client,
            r#"select g::text, total::text from "OrderRollup" order by g"#
        )
        .await,
        text(&[&[Some("1"), Some("9")]]),
    );
    operator.shutdown().await.expect("shut down");
}
