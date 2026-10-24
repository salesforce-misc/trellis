//! Issue #330, #888: `RESUME TRANSFORM` rebuilds a frozen target with a
//! Re-derive build (`staging::build`), whose sweep deletes every target row
//! no source row backs any more and tombstones its ledger entry
//! (`staging::build::one_to_one::sweep_batch`), reporting each deletion
//! through the target-mutation seam. The build's chunks only reach keys the
//! source still has, so the sweep is what removes the rest.
//!
//! The deletes here are written straight to the source with no capture
//! installed, which is exactly the frozen-transform gap: a paused definition's
//! share of the change stream is never folded into its target. Every test
//! drives the build and the drain by hand, so they wait on nothing.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::{ValueType, install_definition};
use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
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
    // ADR-0002 (#418): registration only records a definition; the staging
    // worker's pass starts its build.
    trellis::intake::harness::start_registered_builds(pool)
        .await
        .expect("dispatch registered definitions' builds");
    trellis::staging::build::settle_builds(pool).await;
}

/// Seals and drains until nothing is pending anywhere in the ring. A bounded
/// deterministic loop, not a wait-for-convergence poll.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
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

/// Builds what `RESUME` left `waiting_to_backfill`, then drains what it staged.
async fn run_resume_rebuild(pool: &trellis::Pool, client: &mut Client) {
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
        ("g", ValueType::Integer(trellis::integer::IntWidth::Int8)),
        ("a", ValueType::Integer(trellis::integer::IntWidth::Int4)),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

/// `orders`: group `g = 0` holds ids 2, 4, 6; group `g = 1` holds 1, 3, 5.
async fn create_orders(client: &Client) {
    client
        .batch_execute(
            "create table orders (id bigint primary key, g bigint, a integer); \
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
        "TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total",
        &orders_columns(),
        "public",
    )
    .await
    .expect("install the aggregate");
    // Registration only records the aggregate (#419): run its build.
    drain_backfill_chunks(&db.pool).await;
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
    // #888: the rebuild's sweep tombstones the ledger entries of the keys it
    // deleted, so `retire::collect_tombstones` can collect them. A live entry
    // with no row behind it would leak.
    let entries: Vec<(String, bool)> = client
        .query(
            "select __from_key, __tombstone from public.order_doubles__ledger \
             order by __from_key::bigint",
            &[],
        )
        .await
        .expect("read order_doubles' ledger")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        entries,
        vec![
            ("1".to_string(), false),
            ("2".to_string(), true),
            ("3".to_string(), false),
            ("4".to_string(), true),
            ("5".to_string(), false),
            ("6".to_string(), true),
            ("7".to_string(), false),
        ],
        "a key deleted at the source while the definition was paused is tombstoned"
    );
    operator.shutdown().await.expect("shut down");
}

/// Runs the builds, their chunks and the ring until nothing is left to do: a
/// rebuild, plus the rebuilds of the readers a build rewrote outside the
/// seam. A bounded loop, not a convergence wait.
async fn settle(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..8 {
        drain_backfill_chunks(pool).await;
        drain_to_quiescence(pool, client).await;
        let outstanding: i64 = client
            .query_one("select count(*) from backfill_chunks where not done", &[])
            .await
            .expect("count outstanding work")
            .get(0);
        if outstanding == 0 {
            return;
        }
    }
    panic!("builds and chunks did not settle within 8 rounds");
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
        &["TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total"],
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
/// The aggregate's rebuild (`staging::group_basis::rebuild`) must match a
/// `NULL` grouping column as well as a non-`NULL` one, so this covers both
/// patterns.
#[tokio::test]
async fn resume_drops_extinct_composite_and_null_groups() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table lines (id bigint primary key, g bigint, h text, a integer); \
             insert into lines values \
                 (1, 0, null, 1), (2, 0, null, 2), (3, 1, null, 3), \
                 (4, 1, 'x', 4), (5, 2, 'y', 5), (6, 2, 'y', 6);",
        )
        .await
        .expect("seed lines");
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &["TRANSFORM line_rollup FROM lines GROUP BY g, h WITHOUT SOURCE INDEX SELECT sum(a) AS total"],
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

/// Two hops: `rollup_echo` aggregates `order_rollup`'s target and stays
/// `live` while `order_rollup` is paused. When the rebuild deletes
/// `order_rollup`'s extinct group 0, `rollup_echo` must drop its group 0 too.
/// The seam's `Recompute` for the deleted key finds nothing when it re-reads
/// it, and the build that completes the resume rebuilds `rollup_echo` from
/// `order_rollup`'s current rows, which no longer include 0.
#[tokio::test]
async fn resume_drops_an_extinct_group_one_hop_down_a_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;
    let operator = define_only(db.dsn()).await;
    apply_all(
        &operator,
        &["TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total"],
    )
    .await;
    settle(&db.pool, &mut client).await;
    apply_all(
        &operator,
        &[&format!(
            "TRANSFORM rollup_echo FROM {DEFAULT_TARGET_SCHEMA}.order_rollup \
             GROUP BY g WITHOUT SOURCE INDEX SELECT sum(total) AS total"
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
/// rebuilt, so the resume must not touch its target. With no intake
/// running, the sibling never saw the gap's deletes, so its stale rows are
/// still there afterwards: the resumed definition's sweep left them alone.
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
            "TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total",
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

/// Mixed-case target names (the lexer keeps an identifier's case, and the
/// target is created quoted). The sweep must find their keys, not
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
            "TRANSFORM OrderRollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total",
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
