//! Issue #476: `live` means steady state (ADR-0016, "What `live` promises").
//!
//! Once a definition reports `live`, a watermark token taken after a commit
//! and awaited with `Trellis::await_converged` guarantees the target reflects
//! every commit at or before the token. Each test here drives one path that
//! used to report `live` with a go-live catch-up still parked, and checks
//! exactly that contract against a running engine: wait for `live` through
//! `Trellis::status`, take a token, await it, compare the target with the
//! source. Nothing discharges a marker by hand; the staging worker does.
//!
//! Each test makes a change drain while the definition is still being built,
//! so apply skips it and only the go-live catch-up can fold it in. Before
//! #476 the build's completion flipped the definition `live` and only parked
//! that catch-up, so the reads below saw the target without the change.
//!
//! The waits are on the contract's own signals (status, then the token). Each
//! test holds its build at a fixed point with an advisory lock, so the
//! interleaving that matters is set up, not raced.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, TransformStatus, Trellis, TrellisOptions};

/// Budget for each wait on the running engine.
const TIMEOUT: Duration = Duration::from_secs(60);

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

/// A define-only facade connection: no staging worker, no drain threads.
async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

/// The whole engine: intake, the maintenance loop (the only discharger of
/// backfill markers) and drain threads.
async fn running(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("start the engine")
}

async fn status(trellis: &Trellis, target: &str) -> TransformStatus {
    trellis
        .status(target)
        .await
        .expect("read status")
        .expect("the definition is registered")
        .status
}

/// Polls `Trellis::status` until `target` reports `live`, the operator's
/// signal of steady state.
async fn wait_for_live(trellis: &Trellis, target: &str) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let current = status(trellis, target).await;
        if current == TransformStatus::Live {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{target} never reported live (last: {current:?})"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Takes a watermark token now and awaits it: read-your-writes for every
/// commit before this call.
async fn converge(trellis: &Trellis) {
    let token = trellis.watermark_token().await.expect("watermark token");
    trellis
        .await_converged(token, TIMEOUT)
        .await
        .expect("await_converged");
}

/// Blocks any `ALTER TABLE` on advisory lock `key` until the test releases
/// it. A direct build reads each table into temp staging with `CREATE TEMP
/// TABLE ... AS SELECT` and then `ALTER`s it to add its key before writing
/// the target: held there, the build has read the source and written
/// nothing. The `ALTER` is held at `ddl_command_start`, before it has an
/// xid, so the ring keeps sealing and draining around it.
async fn hold_direct_builds(raw: &Client, key: i64) {
    raw.batch_execute(&format!(
        "create function hold_direct_build() returns event_trigger \
         language plpgsql as $$ \
         begin \
           perform pg_advisory_lock({key}); \
           perform pg_advisory_unlock({key}); \
         end $$; \
         create event trigger hold_direct_build on ddl_command_start \
           when tag in ('ALTER TABLE') execute function hold_direct_build()"
    ))
    .await
    .expect("install the build hold");
}

/// Waits until some backend is blocked on advisory lock `key`: the held
/// build (or chunk) has read its source and is parked before its write.
async fn wait_until_held(raw: &Client, key: i64) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let held: bool = raw
            .query_one(
                "select exists (select 1 from pg_locks \
                 where locktype = 'advisory' and objid = $1::bigint::oid and not granted \
                   and database = (select oid from pg_database where datname = current_database()))",
                &[&key],
            )
            .await
            .expect("read pg_locks")
            .get(0);
        if held {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the build never reached its hold"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `public.sales`, seeded with `sku = 'a'` totalling 12.
async fn seed_sales(raw: &Client) {
    raw.batch_execute(
        "create table public.sales (id integer primary key, sku text, amount integer); \
         alter table public.sales replica identity full; \
         insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
    )
    .await
    .expect("create and seed sales");
}

/// The issue's repro, end to end: an aggregate's direct build reads
/// `sales`, a change commits and drains while it is held before its write,
/// then the build finishes. At `live` the change is folded in (`1012`);
/// before #476 the target read `12` there.
#[tokio::test]
async fn an_aggregate_build_is_live_only_with_a_change_drained_during_it_folded_in() {
    const HOLD: i64 = 4761;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_sales(&raw).await;
    hold_direct_builds(&raw, HOLD).await;
    raw.execute("select pg_advisory_lock($1)", &[&HOLD])
        .await
        .expect("take the hold");

    define_only(db.dsn())
        .await
        .apply("TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total")
        .await
        .expect("register the aggregate");
    let engine = running(db.dsn()).await;
    wait_until_held(&raw, HOLD).await;

    raw.execute("insert into public.sales values (4, 'a', 1000)", &[])
        .await
        .expect("commit a change while the build is held");
    converge(&engine).await;
    assert_eq!(
        status(&engine, "sku_totals").await,
        TransformStatus::Backfilling,
        "the change drained while the definition was still building, so apply skipped it"
    );

    raw.execute("select pg_advisory_unlock($1)", &[&HOLD])
        .await
        .expect("release the build");
    wait_for_live(&engine, "sku_totals").await;
    converge(&engine).await;

    let total: String = raw
        .query_one("select total::text from sku_totals where sku = 'a'", &[])
        .await
        .expect("read sku_totals")
        .get(0);
    assert_eq!(
        total, "1012",
        "live, and caught up to a token taken after the change"
    );
    engine.shutdown().await.expect("shut down");
}

/// The chunked path: a plain 1-1 definition's chunk reads `orders` and is
/// held (by a row trigger on the target) before its write, while an update
/// to a row it read commits and drains. The chunk then writes the old value.
/// At `live` the update is there.
#[tokio::test]
async fn a_chunked_build_is_live_only_with_a_change_drained_during_it_folded_in() {
    const HOLD: i64 = 4762;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id integer primary key, a integer); \
         alter table public.orders replica identity full; \
         insert into public.orders values (1, 1), (2, 2)",
    )
    .await
    .expect("create and seed orders");

    define_only(db.dsn())
        .await
        .apply("TRANSFORM order_view FROM orders SELECT a AS a")
        .await
        .expect("register the 1-1 transform");
    raw.batch_execute(&format!(
        "create function hold_chunk() returns trigger language plpgsql as $$ \
         begin \
           perform pg_advisory_lock({HOLD}); \
           perform pg_advisory_unlock({HOLD}); \
           return new; \
         end $$; \
         create trigger hold_chunk before insert on order_view \
           for each row execute function hold_chunk()"
    ))
    .await
    .expect("install the chunk hold");
    raw.execute("select pg_advisory_lock($1)", &[&HOLD])
        .await
        .expect("take the hold");

    let engine = running(db.dsn()).await;
    wait_until_held(&raw, HOLD).await;

    raw.execute("update public.orders set a = 100 where id = 1", &[])
        .await
        .expect("update a row the chunk already read");
    converge(&engine).await;
    assert_eq!(
        status(&engine, "order_view").await,
        TransformStatus::Backfilling,
        "the update drained while the definition was still building, so apply skipped it"
    );

    raw.execute("select pg_advisory_unlock($1)", &[&HOLD])
        .await
        .expect("release the chunk");
    wait_for_live(&engine, "order_view").await;
    converge(&engine).await;

    let rows: Vec<(i32, i32)> = raw
        .query("select id, a from order_view order by id", &[])
        .await
        .expect("read order_view")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        rows,
        vec![(1, 100), (2, 2)],
        "live, and caught up to a token taken after the update"
    );
    engine.shutdown().await.expect("shut down");
}

/// A ring enumeration whose source is another definition's target (an
/// aggregate's, which has no primary key to chunk by). That target is never
/// published: only the target-mutation seam carries its writes, and only to
/// a reader that is already applying. So the chained definition applies from
/// its enumeration on but reports `live` only once the catch-up on its
/// source has run. At `live`, a change upstream is reflected downstream once
/// its token is.
#[tokio::test]
async fn a_ring_build_on_another_definitions_target_is_live_only_once_caught_up() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_sales(&raw).await;

    define_only(db.dsn())
        .await
        .apply("TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total")
        .await
        .expect("register the upstream aggregate");
    let engine = running(db.dsn()).await;
    wait_for_live(&engine, "sku_totals").await;

    engine
        .apply("TRANSFORM sku_view FROM sku_totals SELECT total AS total")
        .await
        .expect("register the chained transform");
    raw.execute("insert into public.sales values (4, 'a', 1000)", &[])
        .await
        .expect("commit a change upstream while the chained transform backfills");
    wait_for_live(&engine, "sku_view").await;
    converge(&engine).await;

    let totals: Vec<(String, String)> = raw
        .query("select sku, total::text from sku_view order by sku", &[])
        .await
        .expect("read sku_view")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        totals,
        vec![
            ("a".to_string(), "1012".to_string()),
            ("b".to_string(), "2".to_string())
        ],
        "live, and caught up to a token taken after the upstream change"
    );
    engine.shutdown().await.expect("shut down");
}
