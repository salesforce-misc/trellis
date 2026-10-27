//! `live` means steady state (ADR-0002, "What `live` promises").
//!
//! Once a definition reports `live`, a watermark token taken after a commit
//! and awaited with `Trellis::await_converged` guarantees the target reflects
//! every commit at or before the token. Each test here drives one build path
//! and checks exactly that contract against a running engine: wait for
//! `live` through `Trellis::status`, take a token, await it, compare the
//! target with the source. Nothing starts a build by hand; the staging
//! worker does.
//!
//! Each test makes a change commit while the definition is still being
//! built. A Re-derive build applies it while `backfilling`, so it is in the
//! target by `live`.
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

/// The whole engine: capture, the maintenance loop (the only starter of
/// builds) and drain threads.
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
         insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
    )
    .await
    .expect("create and seed sales");
}

/// The Re-derive build of a plain 1-1 target: there is no
/// `catching_up`. The definition applies from its start, so a row inserted
/// while the build's chunk is held before its write is applied while it is
/// still `backfilling`; once the chunk commits it goes straight to `live`,
/// and a token taken after `live` covers every commit before it. The chunk
/// is held by a row trigger on the target, for the rows it read, after its
/// entry lock and its read.
#[tokio::test]
async fn a_one_to_one_rederive_build_applies_while_backfilling_and_is_steady_at_live() {
    const HOLD: i64 = 6254;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id integer primary key, a integer); \
         insert into public.orders values (1, 1), (2, 2)",
    )
    .await
    .expect("create and seed orders");
    define_only(db.dsn())
        .await
        .apply("TRANSFORM order_view FROM orders SELECT a + a AS a2")
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
           for each row when (new.id <= 2) execute function hold_chunk()"
    ))
    .await
    .expect("install the chunk hold");
    raw.execute("select pg_advisory_lock($1)", &[&HOLD])
        .await
        .expect("take the hold");

    let engine = running(db.dsn()).await;
    wait_until_held(&raw, HOLD).await;
    let build: Option<String> = raw
        .query_one(
            "select build from transform_definitions where target_table = 'public.order_view'",
            &[],
        )
        .await
        .expect("read the build")
        .get(0);
    assert_eq!(build.as_deref(), Some("rederive"));

    raw.execute("insert into public.orders values (3, 30)", &[])
        .await
        .expect("commit a change while the chunk is held");
    converge(&engine).await;
    assert_eq!(
        status(&engine, "order_view").await,
        TransformStatus::Backfilling,
        "the chunk hasn't committed"
    );
    let applied: Vec<(i32, i32)> = raw
        .query("select id, a2 from order_view order by id", &[])
        .await
        .expect("read order_view")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        applied,
        vec![(3, 60)],
        "the change was applied while backfilling; the chunk's rows are still owed"
    );

    raw.execute("select pg_advisory_unlock($1)", &[&HOLD])
        .await
        .expect("release the chunk");
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let current = status(&engine, "order_view").await;
        assert_ne!(
            current,
            TransformStatus::CatchingUp,
            "a re-derive build never reports catching_up"
        );
        if current == TransformStatus::Live {
            break;
        }
        assert!(Instant::now() < deadline, "never reported live");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    raw.batch_execute(
        "update public.orders set a = 5 where id = 1; delete from public.orders where id = 2",
    )
    .await
    .expect("commit after live");
    converge(&engine).await;

    let rows: Vec<(i32, i32)> = raw
        .query("select id, a2 from order_view order by id", &[])
        .await
        .expect("read order_view")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        rows,
        vec![(1, 10), (3, 60)],
        "live, and caught up to a token taken after the last change"
    );
    engine.shutdown().await.expect("shut down");
}

/// A build whose source is another definition's target (an aggregate's). That
/// target is never captured: only the target-mutation seam carries its
/// writes, and only to a reader that is already applying. So the chained
/// definition's build waits on a fence, applies from its start, and is `live`
/// once built. At `live`, a change upstream is reflected downstream once its
/// token is.
#[tokio::test]
async fn a_build_on_another_definitions_target_follows_a_change_upstream() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_sales(&raw).await;

    define_only(db.dsn())
        .await
        .apply("TRANSFORM sku_totals FROM sales GROUP BY sku WITHOUT SOURCE INDEX SELECT sum(amount) AS total")
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
