//! Issue #452: `Trellis::await_converged` on a quiet stream must not wait for
//! intake's next keepalive.
//!
//! The token is `pg_current_wal_lsn()`, so any WAL between the caller's commit
//! and its token read that carries no published change (a write to an
//! unpublished table, the engine's own staging and draining of the write)
//! leaves intake nothing to confirm past the token with. It used to wait for a
//! keepalive-driven persist, throttled to once per 10s, and these waits took
//! ~9s every time. Now the waiter writes a `trellis.converge` logical message
//! that intake confirms through as soon as it decodes it.
//!
//! Each wait gets a 5s budget: ample for a pipeline that converges in well
//! under a second, and short of the ~9s the keepalive path took.

use std::time::Duration;

use testkit::TestCluster;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

const BUDGET: Duration = Duration::from_secs(5);

/// A running staging instance with one live 1-1 transform over `widgets`,
/// plus an unpublished `audit_log` table.
async fn running_instance(db: &testkit::TestDatabase) -> (Trellis, deadpool_postgres::Object) {
    let config = Config::with_schema(db.dsn().to_string(), "trellis").expect("valid schema");
    let pool = trellis::Pool::new(&config).expect("pool");
    trellis::migrate(&pool, &config).await.expect("migrate");
    let conn = pool.get().await.expect("connection");
    conn.batch_execute(
        "create table widgets (id integer primary key, price integer); \
         create table audit_log (id serial primary key, note text)",
    )
    .await
    .expect("create tables");

    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect definer");
    definer
        .apply("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown definer");

    let trellis = Trellis::connect(
        config,
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect staging instance");
    for _ in 0..1_000 {
        let status = trellis.status("widget_prices").await.expect("status");
        if status.is_some_and(|s| s.status == TransformStatus::Live) {
            return (trellis, conn);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("widget_prices never went live");
}

async fn price(conn: &deadpool_postgres::Object, id: i32) -> Option<i32> {
    conn.query_opt(
        "select price from public.widget_prices where id = $1",
        &[&id],
    )
    .await
    .expect("read target")
    .map(|row| row.get(0))
}

#[tokio::test]
async fn an_unpublished_write_before_the_token_does_not_stall_the_wait() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, conn) = running_instance(&db).await;

    for id in 1..=3 {
        conn.execute("insert into widgets values ($1, $1)", &[&id])
            .await
            .expect("insert source row");
        conn.execute(
            "insert into audit_log (note) values ('wrote a widget')",
            &[],
        )
        .await
        .expect("insert audit row");
        let token = trellis.watermark_token().await.expect("watermark_token");
        trellis
            .await_converged(token, BUDGET)
            .await
            .expect("converges without waiting for a keepalive");
        assert_eq!(price(&conn, id).await, Some(id));
    }
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_token_taken_after_the_engine_staged_the_write_does_not_stall_the_wait() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, conn) = running_instance(&db).await;

    for id in 1..=3 {
        conn.execute("insert into widgets values ($1, $1)", &[&id])
            .await
            .expect("insert source row");
        // Long enough for intake to stage (and the engine to start draining)
        // the write, so its own WAL lands before the token.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let token = trellis.watermark_token().await.expect("watermark_token");
        trellis
            .await_converged(token, BUDGET)
            .await
            .expect("converges without waiting for a keepalive");
        assert_eq!(price(&conn, id).await, Some(id));
    }
    trellis.shutdown().await.expect("shutdown");
}
