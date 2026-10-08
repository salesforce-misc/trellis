//! DDL on a user table never makes an application writer wait (issue #621,
//! ADR-0002 I6), and never waits for a lock inside a transaction for longer
//! than its `lock_timeout` (I7).
//!
//! Each attempt runs under a per-attempt `lock_timeout`
//! (`trellis::locks::USER_TABLE_DDL_LOCK_TIMEOUT`) and
//! [`trellis::locks::DdlRetry`] retries it, one attempt per interval, until it
//! lands. The tests hold a lock the DDL needs for a few seconds, write to the
//! table throughout, and check from `pg_locks` that no one DDL attempt
//! blocks the writer for long (`testkit::blocking`, #893), and the DDL's
//! transactions against the timeout, never by waiting for anything to
//! converge (#297).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::locks::{DdlRetry, USER_TABLE_DDL_LOCK_TIMEOUT, set_local_lock_timeout};

/// How long the `CREATE TRIGGER` test holds the lock the DDL needs.
const HOLD: Duration = Duration::from_secs(2);

async fn backend_pid(client: &Client) -> i32 {
    client
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0)
}

async fn connect(dsn: &str) -> Client {
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

/// Inserts into `public.t` one row at a time until `stop`.
fn spawn_writer(client: Client, stop: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut id = 1_000_000i64;
        while !stop.load(Ordering::Relaxed) {
            id += 1;
            client
                .execute("insert into public.t (id) values ($1)", &[&id])
                .await
                .expect("writer insert");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
}

/// The loop #622's `CREATE TRIGGER` runs in, in #565 E7's shape: an open
/// transaction has written to the table, so the trigger's `SHARE ROW
/// EXCLUSIVE` waits for it, and every writer that arrives meanwhile queues
/// behind the waiting DDL. Under the short timeout that queue empties every
/// attempt, so no one attempt blocks a writer for long; unbounded, the one
/// attempt would block every writer for the whole open transaction.
#[tokio::test]
async fn a_user_table_ddl_retry_never_queues_writers_behind_it() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let observer = connect(db.dsn()).await;
    observer
        .batch_execute(
            "create table public.t (id bigint primary key); \
             create function public.noop() returns trigger language plpgsql \
                 as $$ begin return null; end $$",
        )
        .await
        .expect("create table and function");

    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin; insert into public.t (id) values (-1)")
        .await
        .expect("an open write");
    let writer = connect(db.dsn()).await;
    let writer_pid = backend_pid(&writer).await;
    let stop = Arc::new(AtomicBool::new(false));
    let writing = spawn_writer(writer, stop.clone());

    let mut ddl = connect(db.dsn()).await;
    let create = tokio::spawn(async move {
        let mut retry = DdlRetry::new("create trigger", USER_TABLE_DDL_LOCK_TIMEOUT, None);
        loop {
            let lock_timeout = retry.lock_timeout();
            let attempt = async {
                let txn = ddl.transaction().await?;
                set_local_lock_timeout(&txn, lock_timeout).await?;
                txn.batch_execute(
                    "create trigger t_noop after insert on public.t \
                     for each row execute function public.noop()",
                )
                .await?;
                txn.commit().await
            }
            .await;
            match attempt {
                Err(err) if retry.again(&err).await => continue,
                other => return other,
            }
        }
    });

    let watch = testkit::watch_blocked(&observer, writer_pid, HOLD).await;
    eprintln!("while the DDL retried: {watch}");
    assert!(!create.is_finished(), "the DDL waits for the open write");
    // Each attempt gives up after 50 ms; one that waited out the open write
    // would block the writer from the hold's start to its end, and attempts
    // retried back to back would block it most of the time.
    watch.assert_brief_blocks(HOLD, "create trigger");
    stop.store(true, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(10), writing)
        .await
        .expect("a writer is stuck behind the DDL")
        .expect("writer");

    holder.batch_execute("commit").await.expect("release");
    tokio::time::timeout(Duration::from_secs(10), create)
        .await
        .expect("the DDL lands once the write commits")
        .expect("ddl task")
        .expect("create trigger");
    let triggers: i64 = observer
        .query_one(
            "select count(*) from pg_trigger where tgname = 't_noop'",
            &[],
        )
        .await
        .expect("read pg_trigger")
        .get(0);
    assert_eq!(triggers, 1);
}
