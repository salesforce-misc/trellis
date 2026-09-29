//! DDL on a user table never makes an application writer wait (issue #621,
//! ADR-0002 I6), and never waits for a lock inside a transaction for longer
//! than its `lock_timeout` (I7).
//!
//! Each attempt runs under `trellis::locks::USER_TABLE_DDL_LOCK_TIMEOUT` and
//! [`trellis::locks::DdlRetry`] retries it, one attempt per interval, until it
//! lands. The tests hold a lock the DDL needs for a few seconds, write to the
//! table throughout, and check the writers' latency and the DDL's
//! transactions against the timeout, never by waiting for anything to
//! converge (#297).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::intake::publication;
use trellis::locks::{DdlRetry, USER_TABLE_DDL_LOCK_TIMEOUT, set_local_lock_timeout};

/// How long each test holds the lock the DDL needs.
const HOLD: Duration = Duration::from_secs(2);

/// Well above `USER_TABLE_DDL_LOCK_TIMEOUT` (50 ms) for a loaded box, and
/// far below `HOLD`, which is what a writer queued behind a DDL waiting out
/// the whole hold would see.
const SLACK: Duration = Duration::from_millis(750);

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

async fn backend_pid(client: &Client) -> i32 {
    client
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0)
}

/// Inserts into `public.t` one row at a time until `stop`, and returns the
/// longest any one insert took.
fn spawn_writer(client: Client, stop: Arc<AtomicBool>) -> tokio::task::JoinHandle<Duration> {
    tokio::spawn(async move {
        let mut longest = Duration::ZERO;
        let mut id = 1_000_000i64;
        while !stop.load(Ordering::Relaxed) {
            id += 1;
            let started = Instant::now();
            client
                .execute("insert into public.t (id) values ($1)", &[&id])
                .await
                .expect("writer insert");
            longest = longest.max(started.elapsed());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        longest
    })
}

/// While `HOLD` runs, the transactions of every backend waiting for a lock
/// except `except`, and the longest any of them had been open.
async fn watch_lock_waiters(observer: &Client, except: &[i32]) -> (usize, f64) {
    let started = Instant::now();
    let mut transactions = std::collections::HashSet::new();
    let mut longest = 0f64;
    while started.elapsed() < HOLD {
        for row in observer
            .query(
                "select pid, xact_start::text, \
                        extract(epoch from clock_timestamp() - xact_start)::float8 \
                 from pg_stat_activity \
                 where wait_event_type = 'Lock' and pid <> all($1) \
                   and datname = current_database()",
                &[&except],
            )
            .await
            .expect("read pg_stat_activity")
        {
            let (pid, xact_start, open): (i32, String, f64) = (row.get(0), row.get(1), row.get(2));
            transactions.insert((pid, xact_start));
            longest = longest.max(open);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (transactions.len(), longest)
}

/// A join (`ALTER PUBLICATION ... ADD TABLE`) while something holds a lock
/// that conflicts with it: `SHARE UPDATE EXCLUSIVE`, as a manual `VACUUM`,
/// an `ANALYZE` or a `CREATE INDEX CONCURRENTLY` does. Writers never wait on
/// it, and each of its transactions ends within its timeout instead of
/// holding a snapshot for the whole hold; it lands once the lock is gone.
#[tokio::test]
async fn a_join_retries_a_locked_table_in_short_transactions() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let observer = connect(db.dsn()).await;
    observer
        .batch_execute("create table public.t (id bigint primary key); create publication test_pub")
        .await
        .expect("create table and publication");

    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin; lock table public.t in share update exclusive mode")
        .await
        .expect("hold the table");
    let writer = connect(db.dsn()).await;
    let except = vec![backend_pid(&holder).await, backend_pid(&writer).await];
    let stop = Arc::new(AtomicBool::new(false));
    let writing = spawn_writer(writer, stop.clone());

    let mut joiner = connect(db.dsn()).await;
    let join = tokio::spawn(async move {
        publication::reconcile_publication(&mut joiner, "test_pub", &["public.t".to_string()]).await
    });

    let (transactions, longest) = watch_lock_waiters(&observer, &except).await;
    assert!(
        !join.is_finished(),
        "the join waits while the table is held"
    );
    stop.store(true, Ordering::Relaxed);
    let writer_longest = writing.await.expect("writer");

    assert!(
        transactions >= 2,
        "the join must give up and retry in a fresh transaction, saw {transactions}"
    );
    assert!(
        longest < (USER_TABLE_DDL_LOCK_TIMEOUT + SLACK).as_secs_f64(),
        "a join transaction waited {longest:.3}s for its lock"
    );
    assert!(
        writer_longest < USER_TABLE_DDL_LOCK_TIMEOUT + SLACK,
        "a writer waited {writer_longest:?}"
    );

    holder.batch_execute("commit").await.expect("release");
    tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("the join lands once the table is free")
        .expect("join task")
        .expect("join");
    let published: i64 = observer
        .query_one(
            "select count(*) from pg_publication_tables \
             where pubname = 'test_pub' and schemaname = 'public' and tablename = 't'",
            &[],
        )
        .await
        .expect("read publication")
        .get(0);
    assert_eq!(published, 1);
}

/// The loop #622's `CREATE TRIGGER` runs in, in #565 E7's shape: an open
/// transaction has written to the table, so the trigger's `SHARE ROW
/// EXCLUSIVE` waits for it, and every writer that arrives meanwhile queues
/// behind the waiting DDL. Under the short timeout that queue empties every
/// attempt, so no writer waits much longer than the timeout; unbounded, each
/// would wait out the whole open transaction.
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
    let stop = Arc::new(AtomicBool::new(false));
    let writing = spawn_writer(writer, stop.clone());

    let mut ddl = connect(db.dsn()).await;
    let create = tokio::spawn(async move {
        let mut retry = DdlRetry::new("create trigger", None);
        loop {
            let attempt = async {
                let txn = ddl.transaction().await?;
                set_local_lock_timeout(&txn, USER_TABLE_DDL_LOCK_TIMEOUT).await?;
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

    tokio::time::sleep(HOLD).await;
    assert!(!create.is_finished(), "the DDL waits for the open write");
    stop.store(true, Ordering::Relaxed);
    // A writer queued behind a DDL that waits out the open write never
    // finishes its insert while the write stays open.
    let writer_longest = tokio::time::timeout(HOLD, writing)
        .await
        .expect("a writer is stuck behind the DDL")
        .expect("writer");
    assert!(
        writer_longest < USER_TABLE_DDL_LOCK_TIMEOUT + SLACK,
        "a writer queued behind the DDL for {writer_longest:?}"
    );

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
