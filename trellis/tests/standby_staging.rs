//! A `staging: true` connect that cannot take the producer lock comes up as a
//! standby: a working handle that polls for the lock and takes over when its
//! holder goes away (issue #1470). Run against a real, ephemeral Postgres.

use std::time::Duration;

use testkit::TestCluster;
use trellis::{Config, Trellis, TrellisOptions};

/// The standby polls every 5 seconds; waits give it a few polls.
const TAKEOVER_WAIT: Duration = Duration::from_secs(30);

fn staging() -> TrellisOptions {
    TrellisOptions {
        staging: true,
        ..Default::default()
    }
}

async fn connect(dsn: &str, options: TrellisOptions) -> Trellis {
    Trellis::connect(Config::from_dsn(dsn.to_string()).expect("dsn"), options)
        .await
        .expect("connect")
}

async fn raw_connection(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The backend pids that hold an advisory lock (the producer singleton is the
/// only one held for good), sorted.
async fn lock_holders(raw: &tokio_postgres::Client) -> Vec<i32> {
    raw.query(
        "select distinct pid from pg_locks where locktype = 'advisory' and granted order by pid",
        &[],
    )
    .await
    .expect("read pg_locks")
    .iter()
    .map(|row| row.get(0))
    .collect()
}

/// Polls `holders` until it reads a single backend that is not `not`.
async fn wait_for_new_leader(raw: &tokio_postgres::Client, not: &[i32]) -> i32 {
    let deadline = tokio::time::Instant::now() + TAKEOVER_WAIT;
    loop {
        let holders = lock_holders(raw).await;
        if let [pid] = holders[..]
            && !not.contains(&pid)
        {
            return pid;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no new leader within {TAKEOVER_WAIT:?}; holders: {holders:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The second `staging: true` connect returns a working handle instead of a
/// conflict. It holds no lock and no idle connection of its own for the
/// lock; the first stays the leader.
#[tokio::test]
async fn a_second_staging_connect_stands_by() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw_connection(db.dsn()).await;
    let leader = connect(db.dsn(), staging()).await;
    let [leader_pid] = lock_holders(&raw).await[..] else {
        panic!("the leader holds the singleton");
    };

    let standby = connect(db.dsn(), staging()).await;

    // Its calls run.
    standby
        .status("nothing")
        .await
        .expect("a call on a standby");
    assert!(standby.has_live_staging_worker().await.expect("health"));
    assert_eq!(
        lock_holders(&raw).await,
        [leader_pid],
        "only the leader holds the lock"
    );

    standby.shutdown().await.expect("shutdown the standby");
    assert!(leader.has_live_staging_worker().await.expect("health"));
    leader.shutdown().await.expect("shutdown the leader");
}

/// A clean shutdown of the leader frees the lock at once, and the standby
/// takes it within its poll.
#[tokio::test]
async fn shutting_down_the_leader_hands_over_to_the_standby() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw_connection(db.dsn()).await;
    let leader = connect(db.dsn(), staging()).await;
    let [leader_pid] = lock_holders(&raw).await[..] else {
        panic!("the leader holds the singleton");
    };
    let standby = connect(db.dsn(), staging()).await;

    leader.shutdown().await.expect("shutdown the leader");
    assert!(
        lock_holders(&raw).await.is_empty(),
        "a clean shutdown releases the lock"
    );

    let new_leader = wait_for_new_leader(&raw, &[leader_pid]).await;
    assert_ne!(new_leader, leader_pid);
    assert!(standby.has_live_staging_worker().await.expect("health"));
    standby.shutdown().await.expect("shutdown the new leader");
}

/// A leader whose session is killed (a backend terminated) loses the lock;
/// whichever of the two takes it next, a new session leads again.
#[tokio::test]
async fn a_killed_leader_session_hands_over() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw_connection(db.dsn()).await;
    let a = connect(db.dsn(), staging()).await;
    let [killed] = lock_holders(&raw).await[..] else {
        panic!("the leader holds the singleton");
    };
    let b = connect(db.dsn(), staging()).await;

    raw.execute("select pg_terminate_backend($1)", &[&killed])
        .await
        .expect("terminate the leader's session");

    wait_for_new_leader(&raw, &[killed]).await;
    // Both handles still work: the one that did not take over stands by.
    for handle in [&a, &b] {
        handle.status("nothing").await.expect("a call");
    }
    a.shutdown().await.expect("shutdown a");
    b.shutdown().await.expect("shutdown b");
}

/// Both pools close connections left idle: the sweep's `close_idle` leaves
/// an unused call pool empty.
#[tokio::test]
async fn an_idle_call_pool_closes_its_connections() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let handle = connect(db.dsn(), TrellisOptions::default()).await;
    handle.status("nothing").await.expect("a call");
    // A call's connection goes back once its session reset lands.
    while handle.pool().counts() != (1, 1) {
        tokio::task::yield_now().await;
    }

    handle.pool().close_idle(Duration::from_secs(3600));
    assert_eq!(handle.pool().counts(), (1, 1), "a recent connection stays");
    handle.pool().close_idle(Duration::ZERO);
    assert_eq!(handle.pool().counts(), (0, 0));

    handle.status("nothing").await.expect("a call reconnects");
    handle.shutdown().await.expect("shutdown");
}
