//! Tests of the harness itself (issue #21): that it spins up and tears down
//! cleanly, that isolated databases don't bleed into each other, and that
//! the instance is genuinely configured for logical replication, and that a
//! restart keeps the data while severing every connection (issue #236).

use std::process::{Command, Stdio};
use testkit::{StopMode, TestCluster, fixtures};

#[tokio::test]
async fn harness_starts_runs_a_query_and_tears_down_cleanly() {
    let cluster = TestCluster::start();
    let root = cluster.root().to_path_buf();
    let pid = cluster.server_pid();

    let db = cluster.create_empty_database().await;
    let row = db
        .pool
        .get()
        .await
        .expect("acquire connection")
        .query_one("select 1", &[])
        .await
        .expect("select 1");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);

    drop(db);
    drop(cluster);

    assert!(!root.exists(), "temp dir should be removed on teardown");

    // `kill -0` sends no signal; it just checks whether the process still
    // exists. A non-zero exit means it doesn't (ESRCH), proving the server
    // was actually terminated rather than merely disconnected from.
    let still_alive = Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    assert!(
        !still_alive,
        "postgres process {pid} should have exited on teardown"
    );
}

#[tokio::test]
async fn isolated_databases_do_not_bleed_into_each_other() {
    let cluster = TestCluster::start();
    let db_a = cluster.create_isolated_database().await;
    let db_b = cluster.create_isolated_database().await;

    assert_ne!(db_a.name(), db_b.name(), "each database gets a unique name");

    fixtures::create_source_table(&db_a.pool, "widgets").await;
    fixtures::create_source_table(&db_b.pool, "widgets").await;

    // Same table name, same id, different content, run concurrently: if
    // isolation didn't hold (e.g. both landed in the same database) this
    // would either collide on the primary key or read back the wrong
    // payload.
    let (rows_a, rows_b) = tokio::join!(
        async {
            fixtures::insert_row(&db_a.pool, "widgets", 1, "from-a").await;
            fixtures::read_rows(&db_a.pool, "widgets").await
        },
        async {
            fixtures::insert_row(&db_b.pool, "widgets", 1, "from-b").await;
            fixtures::read_rows(&db_b.pool, "widgets").await
        },
    );

    assert_eq!(rows_a, vec![(1, "from-a".to_string())]);
    assert_eq!(rows_b, vec![(1, "from-b".to_string())]);
}

#[tokio::test]
async fn logical_replication_slot_can_be_created_and_consumed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("acquire connection");

    // Fails outright if wal_level isn't at least `logical`, so this alone
    // proves the harness's server configuration took effect.
    //
    // The hardcoded slot name is safe only because each test owns its own
    // `TestCluster`: replication slots are cluster-wide (not scoped to a
    // database), so two tests sharing one cluster would need distinct
    // names.
    client
        .batch_execute(
            "select pg_create_logical_replication_slot('trellis_test_slot', 'test_decoding')",
        )
        .await
        .expect("create logical replication slot (requires wal_level=logical)");

    client
        .batch_execute("create table logical_probe (id bigint primary key, payload text not null)")
        .await
        .expect("create probe table");
    client
        .execute(
            "insert into logical_probe (id, payload) values (1, 'hello')",
            &[],
        )
        .await
        .expect("insert probe row");

    let changes = client
        .query(
            "select data from pg_logical_slot_get_changes('trellis_test_slot', null, null)",
            &[],
        )
        .await
        .expect("consume logical decoding changes");

    // test_decoding emits one row per WAL event in the transaction (BEGIN,
    // the actual change, COMMIT, ...), so look for the insert among them
    // rather than assuming it's first.
    let decoded: Vec<String> = changes.iter().map(|row| row.get(0)).collect();
    assert!(
        decoded
            .iter()
            .any(|change| change.contains("logical_probe")),
        "expected a decoded change mentioning the table that was written to, got: {decoded:?}"
    );

    client
        .batch_execute("select pg_drop_replication_slot('trellis_test_slot')")
        .await
        .expect("drop replication slot");
}

/// Issue #236: `restart` in either mode brings the same cluster back — a new
/// server process, every existing connection severed, and the data (tables
/// and replication slots alike) intact.
///
/// Multi-threaded so each pooled connection's driver task observes the
/// server closing it while `restart` blocks this thread: a pooled
/// connection whose close nobody has observed yet looks healthy to the
/// pool's recycle check and fails its first query.
#[tokio::test(flavor = "multi_thread")]
async fn restart_severs_connections_and_keeps_data_and_slots() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    fixtures::create_source_table(&db.pool, "widgets").await;
    fixtures::insert_row(&db.pool, "widgets", 1, "before").await;

    let (raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    raw.query_one(
        "select pg_create_logical_replication_slot('restart_slot', 'pgoutput')",
        &[],
    )
    .await
    .expect("create a slot");

    for mode in [StopMode::Fast, StopMode::Immediate] {
        let pid = cluster.server_pid();
        cluster.restart(mode);
        assert_ne!(cluster.server_pid(), pid, "{mode:?}: a new server process");
        fixtures::insert_row(&db.pool, "widgets", 2 + mode as i64, "after").await;
    }

    assert!(
        raw.query_one("select 1", &[]).await.is_err(),
        "a connection opened before the restart must be severed"
    );
    let rows = fixtures::read_rows(&db.pool, "widgets").await;
    assert_eq!(
        rows,
        vec![
            (1, "before".to_string()),
            (2, "after".to_string()),
            (3, "after".to_string()),
        ]
    );
    let slots: i64 = db
        .pool
        .get()
        .await
        .expect("acquire connection")
        .query_one(
            "select count(*) from pg_replication_slots where slot_name = 'restart_slot'",
            &[],
        )
        .await
        .expect("count slots")
        .get(0);
    assert_eq!(slots, 1, "a replication slot survives both restarts");
    db.pool
        .get()
        .await
        .expect("acquire connection")
        .execute("select pg_drop_replication_slot('restart_slot')", &[])
        .await
        .expect("drop the slot");
}
