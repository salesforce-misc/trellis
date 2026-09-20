//! Integration tests for the staging ring (issue #6, stage 02), run against
//! a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/02-the-staging-ring.md for the design
//! these tests hold the implementation to.

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{CdcOp, StagedChange, StagingError, append};

fn recompute(src_table: &str, key: &str) -> StagedChange {
    StagedChange::Recompute {
        src_table: src_table.to_string(),
        key: key.to_string(),
        hop_gen: 0,
        group_key: None,
        src_changed: None,
    }
}

/// Connects directly to `dsn` (bypassing `trellis::Pool` and its
/// `search_path`-pinning hook), spawns its connection driver, and pins
/// `search_path` itself so the client sees the migrated tables unqualified.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

/// Appends `changes` into `seg_0` on a brand-new connection (bypassing the
/// pool and any `ProducerSession` guard), for tests that need a plain
/// producer without the singleton lock in play.
async fn append_on_new_connection(dsn: &str, changes: &[StagedChange]) {
    let mut client = connect_raw(dsn).await;
    let txn = client.transaction().await.expect("begin");
    append::append(&txn, changes).await.expect("append");
    txn.commit().await.expect("commit");
}

#[tokio::test]
async fn append_takes_no_lock_beyond_its_own_row_insert() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Txn A: append and stay open (don't commit yet).
    let mut client_a = connect_raw(db.dsn()).await;
    let txn_a = client_a.transaction().await.expect("begin A");
    append::append(&txn_a, &[recompute("orders", "1")])
        .await
        .expect("append A");

    // Txn B: append concurrently while A is still open. If append took any
    // lock beyond its own row insert (e.g. locked `segment_pointer`), this
    // would block on A and time out.
    let mut client_b = connect_raw(db.dsn()).await;
    let txn_b = client_b.transaction().await.expect("begin B");
    tokio::time::timeout(
        Duration::from_secs(5),
        append::append(&txn_b, &[recompute("orders", "2")]),
    )
    .await
    .expect("txn B's append must not block on open txn A")
    .expect("append B");
    txn_b.commit().await.expect("commit B");

    // Clean up txn A.
    txn_a.commit().await.expect("commit A");

    let client = db.pool.get().await.expect("acquire connection");
    let count: i64 = client
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0")
        .get(0);
    assert_eq!(count, 2, "both concurrent appends should have landed");
}

#[tokio::test]
async fn route_is_identical_for_client_side_and_server_side_appends() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Client-rendered VALUES tuple (the shape CDC intake, reverse
    // propagation, and definition re-derive use).
    append_on_new_connection(db.dsn(), &[recompute("orders", "same-key")]).await;

    let client = db.pool.get().await.expect("acquire connection");

    // Server-side INSERT ... SELECT (backfill's shape): same src_table/key,
    // computed by Postgres itself rather than rendered by a client.
    client
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen)
             select 'orders', 'same-key', 'recompute', 0",
            &[],
        )
        .await
        .expect("server-side append");

    let routes: Vec<i64> = client
        .query(
            "select route from seg_0 where src_table = 'orders' and key = 'same-key' order by appended_at",
            &[],
        )
        .await
        .expect("query routes")
        .into_iter()
        .map(|row| row.get(0))
        .collect();

    assert_eq!(routes.len(), 2);
    assert_eq!(
        routes[0], routes[1],
        "the same (src_table, key) must route identically regardless of which producer wrote it"
    );
}

#[tokio::test]
async fn all_ring_segments_share_one_route_definition() {
    // The route-equivalence test above exercises only seg_0, but the
    // generated `route` expression is repeated across all four seg_N tables,
    // so a future edit could silently diverge one — reintroducing the "one
    // key, two partitions" hazard. Assert all four share one byte-identical
    // route definition.
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("acquire connection");
    let rows = client
        .query(
            "select generation_expression
               from information_schema.columns
              where table_schema = $1
                and table_name in ('seg_0', 'seg_1', 'seg_2', 'seg_3')
                and column_name = 'route'",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("query route generation expressions");

    assert_eq!(
        rows.len(),
        4,
        "every ring segment must define a generated route column"
    );
    let exprs: std::collections::HashSet<String> =
        rows.iter().map(|row| row.get::<_, String>(0)).collect();
    assert_eq!(
        exprs.len(),
        1,
        "all four ring segments must share one identical route definition, found: {exprs:?}"
    );
}

#[tokio::test]
async fn row_txid_is_the_writers_real_top_level_xid() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mut client = connect_raw(db.dsn()).await;
    let mut txn = client.transaction().await.expect("begin");

    // Insert from inside a SAVEPOINT (a subtransaction) — row_txid must
    // still reflect the *top-level* transaction, not the savepoint.
    let savepoint = txn.transaction().await.expect("begin savepoint");
    append::append(&savepoint, &[recompute("orders", "sp-1")])
        .await
        .expect("append inside savepoint");
    savepoint.commit().await.expect("release savepoint");

    // xid8 has no tokio-postgres FromSql; bridge via ::text like `seal`
    // does.
    let top_level_xid: String = txn
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("read top-level xid")
        .get(0);

    txn.commit().await.expect("commit");

    let client = db.pool.get().await.expect("acquire connection");
    let row_txid: String = client
        .query_one(
            "select row_txid::text from seg_0 where src_table = 'orders' and key = 'sp-1'",
            &[],
        )
        .await
        .expect("query row_txid")
        .get(0);

    assert_eq!(
        row_txid, top_level_xid,
        "row_txid must be the top-level xid even for a row inserted inside a SAVEPOINT"
    );
}

/// The gap issue #31 closes: `lsn`/`row_txid`/`appended_at` are all constant
/// across every row of one source transaction, so nothing but `change_id`
/// can tell an INSERT from a later UPDATE of the same key committed in that
/// transaction. Appends an insert then an update for the same key via a
/// single `append` call (mirroring how intake buffers one transaction and
/// stages it in one shot) and checks `change_id` alone records that order.
/// Fails on the pre-#31 schema, which has no `change_id` column at all.
#[tokio::test]
async fn change_id_records_intra_transaction_append_order() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let insert = StagedChange::Cdc {
        src_table: "orders".to_string(),
        key: "same-key".to_string(),
        op: CdcOp::Insert,
        lsn: None,
        old_image: None,
        new_image: Some(r#"{"id":"same-key","status":"new"}"#.to_string()),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    };
    let update = StagedChange::Cdc {
        src_table: "orders".to_string(),
        key: "same-key".to_string(),
        op: CdcOp::Update,
        lsn: None,
        old_image: Some(r#"{"id":"same-key","status":"new"}"#.to_string()),
        new_image: Some(r#"{"id":"same-key","status":"done"}"#.to_string()),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    };

    let mut client = connect_raw(db.dsn()).await;
    let txn = client.transaction().await.expect("begin");
    // One call, one transaction: exactly how intake stages one source txn's
    // buffered changes (see `trellis::intake::stage_and_advance`).
    append::append(&txn, &[insert, update])
        .await
        .expect("append insert then update in one txn");
    txn.commit().await.expect("commit");

    let client = db.pool.get().await.expect("acquire connection");
    let rows = client
        .query(
            "select op, change_id, lsn, row_txid::text, appended_at
               from seg_0
              where src_table = 'orders' and key = 'same-key'
              order by change_id",
            &[],
        )
        .await
        .expect("query rows for key");
    assert_eq!(rows.len(), 2);

    let ops: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(
        ops,
        vec!["insert", "update"],
        "change_id must sort the insert before the later update, the order they were appended in"
    );

    let change_ids: Vec<i64> = rows.iter().map(|r| r.get(1)).collect();
    assert!(
        change_ids[0] < change_ids[1],
        "change_id must be strictly increasing in append order, got {change_ids:?}"
    );

    // lsn/row_txid/appended_at are constant within one transaction — proving
    // change_id is the *only* thing distinguishing these two rows.
    let lsns: Vec<Option<tokio_postgres::types::PgLsn>> = rows.iter().map(|r| r.get(2)).collect();
    assert_eq!(lsns[0], lsns[1], "lsn must be identical within one txn");
    let row_txids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(3)).collect();
    assert_eq!(
        row_txids[0], row_txids[1],
        "row_txid must be identical within one txn"
    );
    let appended_ats: Vec<std::time::SystemTime> = rows.iter().map(|r| r.get(4)).collect();
    assert_eq!(
        appended_ats[0], appended_ats[1],
        "appended_at must be identical within one txn"
    );
}

#[tokio::test]
async fn append_chunks_batches_past_the_bind_parameter_limit() {
    // 10 columns/row * 6553 rows = 65530 params — one row past 6553 already
    // exceeds Postgres's 65535 Bind-parameter cap if sent as a single
    // unchunked INSERT. Use a batch that spans three chunks (6000 rows
    // each) to prove the chunking loop, not just a single boundary crossing.
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let changes: Vec<StagedChange> = (0..13_000)
        .map(|i| recompute("orders", &i.to_string()))
        .collect();

    append_on_new_connection(db.dsn(), &changes).await;

    let client = db.pool.get().await.expect("acquire connection");
    let count: i64 = client
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0")
        .get(0);
    assert_eq!(
        count, 13_000,
        "all rows across every chunked INSERT must land in one transaction"
    );
}

#[tokio::test]
async fn a_session_with_synchronous_commit_off_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Bake `synchronous_commit=off` into the connection string itself, so
    // it is the session's effective setting from the moment it connects —
    // exactly what the guard is checking.
    let dsn = format!("{} options='-c synchronous_commit=off'", db.dsn());

    let err = trellis::staging::ProducerSession::connect(&dsn, DEFAULT_SCHEMA)
        .await
        .expect_err("a synchronous_commit=off session must be refused");
    match err {
        StagingError::SynchronousCommitOff => {}
        other => panic!("expected SynchronousCommitOff, got {other:?}"),
    }
}

#[tokio::test]
async fn a_second_producer_cannot_acquire_the_singleton_while_the_first_holds_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let first = trellis::staging::ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("first producer session should be granted the singleton");

    let second = trellis::staging::ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA).await;
    match second {
        Err(StagingError::ProducerAlreadyRunning) => {}
        other => panic!(
            "expected the second session to be refused with ProducerAlreadyRunning, got {:?}",
            other.map(|_| "Ok")
        ),
    }

    // Dropping the first session closes its connection, releasing the
    // session-scoped lock instantly (not a TTL lease) — a new producer
    // should start right away.
    drop(first);

    // The release happens when Postgres notices the socket closed; poll
    // briefly rather than assume it's instant from our side.
    let mut acquired = false;
    for _ in 0..50 {
        if trellis::staging::ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
            .await
            .is_ok()
        {
            acquired = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        acquired,
        "a new producer should acquire the singleton after the first disconnects"
    );
}

/// Issue #234, the direct engine-level counterpart of
/// `a_second_producer_cannot_acquire_the_singleton_while_the_first_holds_it`:
/// the singleton is scoped to one **Trellis instance**, not to the database.
///
/// Postgres advisory locks are keyed by `(database, key)` — a session's
/// `search_path` does not enter into the lock tag — so while the key was a
/// single global constant, two instances sharing one database (the topology
/// `docs/instance-identity.md` explicitly promises: "several Trellis
/// instances can coexist in one cluster — even one database — each isolated
/// within its own schema") could never both run a producer. The second one
/// was refused with `ProducerAlreadyRunning`, citing a producer that was not
/// its own. `staging::session::producer_singleton_lock_key` now derives the
/// key from the schema; this is the regression pin.
///
/// The two sessions are both held live simultaneously and only dropped at the
/// end, so this really is "two producers at once", not two in sequence.
#[tokio::test]
async fn two_instances_in_one_database_each_hold_their_own_producer_singleton() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Neither of these is `DEFAULT_SCHEMA`, and neither schema needs to
    // exist: `ProducerSession::connect` only pins `search_path` to it (a
    // `search_path` naming a missing schema is legal in Postgres) and takes
    // the lock. Nothing here appends, so no ring objects are needed — this
    // test is about the guard, not the ring.
    let first = trellis::staging::ProducerSession::connect(db.dsn(), "trellis_instance_one")
        .await
        .expect("the first instance's producer should be granted its own singleton");
    let second = trellis::staging::ProducerSession::connect(db.dsn(), "trellis_instance_two")
        .await
        .expect(
            "a second Trellis instance in the same database, in its own schema, must be able to \
             run its own producer concurrently — the singleton is per instance, not per database",
        );

    // And the guard still bites *within* one instance: a third session in
    // the first instance's own schema is still refused, so scoping the key
    // did not weaken the singleton into a no-op.
    let third = trellis::staging::ProducerSession::connect(db.dsn(), "trellis_instance_one").await;
    match third {
        Err(StagingError::ProducerAlreadyRunning) => {}
        other => panic!(
            "a second producer in the *same* instance schema must still be refused, got {:?}",
            other.map(|_| "Ok")
        ),
    }

    drop(first);
    drop(second);
}
