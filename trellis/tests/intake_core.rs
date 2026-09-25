//! The intake durability battery (issue #7, stage 01): the linchpin's
//! kill-9 safety, watermark monotonicity, NOTIFY-after-visible, the
//! REPLICA IDENTITY FULL rejection, and one end-to-end happy path.
//!
//! ## The crash-test seam
//!
//! The three crash points are tested directly against the linchpin
//! (`trellis::intake::stage_and_advance`) rather than through a live stream:
//!
//! - Crash point 1 (before the stage commit): drop the `Transaction` before
//!   committing. Postgres aborts an in-flight transaction identically
//!   whether the client disconnects or the process is SIGKILLed — either way
//!   the socket just closes — so this is a faithful in-process surrogate.
//! - Crash point 2 (between commit and acknowledgment): simply don't perform
//!   the acknowledgment, then re-run the same linchpin call to simulate the
//!   walsender's replay (the slot's confirmed position never moved). There
//!   is nothing to crash mid-ack — advancing the position and sending the
//!   Standby Status Update are unfailable once the commit returned.
//! - Crash point 3 (stage transaction fails): force the transaction to error
//!   before it can commit.

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::create_relationship;
use trellis::intake::{self, IntakeError, replica_identity};
use trellis::staging::{CdcOp, StagedChange, StagedWatermark, TRUNCATE_SENTINEL_KEY};

/// Connects directly to `dsn` (bypassing `trellis::Pool`) and pins
/// `search_path`, matching `trellis/tests/staging_ring.rs`'s helper of the
/// same name — duplicated rather than shared, since integration test files
/// are separate crates and this is a few lines.
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

/// Seeds a `replication_progress` row for `slot`, standing in for issue #8's
/// not-yet-built slot-lifecycle setup. A missing row reads as "not
/// converged", so tests that exercise the monotonic guard need a starting
/// point to guard from.
async fn seed_progress(client: &Client, slot: &str, confirmed_lsn: u64) {
    client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
            &[&slot, &PgLsn::from(confirmed_lsn)],
        )
        .await
        .expect("seed replication_progress");
}

/// Seeds `slot`'s `replication_progress` row at the slot's own starting
/// position, as `create_slot_and_park_markers` does. A row behind the slot
/// would read as a slot recreated past this instance's confirmed position
/// (issue #406) and fail `Intake::connect`.
async fn seed_progress_at_slot(client: &Client, slot: &str) {
    let seeded = client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) \
             select slot_name, confirmed_flush_lsn from pg_replication_slots \
             where slot_name = $1 and database = current_database()",
            &[&slot],
        )
        .await
        .expect("seed replication_progress at the slot's start");
    assert_eq!(
        seeded, 1,
        "slot {slot} must exist before seeding its progress"
    );
}

async fn confirmed_lsn(client: &Client, slot: &str) -> Option<u64> {
    client
        .query_opt(
            "select confirmed_lsn from replication_progress where slot_name = $1",
            &[&slot],
        )
        .await
        .expect("query replication_progress")
        .map(|row| {
            let lsn: PgLsn = row.get(0);
            u64::from(lsn)
        })
}

async fn seg_0_count(client: &Client) -> i64 {
    client
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0")
        .get(0)
}

fn insert_change(table: &str, key: &str) -> StagedChange {
    StagedChange::Cdc {
        src_table: table.to_string(),
        key: key.to_string(),
        op: CdcOp::Insert,
        lsn: None,
        old_image: None,
        new_image: Some(r#"{"id":"1"}"#.to_string()),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

#[tokio::test]
async fn crash_before_stage_commit_loses_nothing_and_leaves_the_watermark_untouched() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_progress(&client, "slot1", 100).await;

    {
        let txn = client.transaction().await.expect("begin");
        intake::stage_and_advance(
            &txn,
            &[insert_change("orders", "1")],
            "slot1",
            "wake",
            PgLsn::from(200),
        )
        .await
        .expect("stage_and_advance runs its statements");
        // "kill -9 before the stage commit": drop the transaction without
        // committing (see the crash-test seam in the module doc).
    }

    assert_eq!(seg_0_count(&client).await, 0, "nothing should be staged");
    assert_eq!(
        confirmed_lsn(&client, "slot1").await,
        Some(100),
        "the watermark must be exactly what it was before the aborted attempt"
    );
}

#[tokio::test]
async fn crash_between_stage_commit_and_ack_re_stages_duplicates_but_never_regresses_the_watermark()
{
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_progress(&client, "slot1", 100).await;

    let changes = [insert_change("orders", "1")];

    // First attempt: the stage commits durably...
    {
        let mut client = connect_raw(db.dsn()).await;
        let txn = client.transaction().await.expect("begin");
        intake::stage_and_advance(&txn, &changes, "slot1", "wake", PgLsn::from(200))
            .await
            .expect("first stage_and_advance");
        txn.commit().await.expect("commit");
        // ...then the process is imagined to die here, before the
        // acknowledgment. Nothing to crash: the test simply never performs
        // those remaining in-memory steps.
    }

    assert_eq!(seg_0_count(&client).await, 1);
    assert_eq!(confirmed_lsn(&client, "slot1").await, Some(200));

    // The server never saw an acknowledgment for this transaction (the
    // slot's own confirmed position never moved), so on reconnect it
    // replays the same transaction from scratch — re-staging it.
    {
        let mut client = connect_raw(db.dsn()).await;
        let txn = client.transaction().await.expect("begin");
        intake::stage_and_advance(&txn, &changes, "slot1", "wake", PgLsn::from(200))
            .await
            .expect("replayed stage_and_advance");
        txn.commit().await.expect("commit");
    }

    assert_eq!(
        seg_0_count(&client).await,
        2,
        "the replay must re-stage a duplicate row — this is the one \
         at-least-once seam the design accepts; the fold (stage 04) is what \
         collapses it, not this stage"
    );
    assert_eq!(
        confirmed_lsn(&client, "slot1").await,
        Some(200),
        "the watermark must not move on replay of an already-confirmed end_lsn"
    );
}

#[tokio::test]
async fn stage_transaction_failure_rolls_back_and_leaves_the_watermark_untouched() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_progress(&client, "slot1", 100).await;

    let txn = client.transaction().await.expect("begin");
    // Force the stage to fail: corrupt `segment_pointer` to name an
    // out-of-range ring slot in this transaction, so `append::append`'s slot
    // resolution errors out. The corruption dies with the rollback.
    txn.execute("update segment_pointer set ring_slot = 9", &[])
        .await
        .expect("corrupt the pointer for this transaction only");

    let result = intake::stage_and_advance(
        &txn,
        &[insert_change("orders", "1")],
        "slot1",
        "wake",
        PgLsn::from(200),
    )
    .await;
    assert!(result.is_err(), "an invalid ring slot must fail the stage");
    txn.rollback().await.expect("rollback");

    assert_eq!(seg_0_count(&client).await, 0, "nothing should be staged");
    assert_eq!(
        confirmed_lsn(&client, "slot1").await,
        Some(100),
        "a failed stage transaction must leave the watermark untouched"
    );
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("query pointer")
        .get(0);
    assert_eq!(
        ring_slot, 0,
        "the corruption must not have survived the rollback"
    );
}

#[tokio::test]
async fn watermark_advance_is_monotonic_under_the_sql_guard() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_progress(&client, "slot1", 100).await;

    async fn advance(client: &mut Client, end_lsn: u64) {
        let txn = client.transaction().await.expect("begin");
        intake::stage_and_advance(&txn, &[], "slot1", "wake", PgLsn::from(end_lsn))
            .await
            .expect("stage_and_advance");
        txn.commit().await.expect("commit");
    }

    // A lower end_lsn than what's already confirmed: no-op.
    advance(&mut client, 50).await;
    assert_eq!(confirmed_lsn(&client, "slot1").await, Some(100));

    // Exactly equal: still a no-op — the `<` guard means equal doesn't
    // re-trigger a write.
    advance(&mut client, 100).await;
    assert_eq!(confirmed_lsn(&client, "slot1").await, Some(100));

    // Strictly greater: advances.
    advance(&mut client, 150).await;
    assert_eq!(confirmed_lsn(&client, "slot1").await, Some(150));
}

#[tokio::test]
async fn notify_is_only_delivered_after_staged_rows_are_visible() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // A dedicated LISTEN connection, driven on its own task so its
    // `Connection` future keeps polling for asynchronous notifications
    // rather than being spawned-and-ignored like `connect_raw`.
    let (listener, mut connection) = tokio_postgres::connect(db.dsn(), NoTls)
        .await
        .expect("connect listener");

    // Drive `connection` *before* issuing anything on `listener`: nothing is
    // flushed or read back until something polls `connection`, so issuing
    // `batch_execute` first would hang forever.
    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(_))) => {
                    let _ = notify_tx.send(());
                }
                Some(Ok(_)) => continue,
                _ => break,
            }
        }
    });

    listener
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'; listen wake"
        ))
        .await
        .expect("listen");

    let mut producer = connect_raw(db.dsn()).await;
    seed_progress(&producer, "slot1", 100).await;
    let txn = producer.transaction().await.expect("begin");
    intake::stage_and_advance(
        &txn,
        &[insert_change("orders", "1")],
        "slot1",
        "wake",
        PgLsn::from(200),
    )
    .await
    .expect("stage_and_advance");
    txn.commit().await.expect("commit");

    tokio::time::timeout(std::time::Duration::from_secs(5), notify_rx.recv())
        .await
        .expect("must receive the notification")
        .expect("channel must not have closed");

    // By the time the listener sees the notification, the staged row is
    // already visible on a separate connection: `pg_notify` is transactional
    // and committed with the `INSERT`, so no listener wakes to absent work.
    let observer = connect_raw(db.dsn()).await;
    assert_eq!(seg_0_count(&observer).await, 1);
}

#[tokio::test]
async fn a_definition_needing_the_old_image_is_rejected_with_the_exact_ddl() {
    let err = replica_identity::require_replica_identity_full("line_items", true)
        .expect_err("must be rejected");
    match err {
        IntakeError::ReplicaIdentityRequired { table, statement } => {
            assert_eq!(table, "line_items");
            assert_eq!(statement, "ALTER TABLE line_items REPLICA IDENTITY FULL;");
        }
        other => panic!("expected ReplicaIdentityRequired, got {other:?}"),
    }
}

/// End-to-end happy path: a real logical replication slot and publication
/// on an ephemeral cluster, a genuine change to a source table, `Intake`
/// consuming the live stream, and both halves of the guarantee observed
/// afterward — the change staged into the active ring segment, and
/// `replication_progress.confirmed_lsn` advanced to the commit's `end_lsn`.
#[tokio::test]
async fn end_to_end_happy_path_stages_a_change_and_advances_the_watermark() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key, payload text not null);
             create publication intake_pub for table widgets;",
        )
        .await
        .expect("create source table and publication");
    let slot_row = setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    let _: String = slot_row.get(0);
    seed_progress_at_slot(&setup, "intake_slot").await;

    // The change intake should pick up, made *after* the slot exists so it
    // is guaranteed to be in the stream.
    setup
        .execute("insert into widgets (id, payload) values (1, 'hello')", &[])
        .await
        .expect("insert source row");

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "intake_slot".to_string(),
        publication: "intake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: intake::spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");

    // Run the consumer loop in the background; poll a separate observer
    // connection until the transaction lands, or time out. The task is
    // abandoned when the test returns — fine for observing one transaction.
    tokio::spawn(async move {
        let _ = consumer.run().await;
    });

    let observer = connect_raw(db.dsn()).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while seg_0_count(&observer).await < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the change to be staged"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let staged = observer
        .query_one("select src_table, key, op from seg_0", &[])
        .await
        .expect("query staged row");
    let src_table: String = staged.get(0);
    let key: String = staged.get(1);
    let op: String = staged.get(2);
    // Unqualified DDL lands in the first schema on `search_path`, so the
    // namespace is `trellis`, not `public`.
    assert_eq!(src_table, format!("{DEFAULT_SCHEMA}.widgets"));
    assert_eq!(key, "1");
    assert_eq!(op, "insert");

    let watermark = confirmed_lsn(&observer, "intake_slot")
        .await
        .expect("watermark must have advanced");
    assert!(
        watermark > 0,
        "watermark must have advanced past its seeded value"
    );
}

/// Issue #274 ("intake group-commit"): three separate single-row source
/// transactions, all committed before the consumer ever starts streaming (so
/// intake decodes their three `Commit`s back to back, well inside a generous
/// `max_delay`), with `group_commit` configured at a `max_rows` none of them
/// individually reach. All three must still land, in full and exactly once —
/// grouping must never drop, duplicate, or reorder a row — and the watermark
/// must still advance normally. (A `pg_stat_database.xact_commit`-based
/// "fewer commits than source transactions" assertion was considered and
/// dropped: it's too noisy to assert reliably inside a fast integration test
/// — `ProducerSession::connect`'s own setup queries, the per-relation catalog
/// lookups `handle_xlog_data` makes on a cache miss, and the polling loop
/// below all add their own uncounted autocommit commits to the same
/// database-wide counter. The commit-count reduction this feature is *for*
/// is measured empirically by the benchmark harness instead — see issue
/// #274's validation table.)
#[tokio::test]
async fn group_commit_batches_several_source_transactions_into_fewer_ring_commits() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key, payload text not null);
             create publication intake_pub for table widgets;",
        )
        .await
        .expect("create source table and publication");
    let slot_row = setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    let _: String = slot_row.get(0);
    seed_progress_at_slot(&setup, "intake_slot").await;

    // Three separate single-row transactions (three implicit-autocommit
    // `execute` calls, matching this crate's own "one INSERT == one
    // transaction" convention elsewhere — see
    // `benchmark/src/streaming/load.rs`), all committed before the consumer
    // connects, so intake sees three `Commit`s in immediate succession once
    // it starts streaming.
    for id in 1..=3i64 {
        setup
            .execute(
                "insert into widgets (id, payload) values ($1, 'hello')",
                &[&id],
            )
            .await
            .expect("insert source row");
    }

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "intake_slot".to_string(),
        publication: "intake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: intake::spill::DEFAULT_HARD_CAP,
        group_commit: Some(intake::GroupCommitConfig {
            max_rows: 1000,
            max_delay: std::time::Duration::from_millis(50),
        }),
    };
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");

    tokio::spawn(async move {
        let _ = consumer.run().await;
    });

    let observer = connect_raw(db.dsn()).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while seg_0_count(&observer).await < 3 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for all three changes to be staged"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let keys: Vec<String> = observer
        .query("select key from seg_0 order by key", &[])
        .await
        .expect("query staged rows")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        keys,
        vec!["1".to_string(), "2".to_string(), "3".to_string()],
        "grouping must stage every row from every batched transaction, none dropped or duplicated"
    );

    let watermark = confirmed_lsn(&observer, "intake_slot")
        .await
        .expect("watermark must have advanced");
    assert!(
        watermark > 0,
        "watermark must have advanced past its seeded value"
    );
}

/// Regression test for issue #56: under `REPLICA IDENTITY FULL`, `pgoutput`
/// marks every column of a `Relation` message `is_key` (not just the primary
/// key), so `extract_key` must not derive the staged key by blindly joining
/// every `is_key` column — it must fall back to the source table's actual
/// primary key. Mirrors the happy-path test above (a real replication slot,
/// publication, and live `Intake` consumer) but against a table with two
/// extra non-key columns and `REPLICA IDENTITY FULL` set, so a regression
/// would stage `key = "1\x1f10.00\x1f1.50"` instead of `key = "1"`.
#[tokio::test]
async fn a_full_replica_identity_change_extracts_the_primary_key_not_the_whole_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table priced_widgets ( \
                 id bigint primary key, \
                 amount numeric not null, \
                 rate numeric not null \
             ); \
             alter table priced_widgets replica identity full; \
             create publication intake_pub for table priced_widgets;",
        )
        .await
        .expect("create source table and publication");
    let slot_row = setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    let _: String = slot_row.get(0);
    seed_progress_at_slot(&setup, "intake_slot").await;

    // The change intake should pick up, made *after* the slot exists so it
    // is guaranteed to be in the stream.
    setup
        .execute(
            "insert into priced_widgets (id, amount, rate) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("insert source row");

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "intake_slot".to_string(),
        publication: "intake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: intake::spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");

    tokio::spawn(async move {
        let _ = consumer.run().await;
    });

    let observer = connect_raw(db.dsn()).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while seg_0_count(&observer).await < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the change to be staged"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let staged = observer
        .query_one("select src_table, key, op from seg_0", &[])
        .await
        .expect("query staged row");
    let src_table: String = staged.get(0);
    let key: String = staged.get(1);
    let op: String = staged.get(2);
    assert_eq!(src_table, format!("{DEFAULT_SCHEMA}.priced_widgets"));
    assert_eq!(
        key, "1",
        "the extracted key must be just the primary key, not the whole \
         row joined together"
    );
    assert_eq!(op, "insert");
}

/// Regression test for issue #163: a table whose `PRIMARY KEY (...)` clause
/// lists its columns in a *different* order than they are physically
/// declared —
///
/// ```sql
/// create table t (tag text, post bigint, primary key (post, tag));
/// ```
///
/// — must still stage its composite key in the primary key's own **declared**
/// order (`post` then `tag`), because that is the order every consumer of an
/// encoded composite key decodes with: `ddl::split_pk_key`, whose `pk` slice
/// comes from `ddl::source_primary_key`'s `array_position(i.indkey, a.attnum)`
/// sort, and `ddl::pk_key_sql_expr`, which re-renders the same identity in
/// SQL for `apply::read_live_rows_batch`'s live re-fetch.
///
/// `pgoutput` hands `extract_key` the columns in *physical* order (`tag`,
/// `post`) with per-column `is_key` flags, and `extract_key` used to simply
/// filter that order to the flagged columns — silently producing
/// `"rust\x1f7"` here, which the read side would then bind as `post =
/// 'rust'`/`tag = '7'`. Latent (no fixture had a PK declared out of physical
/// order) until something reached it; normalized now, and pinned here.
///
/// This table keeps the default replica identity deliberately: `DEFAULT` is
/// the case where `pgoutput`'s flags are *right* and only their order is
/// wrong, so it exercises the ordering normalization on its own rather than
/// through `REPLICA IDENTITY FULL`'s separate "every column is flagged"
/// problem (issue #56, the test above).
#[tokio::test]
async fn a_composite_key_declared_out_of_physical_column_order_stages_in_declared_order() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table ordered_pk_widgets ( \
                 tag text, \
                 post bigint, \
                 payload text, \
                 primary key (post, tag) \
             ); \
             create publication intake_pub for table ordered_pk_widgets;",
        )
        .await
        .expect("create source table and publication");
    let slot_row = setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    let _: String = slot_row.get(0);
    seed_progress_at_slot(&setup, "intake_slot").await;

    setup
        .execute(
            "insert into ordered_pk_widgets (tag, post, payload) values ('rust', 7, 'hi')",
            &[],
        )
        .await
        .expect("insert source row");

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "intake_slot".to_string(),
        publication: "intake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: intake::spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");

    tokio::spawn(async move {
        let _ = consumer.run().await;
    });

    let observer = connect_raw(db.dsn()).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while seg_0_count(&observer).await < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the change to be staged"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let staged = observer
        .query_one("select key from seg_0", &[])
        .await
        .expect("query staged row");
    let key: String = staged.get(0);
    assert_eq!(
        key, "7\u{1f}rust",
        "the composite key must be joined in the primary key's declared \
         order (post, tag), not the physical order (tag, post)"
    );

    // ...and the same claim expressed as the actual producer/consumer
    // agreement it stands for: the parts, in the order `ddl::source_primary_key`
    // reports the key's columns (the order `ddl::split_pk_key` decodes into),
    // are this row's `(post, tag)` values.
    let pk = trellis::defs::source_primary_key(
        &db.pool,
        &format!("{DEFAULT_SCHEMA}.ordered_pk_widgets"),
    )
    .await
    .expect("introspect the declared primary key");
    let pk_names: Vec<&str> = pk.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        pk_names,
        vec!["post", "tag"],
        "sanity: source_primary_key reports the declared order"
    );
    assert_eq!(
        key.split('\u{1f}').collect::<Vec<&str>>(),
        vec!["7", "rust"],
        "the staged key's parts must line up positionally with the primary \
         key's declared columns"
    );
}

/// Issue #163 review follow-up: the declared-order normalization now runs a
/// `pg_catalog` lookup for a `REPLICA IDENTITY DEFAULT` relation too — i.e.
/// for essentially *every* published table, not just the rare opt-in FULL
/// ones (#56). That lookup reads the **current** catalog on intake's own
/// session, while the `Relation` message it answers comes out of the WAL at
/// a possibly much older position, so the two can legitimately disagree: a
/// table inserted into and then dropped is still decoded (logical decoding
/// resolves the relation against a historic snapshot), but
/// `primary_key_columns`' `to_regclass` finds nothing and returns an empty
/// `Vec`.
///
/// An empty lookup must fall back to `pgoutput`'s own `is_key` flags — which
/// under DEFAULT *are* the primary key's columns, only unordered — rather
/// than override them with "no key columns", which would fail
/// `extract_key`'s "at least one key column" check and take down the whole
/// intake loop (`IntakeError::MissingKeyValue` propagates out of `run`) for
/// a change that staged fine before the normalization existed. FULL keeps
/// overriding unconditionally: there an empty lookup must *not* fall back,
/// because every column is flagged there and the flags would yield a key
/// made of the entire row (exactly issue #56's bug).
#[tokio::test]
async fn a_dropped_default_identity_table_still_stages_its_pending_change() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table doomed_widgets (id bigint primary key, payload text); \
             create publication intake_pub for table doomed_widgets;",
        )
        .await
        .expect("create source table and publication");
    let slot_row = setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    let _: String = slot_row.get(0);
    seed_progress_at_slot(&setup, "intake_slot").await;

    setup
        .execute(
            "insert into doomed_widgets (id, payload) values (1, 'hi')",
            &[],
        )
        .await
        .expect("insert source row");
    // The change is in the WAL and the slot hasn't consumed it yet; the
    // table itself is gone by the time intake decodes it.
    setup
        .batch_execute("drop table doomed_widgets")
        .await
        .expect("drop the source table out from under the slot");

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "intake_slot".to_string(),
        publication: "intake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: intake::spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");
    let outcome = tokio::sync::Mutex::new(None);
    let outcome = std::sync::Arc::new(outcome);
    let recorded = outcome.clone();
    tokio::spawn(async move {
        let result = consumer.run().await;
        *recorded.lock().await = Some(result.map_err(|e| e.to_string()));
    });

    let observer = connect_raw(db.dsn()).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while seg_0_count(&observer).await < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the change to be staged; intake reported \
             {:?}",
            outcome.try_lock().ok().and_then(|o| o.clone())
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let staged = observer
        .query_one("select key from seg_0", &[])
        .await
        .expect("query staged row");
    let key: String = staged.get(0);
    assert_eq!(
        key, "1",
        "the key must still come from pgoutput's own key flags when the \
         catalog no longer has the table to normalize the order against"
    );
}

/// A real `TRUNCATE` on a published source table must decode to a staged
/// `op = 'truncate'` sentinel row (issue #60) — not silently dropped, which
/// is what `handle_xlog_data`'s previous `Message::Truncate { .. } => {}`
/// no-op arm did. Mirrors the happy-path test above: a real replication
/// slot and publication, a live `Intake` consumer, and the same
/// poll-until-staged pattern.
#[tokio::test]
async fn a_truncate_message_becomes_a_staged_sentinel_not_dropped() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key, payload text not null);
             create publication intake_pub for table widgets;",
        )
        .await
        .expect("create source table and publication");
    let slot_row = setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    let _: String = slot_row.get(0);
    seed_progress_at_slot(&setup, "intake_slot").await;

    // A TRUNCATE made after the slot exists, so it's guaranteed to be in the
    // stream — publications publish TRUNCATE by default (`publish` defaults
    // to including it), so no extra publication option is needed here.
    setup
        .execute("insert into widgets (id, payload) values (1, 'hello')", &[])
        .await
        .expect("insert a row so the table isn't already empty");
    setup
        .execute("truncate widgets", &[])
        .await
        .expect("truncate source table");

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "intake_slot".to_string(),
        publication: "intake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: intake::spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");

    tokio::spawn(async move {
        let _ = consumer.run().await;
    });

    let observer = connect_raw(db.dsn()).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    // Wait for the truncate sentinel specifically (not just any row —
    // the insert above stages one too), since it lands in a later
    // transaction.
    loop {
        let count: i64 = observer
            .query_one("select count(*) from seg_0 where op = 'truncate'", &[])
            .await
            .expect("count staged truncate rows")
            .get(0);
        if count >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the truncate to be staged"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let staged = observer
        .query_one(
            "select src_table, key, old_image::text, new_image::text \
             from seg_0 where op = 'truncate'",
            &[],
        )
        .await
        .expect("query staged truncate row");
    let src_table: String = staged.get(0);
    let key: String = staged.get(1);
    let old_image: Option<String> = staged.get(2);
    let new_image: Option<String> = staged.get(3);
    assert_eq!(src_table, format!("{DEFAULT_SCHEMA}.widgets"));
    assert_eq!(key, TRUNCATE_SENTINEL_KEY);
    assert_eq!(old_image, None, "a truncate sentinel must be image-less");
    assert_eq!(new_image, None, "a truncate sentinel must be image-less");
}

/// Issue #133: real intake — decoding a live `pgoutput` stream, not the
/// low-level ring-staging helpers `apply_relationship_reverse.rs`'s own
/// #133 tests use to simulate the signal — populates `group_key` for a
/// from-side row whose table has a declared outbound (`from_table`)
/// relationship, via `Intake`'s `GroupKeyColumns` cache
/// (`handle_xlog_data`'s `touched_group_key` call). Mirrors the happy-path
/// test above's real-slot/real-stream shape, with a `RELATIONSHIP`
/// declared before the row is inserted.
#[tokio::test]
async fn intake_populates_group_key_for_a_from_side_row_with_an_outbound_relationship() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table categories (id bigint primary key, name text not null); \
             create table articles (id bigint primary key, category_id bigint not null); \
             alter table categories replica identity full; \
             alter table articles replica identity full; \
             create publication intake_pub for table categories, articles; \
             insert into categories (id, name) values (10, 'Tech');",
        )
        .await
        .expect("create source tables, publication, and seed a category");
    let slot_row = setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    let _: String = slot_row.get(0);
    seed_progress_at_slot(&setup, "intake_slot").await;

    // Declared before intake connects, so `GroupKeyColumns`' first (empty)
    // cache lookup for `articles` is a genuine catalog miss that finds it —
    // proving the cache-population path, not just a pre-seeded map.
    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create the outbound relationship");

    // The change intake should pick up, made *after* the slot exists so it
    // is guaranteed to be in the stream.
    setup
        .execute("insert into articles (id, category_id) values (1, 10)", &[])
        .await
        .expect("insert source row");

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "intake_slot".to_string(),
        publication: "intake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: intake::spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");

    tokio::spawn(async move {
        let _ = consumer.run().await;
    });

    let observer = connect_raw(db.dsn()).await;
    let articles_src_table = format!("{DEFAULT_SCHEMA}.articles");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let count: i64 = observer
            .query_one(
                "select count(*) from seg_0 where src_table = $1 and key = '1'",
                &[&articles_src_table],
            )
            .await
            .expect("count staged articles row")
            .get(0);
        if count >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the change to be staged"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let group_key: Option<Vec<String>> = observer
        .query_one(
            "select group_key from seg_0 where src_table = $1 and key = '1'",
            &[&articles_src_table],
        )
        .await
        .expect("read staged group_key")
        .get(0);
    assert_eq!(
        group_key,
        Some(vec!["10".to_string()]),
        "intake must populate group_key from the row's own outbound \
         relationship column (category_id), via the cached catalog lookup"
    );
}
