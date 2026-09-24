//! Issue #8's robustness battery: memory-bounded intake (spill-to-disk +
//! hard cap), the quiet-stream keepalive watermark advance, publication
//! reconciliation's backfill marker and its transaction fence, and the loud
//! startup error on slot loss. See
//! docs/staging-and-claiming/01-intake-and-lsn-confirmation.md.

use std::collections::HashMap;
use std::time::Duration;

use pgwire_replication::{Lsn, ReplicationEvent};
use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, create_definition};
use trellis::intake::{self, IntakeError, publication, spill};
use trellis::staging::session::ProducerSession;
use trellis::staging::{CdcOp, StagedChange, StagedWatermark};

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

/// Registers a definition reading `table`'s `id`, so the backfill discharge
/// has a reader to stage for: it skips a table no definition reads (issue
/// #417). Call it while `table` is still empty, so the registration's own
/// read stages nothing and every staged row the test counts is the
/// discharge's.
async fn register_reader(db: &testkit::TestDatabase, table: &str) {
    create_definition(
        &db.pool,
        &format!("TRANSFORM {table}_reader FROM {table} SELECT id AS total"),
        &HashMap::from([("id".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("register a definition reading the table");
}

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
/// position, as `initial_snapshot_handshake` does. A row behind the slot
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

fn cdc_change(table: &str, key: &str) -> StagedChange {
    StagedChange::Cdc {
        src_table: table.to_string(),
        key: key.to_string(),
        op: CdcOp::Insert,
        lsn: None,
        old_image: None,
        new_image: Some(format!(r#"{{"id":"{key}"}}"#)),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// Spill path ≡ buffered path: the same transaction's changes, staged once
/// through a buffer that spills to disk repeatedly and once through a buffer
/// that never spills, must produce identical staged rows. Cross-chunk
/// coalescing is deliberately not attempted (see `spill`'s module doc), so
/// this only needs to show the replay reproduces the same *set* of rows —
/// the fold (stage 04), not this stage, is what would ever collapse
/// duplicates.
#[tokio::test]
async fn spilled_replay_produces_the_same_staged_rows_as_a_fully_buffered_transaction() {
    let cluster = TestCluster::start();

    let changes: Vec<StagedChange> = (0..250)
        .map(|i| cdc_change("public.widgets", &format!("k{i}")))
        .collect();

    async fn stage_via(
        dsn: &str,
        threshold: usize,
        changes: &[StagedChange],
    ) -> Vec<(String, String)> {
        let mut client = connect_raw(dsn).await;
        seed_progress(&client, "slot1", 100).await;

        let mut buffer = spill::TxnBuffer::new(threshold, 10_000);
        for change in changes {
            buffer.push(change.clone(), 42).expect("push");
        }
        let txn = client.transaction().await.expect("begin");
        buffer
            .stage_and_advance(
                &txn,
                "slot1",
                "wake",
                PgLsn::from(200),
                std::time::SystemTime::now(),
            )
            .await
            .expect("stage_and_advance");
        txn.commit().await.expect("commit");

        let mut rows: Vec<(String, String)> = client
            .query("select key, new_image::text from seg_0 order by key", &[])
            .await
            .expect("query seg_0")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        rows.sort();
        rows
    }

    let db_spilled = cluster.create_isolated_database().await;
    let db_buffered = cluster.create_isolated_database().await;

    // Threshold 10 forces many spills across 250 pushes; threshold well
    // above the total never spills at all.
    let spilled = stage_via(db_spilled.dsn(), 10, &changes).await;
    let buffered = stage_via(db_buffered.dsn(), 1_000_000, &changes).await;

    assert_eq!(spilled.len(), 250);
    assert_eq!(
        spilled, buffered,
        "spilled and fully-buffered replay must produce identical staged rows"
    );
}

/// The hard cap trips before a transaction can grow without bound, naming
/// the xid and every table touched so the failure reads as a diagnosable
/// stall rather than an opaque OOM crash-loop.
#[test]
fn hard_cap_names_the_xid_and_every_table_touched() {
    let mut buffer = spill::TxnBuffer::new(1_000, 5);
    for i in 0..5 {
        buffer
            .push(cdc_change("public.widgets", &format!("w{i}")), 7)
            .expect("under the cap");
    }
    let err = buffer
        .push(cdc_change("public.gadgets", "g0"), 7)
        .expect_err("must trip the hard cap");
    match err {
        IntakeError::TransactionTooLarge { xid, cap, tables } => {
            assert_eq!(xid, 7);
            assert_eq!(cap, 5);
            assert_eq!(
                tables,
                vec!["public.widgets".to_string()],
                "the table the 6th (over-cap) push targets must not itself be recorded"
            );
        }
        other => panic!("expected TransactionTooLarge, got {other:?}"),
    }
}

/// The quiet-stream watermark advance on keepalive frames, exercised against
/// a live `Intake` via its public `handle_event` — all four guards:
/// (a) never mid-transaction, (b) never regress, (d) rate-limited. (c),
/// persist-before-report, is implied by every assertion here reading the
/// *durable* table, never an in-memory value `Intake` doesn't expose.
#[tokio::test]
async fn keepalive_watermark_advance_is_guarded_on_every_axis() {
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
    setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    seed_progress_at_slot(&setup, "intake_slot").await;

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
        spill_threshold: spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");
    // Every position below is an offset from where the slot (and so the
    // seeded watermark) starts.
    let base = confirmed_lsn(&setup, "intake_slot")
        .await
        .expect("seeded watermark");

    // Guard (a): a keepalive whose wal_end is ahead of an open transaction's
    // Begin must not move the watermark — those changes are still only
    // buffered, not staged.
    consumer
        .handle_event(ReplicationEvent::Begin {
            final_lsn: Lsn::from(base + 1_000),
            xid: 42,
            commit_time_micros: 0,
        })
        .await
        .expect("handle Begin");
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(base + 5_000),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base),
        "a keepalive received mid-transaction must not advance the watermark"
    );

    // The transaction commits with no buffered changes (no XLogData was fed
    // in) — the commit's own watermark advance still applies.
    consumer
        .handle_event(ReplicationEvent::Commit {
            lsn: Lsn::from(base + 900),
            end_lsn: Lsn::from(base + 1_000),
            commit_time_micros: 0,
        })
        .await
        .expect("handle Commit");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 1_000)
    );

    // Guard (b): a keepalive behind the already-confirmed position is a
    // silent no-op, not a regression.
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(base + 500),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 1_000),
        "a keepalive behind the confirmed position must never regress it"
    );

    // A quiet stream's keepalive, strictly ahead and outside a transaction,
    // with the rate limit not yet engaged (it starts pre-expired) — this is
    // the case the whole feature exists for.
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(base + 2_000),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 2_000)
    );

    // Guard (d): the very next keepalive, still within the rate-limit
    // window, must not re-persist even though its wal_end is higher still.
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(base + 3_000),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 2_000),
        "a second persist inside the rate-limit window must be suppressed"
    );
}

/// Adding a table to the publication backfills its pre-existing rows exactly
/// once, gated by the marker's transaction fence: a straggling writer whose
/// transaction was open when the `ALTER` committed must settle before
/// enumeration runs. Re-running the setup pass without ever deleting the
/// marker (the "survives a crash between the ALTER and the backfill" case)
/// must be a harmless retry, not a duplicate.
#[tokio::test]
async fn table_add_backfills_existing_rows_once_the_fence_settles_and_retries_safely() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key);
             create publication test_pub;",
        )
        .await
        .expect("create source table");
    register_reader(&db, "widgets").await;
    setup
        .batch_execute("insert into widgets (id) values (1), (2), (3)")
        .await
        .expect("seed pre-existing rows");

    // A straggling writer: opens (and holds open) a transaction with its own
    // xid before the ALTER runs, so the fence captured by reconcile must
    // name it as still in flight.
    let mut straggler = connect_raw(db.dsn()).await;
    let straggler_txn = straggler.transaction().await.expect("begin straggler");
    straggler_txn
        .query_one("select txid_current()", &[])
        .await
        .expect("assign the straggler an xid");

    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");
    publication::reconcile_publication(
        session.client_mut(),
        "test_pub",
        &[format!("{DEFAULT_SCHEMA}.widgets")],
    )
    .await
    .expect("reconcile adds widgets and leaves a pending_backfill marker");

    let marker_count: i64 = setup
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("query pending_backfill")
        .get(0);
    assert_eq!(marker_count, 1, "the ALTER must leave exactly one marker");

    // The fence hasn't settled — the straggler is still open — so this pass
    // must leave the marker untouched and stage nothing.
    publication::run_pending_backfills(
        session.client_mut(),
        "wake",
        &StagedWatermark::saturated(),
        std::time::Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills (unsettled)");
    let seg_0_count_before: i64 = setup
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0")
        .get(0);
    assert_eq!(
        seg_0_count_before, 0,
        "backfill must not run while its fence hasn't settled"
    );
    let marker_count: i64 = setup
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("query pending_backfill")
        .get(0);
    assert_eq!(
        marker_count, 1,
        "the unsettled marker must survive this pass"
    );

    // The straggler settles.
    straggler_txn.commit().await.expect("commit straggler");

    // Now the fence has settled: this pass backfills the 3 pre-existing rows
    // and discharges the marker, atomically.
    publication::run_pending_backfills(
        session.client_mut(),
        "wake",
        &StagedWatermark::saturated(),
        std::time::Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills (settled)");

    let staged: Vec<String> = setup
        .query(
            "select key from seg_0 where op = 'recompute' order by key",
            &[],
        )
        .await
        .expect("query seg_0")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        staged,
        vec!["1".to_string(), "2".to_string(), "3".to_string()]
    );
    let marker_count: i64 = setup
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("query pending_backfill")
        .get(0);
    assert_eq!(marker_count, 0, "the discharged marker must be deleted");

    // Re-running the setup pass with the marker already gone (the crash
    // scenario where a *later* pass finds nothing left to do) must be a
    // harmless no-op, never a duplicate backfill.
    publication::run_pending_backfills(
        session.client_mut(),
        "wake",
        &StagedWatermark::saturated(),
        std::time::Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills (idempotent no-op)");
    let seg_0_count_after: i64 = setup
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0")
        .get(0);
    assert_eq!(
        seg_0_count_after, 3,
        "retrying after the marker is gone must not re-stage anything"
    );
}

/// Issue #417: a marker on a table no definition reads is discharged without
/// enumerating it, since nothing would consume the `Recompute` rows. A fresh
/// install parks one on every configured source table, used or not.
#[tokio::test]
async fn a_marker_on_a_table_nothing_reads_is_discharged_without_staging() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key);
             insert into widgets (id) values (1), (2), (3);
             create publication test_pub;",
        )
        .await
        .expect("create source table with pre-existing rows");

    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");
    publication::reconcile_publication(
        session.client_mut(),
        "test_pub",
        &[format!("{DEFAULT_SCHEMA}.widgets")],
    )
    .await
    .expect("reconcile adds widgets and parks a marker");
    publication::run_pending_backfills(
        session.client_mut(),
        "wake",
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");

    let staged: i64 = setup
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0")
        .get(0);
    assert_eq!(staged, 0, "a table nothing reads must not be enumerated");
    let markers: i64 = setup
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("count pending_backfill")
        .get(0);
    assert_eq!(markers, 0, "its marker is still discharged");
}

/// Issue #312: a backfill enumeration must not stage its `Recompute` rows
/// until intake has staged everything the enumeration's snapshot can see.
/// Otherwise the recompute can seal and drain ahead of the CDC for a change
/// it already reflects, and an aggregate counts that change twice. While
/// intake is behind, the pass stages nothing and keeps the marker; once
/// intake catches up during the wait, the same pass goes ahead.
#[tokio::test]
async fn a_backfill_enumeration_waits_for_intake_to_stage_what_its_snapshot_saw() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key);
             create publication test_pub;",
        )
        .await
        .expect("create source table");
    register_reader(&db, "widgets").await;
    setup
        .batch_execute("insert into widgets (id) values (1), (2), (3)")
        .await
        .expect("seed pre-existing rows");

    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");
    publication::reconcile_publication(
        session.client_mut(),
        "test_pub",
        &[format!("{DEFAULT_SCHEMA}.widgets")],
    )
    .await
    .expect("reconcile adds widgets and leaves a pending_backfill marker");

    let staged_count = async || -> i64 {
        setup
            .query_one("select count(*) from seg_0", &[])
            .await
            .expect("count seg_0")
            .get(0)
    };
    let marker_count = async || -> i64 {
        setup
            .query_one("select count(*) from pending_backfill", &[])
            .await
            .expect("count pending_backfill")
            .get(0)
    };

    // Intake has staged nothing: the enumeration must defer, not stage.
    publication::run_pending_backfills(
        session.client_mut(),
        "wake",
        &StagedWatermark::new(),
        Duration::from_millis(50),
    )
    .await
    .expect("run_pending_backfills (intake behind)");
    assert_eq!(
        staged_count().await,
        0,
        "an enumeration must not stage while intake is behind its snapshot"
    );
    assert_eq!(marker_count().await, 1, "the deferred marker must survive");

    // Intake catches up while the enumeration is waiting on it.
    let watermark = StagedWatermark::new();
    let intake_side = watermark.clone();
    let catch_up = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        intake_side.advance(PgLsn::from(u64::MAX));
    });
    publication::run_pending_backfills(
        session.client_mut(),
        "wake",
        &watermark,
        Duration::from_secs(30),
    )
    .await
    .expect("run_pending_backfills (intake catches up)");
    catch_up.await.expect("catch-up task");
    assert_eq!(
        staged_count().await,
        3,
        "once intake catches up, the enumeration must stage every row"
    );
    assert_eq!(
        marker_count().await,
        0,
        "the discharged marker must be deleted"
    );
}

/// A slot this instance previously confirmed work against, but which is now
/// missing or invalidated (retention exceeded, or lost across a pre-PG17
/// failover), must fail `Intake::connect` loudly, naming the slot and the
/// last confirmed position — never resume silently into a gap. A `Client`
/// never reaches this in practice: its staging setup recovers from the loss
/// first (issue #310, `intake::slot_loss`), so this guards `Intake` used
/// directly and a slot vanishing between that setup and the connect.
#[tokio::test]
async fn a_missing_slot_with_prior_confirmed_progress_is_a_loud_startup_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    // A `replication_progress` row exists (this instance has confirmed work
    // before), but no slot or publication was ever created for it — standing
    // in for "the slot is gone by the time we reconnect."
    seed_progress(&setup, "ghost_slot", 500).await;

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "ghost_slot".to_string(),
        publication: "ghost_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };

    match intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone()).await {
        Err(IntakeError::SlotLost {
            slot,
            last_confirmed_lsn,
        }) => {
            assert_eq!(slot, "ghost_slot");
            assert_eq!(last_confirmed_lsn, 500);
        }
        Err(other) => panic!("expected SlotLost, got {other:?}"),
        Ok(_) => panic!("a missing slot with prior confirmed progress must refuse to start"),
    }
}

/// Issue #31, finding 2: connecting `Intake` for a slot with no
/// `replication_progress` row at all must fail loud with
/// `MissingProgressRow`, never silently proceed. Left unchecked, the
/// linchpin's watermark `UPDATE ... WHERE slot_name = $2 AND confirmed_lsn <
/// $1` (`stage_and_advance`) would match zero rows forever while intake
/// keeps acking the slot — WAL reclaims ahead of a watermark that never
/// persists, with no diagnostic anywhere. A real slot/publication do exist
/// here (unlike the `SlotLost` case above) — only the progress row is
/// missing, isolating this from slot loss.
#[tokio::test]
async fn connecting_with_no_progress_row_is_a_loud_startup_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key, payload text not null);
             create publication orphan_pub for table widgets;",
        )
        .await
        .expect("create source table and publication");
    setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('orphan_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    // Deliberately skip seeding replication_progress — standing in for
    // `create_slot_and_park_markers` never having run against this slot.

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "orphan_slot".to_string(),
        publication: "orphan_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };

    match intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone()).await {
        Err(IntakeError::MissingProgressRow { slot }) => {
            assert_eq!(slot, "orphan_slot");
        }
        Err(other) => panic!("expected MissingProgressRow, got {other:?}"),
        Ok(_) => panic!("a slot with no replication_progress row must refuse to start"),
    }
}

/// The other half of finding 2: `create_slot_and_park_markers` is the code
/// that is supposed to create the `replication_progress` row (per
/// `V4__replication_progress.sql`'s "whatever first uses a slot's name"),
/// seeded at the slot's own consistent point — so a slot set up through it,
/// and nothing else, must pass `Intake::connect`'s precondition check
/// without the test seeding anything itself.
#[tokio::test]
async fn fresh_slot_setup_seeds_the_progress_row_connect_requires() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key, payload text not null);
             create publication handshake_pub for table widgets;",
        )
        .await
        .expect("create source table and publication");

    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");
    publication::create_slot_and_park_markers(
        &mut session,
        "handshake_slot",
        &[format!("{DEFAULT_SCHEMA}.widgets")],
    )
    .await
    .expect("create the slot");
    drop(session);

    assert!(
        confirmed_lsn(&setup, "handshake_slot").await.is_some(),
        "create_slot_and_park_markers must seed a replication_progress row"
    );
    // Issue #417: `widgets` was already published, so no join parked a
    // marker for it. Setup must, or its rows would never be captured.
    let marked: Vec<String> = setup
        .query("select table_name from pending_backfill", &[])
        .await
        .expect("read pending_backfill")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(marked, vec![format!("{DEFAULT_SCHEMA}.widgets")]);
    let staged: i64 = setup
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0")
        .get(0);
    assert_eq!(staged, 0, "slot setup must read no source rows itself");

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "handshake_slot".to_string(),
        publication: "handshake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect must succeed once slot setup has seeded the row");
}

/// Finding 1: `pg_create_logical_replication_slot` persists the slot to disk
/// the instant it returns, independent of the transaction
/// `create_slot_and_park_markers` runs it in — so a crash between slot creation
/// and that function's own commit leaves the slot on disk with no
/// `replication_progress` row. Reproduced directly (create the slot with raw
/// SQL, seed no row) rather than by actually crashing mid-setup; the
/// resulting on-disk state is identical either way. Re-running slot setup
/// against this state must not die at slot-create with "already exists" — it
/// must name the orphan so an operator can drop and retry.
#[tokio::test]
async fn fresh_slot_setup_reports_an_orphaned_slot_instead_of_retrying_blind() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets (id bigint primary key, payload text not null);
             create publication orphan_slot_pub for table widgets;",
        )
        .await
        .expect("create source table and publication");
    setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('orphan_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot directly, standing in for the crash-orphaned state");
    // Deliberately no replication_progress row — this is the orphan.

    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");

    match publication::create_slot_and_park_markers(
        &mut session,
        "orphan_slot",
        &[format!("{DEFAULT_SCHEMA}.widgets")],
    )
    .await
    {
        Err(IntakeError::OrphanedSlot { slot }) => assert_eq!(slot, "orphan_slot"),
        Err(other) => panic!("expected OrphanedSlot, got {other:?}"),
        Ok(()) => panic!("an orphaned slot must not be silently re-initialized"),
    }

    setup
        .execute("select pg_drop_replication_slot('orphan_slot')", &[])
        .await
        .expect("clean up the orphaned slot so it doesn't leak");
}

/// Issue #188's production-hardening finding: `pg_replication_slots` is a
/// cluster-wide system view, not scoped to the connected database, but a
/// logical slot only ever belongs to the database it was created against.
/// Before this fix, `slot_is_orphaned`'s existence check didn't filter on
/// `database = current_database()`, so a same-named slot that genuinely
/// belongs to a *different* database on the same cluster was
/// indistinguishable from this database's own orphan (the name "exists"
/// somewhere, and this database's `replication_progress` naturally has no
/// row for a slot it never created) — exactly the false diagnosis
/// `generative/tests/convergence.rs`'s shared-cluster harness hit when two
/// proptest cases' isolated databases collided on the same hardcoded slot
/// name.
///
/// Reproduced directly with two isolated databases sharing one
/// `TestCluster`: `db_a` creates a real slot; `db_b`'s slot setup for a slot
/// of the *same name* must not call it *its own* orphan. Post-fix, the
/// scoped existence check correctly reports "no such slot in this
/// database", so slot setup proceeds to actually create one — and
/// Postgres's own cluster-wide slot-name uniqueness constraint rejects that,
/// surfacing as a plain [`IntakeError::Db`] rather than a misleading
/// [`IntakeError::OrphanedSlot`].
#[tokio::test]
async fn fresh_slot_setup_does_not_mistake_another_databases_slot_for_its_own_orphan() {
    let cluster = TestCluster::start();
    let db_a = cluster.create_isolated_database().await;
    let db_b = cluster.create_isolated_database().await;

    // A slot that genuinely belongs to db_a — not orphaned from db_a's own
    // point of view, just irrelevant to the assertion below (db_b never
    // looks at db_a's `replication_progress` row either way).
    let setup_a = connect_raw(db_a.dsn()).await;
    setup_a
        .batch_execute(
            "create table widgets (id bigint primary key, payload text not null);
             create publication shared_name_pub for table widgets;",
        )
        .await
        .expect("create source table and publication on db_a");
    setup_a
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('shared_name_slot', \
             'pgoutput')",
            &[],
        )
        .await
        .expect("create a real replication slot on db_a");

    // db_b never created a slot by this name at all — its own slot setup for
    // the same name must not be told it's *its own* crash-orphaned slot.
    let mut session_b = ProducerSession::connect(db_b.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session for db_b");

    match publication::create_slot_and_park_markers(&mut session_b, "shared_name_slot", &[]).await {
        Err(IntakeError::OrphanedSlot { slot }) => panic!(
            "db_b misdiagnosed db_a's slot {slot:?} as its own orphan — \
             slot_is_orphaned must scope its pg_replication_slots check by \
             database = current_database()"
        ),
        Err(IntakeError::Db(_)) => {
            // Expected: the scoped check correctly says "not this
            // database's slot", so slot setup goes on to actually try
            // `pg_create_logical_replication_slot`, which Postgres itself
            // rejects — the name is taken cluster-wide, just not by db_b.
        }
        Err(other) => panic!("expected IntakeError::Db (slot name collision), got {other:?}"),
        Ok(()) => {
            panic!("db_b's slot setup must not succeed while db_a still holds the same slot name")
        }
    }

    setup_a
        .execute("select pg_drop_replication_slot('shared_name_slot')", &[])
        .await
        .expect("clean up db_a's slot so it doesn't leak");
}

/// The other half of issue #188's cluster-wide-view finding, and the more
/// dangerous half: `require_slot_healthy`'s own `pg_replication_slots` lookup
/// (`slot_health`) must be scoped by `database = current_database()` too.
/// Unscoped, it fails *open* rather than loud — a same-named slot owned by a
/// different database on the same cluster makes this database's missing (or
/// invalidated) slot read back as `Healthy`, so intake would resume across the
/// very WAL gap this check exists to refuse, on the strength of a slot it can
/// never actually stream from.
///
/// Same two-database setup as the orphan case above, inverted: `db_a` owns a
/// real, healthy slot, and `db_b` has confirmed progress against a slot of the
/// same name that does not exist in `db_b` at all. `db_b` must still be told
/// [`IntakeError::SlotLost`].
#[tokio::test]
async fn another_databases_healthy_slot_does_not_make_a_lost_slot_look_healthy() {
    let cluster = TestCluster::start();
    let db_a = cluster.create_isolated_database().await;
    let db_b = cluster.create_isolated_database().await;

    let setup_a = connect_raw(db_a.dsn()).await;
    setup_a
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('shared_health_slot', \
             'pgoutput')",
            &[],
        )
        .await
        .expect("create a real, healthy replication slot on db_a");

    // db_b has confirmed work against this slot name before, but the slot
    // itself is gone from db_b (it never existed there) — the `SlotLost`
    // condition, with a decoy of the same name one database over.
    let setup_b = connect_raw(db_b.dsn()).await;
    seed_progress(&setup_b, "shared_health_slot", 500).await;

    match publication::require_slot_healthy(&setup_b, "shared_health_slot", PgLsn::from(500)).await
    {
        Err(IntakeError::SlotLost {
            slot,
            last_confirmed_lsn,
        }) => {
            assert_eq!(slot, "shared_health_slot");
            assert_eq!(last_confirmed_lsn, 500);
        }
        Err(other) => panic!("expected SlotLost, got {other:?}"),
        Ok(()) => panic!(
            "db_b's own slot is gone; db_a's same-named slot must not make it look healthy — \
             slot_health must scope its pg_replication_slots check by database = \
             current_database()"
        ),
    }

    setup_a
        .execute("select pg_drop_replication_slot('shared_health_slot')", &[])
        .await
        .expect("clean up db_a's slot so it doesn't leak");
}

/// Issue #406: a slot that exists and isn't `lost` is still not healthy if it
/// was dropped and recreated under the same name, because everything committed
/// between the drop and the recreate is gone. The recreated slot starts past
/// the position this instance last confirmed; a slot it has been
/// acknowledging never gets there, since the acknowledgment only goes out
/// after the position is persisted. Both halves are pinned here: a slot at or
/// behind the confirmed position is healthy (behind is the crash window
/// between persisting a position and acknowledging it), and one ahead of it
/// is lost.
#[tokio::test]
async fn a_slot_recreated_under_the_same_name_is_not_healthy() {
    const SLOT: &str = "reborn_slot";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let setup = connect_raw(db.dsn()).await;

    let created: PgLsn = setup
        .query_one(
            "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .expect("create the slot")
        .get(0);
    publication::require_slot_healthy(&setup, SLOT, created)
        .await
        .expect("a slot at exactly the confirmed position is healthy");
    let persisted_ahead = PgLsn::from(u64::from(created) + 0x10_0000);
    publication::require_slot_healthy(&setup, SLOT, persisted_ahead)
        .await
        .expect("a slot behind the confirmed position (acknowledgment not yet sent) is healthy");

    setup
        .execute("select pg_drop_replication_slot($1)", &[&SLOT])
        .await
        .expect("drop the slot");
    setup
        .batch_execute(
            "create table gap_writes (id int primary key); insert into gap_writes values (1);",
        )
        .await
        .expect("commit a write no stream will deliver");
    setup
        .query_one(
            "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .expect("recreate the slot under the same name");

    match publication::require_slot_healthy(&setup, SLOT, created).await {
        Err(IntakeError::SlotLost {
            slot,
            last_confirmed_lsn,
        }) => {
            assert_eq!(slot, SLOT);
            assert_eq!(last_confirmed_lsn, u64::from(created));
        }
        Err(other) => panic!("expected SlotLost, got {other:?}"),
        Ok(()) => panic!(
            "a slot recreated past the confirmed position has lost the writes in between and \
             must not be reported healthy"
        ),
    }

    setup
        .execute("select pg_drop_replication_slot($1)", &[&SLOT])
        .await
        .expect("clean up the slot");
}

/// Issue #406: a slot another session is still creating under the name this
/// instance confirmed work against has no `confirmed_flush_lsn` yet (Postgres
/// sets it only once the new slot reaches its consistent point, which waits
/// for every transaction running at creation to finish). The slot this
/// instance acknowledged always has one, so a NULL position is a recreate in
/// progress, not a healthy slot. Called healthy, intake could start streaming
/// the moment the creation finished, from past the gap.
#[tokio::test]
async fn a_slot_another_session_is_still_creating_is_not_healthy() {
    const SLOT: &str = "creating_slot";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let setup = connect_raw(db.dsn()).await;

    let confirmed: PgLsn = setup
        .query_one(
            "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .expect("create the original slot")
        .get(0);
    setup
        .execute("select pg_drop_replication_slot($1)", &[&SLOT])
        .await
        .expect("drop the original slot");

    // An open transaction with an xid holds the new slot's creation short of
    // its consistent point until it ends.
    let blocker = connect_raw(db.dsn()).await;
    blocker
        .batch_execute("begin; select pg_current_xact_id();")
        .await
        .expect("open a transaction with an xid");
    let creator = connect_raw(db.dsn()).await;
    let creating = tokio::spawn(async move {
        creator
            .query_one(
                "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&SLOT],
            )
            .await
            .map(|_| ())
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let in_creation: bool = setup
            .query_one(
                "select exists(select 1 from pg_replication_slots where slot_name = $1 \
                 and database = current_database() and confirmed_flush_lsn is null)",
                &[&SLOT],
            )
            .await
            .expect("look for the slot in creation")
            .get(0);
        if in_creation {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the slot never showed up mid-creation"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let health = publication::require_slot_healthy(&setup, SLOT, confirmed).await;

    blocker
        .batch_execute("commit")
        .await
        .expect("end the blocking transaction");
    creating
        .await
        .expect("join the creating task")
        .expect("the slot's creation finishes once the blocker ends");
    setup
        .execute("select pg_drop_replication_slot($1)", &[&SLOT])
        .await
        .expect("clean up the slot");

    match health {
        Err(IntakeError::SlotLost { slot, .. }) => assert_eq!(slot, SLOT),
        Err(other) => panic!("expected SlotLost, got {other:?}"),
        Ok(()) => panic!(
            "a slot still being created under this instance's slot name is a recreate, not the \
             slot this instance acknowledged, and must not be reported healthy"
        ),
    }
}

/// Item 6 (issue #32): the `wal_status = 'lost'` branch of
/// `require_slot_healthy` — a slot the server actively invalidated because
/// its retained WAL blew past `max_slot_wal_keep_size`, as distinct from
/// [`a_missing_slot_with_prior_confirmed_progress_is_a_loud_startup_error`]'s
/// slot that simply never existed. Forces invalidation for real (no
/// inspection-only coverage): sets `max_slot_wal_keep_size` to the smallest
/// possible budget, generates enough WAL to blow past it, and checkpoints —
/// invalidation is decided at checkpoint time, not the instant WAL is
/// written. A dedicated `TestCluster` because this flips a cluster-wide GUC;
/// nothing else shares this instance.
#[tokio::test]
async fn an_invalidated_slot_is_detected_and_recovered_by_recreating_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let setup = connect_raw(db.dsn()).await;
    // Zero — the smallest possible retention budget — so any WAL generated
    // after the slot exists is enough to exceed it. `sighup` context: no
    // restart needed, just a reload.
    // Each as its own simple-query statement: `ALTER SYSTEM` refuses to run
    // inside a transaction block, and `batch_execute`'s multi-statement
    // string is sent as one that Postgres treats as an implicit block.
    setup
        .execute("alter system set max_slot_wal_keep_size = '0'", &[])
        .await
        .expect("set max_slot_wal_keep_size");
    setup
        .execute("select pg_reload_conf()", &[])
        .await
        .expect("reload config");

    setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('lossy_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");

    // Generate several WAL segments' worth of data, force segment switches,
    // then checkpoint — the point at which the server actually notices the
    // slot's retained WAL exceeds the (zero) budget and marks it lost.
    setup
        .batch_execute(
            "create table wal_filler (id bigint primary key, payload text not null);
             insert into wal_filler (id, payload)
             select g, repeat('x', 2000) from generate_series(1, 20000) g;",
        )
        .await
        .expect("generate WAL past the slot's retention budget");
    for _ in 0..3 {
        setup
            .execute("select pg_switch_wal()", &[])
            .await
            .expect("force a WAL segment switch");
    }
    setup
        .execute("checkpoint", &[])
        .await
        .expect("checkpoint to trigger the invalidation check");

    // Poll rather than assume one checkpoint is enough — bounded so a
    // genuine failure to invalidate fails loud instead of hanging.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let wal_status: String = loop {
        let observed: String = setup
            .query_one(
                "select wal_status from pg_replication_slots where slot_name = 'lossy_slot'",
                &[],
            )
            .await
            .expect("query wal_status")
            .get(0);
        if observed == "lost" || tokio::time::Instant::now() >= deadline {
            break observed;
        }
        // Each retry also needs another checkpoint to re-evaluate
        // invalidation, and a little more WAL in case the first batch
        // wasn't quite enough on this build's defaults.
        let _ = setup
            .batch_execute(
                "insert into wal_filler (id, payload) \
                 select g, repeat('y', 2000) from generate_series(20001, 25000) g; \
                 checkpoint;",
            )
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    };

    if wal_status != "lost" {
        panic!(
            "could not force the slot into wal_status = 'lost' within the timeout \
             (last observed: {wal_status:?}) — see item 6's fallback note if this recurs"
        );
    }

    seed_progress(&setup, "lossy_slot", 500).await;

    match publication::require_slot_healthy(&setup, "lossy_slot", PgLsn::from(500)).await {
        Err(IntakeError::SlotLost {
            slot,
            last_confirmed_lsn,
        }) => {
            assert_eq!(slot, "lossy_slot");
            assert_eq!(last_confirmed_lsn, 500);
        }
        Err(other) => panic!("expected SlotLost, got {other:?}"),
        Ok(()) => panic!("an invalidated slot must not be reported healthy"),
    }

    // Issue #310: the staging worker's recovery replaces the invalidated
    // slot — which can never stream again — with a fresh one and moves the
    // durable watermark to its start. Restore a real retention budget first
    // so the new slot isn't invalidated again by the next checkpoint.
    setup
        .execute("alter system reset max_slot_wal_keep_size", &[])
        .await
        .expect("reset max_slot_wal_keep_size");
    setup
        .execute("select pg_reload_conf()", &[])
        .await
        .expect("reload config");
    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("open a producer session");
    let recovery =
        intake::slot_loss::pause_if_slot_lost(&mut session, &db.pool, "lossy_slot", "no_pub")
            .await
            .expect("recover from the invalidated slot")
            .expect("an invalidated slot is a loss to recover from");
    assert!(recovery.paused.is_empty() && recovery.already_frozen.is_empty());
    let wal_status: String = setup
        .query_one(
            "select wal_status from pg_replication_slots where slot_name = 'lossy_slot'",
            &[],
        )
        .await
        .expect("the slot exists again")
        .get(0);
    assert_ne!(wal_status, "lost", "the invalidated slot must be replaced");
    assert_eq!(
        confirmed_lsn(&setup, "lossy_slot").await,
        Some(u64::from(recovery.new_slot_lsn)),
        "the durable watermark moves to the recreated slot's start"
    );
    publication::require_slot_healthy(&setup, "lossy_slot", recovery.new_slot_lsn)
        .await
        .expect("the recreated slot is healthy");
    drop(session);
    setup
        .execute("select pg_drop_replication_slot('lossy_slot')", &[])
        .await
        .expect("clean up the recreated slot");
}

fn converge_message(lsn: u64, prefix: &str, transactional: bool) -> ReplicationEvent {
    ReplicationEvent::Message {
        transactional,
        lsn: Lsn::from(lsn),
        prefix: prefix.to_string(),
        content: bytes::Bytes::new(),
    }
}

/// Issue #452: a waiter's `trellis.converge` message confirms through its own
/// position at once, skipping the keepalive throttle (guard (d)) that would
/// otherwise hold a quiet stream's waiter for ~10s. Guards (a) and (b) still
/// hold, a still-open group-commit batch is flushed before the message is
/// confirmed past it, and any other message is ignored.
#[tokio::test]
async fn a_converge_message_confirms_through_it_at_once_but_keeps_the_other_guards() {
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
    setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    seed_progress_at_slot(&setup, "intake_slot").await;

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
        spill_threshold: spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: spill::DEFAULT_HARD_CAP,
        // Driven through `handle_event`, a batch only flushes on `max_rows`,
        // so a Commit below stays pending until something flushes it.
        group_commit: Some(intake::GroupCommitConfig {
            max_rows: 1000,
            max_delay: Duration::from_secs(60),
        }),
    };
    let watermark = StagedWatermark::new();
    let mut consumer = intake::Intake::connect(&config, watermark.clone(), db.pool.clone())
        .await
        .expect("connect intake");
    let base = confirmed_lsn(&setup, "intake_slot")
        .await
        .expect("seeded watermark");
    let prefix = "trellis.converge";

    // Guard (a): never mid-transaction.
    consumer
        .handle_event(ReplicationEvent::Begin {
            final_lsn: Lsn::from(base + 1_000),
            xid: 42,
            commit_time_micros: 0,
        })
        .await
        .expect("handle Begin");
    consumer
        .handle_event(converge_message(base + 5_000, prefix, false))
        .await
        .expect("handle Message");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base),
        "a converge message received mid-transaction must not advance the watermark"
    );

    // The commit joins the open group batch, so nothing is confirmed yet.
    consumer
        .handle_event(ReplicationEvent::Commit {
            lsn: Lsn::from(base + 900),
            end_lsn: Lsn::from(base + 1_000),
            commit_time_micros: 0,
        })
        .await
        .expect("handle Commit");
    assert_eq!(confirmed_lsn(&setup, "intake_slot").await, Some(base));

    // The message flushes that batch and confirms through its own position,
    // durably and in memory.
    consumer
        .handle_event(converge_message(base + 2_000, prefix, false))
        .await
        .expect("handle Message");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 2_000)
    );
    assert_eq!(u64::from(watermark.get()), base + 2_000);

    // Guard (d) still throttles keepalives: that persist just reset it.
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(base + 3_000),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 2_000),
        "a keepalive inside the throttle window must still not persist"
    );

    // ...but not a converge message: that's the point of it.
    consumer
        .handle_event(converge_message(base + 4_000, prefix, false))
        .await
        .expect("handle Message");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 4_000),
        "a converge message must confirm at once, whatever the keepalive throttle says"
    );

    // Guard (b): never regress.
    consumer
        .handle_event(converge_message(base + 3_500, prefix, false))
        .await
        .expect("handle Message");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 4_000)
    );

    // Anything else is ignored: another prefix, or a transactional message.
    for event in [
        converge_message(base + 6_000, "someone.else", false),
        converge_message(base + 6_000, prefix, true),
    ] {
        consumer.handle_event(event).await.expect("handle Message");
    }
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(base + 4_000),
        "only a non-transactional trellis.converge message may confirm"
    );
}
