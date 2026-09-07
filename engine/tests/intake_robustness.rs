//! Issue #8's robustness battery: memory-bounded intake (spill-to-disk +
//! hard cap), the quiet-stream keepalive watermark advance, publication
//! reconciliation's backfill marker and its transaction fence, and the loud
//! startup error on slot loss. See
//! docs/staging-and-claiming/01-intake-and-lsn-confirmation.md.

use std::time::Duration;

use engine::config::DEFAULT_SCHEMA;
use engine::intake::{self, IntakeError, publication, spill};
use engine::staging::session::ProducerSession;
use engine::staging::{CdcOp, StagedChange};
use pgwire_replication::{Lsn, ReplicationEvent};
use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};

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

async fn seed_progress(client: &Client, slot: &str, confirmed_lsn: u64) {
    client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
            &[&slot, &PgLsn::from(confirmed_lsn)],
        )
        .await
        .expect("seed replication_progress");
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
    // This spill fixture has no catalog-backed source relation. Live intake
    // supplies `Some(relation_id)` from pgoutput instead.
    StagedChange::Cdc {
        src_table: table.to_string(),
        source_relation_oid: None,
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
    seed_progress(&setup, "intake_slot", 0).await;

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
    };
    let mut consumer = intake::Intake::connect(&config)
        .await
        .expect("connect intake");

    // Guard (a): a keepalive whose wal_end is ahead of an open transaction's
    // Begin must not move the watermark — those changes are still only
    // buffered, not staged.
    consumer
        .handle_event(ReplicationEvent::Begin {
            final_lsn: Lsn::from(1_000),
            xid: 42,
            commit_time_micros: 0,
        })
        .await
        .expect("handle Begin");
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(5_000),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(0),
        "a keepalive received mid-transaction must not advance the watermark"
    );

    // The transaction commits with no buffered changes (no XLogData was fed
    // in) — the commit's own watermark advance still applies.
    consumer
        .handle_event(ReplicationEvent::Commit {
            lsn: Lsn::from(900),
            end_lsn: Lsn::from(1_000),
            commit_time_micros: 0,
        })
        .await
        .expect("handle Commit");
    assert_eq!(confirmed_lsn(&setup, "intake_slot").await, Some(1_000));

    // Guard (b): a keepalive behind the already-confirmed position is a
    // silent no-op, not a regression.
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(500),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(1_000),
        "a keepalive behind the confirmed position must never regress it"
    );

    // A quiet stream's keepalive, strictly ahead and outside a transaction,
    // with the rate limit not yet engaged (it starts pre-expired) — this is
    // the case the whole feature exists for.
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(2_000),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(confirmed_lsn(&setup, "intake_slot").await, Some(2_000));

    // Guard (d): the very next keepalive, still within the rate-limit
    // window, must not re-persist even though its wal_end is higher still.
    consumer
        .handle_event(ReplicationEvent::KeepAlive {
            wal_end: Lsn::from(3_000),
            reply_requested: false,
            server_time_micros: 0,
        })
        .await
        .expect("handle KeepAlive");
    assert_eq!(
        confirmed_lsn(&setup, "intake_slot").await,
        Some(2_000),
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
             insert into widgets (id) values (1), (2), (3);
             create publication test_pub;",
        )
        .await
        .expect("create source table with pre-existing rows");

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
    publication::run_pending_backfills(session.client_mut(), "wake")
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
    publication::run_pending_backfills(session.client_mut(), "wake")
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
    publication::run_pending_backfills(session.client_mut(), "wake")
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

/// A slot this instance previously confirmed work against, but which is now
/// missing or invalidated (retention exceeded, or lost across a pre-PG17
/// failover), must fail `Intake::connect` loudly, naming the slot and the
/// last confirmed position — never resume silently into a gap.
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
    };

    match intake::Intake::connect(&config).await {
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
    // `initial_snapshot_handshake` never having run against this slot.

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
    };

    match intake::Intake::connect(&config).await {
        Err(IntakeError::MissingProgressRow { slot }) => {
            assert_eq!(slot, "orphan_slot");
        }
        Err(other) => panic!("expected MissingProgressRow, got {other:?}"),
        Ok(_) => panic!("a slot with no replication_progress row must refuse to start"),
    }
}

/// The other half of finding 2: `initial_snapshot_handshake` is the code
/// that is supposed to create the `replication_progress` row (per
/// `V4__replication_progress.sql`'s "whatever first uses a slot's name"),
/// seeded at the slot's own consistent point — so a slot set up through it,
/// and nothing else, must pass `Intake::connect`'s precondition check
/// without the test seeding anything itself.
#[tokio::test]
async fn initial_snapshot_handshake_seeds_the_progress_row_connect_requires() {
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
    publication::initial_snapshot_handshake(
        &mut session,
        "handshake_slot",
        &[format!("{DEFAULT_SCHEMA}.widgets")],
    )
    .await
    .expect("run initial snapshot handshake");
    drop(session);

    assert!(
        confirmed_lsn(&setup, "handshake_slot").await.is_some(),
        "initial_snapshot_handshake must seed a replication_progress row"
    );

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
    };
    intake::Intake::connect(&config)
        .await
        .expect("connect must succeed once the handshake has seeded the row");
}

/// Finding 1: `pg_create_logical_replication_slot` persists the slot to disk
/// the instant it returns, independent of the transaction
/// `initial_snapshot_handshake` runs it in — so a crash between slot creation
/// and that handshake's own commit leaves the slot on disk with no
/// `replication_progress` row. Reproduced directly (create the slot with raw
/// SQL, seed no row) rather than by actually crashing mid-handshake; the
/// resulting on-disk state is identical either way. Re-running the handshake
/// against this state must not die at slot-create with "already exists" — it
/// must name the orphan so an operator can drop and retry.
#[tokio::test]
async fn initial_snapshot_handshake_reports_an_orphaned_slot_instead_of_retrying_blind() {
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

    match publication::initial_snapshot_handshake(
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
async fn an_invalidated_slot_with_prior_confirmed_progress_is_a_loud_startup_error() {
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
}
