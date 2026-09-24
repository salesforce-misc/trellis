//! Issue #310: losing the replication slot (invalidated by the retention cap,
//! gone after a pre-PG-17 failover, or absent after the source database was
//! restored from a backup) pauses every transform the slot fed instead of
//! refusing to start.
//!
//! - the whole path through the facade: a restart against a lost slot comes
//!   up, pauses the fed transforms, recreates the slot, and a `RESUME`
//!   rebuilds the target by a fresh backfill that includes the changes the
//!   lost slot never delivered
//!   (`a_lost_slot_pauses_fed_transforms_and_resume_rebuilds_them`)
//! - which transforms count as "fed by the slot": everything sourced from a
//!   published table, directly or through a chain of targets, and nothing else
//!   (`fed_transforms_include_chained_targets_and_exclude_unpublished_sources`)
//! - the recovery called directly: prior freezes are left alone and not
//!   recorded, only orphaned backfill markers are discarded, a second pass over
//!   the same loss is a no-op, and a resume drops the record
//!   (`recovery_leaves_prior_freezes_alone_and_discards_only_orphaned_markers`)

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::intake::slot_loss;
use trellis::{Config, Trellis, TrellisOptions};

const SLOT: &str = "trellis_slot";

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

/// Polls `predicate` until it holds or `timeout` elapses, then panics with
/// `message`.
async fn poll_until<F>(timeout: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {timeout:?}: {message}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

async fn live_pipeline(dsn: &str) -> Result<Trellis, trellis::TrellisError> {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
}

async fn persisted_status(raw: &Client, target: &str) -> Option<String> {
    raw.query_opt(
        "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read status")
    .map(|row| row.get(0))
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect("count query").get(0)
}

async fn seed_source(raw: &Client, table: &str, rows: i64) {
    raw.batch_execute(&format!(
        "create table {table} (id bigint primary key, g bigint, a numeric); \
         alter table {table} replica identity full; \
         insert into {table} (id, g, a) select s, s % 2, s from generate_series(1, {rows}) s;"
    ))
    .await
    .expect("seed source table");
}

async fn rollup_total(raw: &Client, target: &str) -> i64 {
    count(
        raw,
        &format!("select coalesce(sum(total), 0)::bigint from {DEFAULT_TARGET_SCHEMA}.{target}"),
    )
    .await
}

/// The DR scenario the issue is about. Trellis ran against a slot, stopped,
/// and while it was down the slot disappeared and the source kept taking
/// writes that no stream will ever deliver. The restart must:
///
/// 1. come up rather than refuse to start (the old behaviour was a hard
///    `SlotLost` error out of `Trellis::connect`);
/// 2. pause every transform the slot fed, with the same `paused` status an
///    operator's `PAUSE` writes, and remember why;
/// 3. not resume anything on its own;
/// 4. leave intake running on a recreated slot, so a later `RESUME` gets the
///    normal fresh-backfill semantics: the rebuilt target includes both the
///    rows written while the slot was gone and the rows written afterwards.
#[tokio::test]
async fn a_lost_slot_pauses_fed_transforms_and_resume_rebuilds_them() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define the transform under test");
    definer
        .apply("TRANSFORM order_echo FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define a sibling over the same source");
    // Registration only records the two aggregates (#419); run their
    // direct-build jobs so both targets hold their pre-loss value.
    trellis::intake::publication::settle_registrations(&db.pool).await;
    definer.shutdown().await.expect("shut the definer down");

    // First run: creates the slot and confirms work against it, so a
    // `replication_progress` row exists — the precondition for "lost".
    let running = live_pipeline(db.dsn())
        .await
        .expect("start the live pipeline");
    running.shutdown().await.expect("stop the pipeline");

    // The slot goes away while Trellis is down. The walsender can linger for a
    // moment after the client disconnects, so wait for the slot to go
    // inactive before dropping it.
    poll_until(
        Duration::from_secs(30),
        "the slot must go inactive once the pipeline stops",
        async || {
            count(
                &raw,
                &format!(
                    "select count(*) from pg_replication_slots \
                     where slot_name = '{SLOT}' and active"
                ),
            )
            .await
                == 0
        },
    )
    .await;
    raw.execute("select pg_drop_replication_slot($1)", &[&SLOT])
        .await
        .expect("drop the slot out from under the stopped pipeline");
    let lost_lsn: String = raw
        .query_one(
            "select confirmed_lsn::text from replication_progress where slot_name = $1",
            &[&SLOT],
        )
        .await
        .expect("read the last confirmed position")
        .get(0);

    // Writes no stream will ever deliver.
    raw.batch_execute(
        "insert into orders (id, g, a) select s, s % 2, s from generate_series(5, 8) s",
    )
    .await
    .expect("write to the source while the slot is gone");

    // Claim 1: the restart comes up.
    let running = live_pipeline(db.dsn())
        .await
        .expect("a lost slot must pause its transforms, not refuse to start");

    // Claim 2: both fed transforms are paused, and the reason is recorded.
    for target in ["order_rollup", "order_echo"] {
        assert_eq!(
            persisted_status(&raw, target).await.as_deref(),
            Some("paused"),
            "{target} was fed by the lost slot and must be paused"
        );
    }
    let recorded = slot_loss::slot_loss_paused_transforms(&raw)
        .await
        .expect("read the slot-loss pause record");
    let names: Vec<&str> = recorded.iter().map(|p| p.transform.as_str()).collect();
    assert_eq!(names, vec!["order_rollup", "order_echo"]);
    assert!(
        recorded
            .iter()
            .all(|p| p.slot == SLOT && p.lost_confirmed_lsn.to_string() == lost_lsn),
        "each record names the lost slot and its last confirmed position: {recorded:?}"
    );

    // The slot is back and the durable watermark moved to its start, so
    // intake streams from the new slot rather than failing again.
    assert_eq!(
        count(
            &raw,
            &format!(
                "select count(*) from pg_replication_slots \
                 where slot_name = '{SLOT}' and database = current_database() \
                 and wal_status <> 'lost'"
            ),
        )
        .await,
        1,
        "recovery recreates the slot"
    );

    // Writes after the restart flow through the new slot.
    raw.batch_execute(
        "insert into orders (id, g, a) select s, s % 2, s from generate_series(9, 12) s",
    )
    .await
    .expect("write to the source after the restart");

    // Claim 3: nothing resumes on its own. Both targets still hold their
    // pre-loss value, since a paused target is not written to.
    assert_eq!(
        rollup_total(&raw, "order_rollup").await,
        (1..=4).sum::<i64>()
    );
    assert_eq!(rollup_total(&raw, "order_echo").await, (1..=4).sum::<i64>());

    // Claim 4: an operator's resume rebuilds from current source data,
    // including the rows the lost slot never delivered.
    running
        .apply("RESUME TRANSFORM order_rollup")
        .await
        .expect("resume one of the paused transforms");
    let expected_total: i64 = (1..=12).sum();
    poll_until(
        Duration::from_secs(60),
        "a resumed target must be rebuilt from current source data",
        async || rollup_total(&raw, "order_rollup").await == expected_total,
    )
    .await;

    // Resuming one does not resume the other, and the record now names only
    // the one still waiting on the operator.
    assert_eq!(
        persisted_status(&raw, "order_echo").await.as_deref(),
        Some("paused")
    );
    let recorded = slot_loss::slot_loss_paused_transforms(&raw)
        .await
        .expect("read the slot-loss pause record");
    let names: Vec<&str> = recorded.iter().map(|p| p.transform.as_str()).collect();
    assert_eq!(names, vec!["order_echo"]);

    running.shutdown().await.expect("shut the pipeline down");
}

/// "Every transform sourced (directly or transitively) from the slot's
/// publication": a transform over a published table, a transform chained off
/// that one's target, and not a transform over a table the publication
/// doesn't carry.
#[tokio::test]
async fn fed_transforms_include_chained_targets_and_exclude_unpublished_sources() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;
    seed_source(&raw, "refunds", 4).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define a transform over the published table");
    // A transform chains only off a live target; the aggregate's direct-build
    // job takes it there (#419).
    trellis::intake::publication::settle_registrations(&db.pool).await;
    definer
        .apply(&format!(
            "TRANSFORM rollup_copy FROM {DEFAULT_TARGET_SCHEMA}.order_rollup \
             SELECT total AS total"
        ))
        .await
        .expect("define a transform chained off the first one's target");
    definer
        .apply("TRANSFORM refund_rollup FROM refunds GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define a transform over an unpublished table");
    definer.shutdown().await.expect("shut the definer down");

    raw.batch_execute("create publication only_orders for table orders")
        .await
        .expect("create a publication carrying only orders");

    let fed = slot_loss::transforms_fed_by_publication(&raw, "only_orders")
        .await
        .expect("enumerate the transforms the publication feeds");
    let names: Vec<&str> = fed.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["order_rollup", "rollup_copy"]);
}

/// `pause_if_slot_lost` run directly against a slot that was never created,
/// covering what the facade test above can't observe:
///
/// - a transform that was already frozen before the loss is left as it was
///   and is not recorded as a slot-loss pause, so resuming it later is not
///   attributed to the loss;
/// - a pending backfill marker is discarded only when its table has nothing
///   unfrozen left to feed (a marker on an unpublished table whose transform
///   keeps running survives);
/// - a second pass over the same loss (the state a crash between the pauses
///   and the slot's recreation leaves) pauses nothing new and keeps the
///   original record;
/// - `RESUME TRANSFORM` deletes the record itself, so a later operator
///   `PAUSE` of the same transform is not reported as a slot-loss pause.
#[tokio::test]
async fn recovery_leaves_prior_freezes_alone_and_discards_only_orphaned_markers() {
    const LOST: &str = "lost_slot";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;
    seed_source(&raw, "refunds", 4).await;

    let definer = define_only(db.dsn()).await;
    for statement in [
        "TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total",
        "TRANSFORM order_echo FROM orders GROUP BY g SELECT sum(a) AS total",
        "TRANSFORM refund_rollup FROM refunds GROUP BY g SELECT sum(a) AS total",
    ] {
        definer.apply(statement).await.expect(statement);
    }
    // Registration only records the aggregates (#419); their direct-build
    // jobs take all three live, so the one paused below is a prior freeze of
    // a live transform and the other two are what the recovery sees running.
    trellis::intake::publication::settle_registrations(&db.pool).await;
    definer
        .apply("PAUSE TRANSFORM order_echo")
        .await
        .expect("pause order_echo before the loss");

    // Replace the go-live catch-up markers the aggregate builds parked
    // (issue #430) with the two this test stages.
    raw.batch_execute(&format!(
        "create publication only_orders for table orders; \
         insert into replication_progress (slot_name, confirmed_lsn) values ('{LOST}', '0/10'); \
         delete from pending_backfill; \
         insert into pending_backfill (table_name, fence_snapshot) values \
             ('{DEFAULT_SCHEMA}.orders', pg_current_snapshot()), \
             ('{DEFAULT_SCHEMA}.refunds', pg_current_snapshot());"
    ))
    .await
    .expect("stage a lost slot with pending markers on both tables");

    let mut session = trellis::staging::session::ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("open a producer session");
    let recovery = slot_loss::pause_if_slot_lost(&mut session, &db.pool, LOST, "only_orders")
        .await
        .expect("recover from the lost slot")
        .expect("a slot that doesn't exist is lost");
    assert_eq!(recovery.paused, vec!["order_rollup"]);
    assert_eq!(recovery.already_frozen, vec!["order_echo"]);
    assert_eq!(
        persisted_status(&raw, "refund_rollup").await.as_deref(),
        Some("live"),
        "a transform over an unpublished table was never fed by the slot"
    );
    let markers: Vec<String> = raw
        .query("select table_name from pending_backfill order by 1", &[])
        .await
        .expect("read markers")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        markers,
        vec![format!("{DEFAULT_SCHEMA}.refunds")],
        "only the marker whose table has nothing unfrozen left is discarded"
    );
    let recorded = |pauses: Vec<slot_loss::SlotLossPause>| -> Vec<(String, String)> {
        pauses
            .into_iter()
            .map(|p| (p.transform, p.lost_confirmed_lsn.to_string()))
            .collect()
    };
    let expected = vec![("order_rollup".to_string(), "0/10".to_string())];
    assert_eq!(
        recorded(slot_loss::slot_loss_paused_transforms(&raw).await.unwrap()),
        expected,
        "the transform paused before the loss is not recorded as paused by it"
    );

    // The same loss seen again: roll the watermark back and drop the new
    // slot, which is where a crash after the pauses but before the recreated
    // slot committed would leave things.
    raw.batch_execute(&format!(
        "update replication_progress set confirmed_lsn = '0/10' where slot_name = '{LOST}'; \
         select pg_drop_replication_slot('{LOST}');"
    ))
    .await
    .expect("lose the slot again");
    let again = slot_loss::pause_if_slot_lost(&mut session, &db.pool, LOST, "only_orders")
        .await
        .expect("recover again")
        .expect("still lost");
    assert!(again.paused.is_empty(), "nothing new to pause: {again:?}");
    assert_eq!(again.already_frozen, vec!["order_rollup", "order_echo"]);
    assert_eq!(
        recorded(slot_loss::slot_loss_paused_transforms(&raw).await.unwrap()),
        expected
    );
    assert!(
        slot_loss::pause_if_slot_lost(&mut session, &db.pool, LOST, "only_orders")
            .await
            .expect("check the recreated slot")
            .is_none(),
        "the recreated slot is healthy"
    );

    // Resume, then an ordinary operator pause: the record went with the
    // resume, so the new pause is not reported as a slot-loss one.
    for statement in [
        "RESUME TRANSFORM order_rollup",
        "PAUSE TRANSFORM order_rollup",
    ] {
        definer.apply(statement).await.expect(statement);
    }
    assert!(
        slot_loss::slot_loss_paused_transforms(&raw)
            .await
            .unwrap()
            .is_empty()
    );

    drop(session);
    definer.shutdown().await.expect("shut the definer down");
    raw.execute("select pg_drop_replication_slot($1)", &[&LOST])
        .await
        .expect("clean up the recreated slot");
}

/// Issue #406: the slot is dropped while Trellis is down, the source takes a
/// write nothing will ever stream, and then something recreates the slot
/// under the same name before Trellis restarts. The slot is present and not
/// `lost`, so a check that only asks "does it exist?" calls it healthy and
/// the transform silently misses the write. The recreated slot starts past
/// the position this instance last confirmed, which a slot it has been
/// acknowledging never does, so it is recovered as a loss: the fed transform
/// is paused and recorded, and the slot and watermark end up realigned.
#[tokio::test]
async fn a_slot_recreated_under_the_same_name_is_recovered_as_lost() {
    const REBORN: &str = "reborn_slot";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define the transform under test");
    definer.shutdown().await.expect("shut the definer down");

    raw.batch_execute("create publication only_orders for table orders")
        .await
        .expect("create the publication");
    let created: tokio_postgres::types::PgLsn = raw
        .query_one(
            "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&REBORN],
        )
        .await
        .expect("create the slot")
        .get(0);
    raw.execute(
        "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
        &[&REBORN, &created],
    )
    .await
    .expect("seed progress at the slot's start, as the handshake does");

    let mut session = trellis::staging::session::ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("open a producer session");
    assert!(
        slot_loss::pause_if_slot_lost(&mut session, &db.pool, REBORN, "only_orders")
            .await
            .expect("check the original slot")
            .is_none(),
        "a slot sitting exactly at the confirmed position is healthy"
    );

    raw.execute("select pg_drop_replication_slot($1)", &[&REBORN])
        .await
        .expect("drop the slot");
    raw.batch_execute(
        "insert into orders (id, g, a) select s, s % 2, s from generate_series(5, 8) s",
    )
    .await
    .expect("write to the source while the slot is gone");
    raw.query_one(
        "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
        &[&REBORN],
    )
    .await
    .expect("recreate the slot under the same name");

    let recovery = slot_loss::pause_if_slot_lost(&mut session, &db.pool, REBORN, "only_orders")
        .await
        .expect("recover from the recreated slot")
        .expect("a slot recreated past the confirmed position has lost the writes in between");
    assert_eq!(recovery.paused, vec!["order_rollup"]);
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("paused")
    );
    let recorded = slot_loss::slot_loss_paused_transforms(&raw)
        .await
        .expect("read the slot-loss pause record");
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].lost_confirmed_lsn, created);

    assert!(
        slot_loss::pause_if_slot_lost(&mut session, &db.pool, REBORN, "only_orders")
            .await
            .expect("check the slot recovery made")
            .is_none(),
        "recovery leaves the slot and the watermark aligned, so the next check passes"
    );

    drop(session);
    raw.execute("select pg_drop_replication_slot($1)", &[&REBORN])
        .await
        .expect("clean up the slot");
}
