//! A paused definition's message: every reason it is paused for, and what
//! `RESUME` would do now, kept current by the staging worker's capture
//! pass when a relation it reads changes (`staging::pause_message`).
//!
//! Every test is stepped by hand: schema changes from a separate session,
//! then explicit capture passes. Nothing polls for convergence.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{CaptureFailure, Config, Trellis, TrellisOptions};

const SCHEMA: &str = "trellis";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

async fn full_pass(raw: &mut Client, pool: &trellis::pool::Pool) {
    trellis::client::reconcile_pass(raw, pool, SCHEMA)
        .await
        .expect("reconcile pass");
}

/// The capture half of one pass only, which pauses and refreshes.
async fn capture_pass(raw: &mut Client, pool: &trellis::pool::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn drain_to_quiescence(pool: &trellis::pool::Pool, client: &mut Client) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1_waiting(client).await;
        seal::seal_phase2(client, outcome.sealed_seg_seq, "paused_message_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "paused_message_test",
            1,
            "trellis_paused_message_test",
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("the ring did not reach quiescence within 16 seal/drain rounds");
}

async fn stored_status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    trellis::defs::StoredStatus::from_persisted(&text)
        .expect("a stored status")
        .into()
}

/// Alternates staging-worker passes, builds and drains until `target` is
/// live: a bounded number of explicit steps, not a timed wait.
async fn bring_live(raw: &mut Client, pool: &trellis::pool::Pool, target: &str) {
    for _ in 0..8 {
        full_pass(raw, pool).await;
        trellis::staging::build::settle_builds(pool).await;
        drain_to_quiescence(pool, raw).await;
        if stored_status(raw, target).await == TransformStatus::Live {
            return;
        }
    }
    panic!("{target} did not go live within 8 pass/drain rounds");
}

/// `target`'s reported status and its `capture_failure`.
async fn paused(trellis: &Trellis, target: &str) -> CaptureFailure {
    let reported = trellis.status(target).await.expect("status").expect(target);
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target} reports why it is paused"))
}

async fn id_of(raw: &Client, target: &str) -> i64 {
    raw.query_one(
        "select id from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read id")
    .get(0)
}

async fn database(raw: &Client) -> String {
    raw.query_one("select current_database()::text", &[])
        .await
        .expect("current database")
        .get(0)
}

/// `public.u` with one 1-1 reader `t` (`a`, `b`) and one aggregate `s`
/// (`SUM(b)` by `g`), both live.
async fn setup(dsn: &str, raw: &mut Client, pool: &trellis::pool::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.u (id int primary key, g int not null, a int, b int); \
         insert into public.u select i, i % 2, i, 10 * i from generate_series(1, 4) i;",
    )
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    for text in [
        "TRANSFORM t FROM public.u SELECT a AS a, b AS b",
        "TRANSFORM s FROM public.u GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(b) AS total",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, "t").await;
    bring_live(raw, pool, "s").await;
    trellis
}

/// A restore pauses `t`, and a column it reads is renamed while it stays
/// paused: the next pass adds that reason, and both show, the newest
/// first, with what `RESUME` would do. With the column back, the restore's
/// reason is the one left, and `RESUME` resets the ordering state the
/// restore recorded before it rebuilds.
#[tokio::test]
async fn two_reasons_show_and_resume_honours_the_restore_that_came_first() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    // A dump and restore, as `capture_disruption.rs` shows one.
    app.batch_execute(
        "update capture_triggers set catalog_oid = (catalog_oid::bigint + 1000)::oid \
         where table_name = 'public.u'",
    )
    .await
    .expect("look restored");
    capture_pass(&mut raw, &db.pool).await;
    let restored = paused(&trellis, "t").await;
    assert_eq!(restored.reasons.len(), 1, "{restored:?}");
    assert!(
        restored.error.contains("restored from a logical dump"),
        "{restored:?}"
    );

    app.batch_execute("alter table public.u rename column b to b2")
        .await
        .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    let both = paused(&trellis, "t").await;
    assert_eq!(both.reasons.len(), 2, "{both:?}");
    assert!(both.reasons[0].contains("\"b\"") && both.reasons[0].contains("renamed or dropped"));
    assert!(both.reasons[1].contains("restored from a logical dump"));
    assert!(
        both.error.starts_with(&both.reasons[0])
            && both
                .error
                .contains(" It was already paused: the database was restored"),
        "{both:?}"
    );
    let outcome = both
        .resume_outcome
        .clone()
        .expect("the pass worked out the outcome");
    assert!(outcome.starts_with("RESUME would refuse: "), "{outcome}");
    assert!(both.error.ends_with(&outcome), "{both:?}");

    app.batch_execute("alter table public.u rename column b2 to b")
        .await
        .expect("rename back");
    capture_pass(&mut raw, &db.pool).await;
    let left = paused(&trellis, "t").await;
    assert_eq!(left.reasons.len(), 1, "{left:?}");
    assert!(left.reasons[0].contains("restored from a logical dump"));
    let outcome = left.resume_outcome.expect("outcome");
    assert!(
        outcome.contains("reset the ordering state") && outcome.contains("rebuild public.t"),
        "{outcome}"
    );
    let restored_flag: bool = raw
        .query_one(
            "select restored from capture_failures where transform_id = $1",
            &[&id_of(&raw, "t").await],
        )
        .await
        .expect("read the record")
        .get(0);
    assert!(restored_flag, "the second reason kept the restore's state");

    trellis.apply("RESUME TRANSFORM t").await.expect("resume");
    bring_live(&mut raw, &db.pool, "t").await;
    let rows: Vec<(i32, i32)> = raw
        .query("select a, b from public.t order by a", &[])
        .await
        .expect("read t")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30), (4, 40)]);
}

/// The source's primary key is redefined, pausing its readers, then
/// redefined again while they stay paused: the next pass rewrites the
/// reason to name the key as it is now. Put back, the reason leaves, the
/// message says the cause is gone, and the definition stays paused.
#[tokio::test]
async fn a_key_changed_again_while_paused_rewrites_the_message() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id, g)")
        .await
        .expect("redefine the key");
    capture_pass(&mut raw, &db.pool).await;
    let first = paused(&trellis, "s").await;
    assert!(first.error.contains("to (\"id\", \"g\")"), "{first:?}");
    assert_eq!(first.resume_outcome, None, "worked out by the next pass");
    capture_pass(&mut raw, &db.pool).await;
    let evaluated = paused(&trellis, "s").await;
    assert!(evaluated.resume_outcome.is_some(), "{evaluated:?}");
    assert!(
        evaluated.error.contains("to (\"id\", \"g\")"),
        "{evaluated:?}"
    );

    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (g, id)")
        .await
        .expect("redefine the key again");
    capture_pass(&mut raw, &db.pool).await;
    let again = paused(&trellis, "s").await;
    assert_eq!(again.reasons.len(), 1, "{again:?}");
    assert!(
        again
            .error
            .contains("changed from (\"id\") to (\"g\", \"id\")"),
        "{again:?}"
    );
    assert_eq!(again.columns, vec!["id".to_string()]);

    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id)")
        .await
        .expect("put the key back");
    capture_pass(&mut raw, &db.pool).await;
    let gone = paused(&trellis, "s").await;
    assert!(gone.reasons.is_empty(), "{gone:?}");
    assert!(
        gone.error
            .starts_with("the cause it was paused for is gone. RESUME would "),
        "{gone:?}"
    );
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(stored_status(&raw, "s").await, TransformStatus::Paused);
    assert_eq!(stored_status(&raw, "t").await, TransformStatus::Paused);
}

/// A read column renamed pauses its reader; renamed back while it stays
/// paused, the reason leaves the message, which says `RESUME` would
/// rebuild it, and it stays paused until `RESUME`.
#[tokio::test]
async fn a_cause_fixed_while_paused_leaves_the_message_and_the_pause() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.u rename column a to a2")
        .await
        .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let missing = paused(&trellis, "t").await;
    assert_eq!(missing.columns, vec!["a".to_string()], "{missing:?}");
    assert!(
        missing
            .resume_outcome
            .as_deref()
            .is_some_and(|o| o.starts_with("RESUME would refuse: ")),
        "{missing:?}"
    );

    app.batch_execute("alter table public.u rename column a2 to a")
        .await
        .expect("rename back");
    capture_pass(&mut raw, &db.pool).await;
    let fixed = paused(&trellis, "t").await;
    assert!(fixed.reasons.is_empty(), "{fixed:?}");
    assert!(fixed.columns.is_empty(), "{fixed:?}");
    assert_eq!(
        fixed.error,
        "the cause it was paused for is gone. RESUME would rebuild public.t from public.u"
    );
    for _ in 0..2 {
        full_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(stored_status(&raw, "t").await, TransformStatus::Paused);

    trellis.apply("RESUME TRANSFORM t").await.expect("resume");
    bring_live(&mut raw, &db.pool, "t").await;
}

/// A paused definition whose relations don't change is evaluated once,
/// by the first pass after its pause, and never again; a change to a
/// relation it reads has the next pass evaluate it again, and a change to
/// one it doesn't read doesn't.
#[tokio::test]
async fn an_unchanged_paused_definition_is_not_evaluated_again() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    app.batch_execute("create table public.other (id int primary key)")
        .await
        .expect("an unrelated table");
    let id = id_of(&raw, "t").await;
    let database = database(&raw).await;
    let refreshes = || trellis::staging::pause_message_refreshes(&database, SCHEMA, id);

    app.batch_execute("alter table public.u rename column a to a2")
        .await
        .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    paused(&trellis, "t").await;
    let before = refreshes();
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        refreshes(),
        before + 1,
        "the first pass after the pause evaluates it"
    );
    for _ in 0..3 {
        capture_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(refreshes(), before + 1, "nothing it reads changed");

    app.batch_execute("alter table public.other add column x int")
        .await
        .expect("change another table");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(refreshes(), before + 1, "a table it doesn't read changed");

    app.batch_execute("alter table public.u add column spare int")
        .await
        .expect("change its source");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(refreshes(), before + 2, "its source changed");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(refreshes(), before + 2);
    assert!(paused(&trellis, "t").await.resume_outcome.is_some());
}
