//! The staging worker's capture reconcile pass (#622 C5): a registration
//! makes the next pass install a table's capture triggers, and every later
//! change to the catalog widens, narrows or uninstalls them, in the
//! background, never on `apply`'s path.
//!
//! Most tests step the pass by hand (`trellis::capture::reconcile::reconcile`
//! for the capture half, `trellis::client::reconcile_pass` for the whole pass
//! with its discharge) and hold a table's lock with a transaction left open,
//! with no convergence polling (#297). Two drive a running client end to
//! end: A3 (a join and a drop wait out a writer that stays open, and no other
//! writer queues behind them) and an `ON DELETE CASCADE` parent/child pair,
//! whose capture order within one transaction is the reverse of the WAL's.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::install::{Installed, LockingOperation, installed};
use trellis::capture::reconcile::{self, PassOutcome};
use trellis::defs::TransformStatus;
use trellis::locks::USER_TABLE_DDL_LOCK_TIMEOUT;
use trellis::{ClientOptions, Config, Trellis, TrellisOptions};

const SCHEMA: &str = "trellis";

/// Well above `USER_TABLE_DDL_LOCK_TIMEOUT` (50 ms) for a loaded box, and far
/// below how long the lock tests hold a table, which is what a writer queued
/// behind a DDL waiting out the whole hold would see (as in
/// `tests/capture_install.rs`).
const SLACK: Duration = Duration::from_millis(750);

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

/// The capture half of one pass, over the tables the catalog says to
/// capture, with `budget` for locked tables.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool, budget: Duration) -> PassOutcome {
    let desired = trellis::defs::publication_tables(pool)
        .await
        .expect("read the tables to capture");
    reconcile::reconcile(raw, SCHEMA, &desired, Instant::now() + budget)
        .await
        .expect("capture pass")
}

/// One whole staging-worker pass: capture, markers, discharge.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(
        raw,
        pool,
        SCHEMA,
        "capture_join_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

/// The columns `table`'s installed capture images, or `None` when nothing
/// is installed.
async fn captured_columns(raw: &Client, table: &str) -> Option<Vec<String>> {
    match installed(raw, SCHEMA, table)
        .await
        .expect("read the capture")
    {
        Installed::Complete { spec, current } => {
            assert!(current, "a pass installs current functions");
            Some(spec.columns().to_vec())
        }
        Installed::Absent => None,
        partial => panic!("a pass leaves no partial install: {partial:?}"),
    }
}

async fn definition_id(raw: &Client, target: &str) -> i64 {
    raw.query_one(
        "select id from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read the definition")
    .get(0)
}

async fn status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    TransformStatus::from_persisted(&text).expect("known status")
}

/// `(capture_gate_lsn is not null)` of `table`'s pending marker, or `None`
/// when it has none.
async fn marker_gated(raw: &Client, table: &str) -> Option<bool> {
    raw.query_opt(
        "select capture_gate_lsn is not null from pending_backfill where table_name = $1",
        &[&table],
    )
    .await
    .expect("read the marker")
    .map(|row| row.get(0))
}

/// Runs whole passes until `table` has no marker left: its join marker's
/// discharge dispatches the build, and a build that finishes at once parks a
/// go-live catch-up (#476), which the next pass discharges. Nothing drains
/// here, so a pass only dispatches; a few passes always suffice.
async fn settle(raw: &mut Client, pool: &trellis::Pool, table: &str) {
    for _ in 0..5 {
        full_pass(raw, pool).await;
        if marker_gated(raw, table).await.is_none() {
            return;
        }
    }
    panic!("{table}'s markers never discharged");
}

/// A transaction left open holding `ROW EXCLUSIVE` on `table`, as a writer
/// mid-transaction does, without writing anything the ring would stage.
async fn hold_table(dsn: &str, table: &str) -> Client {
    let holder = connect(dsn).await;
    holder
        .batch_execute(&format!("begin; lock table {table} in row exclusive mode"))
        .await
        .expect("hold the table");
    holder
}

async fn backend_pid(client: &Client) -> i32 {
    client
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0)
}

/// A registration installs nothing at `apply`; the next pass installs the
/// table's triggers, imaging the key and the column the definition reads,
/// and parks the join marker (with its capture gate) in the same
/// transaction. The whole pass then dispatches the definition.
#[tokio::test]
async fn a_registration_makes_the_next_pass_install_capture_with_its_join_marker() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.u (id int primary key, a int, b int); \
         insert into public.u select g, g, g from generate_series(1, 3) g;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM tu FROM public.u SELECT a AS a")
        .await
        .expect("define");
    assert_eq!(
        captured_columns(&raw, "public.u").await,
        None,
        "apply only registers: the install is the staging worker's, in the background"
    );

    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_secs(1)).await;
    assert!(outcome.waiting.is_empty() && outcome.failed.is_empty());
    assert_eq!(
        captured_columns(&raw, "public.u").await,
        Some(vec!["a".to_string(), "id".to_string()])
    );
    assert_eq!(
        marker_gated(&raw, "public.u").await,
        Some(true),
        "the install's join marker commits with the triggers, gated"
    );
    assert_eq!(outcome.ready, vec![definition_id(&raw, "tu").await]);

    full_pass(&mut raw, &db.pool).await;
    assert_ne!(
        status(&raw, "tu").await,
        TransformStatus::WaitingToBackfill,
        "the pass dispatched the definition"
    );
}

/// Dropping a table's last reader uninstalls its capture on the next pass;
/// dropping one of two readers only narrows it to what the other reads.
#[tokio::test]
async fn dropping_readers_narrows_then_uninstalls_capture() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute("create table public.u (id int primary key, a int, b int)")
        .await
        .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM ta FROM public.u SELECT a AS a")
        .await
        .expect("define ta");
    trellis
        .apply("TRANSFORM tb FROM public.u SELECT b AS b")
        .await
        .expect("define tb");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(
        captured_columns(&raw, "public.u").await,
        Some(vec!["a".to_string(), "b".to_string(), "id".to_string()])
    );

    for statement in ["PAUSE TRANSFORM tb", "DROP TRANSFORM tb"] {
        trellis.apply(statement).await.expect(statement);
    }
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(
        captured_columns(&raw, "public.u").await,
        Some(vec!["a".to_string(), "id".to_string()]),
        "the pass narrows to what the remaining reader needs"
    );

    for statement in ["PAUSE TRANSFORM ta", "DROP TRANSFORM ta"] {
        trellis.apply(statement).await.expect(statement);
    }
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(captured_columns(&raw, "public.u").await, None);
    let triggers: i64 = raw
        .query_one(
            "select count(*) from pg_trigger where tgrelid = 'public.u'::regclass \
             and not tgisinternal",
            &[],
        )
        .await
        .expect("count triggers")
        .get(0);
    assert_eq!(triggers, 0, "nothing of the capture is left on the table");
}

/// A second definition that reads a column the triggers don't image yet
/// widens them, and the widen parks a gated marker for it.
#[tokio::test]
async fn a_second_definition_reading_a_new_column_widens_capture() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute("create table public.u (id int primary key, a int, b int)")
        .await
        .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM ta FROM public.u SELECT a AS a")
        .await
        .expect("define ta");
    settle(&mut raw, &db.pool, "public.u").await;

    trellis
        .apply("TRANSFORM tb FROM public.u SELECT b AS b")
        .await
        .expect("define tb");
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_secs(1)).await;
    assert_eq!(
        captured_columns(&raw, "public.u").await,
        Some(vec!["a".to_string(), "b".to_string(), "id".to_string()])
    );
    assert_eq!(
        marker_gated(&raw, "public.u").await,
        Some(true),
        "the widen parks a marker gated on the rows the old function staged"
    );
    assert_eq!(outcome.ready, vec![definition_id(&raw, "tb").await]);
}

/// A table whose join is blocked by a writer that stays open doesn't hold
/// back another table's join in the same pass. The blocked one reports who
/// holds it, on the waiting definition's status too (#622 plan Q5), and the
/// pass after the writer ends installs it. Nothing is cancelled.
#[tokio::test]
async fn a_table_whose_join_is_blocked_does_not_delay_another_tables_join() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.u (id int primary key, a int); \
         create table public.v (id int primary key, a int);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM tu FROM public.u SELECT a AS a")
        .await
        .expect("define tu");
    trellis
        .apply("TRANSFORM tv FROM public.v SELECT a AS a")
        .await
        .expect("define tv");

    let holder = hold_table(db.dsn(), "public.u").await;
    let holder_pid = backend_pid(&holder).await;
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_millis(300)).await;

    assert!(outcome.captured.contains("public.v"), "{outcome:?}");
    assert!(!outcome.captured.contains("public.u"), "{outcome:?}");
    assert_eq!(outcome.ready, vec![definition_id(&raw, "tv").await]);
    assert_eq!(outcome.waiting.len(), 1, "{outcome:?}");
    let wait = &outcome.waiting[0];
    assert_eq!(wait.table, "public.u");
    assert_eq!(wait.operation, LockingOperation::Install);
    assert!(
        wait.blockers
            .iter()
            .any(|b| b.pid == Some(holder_pid) && b.granted),
        "the report names the writer holding the table: {wait}"
    );
    assert_eq!(captured_columns(&raw, "public.u").await, None);
    assert_eq!(
        marker_gated(&raw, "public.u").await,
        None,
        "a waiting install changed nothing"
    );

    let status = trellis
        .status("tu")
        .await
        .expect("status")
        .expect("tu is registered");
    assert_eq!(status.status, TransformStatus::WaitingToBackfill);
    let reported = status
        .capture_wait
        .expect("the waiting definition's status names what it waits on");
    assert_eq!(reported.table, "public.u");
    assert_eq!(reported.operation, "install");
    assert!(
        reported
            .blockers
            .iter()
            .any(|line| line.contains(&format!("pid {holder_pid}"))),
        "{reported:?}"
    );
    let other = trellis.status("tv").await.expect("status").expect("tv");
    assert_eq!(other.capture_wait, None);

    holder.batch_execute("commit").await.expect("release u");
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_secs(1)).await;
    assert!(outcome.waiting.is_empty(), "{outcome:?}");
    assert!(captured_columns(&raw, "public.u").await.is_some());
    let status = trellis.status("tu").await.expect("status").expect("tu");
    assert_eq!(
        status.capture_wait, None,
        "a landed install clears the wait"
    );
}

/// The maintenance loop is the only sealer, so a pass may spend only its
/// budget on locked tables: each gets its first attempt, and no retry starts
/// that couldn't end by the deadline.
#[tokio::test]
async fn a_pass_respects_its_budget_on_locked_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.u (id int primary key, a int); \
         create table public.v (id int primary key, a int);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for statement in [
        "TRANSFORM tu FROM public.u SELECT a AS a",
        "TRANSFORM tv FROM public.v SELECT a AS a",
    ] {
        trellis.apply(statement).await.expect(statement);
    }
    let _hold_u = hold_table(db.dsn(), "public.u").await;
    let _hold_v = hold_table(db.dsn(), "public.v").await;

    let budget = Duration::from_millis(400);
    let started = Instant::now();
    let outcome = capture_pass(&mut raw, &db.pool, budget).await;
    let took = started.elapsed();

    assert_eq!(outcome.waiting.len(), 2, "{outcome:?}");
    assert!(outcome.ready.is_empty());
    // The budget, plus v's one attempt past it, plus the pass's own reads.
    let bound = budget + 2 * USER_TABLE_DDL_LOCK_TIMEOUT + SLACK;
    assert!(took <= bound, "the pass took {took:?}, over {bound:?}");
}

/// A definition that reads a new column of a relationship's to-side is not
/// dispatched until the to-side's widen has landed and its gated marker has
/// discharged, even though its own source's capture is current: C3's
/// documented gap ("the gate only holds definitions sourced from the gated
/// table"), closed by the pass's readiness rules.
#[tokio::test]
async fn a_reader_of_a_widened_to_side_waits_for_the_to_sides_gate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.p (id int primary key, name text, tier text); \
         create table public.c (id int primary key, pid int, amount int); \
         alter table public.p replica identity full; \
         alter table public.c replica identity full;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("relationship");
    trellis
        .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
        .await
        .expect("define c_named");
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_secs(1)).await;
    assert!(outcome.waiting.is_empty(), "{outcome:?}");
    let p_columns = captured_columns(&raw, "public.p")
        .await
        .expect("p captured");
    assert!(!p_columns.contains(&"tier".to_string()), "{p_columns:?}");
    // Discharge both join markers (nothing is written, so no gate holds).
    settle(&mut raw, &db.pool, "public.p").await;

    trellis
        .apply("TRANSFORM c_tiered FROM public.c SELECT amount AS amount, parent.tier AS tier")
        .await
        .expect("define c_tiered");
    let tiered = definition_id(&raw, "c_tiered").await;

    // p's widen can't take its lock: c's capture is current, but the reader
    // of p.tier isn't ready.
    let holder = hold_table(db.dsn(), "public.p").await;
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_millis(200)).await;
    assert!(outcome.captured.contains("public.c"));
    assert_eq!(outcome.waiting.len(), 1, "{outcome:?}");
    assert!(!outcome.ready.contains(&tiered), "{outcome:?}");
    holder.batch_execute("commit").await.expect("release p");

    // The widen lands with a gated marker on p: still not ready.
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_secs(1)).await;
    assert!(
        captured_columns(&raw, "public.p")
            .await
            .expect("p captured")
            .contains(&"tier".to_string())
    );
    assert_eq!(marker_gated(&raw, "public.p").await, Some(true));
    assert!(
        !outcome.ready.contains(&tiered),
        "held while p's gated marker is pending: {outcome:?}"
    );
    assert_eq!(
        status(&raw, "c_tiered").await,
        TransformStatus::WaitingToBackfill
    );

    // A whole pass discharges p's marker (nothing old is pending), and only
    // the pass after that finds the reader ready.
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(marker_gated(&raw, "public.p").await, None);
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_secs(1)).await;
    assert!(outcome.ready.contains(&tiered), "{outcome:?}");
}

/// A pass reads the tables to capture (`desired`) before it reads its
/// catalog snapshot, so a definition registered in between is in the
/// snapshot's waiting list while its source isn't in `desired` (#622 C5
/// review). Such a source must not read as seam-fed: nothing captures it
/// yet, so the definition waits for the next pass.
#[tokio::test]
async fn a_definition_registered_after_the_pass_read_its_tables_waits() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.u (id int primary key, a int); \
         create table public.v (id int primary key, a int);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM tu FROM public.u SELECT a AS a")
        .await
        .expect("define tu");
    let desired = trellis::defs::publication_tables(&db.pool)
        .await
        .expect("read the tables to capture");
    trellis
        .apply("TRANSFORM tv FROM public.v SELECT a AS a")
        .await
        .expect("define tv after the pass read its tables");

    let outcome = reconcile::reconcile(
        &mut raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert_eq!(captured_columns(&raw, "public.v").await, None);
    assert_eq!(
        outcome.ready,
        vec![definition_id(&raw, "tu").await],
        "tv's source isn't captured yet: {outcome:?}"
    );

    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_secs(1)).await;
    assert!(
        outcome.ready.contains(&definition_id(&raw, "tv").await),
        "the next pass captures v: {outcome:?}"
    );
}

/// The in-memory lock wait `status().capture_wait` reports goes once the
/// table stops waiting for its lock, including when its capture then fails
/// for another reason (#622 C5 review): a read column dropped here.
#[tokio::test]
async fn a_table_that_fails_after_waiting_no_longer_reports_the_wait() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute("create table public.u (id int primary key, a int)")
        .await
        .expect("seed");
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .expect("database")
        .get(0);
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM tu FROM public.u SELECT a AS a")
        .await
        .expect("define");

    let holder = hold_table(db.dsn(), "public.u").await;
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_millis(200)).await;
    assert_eq!(outcome.waiting.len(), 1, "{outcome:?}");
    assert!(reconcile::lock_wait(&database, SCHEMA, "public.u").is_some());
    holder
        .batch_execute("commit")
        .await
        .expect("end the holder");

    raw.batch_execute("alter table public.u drop column a")
        .await
        .expect("drop the read column");
    let outcome = capture_pass(&mut raw, &db.pool, Duration::from_secs(1)).await;
    assert_eq!(outcome.failed.len(), 1, "{outcome:?}");
    assert_eq!(
        reconcile::lock_wait(&database, SCHEMA, "public.u"),
        None,
        "the table fails now; it no longer waits for a lock"
    );
}

/// A partitioned table can't be captured by statement triggers on its
/// parent (a write aimed at a partition bypasses them, and a partition
/// attached later has none), so it is refused as a source at define time.
#[tokio::test]
async fn a_partitioned_source_is_refused_at_define_time() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.events (id int, at date, v int, primary key (id, at)) \
             partition by range (at); \
         create table public.events_2026 partition of public.events \
             for values from ('2026-01-01') to ('2027-01-01');",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    let err = trellis
        .apply("TRANSFORM ev FROM public.events SELECT v AS v")
        .await
        .expect_err("a partitioned source is refused");
    assert_eq!(err.code(), trellis::ErrorCode::Validation, "{err}");
    assert!(err.to_string().contains("not partitioned"), "{err}");
}

/// Options for a running client whose staging worker reconciles often, so an
/// end-to-end test doesn't wait out the 5 s default between passes.
fn quick_options() -> ClientOptions {
    ClientOptions {
        staging_worker: true,
        application_threads: 1,
        reconcile_interval: Duration::from_millis(200),
        maintenance_interval: Duration::from_millis(50),
        ..Default::default()
    }
}

/// Inserts into `public.u` from a separate connection until `stop`, one
/// autocommit row at a time, and returns the longest any insert took.
fn spawn_writer(
    dsn: &str,
    stop: Arc<AtomicBool>,
    first_id: i32,
) -> tokio::task::JoinHandle<Duration> {
    let dsn = dsn.to_string();
    tokio::spawn(async move {
        let writer = connect(&dsn).await;
        let mut longest = Duration::ZERO;
        let mut id = first_id;
        while !stop.load(Ordering::Relaxed) {
            let started = Instant::now();
            writer
                .execute("insert into public.u (id, a) values ($1, $1)", &[&id])
                .await
                .expect("insert");
            longest = longest.max(started.elapsed());
            id += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        longest
    })
}

/// Polls `check` every 50 ms until it holds or `within` runs out. Used only
/// by the two end-to-end tests, which run a real client.
async fn eventually<F, Fut>(within: Duration, what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    while !check().await {
        assert!(Instant::now() < deadline, "{what}: not within {within:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A3 end to end through reconcile: a join, then a drop, each while a
/// writer transaction on the table stays open. Other writers keep writing
/// the table meanwhile and none waits more than one attempt's lock timeout
/// (plus slack for a loaded box). The join lands once the holder ends, and
/// the definition converges; the drop's uninstall lands the same way.
#[tokio::test]
async fn a3_join_and_drop_wait_out_an_open_writer_without_stalling_other_writers() {
    const HOLD: Duration = Duration::from_secs(3);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.u (id int primary key, a int); \
         insert into public.u select g, g from generate_series(1, 100) g;",
    )
    .await
    .expect("seed");
    let client = trellis::Client::start(db.dsn(), quick_options()).expect("start the client");
    let trellis = definer(db.dsn()).await;

    // The join.
    let holder = hold_table(db.dsn(), "public.u").await;
    let stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer(db.dsn(), stop.clone(), 1_000);
    trellis
        .apply("TRANSFORM tu FROM public.u SELECT a AS a")
        .await
        .expect("define: apply never waits on the table");
    tokio::time::sleep(HOLD).await;
    assert_eq!(
        captured_columns(&raw, "public.u").await,
        None,
        "the join waits out the open writer"
    );
    let waiting = trellis.status("tu").await.expect("status").expect("tu");
    assert_eq!(waiting.status, TransformStatus::WaitingToBackfill);
    assert!(waiting.capture_wait.is_some(), "{waiting:?}");
    holder
        .batch_execute("commit")
        .await
        .expect("end the holder");
    eventually(Duration::from_secs(30), "tu goes live", || async {
        status(&raw, "tu").await == TransformStatus::Live
    })
    .await;
    stop.store(true, Ordering::Relaxed);
    let longest = writer.await.expect("writer");
    assert!(
        longest < SLACK,
        "a writer queued {longest:?} behind the join's attempts"
    );
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, Duration::from_secs(30))
        .await
        .expect("converge");
    let (source, target): (i64, i64) = {
        let row = raw
            .query_one(
                "select (select count(*) from public.u), (select count(*) from public.tu)",
                &[],
            )
            .await
            .expect("count");
        (row.get(0), row.get(1))
    };
    assert_eq!(
        source, target,
        "every row written during the join reached the target"
    );

    // The drop.
    let holder = hold_table(db.dsn(), "public.u").await;
    let stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer(db.dsn(), stop.clone(), 100_000);
    for statement in ["PAUSE TRANSFORM tu", "DROP TRANSFORM tu"] {
        trellis.apply(statement).await.expect(statement);
    }
    tokio::time::sleep(HOLD).await;
    assert!(
        captured_columns(&raw, "public.u").await.is_some(),
        "the uninstall waits out the open writer"
    );
    holder
        .batch_execute("commit")
        .await
        .expect("end the holder");
    eventually(
        Duration::from_secs(30),
        "the capture is uninstalled",
        // Read while the worker may be committing the uninstall: `installed`
        // reads every event in one statement, so it never sees half of it
        // (`captured_columns` fails on a partial install).
        || async { captured_columns(&raw, "public.u").await.is_none() },
    )
    .await;
    stop.store(true, Ordering::Relaxed);
    let longest = writer.await.expect("writer");
    assert!(
        longest < SLACK,
        "a writer queued {longest:?} behind the uninstall's attempts"
    );

    client.shutdown().await.expect("shutdown");
}

/// An `ON DELETE CASCADE` child's capture runs before its parent's statement
/// trigger, whereas the WAL put the parent's delete first (#622 plan Risks).
/// Per-key order is unaffected, and a parent/child pair read through a
/// relationship still converges.
#[tokio::test]
async fn an_on_delete_cascade_parent_child_pair_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.p (id int primary key, name text); \
         create table public.c (id int primary key, \
             pid int references public.p (id) on delete cascade, amount int); \
         alter table public.p replica identity full; \
         alter table public.c replica identity full; \
         insert into public.p select g, 'p' || g from generate_series(1, 10) g; \
         insert into public.c select g, 1 + g % 10, g from generate_series(1, 100) g;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("relationship");
    trellis
        .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
        .await
        .expect("define c_named");
    trellis
        .apply("TRANSFORM c_totals FROM public.c GROUP BY pid SELECT SUM(amount) AS total")
        .await
        .expect("define c_totals");
    let client = trellis::Client::start(db.dsn(), quick_options()).expect("start the client");
    for target in ["c_named", "c_totals"] {
        eventually(
            Duration::from_secs(30),
            "the definitions go live",
            || async { status(&raw, target).await == TransformStatus::Live },
        )
        .await;
    }

    raw.batch_execute(
        "begin; \
         update public.p set name = 'renamed' where id = 2; \
         delete from public.p where id in (1, 3, 5); \
         insert into public.c values (1000, 2, 7); \
         commit;",
    )
    .await
    .expect("cascade");
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, Duration::from_secs(30))
        .await
        .expect("converge");

    let named: BTreeMap<i32, (Option<i32>, Option<String>)> = raw
        .query("select id, amount, name from public.c_named", &[])
        .await
        .expect("read c_named")
        .into_iter()
        .map(|row| (row.get(0), (row.get(1), row.get(2))))
        .collect();
    let expected: BTreeMap<i32, (Option<i32>, Option<String>)> = raw
        .query(
            "select c.id, c.amount, p.name from public.c left join public.p on p.id = c.pid",
            &[],
        )
        .await
        .expect("oracle")
        .into_iter()
        .map(|row| (row.get(0), (row.get(1), row.get(2))))
        .collect();
    assert_eq!(named, expected);

    let totals: BTreeMap<Option<i32>, Option<i64>> = raw
        .query("select pid, total::bigint from public.c_totals", &[])
        .await
        .expect("read c_totals")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let expected: BTreeMap<Option<i32>, Option<i64>> = raw
        .query(
            "select pid, sum(amount)::bigint from public.c group by pid",
            &[],
        )
        .await
        .expect("oracle")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(totals, expected);

    client.shutdown().await.expect("shutdown");
}

/// #680 (Requires Design Decision), #622 plan finding 10: an application
/// `AFTER ROW` trigger that rewrites the row its own statement just wrote.
/// The nested statement's capture runs first, so the ring holds the newer
/// image at the lower position, and a `GROUP BY` target folds the two into
/// the wrong group. D's NEW-only apply with a re-read image (#623) fixes it;
/// until then this is a documented limitation
/// (`docs/staging-and-claiming/01-capture-by-triggers.md`).
#[tokio::test]
#[ignore = "#680"]
async fn a_nested_rewrite_of_the_same_key_lands_in_the_right_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.s (id int primary key, grp text); \
         alter table public.s replica identity full; \
         insert into public.s values (1, 'g0'); \
         create function public.promote() returns trigger language plpgsql as $$ \
         begin \
             if new.grp = 'g1' then \
                 update public.s set grp = 'g2' where id = new.id; \
             end if; \
             return null; \
         end $$; \
         create trigger promote after update on public.s \
             for each row execute function public.promote();",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM by_grp FROM public.s GROUP BY grp SELECT COUNT(*) AS n")
        .await
        .expect("define");
    let client = trellis::Client::start(db.dsn(), quick_options()).expect("start the client");
    eventually(Duration::from_secs(30), "by_grp goes live", || async {
        status(&raw, "by_grp").await == TransformStatus::Live
    })
    .await;

    raw.batch_execute("update public.s set grp = 'g1' where id = 1")
        .await
        .expect("update, rewritten to g2 by the trigger");
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, Duration::from_secs(30))
        .await
        .expect("converge");

    let groups: BTreeMap<String, i64> = raw
        .query("select grp, n::bigint from public.by_grp where n > 0", &[])
        .await
        .expect("read by_grp")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(groups, BTreeMap::from([("g2".to_string(), 1)]));

    client.shutdown().await.expect("shutdown");
}

/// Seals and drains through the engine's own apply path until nothing is
/// pending anywhere in the ring: the hand-driven stand-in for a running
/// client's maintenance loop and drain workers (as in `alter_transform.rs`).
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_join_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_join_test",
            1,
            "trellis_capture_join_test",
            &watermark,
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

/// Whether `tu.b2`'s pause waits for a capture that images its column.
async fn b2_awaits_capture(raw: &Client) -> bool {
    raw.query_one(
        "select exists (select 1 from column_status \
         where transform_table = 'tu' and column_name = 'b2' and awaiting_capture)",
        &[],
    )
    .await
    .expect("read column_status")
    .get(0)
}

/// `ALTER TRANSFORM ... ADD` of a field on a source column the capture
/// doesn't image yet (#622 C5 review). The edit is `apply`'s, so it only
/// registers: the widen that images the new column is the staging worker's,
/// in the background, and here an open writer keeps it from landing. A row
/// written meanwhile runs the old capture body, so its image lacks the
/// column. The new field must stay paused until the widen has landed and
/// every row the old body staged has drained (the widen's capture gate), or
/// that row fails with `MissingColumn` and its key is quarantined. The
/// edit's catch-up marker is what unpauses it, so its discharge must hold
/// while the capture doesn't image the column.
///
/// A running client takes the definition live; the rest is stepped by hand.
#[tokio::test]
async fn an_alter_adding_a_field_on_a_new_source_column_waits_for_the_widen() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.u (id int primary key, a int, b int); \
         insert into public.u select g, g, 10 * g from generate_series(1, 3) g;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM tu FROM public.u SELECT a AS a")
        .await
        .expect("define");
    let client = trellis::Client::start(db.dsn(), quick_options()).expect("start the client");
    eventually(Duration::from_secs(30), "tu goes live", || async {
        status(&raw, "tu").await == TransformStatus::Live
    })
    .await;
    client.shutdown().await.expect("shutdown");
    let mut ring = connect(db.dsn()).await;
    drain_to_quiescence(&db.pool, &mut ring).await;

    let holder = hold_table(db.dsn(), "public.u").await;
    trellis
        .apply("ALTER TRANSFORM tu ADD b AS b2")
        .await
        .expect("alter: apply never waits on the table");
    assert!(
        b2_awaits_capture(&raw).await,
        "b2 stays paused until its capture covers it"
    );

    // A pass: the widen waits out the holder, and the edit's catch-up
    // marker's discharge holds, so b2 stays paused.
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(
        captured_columns(&raw, "public.u").await,
        Some(vec!["a".to_string(), "id".to_string()]),
        "the widen waits out the open writer"
    );
    assert!(
        b2_awaits_capture(&raw).await,
        "the discharge holds while b isn't imaged"
    );
    assert_eq!(status(&raw, "tu").await, TransformStatus::CatchingUp);

    // A write the old capture body images without b drains with b2 paused.
    raw.batch_execute("update public.u set b = 100 where id = 1")
        .await
        .expect("write");
    drain_to_quiescence(&db.pool, &mut ring).await;

    holder
        .batch_execute("commit")
        .await
        .expect("end the holder");
    for _ in 0..5 {
        full_pass(&mut raw, &db.pool).await;
        drain_to_quiescence(&db.pool, &mut ring).await;
        if status(&raw, "tu").await == TransformStatus::Live {
            break;
        }
    }
    assert_eq!(status(&raw, "tu").await, TransformStatus::Live);
    assert!(
        captured_columns(&raw, "public.u")
            .await
            .expect("captured")
            .contains(&"b".to_string())
    );

    let (poisoned, failed, wrong, paused): (i64, i64, i64, i64) = {
        let row = raw
            .query_one(
                "select (select count(*) from poison), \
                        (select count(*) from column_failures), \
                        (select count(*) from public.tu t join public.u s using (id) \
                         where t.b2 is distinct from s.b or t.a is distinct from s.a), \
                        (select count(*) from column_status)",
                &[],
            )
            .await
            .expect("read the outcome");
        (row.get(0), row.get(1), row.get(2), row.get(3))
    };
    assert_eq!((poisoned, failed), (0, 0), "no row met the old images");
    assert_eq!(wrong, 0, "the target equals the source");
    assert_eq!(paused, 0, "b2 unpaused once its capture covered it");
}
