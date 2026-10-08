//! Installing, widening, narrowing and uninstalling trigger capture (#622
//! C3), driven by hand. The staging worker's reconcile pass that calls
//! `trellis::capture::install` (C5) is `capture_join.rs`'s subject.
//!
//! Every test here steps the pieces itself (the seal's phases, the drain, the
//! discharge) and holds a writer open where it needs one. None waits for
//! anything to converge (#297).
//!
//! - A2: writers at `REPEATABLE READ` and `SERIALIZABLE` that took their
//!   snapshot before two seals, and a prepared transaction that straddles a
//!   seal, are each claimed by exactly one batch through the trigger.
//! - A3's primitive: an install and an uninstall wait out a writer that stays
//!   open, and no other writer queues behind them for longer than one
//!   attempt's lock timeout.
//! - The waiting report (#622 plan Q1): an install that runs out of its
//!   deadline names the session holding the table and one queued for it,
//!   and the next pass lands once they let go. Nothing is cancelled.
//! - A6's trigger-fed half: a `TRUNCATE` whose writer straddles a seal
//!   reaches the single-bucket barrier.
//! - The widening gap: a widen waits out a writer running the old function,
//!   and a new reader isn't dispatched until the rows the old function staged
//!   have drained, so none of them reaches it without the column it reads.
//! - Install is idempotent, uninstall leaves nothing behind, a partial
//!   install is repaired, and a read racing installs never sees a partial
//!   one.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, IsolationLevel, NoTls};
use trellis::capture::columns::{capture_spec, load_catalog};
use trellis::capture::install::{self, CaptureAction, Installed, LockingOperation, Progress};
use trellis::capture::sql::{CaptureEvent, CaptureSpec, function_name, trigger_name};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{TransformStatus, ValueType, install_definition};
use trellis::intake::markers;
use trellis::locks::USER_TABLE_DDL_LOCK_TIMEOUT;
use trellis::staging::converge::table_changes_pending_through;
use trellis::staging::{
    MIN_ROWS_TO_SPLIT, StagedWatermark, TRUNCATE_SENTINEL_KEY, apply, has_pending,
    retire_drained_segments, seal,
};

const TEST_NAME: &str = "capture_install_test";
const WAKE: &str = "capture_install_wake";

/// How long the lock tests hold a writer open.
const HOLD: Duration = Duration::from_secs(2);

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

/// A pass run without a deadline always lands.
fn landed<T>(progress: Progress<T>) -> T {
    match progress {
        Progress::Done(value) => value,
        Progress::Waiting(wait) => panic!("an operation without a deadline landed: {wait}"),
    }
}

/// `public.t`'s spec: key `id`, imaging `columns` too.
fn spec_of_t(columns: &[&str]) -> CaptureSpec {
    CaptureSpec::new(
        "public.t",
        vec!["id".to_string()],
        columns.iter().map(|c| c.to_string()),
        Vec::new(),
    )
    .expect("valid spec")
}

/// Creates `public.t (id text primary key, a int, b int)` and installs its
/// capture imaging `a`.
async fn install_t(client: &mut Client) -> CaptureSpec {
    client
        .batch_execute("create table public.t (id text primary key, a int, b int)")
        .await
        .expect("create public.t");
    let spec = spec_of_t(&["a"]);
    assert!(
        landed(
            install::install(client, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("install")
        ),
        "a fresh install changes something"
    );
    spec
}

/// Every ring row for `public.t` keyed `key`: `(op, new_image, origin_lsn)`,
/// in staging order.
async fn ring_rows(client: &Client, key: &str) -> Vec<(String, Option<String>, Option<PgLsn>)> {
    client
        .query(
            "select op, new_image::text, origin_lsn from ( \
                 select op, new_image, origin_lsn, change_id, key, src_table from seg_0 \
                 union all select op, new_image, origin_lsn, change_id, key, src_table from seg_1 \
                 union all select op, new_image, origin_lsn, change_id, key, src_table from seg_2 \
                 union all select op, new_image, origin_lsn, change_id, key, src_table from seg_3 \
             ) r where src_table = 'public.t' and key = $1 order by change_id",
            &[&key],
        )
        .await
        .expect("read the ring")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

async fn capture_gate(client: &Client, table: &str) -> Option<PgLsn> {
    client
        .query_opt(
            "select capture_gate_lsn from pending_backfill where table_name = $1",
            &[&table],
        )
        .await
        .expect("read the marker")
        .and_then(|row| row.get(0))
}

/// The `xmin` of each of `public.t`'s capture functions and triggers, so a
/// test can tell whether anything rewrote them.
async fn catalog_versions(client: &Client) -> Vec<String> {
    let mut versions = Vec::new();
    for event in CaptureEvent::ALL {
        let function = function_name("public.t", event).expect("name");
        let trigger = trigger_name(DEFAULT_SCHEMA, event);
        let row = client
            .query_one(
                "select (select xmin::text from pg_proc where proname = $1), \
                        (select xmin::text from pg_trigger where tgname = $2 \
                          and tgrelid = 'public.t'::regclass)",
                &[&function, &trigger],
            )
            .await
            .expect("read xmins");
        versions.push(row.get(0));
        versions.push(row.get(1));
    }
    versions
}

#[tokio::test]
async fn an_install_captures_writes_and_a_repeat_install_changes_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    let spec = install_t(&mut client).await;

    assert_eq!(
        install::installed(&client, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed"),
        Installed::Complete {
            spec: spec.clone(),
            current: true
        }
    );
    // The join marker commits with the triggers, gated at the install.
    let gate = capture_gate(&client, "public.t")
        .await
        .expect("the install parks a gated join marker");

    client
        .batch_execute("insert into public.t values ('k', 1, 2)")
        .await
        .expect("write");
    let rows = ring_rows(&client, "k").await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let (op, image, origin) = &rows[0];
    assert_eq!(op, "insert");
    assert_eq!(
        image.as_deref(),
        Some(r#"{"a": "1", "id": "k"}"#),
        "the image carries the spec's columns, not b"
    );
    assert!(
        origin.expect("an origin") > gate,
        "a write after the install is above its gate"
    );

    // Nothing for PUBLIC: an application role could otherwise attach a
    // capture function to a table of its own and forge ring rows.
    let public_grants: i64 = client
        .query_one(
            "select count(*) from pg_proc p, aclexplode(p.proacl) acl \
             where p.pronamespace = $1::text::regnamespace and p.proname like 'cap\\_%' \
               and acl.grantee = 0",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("read acls")
        .get(0);
    assert_eq!(public_grants, 0);
    let unrevoked: i64 = client
        .query_one(
            "select count(*) from pg_proc \
             where pronamespace = $1::text::regnamespace and proname like 'cap\\_%' \
               and proacl is null",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("read acls")
        .get(0);
    assert_eq!(unrevoked, 0, "a null proacl means PUBLIC may execute");
    // Owned by the role that owns the ring.
    let foreign_owned: i64 = client
        .query_one(
            "select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace \
             where n.nspname = $1 and p.proname like 'cap\\_%' \
               and p.proowner <> (select c.relowner from pg_class c \
                                  where c.relnamespace = n.oid and c.relname = 'seg_0')",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("read owners")
        .get(0);
    assert_eq!(foreign_owned, 0);

    // A repeat install rewrites nothing and parks no new marker generation.
    let versions = catalog_versions(&client).await;
    let generation: i64 = client
        .query_one(
            "select generation from pending_backfill where table_name = 'public.t'",
            &[],
        )
        .await
        .expect("read generation")
        .get(0);
    assert!(
        !landed(
            install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("repeat install")
        ),
        "a repeat install reports no change"
    );
    assert_eq!(
        landed(
            install::reconcile(&mut client, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("reconcile")
        ),
        CaptureAction::Unchanged
    );
    assert_eq!(catalog_versions(&client).await, versions);
    let generation_after: i64 = client
        .query_one(
            "select generation from pending_backfill where table_name = 'public.t'",
            &[],
        )
        .await
        .expect("read generation")
        .get(0);
    assert_eq!(generation_after, generation);
}

/// The capture triggers are `ENABLE ALWAYS`, so a session in
/// `session_replication_role = replica` (a migration tool, a trigger-skipping
/// bulk load) is captured like any other, transition tables included. Only a
/// logical-replication apply worker skips statement triggers (#751,
/// `trellis::defs::subscription`), and the docs rely on this to say so.
#[tokio::test]
async fn a_replica_role_session_is_captured() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    install_t(&mut client).await;

    client
        .batch_execute(
            "set session_replication_role = replica; \
             insert into public.t values ('k', 1, 2); \
             update public.t set a = 3 where id = 'k'; \
             delete from public.t where id = 'k'; \
             reset session_replication_role",
        )
        .await
        .expect("write as a replica-role session");
    let ops: Vec<(String, Option<String>)> = ring_rows(&client, "k")
        .await
        .into_iter()
        .map(|(op, image, _)| (op, image))
        .collect();
    assert_eq!(
        ops,
        vec![
            (
                "insert".to_string(),
                Some(r#"{"a": "1", "id": "k"}"#.to_string())
            ),
            (
                "update".to_string(),
                Some(r#"{"a": "3", "id": "k"}"#.to_string())
            ),
            ("delete".to_string(), None),
        ]
    );
}

#[tokio::test]
async fn an_uninstall_leaves_nothing_behind() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    install_t(&mut client).await;

    assert!(landed(
        install::uninstall(&mut client, DEFAULT_SCHEMA, "public.t", None)
            .await
            .expect("uninstall")
    ));
    assert_eq!(
        install::installed(&client, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed"),
        Installed::Absent
    );
    let leftovers: i64 = client
        .query_one(
            "select (select count(*) from pg_trigger \
                     where tgrelid = 'public.t'::regclass and not tgisinternal) \
                  + (select count(*) from pg_proc where pronamespace = $1::text::regnamespace \
                     and proname like 'cap\\_%') \
                  + (select count(*) from pg_description d \
                     where d.classoid = 'pg_proc'::regclass \
                       and not exists (select 1 from pg_proc p where p.oid = d.objoid))",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("look for leftovers")
        .get(0);
    assert_eq!(
        leftovers, 0,
        "no trigger, function or orphaned comment left"
    );
    client
        .batch_execute("insert into public.t values ('after', 1, 1)")
        .await
        .expect("write after the uninstall");
    assert!(ring_rows(&client, "after").await.is_empty());
    assert!(
        !landed(
            install::uninstall(&mut client, DEFAULT_SCHEMA, "public.t", None)
                .await
                .expect("repeat uninstall")
        ),
        "a repeat uninstall reports no change"
    );

    // A table the application dropped took its triggers with it: the
    // uninstall drops the functions it left behind.
    let spec = install_t_named(&mut client, "public.gone").await;
    client
        .batch_execute("drop table public.gone")
        .await
        .expect("drop the table");
    assert!(matches!(
        install::installed(&client, DEFAULT_SCHEMA, spec.table())
            .await
            .expect("installed"),
        Installed::Partial { .. }
    ));
    assert!(landed(
        install::uninstall(&mut client, DEFAULT_SCHEMA, "public.gone", None)
            .await
            .expect("uninstall a dropped table's capture")
    ));
    assert_eq!(
        install::installed(&client, DEFAULT_SCHEMA, "public.gone")
            .await
            .expect("installed"),
        Installed::Absent
    );
}

/// A read of what is installed that races installs and uninstalls sees
/// each one whole or not at all (#622 C5 review): the four events are read
/// in one statement, so a commit can't land between them and leave a
/// spurious [`Installed::Partial`]. Read one statement per event, this saw a
/// partial install on about a third of its reads.
#[tokio::test]
async fn a_read_racing_installs_and_uninstalls_never_sees_a_partial_install() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    let spec = install_t(&mut client).await;
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let stop = stop.clone();
        let reader = connect(db.dsn()).await;
        tokio::spawn(async move {
            let (mut partial, mut reads) = (Vec::new(), 0);
            while !stop.load(Ordering::Relaxed) {
                if let Installed::Partial { faults } =
                    install::installed(&reader, DEFAULT_SCHEMA, "public.t")
                        .await
                        .expect("installed")
                {
                    partial.push(faults);
                }
                reads += 1;
            }
            (partial, reads)
        })
    };
    for _ in 0..100 {
        landed(
            install::uninstall(&mut client, DEFAULT_SCHEMA, "public.t", None)
                .await
                .expect("uninstall"),
        );
        landed(
            install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("install"),
        );
    }
    stop.store(true, Ordering::Relaxed);
    let (partial, reads) = reader.await.expect("reader");
    assert!(reads > 0);
    assert!(
        partial.is_empty(),
        "{} of {reads} reads saw a partial install, first: {:?}",
        partial.len(),
        partial.first()
    );
}

async fn install_t_named(client: &mut Client, table: &str) -> CaptureSpec {
    client
        .batch_execute(&format!("create table {table} (id text primary key)"))
        .await
        .expect("create table");
    let spec = CaptureSpec::new(table, vec!["id".to_string()], Vec::new(), Vec::new())
        .expect("valid spec");
    landed(
        install::install(client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install"),
    );
    spec
}

#[tokio::test]
async fn a_partial_install_is_reported_and_repaired() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    let spec = install_t(&mut client).await;

    let insert_trigger = trigger_name(DEFAULT_SCHEMA, CaptureEvent::Insert);
    let begin_trigger = trigger_name(DEFAULT_SCHEMA, CaptureEvent::Begin);
    client
        .batch_execute(&format!(
            "alter table public.t disable trigger {insert_trigger}; \
             drop trigger {begin_trigger} on public.t"
        ))
        .await
        .expect("disable a trigger and drop the begin trigger");
    let Installed::Partial { faults } = install::installed(&client, DEFAULT_SCHEMA, "public.t")
        .await
        .expect("installed")
    else {
        panic!("a disabled trigger makes the install partial");
    };
    assert_eq!(faults.len(), 2, "{faults:?}");
    assert!(faults[0].contains("not ENABLE ALWAYS"), "{faults:?}");
    assert!(
        faults[1].contains("the begin trigger") && faults[1].contains("is missing"),
        "{faults:?}"
    );

    assert_eq!(
        landed(
            install::reconcile(&mut client, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("reconcile")
        ),
        CaptureAction::Install
    );
    assert_eq!(
        install::installed(&client, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed"),
        Installed::Complete {
            spec,
            current: true
        }
    );

    // A function body this build wouldn't generate is replaced too.
    let insert_function = function_name("public.t", CaptureEvent::Insert).expect("name");
    client
        .batch_execute(&format!(
            "create or replace function {DEFAULT_SCHEMA}.{insert_function}() \
             returns trigger language plpgsql as $$ begin return null; end $$"
        ))
        .await
        .expect("replace a function by hand");
    let Installed::Complete { current, .. } =
        install::installed(&client, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed")
    else {
        panic!("the comment and triggers still describe a complete install");
    };
    assert!(!current, "a hand-edited body isn't current");
    assert_eq!(
        landed(
            install::reconcile(&mut client, DEFAULT_SCHEMA, &spec_of_t(&["a"]), None)
                .await
                .expect("reconcile")
        ),
        CaptureAction::Widen
    );
    // The replaced body reads back as current, so the repair doesn't repeat.
    assert_eq!(
        landed(
            install::reconcile(&mut client, DEFAULT_SCHEMA, &spec_of_t(&["a"]), None)
                .await
                .expect("reconcile")
        ),
        CaptureAction::Unchanged
    );
    client
        .batch_execute("insert into public.t values ('repaired', 1, 1)")
        .await
        .expect("write");
    assert_eq!(ring_rows(&client, "repaired").await.len(), 1);
}

/// Inserts into `public.t` one row at a time until `stop`.
fn spawn_writer(
    client: Client,
    prefix: &'static str,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut id = 0i64;
        while !stop.load(Ordering::Relaxed) {
            id += 1;
            client
                .execute(
                    "insert into public.t (id) values ($1)",
                    &[&format!("{prefix}-{id}")],
                )
                .await
                .expect("writer insert");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
}

/// A3's primitive, in #565 E7's shape: a writer transaction stays open on the
/// table, so the install's `SHARE ROW EXCLUSIVE` (and the uninstall's
/// `ACCESS EXCLUSIVE`) waits for it, and every other writer that arrives
/// meanwhile would queue behind a waiting DDL. Each attempt gives up after
/// 50 ms, so no one attempt blocks a writer for long (read from `pg_locks`,
/// `testkit::blocking`), and the DDL lands once the open transaction ends. With a deadline the DDL gives up instead,
/// having changed nothing.
#[tokio::test]
async fn install_and_uninstall_wait_out_an_open_writer_without_queueing_others() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let observer = connect(db.dsn()).await;
    observer
        .batch_execute("create table public.t (id text primary key, a int, b int)")
        .await
        .expect("create public.t");
    let spec = spec_of_t(&["a"]);

    for step in ["install", "uninstall"] {
        let holder = connect(db.dsn()).await;
        holder
            .batch_execute(
                "begin; insert into public.t (id) values ('holder-' || gen_random_uuid())",
            )
            .await
            .expect("an open write");
        let holder_pid = backend_pid(&holder).await;
        let writer = connect(db.dsn()).await;
        let writer_pid = backend_pid(&writer).await;
        let stop = Arc::new(AtomicBool::new(false));
        let writing = spawn_writer(writer, step, stop.clone());

        // With a deadline, the DDL stops retrying and reports who it waits on.
        let mut ddl = connect(db.dsn()).await;
        let deadline = Some(Instant::now() + Duration::from_millis(600));
        let before = install::installed(&observer, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed");
        let progress = match step {
            "install" => install::install(&mut ddl, DEFAULT_SCHEMA, &spec, deadline).await,
            _ => install::uninstall(&mut ddl, DEFAULT_SCHEMA, "public.t", deadline).await,
        }
        .unwrap_or_else(|e| panic!("{step}: {e}"));
        let Progress::Waiting(wait) = progress else {
            panic!("{step}: the open write holds the table past the deadline: {progress:?}");
        };
        assert_eq!(
            install::installed(&observer, DEFAULT_SCHEMA, "public.t")
                .await
                .expect("installed"),
            before,
            "{step} past its deadline changed nothing"
        );
        assert!(
            wait.blockers
                .iter()
                .any(|b| b.pid == Some(holder_pid) && b.backend_type == "client backend"),
            "{step}: {wait}"
        );

        // Without one, it waits the write out.
        let spec_owned = spec.clone();
        let task = tokio::spawn(async move {
            match step {
                "install" => install::install(&mut ddl, DEFAULT_SCHEMA, &spec_owned, None)
                    .await
                    .map(|_| ()),
                _ => install::uninstall(&mut ddl, DEFAULT_SCHEMA, "public.t", None)
                    .await
                    .map(|_| ()),
            }
        });
        let watch = testkit::watch_blocked(&observer, writer_pid, HOLD).await;
        eprintln!("{step}: while the DDL retried: {watch}");
        assert!(!task.is_finished(), "{step} waits for the open write");
        // Each attempt gives up after 50 ms; one that waited out the open
        // write would block the writer from the hold's start to its end, and
        // attempts retried back to back would block it most of the time.
        watch.assert_brief_blocks(HOLD, step);
        stop.store(true, Ordering::Relaxed);
        tokio::time::timeout(Duration::from_secs(10), writing)
            .await
            .expect("a writer is stuck behind the DDL")
            .expect("writer");

        holder.batch_execute("commit").await.expect("release");
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the DDL lands once the write commits")
            .expect("ddl task")
            .unwrap_or_else(|e| panic!("{step}: {e}"));
        let now = install::installed(&observer, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed");
        match step {
            "install" => assert_eq!(
                now,
                Installed::Complete {
                    spec: spec.clone(),
                    current: true
                }
            ),
            _ => assert_eq!(now, Installed::Absent),
        }
    }
}

/// The waiting report (#622 plan Q1): an install whose deadline passes while
/// another session holds the table returns, having changed nothing, a
/// [`install::LockWait`] naming that session, and one queued behind it; the
/// next bounded pass after both let go installs.
#[tokio::test]
async fn a_bounded_install_reports_who_holds_the_table_then_lands_once_released() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let observer = connect(db.dsn()).await;
    observer
        .batch_execute("create table public.t (id text primary key, a int, b int)")
        .await
        .expect("create public.t");
    let holder = connect(db.dsn()).await;
    let holder_pid = backend_pid(&holder).await;
    holder
        .batch_execute(
            "begin; lock table public.t in row exclusive mode; \
             select 'capture_install_holder'",
        )
        .await
        .expect("hold the table");

    // A second session queued behind the holder blocks the install too.
    let queued = connect(db.dsn()).await;
    let queued_pid = backend_pid(&queued).await;
    let queued_task = tokio::spawn(async move {
        queued
            .batch_execute("begin; lock table public.t in exclusive mode; commit")
            .await
    });
    let queue_began = Instant::now();
    loop {
        let waiting: bool = observer
            .query_one(
                "select exists (select 1 from pg_locks where pid = $1 and not granted)",
                &[&queued_pid],
            )
            .await
            .expect("read pg_locks")
            .get(0);
        if waiting {
            break;
        }
        assert!(
            queue_began.elapsed() < Duration::from_secs(10),
            "the second session never queued for the table"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let spec = spec_of_t(&["a"]);
    let mut ddl = connect(db.dsn()).await;
    let pass = || Some(Instant::now() + Duration::from_millis(300));
    let progress = install::install(&mut ddl, DEFAULT_SCHEMA, &spec, pass())
        .await
        .expect("install");
    let Progress::Waiting(wait) = progress else {
        panic!("the held table keeps the install waiting: {progress:?}");
    };
    eprintln!("{wait}");
    assert_eq!(wait.table, "public.t");
    assert_eq!(wait.operation, LockingOperation::Install);
    assert_eq!(wait.lock_mode, "ShareRowExclusiveLock");
    assert!(
        wait.waited() >= USER_TABLE_DDL_LOCK_TIMEOUT,
        "at least one attempt timed out: {wait}"
    );
    let held = wait
        .blockers
        .iter()
        .find(|b| b.pid == Some(holder_pid))
        .unwrap_or_else(|| panic!("the holder is named: {wait}"));
    assert_eq!(
        (held.backend_type.as_str(), held.mode.as_str(), held.granted),
        ("client backend", "RowExclusiveLock", true),
        "{held:?}"
    );
    assert!(held.query.contains("capture_install_holder"), "{held:?}");
    assert!(
        held.since.is_some_and(|since| since <= wait.waiting_since),
        "the holder's transaction began before the install waited: {held:?}"
    );
    let queued = wait
        .blockers
        .iter()
        .find(|b| b.pid == Some(queued_pid))
        .unwrap_or_else(|| panic!("the queued session is named: {wait}"));
    assert_eq!(
        (queued.mode.as_str(), queued.granted),
        ("ExclusiveLock", false),
        "{queued:?}"
    );
    assert!(queued.since.is_some(), "{queued:?}");
    assert_eq!(
        install::installed(&observer, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed"),
        Installed::Absent,
        "an install past its deadline changed nothing"
    );

    holder.batch_execute("commit").await.expect("release");
    queued_task
        .await
        .expect("queued task")
        .expect("the queued lock lands once the holder commits");
    assert_eq!(
        install::install(&mut ddl, DEFAULT_SCHEMA, &spec, pass())
            .await
            .expect("install"),
        Progress::Done(true),
        "the next pass lands once nothing holds the table"
    );
    assert_eq!(
        install::installed(&observer, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed"),
        Installed::Complete {
            spec,
            current: true
        }
    );
}

/// Seals both phases and returns the sealed batch.
async fn seal_both_phases(sealer: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(sealer).await.expect("seal phase 1");
    seal::seal_phase2(sealer, outcome.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// The physical identity (`tableoid:ctid`) of the one ring row keyed `key`.
async fn row_identity(client: &Client, key: &str) -> String {
    client
        .query_one(
            "select tableoid::text || ':' || ctid::text from ( \
                 select tableoid, ctid, key from seg_0 \
                 union all select tableoid, ctid, key from seg_1 \
                 union all select tableoid, ctid, key from seg_2 \
                 union all select tableoid, ctid, key from seg_3 \
             ) rows where key = $1",
            &[&key],
        )
        .await
        .expect("exactly one ring row for the key")
        .get(0)
}

/// Asserts the ring row keyed `key` is in exactly one of `batches`' fenced
/// windows.
async fn assert_claimed_exactly_once(client: &Client, batches: &[i64], key: &str) {
    let identity = row_identity(client, key).await;
    let mut hits = Vec::new();
    for &batch in batches {
        if seal::fenced_rows(client, batch)
            .await
            .expect("fenced rows")
            .contains(&identity)
        {
            hits.push(batch);
        }
    }
    assert_eq!(
        hits.len(),
        1,
        "{key:?} must be claimed by exactly one of {batches:?}, not {hits:?}"
    );
}

/// A2, through the trigger (#597's tests restated): a writer at a snapshot
/// isolation level takes its snapshot, two seals flip the ring past it, and
/// only then does it write. The capture function must read the live slot from
/// the mirror, not its snapshot's stale pointer, or the row lands in a slot no
/// later batch reads.
async fn a_snapshot_isolation_writer_is_claimed_exactly_once(level: IsolationLevel) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect(db.dsn()).await;
    install_t(&mut sealer).await;

    let mut writer = connect(db.dsn()).await;
    let txn = writer
        .build_transaction()
        .isolation_level(level)
        .start()
        .await
        .expect("begin writer");
    let seen: i16 = txn
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("take the writer's snapshot")
        .get(0);
    assert_eq!(seen, 0);

    assert_eq!(seal_both_phases(&mut sealer).await, 1);
    assert_eq!(seal_both_phases(&mut sealer).await, 2);

    txn.batch_execute("insert into public.t values ('snapshot-writer', 1, 1)")
        .await
        .expect("write through the trigger");
    txn.commit().await.expect("commit writer");

    assert_eq!(seal_both_phases(&mut sealer).await, 3);
    assert_claimed_exactly_once(&sealer, &[1, 2, 3], "snapshot-writer").await;
}

#[tokio::test]
async fn a_repeatable_read_writer_is_captured_into_a_batch_that_claims_it() {
    a_snapshot_isolation_writer_is_claimed_exactly_once(IsolationLevel::RepeatableRead).await;
}

#[tokio::test]
async fn a_serializable_writer_is_captured_into_a_batch_that_claims_it() {
    a_snapshot_isolation_writer_is_claimed_exactly_once(IsolationLevel::Serializable).await;
}

/// Issue #701: a DBA pre-creates the instance schema as role X, and a login
/// role L, a member of X, runs the migrations and the install. The ring is
/// L's and X has no privilege on it, so the capture functions must be L's
/// too, or every captured write fails. A test cluster connects as a
/// superuser, for whom every privilege check passes, so the setup uses real
/// roles and the application writes as a third one.
///
/// `self_check`'s capture audit must agree that this install is whole: it
/// expects the functions to belong to the ring's owner, as install hands
/// them, and not to the schema's. It runs before the writes, while the ring
/// is empty, so its convergence wait has nothing to wait for.
#[tokio::test]
async fn a_schema_pre_created_by_another_role_still_captures_writes() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let admin = connect(db.dsn()).await;
    admin
        .batch_execute(&format!(
            "create role cap701_schema_owner nologin; \
             create role cap701_trellis login in role cap701_schema_owner; \
             create role cap701_app login; \
             grant create on database \"{}\" to cap701_trellis; \
             grant create on schema public to cap701_trellis; \
             create schema {DEFAULT_SCHEMA} authorization cap701_schema_owner",
            db.name()
        ))
        .await
        .expect("roles and a pre-created schema");
    let as_role = |role: &str| {
        assert!(db.dsn().contains("user=postgres"), "{}", db.dsn());
        db.dsn().replace("user=postgres", &format!("user={role}"))
    };

    let config = trellis::Config::with_schema(as_role("cap701_trellis"), DEFAULT_SCHEMA)
        .expect("valid config");
    let pool = trellis::Pool::new(&config).expect("pool");
    trellis::migrate(&pool, &config)
        .await
        .expect("migrate as the login role");
    let mut installer = connect(&as_role("cap701_trellis")).await;
    install_t(&mut installer).await;
    installer
        .batch_execute("grant select, insert, update, delete on public.t to cap701_app")
        .await
        .expect("grant the application its table");

    let owners: Vec<(String, String)> = admin
        .query(
            "select pg_catalog.pg_get_userbyid(n.nspowner)::text, \
                    pg_catalog.pg_get_userbyid(p.proowner)::text \
             from pg_proc p join pg_namespace n on n.oid = p.pronamespace \
             where n.nspname = $1 and p.proname like 'cap\\_%'",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("read owners")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(owners.len(), CaptureEvent::ALL.len(), "{owners:?}");
    for (schema_owner, function_owner) in &owners {
        assert_eq!(schema_owner, "cap701_schema_owner");
        assert_eq!(function_owner, "cap701_trellis", "the ring's owner");
    }

    let trellis = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect as the login role");
    trellis
        .apply("TRANSFORM t_copy FROM public.t SELECT a AS a")
        .await
        .expect("define a reader of public.t");
    // A plain 1-1 over a captured table is the Re-derive build's (#625
    // F8a), which the discharge only starts: run it to `live`.
    markers::settle_registrations(&pool).await;
    let def = trellis::defs::catalog::definition_by_target(&pool, "t_copy")
        .await
        .expect("read the definition")
        .expect("the definition exists");
    assert_eq!(
        def.status,
        TransformStatus::Live,
        "so the capture audit runs"
    );
    let report = trellis
        .self_check(
            "t_copy",
            trellis::SelfCheckScope {
                after: None,
                limit: 100,
            },
            trellis::SelfCheckMode::Strict,
            Duration::from_secs(30),
        )
        .await
        .expect("self_check");
    assert!(
        matches!(report.outcome, trellis::SelfCheckOutcome::Converged),
        "an install into a pre-created schema has no capture fault: {:?}",
        report.outcome
    );
    trellis.shutdown().await.expect("shutdown");

    let app = connect(&as_role("cap701_app")).await;
    for statement in [
        "insert into public.t values ('k', 1, 2)",
        "update public.t set a = 3 where id = 'k'",
        "delete from public.t where id = 'k'",
    ] {
        app.batch_execute(statement)
            .await
            .unwrap_or_else(|err| panic!("{statement}: {err:?}"));
    }
    let ops: Vec<String> = ring_rows(&installer, "k")
        .await
        .into_iter()
        .map(|(op, _, _)| op)
        .collect();
    assert_eq!(ops, ["insert", "update", "delete"]);
}

/// A role without `pg_read_all_stats` can't see another role's backend in
/// `pg_stat_activity`: the blocker report calls it `unknown`, not a prepared
/// transaction (it has a pid), with no transaction start and no query.
#[tokio::test]
async fn a_blocker_this_role_cant_see_is_unknown_not_prepared() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let observer = connect(db.dsn()).await;
    observer
        .batch_execute(
            "create table public.t (id text primary key, a int, b int); \
             do $$ begin create role capture_install_unprivileged; \
             exception when duplicate_object then null; end $$",
        )
        .await
        .expect("create public.t and an unprivileged role");
    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin; insert into public.t values ('held', 1, 1)")
        .await
        .expect("an open write");
    let holder_pid = backend_pid(&holder).await;

    observer
        .batch_execute("set role capture_install_unprivileged")
        .await
        .expect("set role");
    let blockers = install::blockers(&observer, "public.t", &["RowExclusiveLock"])
        .await
        .expect("read blockers");
    let held = blockers
        .iter()
        .find(|b| b.pid == Some(holder_pid))
        .unwrap_or_else(|| panic!("the holder is a blocker: {blockers:?}"));
    assert_eq!(held.backend_type, "unknown", "{held:?}");
    assert_eq!(
        (held.since, held.query.as_str()),
        (None, "<insufficient privilege>"),
        "{held:?}"
    );
    holder.batch_execute("commit").await.expect("commit");
}

/// A prepared transaction that wrote through the trigger before a seal and
/// commits after it: its xid is in progress in the seal's fence, so its row
/// belongs to the next batch's predecessor half, and that batch claims it
/// once `COMMIT PREPARED` lands (the #565 spike's `prepared_probe`). Until
/// then it holds the table like any open writer, and the blocker report
/// names it by its gid even though it has no backend.
#[tokio::test]
async fn a_prepared_transaction_straddling_a_seal_is_claimed_once_committed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect(db.dsn()).await;
    install_t(&mut sealer).await;
    sealer
        .batch_execute("insert into public.t values ('batch-1', 1, 1)")
        .await
        .expect("write");

    let writer = connect(db.dsn()).await;
    writer
        .batch_execute(
            "begin; insert into public.t values ('prepared', 1, 1); \
             prepare transaction 'capture_install_prepared'",
        )
        .await
        .expect("prepare a write");
    drop(writer);

    assert_eq!(seal_both_phases(&mut sealer).await, 1);

    let blockers = install::blockers(&sealer, "public.t", &["RowExclusiveLock"])
        .await
        .expect("read blockers");
    assert!(
        blockers.iter().any(|b| b.pid.is_none()
            && b.backend_type == "prepared transaction"
            && b.prepared_gid.as_deref() == Some("capture_install_prepared")
            && b.since.is_some()),
        "{blockers:?}"
    );

    sealer
        .batch_execute("commit prepared 'capture_install_prepared'")
        .await
        .expect("commit prepared");
    sealer
        .batch_execute("insert into public.t values ('batch-2', 1, 1)")
        .await
        .expect("write");
    assert_eq!(seal_both_phases(&mut sealer).await, 2);
    assert_claimed_exactly_once(&sealer, &[1, 2], "prepared").await;
    let rows_2 = seal::fenced_rows(&sealer, 2).await.expect("fenced rows");
    assert!(
        rows_2.contains(&row_identity(&sealer, "prepared").await),
        "batch 2 claims the prepared write through its predecessor half"
    );
}

/// A6's trigger-fed half (#598 case 2 through the truncate trigger): the
/// writer's `TRUNCATE` stages its sentinel into slot 0 and stays open across
/// a seal, so batch 2 claims it through its predecessor half. Batch 2's own
/// slot holds enough rows to split, and the barrier must still see the
/// truncate: batch 2 seals single-bucket with `has_truncate`.
#[tokio::test]
async fn a_trigger_fed_truncate_straddling_a_seal_reaches_the_barrier() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect(db.dsn()).await;
    install_t(&mut sealer).await;
    sealer
        .batch_execute("insert into public.t values ('batch-1', 1, 1)")
        .await
        .expect("write");

    let writer = connect(db.dsn()).await;
    writer
        .batch_execute("begin; truncate public.t")
        .await
        .expect("an open truncate");

    assert_eq!(seal_both_phases(&mut sealer).await, 1);
    writer.batch_execute("commit").await.expect("commit");

    let rows = MIN_ROWS_TO_SPLIT * 2;
    sealer
        .execute(
            "insert into public.t select 'bulk-' || g, 1, 1 from generate_series(1, $1::bigint) g",
            &[&rows],
        )
        .await
        .expect("bulk write");
    assert_eq!(seal_both_phases(&mut sealer).await, 2);

    assert_claimed_exactly_once(&sealer, &[1, 2], TRUNCATE_SENTINEL_KEY).await;
    let decisions = |batch: i64| {
        let sealer = &sealer;
        async move {
            let row = sealer
                .query_one(
                    "select bucket_count, has_truncate from segments where seg_seq = $1",
                    &[&batch],
                )
                .await
                .expect("read segment");
            (row.get::<_, i16>(0), row.get::<_, bool>(1))
        }
    };
    assert_eq!(decisions(1).await, (1, false), "batch 1 never sees it");
    assert_eq!(
        decisions(2).await,
        (1, true),
        "batch 2 holds the truncate through its predecessor half: single bucket"
    );
}

/// A widen takes the table lock, so it waits out a writer already running
/// the old function. Everything that writer staged, including rows it wrote
/// while the widen waited, has the old image and an origin below the widen's
/// gate; the next write has the new image and an origin above it.
#[tokio::test]
async fn a_widen_waits_out_a_writer_running_the_old_function() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    install_t(&mut client).await;
    // Discharge the install's marker by hand, so the widen's gate is its own.
    client
        .batch_execute("delete from pending_backfill")
        .await
        .expect("clear the join marker");

    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin; insert into public.t values ('w1', 1, 1)")
        .await
        .expect("an open write");

    let mut ddl = connect(db.dsn()).await;
    let wide = spec_of_t(&["a", "b"]);
    let wide_owned = wide.clone();
    let widen = tokio::spawn(async move {
        install::reconcile(&mut ddl, DEFAULT_SCHEMA, &wide_owned, None).await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!widen.is_finished(), "the widen waits for the open write");
    holder
        .batch_execute("insert into public.t values ('w2', 1, 1); commit")
        .await
        .expect("write again and commit");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), widen)
            .await
            .expect("the widen lands once the write commits")
            .expect("widen task")
            .expect("widen"),
        Progress::Done(CaptureAction::Widen)
    );
    client
        .batch_execute("insert into public.t values ('w3', 1, 1)")
        .await
        .expect("write after the widen");

    let gate = capture_gate(&client, "public.t")
        .await
        .expect("the widen parks a gated marker");
    for key in ["w1", "w2"] {
        let rows = ring_rows(&client, key).await;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(
            rows[0].1.as_deref(),
            Some(r#"{"a": "1", "id": "KEY"}"#.replace("KEY", key).as_str())
        );
        assert!(
            rows[0].2.expect("origin") < gate,
            "{key} precedes the widen"
        );
    }
    let rows = ring_rows(&client, "w3").await;
    assert_eq!(
        rows[0].1.as_deref(),
        Some(r#"{"a": "1", "b": "1", "id": "w3"}"#)
    );
    assert!(rows[0].2.expect("origin") > gate, "w3 follows the widen");

    assert!(
        table_changes_pending_through(&client, "public.t", gate)
            .await
            .expect("gate predicate"),
        "w1 and w2 are still undrained"
    );
    assert_eq!(
        install::installed(&client, DEFAULT_SCHEMA, "public.t")
            .await
            .expect("installed"),
        Installed::Complete {
            spec: wide,
            current: true
        }
    );

    // A narrow takes no lock: it lands while a writer stays open.
    holder
        .batch_execute("begin; insert into public.t values ('w4', 1, 1)")
        .await
        .expect("an open write");
    let narrowed = spec_of_t(&["b"]);
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            install::reconcile(&mut client, DEFAULT_SCHEMA, &narrowed, None)
        )
        .await
        .expect("a narrow doesn't wait for the open write")
        .expect("narrow"),
        Progress::Done(CaptureAction::Narrow)
    );
    holder.batch_execute("commit").await.expect("commit");
}

/// The gate is read once the widen holds the table lock, not when its
/// attempt starts. A writer open since before the attempt writes again and
/// commits while that attempt waits for the lock: it ran the old function
/// after the attempt began, and its row must still be below the gate. Read
/// before `LOCK TABLE`, the gate would precede that row, and the row would
/// reach the new reader without the new column.
///
/// The writer's second insert doesn't queue behind the waiting widen: its
/// transaction already holds `ROW EXCLUSIVE`. The test waits for an attempt
/// that has only just started waiting, so the insert and commit land inside
/// that attempt's 50 ms. On a box too loaded for that, the widen lands on a
/// later attempt and the test still passes; it only stops catching the
/// mis-ordering.
#[tokio::test]
async fn a_row_staged_while_the_widen_waits_for_its_lock_is_below_the_gate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    install_t(&mut client).await;
    client
        .batch_execute("delete from pending_backfill")
        .await
        .expect("clear the join marker");

    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin; insert into public.t values ('w1', 1, 1)")
        .await
        .expect("an open write");

    let mut ddl = connect(db.dsn()).await;
    let ddl_pid = backend_pid(&ddl).await;
    let wide = spec_of_t(&["a", "b"]);
    let widen =
        tokio::spawn(
            async move { install::reconcile(&mut ddl, DEFAULT_SCHEMA, &wide, None).await },
        );

    // An attempt that began waiting for the table lock within the last few
    // milliseconds.
    let waiting_since = Instant::now();
    loop {
        let fresh: bool = client
            .query_one(
                "select exists (select 1 from pg_locks \
                 where pid = $1 and not granted \
                   and relation = 'public.t'::regclass \
                   and clock_timestamp() - waitstart < interval '10 ms')",
                &[&ddl_pid],
            )
            .await
            .expect("read pg_locks")
            .get(0);
        if fresh {
            break;
        }
        assert!(
            waiting_since.elapsed() < Duration::from_secs(10),
            "the widen never waited for the table lock"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    holder
        .batch_execute("insert into public.t values ('w2', 1, 1); commit")
        .await
        .expect("write again and commit while the widen waits");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), widen)
            .await
            .expect("the widen lands once the write commits")
            .expect("widen task")
            .expect("widen"),
        Progress::Done(CaptureAction::Widen)
    );

    let gate = capture_gate(&client, "public.t")
        .await
        .expect("the widen parks a gated marker");
    for key in ["w1", "w2"] {
        let rows = ring_rows(&client, key).await;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert!(
            rows[0].2.expect("origin") < gate,
            "{key}, staged by the old function, is below the gate"
        );
    }
}

/// Seals and drains until nothing is pending anywhere in the ring. A bounded
/// deterministic loop, not a wait-for-convergence poll.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..16 {
        drain_batch(pool, seal_both_phases(client).await).await;
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

/// Drains every bucket of `batch`.
async fn drain_batch(pool: &trellis::Pool, batch: i64) {
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(pool, batch, TEST_NAME, 1, WAKE, &watermark)
        .await
        .expect("drain_once")
        .is_some()
    {}
}

async fn status_of(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("read the definition's status")
        .get(0)
}

fn sales_columns() -> HashMap<String, ValueType> {
    [
        ("id", ValueType::Numeric),
        ("sku", ValueType::Text),
        ("amount", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

/// The table's spec as the catalog asks for it now.
async fn catalog_spec(client: &Client, table: &str) -> CaptureSpec {
    let catalog = load_catalog(client, DEFAULT_SCHEMA)
        .await
        .expect("load the catalog");
    capture_spec(client, &catalog, table)
        .await
        .expect("capture spec")
}

/// The widening gap (the C2 review's finding). `sales` is captured for a 1-1
/// reader of `sku`, so its images carry `id` and `sku` only. A change staged
/// then is still undrained when an aggregate over `amount` registers and the
/// table is widened for it. Dispatched at once, the aggregate would build, go
/// live and then drain that row, whose image has no `amount`:
/// `EvalError::MissingColumn`, and the key quarantined. The widen's capture
/// gate holds the discharge until the row has drained through the 1-1 reader
/// alone; then the aggregate builds from the table and applies only rows
/// that carry `amount`.
#[tokio::test]
async fn a_new_reader_waits_for_the_rows_staged_before_its_widen_to_drain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
        )
        .await
        .expect("create + seed sales");

    install_definition(
        &db.pool,
        "TRANSFORM sku_of FROM sales SELECT sku AS sku",
        &sales_columns(),
        "public",
    )
    .await
    .expect("install the 1-1 reader");
    let narrow = catalog_spec(&client, "public.sales").await;
    assert_eq!(narrow.columns(), ["id", "sku"]);
    assert_eq!(
        landed(
            install::reconcile(&mut client, DEFAULT_SCHEMA, &narrow, None)
                .await
                .expect("install")
        ),
        CaptureAction::Install
    );
    markers::settle_registrations(&db.pool).await;
    assert_eq!(status_of(&client, "public.sku_of").await, "live");
    drain_to_quiescence(&db.pool, &mut client).await;

    // Staged by the narrow function, and sealed into a batch of its own that
    // stays undrained. Were it still in the active slot, the discharge's
    // enumeration of the table (it has a reader) would stage a `Recompute`
    // for the same key into the same batch, and the fold would re-derive the
    // key from the live row, hiding the missing column.
    client
        .batch_execute("insert into public.sales values (4, 'a', 1000)")
        .await
        .expect("write before the widen");
    let pre_widen_batch = seal_both_phases(&mut client).await;

    let definition = install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
        &sales_columns(),
        "public",
    )
    .await
    .expect("install the aggregate");
    assert_eq!(definition.status, TransformStatus::WaitingToBackfill);
    let wide = catalog_spec(&client, "public.sales").await;
    assert_eq!(wide.columns(), ["amount", "id", "sku"]);
    assert_eq!(
        landed(
            install::reconcile(&mut client, DEFAULT_SCHEMA, &wide, None)
                .await
                .expect("widen")
        ),
        CaptureAction::Widen
    );
    let gate = capture_gate(&client, "public.sales")
        .await
        .expect("the widen parks a gated marker");
    assert!(
        table_changes_pending_through(&client, "public.sales", gate)
            .await
            .expect("gate predicate")
    );

    // The discharge and a build pass: the gate holds the aggregate back.
    markers::settle_builds(&db.pool).await;
    assert_eq!(
        status_of(&client, "public.sku_totals").await,
        "waiting_to_backfill",
        "the capture gate holds the dispatch while a pre-widen row is undrained"
    );

    // The pre-widen row drains through the 1-1 reader alone.
    drain_batch(&db.pool, pre_widen_batch).await;
    assert!(
        !table_changes_pending_through(&client, "public.sales", gate)
            .await
            .expect("gate predicate")
    );
    markers::settle_registrations(&db.pool).await;
    assert_eq!(status_of(&client, "public.sku_totals").await, "live");

    client
        .batch_execute("insert into public.sales values (5, 'b', 3)")
        .await
        .expect("write after the widen");
    drain_to_quiescence(&db.pool, &mut client).await;
    let poisoned: i64 = client
        .query_one(
            "select (select count(*) from poison) + (select count(*) from poison_held)",
            &[],
        )
        .await
        .expect("read quarantine")
        .get(0);
    assert_eq!(poisoned, 0, "no row reached a reader without its column");
    let totals: Vec<(String, String)> = client
        .query(
            "select sku, total::text from public.sku_totals order by sku",
            &[],
        )
        .await
        .expect("read sku_totals")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        totals,
        [
            ("a".to_string(), "1012".to_string()),
            ("b".to_string(), "5".to_string())
        ]
    );
    let skus: i64 = client
        .query_one("select count(*) from public.sku_of", &[])
        .await
        .expect("read sku_of")
        .get(0);
    assert_eq!(skus, 5);
}
