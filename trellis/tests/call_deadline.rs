//! Issue #599: every public `Trellis` call returns within its deadline, and
//! the work it started on the server is killed at the deadline, not abandoned.
//!
//! The deadline is shortened from 30 seconds to `DEADLINE` through
//! `TrellisOptions::call_deadline`. The stuck statement is a held lock: a
//! second session takes `ACCESS EXCLUSIVE` on the catalog tables, so any
//! statement of the engine's that reads one queues behind it. No test sleeps
//! or polls for convergence (#297): each call returns when the server's
//! `statement_timeout` fires, and the assertions read `pg_stat_activity` at
//! that moment.
//!
//! Every public method is accounted for in
//! `every_public_method_is_tested_or_exempt`.

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime};

use testkit::crash::OpenTransaction;
use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, ErrorCode, SelfCheckMode, Trellis, TrellisError, TrellisOptions};

/// The shortened call deadline.
const DEADLINE: Duration = Duration::from_millis(700);

/// `trellis::deadline::BACKSTOP_GRACE`: a call that returns sooner than this
/// past its deadline was ended by the server, not by the client-side backstop.
const BACKSTOP_GRACE: Duration = Duration::from_secs(1);

async fn raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn options(call_deadline: Duration) -> TrellisOptions {
    TrellisOptions {
        call_deadline: Some(call_deadline),
        ..Default::default()
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, TrellisError>> + 'a>>;

/// `f.expect_timeout`, with the call a closure of the fixture's handle.
macro_rules! stuck {
    ($f:expr, $what:expr, $t:ident => $call:expr) => {
        $f.expect_timeout($what, |$t| Box::pin($call)).await
    };
}

struct Fixture {
    db: TestDatabase,
    /// Calls under `DEADLINE`.
    trellis: Trellis,
    /// Reads `pg_stat_activity` and the catalog.
    observer: Client,
    _cluster: TestCluster,
}

/// A migrated database with `widgets` and a registered `widget_totals`.
async fn fixture() -> Fixture {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let observer = raw(db.dsn()).await;
    observer
        .batch_execute("create table public.widgets (id bigint primary key, price numeric)")
        .await
        .expect("create the source table");
    // Registered by a handle with the default deadline, so a slow box can't
    // fail the setup.
    let setup = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    setup
        .apply("TRANSFORM widget_totals FROM public.widgets SELECT price + price AS total")
        .await
        .expect("define widget_totals");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("dsn"),
        options(DEADLINE),
    )
    .await
    .expect("connect");
    Fixture {
        db,
        trellis,
        observer,
        _cluster: cluster,
    }
}

impl Fixture {
    /// Takes `mode` on `tables` (every table of the catalog schema
    /// for `None`) in a transaction held open until it is dropped or rolled
    /// back.
    async fn hold(&self, tables: Option<&[&str]>, mode: &str) -> OpenTransaction {
        let names: Vec<String> = match tables {
            Some(tables) => tables
                .iter()
                .map(|table| format!("{DEFAULT_SCHEMA}.{table}"))
                .collect(),
            None => self
                .observer
                .query(
                    "select format('%I.%I', schemaname, tablename) from pg_tables \
                     where schemaname = $1 and tablename !~ '^pg_'",
                    &[&DEFAULT_SCHEMA],
                )
                .await
                .expect("list the catalog tables")
                .iter()
                .map(|row| row.get(0))
                .collect(),
        };
        let holder = OpenTransaction::begin(self.db.dsn()).await;
        holder
            .execute(&format!("lock table {} in {mode} mode", names.join(", ")))
            .await;
        holder
    }

    /// How many backends of this database are waiting on a lock.
    async fn lock_waiters(&self) -> i64 {
        self.observer
            .query_one(
                "select count(*) from pg_stat_activity \
                 where datname = current_database() and wait_event_type = 'Lock'",
                &[],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0)
    }

    /// Runs `call` and asserts it ended at the deadline with the timeout
    /// error, and that nothing is left waiting on the server.
    async fn expect_timeout<T: Debug>(
        &self,
        what: &str,
        call: impl for<'a> FnOnce(&'a Trellis) -> BoxFuture<'a, T>,
    ) {
        let started = Instant::now();
        // Boxed, so the engine's large futures live on the heap rather than in
        // this test's own (a debug build overflows the stack otherwise).
        let outcome = call(&self.trellis).await;
        let elapsed = started.elapsed();
        let err = outcome.expect_err(&format!("{what} must not finish"));
        assert_eq!(err.code(), ErrorCode::Timeout, "{what}: {err}");
        assert!(
            elapsed >= DEADLINE - Duration::from_millis(50),
            "{what} returned after {elapsed:?}, before its deadline: {err}"
        );
        assert!(
            elapsed < DEADLINE + BACKSTOP_GRACE,
            "{what} took {elapsed:?}: the server's timeout should end it at {DEADLINE:?}, \
             before the client-side backstop does: {err}"
        );
        assert_eq!(
            self.lock_waiters().await,
            0,
            "{what} left a statement waiting on the server"
        );
    }
}

/// Every method that reads the catalog, against a catalog another session
/// holds: each ends at the deadline with the timeout error and leaves no
/// statement behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_stuck_on_a_lock_ends_at_its_deadline_and_the_server_drops_it() {
    let f = fixture().await;
    // Taken before the catalog is locked: `watermark_token` reads no table
    // (it and `has_live_staging_worker` are covered by the pool-wait test).
    let token = f.trellis.watermark_token().await.expect("a token");
    let holder = f.hold(None, "access exclusive").await;
    stuck!(f, "migrate", t => t.migrate());
    stuck!(f, "apply (define)", t => t.apply("TRANSFORM other FROM public.widgets SELECT price AS p"));
    stuck!(f, "apply (pause)", t => t.apply("PAUSE TRANSFORM widget_totals"));
    stuck!(f, "apply (resume)", t => t.apply("RESUME TRANSFORM widget_totals"));
    stuck!(f, "apply (drop)", t => t.apply("DROP TRANSFORM widget_totals"));
    stuck!(f, "apply (alter)", t => t.apply("ALTER TRANSFORM widget_totals ADD price + price AS twice"));
    stuck!(f, "definitions", t => t.definitions());
    stuck!(f, "status", t => t.status("widget_totals"));
    stuck!(f, "relationships", t => t.relationships());
    stuck!(f, "request_backfill", t => t.request_backfill("widgets"));
    stuck!(f, "poisoned_since", t => t.poisoned_since(SystemTime::UNIX_EPOCH));
    stuck!(f, "quarantined", t => t.quarantined());
    stuck!(f, "quarantine_status", t => t.quarantine_status("widget_totals"));
    stuck!(f, "sample_quarantined", t => t.sample_quarantined("widget_totals", None, 10));
    stuck!(f, "release_key", t => t.release_key("widget_totals", "public.widgets", "1"));
    stuck!(f, "has_live_drain_workers", t => t.has_live_drain_workers());
    // Capped to the deadline: an hour's wait is one call's 700 ms.
    stuck!(f, "await_converged", t => t.await_converged(token, Duration::from_secs(3600)));
    // A start and a poll read the catalog and nothing else: the comparison is
    // a worker's, not a call's (#1023).
    stuck!(f, "self_check", t => t.self_check("widget_totals", SelfCheckMode::Strict, Duration::from_secs(1)));
    stuck!(f, "self_check_job", t => t.self_check_job(1));

    holder.rollback().await;
}

/// A call that ran out of budget rolled its transaction back: the definition
/// it was registering is not there, and neither is its target table.
#[tokio::test(flavor = "multi_thread")]
async fn a_define_that_times_out_registers_nothing() {
    let f = fixture().await;
    // `share` keeps reads working and blocks writes: the define gets as far
    // as inserting its definition, after it has bumped the source's version
    // and created the target, so its transaction has something to roll back.
    let holder = f.hold(Some(&["transform_definitions"]), "share").await;
    stuck!(f, "apply (define)", t => t.apply("TRANSFORM another FROM public.widgets SELECT price AS p"));
    holder.rollback().await;

    let registered: Vec<String> = f
        .trellis
        .definitions()
        .await
        .expect("definitions")
        .into_iter()
        .map(|summary| summary.target_table)
        .collect();
    assert_eq!(registered.len(), 1, "{registered:?}");
    assert!(registered[0].ends_with("widget_totals"), "{registered:?}");
    let target_exists: bool = f
        .observer
        .query_one("select to_regclass('public.another') is not null", &[])
        .await
        .expect("look for the target")
        .get(0);
    assert!(!target_exists, "the timed-out define left its target table");
}

/// The budget includes the wait for a pooled connection: with the pool's only
/// connection held, the two calls that read no table (`watermark_token` reads
/// the WAL position, `has_live_staging_worker` `pg_locks`), and so have no lock
/// to be stuck on, still end at the deadline.
#[tokio::test(flavor = "multi_thread")]
async fn the_wait_for_a_pooled_connection_counts() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let config = Config::from_dsn(db.dsn().to_string())
        .expect("dsn")
        .with_pool_max_size(1)
        .expect("pool size");
    let trellis = Trellis::connect(config, options(DEADLINE))
        .await
        .expect("connect");
    let only = trellis
        .pool()
        .get()
        .await
        .expect("the pool's one connection");

    for what in ["watermark_token", "has_live_staging_worker"] {
        let started = Instant::now();
        let err = match what {
            "watermark_token" => trellis.watermark_token().await.map(|_| ()),
            _ => trellis.has_live_staging_worker().await.map(|_| ()),
        }
        .expect_err("no connection");
        let elapsed = started.elapsed();
        assert!(
            matches!(err, TrellisError::CallTimeout { .. }),
            "{what}: {err}"
        );
        assert_eq!(err.code(), ErrorCode::Timeout);
        assert!(
            elapsed >= DEADLINE - Duration::from_millis(50),
            "{what}: {elapsed:?}"
        );
        assert!(elapsed < DEADLINE + BACKSTOP_GRACE, "{what}: {elapsed:?}");
    }

    drop(only);
    trellis.watermark_token().await.expect("the pool recovered");
}

/// A connection a call used goes back to the pool without the call's
/// `statement_timeout`: a later borrower (a background worker) must not
/// inherit it. With one connection in the pool, the next checkout is
/// necessarily the same session.
#[tokio::test(flavor = "multi_thread")]
async fn a_pooled_connection_comes_back_without_the_calls_timeout() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let config = Config::from_dsn(db.dsn().to_string())
        .expect("dsn")
        .with_pool_max_size(1)
        .expect("pool size");
    let trellis = Trellis::connect(config, options(Duration::from_secs(30)))
        .await
        .expect("connect");

    // Runs on the pool's connection, under the call's deadline.
    trellis.definitions().await.expect("definitions");

    let client = trellis.pool().get().await.expect("check out again");
    let timeout: String = client
        .query_one("show statement_timeout", &[])
        .await
        .expect("show")
        .get(0);
    assert_eq!(timeout, "0", "the call's statement_timeout leaked");
}

/// A timeout shorter than the call's remaining budget, set on the session
/// (a DSN's `options`, a role default), is kept, not raised.
#[tokio::test(flavor = "multi_thread")]
async fn a_shorter_session_timeout_is_kept() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let dsn = format!("{} options='-c statement_timeout=50'", db.dsn());
    let trellis = Trellis::connect(
        Config::from_dsn(dsn).expect("dsn"),
        options(Duration::from_secs(30)),
    )
    .await
    .expect("connect");
    let observer = raw(db.dsn()).await;
    let holder = OpenTransaction::begin(db.dsn()).await;
    holder
        .execute(&format!(
            "lock table {DEFAULT_SCHEMA}.transform_definitions in access exclusive mode"
        ))
        .await;

    let started = Instant::now();
    let err = trellis.definitions().await.expect_err("blocked");
    // The session's 50 ms ended it long before the call's 30 s, and since
    // that is not the call's deadline it stays the database's own error.
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(!matches!(err, TrellisError::CallTimeout { .. }), "{err}");
    drop(observer);
    holder.rollback().await;
}

/// Methods with no deadline test above, and why. A new public method fails
/// this test until it is tested or listed here with a reason.
#[test]
fn every_public_method_is_tested_or_exempt() {
    const TESTED: &[&str] = &[
        "migrate",
        "apply",
        "definitions",
        "status",
        "relationships",
        "request_backfill",
        "poisoned_since",
        "quarantined",
        "quarantine_status",
        "sample_quarantined",
        "release_key",
        "has_live_drain_workers",
        "has_live_staging_worker",
        "watermark_token",
        "await_converged",
        "self_check",
        "self_check_job",
    ];
    const EXEMPT: &[(&str, &str)] = &[
        (
            "connect",
            "opens a lazy pool and starts background work; reads nothing",
        ),
        ("pool", "a borrow of the pool, no call"),
        ("config", "a borrow, no call"),
        ("metrics", "reads the in-process registry, no database"),
        (
            "shutdown",
            "ends background work, not a request; each worker's own waits are bounded by \
             the lock timeout",
        ),
    ];
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/app.rs"),
    )
    .expect("read app.rs");
    let mut public = Vec::new();
    let mut in_impl = false;
    for line in source.lines() {
        if line == "impl Trellis {" {
            in_impl = true;
        } else if in_impl && line == "}" {
            in_impl = false;
        } else if in_impl && let Some(rest) = line.strip_prefix("    pub ") {
            let rest = rest.strip_prefix("async ").unwrap_or(rest);
            if let Some(name) = rest.strip_prefix("fn ") {
                public.push(name.split(['(', '<']).next().unwrap().to_string());
            }
        }
    }
    assert!(public.len() > 10, "the scan found {public:?}");
    for name in &public {
        assert!(
            TESTED.contains(&name.as_str()) || EXEMPT.iter().any(|(exempt, _)| exempt == name),
            "`Trellis::{name}` has no deadline test and no documented reason to need none"
        );
    }
    for name in TESTED.iter().chain(EXEMPT.iter().map(|(name, _)| name)) {
        assert!(public.contains(&name.to_string()), "`{name}` is not public");
    }
}
