//! Integration tests for connectivity + migrations (issue #20), run against
//! a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`, issue #21).

use testkit::TestCluster;
use trellis::{Config, Error, migrate};

/// The number of migration files embedded in the crate. The ledger holds
/// exactly versions `1..=N`: migrations are numbered contiguously.
fn migration_count() -> i32 {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
    let count = std::fs::read_dir(dir)
        .expect("read the migrations directory")
        .filter(|entry| {
            let name = entry.as_ref().expect("directory entry").file_name();
            let name = name.to_string_lossy();
            name.starts_with('V') && name.ends_with(".sql")
        })
        .count();
    i32::try_from(count).expect("migration count fits an i32")
}

#[tokio::test]
async fn migrate_up_is_idempotent() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid schema");

    migrate(&db.pool, &config).await.expect("first migrate run");

    let client = db.pool.get().await.expect("acquire connection");
    let first_run: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("query ledger")
        .iter()
        .map(|row| row.get(0))
        .collect();
    drop(client);

    assert_eq!(
        first_run,
        (1..=migration_count()).collect::<Vec<i32>>(),
        "expected exactly the contiguous versions 1..=N of the migration files to be applied"
    );

    // Running again should be a no-op: same ledger, no error.
    migrate(&db.pool, &config)
        .await
        .expect("second migrate run");

    let client = db.pool.get().await.expect("acquire connection");
    let second_run: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("query ledger")
        .iter()
        .map(|row| row.get(0))
        .collect();

    assert_eq!(
        first_run, second_run,
        "re-running migrate changed the ledger"
    );
}

#[tokio::test]
async fn pool_acquires_and_releases_a_working_connection() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    let client = db.pool.get().await.expect("acquire connection");
    let row = client.query_one("select 1", &[]).await.expect("select 1");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
    drop(client);

    // The connection should be back in the pool, ready to be reused.
    let client = db.pool.get().await.expect("acquire connection again");
    let row = client
        .query_one("select 1", &[])
        .await
        .expect("select 1 again");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
}

#[tokio::test]
async fn unreachable_dsn_surfaces_a_typed_error_without_panicking() {
    // Syntactically valid, but nothing is listening: a unix socket
    // directory that was never created.
    let config = Config::from_dsn(
        "host=/tmp/trellis-nonexistent-socket-dir port=5432 user=postgres dbname=postgres",
    )
    .expect("valid schema");
    let pool = trellis::Pool::new(&config).expect("pool builds lazily; DSN is well-formed");

    match pool.get().await {
        Err(Error::Pool(_)) => {}
        other => panic!("expected a typed pool error for an unreachable DSN, got {other:?}"),
    }
}

/// Issue #340: a connection killed server-side (`pg_terminate_backend`, the
/// same fault a chaos test injects into the producer connection) must
/// classify as `Connectivity`, the same as a transport-level drop. The
/// server reports it as a real `DbError` with SQLSTATE `57P01`
/// (admin_shutdown), which `classify_pg_error` used to fold into `Internal`.
/// The connection future is what deterministically receives that `FATAL`
/// (a query issued afterward sees only "connection closed"), so that's the
/// error this test classifies.
#[tokio::test]
async fn terminated_backend_classifies_as_connectivity() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    let (victim, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect victim");
    let connection = tokio::spawn(connection);
    let pid: i32 = victim
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("victim pid")
        .get(0);

    let killer = db.pool.get().await.expect("acquire connection");
    let terminated: bool = killer
        .query_one("select pg_terminate_backend($1)", &[&pid])
        .await
        .expect("terminate victim")
        .get(0);
    assert!(terminated, "pg_terminate_backend must find the victim");

    let err = connection
        .await
        .expect("connection task must not panic")
        .expect_err("a terminated backend must end its connection with an error");
    let db_err = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected the server's FATAL as a DbError, got {err:?}"));
    assert_eq!(
        *db_err.code(),
        tokio_postgres::error::SqlState::ADMIN_SHUTDOWN,
        "{db_err:?}"
    );
    assert_eq!(
        trellis::error_code::classify_pg_error(&err),
        trellis::ErrorCode::Connectivity
    );
}

/// Issue #182: a pool with no `max_size`/`wait_timeout` configured lets a
/// caller that shows up once every slot is already checked out wait
/// forever — silently, with no error and no log line, ever. Configures a
/// deliberately tiny `max_size` (2, via [`Config::with_pool_max_size`]) and
/// a short `wait_timeout` (via [`Config::with_pool_wait_timeout`]) so
/// exhaustion is trivial to trigger without spinning up dozens of real
/// connections, then proves a third caller gets back a typed, bounded
/// failure — never a hang — once every slot is held.
#[tokio::test]
async fn pool_exhaustion_times_out_with_a_typed_error_instead_of_hanging() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    let config = Config::from_dsn(db.dsn().to_string())
        .expect("valid schema")
        .with_pool_max_size(2)
        .expect("2 is a valid pool size")
        .with_pool_wait_timeout(std::time::Duration::from_millis(300));
    let pool = trellis::Pool::new(&config).expect("pool builds lazily; DSN is well-formed");

    // Check out every slot the pool has and hold onto the guards — dropping
    // either would return its connection to the pool and defeat the point
    // of this test.
    let first = pool.get().await.expect("acquire slot 1 of 2");
    let second = pool.get().await.expect("acquire slot 2 of 2");

    // A third caller now has nowhere to go. Wrap the call in a generous
    // outer `tokio::time::timeout` (well beyond the pool's own 300ms
    // `wait_timeout`) so a regression that reintroduces an unbounded wait
    // fails this test loudly and quickly instead of hanging the whole test
    // binary.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), pool.get())
        .await
        .expect("pool.get() must return well within 5s once wait_timeout elapses, not hang");

    match outcome {
        Err(Error::Pool(err)) => {
            let message = err.to_string();
            assert!(
                message.to_lowercase().contains("timeout"),
                "expected a wait-timeout pool error, got: {message}"
            );
        }
        other => panic!(
            "expected a typed, bounded pool-timeout error once the pool was exhausted, got \
             {other:?}"
        ),
    }

    drop(first);
    drop(second);
}

#[tokio::test]
async fn failing_migration_rolls_back_without_partial_ledger_entry() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid schema");

    migrate(&db.pool, &config)
        .await
        .expect("apply real migrations first");

    let mut client = db.pool.get().await.expect("acquire connection");
    let before: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("query ledger")
        .iter()
        .map(|row| row.get(0))
        .collect();

    // Inject a deliberately broken migration directly, bypassing the
    // embedded set, so this test doesn't require a permanently-broken file
    // under trellis/migrations/.
    let broken = refinery::Migration::unapplied("V999__broken", "this is not valid sql;")
        .expect("construct ad-hoc migration");
    let runner = refinery::Runner::new(&[broken]);
    let pg_client: &mut tokio_postgres::Client = &mut client;
    let result = runner.run_async(pg_client).await;
    assert!(result.is_err(), "expected the broken migration to fail");

    let after: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("query ledger after failed migration")
        .iter()
        .map(|row| row.get(0))
        .collect();

    assert_eq!(
        before, after,
        "a failed migration must not leave a partial ledger entry"
    );
}
