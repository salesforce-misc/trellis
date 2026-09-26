//! Issue #364: every connection Trellis opens turns on server-side TCP
//! keepalives, so a partitioned client's backend (and the locks it holds,
//! the producer singleton above all) goes away in seconds rather than the
//! ~2h the kernel defaults allow.
//!
//! Postgres ignores these settings on a unix socket and reads them back as
//! `0` there, so these tests connect over loopback TCP
//! ([`TestCluster::start_with_tcp`]). They check the settings are in effect
//! on the session, and that an operator's own per-connection settings are
//! left alone. Actually partitioning a connection is out of reach here.

use testkit::TestCluster;
use tokio_postgres::GenericClient;
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::ProducerSession;
use trellis::{Config, Pool};

/// What a dedicated connection should read back: 10s idle, 3 probes 5s
/// apart, and a 25s `tcp_user_timeout`. `SHOW` prints these without units
/// (seconds for the keepalives, milliseconds for the user timeout), because
/// on a TCP session it reports what's actually set on the socket.
const DEDICATED: [(&str, &str); 4] = [
    ("tcp_keepalives_idle", "10"),
    ("tcp_keepalives_interval", "5"),
    ("tcp_keepalives_count", "3"),
    ("tcp_user_timeout", "25000"),
];

/// A pooled connection gets the same keepalives but no user timeout (`0`,
/// the kernel default): the pool is handed to embedders, and a user timeout
/// would also end a healthy connection whose caller reads a large result
/// slowly. See `pool::DeadPeerDetection::KeepalivesOnly`.
const POOLED: [(&str, &str); 4] = [
    ("tcp_keepalives_idle", "10"),
    ("tcp_keepalives_interval", "5"),
    ("tcp_keepalives_count", "3"),
    ("tcp_user_timeout", "0"),
];

async fn assert_settings(client: &impl GenericClient, which: &str, expected: &[(&str, &str)]) {
    let tcp: bool = client
        .query_one("select inet_client_addr() is not null", &[])
        .await
        .expect("read client address")
        .get(0);
    assert!(
        tcp,
        "{which}: the session must be TCP for the check to mean anything"
    );
    for (guc, expected) in expected {
        assert_eq!(&show(client, guc).await, expected, "{which}: {guc}");
    }
}

#[tokio::test]
async fn producer_session_sets_server_side_tcp_keepalives() {
    let cluster = TestCluster::start_with_tcp();
    let db = cluster.create_isolated_database().await;

    let session = ProducerSession::connect(&cluster.tcp_database_dsn(db.name()), DEFAULT_SCHEMA)
        .await
        .expect("open the producer session over TCP");

    assert_settings(session.client(), "producer session", &DEDICATED).await;
}

#[tokio::test]
async fn pooled_connections_set_server_side_tcp_keepalives() {
    let cluster = TestCluster::start_with_tcp();
    let db = cluster.create_isolated_database().await;
    let config = Config::from_dsn(cluster.tcp_database_dsn(db.name())).expect("valid config");
    let pool = Pool::new(&config).expect("build pool");

    let client = pool.get().await.expect("pooled connection over TCP");

    assert_settings(&**client, "pooled connection", &POOLED).await;
}

/// A `tcp_*` GUC the operator set for this connection, here through the
/// DSN's `options`, is theirs: Trellis leaves all four alone rather than
/// mixing its schedule with theirs.
#[tokio::test]
async fn a_dsn_options_setting_is_respected() {
    let cluster = TestCluster::start_with_tcp();
    let db = cluster.create_isolated_database().await;
    let dsn = format!(
        "{} options='-c tcp_keepalives_idle=60'",
        cluster.tcp_database_dsn(db.name())
    );

    let session = ProducerSession::connect(&dsn, DEFAULT_SCHEMA)
        .await
        .expect("open the producer session over TCP");

    assert_settings(
        session.client(),
        "producer session, DSN options",
        &[("tcp_keepalives_idle", "60"), ("tcp_user_timeout", "0")],
    )
    .await;
}

/// The same for a per-database default (`ALTER DATABASE ... SET`, and by
/// the same rule `ALTER ROLE`), on both kinds of connection.
#[tokio::test]
async fn a_database_default_is_respected() {
    let cluster = TestCluster::start_with_tcp();
    let db = cluster.create_isolated_database().await;
    let dsn = cluster.tcp_database_dsn(db.name());
    let config = Config::from_dsn(dsn.clone()).expect("valid config");
    let pool = Pool::new(&config).expect("build pool");
    pool.get()
        .await
        .expect("pooled connection over TCP")
        .batch_execute(&format!(
            "alter database \"{}\" set tcp_user_timeout = 60000",
            db.name()
        ))
        .await
        .expect("set a database default");
    // The connection above was bootstrapped before the default existed;
    // only new ones see it.
    let pool = Pool::new(&config).expect("build a fresh pool");
    let pooled = pool.get().await.expect("pooled connection over TCP");
    let session = ProducerSession::connect(&dsn, DEFAULT_SCHEMA)
        .await
        .expect("open the producer session over TCP");

    let respected = [("tcp_user_timeout", "60000")];
    assert_settings(&**pooled, "pooled connection, database default", &respected).await;
    assert_settings(
        session.client(),
        "producer session, database default",
        &respected,
    )
    .await;
    // And the keepalives are left at the kernel's values, not ours.
    assert_ne!(show(&**pooled, "tcp_keepalives_idle").await, "10");
    assert_ne!(show(session.client(), "tcp_keepalives_idle").await, "10");
}

async fn show(client: &impl GenericClient, guc: &str) -> String {
    client
        .query_one(&format!("show {guc}"), &[])
        .await
        .unwrap_or_else(|err| panic!("show {guc}: {err}"))
        .get(0)
}
