//! Issue #364: every connection Trellis opens turns on server-side TCP
//! keepalives, so a partitioned client's backend (and the locks it holds,
//! the producer singleton above all) goes away in seconds rather than the
//! ~2h the kernel defaults allow.
//!
//! Postgres ignores these settings on a unix socket and reads them back as
//! `0` there, so these tests connect over loopback TCP
//! ([`TestCluster::start_with_tcp`]). They check the settings are in effect
//! on the session. Actually partitioning a connection is out of reach here.

use testkit::TestCluster;
use tokio_postgres::GenericClient;
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::ProducerSession;
use trellis::{Config, Pool};

/// What each session should read back: 10s idle, 3 probes 5s apart, and a
/// 25s `tcp_user_timeout`. `SHOW` prints these without units (seconds for
/// the keepalives, milliseconds for the user timeout), because on a TCP
/// session it reports what's actually set on the socket.
const EXPECTED: [(&str, &str); 4] = [
    ("tcp_keepalives_idle", "10"),
    ("tcp_keepalives_interval", "5"),
    ("tcp_keepalives_count", "3"),
    ("tcp_user_timeout", "25000"),
];

async fn assert_keepalives(client: &impl GenericClient, which: &str) {
    let tcp: bool = client
        .query_one("select inet_client_addr() is not null", &[])
        .await
        .expect("read client address")
        .get(0);
    assert!(
        tcp,
        "{which}: the session must be TCP for the check to mean anything"
    );
    for (guc, expected) in EXPECTED {
        let value: String = client
            .query_one(&format!("show {guc}"), &[])
            .await
            .unwrap_or_else(|err| panic!("{which}: show {guc}: {err}"))
            .get(0);
        assert_eq!(value, expected, "{which}: {guc}");
    }
}

#[tokio::test]
async fn producer_session_sets_server_side_tcp_keepalives() {
    let cluster = TestCluster::start_with_tcp();
    let db = cluster.create_isolated_database().await;

    let session = ProducerSession::connect(&cluster.tcp_database_dsn(db.name()), DEFAULT_SCHEMA)
        .await
        .expect("open the producer session over TCP");

    assert_keepalives(session.client(), "producer session").await;
}

#[tokio::test]
async fn pooled_connections_set_server_side_tcp_keepalives() {
    let cluster = TestCluster::start_with_tcp();
    let db = cluster.create_isolated_database().await;
    let config = Config::from_dsn(cluster.tcp_database_dsn(db.name())).expect("valid config");
    let pool = Pool::new(&config).expect("build pool");

    let client = pool.get().await.expect("pooled connection over TCP");

    assert_keepalives(&**client, "pooled connection").await;
}
