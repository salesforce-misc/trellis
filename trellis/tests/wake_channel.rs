//! Issue #875 (epic #806): two instances in one database wake only their own
//! workers. `LISTEN/NOTIFY` channels are per database, so the default wake
//! channel is derived from the catalog schema.
//!
//! The test drives a real staging-worker `Client` for instance A and asserts
//! on what a test connection listening on both instances' channels receives.
//! The only wait is a bounded `recv` for a notification that must arrive;
//! "nothing on B's channel" is checked by a marker notify sent on B's channel
//! afterwards. Postgres delivers one session's notifications in commit order,
//! so any earlier notify on B's channel would have arrived before the marker.

use std::time::Duration;

use testkit::TestCluster;
use tokio::sync::mpsc;
use tokio_postgres::{AsyncMessage, Client, NoTls};
use trellis::client::default_wake_channel;
use trellis::{Config, Pool, migrate};

const SCHEMA_A: &str = "inst_a";
const SCHEMA_B: &str = "inst_b";
const RECV_TIMEOUT: Duration = Duration::from_secs(20);

async fn connect(dsn: &str, schema: &str) -> (Client, mpsc::UnboundedReceiver<(String, String)>) {
    let (client, mut connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(n))) => {
                    let _ = tx.send((n.channel().to_string(), n.payload().to_string()));
                }
                Some(Ok(_)) => continue,
                _ => break,
            }
        }
    });
    client
        .batch_execute(&format!("set search_path to {schema}, public"))
        .await
        .expect("set search_path");
    (client, rx)
}

#[tokio::test]
async fn a_seal_in_one_instance_does_not_wake_the_other_instances_channel() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    for schema in [SCHEMA_A, SCHEMA_B] {
        let config = Config::with_schema(db.dsn(), schema).expect("valid schema");
        let pool = Pool::new(&config).expect("pool");
        migrate(&pool, &config).await.expect("migrate");
    }

    let channel_a = default_wake_channel(SCHEMA_A);
    let channel_b = default_wake_channel(SCHEMA_B);
    assert_ne!(channel_a, channel_b);

    let (listener, mut notifications) = connect(db.dsn(), "public").await;
    listener
        .batch_execute(&format!("listen \"{channel_a}\"; listen \"{channel_b}\""))
        .await
        .expect("listen on both channels");

    // Instance A's staging worker and one app worker, on default options:
    // the staging worker seals the active segment once a row lands in it and
    // notifies its wake channel; the app worker then drains that segment and
    // notifies the channel it `LISTEN`s on. Both must be A's channel.
    let config_a = Config::with_schema(db.dsn(), SCHEMA_A).expect("valid schema");
    let options = trellis::ClientOptions {
        staging_worker: true,
        application_threads: 1,
        maintenance_interval: Duration::from_millis(50),
        ..Default::default()
    };
    let client = tokio::task::spawn_blocking(move || {
        trellis::Client::start_with_config(config_a, options).expect("start instance A's client")
    })
    .await
    .expect("join");

    let (writer, _) = connect(db.dsn(), SCHEMA_A).await;
    writer
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen) values ('orders', 'k1', 'recompute', 0)",
            &[],
        )
        .await
        .expect("stage a row in A's active segment");

    // The seal's notify, then the drain's. The drain can't start before the
    // seal publishes, so they arrive in that order; the second one pins the
    // app worker to the same resolved name as the staging worker (the drain
    // notifies the channel the worker's `LISTEN` is on).
    for step in ["A's seal", "A's drain"] {
        let (channel, _) = tokio::time::timeout(RECV_TIMEOUT, notifications.recv())
            .await
            .unwrap_or_else(|_| panic!("{step} must notify"))
            .expect("listener open");
        assert_eq!(channel, channel_a, "{step} notifies A's channel");
    }

    // Synchronization point: a marker on B's channel, sent after A's seal and
    // drain committed. Everything A sent before it has been delivered by the
    // time the marker is.
    let (observer, _) = connect(db.dsn(), "public").await;
    observer
        .execute("select pg_notify($1, 'marker')", &[&channel_b])
        .await
        .expect("send marker");
    let mut on_b = Vec::new();
    loop {
        let (channel, payload) = tokio::time::timeout(RECV_TIMEOUT, notifications.recv())
            .await
            .expect("the marker must arrive")
            .expect("listener open");
        if channel == channel_b {
            on_b.push(payload);
            break;
        }
    }
    assert_eq!(
        on_b,
        vec!["marker".to_string()],
        "nothing but the marker reached B's channel"
    );

    client.shutdown().await.expect("shutdown");
}
