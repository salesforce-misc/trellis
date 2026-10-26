//! The benchmark's shared raw-connection helper.

use tokio_postgres::{Client as RawClient, NoTls};
use trellis::dev::config::DEFAULT_SCHEMA;

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching the
/// convention `trellis`'s own integration tests use: the scenarios' reference
/// and correctness queries want a plain `tokio_postgres::Client`, which the
/// pool's wrapped client doesn't expose, so they go through a raw connection.
pub(crate) async fn connect_raw(dsn: &str) -> RawClient {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}
