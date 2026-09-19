//! Session guards a producer must hold before it may append.
//!
//! Two guards, both enforced once, at connect time, rather than on every
//! append:
//!
//! - **`synchronous_commit`** must not be `off` — a correctness
//!   requirement (see [`StagingError::SynchronousCommitOff`]), not
//!   tuning.
//! - **The producer singleton**: exactly one CDC intake producer may run
//!   at a time, enforced by a session-scoped `pg_try_advisory_lock` that
//!   releases the instant the holding connection closes.
//!
//! [`ProducerSession`] owns a standalone `tokio_postgres` connection rather
//! than a pooled `deadpool_postgres::Client`: the pool's `Fast` recycling
//! can hand one physical connection to a different caller between checkouts,
//! which would let an unrelated checkout inherit or silently lose the
//! session-scoped advisory lock. The singleton lock must be pinned to one
//! connection for its whole lifetime, so this opens one directly.

use tokio_postgres::{Client, NoTls, Transaction};

use super::error::StagingError;

/// The advisory lock key the producer singleton is acquired under. Any
/// unique `i64` works; it is a named constant so a future second advisory
/// lock doesn't collide with it by accident.
pub const PRODUCER_SINGLETON_LOCK_KEY: i64 = 0x7247_4c53_5453_4747; // "trellis producer" byte soup

/// A guarded connection held for the lifetime of one producer (CDC intake).
/// Acquiring one enforces both session guards; dropping it closes the
/// connection, releasing the singleton lock instantly. Session-scoped rather
/// than a TTL lease so "the process died" and "the lock is free" are the
/// same instant, not eventually the same.
pub struct ProducerSession {
    client: Client,
    _connection: tokio::task::JoinHandle<()>,
}

// `tokio_postgres::Client` isn't `Debug`, so this is hand-rolled — mainly so
// test assertions like `Result::expect_err` compile.
impl std::fmt::Debug for ProducerSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerSession").finish_non_exhaustive()
    }
}

impl ProducerSession {
    /// Opens a dedicated connection to `dsn`, pins its `search_path` to
    /// `schema` (this connection bypasses [`crate::pool::Pool`], so nothing
    /// else does), and enforces both guards in order: `synchronous_commit`
    /// first, so a session that fails the durability check never briefly
    /// holds the singleton lock, then the lock itself.
    pub async fn connect(dsn: &str, schema: &str) -> Result<Self, StagingError> {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(&format!(
                "set search_path to {}, public",
                crate::pool::quote_ident(schema)
            ))
            .await?;

        require_synchronous_commit_on(&client).await?;
        acquire_singleton(&client).await?;

        Ok(Self {
            client,
            _connection: handle,
        })
    }

    /// Starts a transaction on this session's connection, for callers that
    /// need to compose [`super::append::append`] with other statements
    /// atomically.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, StagingError> {
        Ok(self.client.transaction().await?)
    }

    /// The underlying connection, for callers that need it directly (e.g.
    /// to check `pg_backend_pid()` in a test).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// A mutable handle to the underlying connection, for callers (e.g.
    /// [`crate::intake::publication::reconcile_publication`]) that take a
    /// plain `&mut Client` rather than a whole `ProducerSession`, so the
    /// same function also works against a non-producer connection (see
    /// that function's doc comment).
    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }
}

/// `SHOW synchronous_commit` rather than `pg_settings`: this is the session's
/// *effective* setting — whatever a role/database default, `ALTER SYSTEM`, or
/// the connection string resolved to — which is what decides whether this
/// connection's commits are durable-before-ack.
async fn require_synchronous_commit_on(client: &Client) -> Result<(), StagingError> {
    let row = client.query_one("show synchronous_commit", &[]).await?;
    let value: String = row.get(0);
    if value.eq_ignore_ascii_case("off") {
        return Err(StagingError::SynchronousCommitOff);
    }
    Ok(())
}

/// `pg_try_advisory_lock` (non-blocking) rather than `pg_advisory_lock`: a
/// second producer should fail fast with a typed error, not hang waiting for
/// a lock the long-lived first producer may never release.
async fn acquire_singleton(client: &Client) -> Result<(), StagingError> {
    let row = client
        .query_one(
            "select pg_try_advisory_lock($1)",
            &[&PRODUCER_SINGLETON_LOCK_KEY],
        )
        .await?;
    let acquired: bool = row.get(0);
    if !acquired {
        return Err(StagingError::ProducerAlreadyRunning);
    }
    Ok(())
}
