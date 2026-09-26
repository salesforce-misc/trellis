//! Session guards a producer must hold before it may append.
//!
//! Two guards, both enforced once, at connect time, rather than on every
//! append:
//!
//! - **`synchronous_commit`** must not be `off` — a correctness
//!   requirement (see [`StagingError::SynchronousCommitOff`]), not
//!   tuning.
//! - **The producer singleton**: exactly one CDC intake producer **per
//!   Trellis instance** may run at a time, enforced by a session-scoped
//!   `pg_try_advisory_lock` that releases the instant the holding connection
//!   closes. "Per instance" — not per database — is load-bearing and was a
//!   real bug until issue #234; see [`producer_singleton_lock_key`].
//!
//! [`ProducerSession`] owns a standalone `tokio_postgres` connection rather
//! than a pooled `deadpool_postgres::Client`: the pool's `Fast` recycling
//! can hand one physical connection to a different caller between checkouts,
//! which would let an unrelated checkout inherit or silently lose the
//! session-scoped advisory lock. The singleton lock must be pinned to one
//! connection for its whole lifetime, so this opens one directly.

use tokio_postgres::{Client, GenericClient, Transaction};

use super::error::StagingError;

/// The namespace seed the producer-singleton advisory lock key is derived
/// from. Any unique `i64` works; it is a named constant so a future second
/// advisory lock doesn't collide with it by accident.
///
/// Not itself the lock key — see [`producer_singleton_lock_key`].
const PRODUCER_SINGLETON_LOCK_NAMESPACE: u64 = 0x7247_4c53_5453_4747; // "trellis producer" byte soup

/// The advisory lock key the producer singleton is acquired under, **derived
/// from the instance schema** (issue #234).
///
/// # Why this is per-schema and not a global constant
///
/// Postgres advisory locks are keyed by `(database, key)`. The schema a
/// session's `search_path` happens to be pinned to does not enter into the
/// lock tag at all. So while this lock was a single global constant, "exactly
/// one CDC intake producer may run at a time" was in fact enforced **per
/// database**, not per Trellis instance — and `docs/instance-identity.md`
/// promises that "several Trellis instances can coexist in one cluster — even
/// one database — each isolated within its own schema."
///
/// Those two statements cannot both be true. Two correctly-configured
/// instances sharing one database could never both run: whichever started
/// second failed its staging setup with
/// [`StagingError::ProducerAlreadyRunning`], reporting a conflict with a
/// producer that was not its own and that it had no business seeing. Found by
/// `generative/tests/two_instance_noise.rs` (issue #234's two-instance
/// side-by-side property), which is also its regression coverage.
///
/// Deriving the key from the schema restores the intended scope — one
/// producer per *instance* — while keeping the lock exactly as cheap and
/// exactly as session-scoped as before.
///
/// # The derivation
///
/// FNV-1a over the schema's bytes, mixed with
/// [`PRODUCER_SINGLETON_LOCK_NAMESPACE`] so keys stay clustered in a
/// recognizably Trellis-ish region of the keyspace rather than spread over an
/// arbitrary one. FNV-1a is chosen for being tiny, dependency-free, and
/// stable across processes and builds — `std::hash::DefaultHasher` is
/// explicitly *not* guaranteed stable across releases, and a key that could
/// change under the feet of a running fleet would silently un-enforce the
/// singleton. It needs no cryptographic properties: schema names are
/// operator-chosen configuration, not adversarial input, and the only
/// consequence of a collision is two distinct instances in the same database
/// conservatively refusing to run concurrently — the pre-#234 behavior for
/// *every* pair, and a loud, typed error rather than a correctness bug.
///
/// **Upgrade note:** this changes the key a default-schema
/// (`config::DEFAULT_SCHEMA`) instance acquires, so a fleet running a mix of
/// pre- and post-#234 binaries against the same database would not see each
/// other's singleton for the duration of the rollout. That window is a
/// deliberate accepted cost: preserving the old constant as a special case
/// for the default schema would leave the documented multi-instance topology
/// broken for exactly the configuration it is most likely to be used in (a
/// default-schema instance alongside a named one).
pub fn producer_singleton_lock_key(schema: &str) -> i64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in schema.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    // `as i64` is a plain bit reinterpretation, not a lossy narrowing:
    // `pg_advisory_lock` takes a signed `bigint`, and every one of the 64
    // bits is equally good as a key, so wrapping into the negative half of
    // the range is fine and deliberate.
    (hash ^ PRODUCER_SINGLETON_LOCK_NAMESPACE) as i64
}

/// A guarded connection held for the lifetime of one producer (CDC intake).
/// Acquiring one enforces both session guards. Session-scoped rather than a
/// TTL lease, so a producer whose process dies frees the lock as soon as
/// Postgres notices the connection is gone.
///
/// Dropping a session frees the lock only *eventually*: the server-side
/// backend releases it when it sees the socket close, which can be after the
/// next statement on another connection has already run. A caller handing the
/// singleton to another session must call [`ProducerSession::release`]
/// rather than drop it.
pub struct ProducerSession {
    client: Client,
    schema: String,
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
    ///
    /// TCP keepalives are on at both ends before the lock is taken (issue
    /// #364), so a partitioned producer's lock frees in about 25s rather
    /// than the ~2h the kernel defaults allow. See
    /// `crate::pool::TCP_KEEPALIVE_IDLE` for the numbers.
    pub async fn connect(dsn: &str, schema: &str) -> Result<Self, StagingError> {
        let (client, connection) = crate::pool::connect_dedicated(dsn).await?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(&crate::pool::dedicated_session_setup(schema))
            .await?;

        require_synchronous_commit_on(&client).await?;
        // Issue #234: scoped to this instance's own schema, not a global
        // constant — see `producer_singleton_lock_key`.
        acquire_singleton(&client, schema).await?;

        Ok(Self {
            client,
            schema: schema.to_string(),
            _connection: handle,
        })
    }

    /// Releases the singleton lock on the server, then closes the
    /// connection. Once this returns, another session can acquire the lock
    /// immediately, which dropping doesn't guarantee (see the type's doc
    /// comment).
    pub async fn release(self) -> Result<(), StagingError> {
        self.client
            .query_one(
                "select pg_advisory_unlock($1)",
                &[&producer_singleton_lock_key(&self.schema)],
            )
            .await?;
        Ok(())
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
///
/// `schema` scopes the key to one Trellis instance (issue #234) — see
/// [`producer_singleton_lock_key`] for why a global constant was wrong.
async fn acquire_singleton(client: &Client, schema: &str) -> Result<(), StagingError> {
    let row = client
        .query_one(
            "select pg_try_advisory_lock($1)",
            &[&producer_singleton_lock_key(schema)],
        )
        .await?;
    let acquired: bool = row.get(0);
    if !acquired {
        return Err(StagingError::ProducerAlreadyRunning);
    }
    Ok(())
}

/// Issue #428: whether some session holds `schema`'s producer singleton
/// right now — that is, whether this instance's staging worker is running.
/// Intake holds the lock on its own connection for as long as it streams,
/// and the staging worker's setup holds it before that, so this is true
/// from before [`crate::client::Client::start`] returns until intake stops.
/// It goes false while a failed intake waits to restart, which is also a
/// window in which nothing is captured.
///
/// Like the lock, this needs no heartbeat: a crashed worker's connection
/// closes, and Postgres frees the lock with it.
pub(crate) async fn producer_is_running(
    client: &impl GenericClient,
    schema: &str,
) -> Result<bool, StagingError> {
    advisory_lock_held(client, producer_singleton_lock_key(schema)).await
}

/// Whether any session in this database holds the session-level advisory
/// lock `key`. `pg_locks` shows a `bigint` key split across `classid` (the
/// high 32 bits) and `objid` (the low 32), with `objsubid = 1` telling it
/// apart from the two-`integer` form. `int8` `<<` doesn't check for
/// overflow, so a negative key round-trips bit for bit. Advisory locks are
/// per database, so the `database` filter keeps another database's lock
/// under the same key from counting.
async fn advisory_lock_held(client: &impl GenericClient, key: i64) -> Result<bool, StagingError> {
    let held: bool = client
        .query_one(
            "select exists(select 1 from pg_locks \
             where locktype = 'advisory' and granted and objsubid = 1 \
               and database = (select oid from pg_database where datname = current_database()) \
               and ((classid::int8 << 32) | objid::int8) = $1)",
            &[&key],
        )
        .await?
        .get(0);
    Ok(held)
}

#[cfg(test)]
mod lock_probe_tests {
    use super::*;

    /// Both halves of the key's range: a key with the top bit set is how
    /// half of all schemas land (`producer_singleton_lock_key` wraps into the
    /// negative range on purpose), and `pg_locks`' unsigned `oid` halves must
    /// still reassemble into it.
    #[tokio::test]
    async fn advisory_lock_held_sees_positive_and_negative_keys() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let holder = db.pool.get().await.expect("holder connection");
        let observer = db.pool.get().await.expect("observer connection");
        for key in [0x0123_4567_89ab_cdef_i64, -0x0123_4567_89ab_cdef_i64, -1] {
            assert!(
                !advisory_lock_held(&**observer, key).await.expect("probe"),
                "{key:#x} is not held yet"
            );
            holder
                .execute("select pg_advisory_lock($1)", &[&key])
                .await
                .expect("take the lock");
            assert!(
                advisory_lock_held(&**observer, key).await.expect("probe"),
                "{key:#x} is held by another session"
            );
            // Only that key: another instance's producer singleton (another
            // schema's key) mustn't count. One neighbour differs in each
            // half, so each half of the reassembly is actually compared.
            for other in [key ^ 1, key ^ (1 << 32)] {
                assert!(
                    !advisory_lock_held(&**observer, other).await.expect("probe"),
                    "{other:#x} is not held while {key:#x} is"
                );
            }
            holder
                .execute("select pg_advisory_unlock($1)", &[&key])
                .await
                .expect("release the lock");
            assert!(
                !advisory_lock_held(&**observer, key).await.expect("probe"),
                "{key:#x} was released"
            );
        }
    }

    /// Advisory locks are per database, and every instance on the default
    /// schema shares one producer-singleton key. So a staging worker for the
    /// same schema in another database on the server mustn't count.
    #[tokio::test]
    async fn advisory_lock_held_ignores_another_databases_lock() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let other_db = cluster.create_isolated_database().await;
        let holder = other_db.pool.get().await.expect("holder connection");
        let observer = db.pool.get().await.expect("observer connection");
        let key = producer_singleton_lock_key(crate::config::DEFAULT_SCHEMA);
        holder
            .execute("select pg_advisory_lock($1)", &[&key])
            .await
            .expect("take the lock");
        assert!(
            !advisory_lock_held(&**observer, key).await.expect("probe"),
            "another database's lock is not this database's"
        );
        assert!(
            advisory_lock_held(&**holder, key).await.expect("probe"),
            "the holder's own database sees it"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::producer_singleton_lock_key;

    /// Issue #234: the whole point of the derivation — two instances in the
    /// same database must not contend for each other's singleton. Pure
    /// arithmetic, no database needed; the end-to-end proof that two
    /// instances can then actually both run lives in
    /// `trellis/tests/staging_ring.rs` and
    /// `generative/tests/two_instance_noise.rs`.
    #[test]
    fn different_schemas_get_different_keys() {
        assert_ne!(
            producer_singleton_lock_key("trellis"),
            producer_singleton_lock_key("trellis_a")
        );
        assert_ne!(
            producer_singleton_lock_key("trellis_a"),
            producer_singleton_lock_key("trellis_b")
        );
    }

    /// The other half of the contract: the same instance must always land on
    /// the same key, or the singleton would silently stop being enforced at
    /// all. Stability across *processes and builds* (not just within one
    /// call) is why this is FNV-1a rather than `DefaultHasher` — see
    /// `producer_singleton_lock_key`'s own doc comment.
    #[test]
    fn the_same_schema_always_gets_the_same_key() {
        assert_eq!(
            producer_singleton_lock_key("trellis"),
            producer_singleton_lock_key("trellis")
        );
        // A hardcoded expected value, so a future change to the derivation
        // that would silently move every deployed instance's key cannot pass
        // unnoticed — updating this number is the deliberate act of
        // acknowledging that rollout window (see the upgrade note on
        // `producer_singleton_lock_key`).
        assert_eq!(
            producer_singleton_lock_key(crate::config::DEFAULT_SCHEMA),
            EXPECTED_DEFAULT_SCHEMA_KEY
        );
    }

    /// `producer_singleton_lock_key(DEFAULT_SCHEMA)`, pinned as a literal
    /// (and named, so the assertion's failure message says what it is).
    const EXPECTED_DEFAULT_SCHEMA_KEY: i64 = -5_959_385_728_162_781_471;
}
