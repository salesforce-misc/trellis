//! Claim liveness (issue #15, stage 04's third piece): keeping a claim
//! alive across a drain, the two ways a claim comes back (release on error,
//! reclaim on TTL), and the loop's own backoff on consecutive fence misses.
//! See docs/staging-and-claiming/04-claiming-and-the-fold.md, "Keeping a
//! claim alive" and "Two ways a claim comes back" — this module implements
//! each in that order below.
//!
//! The fleet-wide pause lease that once lived here (`acquire_pause_lease`,
//! `heartbeat_pause_lease`, `release_pause_lease`, `claiming_is_paused`,
//! `claim_unless_paused`) was deleted per issue #191: its sole documented
//! consumer, a self-check auditor's quiescent read, shipped as
//! `Trellis::self_check` (ADR-0013) and deliberately gets quiescence from a
//! watermark-await + snapshot + re-check instead, never pausing claiming
//! fleet-wide. `heartbeat_inline` (zero callers anywhere) was deleted in
//! the same change.
//!
//! No claim-epoch column exists anywhere here. `seg_claims` row identity
//! *is* the epoch: [`release`] and [`reclaim_stale`] both work by deleting
//! the claim row, so a stale claimant's later completion attempt (issue
//! #11, scoped `claimed_by = me`) simply matches zero rows once the row is
//! gone. Deleting is the epoch bump.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Mutex as AsyncMutex;
use tokio::time::MissedTickBehavior;
use tokio_postgres::{Client, GenericClient, NoTls};

use super::error::StagingError;

// ---------------------------------------------------------------------
// Release on error
// ---------------------------------------------------------------------

/// Releases a claim immediately (doc 04, "Two ways a claim comes back" —
/// released), called by the worker itself on **any** drain error. Nothing
/// was applied: a fold error precedes every write and an apply error rolls
/// back, so the claim being released covers work that never happened. This
/// is what makes a fence miss — the routine, immediately-retryable failure
/// every definition change trips on any worker whose loaded schema
/// predates it — instantly re-claimable instead of parked behind the
/// reclaim TTL for up to 30 s.
///
/// Scoped `claimed_by = $2`, so a claim already reclaimed by the TTL sweep
/// or taken over by another worker is left untouched: a caller that lost
/// its claim out from under itself must not delete the *new* claimant's
/// row. Returns the number of rows deleted (0 or 1) so the caller can tell
/// "I released my own claim" from "I no longer held it, someone else does
/// now" — the latter should retry in place rather than re-claim.
pub async fn release(
    client: &impl GenericClient,
    seg_seq: i64,
    claimed_by: &str,
) -> Result<u64, StagingError> {
    let n = client
        .execute(
            "delete from seg_claims where seg_seq = $1 and claimed_by = $2",
            &[&seg_seq, &claimed_by],
        )
        .await?;
    Ok(n)
}

// ---------------------------------------------------------------------
// Reclaim on TTL
// ---------------------------------------------------------------------

/// Sweeps every claim whose `claimed_at` is older than `ttl` (doc 04, "Two
/// ways a claim comes back" — reclaimed): the backstop for a worker that
/// *died* and could never release. A `WITH ... DELETE` so the row selection
/// and the delete are one statement (matching [`super::claim::CLAIM_SQL`]'s
/// convention of never splitting a claim-table mutation across
/// statements): the `for update skip locked` on the `dead` CTE is the
/// mirror image of the reason [`HeartbeatDaemon`]'s own refresh takes the
/// same lock mode — a row locked by its own claimant's in-flight apply
/// transaction is not a dead claimant, and the sweep must never block
/// behind one. It simply skips that row this pass; it will be swept later
/// once the apply either commits (heartbeat already refreshed it) or the
/// worker really did die (the lock releases with the crashed connection).
///
/// Deleting a stale claim only frees its buckets — the batch itself stays
/// `draining` (this never touches `segments`), and the existing
/// [`claim`](super::claim::claim) call re-picks the freed buckets normally
/// on its next invocation.
const RECLAIM_STALE_SQL: &str = "\
    with dead as ( \
        select ctid from seg_claims \
        where claimed_at < now() - (interval '1 second' * $1) \
        for update skip locked \
    ) \
    delete from seg_claims where ctid in (select ctid from dead)";

/// The fleet's default reclaim TTL: how long a claim (or, since issue #144,
/// a [`super::worker_registry`] row) may go unrefreshed before it's treated
/// as dead. [`crate::client::ClientOptions::default`] uses this for its own
/// `reclaim_ttl` field; [`crate::app::Trellis::has_live_drain_workers`] uses
/// it directly, since [`crate::app::TrellisOptions`] has no knob of its own
/// to override it with — every `Trellis::connect` call already gets this
/// same value today, whether or not that particular connection starts a
/// background `Client`. Kept here, one constant, rather than the literal
/// `30` duplicated at each call site — issue #144's explicit instruction to
/// reuse this notion of liveness rather than invent a second one.
pub const DEFAULT_RECLAIM_TTL: Duration = Duration::from_secs(30);

/// Runs [`RECLAIM_STALE_SQL`] and returns how many claims it reclaimed.
pub async fn reclaim_stale(
    client: &impl GenericClient,
    ttl: Duration,
) -> Result<u64, StagingError> {
    let ttl_secs = ttl.as_secs_f64();
    let n = client.execute(RECLAIM_STALE_SQL, &[&ttl_secs]).await?;
    Ok(n)
}

// ---------------------------------------------------------------------
// Consecutive-fence-miss backoff
// ---------------------------------------------------------------------

/// The first *non-zero* delay in the backoff — the wait after the second
/// consecutive miss. The very first miss waits `0` (that zero comes from
/// [`FenceMissBackoff::next_delay`]'s `None` branch, not this constant), so
/// read-your-writes stays fast on an isolated miss; only a run of misses
/// throttles (doc 04, "Two ways a claim comes back": release removing the
/// TTL's accidental rate-limiter role means the loop must throttle *itself*
/// on consecutive misses).
pub const FENCE_MISS_INITIAL_DELAY: Duration = Duration::from_millis(10);

/// The ceiling [`FenceMissBackoff::next_delay`] never exceeds, so a
/// definition the reload never resolves is re-claimed at a bounded rate
/// instead of hot-looping.
pub const FENCE_MISS_MAX_DELAY: Duration = Duration::from_secs(1);

/// Pure, no-DB backoff state for a drain loop's own fence-miss retries
/// (doc 04, "Two ways a claim comes back"). `0` on the first miss, then
/// doubling from [`FENCE_MISS_INITIAL_DELAY`] up to [`FENCE_MISS_MAX_DELAY`],
/// reset by any clean drain. The drain loop itself is a later stage
/// (unassembled today, issue #11, blocked on aggregate transform-defs);
/// this is just the piece of state it will need.
#[derive(Debug, Clone)]
pub struct FenceMissBackoff {
    /// The delay `next_delay` will return *after* the one it's about to
    /// return. `None` means "no miss recorded yet" — the very next call
    /// returns zero.
    pending: Option<Duration>,
}

impl Default for FenceMissBackoff {
    fn default() -> Self {
        Self::new()
    }
}

impl FenceMissBackoff {
    pub fn new() -> Self {
        Self { pending: None }
    }

    /// Returns the delay to wait before the next retry, then advances the
    /// sequence for the call after that. First call (or the first call
    /// after [`Self::reset`]) returns [`Duration::ZERO`]; every call after
    /// that doubles the previous non-zero delay, starting from
    /// [`FENCE_MISS_INITIAL_DELAY`] and capped at [`FENCE_MISS_MAX_DELAY`].
    pub fn next_delay(&mut self) -> Duration {
        match self.pending {
            None => {
                self.pending = Some(FENCE_MISS_INITIAL_DELAY);
                Duration::ZERO
            }
            Some(delay) => {
                self.pending = Some((delay * 2).min(FENCE_MISS_MAX_DELAY));
                delay
            }
        }
    }

    /// Called on any clean drain: the next miss (if any) is treated as
    /// isolated again, not a continuation of a prior run of misses.
    pub fn reset(&mut self) {
        self.pending = None;
    }
}

#[cfg(test)]
mod fence_miss_backoff_tests {
    use super::*;

    #[test]
    fn sequence_is_zero_then_doubling_capped_at_one_second() {
        let mut backoff = FenceMissBackoff::new();
        assert_eq!(backoff.next_delay(), Duration::ZERO);
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
        assert_eq!(backoff.next_delay(), Duration::from_millis(40));
        assert_eq!(backoff.next_delay(), Duration::from_millis(80));
        assert_eq!(backoff.next_delay(), Duration::from_millis(160));
        assert_eq!(backoff.next_delay(), Duration::from_millis(320));
        assert_eq!(backoff.next_delay(), Duration::from_millis(640));
        // 640ms * 2 = 1280ms, capped at 1s.
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }

    #[test]
    fn reset_returns_to_the_isolated_miss_shape() {
        let mut backoff = FenceMissBackoff::new();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        assert_ne!(backoff.next_delay(), Duration::ZERO);

        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::ZERO);
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
    }
}

// ---------------------------------------------------------------------
// Out-of-band heartbeat daemon
// ---------------------------------------------------------------------

/// Refreshes `claimed_at` for every registered `(seg_seq, claimed_by)` pair
/// in one statement (doc 04, "Keeping a claim alive" — the out-of-band
/// half). `targets` unnests the two parallel arrays bound as `$1`/`$2` into
/// rows, `locked` takes them `for update skip locked` for the same
/// mirror-image reason [`RECLAIM_STALE_SQL`] does — a row an in-flight
/// apply holds is yielded on, not waited for — and the outer `update ...
/// from locked` only touches the rows that lock actually won.
const DAEMON_REFRESH_SQL: &str = "\
    with targets(seg_seq, claimed_by) as ( \
        select * from unnest($1::bigint[], $2::text[]) \
    ), \
    locked as ( \
        select c.seg_seq, c.bucket \
        from seg_claims c \
        join targets t on t.seg_seq = c.seg_seq and t.claimed_by = c.claimed_by \
        for update skip locked \
    ) \
    update seg_claims c \
    set claimed_at = now() \
    from locked l \
    where c.seg_seq = l.seg_seq and c.bucket = l.bucket";

/// [`HeartbeatDaemon`] tuning. Defaults match doc 04's numbers (5 s
/// interval, "exits after a minute idle"); tests use much shorter windows
/// so they run fast against a real cluster.
#[derive(Debug, Clone)]
pub struct HeartbeatDaemonConfig {
    /// How often the daemon refreshes every registered claim in one
    /// statement. Must be ≪ the reclaim TTL — the doc's invariant.
    pub interval: Duration,
    /// How long the daemon may sit with an empty registry before it closes
    /// its connection. The daemon keeps ticking (so a later `register`
    /// still gets picked up and reopens a connection); only the connection
    /// itself is torn down.
    pub idle_timeout: Duration,
}

impl Default for HeartbeatDaemonConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
        }
    }
}

type Registry = Arc<AsyncMutex<HashSet<(i64, String)>>>;

/// A process-wide daemon, on its own dedicated connection, that keeps every
/// registered claim's `claimed_at` fresh against wall-clock time rather
/// than against how much work any one drain is doing (doc 04, "Keeping a
/// claim alive" — this is why the bulk shape doesn't livelock the reclaim
/// sweep).
///
/// Cheap and safe by construction, not by a special case: the daemon's
/// background task only inspects its registry once per `interval`, on a
/// plain timer tick. A claim that's registered and deregistered *between*
/// two ticks is never observed mid-interval, so it never causes a
/// connection to open — "no connection until a claim has been registered
/// for a full interval" falls out of tick-sampling for free, rather than
/// needing separate bookkeeping to detect it. Symmetrically, once the
/// registry has been empty across enough consecutive ticks to exceed
/// `idle_timeout`, the daemon drops its connection (observable via
/// [`HeartbeatDaemon::is_connected`]) — but keeps ticking indefinitely, so a
/// claim registered much later still gets picked up and reopens one.
pub struct HeartbeatDaemon {
    registry: Registry,
    // Read only by `connections_opened`/`is_connected`, which exist for
    // `tests/liveness.rs`'s lazy-connect and idle-exit assertions. The
    // spawned task keeps its own `Arc` clones, so a build without the
    // `internals` feature simply doesn't carry these handles.
    #[cfg(any(test, feature = "internals"))]
    connections_opened: Arc<AtomicU64>,
    #[cfg(any(test, feature = "internals"))]
    connected: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl HeartbeatDaemon {
    /// Spawns the background task against `dsn`, with `search_path` pinned
    /// to `schema` on the connection it opens (mirroring
    /// [`super::session::ProducerSession::connect`]) — connecting lazily,
    /// the first time a tick observes a non-empty registry, per this
    /// struct's own doc comment.
    pub fn spawn(
        dsn: impl Into<String>,
        schema: impl Into<String>,
        config: HeartbeatDaemonConfig,
    ) -> Self {
        let registry: Registry = Arc::new(AsyncMutex::new(HashSet::new()));
        let connections_opened = Arc::new(AtomicU64::new(0));
        let connected = Arc::new(AtomicBool::new(false));

        let task = tokio::spawn(run_daemon(
            dsn.into(),
            schema.into(),
            registry.clone(),
            config,
            connections_opened.clone(),
            connected.clone(),
        ));

        Self {
            registry,
            #[cfg(any(test, feature = "internals"))]
            connections_opened,
            #[cfg(any(test, feature = "internals"))]
            connected,
            task,
        }
    }

    /// Registers a claim the daemon is now responsible for refreshing.
    pub async fn register(&self, seg_seq: i64, claimed_by: impl Into<String>) {
        self.registry
            .lock()
            .await
            .insert((seg_seq, claimed_by.into()));
    }

    /// Stops the daemon refreshing a claim — called once a drain releases
    /// or completes it, so the daemon doesn't keep refreshing a claim
    /// nothing owns anymore.
    pub async fn deregister(&self, seg_seq: i64, claimed_by: &str) {
        self.registry
            .lock()
            .await
            .remove(&(seg_seq, claimed_by.to_string()));
    }

    /// How many times the daemon has opened a connection over its whole
    /// lifetime. Test observability for the lazy-connect claim: it should
    /// stay `0` for a claim that never outlived one full interval.
    #[cfg(any(test, feature = "internals"))]
    pub fn connections_opened(&self) -> u64 {
        self.connections_opened.load(Ordering::Relaxed)
    }

    /// Whether the daemon currently holds an open connection. Test
    /// observability for the idle-exit claim.
    #[cfg(any(test, feature = "internals"))]
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
}

impl Drop for HeartbeatDaemon {
    fn drop(&mut self) {
        // The registry and its refreshes are only meaningful while
        // something holds this handle; nothing here should outlive it.
        self.task.abort();
    }
}

async fn open_daemon_connection(dsn: &str, schema: &str) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {}, public",
            crate::pool::quote_ident(schema)
        ))
        .await?;
    Ok(client)
}

async fn run_daemon(
    dsn: String,
    schema: String,
    registry: Registry,
    config: HeartbeatDaemonConfig,
    connections_opened: Arc<AtomicU64>,
    connected: Arc<AtomicBool>,
) {
    let mut client: Option<Client> = None;
    let mut idle_since: Option<Instant> = None;

    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let snapshot: Vec<(i64, String)> = {
            let guard = registry.lock().await;
            guard.iter().cloned().collect()
        };

        if snapshot.is_empty() {
            let since = idle_since.get_or_insert_with(Instant::now);
            if client.is_some() && since.elapsed() >= config.idle_timeout {
                client = None;
                connected.store(false, Ordering::Relaxed);
            }
            continue;
        }
        idle_since = None;

        if client.is_none() {
            match open_daemon_connection(&dsn, &schema).await {
                Ok(c) => {
                    client = Some(c);
                    connections_opened.fetch_add(1, Ordering::Relaxed);
                    connected.store(true, Ordering::Relaxed);
                }
                Err(_) => continue, // best-effort; retry next tick
            }
        }

        if let Some(c) = &client {
            let seg_seqs: Vec<i64> = snapshot.iter().map(|(s, _)| *s).collect();
            let claimed_bys: Vec<String> = snapshot.iter().map(|(_, w)| w.clone()).collect();
            // A *missed* refresh (transient) costs one interval of staleness,
            // not correctness — the reclaim TTL is deliberately far above the
            // interval, so one dropped refresh is not itself fatal. But a
            // *dead* connection (PG failover, network blip, idle server-side
            // kill) fails every subsequent refresh forever while claims stay
            // registered, letting `claimed_at` go stale until the reclaim
            // sweep takes a live claim — the exact bulk-drain livelock this
            // daemon exists to prevent, and one the idle-timeout reset path
            // never clears because the registry is non-empty. So on any
            // execute error we drop the connection and flip `connected`
            // false; the next tick reopens against the still-non-empty
            // registry (bumping `connections_opened`) and resumes refreshing.
            if c.execute(DAEMON_REFRESH_SQL, &[&seg_seqs, &claimed_bys])
                .await
                .is_err()
            {
                client = None;
                connected.store(false, Ordering::Relaxed);
            }
        }
    }
}
