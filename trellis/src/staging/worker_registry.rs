//! Worker registry (docs/decisions/0010-embeddable-clients.md,
//! decision 3, and docs/decisions/0023-version-handover.md): a row per live
//! background [`Client`](crate::client::Client) process, a staging-only one
//! included, with the version of its binary and its roles, independent of
//! whether it currently holds any claim.
//!
//! ADR-0008 decision 1 names the failure mode the drain role exists to
//! detect: a fleet where every connection runs `drain_threads: 0` leaves every
//! transform sitting in `waiting_to_backfill` forever — nothing errors,
//! nothing looks broken, the pipeline just never starts. [`super::liveness`]
//! heartbeats *claims* (one row per claimed work item, present only while a
//! worker is mid-batch), so a worker that's running, healthy, and idle
//! leaves no trace there — an empty claim table is indistinguishable from an
//! empty fleet. This module is the fix: [`register_worker`] upserts one row
//! per process at `Client::start` and again on every handover pass of the
//! client (`crate::handover`), [`deregister_worker`] removes it on clean
//! shutdown, and [`has_live_workers`] is the cheap, health-check-shaped read
//! `Trellis::has_live_drain_workers` sits behind: it counts the rows whose
//! drain threads run.
//!
//! The version and roles are what version handover reads: a process of one
//! version stands by while a process of another is live. [`claim_staging`]
//! names the session a process takes the staging-worker lock on, so the
//! lock's holder can be matched to its row.
//!
//! **Reuses the reclaim TTL's notion of liveness rather than inventing a
//! second one** — the issue's own instruction. [`has_live_workers`] does
//! *not* run a delete-based reclaim pass first; it compares `last_seen`
//! against the caller-supplied `ttl` directly in the read query, the exact
//! threshold [`super::liveness::reclaim_stale`] already compares a claim's
//! `claimed_at` against (see [`super::liveness::DEFAULT_RECLAIM_TTL`], the
//! same default value [`crate::client::ClientOptions::default`]'s own
//! `reclaim_ttl` uses). That makes the health check correct with zero
//! dependency on some other reclaim task having already run — which matters
//! because the fleet this feature most needs to get right (every
//! `drain_threads` at 0, so `Client::run`'s `maintenance_loop` never even
//! spawns — see that function's own doc comment) is also a fleet where
//! nothing would ever run a reclaim pass to begin with. A design that needed
//! reclaim-before-read would be wrong in precisely the case that matters
//! most. [`reclaim_stale_workers`] still exists — wired into
//! `maintenance_loop` next to every other table's own sweep — purely for
//! table hygiene (bounding row growth after an unclean shutdown);
//! [`has_live_workers`]'s correctness never depends on it having run.

use std::time::Duration;

use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use super::error::StagingError;

/// What a process registers: its id, the version of its binary and its
/// roles (ADR-0023). Every background process registers, a staging-only one
/// included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration<'a> {
    pub worker_id: &'a str,
    /// The binary's version, the exact crate version string.
    pub version: &'a str,
    /// It runs the staging worker (leading or standing by for the lock).
    pub staging: bool,
    /// Drain threads are running in it.
    pub drain: bool,
    /// It stands by for another version: it stages, drains and builds
    /// nothing.
    pub standby: bool,
    /// The backend pid of the session it holds the staging-worker lock on,
    /// written only when the row is inserted (a heartbeat leaves the
    /// column to [`claim_staging`] and [`clear_staging`]).
    pub staging_pid: Option<i32>,
}

impl<'a> Registration<'a> {
    /// A drain-only process of this build's version, active.
    #[cfg_attr(not(feature = "internals"), allow(dead_code))]
    pub fn drain_worker(worker_id: &'a str) -> Self {
        Registration {
            worker_id,
            version: crate::handover::BINARY_VERSION,
            staging: false,
            drain: true,
            standby: false,
            staging_pid: None,
        }
    }
}

/// Upserts `worker.worker_id`'s row, bumping `last_seen` to now and recording
/// the version and roles. Called by a client's first handover pass
/// (registration) and every pass after it (heartbeat) — one call serves both,
/// the same shape [`super::claim::register_drainers`] already uses for a
/// different registry.
pub async fn register_worker(
    client: &impl GenericClient,
    worker: &Registration<'_>,
) -> Result<(), StagingError> {
    client
        .execute(
            "insert into worker_registry \
               (worker_id, registered_at, last_seen, version, staging, drain, standby, staging_pid) \
             values ($1, now(), now(), $2, $3, $4, $5, $6) \
             on conflict (worker_id) do update set last_seen = excluded.last_seen, \
               version = excluded.version, staging = excluded.staging, \
               drain = excluded.drain, standby = excluded.standby",
            &[
                &worker.worker_id,
                &worker.version,
                &worker.staging,
                &worker.drain,
                &worker.standby,
                &worker.staging_pid,
            ],
        )
        .await?;
    Ok(())
}

/// Names the session `client` is as `worker_id`'s staging session
/// (`staging_pid`), before that session takes the staging-worker lock, so
/// whoever sees the lock held finds the row of the process holding it.
/// Returns the session's backend pid, or `None` when `worker_id` has no row
/// (it must register before it leads).
pub async fn claim_staging(
    client: &impl GenericClient,
    worker_id: &str,
) -> Result<Option<i32>, StagingError> {
    Ok(client
        .query_opt(
            "update worker_registry set staging_pid = pg_backend_pid() \
             where worker_id = $1 returning staging_pid",
            &[&worker_id],
        )
        .await?
        .map(|row| row.get(0)))
}

/// Clears `worker_id`'s `staging_pid` when it is still `pid`: the process
/// released the staging-worker lock, lost its session, or was refused it.
pub async fn clear_staging(
    client: &impl GenericClient,
    worker_id: &str,
    pid: i32,
) -> Result<(), StagingError> {
    client
        .execute(
            "update worker_registry set staging_pid = null \
             where worker_id = $1 and staging_pid = $2",
            &[&worker_id, &pid],
        )
        .await?;
    Ok(())
}

/// Deletes `worker_id`'s row outright — a clean shutdown, called once
/// `Client::run`'s own shutdown path has joined every task it started.
/// Scoped by primary key, so it can never touch another worker's row.
pub async fn deregister_worker(
    client: &impl GenericClient,
    worker_id: &str,
) -> Result<(), StagingError> {
    client
        .execute(
            "delete from worker_registry where worker_id = $1",
            &[&worker_id],
        )
        .await?;
    Ok(())
}

/// Whether at least one drain worker's `last_seen` is within `ttl` of now
/// (a row with drain threads running that doesn't stand by for another
/// version, or a row of a release before 0.2.0, where only drain workers
/// registered) — the
/// single, cheap `exists(...)` query (no joins) `Trellis::has_live_drain_workers`
/// sits behind, meant to run behind an application health check on a timer.
/// See the module doc comment for why this is a read-time comparison rather
/// than something that depends on [`reclaim_stale_workers`] having run.
pub async fn has_live_workers(
    client: &impl GenericClient,
    ttl: Duration,
) -> Result<bool, StagingError> {
    let ttl_secs = ttl.as_secs_f64();
    // Read through `to_jsonb`, so a registry migration V9 hasn't reached yet
    // reads as rows of a release before it, each one a drain worker.
    let live: bool = client
        .query_one(
            "select exists(select 1 from worker_registry w, lateral to_jsonb(w) j \
             where w.last_seen > now() - (interval '1 second' * $1) \
               and (j->>'version' is null \
                    or ((j->>'drain')::boolean and not (j->>'standby')::boolean)))",
            &[&ttl_secs],
        )
        .await?
        .get(0);
    Ok(live)
}

/// The `WITH ... DELETE` sweep behind [`reclaim_stale_workers`], mirroring
/// `super::liveness`'s own `RECLAIM_STALE_SQL` shape — including the `for
/// update skip locked`: a row a concurrent heartbeat is mid-upsert on is not
/// a dead worker, and this sweep must never block behind one.
const RECLAIM_STALE_WORKERS_SQL: &str = "\
    with dead as ( \
        select worker_id from worker_registry \
        where last_seen < now() - (interval '1 second' * $1) \
        for update skip locked \
    ) \
    delete from worker_registry where worker_id in (select worker_id from dead)";

/// Runs `RECLAIM_STALE_WORKERS_SQL`: sweeps every worker row whose
/// `last_seen` is older than `ttl`, returning how many it removed. Purely
/// table hygiene after an unclean shutdown elsewhere in the fleet (crash,
/// `kill -9`, a dropped `Client` that never called `shutdown`) — it bounds
/// the table's growth and nothing more. `maintenance_loop` passes its
/// `reclaim_ttl`, but never less than [`super::liveness::DEFAULT_RECLAIM_TTL`]
/// (what `Trellis::has_live_drain_workers` reads), so it never deletes a row
/// that check still counts as live.
///
/// [`has_live_workers`] never depends on this having run — see the module
/// doc comment for why requiring a reclaim pass before the read would be
/// wrong in precisely the fleet this feature exists to catch.
pub async fn reclaim_stale_workers(
    client: &impl GenericClient,
    ttl: Duration,
) -> Result<u64, StagingError> {
    let ttl_secs = ttl.as_secs_f64();
    let n = client
        .execute_typed(RECLAIM_STALE_WORKERS_SQL, &[(&ttl_secs, Type::FLOAT8)])
        .await?;
    Ok(n)
}
