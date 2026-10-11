//! Worker registry (issue #144; docs/decisions/0010-embeddable-clients.md,
//! decision 3): a row per live [`Client`](crate::client::Client)
//! process/connection running with `drain_threads > 0` (application
//! workers), independent of whether it currently holds any claim.
//!
//! ADR-0008 decision 1 names the failure mode this exists to detect: a
//! fleet where every connection runs `drain_threads: 0` leaves every
//! transform sitting in `waiting_to_backfill` forever — nothing errors,
//! nothing looks broken, the pipeline just never starts. [`super::liveness`]
//! heartbeats *claims* (one row per claimed work item, present only while a
//! worker is mid-batch), so a worker that's running, healthy, and idle
//! leaves no trace there — an empty claim table is indistinguishable from an
//! empty fleet. This module is the fix: [`register_worker`] upserts one row
//! per worker at `Client::start` and again every third of the reclaim TTL
//! by the client's one upkeep task (which runs only while an app-worker task
//! does, and is independent of how long a worker spends inside one drain),
//! [`deregister_worker`] removes it on clean shutdown, and
//! [`has_live_workers`] is the cheap, health-check-shaped read
//! `Trellis::has_live_drain_workers` sits behind.
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

use super::error::StagingError;

/// Upserts `worker_id`'s row, bumping `last_seen` to now. Called once at
/// `Client::start` (registration, only when `application_threads > 0`) and
/// again from the client's upkeep task (heartbeat) — one call serves both, the
/// same shape [`super::claim::register_drainer`] already uses for a
/// different registry.
pub async fn register_worker(
    client: &impl GenericClient,
    worker_id: &str,
) -> Result<(), StagingError> {
    client
        .execute(
            "insert into worker_registry (worker_id, registered_at, last_seen) \
             values ($1, now(), now()) \
             on conflict (worker_id) do update set last_seen = excluded.last_seen",
            &[&worker_id],
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

/// Whether at least one worker's `last_seen` is within `ttl` of now — the
/// single, cheap `exists(...)` query (no joins) `Trellis::has_live_drain_workers`
/// sits behind, meant to run behind an application health check on a timer.
/// See the module doc comment for why this is a read-time comparison rather
/// than something that depends on [`reclaim_stale_workers`] having run.
pub async fn has_live_workers(
    client: &impl GenericClient,
    ttl: Duration,
) -> Result<bool, StagingError> {
    let ttl_secs = ttl.as_secs_f64();
    let live: bool = client
        .query_one(
            "select exists(select 1 from worker_registry \
             where last_seen > now() - (interval '1 second' * $1))",
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
        .execute(RECLAIM_STALE_WORKERS_SQL, &[&ttl_secs])
        .await?;
    Ok(n)
}
