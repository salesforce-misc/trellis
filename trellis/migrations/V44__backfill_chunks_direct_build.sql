-- Issue #419 (ADR-0016): an aggregate or relationship-enriched 1-1
-- definition's direct set-based build (ADR-0007) runs as one background job
-- that the backfill discharge dispatches and a drain thread executes. The job
-- is a `backfill_chunks` row, so it reuses the queue's claim, heartbeat,
-- reclaim-stale and resume-staleness machinery unchanged.
--
-- A job builds the whole definition rather than one primary-key range, so it
-- has no bounds: `hi is null` marks it (and it has no `lo` either).
--
-- `prior_attempts` is the failure count of the marker whose discharge
-- dispatched the job (`pending_backfill.attempts`, V43). A job that fails
-- hands its build back to the discharge by re-parking that marker, and records
-- one more failure than this, so the marker's backoff keeps growing across
-- failed builds instead of restarting at every dispatch.
alter table backfill_chunks
    alter column hi drop not null,
    add column if not exists prior_attempts integer not null default 0,
    add constraint backfill_chunks_whole_build_unbounded check (hi is not null or lo is null);
