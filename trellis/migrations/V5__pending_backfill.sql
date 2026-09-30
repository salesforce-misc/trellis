-- Publication reconciliation's durable follow-up (issue #8): adding a table
-- to the publication means its pre-existing rows are not in the replication
-- stream and must be staged by enumeration, and that enumeration must be as
-- durable as the `ALTER PUBLICATION` that requires it. See
-- docs/staging-and-claiming/01-intake-and-lsn-confirmation.md ("Adding a
-- table to the publication needs a durable follow-up") and
-- `trellis::intake::markers`.
--
-- `fence_snapshot` is a transaction fence, captured in the same transaction
-- as the `ALTER`: backfill enumeration must wait until every transaction in
-- flight at that instant has settled (committed or aborted), or a
-- straggling writer's row could land in neither the enumeration nor the
-- (already-live) replication stream — the one gap this design cannot
-- tolerate.
--
-- A row here is deleted in the same transaction as the staging commit that
-- discharges it (see `run_pending_backfills`), so a crash between the
-- `ALTER` and the backfill leaves the marker in place and the next setup
-- pass retries it — never silently drops it.
create table if not exists pending_backfill (
    table_name text primary key,
    fence_snapshot pg_snapshot not null,
    added_at timestamptz not null default now()
);
