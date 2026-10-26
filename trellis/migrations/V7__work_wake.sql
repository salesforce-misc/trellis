-- Work-wake edges: the database announces every change that can make work
-- claimable, so an idle worker, which backs its wake off, is woken by
-- construction rather than by every code path remembering to send a NOTIFY.
-- Seals, discharges and applies send their own NOTIFY; the build work's edges
-- are rows changing, from many code paths (a plan job, a retry, a release, a
-- reclaim sweep, a definition moving to a dispatchable status), and a queued
-- self-check job is one more.
--
-- The channel is `<catalog schema>_work`, cut to Postgres's 63-byte limit by
-- whole characters (a longer schema name shares a channel with its cut twin,
-- which only costs the twin a spurious wake). It is baked into the functions
-- as a literal because a trigger runs under its writer's `search_path`, and
-- `work_wake_channel()` hands it to the workers that `LISTEN` on it, so SQL
-- is the one place the name is computed.
do $migration$
declare
    base text := current_schema();
    channel text := base || '_work';
begin
    while octet_length(channel) > 63 loop
        base := left(base, char_length(base) - 1);
        channel := base || '_work';
    end loop;

    execute format(
        'create function work_wake_channel() returns text language sql immutable as %L',
        format('select %L::text', channel));

    execute format($f$
        create function announce_chunk_work() returns trigger language plpgsql as $body$
        begin
            perform pg_notify(%L, '');
            return null;
        end
        $body$$f$, channel);

    execute format($f$
        create function announce_definition_work() returns trigger language plpgsql as $body$
        begin
            perform pg_notify(%L, '');
            return null;
        end
        $body$$f$, channel);
end
$migration$;

-- A chunk that is undone and unclaimed, new or changed: created by a plan or
-- a resume, released by its worker or by the reclaim sweep, its backoff
-- cleared by a retry, or its `fields` filled in. Claiming it, refreshing a
-- claim and finishing it are not edges to work, so they are not announced.
create trigger announce_chunk_insert
    after insert on backfill_chunks
    for each row
    when (not new.done and new.claimed_by is null)
    execute function announce_chunk_work();

create trigger announce_chunk_update
    after update on backfill_chunks
    for each row
    when (not new.done and new.claimed_by is null and (
        old.done
        or old.claimed_by is not null
        or old.next_attempt_at is distinct from new.next_attempt_at
        or old.fields is distinct from new.fields))
    execute function announce_chunk_work();

-- A definition entering (or changing within) a status the chunk claim hands
-- work in (`TransformStatus::dispatchable()`): a resume or an unfreeze makes
-- its existing chunks claimable without touching a chunk row.
create trigger announce_definition_insert
    after insert on transform_definitions
    for each row
    when (new.status in ('waiting_to_backfill', 'backfilling', 'catching_up', 'live'))
    execute function announce_definition_work();

create trigger announce_definition_update
    after update on transform_definitions
    for each row
    when (new.status in ('waiting_to_backfill', 'backfilling', 'catching_up', 'live')
          and (old.status is distinct from new.status or old.build is distinct from new.build))
    execute function announce_definition_work();

-- A queued `self_check` job announces itself: the one edge that makes a job
-- claimable at once is a row entering `queued`. A job a dead worker left
-- `running` becomes claimable by the clock, not by a write, and is found
-- within the idle backoff cap past its reclaim TTL. It sends on the work
-- channel above, with the name baked in as a literal for the same reason.
do $migration$
begin
    execute format($f$
        create function announce_self_check_work() returns trigger language plpgsql as $body$
        begin
            perform pg_notify(%L, '');
            return null;
        end
        $body$$f$, work_wake_channel());
end
$migration$;

create trigger announce_self_check_insert
    after insert on self_check_jobs
    for each row
    when (new.state = 'queued')
    execute function announce_self_check_work();
