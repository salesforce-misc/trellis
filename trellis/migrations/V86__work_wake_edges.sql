-- The edges that make build work claimable, announced by the database (#273).
--
-- An idle drain worker backs off its wake (`ClientOptions::idle_wake_cap`),
-- so every change that can make the build probe (`staging::build::work_probe`)
-- go from "nothing" to "something" must wake it. Seals, discharges and applies
-- already send a NOTIFY; the build work's edges are rows changing, from many
-- code paths (a plan job, a retry, a release, a reclaim sweep, a definition
-- moving to `backfilling`), so triggers announce them by construction: a new
-- path that makes a chunk claimable cannot forget to.
--
-- The channel is `<catalog schema>_work`, cut to Postgres's 63-byte limit by
-- whole characters (a longer schema name shares a channel with its cut
-- twin, which only costs the twin a spurious wake). It is baked into the
-- functions as a literal because a trigger runs under its writer's
-- `search_path`, and `work_wake_channel()` hands it to the workers that
-- `LISTEN` on it, so SQL is the one place the name is computed.
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

-- A definition entering (or changing within) the states a build runs in.
create trigger announce_definition_insert
    after insert on transform_definitions
    for each row
    when (new.status in ('backfilling', 'catching_up'))
    execute function announce_definition_work();

create trigger announce_definition_update
    after update on transform_definitions
    for each row
    when (new.status in ('backfilling', 'catching_up')
          and (old.status is distinct from new.status or old.build is distinct from new.build))
    execute function announce_definition_work();
