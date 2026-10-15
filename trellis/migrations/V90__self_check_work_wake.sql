-- A queued `self_check` job announces itself (#1075).
--
-- The `self_check` worker (`client::self_check_loop`) no longer polls the
-- claim at the poll floor while no job exists: like an idle drain worker it
-- backs its wait off to `ClientOptions::idle_wake_cap`. The wake that makes
-- that safe is this trigger, on the one edge that makes a job claimable at
-- once: a row entering `queued` (a `self_check` call). A job a dead worker
-- left `running` becomes claimable by the clock, not by a write, and is
-- found within the backoff cap past its reclaim TTL.
--
-- It sends on the work channel V86 created, which every worker already
-- listens on. The name is baked into the function as a literal, as in V86,
-- because a trigger runs under its writer's `search_path`.
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
