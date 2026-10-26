-- Self-check jobs: one row per `self_check` call, run by a background worker.
--
-- `mode` is `standard` or `strict`; `await_timeout_ms` is how long the job
-- waits for the target to catch up. `state` runs `queued` -> `running` ->
-- `done` | `failed` | `cancelled`; `claimed_by` and `claimed_at` name the
-- worker (a job a dead worker left `running` is reclaimed by the clock).
-- `after_key` is the paging cursor, `rows_compared` the count so far,
-- `divergences` what was found, and `outcome` the verdict (`not_live_status`
-- the status of a definition that was not live). `checked_through` is the WAL
-- position the comparison covers and `truncated` whether the divergence list
-- was cut.
create table self_check_jobs (
    id bigint generated always as identity primary key,
    definition_id bigint not null references transform_definitions (id) on delete cascade,
    mode text not null check (mode in ('standard', 'strict')),
    await_timeout_ms bigint not null check (await_timeout_ms >= 0),
    state text not null check (state in ('queued', 'running', 'done', 'failed', 'cancelled')),
    claimed_by text,
    claimed_at timestamptz,
    after_key text,
    rows_compared bigint not null default 0,
    divergences jsonb not null default '[]'::jsonb,
    outcome text check (outcome in ('converged', 'not_caught_up', 'not_live', 'diverged')),
    not_live_status text,
    checked_through pg_lsn,
    truncated boolean not null default false,
    error text,
    created_at timestamptz not null default now(),
    finished_at timestamptz
);

-- At most one unfinished job per definition.
create unique index self_check_jobs_one_unfinished
    on self_check_jobs (definition_id)
    where state in ('queued', 'running');
