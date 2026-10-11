-- `self_check` as a background job (#1023, #599).
--
-- `Trellis::self_check` registers a row here and returns; a process that runs
-- drain workers (`ClientOptions::application_threads`) claims it on a task
-- beside them and compares the target against a fresh recompute one keyset
-- page per pass (`staging::self_check_job`). The caller polls the row by `id`.
--
-- * `state` is `queued` (registered, no worker yet), `running` (a worker holds
--   it), then one of `done` (the comparison ended; `outcome` says how),
--   `failed` (`error` says why) or `cancelled` (a worker shut down under it).
-- * `claimed_by` / `claimed_at` are the holder and the time of its last page.
--   A job whose holder stopped refreshing it is taken over by the next
--   worker, which resumes at `after_key`.
-- * `after_key` is the keyset cursor of the next page, `rows_compared` how
--   many keys the pages so far compared, and `divergences` the ones they
--   found (`staging::self_check::Divergence`, as JSON).
--
-- Dropping the transform deletes the row, which ends the job: its worker's
-- next write finds nothing.
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
    divergences jsonb not null default '[]',
    outcome text check (outcome in ('converged', 'not_caught_up', 'not_live', 'diverged')),
    not_live_status text,
    checked_through pg_lsn,
    truncated boolean not null default false,
    error text,
    created_at timestamptz not null default now(),
    finished_at timestamptz
);

-- One unfinished job per definition: a second `self_check` of a target whose
-- job is still running gets that job back. A finished job stays until the
-- next `self_check` of the target replaces it.
create unique index self_check_jobs_one_unfinished
    on self_check_jobs (definition_id)
    where state in ('queued', 'running');
