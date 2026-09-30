-- #687: what a definition is stuck on, as `Trellis::status` reports it.
--
-- 1. A capture failure records its own sentence.
--
-- A definition can now be paused for two schema changes (#622 C6): a
-- column it reads was renamed or dropped, or its source's primary key was
-- redefined without a rename. The pause records the sentence
-- `Trellis::status` reports as `capture_failure.error`, written where the
-- cause is known, rather than `status` rebuilding it from `columns`.
alter table capture_failures add column error text not null default '';
alter table capture_failures alter column error drop default;

-- 2. What holds each table's capture back, as the staging worker's latest
-- reconcile pass found it, so `Trellis::status` reports it from any process
-- (the user's Q5 decision), not only from the one running the staging
-- worker. Either a lock wait (the install, widen or uninstall couldn't take
-- the table's lock: `operation`, `lock_mode`, `observed_at` and `blockers`)
-- or another failure (no primary key, a failed statement: `error` and
-- `columns`). Nothing in the defined transforms re-derives either (#622
-- plan Q9): a wait describes other sessions, and a failure what the last
-- attempt met. Every pass rewrites the table: a row goes as soon as a pass
-- brings its table current, pauses its readers, or no longer captures it.
-- `since` is when a pass first met this wait (the same operation) or this
-- failure (the same error).
create table capture_holdups (
    table_name text primary key,
    since timestamptz not null,
    operation text,
    lock_mode text,
    observed_at timestamptz,
    blockers text[],
    error text,
    columns text[],
    check (
        (operation is not null and lock_mode is not null and observed_at is not null
            and blockers is not null and error is null and columns is null)
        or (operation is null and lock_mode is null and observed_at is null
            and blockers is null and error is not null and columns is not null)
    )
);
