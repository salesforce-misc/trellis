-- Every reason a definition is paused for, and what `RESUME` would do now
-- (`staging::pause_message`).
--
-- `capture_failure_reasons` holds one row per reason, in the order they were
-- found (`position`): the sentence, the table and columns it is about, and
-- `recheck`, how the staging worker's pass tells whether its cause is still
-- there: `columns` (the columns are missing from the table), `key` (the
-- table's primary key is still not `columns`), or `NULL` (it stands until a
-- resume). The state a resume needs from a reason stays on
-- `capture_failures` (`restored`, `relabel_lsn`, `caused_by`), so a second
-- reason never drops the first's.
--
-- `capture_failures.error` is the message `Trellis::status` reports: every
-- reason, the newest first, then `resume_outcome`, which the pass writes from
-- what a resume would do as the schema stands. `refresh_stamp` is a digest
-- of the definition and the fingerprints of the relations it reads when the
-- pass last wrote them; a pause clears it, and the pass evaluates the
-- definition again only when its stamp differs.
--
-- Runs again harmlessly (a test re-runs the migrations after V15).
--
-- Declares no catalog floor: an older release reads `capture_failures` as
-- before and never reads the reasons; this release recomposes a message it
-- didn't write as a single reason.
alter table capture_failures
    add column if not exists resume_outcome text,
    add column if not exists refresh_stamp text;

create table if not exists capture_failure_reasons (
    transform_id bigint not null
        references capture_failures (transform_id) on delete cascade,
    position integer not null,
    source_table text not null,
    columns text[] not null,
    reason text not null,
    recheck text,
    primary key (transform_id, position),
    constraint capture_failure_reasons_recheck_check check (recheck in ('columns', 'key'))
);

insert into capture_failure_reasons (transform_id, position, source_table, columns, reason)
select transform_id, 1, source_table, columns, error from capture_failures
on conflict do nothing;
