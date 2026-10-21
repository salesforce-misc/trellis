-- #1205: a definition paused because the database was restored from a
-- logical dump (`pg_dump`, `pg_restore`). Its ordering state (the ledger's
-- `__applied_lsn` and `__basis`, a group basis's `__basis` and `__stamp`,
-- truncate floors) holds the original database's WAL
-- positions and transaction ids, which mean nothing in this one, so a resume
-- of such a definition resets that state before its rebuild
-- (`staging::quarantine`). An ordinary resume keeps it.
--
-- Set with the pause (`capture::restore`); the row's `kind` is `disruption`
-- for a definition that was running, so a test helper that resumes
-- disruptions resumes these too.
alter table capture_failures add column restored boolean not null default false;

-- Both tables store the oid of their own table, the restore signal, so a
-- copy of their rows into a new table reads as a restore.
comment on table partition_sources is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore (their catalog_oid no longer names this '
    'table), which pauses every definition.';

comment on table capture_triggers is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore (their catalog_oid no longer names this '
    'table), which pauses every definition.';
