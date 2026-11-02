-- Version handover (ADR-0023): every background process registers with the
-- version of the binary it runs and what it does, so a process of one version
-- can tell that another version is live and stand by until it is gone.
--
-- `version` is the crate version, the exact string; a row without one was
-- written by a release that predates this migration. `staging` and `drain` are
-- the process's roles as configured. `standby` is set while the process stands
-- by for another version (it then stages, drains and builds nothing).
-- `staging_pid` is the backend pid of the session the process holds (or is
-- taking) the staging-worker lock on, so the lock's holder can be matched to
-- its row.
alter table worker_registry
    add column version text,
    add column staging boolean not null default false,
    add column drain boolean not null default false,
    add column standby boolean not null default false,
    add column staging_pid integer;
