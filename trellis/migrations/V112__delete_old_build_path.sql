-- #625 F10: the old build path is gone. A build is a Re-derive build
-- (`staging::build`) and starts from the staging worker's capture pass, so
-- there are no registration markers, no discharge, no go-live re-read and no
-- orphan sweep.
--
-- `pending_backfill` held the markers. The one thing on a marker that
-- outlives them is the capture gate an install or widen records (V55): the
-- WAL position below which a table's old trigger body staged its rows, and
-- which holds a definition's build until no change at or below it is pending
-- (`capture::gate`). It is a row of its own now, one per table, deleted by
-- the first capture pass to find it released.
drop table pending_backfill;

create table capture_gates (
    table_name text primary key,
    gate_lsn pg_lsn not null
);

-- #938: the marker generation a build's start read, for the discharge's
-- enumeration. Nothing enumerates.
alter table transform_definitions drop column build_marker_generation;

-- The old builds' rows: a plain 1-1 definition's primary-key ranges
-- (`range`) and an aggregate's direct build (`direct`), with the failure
-- count (`prior_attempts`) a failed direct build handed back to its marker.
delete from backfill_chunks where kind in ('range', 'direct');
alter table backfill_chunks
    drop constraint backfill_chunks_kind_check,
    drop constraint backfill_chunks_kind_bounds,
    alter column kind drop default,
    drop column prior_attempts,
    add constraint backfill_chunks_kind_check
        check (kind in ('plan', 'rederive', 'sweep', 'correction')),
    add constraint backfill_chunks_kind_bounds check (
        case kind
            when 'rederive' then hi is not null
            when 'correction' then hi is null and lo is null and correction_id is not null
            else hi is null
        end
    );
