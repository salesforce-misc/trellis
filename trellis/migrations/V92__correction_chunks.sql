-- ADR-0017 P4a (#1062): a partition correction runs as a `backfill_chunks`
-- row of kind `correction`, one per correction and aggregate definition
-- reading the source. The row gives the correction the queue's claim (one
-- runner at a time), heartbeat, stale-claim sweep, failure record and
-- backoff, and holds the definition `backfilling` until it is done
-- (`staging::build::try_complete` waits for every undone row).
--
-- `correction_id` names the correction. The row goes with it once the
-- correction leaves the queue. Its progress is
-- `partition_correction_progress`'s, so `lo` and `hi` stay NULL.
alter table backfill_chunks
    add column correction_id bigint references partition_corrections (id) on delete cascade,
    drop constraint backfill_chunks_kind_check,
    drop constraint backfill_chunks_kind_bounds,
    add constraint backfill_chunks_kind_check
        check (kind in ('range', 'direct', 'plan', 'rederive', 'sweep', 'correction')),
    add constraint backfill_chunks_kind_bounds check (
        case kind
            when 'range' then hi is not null
            when 'rederive' then hi is not null
            when 'direct' then hi is null and lo is null
            when 'correction' then hi is null and lo is null and correction_id is not null
            else hi is null
        end
    ),
    add constraint backfill_chunks_correction_kind
        check ((kind = 'correction') = (correction_id is not null));

create index backfill_chunks_correction on backfill_chunks (correction_id)
    where correction_id is not null;
