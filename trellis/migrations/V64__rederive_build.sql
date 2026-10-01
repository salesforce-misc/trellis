-- #625 F2: the Re-derive build, behind `ClientOptions::rederive_build`
-- (off by default). See `trellis::staging::build`'s module doc.
--
-- `backfill_chunks.kind` says what a row builds:
--
-- * `range`: a plain 1-1 definition's primary-key range (V20);
-- * `direct`: an aggregate or relationship-enriched 1-1 definition's whole
--   direct build, one job (V44);
-- * `plan`: a Re-derive build's plan job. It walks the source's primary key
--   and enqueues `rederive` rows in batches as it goes. `lo` is its cursor:
--   the last boundary it has enqueued, `null` before the first. It is done
--   once the walk finds no rows left.
-- * `rederive`: one `(lo, hi]` range of a Re-derive build
--   (`staging::build::run_chunk`);
-- * `sweep`: reserved for #625 F3's orphan Re-derive after a resume.
--
-- `start_xid` is the plan row's start transaction, for F3's sweep filter.
--
-- `transform_definitions.build` is `rederive` while a definition's
-- Re-derive build runs, and `null` otherwise (#625 Q3). A `backfilling`
-- definition with it set is applying: Apply folds its changes and the
-- target-mutation seam stages its source's writes for it
-- (`Definition::applies`). F10 drops it with the old build.
alter table backfill_chunks
    add column kind text not null default 'range',
    add column start_xid xid8,
    drop constraint backfill_chunks_whole_build_unbounded,
    add constraint backfill_chunks_kind_check
        check (kind in ('range', 'direct', 'plan', 'rederive', 'sweep')),
    add constraint backfill_chunks_kind_bounds check (
        case kind
            when 'range' then hi is not null
            when 'rederive' then hi is not null
            when 'direct' then hi is null and lo is null
            else hi is null
        end
    );

alter table transform_definitions
    add column build text,
    add constraint transform_definitions_build_check check (build in ('rederive'));
