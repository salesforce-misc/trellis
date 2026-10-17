-- ADR-0017 P4b (#1063): the correction engine for to-many relationships.
--
-- A definition that reads a partitioned root through a to-many relationship
-- has its correction progress in `partition_correction_progress` too. Its
-- `parents` phase walks the relationship's from-side rows whose join value
-- routes to the node, in primary-key order, staging a recompute of each;
-- `cursor` is then the encoded primary key of the last from-side row. A node
-- with no usable text has its own rows read in the `node` phase instead, as
-- an aggregate's correction reads them.
--
-- The recomputes apply as the ring drains, so the walk ends in the `drain`
-- phase: `drain_seg` is the segment active when its last page committed, and
-- the reader is `done` once every segment through it is drained.
alter table partition_correction_progress
    add column drain_seg bigint,
    drop constraint partition_correction_progress_phase_check,
    add constraint partition_correction_progress_phase_check
        check (phase in ('fence', 'ledger', 'parents', 'node', 'drain', 'done'));
