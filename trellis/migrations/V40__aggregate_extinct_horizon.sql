-- Issue #321: the per-target extinct horizon. When a Phase 3 live read finds
-- an aggregate group empty (`staging::apply_aggregate`'s forced-path survivor
-- probe, or the delta path's `probe_group_exists`), it drops the batch's
-- delta for that group and deletes the group's row if there is one. That
-- read may already have counted source commits whose own CDC deltas have not
-- been applied yet. There is no row left to carry its own
-- `__trellis_recompute_lsn`, so the WAL insert
-- position taken after the read is kept here instead, one row per aggregate
-- target (its qualified `transform_definitions.target_table`). A delta that
-- lands on a group with no target row compares its earliest image-bearing
-- LSN against this value and re-derives the group when it is at or below.
create table aggregate_extinct_horizon (
    target_table text primary key,
    lsn pg_lsn not null
);
