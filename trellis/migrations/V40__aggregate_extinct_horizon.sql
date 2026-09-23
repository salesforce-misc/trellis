-- Issue #321: the per-target extinct horizon. When Phase 3 deletes an
-- aggregate group's target row because a live read found the group empty
-- (`staging::apply_aggregate`'s forced-path extinct `DELETE`, or the delta
-- path's `delete_group_row`), that read may already have counted source
-- commits whose own CDC deltas have not been applied yet. The deleted row
-- can't carry its own `__trellis_recompute_lsn` any more, so the WAL insert
-- position taken after the read is kept here instead, one row per aggregate
-- target (its qualified `transform_definitions.target_table`). A delta that
-- lands on a group with no target row compares its earliest image-bearing
-- LSN against this value and re-derives the group when it is at or below.
create table aggregate_extinct_horizon (
    target_table text primary key,
    lsn pg_lsn not null
);
