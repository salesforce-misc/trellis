-- Issue #531: the WAL position at which
-- `catalog::refresh_relationship_projections_in_txn` last rewrote this
-- relationship's settled projection from its to-side's rows. A reverse
-- record at or below it may carry an image that refresh overtook (CDC
-- streamed before a later, lost change to the same key), so Phase 3 checks
-- such a record against the live to-side row and writes the projection from
-- that row instead (`staging::apply::superseded_to_side`). Phase 3 reads it
-- `for share` and the refresh locks it `for update` before touching the
-- projection, so neither can miss the other. Null until the first refresh.
alter table relationship_projections add column refreshed_lsn pg_lsn;
