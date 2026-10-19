-- A detected operation on a partitioned source's node queues its own
-- correction unless an entry of the same node, kind and text that no reader
-- has started is queued. The unique index deduped against any queued entry,
-- so an operation detected while an older entry was still held by a paused
-- reader (or between the first reader finishing and the last) was swallowed,
-- and a reader that had already finished the older entry never corrected it.
-- The pass and the complement sweep now test for an unstarted entry in the
-- insert itself; two inserters that race past the test both queue, which
-- costs work and never correctness.
drop index partition_corrections_once;
create index partition_corrections_identity on partition_corrections
    (table_name, kind, node_oid);
