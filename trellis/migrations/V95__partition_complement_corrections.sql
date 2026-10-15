-- ADR-0017 P7 (#1066): the every-pass sweep queues a `complement`
-- correction when it finds target groups that route to no current leaf of a
-- partitioned source (a leaf created, written and dropped between two
-- passes). `constraint_text` holds the complement it found them in, a
-- predicate over the source's partition-key columns, and `node_oid` the
-- root's oid. The correction engine re-derives the keys of the live ledger
-- entries whose group satisfies it, as it does a gone node's.
alter table partition_corrections drop constraint partition_corrections_kind_check;
alter table partition_corrections add constraint partition_corrections_kind_check
    check (kind in ('new_leaf', 'gone_node', 'truncated', 'complement'));
