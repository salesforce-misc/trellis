-- ADR-0017 P4a (#1062): the correction engine for aggregates.
--
-- `partition_corrections.node_default` says the node is a `DEFAULT`
-- partition. Its recorded constraint text is NULL when it has no siblings
-- (every group routes to it), which a gone node that left the tree between
-- the tree read and the text fetch (no usable text) also has.
--
-- `parent_oid` is, for a gone node, its parent's oid when it left the tree: a
-- `HASH` text embeds it, so once the parent is dropped the text cannot be
-- evaluated, and the parent's own correction covers the node's groups.
alter table partition_corrections
    add column node_default boolean not null default false,
    add column parent_oid bigint;

-- One row per correction and aggregate definition reading the source: how far
-- the correction's key set has been re-derived. A restart resumes from
-- `cursor`; `keys_done` and `keys_total` are what the status reports as the
-- keys remaining (P6).
--
-- `phase` is `fence` while the engine waits for the transactions that were
-- open when it stamped the definition's `build_seg` to end (`fence_xid`: a
-- transaction id taken after that commit; the plan job of a seam-fed build
-- waits the same way), then `ledger` while the engine walks the live ledger entries whose
-- group routes to the node (through the ledger's `GROUP BY` index), then
-- `node` while it walks the node's current rows in primary-key order, then
-- `done`. `cursor` is the last key of the phase's walk (a JSON array of the
-- last entry's group values and key in the `ledger` phase; the encoded primary
-- key in the `node` phase), NULL before the first page.
create table partition_correction_progress (
    correction_id bigint not null references partition_corrections (id) on delete cascade,
    definition_id bigint not null references transform_definitions (id) on delete cascade,
    phase text not null check (phase in ('fence', 'ledger', 'node', 'done')),
    fence_xid xid8,
    cursor text,
    keys_done bigint not null default 0,
    keys_total bigint,
    primary key (correction_id, definition_id)
);
