-- Cross-table dependency graph (issue #21): persists the edges
-- `docs/transforms.md`'s logical model describes: a `source` edge from a
-- transform's `FROM` table to its target, and a `join` edge from a `JOIN`
-- transform's joined table.
--
-- Both endpoints reference `schema_nodes` (issue #20) rather than a bare
-- table-name string, so the graph is walkable by node id — see
-- `trellis/src/defs/catalog.rs`'s `dependents_of`.
--
-- `on conflict do nothing` on the unique triple below makes re-declaring
-- the same transform's edge idempotent, mirroring `resolve_node_in_txn`'s
-- upsert idiom for nodes.
create table if not exists schema_edges (
    id bigint primary key generated always as identity,
    from_node_id bigint not null references schema_nodes (id),
    to_node_id bigint not null references schema_nodes (id),
    kind text not null check (kind in ('source', 'join')),
    created_at timestamptz not null default now(),
    unique (from_node_id, to_node_id, kind)
);

create index if not exists schema_edges_from_node_id_idx
    on schema_edges (from_node_id);
