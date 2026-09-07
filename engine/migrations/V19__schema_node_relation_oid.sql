-- ADR-0007: source graph nodes are keyed by a physical relation OID. The
-- schema/name columns are refreshable current metadata; table_name remains
-- for unbound transform targets and legacy name-based callers.
alter table schema_nodes
    drop constraint if exists schema_nodes_table_name_key;

alter table schema_nodes
    add column if not exists relation_oid oid;

alter table schema_nodes
    add column if not exists schema_name text;

-- Best-effort only: stale names from a dropped relation remain usable as
-- legacy/unbound nodes and must not prevent the migration from succeeding.
update schema_nodes sn
set relation_oid = c.oid,
    schema_name = n.nspname
from pg_catalog.pg_class c
join pg_catalog.pg_namespace n on n.oid = c.relnamespace
where c.oid = pg_catalog.to_regclass(sn.table_name)
  and sn.is_source
  and sn.relation_oid is null;

-- Coalesce legacy nodes that different old spellings resolved to as the same
-- object, preserving all roles and normalizing graph edges before deleting
-- redundant nodes.
create temporary table schema_node_oid_canonical on commit drop as
select relation_oid, min(id) as canonical_id
from schema_nodes
where relation_oid is not null
group by relation_oid;

update schema_nodes canonical
set is_source = roles.is_source,
    is_target = roles.is_target
from (
    select mapping.canonical_id,
           bool_or(sn.is_source) as is_source,
           bool_or(sn.is_target) as is_target
    from schema_node_oid_canonical mapping
    join schema_nodes sn on sn.relation_oid = mapping.relation_oid
    group by mapping.canonical_id
) roles
where canonical.id = roles.canonical_id;

insert into schema_edges (from_node_id, to_node_id, kind, created_at)
select coalesce(from_mapping.canonical_id, se.from_node_id),
       coalesce(to_mapping.canonical_id, se.to_node_id),
       se.kind,
       se.created_at
from schema_edges se
join schema_nodes from_node on from_node.id = se.from_node_id
join schema_nodes to_node on to_node.id = se.to_node_id
left join schema_node_oid_canonical from_mapping
  on from_mapping.relation_oid = from_node.relation_oid
left join schema_node_oid_canonical to_mapping
  on to_mapping.relation_oid = to_node.relation_oid
on conflict (from_node_id, to_node_id, kind) do nothing;

delete from schema_edges se
using schema_nodes from_node, schema_nodes to_node,
      schema_node_oid_canonical from_mapping, schema_node_oid_canonical to_mapping
where se.from_node_id = from_node.id
  and se.to_node_id = to_node.id
  and from_mapping.relation_oid = from_node.relation_oid
  and to_mapping.relation_oid = to_node.relation_oid
  and (from_node.id <> from_mapping.canonical_id
       or to_node.id <> to_mapping.canonical_id);

delete from schema_nodes duplicate
using schema_node_oid_canonical mapping
where duplicate.relation_oid = mapping.relation_oid
  and duplicate.id <> mapping.canonical_id;

create unique index if not exists schema_nodes_relation_oid_key
    on schema_nodes (relation_oid)
    where relation_oid is not null;

-- Targets are intentionally still unbound in this ADR-0007 slice. Their
-- legacy name identity stays unique only while relation_oid is absent.
create unique index if not exists schema_nodes_unbound_table_name_key
    on schema_nodes (table_name)
    where relation_oid is null;
