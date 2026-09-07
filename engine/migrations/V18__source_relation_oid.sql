-- ADR-0007: a source relation's OID, not its mutable spelling, is the
-- catalog identity. Keep source_table as the original/presentation text for
-- existing callers, but detach the old name foreign key before allowing the
-- same name to identify a replacement relation.
alter table transform_definitions
    drop constraint if exists transform_definitions_source_table_fkey;

alter table source_table_versions
    drop constraint if exists source_table_versions_pkey;

alter table source_table_versions
    add column if not exists id bigint generated always as identity;

alter table source_table_versions
    add column if not exists source_relation_oid oid;

alter table source_table_versions
    add constraint source_table_versions_pkey primary key (id);

alter table transform_definitions
    add column if not exists source_relation_oid oid;

-- Existing catalogs only have source text. Resolve it using this migration
-- connection's normal search_path, and intentionally leave an OID null when
-- the old relation no longer exists rather than failing an upgrade.
update source_table_versions stv
set source_relation_oid = c.oid
from pg_catalog.pg_class c
where c.oid = pg_catalog.to_regclass(stv.source_table)
  and stv.source_relation_oid is null;

update transform_definitions td
set source_relation_oid = c.oid
from pg_catalog.pg_class c
where c.oid = pg_catalog.to_regclass(td.source_table)
  and td.source_relation_oid is null;

-- More than one old spelling can resolve to one relation. Preserve the
-- greatest version on one legacy row, remove its redundant peers, and make
-- the OID the unique key for all future writes.
with grouped as (
    select source_relation_oid, min(id) as canonical_id, max(version) as version
    from source_table_versions
    where source_relation_oid is not null
    group by source_relation_oid
)
update source_table_versions stv
set version = grouped.version
from grouped
where stv.id = grouped.canonical_id;

delete from source_table_versions duplicate
using source_table_versions canonical
where duplicate.source_relation_oid is not null
  and duplicate.source_relation_oid = canonical.source_relation_oid
  and duplicate.id > canonical.id;

create unique index if not exists source_table_versions_source_relation_oid_key
    on source_table_versions (source_relation_oid)
    where source_relation_oid is not null;

create index if not exists transform_definitions_source_relation_oid_idx
    on transform_definitions (source_relation_oid);
