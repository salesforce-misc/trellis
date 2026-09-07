-- ADR-0007: bind each source column a transform actually reads to its stable
-- PostgreSQL attribute identity. Existing definitions deliberately have no
-- rows here and retain the legacy name-based evaluation behavior.
create table if not exists transform_source_columns (
    definition_id bigint not null references transform_definitions (id) on delete cascade,
    logical_name text not null,
    source_relation_oid oid not null,
    attnum int2 not null,
    type_oid oid not null,
    type_modifier int4 not null,
    value_type text not null check (value_type in ('numeric', 'text', 'boolean', 'uuid')),
    primary key (definition_id, logical_name)
);
