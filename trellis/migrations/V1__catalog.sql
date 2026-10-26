-- The catalog: what an instance is and what has been defined in it.
--
-- Every migration runs with the catalog schema first on the `search_path`, so
-- names are unqualified. The ledger (`refinery_schema_history`) lives in the
-- same schema.

-- The singleton row that makes a schema an identifiable Trellis instance
-- (`identity`): the schema it was created as, the instance format it was
-- written in, and the schema holding its target tables.
create table trellis_instance (
    singleton boolean primary key default true check (singleton),
    schema_name text not null,
    instance_format_version integer not null,
    target_schema text
);

-- The version of each source table's registered definitions.
create table source_table_versions (
    source_table text primary key,
    version bigint not null
);

-- One row per defined transform (a target table and how it is built).
--
-- `status` is the definition's lifecycle: the build states
-- (`waiting_to_backfill`, `backfilling`, `catching_up`), `live`, and the two
-- stopped states (`quarantined`, `paused`). `definition_version` counts
-- alters. `build` is `rederive` while a re-derive build is running, and
-- `build_seg` the segment that build's snapshot is fenced against.
-- `grammar_version` is the transform grammar the definition was written in
-- (`defs::grammar`). `join_table` and `join_columns` name the second side of a
-- JOIN definition; they are both set or both null, and the second side is a
-- different table.
--
-- The unique index on the target's suffix keeps two definitions from writing
-- tables with the same bare name in different schemas.
create table transform_definitions (
    id bigint generated always as identity primary key,
    target_table text not null unique,
    source_table text not null references source_table_versions (source_table),
    source_version bigint not null,
    definition_text text not null,
    created_at timestamptz not null default now(),
    source_columns jsonb not null default '{}'::jsonb,
    status text not null default 'live',
    fuse_rearmed_at timestamptz,
    definition_version bigint not null default 1,
    build text,
    build_seg bigint,
    grammar_version integer not null default 1,
    join_table text,
    join_columns jsonb,
    constraint transform_definitions_status_check check (status in (
        'waiting_to_backfill', 'backfilling', 'catching_up', 'live',
        'quarantined', 'paused')),
    constraint transform_definitions_build_check check (build = 'rederive'),
    constraint transform_definitions_join_sides_check check (
        (join_table is null) = (join_columns is null)
        and (join_table is null or join_table <> source_table))
);

create index transform_definitions_source_table_idx
    on transform_definitions (source_table);

create unique index transform_definitions_target_suffix_idx
    on transform_definitions (split_part(target_table, '.', 2));

-- The schema graph: every table that is a source or a target, and the edges
-- between them.
create table schema_nodes (
    id bigint generated always as identity primary key,
    table_name text not null unique,
    is_source boolean not null default false,
    is_target boolean not null default false,
    created_at timestamptz not null default now(),
    check (is_source or is_target)
);

create table schema_edges (
    id bigint generated always as identity primary key,
    from_node_id bigint not null references schema_nodes (id),
    to_node_id bigint not null references schema_nodes (id),
    kind text not null check (kind in ('source', 'join')),
    created_at timestamptz not null default now(),
    unique (from_node_id, to_node_id, kind)
);

create index schema_edges_from_node_id_idx on schema_edges (from_node_id);

-- The types of a definition's key columns, by OID, as of the define.
create table definition_key_types (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    table_name text not null,
    column_name text not null,
    type_oid oid not null,
    type_name text not null,
    typmod integer not null,
    primary key (transform_id, table_name, column_name)
);

-- Every type a definition's value columns use, and the labels of the enum
-- types among them, by OID, as of the define.
create table definition_types (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    type_oid oid not null,
    type_name text not null,
    primary key (transform_id, type_oid)
);

create table definition_enum_values (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    type_oid oid not null,
    value_oid oid not null,
    label text not null,
    primary key (transform_id, value_oid)
);

-- The live workers of the instance.
create table worker_registry (
    worker_id text primary key,
    registered_at timestamptz not null default now(),
    last_seen timestamptz not null default now()
);
