-- First-class schema-node model (issue #20): every table Trellis knows
-- about — source tables and transform targets — gets an identity row here,
-- so later work (issue #21's dependency graph) can resolve an endpoint
-- against a stable node id rather than a bare table-name string.
--
-- Only identity is persisted: which table, and whether it's a source, a
-- target, or (via chained/multi-hop transforms) both. Column names/types
-- are not duplicated here — they're introspected live from
-- pg_catalog/information_schema (see `trellis/src/defs/ddl.rs`'s
-- `source_primary_key`), matching ADR-0005 (Trellis never owns a copy of a
-- source schema it doesn't issue DDL against; it reads live and
-- revalidates rather than caching a schema snapshot that could drift out
-- from under it).
--
-- `is_source`/`is_target` are independent flags, not an exclusive `kind`:
-- a transform's target table is a completely ordinary table a later
-- transform can subscribe to as its source, so the same table can
-- legitimately hold both roles over its lifetime.
create table if not exists schema_nodes (
    id bigint primary key generated always as identity,
    table_name text not null unique,
    is_source boolean not null default false,
    is_target boolean not null default false,
    created_at timestamptz not null default now(),
    check (is_source or is_target)
);
