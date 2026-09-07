-- ADR-0007: poison markers and death counters must distinguish relations
-- that reuse a presentation name after the original relation is dropped.
-- Retain nullable OIDs for legacy rows, with separate identity-aware and
-- legacy uniqueness constraints.
alter table poison
    add column if not exists source_relation_oid oid;

alter table poison
    add column if not exists id bigint generated always as identity;

alter table poison
    drop constraint if exists poison_pkey;

alter table poison
    add constraint poison_pkey primary key (id);

create unique index if not exists poison_source_relation_oid_key_key
    on poison (source_relation_oid, key)
    where source_relation_oid is not null;

create unique index if not exists poison_legacy_src_table_key_key
    on poison (src_table, key)
    where source_relation_oid is null;

alter table key_deaths
    add column if not exists source_relation_oid oid;

alter table key_deaths
    add column if not exists id bigint generated always as identity;

alter table key_deaths
    drop constraint if exists key_deaths_pkey;

alter table key_deaths
    add constraint key_deaths_pkey primary key (id);

create unique index if not exists key_deaths_source_relation_oid_key_key
    on key_deaths (source_relation_oid, key)
    where source_relation_oid is not null;

create unique index if not exists key_deaths_legacy_src_table_key_key
    on key_deaths (src_table, key)
    where source_relation_oid is null;
