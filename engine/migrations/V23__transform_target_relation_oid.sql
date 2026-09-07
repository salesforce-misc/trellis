-- ADR-0007: a transform target is created after its definition, so bind its
-- physical identity only once target DDL has successfully materialized it.
alter table transform_definitions
    add column if not exists target_relation_oid oid;

create index if not exists transform_definitions_target_relation_oid_idx
    on transform_definitions (target_relation_oid);
