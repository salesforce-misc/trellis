-- `ALTER TRANSFORM` (ADR-0015, issues #241/#242): each accepted edit bumps a
-- monotonic version on the target's own catalog row. This is distinct from
-- `source_table_versions.version` (V2__transform_catalog.sql), which stays
-- the value stage 05's version fence actually reads via FOR SHARE/FOR
-- UPDATE — `defs::catalog::alter_transform` bumps that one too, in the same
-- transaction, reusing the exact fence a first `create_definition` already
-- relies on rather than inventing a second one. `definition_version` is this
-- row's own audit-visible edit counter: every definition starts at 1 (the
-- version its `create_definition_inner` insert established) and is bumped by
-- one on every subsequent accepted `ALTER TRANSFORM`.
alter table transform_definitions
    add column if not exists definition_version bigint not null default 1;
