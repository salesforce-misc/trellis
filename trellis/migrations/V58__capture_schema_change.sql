-- #622 C6: a schema change never fails the application's write.
--
-- A capture function whose imaged column has been renamed or dropped no
-- longer fails the statement. It writes a `schema_changed` marker (key
-- `staging::append::SCHEMA_CHANGED_SENTINEL_KEY`, `new_image` =
-- `{"missing": [...], "key_missing": bool}`) and then images the columns
-- that are left (`capture::sql`). Three pieces of state follow from that.
--
-- 1. The ring accepts the new `op`, always with a `new_image` and never with
--    an `old_image`.
--
--    This migration `ALTER`s every `seg_N`, which takes `ACCESS EXCLUSIVE`
--    and so stalls every captured application writer while it runs (#622
--    plan, "Migrations now touch the application's write path").

alter table seg_0 drop constraint seg_0_op_check;
alter table seg_0 add constraint seg_0_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate',
                  'rel_reverse_deferred', 'schema_changed'));
alter table seg_0 add constraint seg_0_schema_changed_shape
    check (op <> 'schema_changed' or (old_image is null and new_image is not null));

alter table seg_1 drop constraint seg_1_op_check;
alter table seg_1 add constraint seg_1_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate',
                  'rel_reverse_deferred', 'schema_changed'));
alter table seg_1 add constraint seg_1_schema_changed_shape
    check (op <> 'schema_changed' or (old_image is null and new_image is not null));

alter table seg_2 drop constraint seg_2_op_check;
alter table seg_2 add constraint seg_2_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate',
                  'rel_reverse_deferred', 'schema_changed'));
alter table seg_2 add constraint seg_2_schema_changed_shape
    check (op <> 'schema_changed' or (old_image is null and new_image is not null));

alter table seg_3 drop constraint seg_3_op_check;
alter table seg_3 add constraint seg_3_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate',
                  'rel_reverse_deferred', 'schema_changed'));
alter table seg_3 add constraint seg_3_schema_changed_shape
    check (op <> 'schema_changed' or (old_image is null and new_image is not null));

-- 2. Seal records whether a segment's fenced window holds a marker, in the
--    same pass that decides `has_truncate`, so a drain looks for markers only
--    in a segment that has one (`staging::schema_change`).
alter table segments add column has_schema_change boolean not null default false;

-- 3. The reason a definition was paused by a marker: the source table and
--    the missing columns it reads. `Trellis::status` reports it as
--    `DefinitionStatus::capture_failure`. While the row exists, capture
--    ignores the definition's columns (`capture::columns`), so the staging
--    worker's reconcile regenerates the table's functions over the columns
--    the other readers need. Resuming the definition (a rebuild) deletes it.
create table capture_failures (
    transform_id bigint primary key
        references transform_definitions (id) on delete cascade,
    source_table text not null,
    columns text[] not null,
    detected_at timestamptz not null default now()
);
