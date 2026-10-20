-- #707: what each of a captured table's Trellis capture triggers looked like
-- when Trellis last installed, widened, narrowed or repaired its capture,
-- one row per (table, trigger). The capture pass compares the live
-- `pg_trigger` rows with these, and pauses the table's readers when any
-- differs: the trigger is gone, has a new oid or `xmin` (recreated, or
-- altered by name), or is not `ENABLE ALWAYS`. Every install, widen, narrow
-- and repair writes the rows in its own transaction (`capture::triggers`), so
-- Trellis's own changes never read as a disruption.
--
-- `system_identifier` and `catalog_oid` (the oid of this table) say whether
-- the rows survived a dump and restore or `pg_upgrade`, which recreate every
-- trigger under a new oid: a row whose stored value differs from the live one
-- is re-recorded, not acted on.
create table capture_triggers (
    table_name text not null,
    trigger_name text not null,
    trigger_oid oid not null,
    trigger_xmin text not null,
    enabled "char" not null,
    system_identifier text not null,
    catalog_oid oid not null,
    primary key (table_name, trigger_name)
);

comment on table capture_triggers is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore: the oid of the table holding them changes.';
