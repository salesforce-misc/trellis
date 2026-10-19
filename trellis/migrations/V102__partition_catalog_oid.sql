-- ADR-0017 P8b (#1067, decided on #1052): the oid of `partition_sources`
-- itself, written with each row. A dump and restore has to recreate the
-- table to bring its rows back, so a stored oid that no longer names the
-- table holding the row says the rows were restored: the pass pauses every
-- definition that reads the source and runs nothing routed from the recorded
-- state. Every row is written by the pass, which sets it.
alter table partition_sources add column catalog_oid oid;
update partition_sources set catalog_oid = 'partition_sources'::regclass;
alter table partition_sources alter column catalog_oid set not null;

comment on table partition_sources is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore (their catalog_oid no longer names this '
    'table), which pauses every definition over a partitioned source.';

comment on table capture_baselines is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore: the oid of the table holding them changes.';
