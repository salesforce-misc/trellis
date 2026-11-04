-- The catalog floor (ADR-0024): the oldest Trellis release that can read this
-- catalog. A binary below it refuses to attach; one at or above it attaches
-- and tolerates applied migrations it doesn't carry. One row, which this
-- migration seeds at 0.2.0, the first release that carries the check.
--
-- A later migration that an older release can't read raises the floor in its
-- own SQL, to the greater of the stored floor and its own, so the floor and
-- the applied history never disagree:
--
--   update catalog_floor set version = '0.3.0'
--    where string_to_array(version, '.')::int[] < '{0,3,0}';
--
-- `version` is a release version, never a pre-release: a floor names the
-- oldest release that can read the catalog.
create table catalog_floor (
    singleton boolean primary key default true check (singleton),
    version text not null
        check (version ~ '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$')
);

insert into catalog_floor (version) values ('0.2.0');

-- The floor replaces the marker's instance_format_version, which no release
-- from 0.2.0 on reads or writes. Releases before 0.2.0 refuse a marker whose
-- format version is above 1 before their migration runner starts, so it is 2
-- from here on: they refuse this catalog, naming a newer Trellis.
alter table trellis_instance alter column instance_format_version set default 2;
update trellis_instance set instance_format_version = 2;
