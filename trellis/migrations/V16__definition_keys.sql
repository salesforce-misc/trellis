-- The key Trellis keys each table by (#1719): one record per table, whatever
-- definitions read or write it. A target's key comes from its definition
-- (an aggregate's or `JOIN`'s group columns, a 1-1 target's source key); a
-- source's and a `JOIN`'s joined table's is its row identity when the first
-- define over it admitted it. A table's row identity
-- (`defs::ddl::identity_key_columns`) is from then on the unique index or
-- primary key over exactly these columns, whatever other unique indexes the
-- table gains or in what order their OIDs fall. A resume of a reader whose
-- table's key changed re-records it for every reader
-- (`defs::recorded_keys::decide`).
--
-- `relation_oid` is the oid of the table the record was taken from: a record
-- whose `relation_oid` isn't the oid at `table_name` now is the record of a
-- table since dropped, and no record of the one there now. `catalog_oid` is
-- the oid of this table: a logical restore recreates every table under new
-- oids, so a row whose stored value differs from the live one came with a
-- restore of the tables at the same names and holds by name until it is
-- tied to them again (`defs::recorded_keys::retie_restored`).
--
-- Existing definitions get theirs when a binary carrying this migration
-- attaches (`defs::recorded_keys::backfill_in`), from their parsed
-- definitions, which SQL can't read.
--
-- Declares no catalog floor: no release before 0.2.0 attaches to a catalog
-- with floors, and a table without a row here keeps the identity it had.
create table definition_keys (
    table_name text primary key,
    key_columns text[] not null check (cardinality(key_columns) > 0),
    relation_oid oid not null,
    catalog_oid oid not null
);

comment on table definition_keys is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore (their catalog_oid no longer names this '
    'table), and are held by name until they are tied again.';
