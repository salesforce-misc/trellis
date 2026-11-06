-- The key space Trellis chose for each table a definition keys by (#1719):
-- its target (an aggregate's or `JOIN`'s group columns, a 1-1 target's
-- source key), its source and a `JOIN`'s joined table, each as the key
-- columns in key order. Define records them, and a table's row identity
-- (`defs::ddl::identity_key_columns`) is from then on the unique index or
-- primary key over exactly these columns, whatever other unique indexes the
-- table gains or in what order their OIDs fall.
--
-- Existing definitions get theirs when a binary carrying this migration
-- attaches (`defs::recorded_keys::backfill_in`), from their parsed
-- definitions, which SQL can't read.
--
-- Declares no catalog floor: no release before 0.2.0 attaches to a catalog
-- with floors, and a definition without rows here keeps the identity it had.
create table definition_keys (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    table_name text not null,
    key_columns text[] not null check (cardinality(key_columns) > 0),
    primary key (transform_id, table_name)
);

create index definition_keys_table_idx on definition_keys (table_name, transform_id);
