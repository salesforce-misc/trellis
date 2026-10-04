-- #760: the type each of a definition's key columns had when Trellis built
-- the definition's state from it (`defs::key_types`).
--
-- A key column is one Trellis matches rows by: the source's row-identity
-- key, a `GROUP BY` key, and the join column and to-side key of each
-- relationship the definition reads through. Its target, ledger and
-- relationship projection hold that column's values, typed and rendered as
-- they were at define. An `ALTER COLUMN ... TYPE` rewrites the source table
-- without firing a capture trigger, so if the new type renders the existing
-- values differently, what Trellis stored no longer matches. The staging
-- worker's capture pass compares the live type with this row and pauses the
-- definition when it does (`staging::schema_change::pause_readers_of_retyped`).
--
-- Written when the definition is accepted, and by the capture pass for a key
-- column it finds unrecorded: one that joined a table's row-identity key
-- after define (a redefined primary key, which pauses the readers first,
-- #687). `ALTER TRANSFORM` adds no key column: it edits only 1-1,
-- relationship-free definitions' fields. A resume
-- keeps the row: the tables Trellis created from the old type are still
-- there, so a resume rebuilds into them.
--
-- `type_name` is `schema.typname`, which survives a dump and restore where
-- an OID wouldn't.
create table definition_key_types (
    transform_id bigint not null
        references transform_definitions (id) on delete cascade,
    table_name text not null,
    column_name text not null,
    type_name text not null,
    typmod integer not null,
    primary key (transform_id, table_name, column_name)
);
