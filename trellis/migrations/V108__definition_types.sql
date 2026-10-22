-- #1242 (ADR-0020): the OID of every user-defined type a definition names,
-- beside the name it persists for it. A type's name is stored where Trellis
-- casts by it: an enum's `enum:schema.typname` token in `source_columns` and
-- `join_columns`, and `definition_key_types.type_name`. A type's OID survives
-- `ALTER TYPE ... RENAME TO` and `... SET SCHEMA`; its name doesn't.
--
-- The capture pass compares each row with `pg_type` by OID
-- (`capture::baseline::follow_renamed_types`): the same OID under another
-- qualified name is a rename, which it re-records in this definition's
-- stores and here, and nothing pauses. An OID whose column now has another
-- type is a re-type, which the existing checks handle. Builtin types
-- (`pg_catalog`) are not recorded: their names never change.
--
-- Written at define, at an `ALTER TRANSFORM` that adds a read, at `RESUME`
-- (which replaces the definition's rows) and when the pass records a key
-- column's type (`defs::key_types`). A logical restore changes every OID: the
-- pass pauses every definition first (`capture::restore`) and compares no row
-- of a definition paused for it, and `RESUME` records the new OIDs.
create table definition_types (
    transform_id bigint not null
        references transform_definitions (id) on delete cascade,
    type_oid oid not null,
    type_name text not null,
    primary key (transform_id, type_oid)
);
