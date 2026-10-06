-- #760, #767: what the staging worker's capture pass compares a definition's
-- key columns against, and the resumes waiting on it to re-type Trellis's
-- copies.
--
-- `definition_key_types` holds the type each of a definition's key columns
-- had when Trellis last built the definition's state from it
-- (`defs::key_types`). A key column is one Trellis matches rows by: the
-- source's row-identity key, a `GROUP BY` key, and the join column and
-- to-side key of each relationship the definition reads through. Its target,
-- ledger and relationship projection hold that column's values, rendered as
-- they were then. An `ALTER COLUMN ... TYPE` rewrites the source table
-- without firing a capture trigger, so if the new type renders the existing
-- values differently, what Trellis stored no longer matches. The capture
-- pass compares the live type with this row and pauses the definition when
-- it does (`staging::schema_change::pause_readers_of_retyped`).
--
-- Written when the definition is accepted, rewritten by every resume (which
-- rebuilds from the live types), and written by the capture pass for a key
-- column it finds unrecorded: one that joined a table's row-identity key
-- after define (a redefined primary key, which pauses the readers first,
-- #687). `ALTER TRANSFORM` adds no key column: it edits only 1-1,
-- relationship-free definitions' fields.
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

-- A resumed definition whose typed copies (`defs::copies`) no longer have
-- the type define would give them from the live schema. `RESUME` returns at
-- once and leaves the definition paused with this row; the staging worker's
-- capture pass re-types the copies (`ALTER ... TYPE`, under `ACCESS
-- EXCLUSIVE`) and then completes the resume, deleting the row in the same
-- transaction (`staging::quarantine::finish_requested_resumes`). A crash
-- between the two leaves the row, and the next pass finishes the work.
create table resume_requests (
    transform_id bigint primary key
        references transform_definitions (id) on delete cascade,
    requested_at timestamptz not null default now()
);
