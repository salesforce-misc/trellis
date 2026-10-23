-- #1241 (ADR-0020): every value of every enum type a definition reads, by
-- its `pg_enum` OID, beside its label as recorded. Trellis keeps enum values
-- as label text (ring images, held keys, a 1-1 ledger's `__from_key`), and
-- `ALTER TYPE ... RENAME VALUE` changes a label without firing a trigger,
-- keeping the value's OID.
--
-- The capture pass compares each row with `pg_enum` by OID
-- (`staging::schema_change::pause_readers_of_relabelled`), for the enum types
-- the definition reads now: a recorded OID whose label changed is a relabel,
-- which pauses the definition with `capture_failures.relabel_lsn` set. A new
-- OID (`ADD VALUE`) is recorded and pauses nothing. The rows of a type the
-- definition no longer reads are deleted.
--
-- Written at define, at an `ALTER TRANSFORM` that adds a read, and at
-- `RESUME`, which replaces the definition's rows. A logical restore changes
-- every OID: the pass pauses every definition first (`capture::restore`),
-- compares no row of a definition paused for it, and `RESUME` records the
-- new OIDs.
create table definition_enum_values (
    transform_id bigint not null
        references transform_definitions (id) on delete cascade,
    type_oid oid not null,
    value_oid oid not null,
    label text not null,
    primary key (transform_id, value_oid)
);

-- The WAL insert position the capture pass read when it found a relabel of
-- an enum type the definition reads. Every change staged with the old label
-- has an `origin_lsn` below it, so a resume waits until none of the
-- definition's source tables has such a change still pending before it
-- rebuilds (`staging::quarantine`). Null for every other pause. Kept by a
-- later record of the same pause (`set_capture_failure` writes the other
-- columns), and raised by a later relabel.
alter table capture_failures add column relabel_lsn pg_lsn;
