-- #828: a resume that re-types the columns of a definition's target
-- (`staging::quarantine::finish_requested_resumes`) can pause the
-- definitions chained off that target, whose own columns were typed from
-- the old types. The capture pass records on each such pause which
-- definition's resume caused it, so its `capture_failure` names that resume
-- rather than a column the operator never altered.
--
-- One row per target column a resume re-typed, written in the re-type's own
-- transaction, so a crash between the two doesn't lose the cause. A later
-- re-type of the same column replaces it. The capture pass reads a row as
-- the cause only while the column still has the type it names, and only
-- while the definition it names still writes the table. It has no foreign
-- key, for the reason `retype_releases` has none (V74): the re-type holds
-- `ACCESS EXCLUSIVE` on a table a drop takes after the definition's row.
-- The next resume's re-type deletes the rows of a definition that's gone.
create table retype_causes (
    table_name text not null,
    column_name text not null,
    transform_id bigint not null,
    old_type text not null,
    new_type text not null,
    retyped_at timestamptz not null default now(),
    primary key (table_name, column_name)
);

-- The definition whose resume re-typed the columns this pause is about,
-- when that resume is what paused it. Null for every other pause, and for
-- a definition paused for another reason first. Following it from a
-- definition finds every definition its resume paused, transitively. No
-- foreign key: a definition can't be dropped while another reads its
-- target, and taking a key lock on the upstream definition's row from the
-- pause's transaction would add a lock to an order that has none.
alter table capture_failures add column caused_by bigint;
