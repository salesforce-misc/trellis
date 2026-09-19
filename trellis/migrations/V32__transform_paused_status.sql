-- Issue #142 / ADR-0014 ("Pausing and Dropping a Definition"): a definition
-- gains a deliberate, operator-driven `paused` state alongside the
-- poison-driven `quarantined` one it already had.
--
-- These are one *state* with two *triggers*, in the ADR's sense: both are
-- simply "not `live`", and the only gate that has ever frozen a definition —
-- the `t.status = 'live'` predicate inside `defs::catalog::dependents_of`,
-- which every claim-time fold dispatch resolves its targets through — stops
-- dispatching to the target for either one, identically. No second freezing
-- mechanism is introduced here; `paused` is a new *value* in the existing
-- gate, not a new gate.
--
-- The two spellings are kept distinct rather than collapsed onto one word
-- because the trigger is the forensically interesting part, and the public
-- surface already leans on the distinction: `Trellis::quarantined()` reports
-- exactly the definitions the poison fuse tripped
-- (`staging::quarantine::trip_transform_fuse_if_crossed`), and an operator
-- pause deliberately taken to stage a schema change is not a quarantine
-- incident and must not show up in that report. Recovery is shared: a single
-- `staging::quarantine::resume_transform` accepts either value and takes it
-- back to `waiting_to_backfill` for a fresh backfill (ADR-0014, "Resume
-- rebuilds by backfill, not by catch-up").
--
-- The `check` constraint has to be dropped and re-added rather than altered
-- in place — Postgres has no `alter constraint ... check`. Same idiom as
-- V22__column_status_drops_target_table_fkey.sql's drop.
alter table transform_definitions
    drop constraint transform_definitions_status_check;

alter table transform_definitions
    add constraint transform_definitions_status_check
        check (status in ('waiting_to_backfill', 'backfilling', 'live', 'quarantined', 'paused'));
