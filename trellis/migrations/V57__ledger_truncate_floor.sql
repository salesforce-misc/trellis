-- #623 D3 (the D split's Q6): the truncate floor of a target on the ledger.
-- A source `TRUNCATE` empties the target's ledger (`<target>__ledger`) and
-- every group row, and records the truncate's ring `lsn` here, one row per
-- target (its qualified `transform_definitions.target_table`). A change whose
-- `lsn` is at or below the floor predates the truncate and is skipped
-- (ADR-0002 I2). That is exact: `TRUNCATE` takes `ACCESS EXCLUSIVE`, so every
-- earlier writer's trigger ran at a lower `lsn` and every later one's at a
-- higher one. Deleted with the definition.
create table ledger_truncate_floor (
    target_table text primary key,
    floor pg_lsn not null
);
