-- Each definition's floor (ADR-0024): the oldest Trellis release that can
-- run it, `major.minor.patch`, compared as semver. Trellis stamps it whenever
-- it writes the definition or recreates its hidden tables (`defs::floor`).
--
-- Every existing definition gets 0.2.0, the baseline: no earlier release can
-- attach to a catalog this migration has run on, and every feature that
-- exists when floors are introduced has floor 0.2.0. The column keeps that
-- default, as an additive column must, so a row written without a floor reads
-- as the baseline.
alter table transform_definitions
    add column floor_version text not null default '0.2.0'
        constraint transform_definitions_floor_version_check
        check (floor_version ~ '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$');
