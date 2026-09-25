-- Issue #372: a relationship records the schema its to-table resolved to at
-- declaration time, the mirror of V34's `from_schema` (#285/#288).
--
-- `to_table` is persisted bare, and every reader resolved it through its own
-- session's `search_path`. A connection whose `search_path` found a different
-- (or no) `users` than the declaring connection's silently pointed the
-- relationship at the wrong table: the fold's joins, the projection's widen,
-- reverse recompute (`relationships_to_table`) and catch-up parking all read
-- whichever `users` came first. Every reader now uses
-- `to_schema || '.' || to_table` instead.
--
-- Not part of the uniqueness key: a relationship is still named per
-- qualified from-table, `(from_schema, from_table, name)`, and where it points
-- doesn't change which relationship a name refers to.
--
-- Existing rows are cleared rather than backfilled, following V34: Trellis is
-- pre-release, and a guessed `to_schema` would be exactly the wrong-table
-- mismatch this column exists to prevent. `cascade` clears
-- `relationship_projections`, which references this table.
truncate table relationship_definitions cascade;

alter table relationship_definitions
    add column to_schema text not null;

drop index if exists relationship_definitions_to_table_idx;

create index relationship_definitions_to_schema_to_table_idx
    on relationship_definitions (to_schema, to_table);
