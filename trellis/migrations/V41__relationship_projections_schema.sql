-- Issue #379: record the schema a relationship's settled projection table was
-- created in. `defs::catalog::ensure_relationship_projection_in_txn` creates
-- it under the declaring connection's `target_schema`, but `projection_table`
-- only held the bare name, so every later reader (definition widening, the
-- forward read, the reverse advance, the `TRUNCATE` clear, `DROP
-- RELATIONSHIP`) qualified it with its *own* connection's `target_schema`.
-- Two connections sharing one catalog with different target schemas then read,
-- altered or dropped a table that isn't there.
--
-- Existing rows are backfilled from `pg_class`: `projection_table` is unique
-- and Trellis-generated (`_trellis_rel_projection_<id>`), so the one table of
-- that name is the projection.
alter table relationship_projections add column projection_schema text;

update relationship_projections p
   set projection_schema = n.nspname
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
 where c.relname = p.projection_table
   and c.relkind in ('r', 'p');

alter table relationship_projections alter column projection_schema set not null;
