-- Issue #379: record the schema a relationship's settled projection table was
-- created in. `defs::catalog::ensure_relationship_projection_in_txn` creates
-- it under the declaring connection's `target_schema`, but `projection_table`
-- only held the bare name, so every later reader (definition widening, the
-- forward read, the reverse advance, the `TRUNCATE` clear, `DROP
-- RELATIONSHIP`) qualified it with its *own* connection's `target_schema`.
-- Two connections sharing one catalog with different target schemas then read,
-- altered or dropped a table that isn't there.
--
-- Existing rows are backfilled from `pg_class`. `projection_table`
-- (`_trellis_rel_projection_<id>`) is unique only within this catalog: another
-- Trellis instance in the same database has its own `_trellis_rel_projection_1`
-- in its own target schema. Until now every reader looked in its own
-- connection's target schema, which this migration's session has on its
-- `search_path` (instance schema, target schema, `public`; see
-- `pool::session_bootstrap`), so a match there wins. Only a projection
-- declared through a connection with some other target schema falls through
-- to a table outside the `search_path`.
alter table relationship_projections add column projection_schema text;

update relationship_projections p
   set projection_schema = (
       select n.nspname
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
        where c.relname = p.projection_table
          and c.relkind in ('r', 'p')
        order by array_position(current_schemas(false), n.nspname) nulls last,
                 n.nspname
        limit 1
   );

alter table relationship_projections alter column projection_schema set not null;
