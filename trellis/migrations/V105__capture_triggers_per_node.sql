-- #1200 (slice 2 of #707): a partitioned source records the capture triggers
-- of every node of its tree, not only the root's. `node_oid` is the oid of
-- the table a trigger sits on; a plain table's rows keep 0, since the table
-- at a name is judged by its name, not an oid. ALTER, never a rebuild:
-- `capture_triggers`' own oid says whether the rows survived a restore.
alter table capture_triggers add column node_oid oid not null default 0;
alter table capture_triggers alter column node_oid drop default;
alter table capture_triggers drop constraint capture_triggers_pkey;
alter table capture_triggers add primary key (table_name, node_oid, trigger_name);
