-- Issue #288: a relationship name is unique per *schema-qualified* from-table,
-- `(from_schema, from_table, name)`, not per bare from-table name.
--
-- V16 keyed uniqueness on `(from_table, name)` with `from_table` bare, and V34
-- added `from_schema` for verification only, leaving it out of the key. So if
-- `blog.posts` declared a relationship named `author`, the unrelated
-- `shop.posts` could not declare its own `author` at all — it failed as a
-- duplicate even though the two tables share nothing but a name. The key now
-- carries the schema, matching the project's rule that internal table
-- references are always fully qualified (ADR-0011); every reader that used to
-- address a relationship by the bare `(from_table, name)` pair now addresses
-- it by `(from_schema, from_table, name)` too.
--
-- No data work: widening a unique key can't make existing rows collide, and
-- every row already has a non-null `from_schema` (V34). Only the constraint
-- changes.
alter table relationship_definitions
    drop constraint relationship_definitions_from_table_name_key;

alter table relationship_definitions
    add constraint relationship_definitions_from_schema_from_table_name_key
        unique (from_schema, from_table, name);
