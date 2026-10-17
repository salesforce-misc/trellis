-- #1107 (J2, epic #1091): a JOIN transform reads two tables. The `FROM`
-- table stays in `source_table`/`source_columns`; the joined table and the
-- live types of its columns, as define or the last resume read them, sit
-- beside them. Both are null for every other definition.
alter table transform_definitions
    add column join_table text,
    add column join_columns jsonb,
    add constraint transform_definitions_join_sides_check
        check ((join_table is null) = (join_columns is null)
               and (join_table is null or join_table <> source_table));
