//! The columns of the tables Trellis created that hold a `varchar` (#1207).

/// Every column of every table outside `pg_catalog` holding a `varchar`, or
/// an array of one, as `relation.column type`, except the sources `sources`
/// and the migration tool's own table: a column Trellis created. A temporary
/// working table a pooled session still holds is one too.
pub async fn read(client: &tokio_postgres::Client, sources: &[&str]) -> Vec<String> {
    let sources: Vec<String> = sources.iter().map(|s| format!("'{s}'")).collect();
    let sql = format!(
        "select n.nspname || '.' || c.relname || '.' || a.attname || ' ' || \
                pg_catalog.format_type(a.atttypid, a.atttypmod) \
         from pg_catalog.pg_attribute a \
         join pg_catalog.pg_class c on c.oid = a.attrelid \
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
         where a.attnum > 0 and not a.attisdropped and c.relkind in ('r', 'p') \
           and a.atttypid in ('pg_catalog.varchar'::regtype, 'pg_catalog._varchar'::regtype) \
           and n.nspname not in ('pg_catalog', 'information_schema') \
           and c.relname <> 'refinery_schema_history' \
           and n.nspname || '.' || c.relname not in ({}) \
         order by 1",
        sources.join(", ")
    );
    client
        .query(&sql, &[])
        .await
        .expect("read the varchar columns")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}
