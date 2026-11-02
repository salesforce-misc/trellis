//! Common fixtures for exercising a source table plus a derived/target
//! table — the shape most staging/claiming test suites need (source rows
//! land; the fence/fold/apply logic produces derived rows downstream).
//!
//! These are deliberately minimal scaffolding, not a framework: copy and
//! adapt the SQL for whatever shape a given test suite actually needs.
//! Table names are interpolated directly into SQL text rather than bound,
//! since Postgres doesn't support binding identifiers — that's fine here
//! because callers are test code, not untrusted input.

use trellis::dev::pool::Pool;

/// Creates a minimal source table: `id bigint primary key`, `payload text`,
/// `updated_at timestamptz`.
pub async fn create_source_table(pool: &Pool, table: &str) {
    let client = pool.get().await.expect("acquire connection");
    client
        .batch_execute(&format!(
            "create table {table} (
                id bigint primary key,
                payload text not null,
                updated_at timestamptz not null default now()
            )"
        ))
        .await
        .expect("create source table");
}

/// Inserts a row into a table created by [`create_source_table`].
pub async fn insert_row(pool: &Pool, table: &str, id: i64, payload: &str) {
    let client = pool.get().await.expect("acquire connection");
    client
        .execute(
            &format!("insert into {table} (id, payload) values ($1, $2)"),
            &[&id, &payload],
        )
        .await
        .expect("insert row");
}

/// Updates the payload of an existing row.
pub async fn update_row(pool: &Pool, table: &str, id: i64, payload: &str) {
    let client = pool.get().await.expect("acquire connection");
    client
        .execute(
            &format!("update {table} set payload = $2, updated_at = now() where id = $1"),
            &[&id, &payload],
        )
        .await
        .expect("update row");
}

/// Deletes a row by id.
pub async fn delete_row(pool: &Pool, table: &str, id: i64) {
    let client = pool.get().await.expect("acquire connection");
    client
        .execute(&format!("delete from {table} where id = $1"), &[&id])
        .await
        .expect("delete row");
}

/// Reads all `(id, payload)` pairs from a table, ordered by id — for
/// asserting a source or target/derived table's contents.
pub async fn read_rows(pool: &Pool, table: &str) -> Vec<(i64, String)> {
    let client = pool.get().await.expect("acquire connection");
    client
        .query(&format!("select id, payload from {table} order by id"), &[])
        .await
        .expect("read rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}
