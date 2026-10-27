//! A from-scratch recompute of a 1-1 target, computed by Postgres.
//!
//! Renders the definition back to the `SELECT` over the source's current
//! contents ([`render_one_to_one_select_sql`]) and runs it. A test compares a
//! maintained target with the rows this returns.

use std::collections::HashMap;

use trellis::Pool;
use trellis::defs::{TransformDef, render_one_to_one_select_sql};

/// Each source row's primary key as text (`"\u{1}"`, the engine's null-key
/// sentinel, for a `NULL` key) mapped to its calculated fields as text (`None`
/// is SQL `NULL`).
pub type Recomputed = HashMap<String, HashMap<String, Option<String>>>;

/// Runs `def` as a `SELECT` over its source and reads the rows back as text.
pub async fn recompute(pool: &Pool, def: &TransformDef, pk_column: &str) -> Recomputed {
    let sql = render_one_to_one_select_sql(def, pk_column);
    let client = pool.get().await.expect("pool connection");
    let rows = client
        .query(sql.as_str(), &[])
        .await
        .unwrap_or_else(|e| panic!("oracle select failed: {e}\n{sql}"));
    rows.iter()
        .map(|row| {
            let pk: Option<String> = row.get(0);
            let pk = pk.unwrap_or_else(|| "\u{1}".to_string());
            let fields = def
                .fields
                .iter()
                .enumerate()
                .map(|(i, field)| (field.name.clone(), row.get(i + 1)))
                .collect();
            (pk, fields)
        })
        .collect()
}
