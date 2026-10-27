//! Test harness: a synchronous stand-in for the staging worker and a drain
//! thread, for a test that runs with no staging worker and no capture
//! triggers.

use std::time::Duration;

use tokio_postgres::GenericClient;

use super::error::IntakeError;
use crate::defs::model::TransformStatus;
use crate::pool::quote_ident;
use crate::staging::append::{self, StagedChange};

/// Test stand-in for the staging worker's maintenance pass over newly
/// registered definitions (ADR-0002), for a test with no staging worker and no
/// capture triggers: starts the Re-derive build of every `waiting_to_backfill`
/// definition ([`crate::staging::build::start_ready_builds`]), treating every
/// source as captured. A definition held by a capture gate, or by a field
/// edit awaiting its capture, stays `waiting_to_backfill`: the call retries
/// for a few seconds, then returns. Builds it starts still need a drain worker
/// (or [`settle_builds`]).
pub async fn start_registered_builds(pool: &crate::pool::Pool) -> Result<(), IntakeError> {
    let mut client = pool
        .get()
        .await
        .map_err(crate::defs::catalog::CatalogError::from)?;
    for _ in 0..100 {
        let waiting: Vec<i64> = client
            .query(
                "select id from transform_definitions where status = $1 order by id",
                &[&TransformStatus::WaitingToBackfill.as_str()],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        if waiting.is_empty() {
            return Ok(());
        }
        crate::staging::build::start_ready_builds(&mut client, pool, &waiting).await?;
        if !still_waiting(&client).await? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

async fn still_waiting(client: &tokio_postgres::Client) -> Result<bool, IntakeError> {
    Ok(client
        .query_one(
            "select exists (select 1 from transform_definitions where status = $1)",
            &[&TransformStatus::WaitingToBackfill.as_str()],
        )
        .await?
        .get(0))
}

/// [`start_registered_builds`], then runs every build to completion: every
/// registered definition is `live` (or paused by its build's failure) when it
/// returns. A synchronous stand-in for the staging worker and a drain thread
/// together, for a test that needs its definitions `live` before it goes on
/// (to chain a definition off one, say). Panics on any failure: it is test
/// harness.
pub async fn settle_builds(pool: &crate::pool::Pool) {
    start_registered_builds(pool)
        .await
        .expect("start registered definitions' builds");
    crate::staging::build::settle_builds(pool).await;
}

/// Test harness: records a `live` definition whose target is
/// `table` (a schema-qualified identity, as `transform_definitions` holds
/// it), feeding it from a table named `<table>__feed` that doesn't exist.
/// Nothing runs that definition; it makes `table` read as another
/// definition's target, which the target-mutation seam feeds, so a definition
/// over `table` is built without capture triggers on it. Idempotent.
pub async fn feed_from_a_test_definition(
    client: &impl GenericClient,
    table: &str,
) -> Result<(), IntakeError> {
    let feed = format!("{table}__feed");
    let bare = table.rsplit('.').next().unwrap_or(table);
    client
        .execute(
            "insert into source_table_versions (source_table, version) values ($1, 1) \
             on conflict do nothing",
            &[&feed],
        )
        .await?;
    client
        .execute(
            "insert into transform_definitions \
             (target_table, source_table, source_version, definition_text, status) \
             values ($1, $2, 1, $3, $4) on conflict (target_table) do nothing",
            &[
                &table,
                &feed,
                &format!("TRANSFORM {bare} FROM {bare}__feed SELECT 0 AS feed"),
                &TransformStatus::Live.as_str(),
            ],
        )
        .await?;
    Ok(())
}

/// Page size for [`enumerate_and_append`]'s server-side cursor, so one
/// enumeration is O(page) memory rather than O(table).
const ENUMERATION_PAGE_ROWS: i64 = 10_000;

/// The fixed cursor name [`enumerate_and_append`] declares.
const ENUMERATION_CURSOR: &str = "trellis_harness_cursor";

/// Enumerates `src_table`'s current rows as image-less
/// [`StagedChange::Recompute`] triggers and appends each page directly into
/// the active ring segment, inside `txn`. The `create_definition` test
/// fixture's stand-in for a build: the rows it stages are what the drain
/// re-derives the fixture's target from. Production builds read the source
/// themselves (`staging::build`).
pub(crate) async fn enumerate_and_append(
    txn: &tokio_postgres::Transaction<'_>,
    src_table: &str,
) -> Result<(), IntakeError> {
    let (schema, table) = src_table
        .split_once('.')
        .ok_or_else(|| IntakeError::InvalidTableName(src_table.to_string()))?;
    let from = format!("{}.{}", quote_ident(schema), quote_ident(table));
    // The table's row-identity key as `ddl` defines it: its `PRIMARY KEY`,
    // or, for an aggregate target (which has none), the `UNIQUE NULLS NOT
    // DISTINCT` grouping columns.
    let key_cols = crate::defs::ddl::identity_key_columns(txn, src_table).await?;
    if key_cols.is_empty() {
        return Err(IntakeError::NoIdentityKey {
            table: src_table.to_string(),
        });
    }
    // The same expression every other producer of this table's key text
    // uses, so the staged key is byte-identical to the live path's.
    let key_expr = crate::defs::ddl::pk_key_sql_expr(&key_cols, None);
    txn.batch_execute(&format!(
        "declare {ENUMERATION_CURSOR} cursor for select {key_expr} from {from}"
    ))
    .await?;
    loop {
        let rows = txn
            .query(
                &format!("fetch forward {ENUMERATION_PAGE_ROWS} from {ENUMERATION_CURSOR}"),
                &[],
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        let page: Vec<StagedChange> = rows
            .iter()
            .map(|row| StagedChange::Recompute {
                src_table: src_table.to_string(),
                key: row.get(0),
                hop_gen: 0,
                src_changed: None,
                prior_image: None,
                // Unknown, so it gates every convergence token until it
                // drains.
                origin_lsn: None,
            })
            .collect();
        append::append(txn, &page).await?;
    }
    txn.batch_execute(&format!("close {ENUMERATION_CURSOR}"))
        .await?;
    Ok(())
}
