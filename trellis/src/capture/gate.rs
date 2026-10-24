//! The capture gate (`capture::install`, "Widening and the capture gate"): a
//! per-table WAL position that a capture install or widen records, below
//! which the rows the old trigger body staged all lie.
//!
//! A definition whose build would meet one of those rows, imaged without a
//! column the definition reads, must not start while any change to the table
//! at or below the gate is still pending
//! ([`crate::staging::converge::table_changes_pending_through`]). Once none
//! is, the gate has served and its row is deleted by the pass that starts the
//! builds it held ([`release_passed`]).

use std::collections::HashSet;

use tokio_postgres::GenericClient;

use super::CaptureError;
use crate::PgLsn;

/// Raises `table`'s gate to `lsn`, never lowering it. Called in the
/// transaction of the install or widen, after it holds the table's lock.
pub(super) async fn record(
    client: &impl GenericClient,
    table: &str,
    lsn: PgLsn,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "insert into capture_gates (table_name, gate_lsn) values ($1, $2) \
             on conflict (table_name) do update \
             set gate_lsn = greatest(capture_gates.gate_lsn, excluded.gate_lsn)",
            &[&table, &lsn],
        )
        .await?;
    Ok(())
}

/// Whether `table` has a gate a pending change still holds.
pub(crate) async fn holds(
    client: &impl GenericClient,
    table: &str,
) -> Result<bool, crate::staging::StagingError> {
    let gate: Option<PgLsn> = client
        .query_opt(
            "select gate_lsn from capture_gates where table_name = $1",
            &[&table],
        )
        .await?
        .map(|row| row.get(0));
    match gate {
        Some(gate) => {
            crate::staging::converge::table_changes_pending_through(client, table, gate).await
        }
        None => Ok(false),
    }
}

/// Every table whose gate a pending change still holds.
pub(crate) async fn held_tables(
    client: &impl GenericClient,
) -> Result<HashSet<String>, CaptureError> {
    let mut held = HashSet::new();
    for (table, gate) in gates(client).await? {
        if pending(client, &table, gate).await? {
            held.insert(table);
        }
    }
    Ok(held)
}

/// Deletes the gates no pending change holds any more, except one an install
/// raised since this read it. The staging worker's pass runs it after it has
/// started the builds the gates held.
pub(crate) async fn release_passed(client: &impl GenericClient) -> Result<(), CaptureError> {
    for (table, gate) in gates(client).await? {
        if !pending(client, &table, gate).await? {
            client
                .execute(
                    "delete from capture_gates where table_name = $1 and gate_lsn = $2",
                    &[&table, &gate],
                )
                .await?;
        }
    }
    Ok(())
}

async fn gates(client: &impl GenericClient) -> Result<Vec<(String, PgLsn)>, CaptureError> {
    Ok(client
        .query("select table_name, gate_lsn from capture_gates", &[])
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

async fn pending(
    client: &impl GenericClient,
    table: &str,
    gate: PgLsn,
) -> Result<bool, CaptureError> {
    Ok(
        crate::staging::converge::table_changes_pending_through(client, table, gate)
            .await
            .map_err(crate::intake::IntakeError::from)?,
    )
}
