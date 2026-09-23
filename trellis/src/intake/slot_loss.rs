//! Issue #310: what the staging worker does when the replication slot it has
//! confirmed work against is gone — missing (a pre-PG-17 failover, a restore
//! of the source database from a backup or PITR, a manual drop) or
//! invalidated (`wal_status = 'lost'`, the retention cap).
//!
//! Every change between the slot's last confirmed position and whatever a new
//! slot starts from is unrecoverable by streaming, so every target the slot
//! fed is now stale in a way no catch-up can fix. The only correct recovery is
//! a fresh backfill per target, which is exactly what `RESUME TRANSFORM`
//! already does (ADR-0014: "resume rebuilds by backfill, not by catch-up").
//! So rather than refuse to start (this used to be a hard
//! [`IntakeError::SlotLost`] out of startup), [`pause_if_slot_lost`]:
//!
//! 1. **Pauses every transform the slot fed**, directly or through a chain of
//!    targets ([`transforms_fed_by_publication`]), with the ordinary
//!    [`crate::defs::lifecycle::pause_transform`] — the same
//!    [`TransformStatus::Paused`] an operator's `PAUSE` writes — and records
//!    why in `slot_loss_pauses`, since the status column has no room for a
//!    reason.
//! 2. **Recreates the slot** and moves `replication_progress` to its start, so
//!    intake comes up streaming again. Nothing it stages reaches a paused
//!    target, but a stream must already be running for a later resume's
//!    backfill to be gap-free: the backfill enumerates the source as of its
//!    fence and relies on the stream for everything after. Recreating it now,
//!    rather than at the first resume, means a resume is exactly the resume
//!    that already exists, with no new sequencing between it and intake.
//! 3. **Logs loudly and keeps logging.** One `error` naming the slot, the
//!    lost position and every affected transform, then a `warn` reminder from
//!    the maintenance loop every [`SLOT_LOSS_REMINDER_INTERVAL`]
//!    ([`log_slot_loss_reminder`]) for as long as any of them is still frozen.
//!    The record is durable, so the reminder survives a restart.
//! 4. **Never resumes anything on its own.** An operator mid-DR resumes each
//!    transform when the source is stable, in whatever order they choose; each
//!    resume is one ordinary fresh backfill.
//!
//! Pending backfill markers whose table has no unfrozen definition left are
//! discarded too: discharging one would enumerate a whole table into the ring
//! for no reader, which is the "expensive work the operator didn't ask for"
//! this path exists to avoid. Each resume parks its own marker.
//!
//! **Crash safety.** The record is written before the pause, and the pause
//! before the slot is recreated, so a crash anywhere in the sequence leaves the
//! slot still missing and the next startup redoes the rest (every step is
//! idempotent). A crash after the slot is created but before the new position
//! commits leaves `replication_progress` at the old position; streaming from an
//! older `start_lsn` than a logical slot's own position starts at the slot's
//! position, and every fed transform is already paused, so nothing is lost
//! that wasn't already lost.

use std::time::Duration;

use tokio_postgres::GenericClient;
use tokio_postgres::types::PgLsn;

use super::error::IntakeError;
use super::publication::require_slot_healthy;
use crate::defs::catalog::CatalogError;
use crate::defs::lifecycle::{PauseOutcome, pause_transform};
use crate::defs::model::TransformStatus;
use crate::pool::Pool;
use crate::staging::session::ProducerSession;

/// How often the maintenance loop re-logs the transforms still paused by a
/// slot loss (issue #310: "roughly once a minute", so an operator who missed
/// the first message still finds it).
pub const SLOT_LOSS_REMINDER_INTERVAL: Duration = Duration::from_secs(60);

/// A transform the lost slot's publication feeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FedTransform {
    /// `transform_definitions.id`.
    pub id: i64,
    /// The bare target name, the spelling `PAUSE`/`RESUME TRANSFORM` take.
    pub name: String,
    /// Its status when enumerated.
    pub status: TransformStatus,
}

/// One `slot_loss_pauses` row whose transform is still frozen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotLossPause {
    /// The bare target name.
    pub transform: String,
    /// The slot whose loss paused it.
    pub slot: String,
    /// The lost slot's last confirmed position: everything after it, up to
    /// the recreated slot's start, never reached this transform.
    pub lost_confirmed_lsn: PgLsn,
}

/// What [`pause_if_slot_lost`] did about a lost slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotLossRecovery {
    /// Transforms this pass moved to [`TransformStatus::Paused`].
    pub paused: Vec<String>,
    /// Transforms the slot fed that were already frozen (paused or
    /// quarantined) and were left as they were.
    pub already_frozen: Vec<String>,
    /// Where the recreated slot starts streaming.
    pub new_slot_lsn: PgLsn,
}

/// Every transform sourced from a table in `publication`, directly or through
/// any chain of transforms reading another's target, in definition order.
///
/// Seeded from `pg_publication_tables` rather than
/// [`crate::defs::catalog::all_source_tables`]: the question is what the slot
/// *actually* delivered, and a definition whose table the publication doesn't
/// carry was never fed by it. The recursive step follows `source_table =
/// target_table`, which is how one transform reads another's output. In a
/// reconciled install every definition's source is published (reconciliation
/// publishes every `source_table`, targets included), so this names every
/// transform; the chain walk is what keeps that true while a newly defined
/// table is still waiting for the next reconcile pass.
pub async fn transforms_fed_by_publication(
    client: &impl GenericClient,
    publication: &str,
) -> Result<Vec<FedTransform>, IntakeError> {
    let rows = client
        .query(
            "with recursive fed(id) as ( \
                 select t.id from transform_definitions t \
                 join pg_publication_tables p \
                   on p.pubname = $1 and t.source_table = p.schemaname || '.' || p.tablename \
               union \
                 select t.id from transform_definitions t \
                 join transform_definitions up on t.source_table = up.target_table \
                 join fed on fed.id = up.id \
             ) \
             select t.id, split_part(t.target_table, '.', 2), t.status \
             from transform_definitions t join fed on fed.id = t.id \
             order by t.id",
            &[&publication],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let status_text: String = row.get(2);
            let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            });
            FedTransform {
                id: row.get(0),
                name: row.get(1),
                status,
            }
        })
        .collect())
}

/// Checks `slot` (which has a `replication_progress` row, so this instance has
/// confirmed work against it) and, if it is missing or invalidated, runs the
/// recovery the module doc describes. `Ok(None)` for a healthy slot.
///
/// Uses `session`, the staging worker's own [`ProducerSession`], for the slot
/// work (the producer singleton lock it holds is what makes this the only
/// process touching the slot), and `pool` for the pauses, which
/// [`pause_transform`] runs on pooled connections.
pub async fn pause_if_slot_lost(
    session: &mut ProducerSession,
    pool: &Pool,
    slot: &str,
    publication: &str,
) -> Result<Option<SlotLossRecovery>, IntakeError> {
    let last_confirmed: PgLsn = session
        .client()
        .query_one(
            "select confirmed_lsn from replication_progress where slot_name = $1",
            &[&slot],
        )
        .await?
        .get(0);
    match require_slot_healthy(session.client(), slot, last_confirmed).await {
        Ok(()) => return Ok(None),
        Err(IntakeError::SlotLost { .. }) => {}
        Err(other) => return Err(other),
    }

    let fed = transforms_fed_by_publication(session.client(), publication).await?;
    let (frozen, unfrozen): (Vec<_>, Vec<_>) = fed.into_iter().partition(|t| t.status.is_frozen());

    // Record before pausing: see the module doc's crash-safety note.
    let ids: Vec<i64> = unfrozen.iter().map(|t| t.id).collect();
    session
        .client()
        .execute(
            "insert into slot_loss_pauses (transform_id, slot_name, lost_confirmed_lsn) \
             select id, $2, $3 from unnest($1::bigint[]) as id \
             on conflict (transform_id) do nothing",
            &[&ids, &slot, &last_confirmed],
        )
        .await?;

    let mut paused = Vec::with_capacity(unfrozen.len());
    for transform in unfrozen {
        match pause_transform(pool, &transform.name).await {
            Ok(PauseOutcome::Paused | PauseOutcome::AlreadyFrozen(_)) => {
                paused.push(transform.name);
            }
            // Dropped concurrently: nothing left to pause, and the record went
            // with it (`on delete cascade`).
            Err(CatalogError::TransformNotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
    }
    let already_frozen: Vec<String> = frozen.into_iter().map(|t| t.name).collect();

    let frozen_statuses: Vec<&str> = TransformStatus::ALL
        .iter()
        .filter(|s| s.is_frozen())
        .map(|s| s.as_str())
        .collect();
    session
        .client()
        .execute(
            "delete from pending_backfill pb where not exists ( \
                 select 1 from transform_definitions t \
                 where t.source_table = pb.table_name and t.status <> all($1) \
             )",
            &[&frozen_statuses],
        )
        .await?;

    let new_slot_lsn = recreate_slot(session, slot).await?;

    tracing::error!(
        slot = %slot,
        lost_confirmed_lsn = %last_confirmed,
        new_slot_lsn = %new_slot_lsn,
        paused = ?paused,
        already_frozen = ?already_frozen,
        "replication slot {slot} was missing or invalidated; every change after its last \
         confirmed position {last_confirmed} is unrecoverable by streaming, so every transform \
         it fed is paused rather than left silently stale. Paused now: [{}]; already frozen: \
         [{}]. Trellis recreated the slot (streaming from {new_slot_lsn}) and will not resume \
         these on its own: once the source database is stable, run RESUME TRANSFORM <name> for \
         each, which rebuilds it by a fresh backfill from current source data",
        paused.join(", "),
        already_frozen.join(", "),
    );

    Ok(Some(SlotLossRecovery {
        paused,
        already_frozen,
        new_slot_lsn,
    }))
}

/// Drops `slot` if an invalidated copy of it is still registered in this
/// database (it can never stream again, so dropping it loses nothing), creates
/// it fresh, and moves `replication_progress` to the new slot's start in the
/// same transaction as the create.
///
/// `pg_create_logical_replication_slot` is the transaction's first statement,
/// as Postgres requires of a transaction that creates a logical slot (it
/// refuses one that has already written); the slot itself persists the moment
/// the call returns, independent of the commit — see the module doc for why a
/// crash in between is harmless here.
async fn recreate_slot(session: &mut ProducerSession, slot: &str) -> Result<PgLsn, IntakeError> {
    session
        .client()
        .execute(
            "select pg_drop_replication_slot(slot_name) from pg_replication_slots \
             where slot_name = $1 and database = current_database()",
            &[&slot],
        )
        .await?;
    let txn = session.transaction().await?;
    let lsn: PgLsn = txn
        .query_one(
            "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await?
        .get(0);
    txn.execute(
        "update replication_progress set confirmed_lsn = $2 where slot_name = $1",
        &[&slot, &lsn],
    )
    .await?;
    txn.commit().await?;
    Ok(lsn)
}

/// Every `slot_loss_pauses` record whose transform is still frozen, in
/// definition order, after pruning the ones that aren't (resumed by some path
/// that didn't delete the record itself).
pub async fn slot_loss_paused_transforms(
    client: &impl GenericClient,
) -> Result<Vec<SlotLossPause>, IntakeError> {
    let frozen_statuses: Vec<&str> = TransformStatus::ALL
        .iter()
        .filter(|s| s.is_frozen())
        .map(|s| s.as_str())
        .collect();
    client
        .execute(
            "delete from slot_loss_pauses s using transform_definitions t \
             where t.id = s.transform_id and t.status <> all($1)",
            &[&frozen_statuses],
        )
        .await?;
    let rows = client
        .query(
            "select split_part(t.target_table, '.', 2), s.slot_name, s.lost_confirmed_lsn \
             from slot_loss_pauses s join transform_definitions t on t.id = s.transform_id \
             order by t.id",
            &[],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| SlotLossPause {
            transform: row.get(0),
            slot: row.get(1),
            lost_confirmed_lsn: row.get(2),
        })
        .collect())
}

/// The periodic half of the operator message: a `warn` naming every transform
/// still paused by a slot loss, or nothing once they have all been resumed.
/// Called from the staging worker's maintenance loop every
/// [`SLOT_LOSS_REMINDER_INTERVAL`].
pub async fn log_slot_loss_reminder(client: &impl GenericClient) -> Result<(), IntakeError> {
    let pauses = slot_loss_paused_transforms(client).await?;
    if pauses.is_empty() {
        return Ok(());
    }
    let names: Vec<&str> = pauses.iter().map(|p| p.transform.as_str()).collect();
    tracing::warn!(
        transforms = ?names,
        "{} transform(s) are still paused because the replication slot feeding them was lost \
         ({}); Trellis will not resume them on its own. Once the source database is stable, run \
         RESUME TRANSFORM <name> for each to rebuild it by a fresh backfill",
        names.len(),
        describe_losses(&pauses),
    );
    Ok(())
}

/// `"order_rollup: slot trellis_slot after 0/16B3748, ..."` — each transform
/// with the slot and position it lost, so the reminder is self-contained.
fn describe_losses(pauses: &[SlotLossPause]) -> String {
    pauses
        .iter()
        .map(|p| {
            format!(
                "{}: slot {} after {}",
                p.transform, p.slot, p.lost_confirmed_lsn
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_losses_names_each_transform_with_its_slot_and_position() {
        let pauses = vec![
            SlotLossPause {
                transform: "order_rollup".into(),
                slot: "trellis_slot".into(),
                lost_confirmed_lsn: PgLsn::from(0x16B3748),
            },
            SlotLossPause {
                transform: "order_echo".into(),
                slot: "trellis_slot".into(),
                lost_confirmed_lsn: PgLsn::from(0x1_0000_0010),
            },
        ];
        assert_eq!(
            describe_losses(&pauses),
            "order_rollup: slot trellis_slot after 0/16B3748, \
             order_echo: slot trellis_slot after 1/10"
        );
    }
}
