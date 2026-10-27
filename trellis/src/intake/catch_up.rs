//! Rebuilding the readers of a table that may be missing changes to it
//! (ADR-0002, "Convergence and status").
//!
//! A definition applying a table's changes can miss some that never reached
//! it: the capture of the table is installed or re-installed, a build wrote an
//! upstream target outside the target-mutation seam, an operator asked for a
//! re-backfill. Every such reader is rebuilt: it moves `live -> backfilling`
//! and gets a plan and a sweep job, one row each, so the call stays fast. It
//! applies throughout, and the build re-derives every row its source has and
//! every ledger entry its source no longer backs.
//!
//! Until the rebuild finishes the reader is not `live`, so `live` holds along
//! a chain: a definition reading an upstream that is not `live` reports
//! `catching_up` ([`crate::defs::catalog::reported_statuses`]), and the reader
//! of a rebuilt upstream is itself rebuilt here.

use tokio_postgres::GenericClient;

use super::error::IntakeError;

/// Rebuilds the readers of `definition_id`'s *target* table, which a build of
/// the definition just rewrote outside the target-mutation seam
/// (`staging::target_mutations`): a reader already attached (only
/// possible for a rebuilt, resumed upstream, since `defs::catalog` refuses to
/// attach a new one to a target still being built) may be missing what the
/// rebuild wrote. Called by the transaction that finishes the build.
///
/// A reader is every applying definition that reads the target
/// ([`crate::defs::catalog::applying_readers`]). A no-op for a target nothing
/// reads. Returns the rebuilt definitions' ids; the caller calls
/// [`crate::staging::build::stamp_rebuild_seg`] with them as the last
/// statement before it commits.
pub(crate) async fn park_target_catchup_if_read(
    client: &impl GenericClient,
    definition_id: i64,
) -> Result<Vec<i64>, IntakeError> {
    let target: String = client
        .query_one(
            "select target_table from transform_definitions where id = $1",
            &[&definition_id],
        )
        .await?
        .get(0);
    let readers = crate::defs::catalog::applying_readers(client, &target).await?;
    rebuild(client, readers).await
}

/// Rebuilds the readers of each of `tables` that may be missing changes to
/// it: a table whose capture triggers are installed (`capture::install`), and
/// an explicit re-backfill ([`crate::Trellis::request_backfill`]).
/// Returns the rebuilt definitions' ids, for the caller to pass to
/// [`crate::staging::build::stamp_rebuild_seg`] as the last statement before
/// it commits, so the share lock on the active segment is held only for the
/// commit.
///
/// `new_nodes` says the install only added nodes new to a captured
/// partitioned root's recorded tree (`capture::install::adds_only_new_nodes`).
/// A definition reading the root that the partition correction engine serves
/// (an aggregate, or a `JOIN` reading the root as either side) is then not
/// rebuilt: the reconcile pass queues each new node's correction, which
/// recomputes the groups (or the `JOIN` side's cells) of the node alone, the
/// node's own rows' among them.
pub(crate) async fn park_table_catch_ups(
    client: &impl GenericClient,
    tables: &[String],
    new_nodes: bool,
) -> Result<Vec<i64>, IntakeError> {
    let mut readers = crate::staging::build::rebuild_candidates(client, tables).await?;
    if new_nodes {
        let mut corrected = Vec::new();
        for &id in &readers {
            if let Some(definition) = crate::defs::catalog::definition_by_id_in(client, id).await?
                && corrected_by_new_node(client, &definition, tables).await?
            {
                corrected.push(id);
            }
        }
        readers.retain(|id| !corrected.contains(id));
    }
    rebuild(client, readers).await
}

/// Rebuilds `readers` ([`crate::staging::build::start_rebuilds`]) under their
/// row locks, taken in id order.
async fn rebuild(
    client: &impl GenericClient,
    mut readers: Vec<i64>,
) -> Result<Vec<i64>, IntakeError> {
    readers.sort_unstable();
    readers.dedup();
    if readers.is_empty() {
        return Ok(readers);
    }
    client
        .execute(
            "select 1 from transform_definitions where id = any($1) order by id for update",
            &[&readers],
        )
        .await?;
    let rebuilt = crate::staging::build::rebuildable(client, &readers).await?;
    crate::staging::build::start_rebuilds(client, &rebuilt).await?;
    Ok(rebuilt)
}

/// Whether the partition correction engine corrects `definition` for a node
/// new to one of `tables`' recorded trees (`park_table_catch_ups`'s
/// `new_nodes`): a `JOIN` reading the table as either side, whose node
/// correction recomputes the side's cells that route to the node;
/// or an aggregate over the table that the engine re-derives
/// ([`crate::staging::build::corrected_on_partitioned_source`]).
async fn corrected_by_new_node(
    client: &impl GenericClient,
    definition: &crate::defs::model::Definition,
    tables: &[String],
) -> Result<bool, IntakeError> {
    if crate::staging::join::applies(&definition.def) {
        return Ok(tables.contains(&definition.source_table)
            || crate::defs::catalog::join_table_of(client, definition.id)
                .await?
                .is_some_and(|joined| tables.contains(&joined)));
    }
    Ok(tables.contains(&definition.source_table)
        && crate::staging::build::corrected_on_partitioned_source(definition))
}
