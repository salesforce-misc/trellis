//! A 1-1 `JOIN` definition's build (ADR-0021): the minimal one.
//!
//! A fresh build, a `RESUME` or a rebuild in place fills `M`, `R`, `J` and
//! the target in one `READ COMMITTED` transaction, held under the pair's lock
//! row `FOR UPDATE` ([`crate::staging::join_one_to_one::build`]), then
//! completes the definition. A rebuild has no sweep job: the build itself
//! turns the entries whose rows are gone into tombstones. Pages wait on the lock row for the whole transaction, which at
//! 100M `a` rows is minutes (#1375 Y3); the chunked build that replaces it is
//! #1400.

use std::time::Instant;

use tokio_postgres::IsolationLevel;

use super::{CHUNK_APPLICATION_NAME, ChunkQueueError, ClaimFence, ClaimedChunk, Definition, Pool};
use super::{build_error, metrics, try_complete};

/// Runs a claimed plan job of the 1-1 `JOIN` `definition`: the whole build,
/// fenced by the claim, and the job's done mark in the same transaction.
pub(super) async fn run(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    definition: &Definition,
    fence: &ClaimFence<'_>,
) -> Result<(), ChunkQueueError> {
    let started = Instant::now();
    let pk = crate::defs::ddl::source_primary_key(pool, &definition.source_table)
        .await
        .map_err(build_error)?;
    let shape = crate::staging::join_one_to_one::JoinShape::load(
        pool,
        definition,
        pk,
        &std::collections::HashSet::new(),
    )
    .await
    .map_err(build_error)?;
    let m_pk = crate::staging::join_one_to_one::joined_primary_key(pool, &shape)
        .await
        .map_err(build_error)?;
    let mut pooled = pool.get().await?;
    let client: &mut tokio_postgres::Client = &mut pooled;
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&format!(
        "set local application_name = '{CHUNK_APPLICATION_NAME}'"
    ))
    .await?;
    // The definition row before the job row, as every other build takes
    // them (`run_group_structure`).
    fence.arm(&txn).await?;
    txn.execute(
        "select 1 from transform_definitions where id = $1 for key share",
        &[&chunk.definition_id],
    )
    .await?;
    if !fence.hold(&txn).await? {
        txn.rollback().await?;
        return crate::defs::chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
    }
    let (written, deleted) = crate::staging::join_one_to_one::build(&txn, &shape, &m_pk)
        .await
        .map_err(build_error)?;
    txn.execute(
        "update backfill_chunks set done = true, claimed_by = null, claimed_at = null \
         where id = $1",
        &[&chunk.id],
    )
    .await?;
    #[cfg(any(test, feature = "test-util"))]
    crate::staging::interleave::pause_at(
        &txn,
        crate::staging::interleave::PausePoint::BeforeRebuildCommit,
        &definition.target_table,
    )
    .await?;
    txn.commit().await?;
    metrics::record_build_chunk(started.elapsed(), (written + deleted) as u64);
    crate::instance_log::debug!(
        definition_id = chunk.definition_id,
        written,
        deleted,
        "1-1 JOIN build filled its hidden tables and its target"
    );
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}
