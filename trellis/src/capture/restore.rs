//! A logical restore of a Trellis database (#1205, ADR-0017 P8).
//!
//! A `pg_dump` and `pg_restore` of the whole database brings back every table
//! Trellis keeps, ordering state included, into a cluster whose WAL position
//! and transaction counter have nothing to do with the original's. Each
//! definition's ledger, group basis and truncate floor then hold positions
//! from the other cluster, and the apply rule (ADR-0002 I2) skips every later
//! change to a key that existed before the dump, for as long as this
//! cluster's counters stay behind.
//!
//! # The signal
//!
//! `capture_triggers` and `partition_sources` each store the oid of their own
//! table. A restore creates the tables again, under new oids, so a stored oid
//! that differs from the live one means the rows came from a logical dump.
//! Every shape of logical restore trips it: into another cluster, into
//! another database of this one, over the same one with `--clean`. None of
//! the operations that keep the WAL and xid counters does (a physical
//! restore, point-in-time recovery, `pg_upgrade`, a promoted replica,
//! `CREATE DATABASE ... TEMPLATE`), and for those the ordering state stays
//! valid. A changed `system_identifier` under the same oid is `pg_upgrade`:
//! [`super::triggers`] re-records and nothing pauses.
//!
//! A logical restore into the same cluster, whose counters only move
//! forward, trips it too and costs one `RESUME` per definition.
//!
//! # The response
//!
//! In one transaction ([`respond`]):
//!
//! - every definition, of every kind and on every source, pauses with
//!   [`reason`] and `capture_failures.restored` set, kind `disruption`;
//! - every change in the ring is discarded. Rows restored from the dump carry
//!   the original cluster's positions and transaction ids, and the restored
//!   sources already contain their changes, which every definition rebuilds
//!   from on `RESUME`. Nothing staged before the detection ever applies,
//!   with one exception (see "The drain side"). The
//!   segment registry starts again with it, because its fences and transaction
//!   ids are the original cluster's and would hold the seal gate until this
//!   cluster's counter passed them, and the slot writers append to is the
//!   active segment's again. A change captured after the detection stays in
//!   the ring;
//! - what the dump brought of the capture record (triggers, baselines,
//!   partition trees, queued corrections) and the capture gates are
//!   forgotten: the next pass records what it finds, and a resume rebuilds.
//!
//! # The drain side
//!
//! A drain worker never responds. Before its first claim it checks [`stale`]
//! and claims nothing while the signal is set, so a fleet started on a
//! restored database applies nothing until a staging worker's pass has
//! responded. It checks once: a drain-only client already running when the
//! database is restored under it can apply rows staged before the detection,
//! until a staging pass runs. The `RESUME` the restore forces rebuilds those
//! targets.
//!
//! Nothing rebuilds on its own. `RESUME` of a definition paused here resets
//! its ordering state before the rebuild (`staging::quarantine`).

use tokio_postgres::Client;

use super::CaptureError;

/// The `capture_failure` of every definition a restore pauses.
pub fn reason() -> String {
    "the database was restored from a logical dump, so the ordering state Trellis kept for \
     this definition belongs to the original database; resume the definition to reset it and \
     rebuild the target from the restored source, or drop the definition"
        .to_string()
}

/// Whether a record that stores the oid of its own table came from another
/// database: a stored oid that isn't the live one.
const STALE_RECORDS: &str = "select exists (select 1 from capture_triggers \
         where catalog_oid is distinct from 'capture_triggers'::pg_catalog.regclass::pg_catalog.oid) \
     or exists (select 1 from partition_sources \
         where catalog_oid is distinct from 'partition_sources'::pg_catalog.regclass::pg_catalog.oid)";

/// Whether the signal is set: a read-only look that responds to nothing. A
/// drain worker's gate before its first claim (`client::wait_out_restore`).
pub(crate) async fn stale(
    client: &impl tokio_postgres::GenericClient,
) -> Result<bool, CaptureError> {
    Ok(client.query_one(STALE_RECORDS, &[]).await?.get(0))
}

/// The capture pass's check, before it looks at any table: when a stored
/// table oid differs from the live one, responds ([`respond`]) and returns
/// `true`.
pub(crate) async fn detect(client: &mut Client) -> Result<bool, CaptureError> {
    if !stale(client).await? {
        return Ok(false);
    }
    respond(client, true).await
}

/// Pauses everything and discards what a restore left (see the module doc),
/// in one transaction. With `recheck`, a concurrent pass that already
/// responded (the records are forgotten, so none is stale any more) leaves
/// nothing to do and this returns `false`.
pub(crate) async fn respond(client: &mut Client, recheck: bool) -> Result<bool, CaptureError> {
    let txn = client.transaction().await?;
    // Two passes responding at once take turns, and a seal waits.
    txn.execute("select 1 from segment_pointer for update", &[])
        .await?;
    if recheck && !txn.query_one(STALE_RECORDS, &[]).await?.get::<_, bool>(0) {
        return Ok(false);
    }
    let error = reason();
    let definitions = txn
        .query(
            "select id, source_table from transform_definitions order by id",
            &[],
        )
        .await?;
    for row in &definitions {
        let (id, table): (i64, String) = (row.get(0), row.get(1));
        if crate::defs::lifecycle::pause_for_restore(&txn, id, &table, &error).await? {
            crate::instance_log::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    // The registry holds the original cluster's fences and transaction ids,
    // and the seal gate compares them with this cluster's (`staging::seal`),
    // so it starts again: one empty active segment in the slot writers
    // already target, numbered above every segment the dump held (a segment
    // number is never reused).
    let next: i64 = txn
        .query_one(
            "select greatest((select coalesce(max(seg_seq), 0) from segments), \
                             (select active_seq from segment_pointer)) + 1",
            &[],
        )
        .await?
        .get(0);
    txn.execute("delete from segments", &[]).await?;
    txn.execute(
        "insert into segments (seg_seq, ring_slot, state) \
         select $1, ring_slot, 'active' from segment_pointer",
        &[&next],
    )
    .await?;
    txn.execute("update segment_pointer set active_seq = $1", &[&next])
        .await?;
    // Writers resolve their slot from `ring_slot_mirror`, which a dump reads
    // outside its snapshot: taken during a seal, it names another slot than
    // the restored pointer, and every write until the next seal would land in
    // a slot no segment covers. Pointed at the active slot again (`setval` is
    // not transactional; the value is the pointer's either way).
    txn.execute(
        "select pg_catalog.setval('ring_slot_mirror', ring_slot, true) from segment_pointer",
        &[],
    )
    .await?;
    // A resume request was made against the original database's state.
    txn.batch_execute(
        "delete from resume_requests; \
         delete from seg_0; delete from seg_1; delete from seg_2; delete from seg_3; \
         delete from capture_triggers; \
         delete from capture_baselines; \
         delete from partition_corrections; \
         delete from partition_sources; \
         delete from capture_gates",
    )
    .await?;
    txn.commit().await?;
    Ok(true)
}
