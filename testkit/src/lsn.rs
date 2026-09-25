//! The LSN a test stages a CDC ring row at (issue #512).
//!
//! Intake stages each change at its commit's `end_lsn`, which is always
//! below the WAL insert position read after the commit and above any
//! position read before it. Aggregate apply compares that LSN with a group's
//! recompute horizon (issue #321's `delta_may_be_absorbed`, the extinct
//! horizon, a build's stamped horizon): at or below, the group is re-derived
//! from the source; above, the delta applies. A test that stages at a made-up
//! LSN like `0/1` sits below every horizon, so it only ever exercises the
//! re-derive branch, and a race that needs the delta branch never shows up.
//!
//! [`wal_insert_lsn`] is the default for staging: read it right after the
//! source write the staged change stands for, and the change lands where
//! intake would have put it relative to every horizon the test set up. A
//! test that genuinely needs a specific LSN (fold ordering, a hand-built
//! horizon comparison) passes its own and says why at the call site.

use tokio_postgres::GenericClient;
use tokio_postgres::types::PgLsn;

/// The server's current WAL insert position, the realistic LSN to stage a
/// CDC ring row at. See the [module docs](self).
pub async fn wal_insert_lsn(client: &impl GenericClient) -> PgLsn {
    client
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("read the WAL insert position")
        .get(0)
}
