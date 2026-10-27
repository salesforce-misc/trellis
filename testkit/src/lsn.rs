//! The LSN a test stages a CDC ring row at.
//!
//! A capture trigger stages each change at the WAL insert position it reads
//! as the statement runs, which is below the position read after the commit
//! and above any position read before the statement. Apply compares that
//! LSN with the positions it keeps: an Apply changes a 1-1 ledger entry only
//! if its `lsn` is above the entry's `__applied_lsn` and the target's
//! truncate floor. A test that stages at a made-up LSN like `0/1` sits below
//! every such position the test set up, so it only ever exercises the
//! refusal, and a race that needs the change to apply never shows up.
//!
//! [`wal_insert_lsn`] is the default for staging: read it right after the
//! source write the staged change stands for, and the change lands where
//! capture would have put it relative to every position the test set up. A
//! test that genuinely needs a specific LSN (fold ordering, a hand-built
//! position comparison) passes its own and says why at the call site.

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
