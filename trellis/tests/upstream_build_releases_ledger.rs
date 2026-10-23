//! `upstream_build_releases.rs`'s aggregate scenarios with the group basis
//! off (#1306): `agg` and the aggregate upstream on the ledger. Its own
//! process, since the knob is process-wide.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/upstream_releases.rs"]
mod upstream_releases;

use upstream_releases::*;

const GROUP_BASIS: bool = false;

#[tokio::test]
async fn chained_bit_aggregates_converge_after_the_upstream_rebuild() {
    chained_bit_aggregates_converge_after_upstream_rebuild(GROUP_BASIS).await;
}

#[tokio::test]
async fn a_persistent_failure_is_retried_once_per_aggregate_upstream_resume() {
    persistent_failure_retried_once_per_aggregate_upstream_resume(GROUP_BASIS).await;
}
