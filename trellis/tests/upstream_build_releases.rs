//! An upstream build's completion releases its readers' held keys (#1306).
//!
//! A reader of a Trellis target can hold a key for a failure the upstream
//! caused while it was being rebuilt: between a source rewrite and the
//! upstream's rebuild, the upstream's copy of `bit(5)` holds 5- and 8-bit
//! values together, and a chained `BIT_AND` raises "cannot AND bit strings
//! of different sizes". Once the upstream's build completes, the staging
//! worker releases every key its direct readers hold, with the automatic
//! retry's semantics. Every step is an explicit pass, build or drain;
//! nothing polls for convergence (#297).
//!
//! On the group basis; `upstream_build_releases_ledger.rs` runs the
//! aggregate scenarios with it off, in its own process, since the knob is
//! process-wide.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/upstream_releases.rs"]
mod upstream_releases;

use upstream_releases::*;

const GROUP_BASIS: bool = true;

#[tokio::test]
async fn chained_bit_aggregates_converge_after_the_upstream_rebuild() {
    chained_bit_aggregates_converge_after_upstream_rebuild(GROUP_BASIS).await;
}

/// The release is asked for in the build's own transaction and made by the
/// pass after it: a worker that stops in between (here, a pass on a fresh
/// session after the build completed with no pass of its own) still makes
/// it.
#[tokio::test]
async fn a_release_the_build_asked_for_survives_a_worker_stop() {
    let (mut d, trellis) = chained_bit_aggregates(GROUP_BASIS).await;
    rewrite_then_settle(&mut d, &trellis).await;
    // The pass that sees the rewrite starts `up`'s rebuild; the chunks
    // complete it, and no pass runs after that commit.
    pass(&mut d).await;
    run_backfill_chunks(d.pool()).await;
    settle_tolerant(&mut d).await;
    assert_eq!(status_of(&trellis, "up").await, "Live");
    assert_eq!(status_of(&trellis, "agg").await, "Live (held keys: 1)");
    assert_eq!(
        d.rows(
            "select split_part(d.target_table, '.', 2) from upstream_releases r \
             join transform_definitions d on d.id = r.transform_id"
        )
        .await,
        vec!["(agg)".to_string()],
        "the completing build asked for its reader's release"
    );

    d.ctl = drain_driver::connect(d.db.dsn()).await;
    pass(&mut d).await;
    settle_tolerant(&mut d).await;
    assert_eq!(status_of(&trellis, "agg").await, "Live");
    assert_eq!(diverged(&d).await, Vec::<String>::new());
    trellis.shutdown().await.expect("shutdown");
}

/// A key held for a failure the upstream's build doesn't repair is retried
/// once per build and held again on its first failure: one more attempt,
/// not a fresh run of deaths.
#[tokio::test]
async fn a_persistent_failure_is_retried_once_per_upstream_backfill() {
    let (mut d, trellis) = persistently_held(GROUP_BASIS).await;
    trellis
        .request_backfill("public.t")
        .await
        .expect("request a backfill of up's source");
    rounds(&mut d, 4).await;
    assert_eq!(status_of(&trellis, "up").await, "Live");
    assert_eq!(status_of(&trellis, "r").await, "Live (held keys: 1)");
    assert_eq!(
        r_retry_attempts(&d).await,
        vec!["(1)".to_string()],
        "the build retried r's key once"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// The same for the build a `RESUME` of the upstream runs.
#[tokio::test]
async fn a_persistent_failure_is_retried_once_per_upstream_resume() {
    let (mut d, trellis) = persistently_held(GROUP_BASIS).await;
    trellis.apply("PAUSE TRANSFORM up").await.expect("pause up");
    trellis
        .apply("RESUME TRANSFORM up")
        .await
        .expect("resume up");
    rounds(&mut d, 4).await;
    assert_eq!(status_of(&trellis, "up").await, "Live");
    assert_eq!(status_of(&trellis, "r").await, "Live (held keys: 1)");
    assert_eq!(
        r_retry_attempts(&d).await,
        vec!["(1)".to_string()],
        "the resume's build retried r's key once"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// A paused reader keeps its keys held through its upstream's build: its
/// `RESUME` releases them.
#[tokio::test]
async fn a_paused_reader_keeps_its_keys_through_an_upstream_build() {
    let (mut d, trellis) = persistently_held(GROUP_BASIS).await;
    trellis.apply("PAUSE TRANSFORM r").await.expect("pause r");
    trellis
        .request_backfill("public.t")
        .await
        .expect("request a backfill of up's source");
    rounds(&mut d, 4).await;
    assert_eq!(status_of(&trellis, "up").await, "Live");
    assert_eq!(
        r_retry_attempts(&d).await,
        vec!["(0)".to_string()],
        "a paused reader's key was released"
    );
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_persistent_failure_is_retried_once_per_aggregate_upstream_resume() {
    persistent_failure_retried_once_per_aggregate_upstream_resume(GROUP_BASIS).await;
}
