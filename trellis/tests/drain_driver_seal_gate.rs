//! The drain driver's seal waits out the seal gate's backpressure, and fails
//! naming the holder when it never clears (#1275: a loaded box left a server
//! process's transaction open across the fence, and `seal` panicked on the
//! first refusal).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;

async fn start() -> Driver {
    Driver::start(
        "create table public.src (id integer primary key)",
        &[],
        &[],
        &[],
    )
    .await
}

/// A session that holds an xid open (`pg_current_xact_id` assigns one), as
/// a server process mid-transaction does.
async fn hold_an_xid(d: &Driver) -> (tokio_postgres::Client, i32) {
    let holder = d.user().await;
    holder
        .batch_execute("begin; select pg_current_xact_id()")
        .await
        .expect("hold an xid");
    let pid = holder
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    (holder, pid)
}

/// Seals once with the holder open (its xid is in that batch's fence), then
/// seals again: the gate refuses until the holder commits. The holder commits
/// from another task a moment later, with no signal the driver could use
/// but the gate itself.
#[tokio::test]
async fn seal_waits_for_a_transaction_that_straddles_the_previous_fence() {
    let mut d = start().await;
    let (holder, _) = hold_an_xid(&d).await;
    d.seal().await;
    assert_eq!(
        d.try_seal().await,
        None,
        "the open transaction was in flight at the previous fence"
    );
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        holder.batch_execute("commit").await.expect("commit");
    });
    d.seal().await;
    release.await.expect("release");
}

/// A holder that never commits is not waited out: the seal fails after its
/// bound, and the message names the holder's pid.
#[tokio::test]
async fn seal_fails_naming_a_transaction_that_never_commits() {
    let mut d = start().await;
    let (holder, pid) = hold_an_xid(&d).await;
    d.seal().await;
    let err = d
        .seal_within(Duration::from_millis(500))
        .await
        .expect_err("the gate stays blocked while the holder is open");
    assert!(err.contains(&format!("pid {pid} ")), "{err}");
    assert!(err.contains("predecessor fence xmax "), "{err}");
    holder.batch_execute("commit").await.expect("commit");
    d.seal().await;
}
