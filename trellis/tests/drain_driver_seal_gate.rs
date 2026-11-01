//! The drain driver's seal waits out the seal gate's and a full ring's
//! backpressure, and fails naming the holder when it never clears. #1275: a
//! loaded box left a server process's transaction open across the fence, and
//! `seal` panicked on the first refusal. #1554: a reader of the slot the seal
//! needed made the retirement pass skip it, and `seal` panicked on the
//! `RingFull` that followed.

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

/// Fills the ring as `join_one_to_one_over_targets`' S9 does: two batches
/// sealed, the older drained, so the next seal needs the slot of the
/// drained segment three behind the active one, which only a retirement
/// pass frees. Then opens a session reading that slot (`ACCESS SHARE` held,
/// as a convergence wait's poll holds it for its statement) and returns it
/// with its pid: retirement's `NOWAIT` lock skips the slot while it is open.
async fn read_the_slot_the_next_seal_needs(d: &mut Driver) -> (tokio_postgres::Client, i32) {
    let first = d.seal().await;
    d.seal().await;
    d.drain(first, "w").await;
    let slot: i16 = d
        .ctl
        .query_one(
            "select ring_slot from segments \
             where seg_seq = (select active_seq from segment_pointer) - 3 and state = 'drained'",
            &[],
        )
        .await
        .expect("the next seal's slot holds a drained segment")
        .get(0);
    let reader = d.user().await;
    reader
        .batch_execute(&format!(
            "begin; lock table seg_{slot} in access share mode"
        ))
        .await
        .expect("read the slot");
    let pid = reader
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    (reader, pid)
}

/// The retirement pass skips a drained slot a reader holds, and the seal
/// needs that slot: a full ring, which the seal waits out as the engine's
/// sealer does, until the reader ends.
#[tokio::test]
async fn seal_waits_for_a_reader_of_the_slot_it_needs() {
    let mut d = start().await;
    let (reader, _) = read_the_slot_the_next_seal_needs(&mut d).await;
    assert_eq!(d.try_seal().await, None, "the reader holds the slot");
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        reader.batch_execute("commit").await.expect("commit");
    });
    d.seal().await;
    release.await.expect("release");
}

/// A reader that never ends is not waited out: the seal fails after its
/// bound, and the message names the slot and the reader's pid.
#[tokio::test]
async fn seal_fails_naming_a_reader_that_never_ends() {
    let mut d = start().await;
    let (reader, pid) = read_the_slot_the_next_seal_needs(&mut d).await;
    let err = d
        .seal_within(Duration::from_millis(500))
        .await
        .expect_err("the ring stays full while the reader is open");
    assert!(err.contains("the ring stayed full"), "{err}");
    assert!(err.contains(&format!("({pid},seg_")), "{err}");
    reader.batch_execute("commit").await.expect("commit");
    d.seal().await;
}

/// A ring full of work not yet drained is not waited out: nothing but the
/// test can drain it, so the seal panics at once, without its wait.
#[tokio::test]
#[should_panic(expected = "work not yet drained holds it")]
async fn seal_panics_at_once_on_a_ring_full_of_undrained_work() {
    let mut d = start().await;
    for _ in 0..4 {
        d.seal().await;
    }
}
