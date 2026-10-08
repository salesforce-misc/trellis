//! Releasing a key a definition holds in quarantine (#759):
//! `Trellis::release_key`, the held-key metadata `status` and `self_check`
//! report, and the release's interleavings with a drain page and a resume,
//! on the real engine, driven by hand through `support/drain_driver.rs`. No
//! test sleeps or polls for convergence (#297): the one wait,
//! `Driver::wait_blocked`, waits for a backend to queue on a lock the test
//! holds.
//!
//! Every test holds key 1 of `public.nums` for `doubles`: a check
//! constraint on its target refuses the doubled value, so each drain of the
//! change fails in `doubles`' apply until the key's deaths reach the
//! threshold and it is poisoned for `doubles`. `copies` reads the same table
//! and applies the key throughout.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::interleave::PausePoint;
use trellis::staging::{ApplyError, DEFAULT_DEATH_THRESHOLD, StagedWatermark, apply};
use trellis::{
    Config, ErrorCode, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, Trellis, TrellisError,
    TrellisOptions,
};

const NUMS: &str = "public.nums";

/// A driver with `public.nums` captured and read by `doubles` and `copies`,
/// and `doubles`' target refusing a doubled value of 100 or more.
async fn start() -> Driver {
    let d = Driver::start(
        "create table public.nums (id integer primary key, x integer); \
         insert into public.nums select g, g from generate_series(1, 3) g;",
        &[("id", ValueType::Numeric), ("x", ValueType::Numeric)],
        &[
            "TRANSFORM doubles FROM public.nums SELECT x + x AS doubled",
            "TRANSFORM copies FROM public.nums SELECT x AS x",
        ],
        &[NUMS],
    )
    .await;
    d.ctl
        .batch_execute("alter table public.doubles add constraint small check (doubled < 100)")
        .await
        .expect("constrain the target");
    d
}

/// A facade handle on the driver's database, running no background work.
async fn trellis(d: &Driver) -> Trellis {
    let config = Config::with_schema(d.db.dsn().to_string(), DEFAULT_SCHEMA).expect("config");
    Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect")
}

/// Drains `batch` until a page commits, returning how many attempts failed
/// first. Each failure charges the key a death, so the page commits once
/// the key is poisoned. Bounded, not a wait for convergence.
async fn drain_through_evictions(d: &Driver, batch: i64) -> usize {
    let mut failures = 0;
    loop {
        match apply::drain_once(
            d.pool(),
            batch,
            "worker",
            1,
            drain_driver::WAKE,
            &StagedWatermark::saturated(),
        )
        .await
        {
            Ok(Some(_)) => continue,
            Ok(None) => return failures,
            Err(err) => {
                failures += 1;
                assert!(failures <= 20, "the batch never drained: {err}");
            }
        }
    }
}

/// Writes `x` to key 1 of `nums` through capture.
async fn write_key_1(d: &Driver, x: i32) {
    d.user()
        .await
        .execute("update public.nums set x = $1 where id = 1", &[&x])
        .await
        .expect("write nums 1");
}

/// Writes key 1 a value `doubles` refuses and drains it until the key is
/// held for `doubles`.
async fn hold_key_1(d: &mut Driver) {
    write_key_1(d, 60).await;
    let batch = d.seal().await;
    assert_eq!(
        drain_through_evictions(d, batch).await,
        DEFAULT_DEATH_THRESHOLD as usize - 1
    );
    assert_eq!(
        held(d).await,
        vec![("doubles".to_string(), "1".to_string())]
    );
    assert_eq!(rows_for(d, "poison_held", "doubles").await, 1);
}

/// Every `(transform, key)` held in `poison`.
async fn held(d: &Driver) -> Vec<(String, String)> {
    d.ctl
        .query(
            "select split_part(d.target_table, '.', 2), p.key from poison p \
             join transform_definitions d on d.id = p.transform_id order by 1, 2",
            &[],
        )
        .await
        .expect("read poison")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// How many rows of quarantine table `table` belong to `target`.
async fn rows_for(d: &Driver, table: &str, target: &str) -> i64 {
    d.ctl
        .query_one(
            &format!(
                "select count(*) from {table} t join transform_definitions d \
                 on d.id = t.transform_id where split_part(d.target_table, '.', 2) = $1"
            ),
            &[&target],
        )
        .await
        .expect("count quarantine rows")
        .get(0)
}

/// `nums`' version fence.
async fn fence(d: &Driver) -> i64 {
    d.ctl
        .query_one(
            "select version from source_table_versions where source_table = $1",
            &[&NUMS],
        )
        .await
        .expect("read the fence")
        .get(0)
}

/// Asserts `doubles` holds what a recompute from `nums` gives.
async fn assert_doubles_match_the_source(d: &Driver) {
    assert_eq!(
        d.rows("select id, doubled::numeric from public.doubles order by id")
            .await,
        d.rows("select id, (x * 2)::numeric from public.nums order by id")
            .await,
    );
}

/// Seals and drains everything staged since, failing on any error.
async fn drain_all(d: &mut Driver) {
    let batch = d.seal().await;
    d.drain(batch, "worker").await;
}

#[tokio::test]
async fn releasing_a_fixed_key_rederives_it_and_clears_its_quarantine() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    assert_eq!(
        d.rows("select x from public.copies where id = 1").await,
        vec!["(60)"],
        "copies applied the key doubles holds"
    );

    d.ctl
        .batch_execute("alter table public.doubles drop constraint small")
        .await
        .expect("fix the cause");
    let trellis = trellis(&d).await;
    // The bare spelling of the table, which the release resolves (#283).
    trellis
        .release_key("doubles", "nums", "1")
        .await
        .expect("release key 1");
    for table in ["poison", "poison_held", "key_deaths"] {
        assert_eq!(rows_for(&d, table, "doubles").await, 0, "{table}");
    }

    drain_all(&mut d).await;
    assert_doubles_match_the_source(&d).await;
    assert_eq!(held(&d).await, vec![]);
}

#[tokio::test]
async fn a_released_key_whose_cause_persists_is_held_again() {
    let mut d = start().await;
    hold_key_1(&mut d).await;

    let trellis = trellis(&d).await;
    trellis
        .release_key("doubles", NUMS, "1")
        .await
        .expect("release key 1");
    assert_eq!(held(&d).await, vec![]);

    // The release's recompute fails as the change before it did, and is
    // charged from zero again.
    let batch = d.seal().await;
    assert_eq!(
        drain_through_evictions(&d, batch).await,
        DEFAULT_DEATH_THRESHOLD as usize - 1
    );
    assert_eq!(
        held(&d).await,
        vec![("doubles".to_string(), "1".to_string())]
    );
    assert_eq!(
        d.rows(
            "select count(*) from poison_held h join transform_definitions d \
             on d.id = h.transform_id where d.target_table = 'public.doubles'"
        )
        .await,
        vec!["(1)"],
        "the recompute is parked for the next release, in the key's one held row"
    );
    assert_eq!(
        d.rows("select doubled from public.doubles where id = 1")
            .await,
        vec!["(2)"],
        "the target row stays as it was before the key was poisoned"
    );
}

#[tokio::test]
async fn status_reports_a_definitions_held_keys_until_they_are_released() {
    let mut d = start().await;
    let trellis = trellis(&d).await;
    let status = trellis.status("doubles").await.expect("status").unwrap();
    assert_eq!(status.held_keys, None, "nothing is held yet");

    hold_key_1(&mut d).await;
    let poisoned_at: std::time::SystemTime = d
        .ctl
        .query_one("select poisoned_at from poison", &[])
        .await
        .expect("read poisoned_at")
        .get(0);
    let status = trellis.status("doubles").await.expect("status").unwrap();
    let held_keys = status.held_keys.expect("doubles holds a key");
    assert_eq!(held_keys.count, 1);
    assert_eq!(held_keys.oldest_poisoned_at, poisoned_at);
    assert_eq!(
        status.status,
        trellis::TransformStatus::Live,
        "a definition holding a key below the fuse is still live"
    );
    let copies = trellis.status("copies").await.expect("status").unwrap();
    assert_eq!(copies.held_keys, None, "the sibling holds nothing (#799)");

    trellis
        .release_key("doubles", NUMS, "1")
        .await
        .expect("release key 1");
    let status = trellis.status("doubles").await.expect("status").unwrap();
    assert_eq!(status.held_keys, None, "the release clears it");
}

#[tokio::test]
async fn self_check_reports_a_held_key() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let trellis = trellis(&d).await;
    let scope = || SelfCheckScope {
        after: None,
        limit: 100,
    };

    // The key's parked change holds back convergence, so the audit can't
    // compare; the held key says why.
    let report = trellis
        .self_check(
            "doubles",
            scope(),
            SelfCheckMode::Strict,
            Duration::from_millis(200),
        )
        .await
        .expect("self_check doubles");
    assert!(
        matches!(report.outcome, SelfCheckOutcome::NotCaughtUp),
        "{:?}",
        report.outcome
    );
    assert_eq!(report.held_keys.expect("a held key").count, 1);

    let report = trellis
        .self_check(
            "copies",
            scope(),
            SelfCheckMode::Strict,
            Duration::from_secs(30),
        )
        .await
        .expect("self_check copies");
    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "{:?}",
        report.outcome
    );
    assert_eq!(report.held_keys, None);
}

#[tokio::test]
async fn a_release_naming_no_held_key_is_refused_and_changes_nothing() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let trellis = trellis(&d).await;
    let before = fence(&d).await;

    let err = trellis
        .release_key("nope", NUMS, "1")
        .await
        .expect_err("an unknown transform");
    assert!(
        matches!(&err, TrellisError::TransformNotFound(name) if name == "nope"),
        "{err:?}"
    );
    assert_eq!(err.code(), ErrorCode::NotFound);

    for (transform, table, key) in [
        // A key the transform doesn't hold.
        ("doubles", NUMS, "2"),
        // A key only a sibling holds: whole-key poison is per transform.
        ("copies", NUMS, "1"),
        // A table the transform doesn't read, or that doesn't exist.
        ("doubles", "public.nowhere", "1"),
    ] {
        let err = trellis
            .release_key(transform, table, key)
            .await
            .expect_err("a key that isn't held");
        assert!(
            matches!(&err, TrellisError::Apply(ApplyError::KeyNotHeld { .. })),
            "{transform} {table} {key}: {err:?}"
        );
        assert_eq!(err.code(), ErrorCode::NotFound);
    }

    assert_eq!(
        held(&d).await,
        vec![("doubles".to_string(), "1".to_string())]
    );
    assert_eq!(rows_for(&d, "poison_held", "doubles").await, 1);
    assert_eq!(fence(&d).await, before, "each refusal rolled its bump back");
}

/// A release whose first lock, the bump of the key's table's version fence,
/// waits out the lock timeout behind a page holding the fence (#842). It's
/// [`ApplyError::ReleaseLockTimeout`], a retryable [`ErrorCode::Timeout`]
/// rather than a raw `55P03`, and it changes nothing: the fence and the held
/// key are as they were, and the same release succeeds once the page is
/// gone. The release's session runs with a 100 ms `lock_timeout` (a shorter
/// setting than Trellis's 30 s cap is kept), so the test waits on nothing
/// but the release's own timeout.
#[tokio::test]
async fn a_release_that_waits_out_the_lock_timeout_is_a_retryable_timeout_and_changes_nothing() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let config = Config::with_schema(
        format!("{} options='-c lock_timeout=100'", d.db.dsn()),
        DEFAULT_SCHEMA,
    )
    .expect("config");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");
    let before = fence(&d).await;

    // A page's hold on the fence: `for share` from its first lock to its
    // commit.
    let mut page = d.user().await;
    let txn = page.transaction().await.expect("begin");
    txn.execute(
        "select version from source_table_versions where source_table = $1 for share",
        &[&NUMS],
    )
    .await
    .expect("hold nums' fence");

    let err = trellis
        .release_key("doubles", NUMS, "1")
        .await
        .expect_err("the fence is held past the lock timeout");
    assert!(
        matches!(
            &err,
            TrellisError::Apply(ApplyError::ReleaseLockTimeout { transform, src_table, key })
                if transform == "doubles" && src_table == NUMS && key == "1"
        ),
        "{err:?}"
    );
    assert_eq!(err.code(), ErrorCode::Timeout);
    let message = err.to_string();
    assert!(message.contains("retry"), "{message}");
    assert!(message.contains("in flight"), "{message}");
    txn.rollback().await.expect("end the page");

    assert_eq!(fence(&d).await, before, "the refusal rolled its bump back");
    assert_eq!(
        held(&d).await,
        vec![("doubles".to_string(), "1".to_string())]
    );
    assert_eq!(rows_for(&d, "poison_held", "doubles").await, 1);

    d.ctl
        .batch_execute("alter table public.doubles drop constraint small")
        .await
        .expect("fix the cause");
    trellis
        .release_key("doubles", NUMS, "1")
        .await
        .expect("the retry releases the key");
    assert!(held(&d).await.is_empty());
    drain_all(&mut d).await;
    assert_doubles_match_the_source(&d).await;
}

/// A page that parks a change for the held key and a release of the key
/// (#759, ADR-0002 I1). The page holds `nums`' fence `for share` from its
/// first lock to its commit, and parks the change before it commits. The
/// release's first lock is a bump of that fence, so it waits for the page,
/// then reads the key's rows, the page's parked change among them, and
/// releases them all. Without the wait, the release would read and delete
/// the key's rows before the page committed its parked row, leaving a held
/// row no `poison` row names, which blocks every watermark token from then
/// on.
#[tokio::test]
async fn a_release_waits_for_a_page_parking_a_change_for_the_key_and_releases_it_too() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    d.ctl
        .batch_execute("alter table public.doubles drop constraint small")
        .await
        .expect("fix the cause");

    write_key_1(&d, 70).await;
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(
            batch,
            "pager",
            &[(PausePoint::BeforeCommit, "public.copies")],
        )
        .await;
    let reached = page.reached(PausePoint::BeforeCommit).await;

    // The eviction's parked change, before the page merges its own into the
    // key's held row (#803).
    let evicted = d
        .rows("select origin_lsn::text, old_image::text from poison_held")
        .await;
    assert_eq!(evicted.len(), 1, "{evicted:?}");

    let pool = d.pool().clone();
    let release =
        tokio::spawn(
            async move { trellis::staging::release_key(&pool, "doubles", NUMS, "1").await },
        );
    d.wait_blocked_behind(reached.backend_pid).await;
    d.release(&mut page, PausePoint::BeforeCommit).await;
    page.finish().await;
    let deleted = release.await.expect("release task").expect("release key 1");
    assert_eq!(
        deleted, 1,
        "the page merged its parked change into the key's one held row"
    );
    assert_eq!(
        d.rows(
            "select origin_lsn::text, old_image::text from ( \
                 select * from seg_0 union all select * from seg_1 \
                 union all select * from seg_2 union all select * from seg_3) ring \
             where key = '1' and op = 'recompute'"
        )
        .await,
        evicted,
        "the release's recompute carries the earliest parked change's origin and pre-image"
    );
    for table in ["poison", "poison_held", "key_deaths"] {
        assert_eq!(rows_for(&d, table, "doubles").await, 0, "{table}");
    }

    drain_all(&mut d).await;
    assert_doubles_match_the_source(&d).await;
    let pending: i64 = d
        .ctl
        .query_one("select count(*) from poison_held", &[])
        .await
        .expect("count held rows")
        .get(0);
    assert_eq!(pending, 0, "no held row is left for convergence to wait on");
}

/// A resume and a release of the same definition (ADR-0002 I1). A resume
/// holds the definition's row `for update` while it deletes the
/// definition's held keys. The release takes the row before it reads the
/// key's rows, so one queued behind the resume finds the key already
/// released and refuses, rather than staging a recompute from rows the
/// resume deleted. The test takes the resume's lock and deletes by hand, so
/// the release is queued behind it.
#[tokio::test]
async fn a_release_queued_behind_a_resume_finds_the_key_released() {
    let mut d = start().await;
    hold_key_1(&mut d).await;

    let mut resumer = d.user().await;
    let txn = resumer.transaction().await.expect("begin");
    let pid: i32 = txn
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    txn.execute(
        "select 1 from transform_definitions where target_table = 'public.doubles' for update",
        &[],
    )
    .await
    .expect("lock doubles' row");

    let pool = d.pool().clone();
    let release =
        tokio::spawn(
            async move { trellis::staging::release_key(&pool, "doubles", NUMS, "1").await },
        );
    d.wait_blocked_behind(pid).await;
    for table in ["poison", "poison_held", "key_deaths"] {
        txn.execute(
            &format!(
                "delete from {table} where transform_id = \
                 (select id from transform_definitions where target_table = 'public.doubles')"
            ),
            &[],
        )
        .await
        .expect("delete doubles' held keys");
    }
    txn.commit().await.expect("commit the resume");

    let refused = release.await.expect("release task");
    assert!(
        matches!(refused, Err(ApplyError::KeyNotHeld { .. })),
        "{refused:?}"
    );
    let staged: i64 = d
        .ctl
        .query_one(
            &format!(
                "select count(*) from seg_{} where key = '1' and op = 'recompute'",
                d.ctl
                    .query_one("select ring_slot from segment_pointer", &[])
                    .await
                    .expect("active slot")
                    .get::<_, i16>(0)
            ),
            &[],
        )
        .await
        .expect("count staged recomputes")
        .get(0);
    assert_eq!(staged, 0, "the refused release staged nothing");
}
