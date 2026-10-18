//! A rebuild of a `live` group-basis aggregate whose target already has
//! groups (#1101's decision, ADR-0002 "An aggregate's build adds each
//! chunk's partials under its group locks"): one absolute write that locks
//! every group the enumeration union names (R8) in key order, runs the
//! source's `GROUP BY` from one snapshot, and replaces each group's partials
//! and visible row, while pages keep applying.
//!
//! Every test turns the group-basis dev knob on before its driver starts,
//! requests the rebuild through `Trellis::request_backfill`, and runs the
//! rebuild's job by hand through `support/drain_driver.rs`, frozen at its
//! pause points where the scenario needs it. None sleeps or polls for
//! convergence (#297): the one wait is for a page to queue on a lock the
//! frozen rebuild holds, which the test has forced.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::{Driver, Running};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::interleave::PausePoint;
use trellis::{Trellis, TrellisOptions};

const SRC: &str = "public.src";
const TARGET: &str = "public.agg";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";
const ACTUAL: &str = "select g, total, n from public.agg order by g";
const EXPECTED: &str = "select g, sum(v), count(*) from public.src group by g order by g";
const STATUS: &str = "select status from transform_definitions where target_table = 'public.agg'";

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// `public.src (id, g, v)`, empty, with [`AGG`] live over it on the group
/// basis, captured by triggers.
async fn start() -> Driver {
    trellis::dev::staging::set_group_basis(true);
    Driver::start(
        "create table public.src (id integer primary key, g integer, v integer)",
        &[("id", int4()), ("g", int4()), ("v", int4())],
        &[AGG],
        &[SRC],
    )
    .await
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it as one sealed page.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

async fn assert_oracle(d: &mut Driver) {
    d.settle().await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "the target (left) differs from the oracle (right)"
    );
}

/// The hidden table of the one definition, qualified.
async fn hidden(d: &Driver) -> String {
    let id = d
        .rows("select id from transform_definitions where target_table = 'public.agg'")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    format!("{DEFAULT_SCHEMA}.agg_{id}")
}

/// The hidden partials of group `g`: `(n, count, sum, has basis)`.
async fn partials(d: &Driver, g: i32) -> Vec<String> {
    d.rows(&format!(
        "select __n, __count_1, __sum_1, __basis is not null from {} where g = {g}",
        hidden(d).await
    ))
    .await
}

/// `Trellis::request_backfill` of the source, the rebuild a user asks for.
async fn request_rebuild(d: &Driver) {
    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill("src")
        .await
        .expect("request a rebuild");
    assert_eq!(d.rows(STATUS).await, ["(backfilling)"], "from the call");
    assert_eq!(
        d.rows("select kind from backfill_chunks where not done")
            .await,
        ["(sweep)"],
        "a group-basis rebuild is one job, with no plan"
    );
}

/// Claims the rebuild's job and runs it in its own task, frozen at each of
/// `points` on the target once reached.
async fn rebuild_frozen(d: &Driver, points: &[PausePoint]) -> Running<()> {
    let points: Vec<(PausePoint, &str)> = points.iter().map(|p| (*p, TARGET)).collect();
    d.run_frozen(&points, |pool| async move {
        let claimed = chunk_queue::claim_chunks_of(
            &**pool.get().await.expect("pool"),
            "rebuild",
            1,
            &[chunk_queue::KIND_SWEEP],
        )
        .await
        .expect("claim the rebuild");
        assert_eq!(claimed.len(), 1, "one rebuild job to claim");
        build::run_claimed(&pool, &claimed[0], "rebuild", &OPTIONS).await;
        Ok(())
    })
    .await
}

/// Runs the rebuild to its commit and checks it left the definition `live`
/// with no job behind.
async fn rebuild(d: &Driver) {
    rebuild_frozen(d, &[]).await.finish().await;
    assert_finished(d).await;
}

async fn assert_finished(d: &Driver) {
    assert_eq!(
        d.rows(
            "select status, (select count(*) from backfill_chunks), \
                    (select string_agg(last_error, '; ') from backfill_chunks) \
             from transform_definitions where target_table = 'public.agg'"
        )
        .await,
        ["(live,0,)"],
        "the rebuild committed and flipped the definition live"
    );
}

/// A rebuild repairs every kind of damage to the target's groups in place:
/// wrong partials (and the visible row that follows them), a visible row
/// that disagrees with its partials, a visible row with no group, and a
/// group missing from both. The target keeps its groups throughout, and the
/// rebuild ends `live`.
#[tokio::test]
async fn a_rebuild_replaces_every_group_in_place() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, 7), (4, 5, 1)",
    )
    .await;
    let hidden = hidden(&d).await;
    d.ctl
        .batch_execute(&format!(
            "update {hidden} set __n = 9, __count_1 = 9, __sum_1 = 99 where g = 1; \
             update public.agg set total = 99, n = 9 where g = 1; \
             update public.agg set total = 1000 where g = 2; \
             insert into public.agg values (42, 4, 2); \
             delete from {hidden} where g = 5; delete from public.agg where g = 5"
        ))
        .await
        .expect("damage the target");

    request_rebuild(&d).await;
    rebuild(&d).await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    assert_eq!(partials(&d, 1).await, ["(2,2,15,t)"]);
    assert_eq!(partials(&d, 2).await, ["(1,1,7,t)"]);
    assert_eq!(partials(&d, 5).await, ["(1,1,1,t)"]);
    assert_eq!(
        partials(&d, 42).await,
        ["(0,0,0,t)"],
        "a visible row with no source row is a group of the target's, gone to zero"
    );
    assert_oracle(&mut d).await;
}

/// Pages keep applying around a rebuild (rule 3). A change committed before
/// the rebuild and drained after it is refused by its group's basis; a page
/// of a locked group, drained while the rebuild holds the lock, waits for
/// the commit and its change, which the rebuild's snapshot saw, is refused
/// too; a change after the rebuild applies as a delta.
#[tokio::test]
async fn changes_before_during_and_after_a_rebuild() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 5)",
    )
    .await;
    write(&d, "insert into public.src values (3, 1, 1)").await;
    let before = d.seal().await;
    request_rebuild(&d).await;

    let mut rebuilding = rebuild_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    let locked = rebuilding.reached(PausePoint::AfterRebuildLock).await;
    write(&d, "update public.src set v = 100 where id = 2").await;
    let during = d.seal().await;
    let page = d.drain_frozen(during, "during", &[]).await;
    d.wait_blocked_behind(locked.backend_pid).await;
    assert!(!page.is_finished(), "the page waits for the rebuild's lock");
    d.release(&mut rebuilding, PausePoint::AfterRebuildLock)
        .await;
    rebuilding.finish().await;
    assert_finished(&d).await;
    page.finish().await;
    assert_eq!(
        partials(&d, 2).await,
        ["(1,1,100,t)"],
        "the waiting page's change was in the rebuild's snapshot, and refused"
    );

    d.drain(before, "before").await;
    assert_eq!(
        partials(&d, 1).await,
        ["(2,2,11,t)"],
        "the change from before the rebuild was in its snapshot, and refused"
    );

    step(&mut d, "insert into public.src values (4, 1, 2)").await;
    assert_eq!(
        partials(&d, 1).await,
        ["(3,3,13,t)"],
        "a later change applies"
    );
    assert_oracle(&mut d).await;
}

/// A page that creates a new group while the rebuild runs: the group wasn't
/// there when the rebuild locked its groups, so the rebuild leaves it to its
/// pages, though its snapshot sees the group's rows. The page that created
/// it applies, and so does a second change to it the snapshot also saw.
#[tokio::test]
async fn a_page_that_creates_a_group_while_a_rebuild_runs() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    request_rebuild(&d).await;

    let mut rebuilding = rebuild_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    rebuilding.reached(PausePoint::AfterRebuildLock).await;
    step(&mut d, "insert into public.src values (2, 9, 4)").await;
    assert_eq!(partials(&d, 9).await, ["(1,1,4,f)"]);
    // Left in the active segment: the rebuild's open transaction holds the
    // seal of a later one, which the ring's four slots then can't spare.
    write(&d, "insert into public.src values (3, 9, 6)").await;
    d.release(&mut rebuilding, PausePoint::AfterRebuildLock)
        .await;
    rebuilding.finish().await;
    assert_finished(&d).await;
    assert_eq!(
        partials(&d, 9).await,
        ["(1,1,4,f)"],
        "the rebuild didn't write a group it hadn't locked"
    );
    let adds = d.seal().await;
    d.drain(adds, "adds").await;
    assert_eq!(partials(&d, 9).await, ["(2,2,10,f)"]);
    assert_eq!(partials(&d, 1).await, ["(1,1,10,t)"]);
    assert_oracle(&mut d).await;
}

/// R8: a group whose source rows are gone goes to zero and its visible row
/// is removed, and a group only an undrained change's old image names (in
/// neither the source nor the target when the rebuild runs) gets a basis
/// too, so the undrained changes the snapshot saw are refused in both.
#[tokio::test]
async fn a_group_that_disappears_and_one_only_an_old_image_names() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 5, 3), (2, 1, 1)").await;
    write(&d, "delete from public.src where id = 1").await;
    write(&d, "insert into public.src values (3, 7, 2)").await;
    write(&d, "update public.src set g = 8 where id = 3").await;
    let undrained = d.seal().await;
    assert!(partials(&d, 7).await.is_empty(), "no group 7 anywhere yet");
    request_rebuild(&d).await;
    rebuild(&d).await;
    assert_eq!(partials(&d, 5).await, ["(0,0,0,t)"]);
    assert_eq!(
        partials(&d, 7).await,
        ["(0,0,0,t)"],
        "the update's old image names group 7"
    );
    assert_eq!(partials(&d, 8).await, ["(1,1,2,t)"]);
    assert_eq!(d.rows(ACTUAL).await, ["(1,1,1)", "(8,2,1)"]);

    d.drain(undrained, "undrained").await;
    assert_eq!(partials(&d, 5).await, ["(0,0,0,t)"]);
    assert_eq!(partials(&d, 7).await, ["(0,0,0,t)"]);
    assert_eq!(partials(&d, 8).await, ["(1,1,2,t)"]);
    assert_oracle(&mut d).await;
}

/// Rule 4: until the rebuild commits, a reader sees the old values, every
/// group of them, and the definition reads `backfilling`; from the commit,
/// the new ones and `live`. The target is never empty or partly rebuilt.
#[tokio::test]
async fn a_reader_sees_old_values_until_the_rebuild_commits() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 5), (3, 3, 1)",
    )
    .await;
    let hidden = hidden(&d).await;
    d.ctl
        .batch_execute(&format!(
            "update {hidden} set __sum_1 = 999 where g = 1; \
             update public.agg set total = 999 where g = 1"
        ))
        .await
        .expect("damage group 1");
    write(&d, "delete from public.src where id = 3").await;
    let old = ["(1,999,1)", "(2,5,1)", "(3,1,1)"];
    assert_eq!(d.rows(ACTUAL).await, old);
    request_rebuild(&d).await;

    let mut rebuilding = rebuild_frozen(
        &d,
        &[
            PausePoint::AfterRebuildLock,
            PausePoint::BeforeRebuildCommit,
        ],
    )
    .await;
    rebuilding.reached(PausePoint::AfterRebuildLock).await;
    assert_eq!(
        d.rows(ACTUAL).await,
        old,
        "a reader while the groups are locked"
    );
    d.release(&mut rebuilding, PausePoint::AfterRebuildLock)
        .await;
    rebuilding.reached(PausePoint::BeforeRebuildCommit).await;
    assert_eq!(d.rows(ACTUAL).await, old, "a reader before the commit");
    assert_eq!(d.rows(STATUS).await, ["(backfilling)"]);
    d.release(&mut rebuilding, PausePoint::BeforeRebuildCommit)
        .await;
    rebuilding.finish().await;
    assert_finished(&d).await;
    assert_eq!(d.rows(ACTUAL).await, ["(1,10,1)", "(2,5,1)"]);
    assert_oracle(&mut d).await;
}

/// A group whose `GROUP BY` part is `NULL` is replaced like any other: the
/// replace matches it to its `GROUP BY` row (`NULL` matching `NULL`), so it
/// keeps its rows rather than going to zero.
#[tokio::test]
async fn a_rebuild_replaces_a_group_with_a_null_part() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, null, 10), (2, null, 5), (3, 1, 7)",
    )
    .await;
    let hidden = hidden(&d).await;
    d.ctl
        .batch_execute(&format!(
            "update {hidden} set __n = 9, __count_1 = 9, __sum_1 = 99 where g is null; \
             update public.agg set total = 99, n = 9 where g is null"
        ))
        .await
        .expect("damage the NULL group");
    write(&d, "insert into public.src values (4, null, 1)").await;
    let undrained = d.seal().await;
    request_rebuild(&d).await;
    rebuild(&d).await;
    assert_eq!(
        d.rows(&format!(
            "select __n, __count_1, __sum_1, __basis is not null from {hidden} where g is null"
        ))
        .await,
        ["(3,3,16,t)"]
    );
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    d.drain(undrained, "undrained").await;
    assert_oracle(&mut d).await;
}

/// `AVG`, a `SUM` over `numeric` and a composed field are computed from the
/// replaced partials, so a rebuild writes the same values the pages do.
#[tokio::test]
async fn a_rebuild_writes_averages_numeric_sums_and_composed_fields() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, v integer, \
         m numeric(12, 2))",
        &[
            ("id", int4()),
            ("g", int4()),
            ("v", int4()),
            ("m", ValueType::Numeric),
        ],
        &["TRANSFORM agg FROM public.src GROUP BY g \
           SELECT AVG(v) AS a, SUM(m) AS sm, SUM(v) + COUNT(m) AS c"],
        &[SRC],
    )
    .await;
    const ACTUAL: &str = "select g, a, sm, c from public.agg order by g";
    const EXPECTED: &str = "select g, avg(v), sum(m), sum(v) + count(m) from public.src \
                            group by g order by g";
    step(
        &mut d,
        "insert into public.src values (1, 1, 1, 1.25), (2, 1, 2, null), (3, 2, 5, 2.50)",
    )
    .await;
    let hidden = hidden(&d).await;
    d.ctl
        .batch_execute(&format!(
            "update {hidden} set __n = 7, __count_1 = 7, __sum_1 = 70 where g = 1; \
             delete from public.agg where g = 2"
        ))
        .await
        .expect("damage the target");
    write(&d, "update public.src set m = 3.75 where id = 2").await;
    let undrained = d.seal().await;
    request_rebuild(&d).await;
    rebuild(&d).await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    d.drain(undrained, "undrained").await;
    d.settle().await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
}
