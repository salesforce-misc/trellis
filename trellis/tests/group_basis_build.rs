//! A fresh build of a group-basis aggregate over a source that already has
//! rows (#1101's decision, ADR-0002 "An aggregate's build adds each chunk's
//! partials under its group locks"): the plan job, after its seam fence,
//! runs one absolute write of every group the enumeration union names (R8)
//! in key order, from one snapshot taken after the locks, while pages apply
//! from the build's first moment. An unpartitioned source.
//!
//! The source is seeded and captured by an unrelated 1-1 definition, then
//! the aggregate is defined over it, so its build starts over rows. Every
//! test turns the group-basis dev knob on before its driver starts, and runs
//! the plan job by hand through `support/drain_driver.rs`, frozen at its
//! pause points where the scenario needs it. None sleeps or polls for
//! convergence (#297): the one wait is for a page to queue on a lock the
//! frozen build holds, which the test has forced.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::{Driver, Running};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::interleave::PausePoint;

const SRC: &str = "public.src";
const TARGET: &str = "public.agg";
const ONE: &str = "TRANSFORM one FROM public.src SELECT g AS g, v AS w";
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

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![("id", int4()), ("g", int4()), ("v", int4())]
}

/// `public.src (id, g, v)` captured, with an unrelated 1-1 definition live
/// over it, and `rows` (an `insert ... values` tail) drained into it.
async fn seeded(rows: &str) -> Driver {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, v integer); \
         create index on public.src (g)",
        &columns(),
        &[ONE],
        &[SRC],
    )
    .await;
    step(&mut d, &format!("insert into public.src values {rows}")).await;
    d
}

/// Defines [`AGG`] over the source and starts its build: it applies from here
/// and has one plan job and nothing else.
async fn define(d: &Driver) {
    define_as(d, AGG).await;
    assert_eq!(d.rows(STATUS).await, ["(backfilling)"], "the build started");
    assert_eq!(
        d.rows("select kind from backfill_chunks where not done")
            .await,
        ["(plan)"],
        "a group-basis build is one plan job"
    );
}

async fn define_as(d: &Driver, definition: &str) {
    let columns = [("id", int4()), ("g", int4()), ("v", int4())]
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    trellis::defs::install_definition(d.pool(), definition, &columns, "public")
        .await
        .expect("define the aggregate");
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("start the build");
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

/// The hidden table of the aggregate, qualified.
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

/// Claims the build's plan job and runs it in its own task, frozen at each
/// of `points` on the target once reached.
async fn build_frozen(d: &Driver, points: &[PausePoint]) -> Running<()> {
    let points: Vec<(PausePoint, &str)> = points.iter().map(|p| (*p, TARGET)).collect();
    d.run_frozen(&points, |pool| async move {
        let claimed = chunk_queue::claim_chunks_of(
            &**pool.get().await.expect("pool"),
            "build",
            1,
            &[chunk_queue::KIND_PLAN],
        )
        .await
        .expect("claim the plan job");
        assert_eq!(claimed.len(), 1, "one plan job to claim");
        build::run_claimed(&pool, &claimed[0], "build", &OPTIONS).await;
        Ok(())
    })
    .await
}

/// Runs the build to its commit and checks it left the definition `live`
/// with no job behind.
async fn run_build(d: &Driver) {
    build_frozen(d, &[]).await.finish().await;
    assert_finished(d).await;
}

async fn assert_finished(d: &Driver) {
    assert_eq!(
        d.rows(
            "select status, (select count(*) from backfill_chunks \
                             where definition_id = d.id), \
                    (select string_agg(last_error, '; ') from backfill_chunks) \
             from transform_definitions d where target_table = 'public.agg'"
        )
        .await,
        ["(live,0,)"],
        "the build committed and flipped the definition live"
    );
}

/// The build over a seeded source writes every group with a basis and a
/// stamp, leaves no job behind, and agrees with a from-scratch `GROUP BY`,
/// the `NULL` group included.
#[tokio::test]
async fn a_build_over_a_seeded_source_writes_every_group() {
    let mut d = seeded("(1, 1, 10), (2, 1, 5), (3, 2, 7), (4, null, 3), (5, null, 4)").await;
    define(&d).await;
    assert_eq!(d.rows(ACTUAL).await, Vec::<String>::new(), "nothing yet");
    run_build(&d).await;
    assert_eq!(
        d.rows(ACTUAL).await,
        ["(1,15,2)", "(2,7,1)", "(,7,2)"],
        "the groups, the NULL one last"
    );
    assert_eq!(partials(&d, 1).await, ["(2,2,15,t)"]);
    assert_eq!(
        d.rows(&format!(
            "select __n, __count_1, __sum_1, __basis is not null, __stamp is not null \
             from {} where g is null",
            hidden(&d).await
        ))
        .await,
        ["(2,2,7,t,t)"],
        "the NULL-key group is a group like any other"
    );
    assert_oracle(&mut d).await;
}

/// Changes from before the build, during it and after it. One committed
/// before the definition was defined and drained after the build is refused
/// by its group's basis (the build's snapshot saw it); a page of a locked
/// group, drained while the build holds the lock, waits for the commit and
/// is refused too; a change after the build applies as a delta.
#[tokio::test]
async fn changes_before_during_and_after_a_build() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)").await;
    write(&d, "insert into public.src values (3, 1, 1)").await;
    let before = d.seal().await;
    define(&d).await;

    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    let locked = building.reached(PausePoint::AfterRebuildLock).await;
    write(&d, "update public.src set v = 100 where id = 2").await;
    let during = d.seal().await;
    let page = d.drain_frozen(during, "during", &[]).await;
    d.wait_blocked_behind(locked.backend_pid).await;
    assert!(!page.is_finished(), "the page waits for the build's lock");
    d.release(&mut building, PausePoint::AfterRebuildLock).await;
    building.finish().await;
    assert_finished(&d).await;
    page.finish().await;
    assert_eq!(
        partials(&d, 2).await,
        ["(1,1,100,t)"],
        "the waiting page's change was in the build's snapshot, and refused"
    );

    d.drain(before, "before").await;
    assert_eq!(
        partials(&d, 1).await,
        ["(2,2,11,t)"],
        "the change from before the build was in its snapshot, and refused"
    );

    step(&mut d, "insert into public.src values (4, 1, 2)").await;
    assert_eq!(
        partials(&d, 1).await,
        ["(3,3,13,t)"],
        "a later change applies"
    );
    assert_oracle(&mut d).await;
}

/// The window between the enumeration and the lock statement: nothing is
/// locked, so pages apply, and each change must count once. Frozen there:
///
/// - a change to an enumerated group whose page drains (it creates the
///   group's hidden row, on a `NULL` basis, which the build's absolute write
///   then overwrites from a snapshot that saw the change);
/// - a change whose page waits until after the build (refused: its basis is
///   the build's snapshot);
/// - a page that creates a group the enumeration didn't name (left to its
///   pages: the build doesn't write a group it hasn't locked).
///
/// A build whose snapshot came before its locks fails this: the first loses
/// the change, the second applies it twice.
#[tokio::test]
async fn pages_between_the_enumeration_and_the_lock() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5), (3, 3, 1)").await;
    define(&d).await;

    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildEnumerate]).await;
    building.reached(PausePoint::AfterRebuildEnumerate).await;
    step(&mut d, "insert into public.src values (4, 1, 1), (5, 9, 4)").await;
    assert_eq!(
        partials(&d, 1).await,
        ["(1,1,1,f)"],
        "the page made the group's hidden row, on no basis"
    );
    assert_eq!(partials(&d, 9).await, ["(1,1,4,f)"]);
    // Left in the active segment: the build's open transaction holds the
    // seal of a later one, which the ring's four slots then can't spare.
    write(&d, "update public.src set v = v + 1 where id = 2").await;
    d.release(&mut building, PausePoint::AfterRebuildEnumerate)
        .await;
    building.finish().await;
    assert_finished(&d).await;
    assert_eq!(
        partials(&d, 1).await,
        ["(2,2,11,t)"],
        "the build's absolute write replaced the page's delta"
    );
    assert_eq!(
        partials(&d, 9).await,
        ["(1,1,4,f)"],
        "the build didn't write a group it hadn't enumerated"
    );

    let undrained = d.seal().await;
    d.drain(undrained, "undrained").await;
    assert_eq!(
        partials(&d, 2).await,
        ["(1,1,6,t)"],
        "a change the build's snapshot saw is refused"
    );
    assert_oracle(&mut d).await;
}

/// A page that creates a group after the locks, while the build holds the
/// rest: the build's snapshot sees the group's row, but the build never
/// locked the group and leaves it to its pages.
#[tokio::test]
async fn a_page_that_creates_a_group_after_the_locks() {
    let mut d = seeded("(1, 1, 10)").await;
    define(&d).await;

    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    building.reached(PausePoint::AfterRebuildLock).await;
    step(&mut d, "insert into public.src values (2, 9, 4)").await;
    assert_eq!(partials(&d, 9).await, ["(1,1,4,f)"]);
    // Left in the active segment: the build's open transaction holds the
    // seal of a later one, which the ring's four slots then can't spare.
    write(&d, "insert into public.src values (3, 9, 6)").await;
    d.release(&mut building, PausePoint::AfterRebuildLock).await;
    building.finish().await;
    assert_finished(&d).await;
    assert_eq!(partials(&d, 9).await, ["(1,1,4,f)"]);
    let adds = d.seal().await;
    d.drain(adds, "adds").await;
    assert_eq!(partials(&d, 9).await, ["(2,2,10,f)"]);
    assert_eq!(partials(&d, 1).await, ["(1,1,10,t)"]);
    assert_oracle(&mut d).await;
}

/// `NULL`-key groups through every step: seeded, changed by a page the
/// build holds back, changed again after it.
#[tokio::test]
async fn a_null_key_group_through_the_build() {
    let mut d = seeded("(1, null, 10), (2, null, 5), (3, 1, 7)").await;
    write(&d, "insert into public.src values (4, null, 1)").await;
    let before = d.seal().await;
    define(&d).await;

    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    let locked = building.reached(PausePoint::AfterRebuildLock).await;
    write(&d, "update public.src set v = 20 where id = 1").await;
    let during = d.seal().await;
    let page = d.drain_frozen(during, "during", &[]).await;
    d.wait_blocked_behind(locked.backend_pid).await;
    d.release(&mut building, PausePoint::AfterRebuildLock).await;
    building.finish().await;
    page.finish().await;
    assert_finished(&d).await;
    d.drain(before, "before").await;
    assert_eq!(
        d.rows(&format!(
            "select __n, __count_1, __sum_1, __basis is not null from {} where g is null",
            hidden(&d).await
        ))
        .await,
        ["(3,3,26,t)"]
    );
    step(&mut d, "insert into public.src values (5, null, 2)").await;
    assert_oracle(&mut d).await;
}

/// A group whose source rows moved out before the build, with the move
/// undrained, is named only by the old image in the ring (R8): it ends as a
/// zero group with a basis, and the page's later change is refused.
#[tokio::test]
async fn a_group_only_an_old_image_names() {
    let mut d = seeded("(1, 5, 3), (2, 1, 1)").await;
    write(&d, "update public.src set g = 6 where id = 1").await;
    let undrained = d.seal().await;
    define(&d).await;
    run_build(&d).await;
    assert_eq!(partials(&d, 5).await, ["(0,0,0,t)"]);
    assert_eq!(partials(&d, 6).await, ["(1,1,3,t)"]);
    d.drain(undrained, "undrained").await;
    assert_eq!(partials(&d, 5).await, ["(0,0,0,t)"]);
    assert_eq!(partials(&d, 6).await, ["(1,1,3,t)"]);
    assert_oracle(&mut d).await;
}

/// A reader sees nothing of the build, then all of it: the target is empty
/// and the definition `backfilling` until the commit.
#[tokio::test]
async fn a_reader_sees_the_build_only_at_its_commit() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)").await;
    define(&d).await;
    let mut building = build_frozen(
        &d,
        &[
            PausePoint::AfterRebuildLock,
            PausePoint::BeforeRebuildCommit,
        ],
    )
    .await;
    building.reached(PausePoint::AfterRebuildLock).await;
    assert!(d.rows(ACTUAL).await.is_empty(), "a reader while locked");
    d.release(&mut building, PausePoint::AfterRebuildLock).await;
    building.reached(PausePoint::BeforeRebuildCommit).await;
    assert!(
        d.rows(ACTUAL).await.is_empty(),
        "a reader before the commit"
    );
    assert_eq!(d.rows(STATUS).await, ["(backfilling)"]);
    d.release(&mut building, PausePoint::BeforeRebuildCommit)
        .await;
    building.finish().await;
    assert_finished(&d).await;
    assert_eq!(d.rows(ACTUAL).await, ["(1,10,1)", "(2,5,1)"]);
    assert_oracle(&mut d).await;
}

/// A `TRUNCATE` of the source while the build runs queues behind the build,
/// which holds the source from its first read to its commit (a truncate can't
/// commit under the build's `GROUP BY`). Once the build commits, the
/// truncate's page wipes the build's groups, and the insert after the
/// truncate applies again on empty groups.
#[tokio::test]
async fn a_truncate_during_the_build() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)").await;
    define(&d).await;
    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    let locked = building.reached(PausePoint::AfterRebuildLock).await;
    let user = d.user().await;
    let truncate = tokio::spawn(async move {
        user.batch_execute("truncate public.src; insert into public.src values (3, 1, 2)")
            .await
            .expect("truncate and insert");
    });
    d.wait_blocked_behind(locked.backend_pid).await;
    assert!(!truncate.is_finished(), "the truncate waits for the build");
    d.release(&mut building, PausePoint::AfterRebuildLock).await;
    building.finish().await;
    truncate.await.expect("the truncate's task");
    assert_finished(&d).await;
    assert_eq!(
        d.rows(ACTUAL).await,
        ["(1,10,1)", "(2,5,1)"],
        "the build saw the rows from before the truncate"
    );
    let page = d.seal().await;
    d.drain(page, "truncate").await;
    assert_oracle(&mut d).await;
    assert_eq!(d.rows(ACTUAL).await, ["(1,2,1)"]);
}

/// A `TRUNCATE` and an insert committed before the build, drained after it:
/// the build counted the insert, the truncate's page wipes the groups
/// (bases included) and raises the floor, and the insert's page, ordered
/// after it, applies again.
#[tokio::test]
async fn a_truncate_before_the_build_drains_after_it() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)").await;
    write(
        &d,
        "truncate public.src; insert into public.src values (3, 1, 2), (4, 3, 8)",
    )
    .await;
    let page = d.seal().await;
    define(&d).await;
    run_build(&d).await;
    assert_eq!(d.rows(ACTUAL).await, ["(1,2,1)", "(3,8,1)"]);
    d.drain(page, "truncate").await;
    assert_oracle(&mut d).await;
    assert_eq!(d.rows(ACTUAL).await, ["(1,2,1)", "(3,8,1)"]);
}

/// A `TRUNCATE` after the build is the ordinary truncate of a live definition.
#[tokio::test]
async fn a_truncate_after_the_build() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)").await;
    define(&d).await;
    run_build(&d).await;
    step(
        &mut d,
        "truncate public.src; insert into public.src values (3, 1, 2)",
    )
    .await;
    assert_oracle(&mut d).await;
    assert_eq!(d.rows(ACTUAL).await, ["(1,2,1)"]);
}

/// The build holds the definition row `for key share` before the job row,
/// the order a resume or a drop (which lock the definition row, then the job
/// row) is ordered against: one that wants the definition row `for update`
/// can't take it while the build runs.
#[tokio::test]
async fn the_build_holds_the_definition_row() {
    let mut d = seeded("(1, 1, 10)").await;
    define(&d).await;
    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    building.reached(PausePoint::AfterRebuildLock).await;
    let error = d
        .ctl
        .batch_execute(
            "select 1 from transform_definitions where target_table = 'public.agg' \
             for update nowait",
        )
        .await
        .expect_err("the build holds the definition row");
    assert_eq!(
        error.code().map(|c| c.code()),
        Some("55P03"),
        "a lock failure: {error}"
    );
    d.release(&mut building, PausePoint::AfterRebuildLock).await;
    building.finish().await;
    assert_oracle(&mut d).await;
}

/// `AVG`, a `SUM` over `numeric` and a composed field build from the
/// absolute partials, so a build writes the same values the pages do.
#[tokio::test]
async fn a_build_writes_averages_numeric_sums_and_composed_fields() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, v integer, \
         m numeric(12, 2)); create index on public.src (g)",
        &[
            ("id", int4()),
            ("g", int4()),
            ("v", int4()),
            ("m", ValueType::Numeric),
        ],
        &[ONE],
        &[SRC],
    )
    .await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1, 1.25), (2, 1, 2, null), (3, 2, 5, 2.50)",
    )
    .await;
    let columns = [
        ("id", int4()),
        ("g", int4()),
        ("v", int4()),
        ("m", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(n, t)| (n.to_string(), t))
    .collect();
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM agg FROM public.src GROUP BY g \
         SELECT AVG(v) AS a, SUM(m) AS sm, SUM(v) + COUNT(m) AS c",
        &columns,
        "public",
    )
    .await
    .expect("define");
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("start the build");
    write(&d, "update public.src set m = 3.75 where id = 2").await;
    let undrained = d.seal().await;
    run_build(&d).await;
    const ACTUAL: &str = "select g, a, sm, c from public.agg order by g";
    const EXPECTED: &str = "select g, avg(v), sum(m), sum(v) + count(m) from public.src \
                            group by g order by g";
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    d.drain(undrained, "undrained").await;
    d.settle().await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
}
