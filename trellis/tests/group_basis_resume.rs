//! `RESUME` of a group-basis aggregate (#1101's decision, #1287): the resume
//! drops and recreates the hidden partials table at the shape define would
//! give it now and keeps the visible target; the build that follows is the
//! plan job's one absolute write of every group, which replaces the rows of
//! the groups the source still has and removes the others. Its commit parks
//! the target's readers (#1287): the seam rows a page stages between the
//! resume and that commit compute their prior images from the empty
//! partials, not from the rows the target holds, so a group-basis reader is
//! rebuilt and any other reader is caught up.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`, frozen at
//! pause points where the scenario needs it. None sleeps or polls for
//! convergence (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
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
const SRC_DDL: &str = "create table public.src (id integer primary key, g integer, v integer); \
                       create index on public.src (g)";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";
const ROLL: &str = "TRANSFORM roll FROM public.agg GROUP BY n WITHOUT SOURCE INDEX \
                    SELECT COUNT(*) AS groups, SUM(total) AS t";
const TOP: &str = "TRANSFORM top FROM public.roll GROUP BY groups WITHOUT SOURCE INDEX \
                   SELECT COUNT(*) AS k, COUNT(t) AS tt";
const MIRROR: &str = "TRANSFORM mirror FROM public.agg SELECT total AS total, n AS n";
const MIRROR_ACTUAL: &str = "select g, total, n from public.mirror order by g";
const ACTUAL: &str = "select g, total, n from public.agg order by g";
const EXPECTED: &str = "select g, sum(v), count(*) from public.src group by g order by g";
const ROLL_ACTUAL: &str = "select n, groups, t from public.roll order by n";
const ROLL_EXPECTED: &str = "select n, count(*), sum(total) from ( \
                                 select count(*) as n, sum(v) as total \
                                 from public.src group by g) s group by n order by n";
const TOP_ACTUAL: &str = "select groups, k, tt from public.top order by groups";
const TOP_EXPECTED: &str = "select groups, count(*), count(t) from ( \
                                select n, count(*) as groups, sum(total) as t from ( \
                                    select count(*) as n, sum(v) as total \
                                    from public.src group by g) s group by n) r \
                            group by groups order by groups";

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn int8() -> ValueType {
    ValueType::Integer(IntWidth::Int8)
}

fn src_columns() -> Vec<(&'static str, ValueType)> {
    vec![("id", int4()), ("g", int4()), ("v", int4())]
}

/// `public.src (id, g, v)` captured, seeded with `rows` (an `insert ...
/// values` tail), with [`AGG`] live over it on the group basis.
async fn start(rows: &str) -> Driver {
    let mut d = Driver::start(SRC_DDL, &src_columns(), &[AGG], &[SRC]).await;
    step(&mut d, &format!("insert into public.src values {rows}")).await;
    assert_oracle(&mut d).await;
    d
}

/// [`start`], with [`ROLL`] live over [`AGG`]'s target, and with `deeper`,
/// [`TOP`] over [`ROLL`]'s.
async fn start_chain(rows: &str, deeper: bool) -> Driver {
    let mut d = start(rows).await;
    install(&d, ROLL, &[("g", int4()), ("total", int8()), ("n", int8())]).await;
    if deeper {
        trellis::intake::markers::settle_registrations(d.pool()).await;
        d.settle().await;
        install(&d, TOP, &[("n", int8()), ("groups", int8()), ("t", int8())]).await;
    }
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    build::settle_builds(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select status from transform_definitions order by id")
            .await,
        vec!["(live)"; 2 + usize::from(deeper)]
    );
    assert_eq!(d.rows(ROLL_ACTUAL).await, d.rows(ROLL_EXPECTED).await);
    d
}

async fn install(d: &Driver, definition: &str, columns: &[(&str, ValueType)]) {
    let columns: HashMap<String, ValueType> = columns
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect();
    trellis::defs::install_definition(d.pool(), definition, &columns, "public")
        .await
        .unwrap_or_else(|e| panic!("install {definition}: {e}"));
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it, and everything it propagates, to quiescence.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    d.settle().await;
}

async fn assert_oracle(d: &mut Driver) {
    d.settle().await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "the target (left) differs from the oracle (right)"
    );
}

fn status(target: &str) -> String {
    format!(
        "select status, build from transform_definitions \
         where target_table = '{target}'"
    )
}

async fn agg_id(d: &Driver) -> i64 {
    d.ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.agg'",
            &[],
        )
        .await
        .expect("the id")
        .get(0)
}

/// The hidden table of [`AGG`], qualified.
async fn hidden(d: &Driver) -> String {
    format!("{DEFAULT_SCHEMA}.agg_{}", agg_id(d).await)
}

/// Pauses [`AGG`] alone, the way a capture disruption does, by hand: its
/// readers stay live.
async fn pause(d: &Driver) {
    d.ctl
        .batch_execute(
            "update transform_definitions set status = 'paused' where target_table = 'public.agg'",
        )
        .await
        .expect("pause");
}

/// Resumes [`AGG`] and starts its build: `backfilling`, one plan job. A
/// resume that re-types a column the staging worker's pass finishes
/// (`reconcile_pass`), which then starts the build itself.
async fn resume(d: &mut Driver) {
    trellis::staging::quarantine::resume_transform(d.pool(), "agg")
        .await
        .expect("resume");
    start_resumed_build(d).await;
}

/// The build of a resume that has committed: the re-type a staging pass
/// finishes, then `start_ready_builds`.
async fn start_resumed_build(d: &mut Driver) {
    if d.rows(&status(TARGET)).await == ["(paused,)"] {
        let pool = d.pool().clone();
        trellis::client::reconcile_pass(
            &mut d.ctl,
            &pool,
            DEFAULT_SCHEMA,
            "group_basis_resume_wake",
            Duration::from_secs(2),
        )
        .await
        .expect("reconcile pass");
    }
    if d.rows(&status(TARGET)).await == ["(waiting_to_backfill,)"] {
        let id = agg_id(d).await;
        let pool = d.pool().clone();
        let taken = build::start_ready_builds(&mut d.ctl, &pool, &[id])
            .await
            .expect("start the resumed build");
        assert_eq!(taken, vec![id]);
    }
    assert_eq!(
        d.rows(&status(TARGET)).await,
        ["(backfilling,rederive)"],
        "the resume leaves the definition building"
    );
    assert_eq!(
        d.rows("select kind from backfill_chunks where not done")
            .await,
        ["(plan)"],
        "a resumed group-basis build is one plan job"
    );
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

/// Runs the resumed build to its commit.
async fn run_build(d: &Driver) {
    build_frozen(d, &[]).await.finish().await;
}

/// The chain's tail after the upstream's resumed build has committed:
/// every reader the build parked is rebuilt, then the pipeline settles and
/// each level agrees with its oracle.
async fn converge(d: &mut Driver, deeper: bool) {
    build::settle_builds(d.pool()).await;
    d.settle().await;
    build::settle_builds(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select status from transform_definitions order by id")
            .await,
        vec!["(live)"; 2 + usize::from(deeper)]
    );
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "agg");
    assert_eq!(
        d.rows(ROLL_ACTUAL).await,
        d.rows(ROLL_EXPECTED).await,
        "roll"
    );
    if deeper {
        assert_eq!(d.rows(TOP_ACTUAL).await, d.rows(TOP_EXPECTED).await, "top");
    }
}

// ------------------------------------------------------------ the resume

/// The resume drops and recreates the hidden partials table (empty, with no
/// basis) and keeps the visible target; the build fills the partials, and
/// the target, from the source.
#[tokio::test]
async fn a_resume_recreates_the_partials_and_keeps_the_target() {
    let mut d = start("(1, 1, 10), (2, 1, 5), (3, 2, 7)").await;
    let before = d
        .rows("select oid::text from pg_class where relname = 'agg'")
        .await;
    let hidden_before = d
        .rows(&format!(
            "select oid::text from pg_class where oid = '{}'::regclass",
            hidden(&d).await
        ))
        .await;
    pause(&d).await;
    write(&d, "insert into public.src values (4, 3, 1), (5, 1, 100)").await;
    resume(&mut d).await;
    assert_eq!(
        d.rows(&format!("select count(*) from {}", hidden(&d).await))
            .await,
        ["(0)"],
        "the partials start over"
    );
    assert_ne!(
        d.rows(&format!(
            "select oid::text from pg_class where oid = '{}'::regclass",
            hidden(&d).await
        ))
        .await,
        hidden_before,
        "a new table"
    );
    assert_eq!(
        d.rows(ACTUAL).await,
        ["(1,15,2)", "(2,7,1)"],
        "the visible target is kept"
    );
    assert_eq!(
        d.rows("select oid::text from pg_class where relname = 'agg'")
            .await,
        before,
        "and is the same table"
    );
    run_build(&d).await;
    assert_oracle(&mut d).await;
    assert_eq!(d.rows(&status(TARGET)).await, ["(live,)"]);
    assert_eq!(
        d.rows(&format!(
            "select count(*) from {} where __basis is not null",
            hidden(&d).await
        ))
        .await,
        ["(3)"],
        "every group carries the build's basis"
    );
}

/// Pause-era writes of every kind: the build converges to the oracle.
#[tokio::test]
async fn changes_written_while_paused_are_built_in() {
    let mut d = start("(1, 1, 10), (2, 1, 5), (3, 2, 7), (4, 3, 2)").await;
    pause(&d).await;
    write(
        &d,
        "insert into public.src values (5, 2, 1); \
         update public.src set v = 50 where id = 1; \
         update public.src set g = 4 where id = 2; \
         delete from public.src where id = 4",
    )
    .await;
    resume(&mut d).await;
    run_build(&d).await;
    assert_oracle(&mut d).await;
    assert_eq!(d.rows(&status(TARGET)).await, ["(live,)"]);
}

/// A dropped `agg_<id>` is recreated by the resume (it used to refuse, #1222).
#[tokio::test]
async fn a_dropped_partials_table_is_recreated() {
    let mut d = start("(1, 1, 10), (2, 2, 7)").await;
    pause(&d).await;
    let hidden = hidden(&d).await;
    d.ctl
        .batch_execute(&format!("drop table {hidden}"))
        .await
        .expect("drop the partials table");
    resume(&mut d).await;
    run_build(&d).await;
    assert_oracle(&mut d).await;
    step(&mut d, "insert into public.src values (3, 1, 1)").await;
    assert_oracle(&mut d).await;
}

/// Rows edited by hand in `agg_<id>` and in the target are repaired by a
/// resume: the partials are dropped, and the build writes every group.
#[tokio::test]
async fn hand_edited_partials_and_target_rows_are_repaired() {
    let mut d = start("(1, 1, 10), (2, 2, 7)").await;
    pause(&d).await;
    d.ctl
        .batch_execute(&format!(
            "update {} set __n = 99, __sum_1 = 1; \
             update public.agg set total = -1; \
             insert into public.agg (g, total, n) values (50, 5, 5)",
            hidden(&d).await
        ))
        .await
        .expect("edit by hand");
    resume(&mut d).await;
    run_build(&d).await;
    assert_oracle(&mut d).await;
}

/// The target holds rows for groups the source no longer has (their rows
/// were deleted while paused, or never existed): the build removes them,
/// since the resume keeps the target rather than truncating it.
#[tokio::test]
async fn target_rows_of_groups_the_source_lost_are_removed() {
    let mut d = start("(1, 1, 10), (2, 2, 7), (3, 3, 4)").await;
    pause(&d).await;
    write(&d, "delete from public.src where g in (1, 2)").await;
    d.ctl
        .batch_execute("insert into public.agg (g, total, n) values (50, 5, 5)")
        .await
        .expect("a row no source row backs");
    resume(&mut d).await;
    assert_eq!(d.rows("select count(*) from public.agg").await, ["(4)"]);
    run_build(&d).await;
    assert_eq!(d.rows(ACTUAL).await, ["(3,4,1)"]);
    assert_oracle(&mut d).await;
}

/// A `GROUP BY` column re-typed while paused: the partials are recreated at
/// the new type (it used to resume over the old one), so a first page with a
/// value past the old type's range applies.
#[tokio::test]
async fn a_retyped_group_column_resumes_over_correctly_typed_partials() {
    let mut d = start("(1, 1, 10), (2, 2, 7)").await;
    pause(&d).await;
    write(&d, "alter table public.src alter column g type bigint").await;
    let column = format!(
        "select format_type(atttypid, atttypmod) from pg_attribute \
         where attrelid = '{}'::regclass and attname = 'g'",
        hidden(&d).await
    );
    assert_eq!(d.rows(&column).await, ["(integer)"]);
    resume(&mut d).await;
    assert_eq!(d.rows(&column).await, ["(bigint)"]);
    run_build(&d).await;
    step(&mut d, "insert into public.src values (3, 5000000000, 4)").await;
    assert_oracle(&mut d).await;
    assert_eq!(
        d.rows(&format!(
            "select count(*) from {} where g = 5000000000",
            hidden(&d).await
        ))
        .await,
        ["(1)"]
    );
}

// -------------------------------------------------------- the readers (#1287)

/// A page applied to a held group between the resume and the build's lock
/// statement computes its prior image from the empty partials, so the seam
/// row it stages moves the reader's groups by a delta against a value the
/// reader never held. The build's commit parks the target's readers instead:
/// a group-basis reader is rebuilt, and converges.
#[tokio::test]
async fn a_group_basis_reader_of_a_resumed_target_is_rebuilt_and_converges() {
    let mut d = start_chain("(1, 1, 10), (2, 1, 5), (3, 2, 7), (4, 3, 1)", false).await;
    pause(&d).await;
    resume(&mut d).await;
    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildEnumerate]).await;
    building.reached(PausePoint::AfterRebuildEnumerate).await;
    // A held group (1) and a group the target holds and the build will
    // write too (2): pages before the build's lock statement.
    write(
        &d,
        "insert into public.src values (5, 1, 100); update public.src set v = 70 where id = 3",
    )
    .await;
    let batch = d.seal().await;
    d.drain(batch, "page").await;
    d.release(&mut building, PausePoint::AfterRebuildEnumerate)
        .await;
    building.finish().await;
    assert_eq!(
        d.rows(&status("public.roll")).await,
        ["(backfilling,rederive)"],
        "the build's commit rebuilds the reader"
    );
    converge(&mut d, false).await;
    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
}

/// The same, two hops down: only the reader of the resumed target is
/// rebuilt. Its own reader receives consistent deltas (the reader's visible
/// rows always match its own partials), and then the rebuild's exact ones.
#[tokio::test]
async fn the_second_reader_converges_without_being_rebuilt() {
    let mut d = start_chain("(1, 1, 10), (2, 1, 5), (3, 2, 7), (4, 3, 1)", true).await;
    pause(&d).await;
    resume(&mut d).await;
    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildEnumerate]).await;
    building.reached(PausePoint::AfterRebuildEnumerate).await;
    write(
        &d,
        "insert into public.src values (5, 1, 100); update public.src set v = 70 where id = 3",
    )
    .await;
    let batch = d.seal().await;
    d.drain(batch, "page").await;
    d.release(&mut building, PausePoint::AfterRebuildEnumerate)
        .await;
    building.finish().await;
    assert_eq!(
        d.rows(&status("public.roll")).await,
        ["(backfilling,rederive)"]
    );
    assert_eq!(
        d.rows(&status("public.top")).await,
        ["(live,)"],
        "the second reader is not rebuilt"
    );
    converge(&mut d, true).await;
    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
}

/// A reader that isn't on the group basis (a 1-1 over the target) gets the
/// image-less catch-up, which re-derives its rows from the target.
#[tokio::test]
async fn a_one_to_one_reader_of_a_resumed_target_is_caught_up() {
    let mut d = start("(1, 1, 10), (2, 2, 7)").await;
    install(
        &d,
        "TRANSFORM mirror FROM public.agg SELECT total AS total, n AS n",
        &[("g", int4()), ("total", int8()), ("n", int8())],
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    build::settle_builds(d.pool()).await;
    d.settle().await;
    pause(&d).await;
    write(&d, "update public.src set v = 70 where id = 2").await;
    resume(&mut d).await;
    run_build(&d).await;
    assert_eq!(
        d.rows(&status("public.mirror")).await,
        ["(catching_up,)"],
        "the build's commit parks the reader's catch-up"
    );
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select g, total, n from public.mirror order by g")
            .await,
        d.rows(ACTUAL).await
    );
    assert_oracle(&mut d).await;
}

// ------------------------------------------- races with work in flight

async fn trellis(d: &Driver) -> trellis::Trellis {
    trellis::Trellis::connect(
        trellis::config::Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

/// A `PAUSE` and `RESUME` issued while a rebuild in place of the `live`
/// aggregate (`request_backfill`) holds every group, frozen after its lock
/// statement: the `PAUSE` waits for the rebuild to commit, and the resume
/// that follows drops the partials it wrote and builds again. No deadlock,
/// and the target and its reader converge.
#[tokio::test]
async fn a_pause_and_resume_wait_for_a_live_rebuild_in_flight() {
    let mut d = start_chain("(1, 1, 10), (2, 1, 5), (3, 2, 7)", false).await;
    let t = trellis(&d).await;
    t.request_backfill("src").await.expect("request a rebuild");
    let mut rebuilding = d
        .run_frozen(
            &[(PausePoint::AfterRebuildLock, TARGET)],
            |pool| async move {
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
            },
        )
        .await;
    let reached = rebuilding.reached(PausePoint::AfterRebuildLock).await;
    write(&d, "insert into public.src values (4, 1, 100), (5, 3, 1)").await;
    let pool = d.pool().clone();
    let operator = tokio::spawn(async move {
        t.apply("PAUSE TRANSFORM agg").await.expect("pause");
        trellis::staging::quarantine::resume_transform(&pool, "agg")
            .await
            .expect("resume");
    });
    d.wait_blocked_behind(reached.backend_pid).await;
    d.release(&mut rebuilding, PausePoint::AfterRebuildLock)
        .await;
    rebuilding.finish().await;
    operator.await.expect("the operator's task");
    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
    assert_eq!(
        d.rows(&format!("select count(*) from {}", hidden(&d).await))
            .await,
        ["(0)"],
        "the resume dropped what the rebuild wrote"
    );
    start_resumed_build(&mut d).await;
    run_build(&d).await;
    converge(&mut d, false).await;
}

/// The build's commit parks its readers last, taking their definition rows
/// `for update` while it holds its groups. Issued while the resumed build
/// holds them, frozen just before that park: a drain whose batch holds a
/// page of the aggregate on a held group (queued behind the build) and the
/// readers' pages of the seam rows an earlier page staged, plus a `PAUSE` of
/// either reader (which completes at once) or an `ALTER TRANSFORM` of the
/// 1-1 one (which queues behind the drain). Nothing deadlocks, and every
/// reader still applying converges.
#[tokio::test]
async fn reader_operations_and_pages_while_the_build_parks() {
    for op in [
        None,
        Some("PAUSE TRANSFORM roll"),
        Some("PAUSE TRANSFORM mirror"),
        Some("ALTER TRANSFORM mirror ADD total + 1 AS t1"),
    ] {
        let what = op.unwrap_or("pages only");
        let mut d = start_chain("(1, 1, 10), (2, 1, 5), (3, 2, 7)", false).await;
        install(
            &d,
            MIRROR,
            &[("g", int4()), ("total", int8()), ("n", int8())],
        )
        .await;
        trellis::intake::markers::settle_registrations(d.pool()).await;
        d.settle().await;
        build::settle_builds(d.pool()).await;
        d.settle().await;
        let t = trellis(&d).await;
        pause(&d).await;
        resume(&mut d).await;
        write(&d, "insert into public.src values (4, 1, 100)").await;
        let batch = d.seal().await;
        d.drain(batch, "page").await;
        write(&d, "insert into public.src values (5, 1, 1), (6, 2, 2)").await;
        let batch = d.seal().await;
        let mut building = build_frozen(&d, &[PausePoint::BeforeRebuildCommit]).await;
        let reached = building.reached(PausePoint::BeforeRebuildCommit).await;
        let page = d.drain_frozen(batch, "held", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        match op {
            Some(op) if op.starts_with("PAUSE") => {
                t.apply(op).await.unwrap_or_else(|e| panic!("{op}: {e}"));
            }
            _ => {}
        }
        let altering = match op {
            Some(op) if op.starts_with("ALTER") => {
                let altering = tokio::spawn(async move {
                    t.apply(op).await.unwrap_or_else(|e| panic!("{op}: {e}"));
                });
                // The build (behind its gate), the drain and the `ALTER`.
                d.wait_blocked(3).await;
                Some(altering)
            }
            _ => None,
        };
        d.release(&mut building, PausePoint::BeforeRebuildCommit)
            .await;
        building.finish().await;
        page.finish().await;
        if let Some(altering) = altering {
            altering.await.expect("the ALTER's task");
        }
        assert_eq!(d.deadlocks_logged(), Vec::<String>::new(), "{what}");
        for _ in 0..2 {
            build::settle_builds(d.pool()).await;
            trellis::intake::markers::settle_registrations(d.pool()).await;
            d.settle().await;
        }
        assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "{what}: agg");
        if op != Some("PAUSE TRANSFORM roll") {
            assert_eq!(
                d.rows(ROLL_ACTUAL).await,
                d.rows(ROLL_EXPECTED).await,
                "{what}: roll"
            );
        }
        if op != Some("PAUSE TRANSFORM mirror") {
            assert_eq!(
                d.rows(MIRROR_ACTUAL).await,
                d.rows(ACTUAL).await,
                "{what}: mirror"
            );
        }
    }
}
